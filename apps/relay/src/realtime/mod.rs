//! 实时面核心(§17.4/§17.5/§17.6):流管理、订阅顺序、Ack/重放/Resync、
//! 命令与查询路由、Bridge/Browser 连接注册表。
//!
//! 订阅顺序(§17.4 逐字):
//! 1. Browser 发送 Subscribe。
//! 2. Relay 为该 subscriber 建立有界队列,并向 Bridge 建立/复用上游订阅。
//! 3. Bridge 固定 snapshot 对应的 `stream_epoch/base_sequence`(Subscribed)。
//! 4. snapshot 生成期间的新事件进入缓冲(awaiting_snapshot 的订阅者不直收事件)。
//! 5. Relay 先发 RuntimeSnapshot(或列表快照),再发 `base_sequence` 之后的事件。
//! 6. Browser 应用后发送 Ack。
//!
//! 下游 sequence 由 Relay 重新分配(每个事件一个 sequence);Bridge 侧
//! `Subscribed.epoch` 锚定上游纪元,Bridge EventBatch 的批级 sequence 仅用于
//! Relay 的上游去重与 epoch 变化检测。

pub mod buffer;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use agent_console_protocol::{
    codec::{new_message_id, PROTOCOL_VERSION},
    v1::{domain_event, envelope, subscribe},
    v1::{
        CommandAccepted, CommandReceiptStatus as Receipt, CommandRequest, CommandResult,
        DevicePresenceChanged, DomainEvent, Envelope, EventBatch, HeartbeatAck, ProtocolError,
        QueryRequest, QueryResponse, ResyncRequest, ResyncRequired, SessionKey, SessionList,
        StableErrorCode, Subscribe, Subscribed,
    },
};
use prost_types::Timestamp;
use tokio::sync::oneshot;

use crate::state::{limits, new_request_id, stable_code_ref_name, AppState};

use buffer::{classify, Frame, FrameKind, Outbox, PushError, StreamBuffer};

// ---------------------------------------------------------------------------
// 领域类型
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TargetTag {
    /// 列表订阅:该 owner 全部在线设备的 SessionSummary。
    List,
    /// 详情订阅:单会话流。
    Session {
        device: uuid::Uuid,
        native_session_id: String,
    },
}

type DKey = (uuid::Uuid, TargetTag);

/// 下游 subscriber 状态。
struct SubscriberState {
    /// 尚未收到首个 snapshot:事件只入缓冲,不发往订阅者(§17.4 步骤 4/5)。
    awaiting_snapshot: bool,
    acked: u64,
}

struct UpstreamBinding {
    device: uuid::Uuid,
    epoch: u64,
    /// 已处理到的上游批 sequence(去重水位;0 表示未定)。
    last_seq: u64,
    snapshot_received: bool,
}

struct DStream {
    #[allow(dead_code)] // 调试/未来路由使用;当前 stream_id 即唯一标识。
    key: DKey,
    stream_id: String,
    epoch: u64,
    next_seq: u64,
    /// 已缓存的 snapshot 帧(会话流通常 1 条;列表流每设备一条,有界)。
    snapshots: VecDeque<Arc<Frame>>,
    buffer: StreamBuffer,
    subscribers: HashMap<uuid::Uuid, SubscriberState>,
    /// 上游绑定:upstream_stream_id → binding。
    upstreams: HashMap<String, UpstreamBinding>,
}

impl DStream {
    fn new(key: DKey) -> Self {
        Self {
            stream_id: format!("s-{}", uuid::Uuid::new_v4()),
            key,
            epoch: 1,
            next_seq: 1,
            snapshots: VecDeque::new(),
            buffer: StreamBuffer::new(
                limits::BUFFER_MAX_EVENTS,
                limits::BUFFER_MAX_BYTES,
                limits::BUFFER_RETENTION,
            ),
            subscribers: HashMap::new(),
            upstreams: HashMap::new(),
        }
    }

    /// 缓存 snapshot 的锚点 sequence(最早一条)。
    fn snapshot_anchor(&self) -> Option<u64> {
        self.snapshots.front().map(|f| f.env.sequence)
    }
}

struct BridgeConn {
    device: uuid::Uuid,
    owner: uuid::Uuid,
    sink: tokio::sync::mpsc::Sender<WireBytes>,
    /// 连接关闭信号(撤销设备立即断开,§21.9)。
    close: tokio::sync::watch::Sender<bool>,
}

struct BrowserConn {
    outbox: Arc<Outbox>,
    ident: BrowserIdentity,
}

/// 浏览器连接身份(经 dev-toolbox ticket 消费获得)。
#[derive(Clone, Debug)]
pub struct BrowserIdentity {
    pub auth_session_id: uuid::Uuid,
    pub owner_id: uuid::Uuid,
    /// 原始 expires_at;本地到达即关闭,无需远程查询(§20.5)。
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

struct PendingCommand {
    conn_id: uuid::Uuid,
    device: uuid::Uuid,
    operation: String,
    started_at: std::time::Instant,
}

#[derive(Default)]
struct HubInner {
    bridges: HashMap<uuid::Uuid, BridgeConn>,
    browsers: HashMap<uuid::Uuid, BrowserConn>,
    /// upstream_stream_id → DKey 路由索引。
    upstream_index: HashMap<String, DKey>,
    streams: HashMap<DKey, DStream>,
    /// browser conn → 订阅的 DStream 集合。
    browser_streams: HashMap<uuid::Uuid, HashSet<DKey>>,
    commands: HashMap<String, PendingCommand>,
    queries: HashMap<String, oneshot::Sender<QueryResponse>>,
    /// dev-toolbox 不可达宽限截止(§20.5);None 表示正常。
    degraded_until: Option<std::time::Instant>,
    shutdown: bool,
}

pub struct Hub {
    inner: std::sync::Mutex<HubInner>,
    /// 全局缓冲字节估算(监控周期重算)。
    pub global_buffer_bytes: Arc<AtomicU64>,
}

fn now_ts() -> Option<Timestamp> {
    Some(Timestamp::from(std::time::SystemTime::now()))
}

fn base_envelope(device_id: &str, payload: envelope::Payload) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: now_ts(),
        device_id: device_id.to_string(),
        agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(payload),
        provider_extension: None,
    }
}

pub type WireBytes = buffer::WireBytes;

fn encode_direct(env: &Envelope) -> WireBytes {
    agent_console_protocol::codec::encode_envelope(env)
        .map(Into::into)
        .unwrap_or_default()
}

fn upstream_stream_id(device: uuid::Uuid, tag: &TargetTag) -> String {
    match tag {
        TargetTag::List => format!("u-{device}-list"),
        TargetTag::Session {
            device,
            native_session_id,
        } => {
            format!("u-{device}-sess-{native_session_id}")
        }
    }
}

fn resync_required_env(stream_id: &str) -> Envelope {
    base_envelope(
        "",
        envelope::Payload::ResyncRequired(ResyncRequired {
            stream_id: stream_id.to_string(),
            reason_code: StableErrorCode::ResyncRequired as i32,
        }),
    )
}

fn subscribed_env(stream_id: &str, epoch: u64, base_sequence: u64) -> Envelope {
    base_envelope(
        "",
        envelope::Payload::Subscribed(Subscribed {
            stream_id: stream_id.to_string(),
            stream_epoch: epoch,
            base_sequence,
        }),
    )
}

impl Hub {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(HubInner::default()),
            global_buffer_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    // -----------------------------------------------------------------------
    // Browser 注册表
    // -----------------------------------------------------------------------

    pub fn register_browser(&self, ident: BrowserIdentity, outbox: Arc<Outbox>) -> uuid::Uuid {
        let conn_id = uuid::Uuid::new_v4();
        self.inner
            .lock()
            .unwrap()
            .browsers
            .insert(conn_id, BrowserConn { outbox, ident });
        conn_id
    }

    pub fn browser_outbox(&self, conn_id: uuid::Uuid) -> Option<Arc<Outbox>> {
        self.inner
            .lock()
            .unwrap()
            .browsers
            .get(&conn_id)
            .map(|b| b.outbox.clone())
    }

    pub fn browser_identity(&self, conn_id: uuid::Uuid) -> Option<BrowserIdentity> {
        self.inner
            .lock()
            .unwrap()
            .browsers
            .get(&conn_id)
            .map(|b| b.ident.clone())
    }

    /// introspection 循环用:全部活跃连接的 (conn_id, auth_session_id, expires_at)。
    pub fn browser_sessions(&self) -> Vec<(uuid::Uuid, uuid::Uuid, chrono::DateTime<chrono::Utc>)> {
        self.inner
            .lock()
            .unwrap()
            .browsers
            .iter()
            .map(|(id, b)| (*id, b.ident.auth_session_id, b.ident.expires_at))
            .collect()
    }

    /// 断开指定浏览器连接(稳定 close reason)。
    pub fn close_browser(&self, conn_id: uuid::Uuid, reason: &'static str) {
        let outbox = {
            let mut g = self.inner.lock().unwrap();
            let outbox = g.browsers.remove(&conn_id).map(|b| b.outbox);
            if outbox.is_some() {
                Self::detach_subscriber(&mut g, conn_id);
            }
            outbox
        };
        if let Some(outbox) = outbox {
            outbox.close(reason);
        }
    }

    /// 关闭全部浏览器连接(宽限结束/shutdown,§20.5/§26.4)。
    pub fn close_all_browsers(&self, reason: &'static str) {
        let mut g = self.inner.lock().unwrap();
        let conn_ids: Vec<uuid::Uuid> = g.browsers.keys().cloned().collect();
        for conn_id in conn_ids {
            let outbox = g.browsers.remove(&conn_id).map(|b| b.outbox);
            Self::detach_subscriber(&mut g, conn_id);
            if let Some(outbox) = outbox {
                outbox.close(reason);
            }
        }
    }

    /// Browser 连接断开:摘除订阅与命令路由。
    pub fn remove_browser(&self, conn_id: uuid::Uuid) {
        let mut g = self.inner.lock().unwrap();
        g.browsers.remove(&conn_id);
        Self::detach_subscriber(&mut g, conn_id);
    }

    fn detach_subscriber(g: &mut HubInner, conn_id: uuid::Uuid) {
        if let Some(keys) = g.browser_streams.remove(&conn_id) {
            for key in keys {
                if let Some(ds) = g.streams.get_mut(&key) {
                    ds.subscribers.remove(&conn_id);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Bridge 注册表
    // -----------------------------------------------------------------------

    /// 注册在线 Bridge(旧连接由调用方先关闭其 socket)。
    pub fn register_bridge(
        &self,
        device: uuid::Uuid,
        owner: uuid::Uuid,
        sink: tokio::sync::mpsc::Sender<WireBytes>,
        close: tokio::sync::watch::Sender<bool>,
    ) {
        {
            let mut g = self.inner.lock().unwrap();
            g.bridges.insert(
                device,
                BridgeConn {
                    device,
                    owner,
                    sink,
                    close,
                },
            );
        }
        // 为已存在的订阅重建上游绑定(重连后立即同步 summary/snapshot,§26.2)。
        self.ensure_bindings_for_device(device, owner);
    }

    /// 撤销设备:立即通知关闭 Bridge socket(§21.9);连接任务随后走统一离线清理。
    pub fn force_close_bridge(&self, device: uuid::Uuid) {
        let close = self
            .inner
            .lock()
            .unwrap()
            .bridges
            .get(&device)
            .map(|b| b.close.clone());
        if let Some(close) = close {
            let _ = close.send(true);
        }
    }

    pub fn is_online(&self, device: uuid::Uuid) -> bool {
        self.inner.lock().unwrap().bridges.contains_key(&device)
    }

    pub fn bridge_owner(&self, device: uuid::Uuid) -> Option<uuid::Uuid> {
        self.inner
            .lock()
            .unwrap()
            .bridges
            .get(&device)
            .map(|b| b.owner)
    }

    /// 注销 Bridge;返回仍未拿到最终结果的命令 (request_id, operation),
    /// 由调用方落库 OUTCOME_UNKNOWN(§15.2/§26.4)。
    pub fn remove_bridge(&self, device: uuid::Uuid) -> Vec<(String, String)> {
        let mut g = self.inner.lock().unwrap();
        g.bridges.remove(&device);
        // 清理上游绑定。
        let prefix = format!("u-{device}-");
        let dead: Vec<String> = g
            .upstream_index
            .keys()
            .filter(|id| id.starts_with(&prefix))
            .cloned()
            .collect();
        for id in &dead {
            g.upstream_index.remove(id);
        }
        for ds in g.streams.values_mut() {
            ds.upstreams.retain(|k, _| !dead.contains(k));
        }
        // 未完成命令:OUTCOME_UNKNOWN 回执给 browser(§15.2)。
        let ids: Vec<String> = g
            .commands
            .iter()
            .filter(|(_, p)| p.device == device)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(p) = g.commands.remove(&id) {
                out.push((id.clone(), p.operation));
                if let Some(bc) = g.browsers.get(&p.conn_id) {
                    let env = base_envelope(
                        &device.to_string(),
                        envelope::Payload::CommandResult(CommandResult {
                            request_id: id,
                            status: Receipt::ReceiptOutcomeUnknown as i32,
                            error_code: StableErrorCode::OutcomeUnknown as i32,
                            details: Default::default(),
                            duration_ms: None,
                        }),
                    );
                    bc.outbox.push_direct(encode_direct(&env));
                }
            }
        }
        out
    }

    pub fn online_devices_of_owner(&self, owner: uuid::Uuid) -> Vec<uuid::Uuid> {
        self.inner
            .lock()
            .unwrap()
            .bridges
            .values()
            .filter(|b| b.owner == owner)
            .map(|b| b.device)
            .collect()
    }

    fn send_to_bridge_locked(g: &HubInner, device: uuid::Uuid, env: Envelope) -> bool {
        match g.bridges.get(&device) {
            Some(conn) => conn.sink.try_send(encode_direct(&env)).is_ok(),
            None => false,
        }
    }

    pub fn send_to_bridge(&self, device: uuid::Uuid, env: Envelope) -> bool {
        let g = self.inner.lock().unwrap();
        Self::send_to_bridge_locked(&g, device, env)
    }

    // -----------------------------------------------------------------------
    // 降级与停机(§20.5/§26.4)
    // -----------------------------------------------------------------------

    pub fn set_degraded(&self, until: Option<std::time::Instant>) {
        self.inner.lock().unwrap().degraded_until = until;
    }

    pub fn degraded(&self) -> Option<std::time::Instant> {
        self.inner.lock().unwrap().degraded_until
    }

    /// dev-toolbox 不可达宽限期内停止接受写命令(§20.5);宽限尽由 introspection 循环关闭。
    pub fn writes_allowed(&self) -> bool {
        let until = self.inner.lock().unwrap().degraded_until;
        match until {
            Some(until) => std::time::Instant::now() < until,
            None => true,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.lock().unwrap().shutdown
    }

    /// 优雅停机第一步:停止接受新命令;给已接受命令明确回执状态(§26.4)。
    /// 返回需落库为 OUTCOME_UNKNOWN 的 (request_id, operation)。
    pub fn begin_shutdown(&self) -> Vec<(String, String)> {
        let mut g = self.inner.lock().unwrap();
        g.shutdown = true;
        let pendings: Vec<(String, PendingCommand)> = g.commands.drain().collect();
        let mut out = Vec::with_capacity(pendings.len());
        for (request_id, p) in pendings {
            if let Some(bc) = g.browsers.get(&p.conn_id) {
                let env = base_envelope(
                    &p.device.to_string(),
                    envelope::Payload::CommandResult(CommandResult {
                        request_id: request_id.clone(),
                        status: Receipt::ReceiptOutcomeUnknown as i32,
                        error_code: StableErrorCode::OutcomeUnknown as i32,
                        details: Default::default(),
                        duration_ms: None,
                    }),
                );
                bc.outbox.push_direct(encode_direct(&env));
            }
            out.push((request_id, p.operation));
        }
        out
    }

    // -----------------------------------------------------------------------
    // 订阅(§17.4)
    // -----------------------------------------------------------------------

    fn upstream_subscribe_env(device: uuid::Uuid, target: &TargetTag) -> Envelope {
        let payload_target = match target {
            TargetTag::List => subscribe_target_list(),
            TargetTag::Session {
                native_session_id, ..
            } => subscribe_target_session(device, native_session_id),
        };
        let mut env = base_envelope(
            &device.to_string(),
            envelope::Payload::Subscribe(Subscribe {
                target: Some(payload_target),
            }),
        );
        env.stream_id = upstream_stream_id(device, target);
        env.correlation_id = new_message_id();
        env
    }

    /// Bridge 新上线/重连:为该 owner 的既有订阅重建此设备的上游绑定。
    fn ensure_bindings_for_device(&self, device: uuid::Uuid, owner: uuid::Uuid) {
        let envs: Vec<Envelope> = {
            let mut g = self.inner.lock().unwrap();
            let mut envs = Vec::new();
            let keys: Vec<DKey> = g
                .streams
                .keys()
                .filter(|(o, tag)| {
                    *o == owner
                        && match tag {
                            TargetTag::List => true,
                            TargetTag::Session { device: d, .. } => *d == device,
                        }
                })
                .cloned()
                .collect();
            for key in keys {
                let uid = upstream_stream_id(device, &key.1);
                let ds = g.streams.get_mut(&key).unwrap();
                if !ds.upstreams.contains_key(&uid) {
                    ds.upstreams.insert(
                        uid.clone(),
                        UpstreamBinding {
                            device,
                            epoch: 0,
                            last_seq: 0,
                            snapshot_received: false,
                        },
                    );
                    g.upstream_index.insert(uid.clone(), key.clone());
                    envs.push(Self::upstream_subscribe_env(device, &key.1));
                }
            }
            envs
        };
        for env in envs {
            self.send_to_bridge(device, env);
        }
    }

    /// Browser Subscribe 入口(needs_binding:需要建立上游绑定的设备)。
    pub fn browser_subscribe(
        &self,
        conn_id: uuid::Uuid,
        key: DKey,
        needs_binding: Vec<(uuid::Uuid, TargetTag)>,
    ) {
        let mut g1 = self.inner.lock().unwrap();
        if !g1.browsers.contains_key(&conn_id) {
            return;
        }
        let mut sub_envs: Vec<(uuid::Uuid, Envelope)> = Vec::new();
        {
            // 拆字段借用,避免 streams/upstream_index 冲突。
            let HubInner {
                streams,
                upstream_index,
                ..
            } = &mut *g1;
            for (device, tag) in needs_binding {
                let uid = upstream_stream_id(device, &tag);
                let ds = streams
                    .entry(key.clone())
                    .or_insert_with(|| DStream::new(key.clone()));
                if !ds.upstreams.contains_key(&uid) {
                    ds.upstreams.insert(
                        uid.clone(),
                        UpstreamBinding {
                            device,
                            epoch: 0,
                            last_seq: 0,
                            snapshot_received: false,
                        },
                    );
                    upstream_index.insert(uid.clone(), key.clone());
                    sub_envs.push((device, Self::upstream_subscribe_env(device, &tag)));
                }
            }
        }
        for (device, env) in sub_envs {
            Self::send_to_bridge_locked(&g1, device, env);
        }
        drop(g1);
        // 挂接 subscriber(§17.4 步骤 2):有 snapshot 则立刻按顺序回放。
        let mut g = self.inner.lock().unwrap();
        if !g.browsers.contains_key(&conn_id) {
            return;
        }
        let HubInner {
            streams,
            browsers,
            browser_streams,
            ..
        } = &mut *g;
        let ds = streams
            .entry(key.clone())
            .or_insert_with(|| DStream::new(key.clone()));
        let outbox = browsers.get(&conn_id).map(|b| b.outbox.clone());
        let Some(outbox) = outbox else { return };
        let has_snapshot = !ds.snapshots.is_empty();
        {
            let sub = ds.subscribers.entry(conn_id).or_insert(SubscriberState {
                awaiting_snapshot: !has_snapshot,
                acked: 0,
            });
            sub.awaiting_snapshot = !has_snapshot;
        }
        browser_streams.entry(conn_id).or_default().insert(key);

        if has_snapshot {
            // 已有 snapshot:Subscribed → snapshot → 缓冲事件(§17.4 步骤 5)。
            let epoch = ds.epoch;
            let anchor = ds.snapshot_anchor().unwrap_or(0);
            outbox.push_direct(encode_direct(&subscribed_env(&ds.stream_id, epoch, anchor)));
            for snap in &ds.snapshots {
                let _ = outbox.push_frame(snap.clone());
            }
            for (i, frame) in ds.buffer.items.iter().enumerate() {
                let _ = outbox.push_frame(frame.renumbered(epoch, anchor + 1 + i as u64));
            }
        }
        // 尚无 snapshot 时不预发 Subscribed:到达后由 flush 统一按
        // Subscribed → snapshot → 缓冲事件发送(§17.4 顺序唯一)。
    }

    pub fn browser_unsubscribe(&self, conn_id: uuid::Uuid, stream_id: &str) {
        let mut g = self.inner.lock().unwrap();
        let key = g
            .streams
            .iter()
            .find(|(_, ds)| ds.stream_id == stream_id)
            .map(|(k, _)| k.clone());
        if let Some(key) = key {
            if let Some(ds) = g.streams.get_mut(&key) {
                ds.subscribers.remove(&conn_id);
            }
            if let Some(set) = g.browser_streams.get_mut(&conn_id) {
                set.remove(&key);
            }
        }
    }

    /// Browser ResyncRequest(§17.5):窗口内重发 snapshot+缓冲;无 snapshot 则继续等待。
    pub fn browser_resync(&self, conn_id: uuid::Uuid, stream_id: &str) {
        let mut g = self.inner.lock().unwrap();
        let HubInner {
            streams, browsers, ..
        } = &mut *g;
        let Some((_, ds)) = streams.iter_mut().find(|(_, ds)| ds.stream_id == stream_id) else {
            return;
        };
        match ds.snapshot_anchor() {
            Some(anchor) => {
                if let Some(sub) = ds.subscribers.get_mut(&conn_id) {
                    sub.awaiting_snapshot = false;
                }
                let outbox = browsers.get(&conn_id).map(|b| b.outbox.clone());
                let Some(outbox) = outbox else { return };
                let (epoch, stream_id) = (ds.epoch, ds.stream_id.clone());
                outbox.push_direct(encode_direct(&subscribed_env(&stream_id, epoch, anchor)));
                for snap in &ds.snapshots {
                    let _ = outbox.push_frame(snap.clone());
                }
                for (i, frame) in ds.buffer.items.iter().enumerate() {
                    let _ = outbox.push_frame(frame.renumbered(epoch, anchor + 1 + i as u64));
                }
            }
            None => {
                if let Some(sub) = ds.subscribers.get_mut(&conn_id) {
                    sub.awaiting_snapshot = true;
                }
            }
        }
    }

    pub fn browser_ack(&self, conn_id: uuid::Uuid, stream_id: &str, sequence: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some((_, ds)) = g
            .streams
            .iter_mut()
            .find(|(_, ds)| ds.stream_id == stream_id)
        {
            if let Some(sub) = ds.subscribers.get_mut(&conn_id) {
                sub.acked = sub.acked.max(sequence);
            }
        }
    }

    // -----------------------------------------------------------------------
    // 上游消息路由(Bridge → 订阅者)
    // -----------------------------------------------------------------------

    fn send_protocol_error(
        outbox: &Outbox,
        code: StableErrorCode,
        request_id: &str,
        stream_id: &str,
        msg: &str,
    ) {
        let env = base_envelope(
            "",
            envelope::Payload::ProtocolError(ProtocolError {
                error_code: code as i32,
                message: msg.to_string(),
                request_id: request_id.to_string(),
                stream_id: stream_id.to_string(),
                details: Default::default(),
            }),
        );
        outbox.push_direct(encode_direct(&env));
    }

    /// 上游 Subscribed:固定 epoch/水位;epoch 变化时重置流并要求重新同步(§17.5)。
    fn handle_upstream_subscribed(
        g: &mut HubInner,
        upstream_id: &str,
        _env: &Envelope,
        sub: &Subscribed,
    ) {
        let Some(key) = g.upstream_index.get(upstream_id).cloned() else {
            return;
        };
        let (epoch_changed, rebinding) = {
            let Some(ds) = g.streams.get_mut(&key) else {
                return;
            };
            let changed = {
                let Some(binding) = ds.upstreams.get_mut(upstream_id) else {
                    return;
                };
                let c = binding.snapshot_received && binding.epoch != sub.stream_epoch;
                binding.epoch = sub.stream_epoch;
                binding.last_seq = sub.base_sequence.saturating_sub(1);
                binding.snapshot_received = false;
                c
            };
            (
                changed,
                ds.upstreams
                    .values()
                    .map(|b| (b.device, key.1.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        if !epoch_changed {
            return;
        }
        // epoch 变化:清空 snapshot/缓冲,提升下游 epoch,通知重同步,重取 snapshot。
        let mut sub_envs: Vec<(uuid::Uuid, Envelope)> = Vec::new();
        {
            let HubInner {
                streams, browsers, ..
            } = &mut *g;
            let Some(ds) = streams.get_mut(&key) else {
                return;
            };
            ds.epoch += 1;
            ds.snapshots.clear();
            ds.buffer.clear();
            let stream_id = ds.stream_id.clone();
            let conn_ids: Vec<uuid::Uuid> = ds.subscribers.keys().cloned().collect();
            for conn_id in conn_ids {
                if let Some(s) = ds.subscribers.get_mut(&conn_id) {
                    s.awaiting_snapshot = true;
                }
                if let Some(bc) = browsers.get(&conn_id) {
                    bc.outbox
                        .push_direct(encode_direct(&resync_required_env(&stream_id)));
                }
            }
        }
        for (device, tag) in rebinding {
            let uid = upstream_stream_id(device, &tag);
            if let Some(ds) = g.streams.get_mut(&key) {
                if let Some(b) = ds.upstreams.get_mut(&uid) {
                    b.snapshot_received = false;
                    b.epoch = 0;
                    b.last_seq = 0;
                }
            }
            sub_envs.push((device, Self::upstream_subscribe_env(device, &tag)));
        }
        for (device, e) in sub_envs {
            Self::send_to_bridge_locked(g, device, e);
        }
    }

    /// 上游 snapshot(RuntimeSnapshot 或 snapshot=true 的 SessionSummaryBatch)。
    fn handle_upstream_snapshot(g: &mut HubInner, upstream_id: &str, env: &Envelope) {
        let Some(key) = g.upstream_index.get(upstream_id).cloned() else {
            return;
        };
        let HubInner {
            streams, browsers, ..
        } = &mut *g;
        let Some(ds) = streams.get_mut(&key) else {
            return;
        };
        if let Some(binding) = ds.upstreams.get_mut(upstream_id) {
            binding.snapshot_received = true;
        }
        // 下游 snapshot 帧:重新编号并缓存。
        let seq = ds.next_seq;
        ds.next_seq += 1;
        let mut frame_env = env.clone();
        frame_env.stream_id = ds.stream_id.clone();
        frame_env.stream_epoch = ds.epoch;
        frame_env.sequence = seq;
        let frame = Frame::new(frame_env, FrameKind::Critical, None);
        ds.snapshots.push_back(frame.clone());
        while ds.snapshots.len() > 16 {
            ds.snapshots.pop_front();
        }
        // flush(§17.4 步骤 5):等待 snapshot 的订阅者按 Subscribed → snapshot → 缓冲;
        // 已活跃订阅者把 snapshot 当普通帧应用。
        let (anchor, stream_id, epoch) = (seq, ds.stream_id.clone(), ds.epoch);
        let buffered: Vec<Arc<Frame>> = ds.buffer.items.iter().cloned().collect();
        let conn_ids: Vec<uuid::Uuid> = ds.subscribers.keys().cloned().collect();
        for conn_id in conn_ids {
            let awaiting = ds
                .subscribers
                .get_mut(&conn_id)
                .map(|s| s.awaiting_snapshot);
            let outbox = browsers.get(&conn_id).map(|b| b.outbox.clone());
            let (Some(awaiting), Some(outbox)) = (awaiting, outbox) else {
                continue;
            };
            if awaiting {
                outbox.push_direct(encode_direct(&subscribed_env(&stream_id, epoch, anchor)));
                let _ = outbox.push_frame(frame.clone());
                // 缓冲事件在 snapshot 之后投递:为该订阅者重新编号,保证同
                // stream+epoch 内 sequence 单调(§17.5)。
                for (i, f) in buffered.iter().enumerate() {
                    let _ = outbox.push_frame(f.renumbered(epoch, anchor + 1 + i as u64));
                }
                ds.subscribers.get_mut(&conn_id).unwrap().awaiting_snapshot = false;
            } else {
                let _ = outbox.push_frame(frame.clone());
            }
        }
    }

    /// 上游事件 → 下游逐事件帧(重编号、缓冲、扇出、慢 consumer 处理)。
    /// 同时按 §24 白名单收集 push 触发(锁外异步发送)。
    fn fan_out_events(
        g: &mut HubInner,
        upstream_id: &str,
        env: &Envelope,
        events: Vec<domain_event::Event>,
    ) -> Vec<crate::push::PushTrigger> {
        let mut push_triggers: Vec<crate::push::PushTrigger> = Vec::new();
        // 上游去重与水位(批级 sequence;0 表示未提供,跳过去重)。
        let Some(key) = g.upstream_index.get(upstream_id).cloned() else {
            return push_triggers;
        };
        {
            let Some(ds) = g.streams.get_mut(&key) else {
                return push_triggers;
            };
            let binding = ds.upstreams.get_mut(upstream_id).unwrap();
            if env.sequence != 0 {
                if env.sequence <= binding.last_seq {
                    tracing::debug!(target: "relay::realtime", "duplicate upstream batch sequence ignored");
                    return push_triggers;
                }
                binding.last_seq = env.sequence;
            }
        }

        if events.is_empty() {
            return push_triggers;
        }

        // push 触发映射(§24):turn 终态 / 等待问题 / 等待审批。
        // 列表流上仅摘要事件携带目标会话;详情流用 tag 目标。
        let (tag_device, tag_native) = match &key.1 {
            TargetTag::Session {
                device,
                native_session_id,
            } => (Some(*device), Some(native_session_id.clone())),
            TargetTag::List => (None, None),
        };
        let owner = key.0;
        for event in &events {
            let (device, native, agent_kind) = match event {
                domain_event::Event::SessionSummaryChanged(s) => match s.session_key.as_ref() {
                    Some(k) => (
                        uuid::Uuid::parse_str(&k.device_id).ok(),
                        Some(k.native_session_id.clone()),
                        crate::realtime::agent_kind_name(k.agent_kind),
                    ),
                    None => continue,
                },
                _ => (
                    tag_device,
                    tag_native.clone(),
                    "AGENT_KIND_CODEX_DESKTOP".to_string(),
                ),
            };
            let (Some(device), Some(native)) = (device, native) else {
                continue;
            };
            push_triggers.extend(crate::push::triggers_from_event(
                owner,
                device,
                &agent_kind,
                &native,
                event,
            ));
        }

        let HubInner {
            streams,
            browsers,
            browser_streams,
            ..
        } = &mut *g;
        let Some(ds) = streams.get_mut(&key) else {
            return push_triggers;
        };
        let device = env.device_id.clone();
        let mut slow_all: Vec<uuid::Uuid> = Vec::new();
        for event in events {
            let (kind, merge) = classify(&event);
            let seq = ds.next_seq;
            ds.next_seq += 1;
            let mut frame_env = base_envelope(
                &device,
                envelope::Payload::EventBatch(EventBatch {
                    stream_id: ds.stream_id.clone(),
                    events: vec![DomainEvent {
                        emitted_at: env.sent_at,
                        event: Some(event),
                    }],
                }),
            );
            frame_env.stream_id = ds.stream_id.clone();
            frame_env.stream_epoch = ds.epoch;
            frame_env.sequence = seq;
            let frame = Frame::new(frame_env, kind, merge);

            // 入缓冲(三重上限 + 优先级;OverflowReset 时广播 ResyncRequired,§17.6)。
            let outcome = ds.buffer.insert(frame.clone());
            if outcome == buffer::InsertOutcome::OverflowReset {
                let stream_id = ds.stream_id.clone();
                for conn_id in ds.subscribers.keys().cloned().collect::<Vec<_>>() {
                    if let Some(bc) = browsers.get(&conn_id) {
                        bc.outbox
                            .push_direct(encode_direct(&resync_required_env(&stream_id)));
                    }
                }
                continue;
            }

            // 扇出给已过 snapshot 的订阅者;慢 consumer → ResyncRequired + 断开(§17.6)。
            for (conn_id, sub) in ds.subscribers.iter_mut() {
                if sub.awaiting_snapshot {
                    continue;
                }
                let Some(bc) = browsers.get(conn_id) else {
                    continue;
                };
                if let Err(PushError::SlowConsumer) = bc.outbox.push_frame(frame.clone()) {
                    slow_all.push(*conn_id);
                }
            }
        }
        // 慢 consumer 处理(事件循环结束后统一执行,不阻塞上游)。
        if !slow_all.is_empty() {
            if let Some(ds) = streams.get_mut(&key) {
                let stream_id = ds.stream_id.clone();
                for conn_id in &slow_all {
                    if let Some(bc) = browsers.get(conn_id) {
                        bc.outbox
                            .push_direct(encode_direct(&resync_required_env(&stream_id)));
                        bc.outbox.close("RESYNC_REQUIRED");
                    }
                    ds.subscribers.remove(conn_id);
                    if let Some(set) = browser_streams.get_mut(conn_id) {
                        set.remove(&key);
                    }
                }
            }
            tracing::debug!(target: "relay::realtime", "slow consumer resynced and detached");
        }
        push_triggers
    }

    /// Bridge presence → 该 owner 的列表订阅者(设备上下线必须可靠展示,按 P1 处理)。
    pub fn fan_out_presence(&self, device: uuid::Uuid, owner: uuid::Uuid, online: bool) {
        let mut g = self.inner.lock().unwrap();
        let event = domain_event::Event::DevicePresenceChanged(DevicePresenceChanged {
            presence: Some(agent_console_protocol::v1::DevicePresence {
                device_id: device.to_string(),
                connection: if online {
                    agent_console_protocol::v1::DeviceConnection::ConnectionOnline as i32
                } else {
                    agent_console_protocol::v1::DeviceConnection::ConnectionOffline as i32
                },
                last_seen_at: now_ts(),
                degraded_reason: String::new(),
            }),
        });
        let HubInner {
            streams,
            browsers,
            browser_streams,
            ..
        } = &mut *g;
        let keys: Vec<DKey> = streams
            .keys()
            .filter(|(o, tag)| *o == owner && matches!(tag, TargetTag::List))
            .cloned()
            .collect();
        for key in keys {
            let Some(ds) = streams.get_mut(&key) else {
                continue;
            };
            let seq = ds.next_seq;
            ds.next_seq += 1;
            let mut frame_env = base_envelope(
                &device.to_string(),
                envelope::Payload::EventBatch(EventBatch {
                    stream_id: ds.stream_id.clone(),
                    events: vec![DomainEvent {
                        emitted_at: now_ts(),
                        event: Some(event.clone()),
                    }],
                }),
            );
            frame_env.stream_id = ds.stream_id.clone();
            frame_env.stream_epoch = ds.epoch;
            frame_env.sequence = seq;
            let frame = Frame::new(frame_env, FrameKind::Critical, None);
            let _ = ds.buffer.insert(frame.clone());
            let mut slow: Vec<uuid::Uuid> = Vec::new();
            for (conn_id, sub) in ds.subscribers.iter_mut() {
                if sub.awaiting_snapshot {
                    continue;
                }
                let Some(bc) = browsers.get(conn_id) else {
                    continue;
                };
                if let Err(PushError::SlowConsumer) = bc.outbox.push_frame(frame.clone()) {
                    slow.push(*conn_id);
                }
            }
            for conn_id in slow {
                if let Some(bc) = browsers.get(&conn_id) {
                    bc.outbox
                        .push_direct(encode_direct(&resync_required_env(&ds.stream_id)));
                    bc.outbox.close("RESYNC_REQUIRED");
                }
                ds.subscribers.remove(&conn_id);
                if let Some(set) = browser_streams.get_mut(&conn_id) {
                    set.remove(&key);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // 查询与命令
    // -----------------------------------------------------------------------

    fn register_query(&self, correlation: &str) -> oneshot::Receiver<QueryResponse> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap()
            .queries
            .insert(correlation.to_string(), tx);
        rx
    }

    /// 在线查询(§27.3):HTTP handler → 在线 Bridge;离线返回 DEVICE_OFFLINE。
    pub async fn device_query(
        &self,
        device: uuid::Uuid,
        request: QueryRequest,
    ) -> Result<QueryResponse, StableErrorCode> {
        if !self.is_online(device) {
            return Err(StableErrorCode::DeviceOffline);
        }
        let correlation = new_request_id();
        let rx = self.register_query(&correlation);
        let mut env = base_envelope(
            &device.to_string(),
            envelope::Payload::QueryRequest(request),
        );
        env.correlation_id = correlation.clone();
        if !self.send_to_bridge(device, env) {
            self.inner.lock().unwrap().queries.remove(&correlation);
            return Err(StableErrorCode::DeviceOffline);
        }
        match tokio::time::timeout(limits::UPSTREAM_QUERY_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            _ => {
                self.inner.lock().unwrap().queries.remove(&correlation);
                Err(StableErrorCode::CodexUnavailable)
            }
        }
    }

    fn register_command(
        &self,
        request_id: &str,
        conn_id: uuid::Uuid,
        device: uuid::Uuid,
        operation: &str,
    ) {
        self.inner.lock().unwrap().commands.insert(
            request_id.to_string(),
            PendingCommand {
                conn_id,
                device,
                operation: operation.to_string(),
                started_at: std::time::Instant::now(),
            },
        );
    }

    fn take_command(&self, request_id: &str) -> Option<PendingCommand> {
        self.inner.lock().unwrap().commands.remove(request_id)
    }

    /// 全局内存重算(监控周期调用);超全局上限时对最大的流强制 resync(§17.6);
    /// 顺带回收无订阅者的流(§18:操作时顺带有界清理)。
    pub fn sweep_global_memory(&self) {
        let mut g = self.inner.lock().unwrap();
        // 回收无订阅者的流。
        let empty: Vec<DKey> = g
            .streams
            .iter()
            .filter(|(_, ds)| ds.subscribers.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        for key in empty {
            if let Some(ds) = g.streams.remove(&key) {
                for uid in ds.upstreams.keys() {
                    g.upstream_index.remove(uid);
                }
            }
        }
        let mut total: u64 = 0;
        let mut worst: Option<(DKey, usize)> = None;
        for (key, ds) in g.streams.iter() {
            total += ds.buffer.bytes as u64;
            if worst
                .as_ref()
                .map(|(_, b)| ds.buffer.bytes > *b)
                .unwrap_or(true)
            {
                worst = Some((key.clone(), ds.buffer.bytes));
            }
        }
        for bc in g.browsers.values() {
            total += (bc.outbox.len() * 1024) as u64; // 队列字节按帧均 1 KiB 估算
        }
        self.global_buffer_bytes.store(total, Ordering::Relaxed);
        if total > limits::GLOBAL_BUFFER_MAX_BYTES as u64 {
            if let Some((key, _)) = worst {
                if let Some(ds) = g.streams.get_mut(&key) {
                    ds.buffer.clear();
                    ds.snapshots.clear();
                    let stream_id = ds.stream_id.clone();
                    for conn_id in ds.subscribers.keys().cloned().collect::<Vec<_>>() {
                        if let Some(bc) = g.browsers.get(&conn_id) {
                            bc.outbox
                                .push_direct(encode_direct(&resync_required_env(&stream_id)));
                        }
                    }
                }
                tracing::debug!(target: "relay::realtime", "global memory pressure resync");
            }
        }
    }

    // -----------------------------------------------------------------------
    // 消息处理入口(由连接任务调用;DB 操作不持锁)
    // -----------------------------------------------------------------------

    /// Bridge 上行 Envelope 处理。
    pub async fn handle_bridge_envelope(&self, app: &AppState, device: uuid::Uuid, env: Envelope) {
        use envelope::Payload as P;
        let upstream_id = env.stream_id.clone();
        match env.payload.as_ref() {
            Some(P::Heartbeat(_)) => {
                let ack = base_envelope(
                    &device.to_string(),
                    envelope::Payload::HeartbeatAck(HeartbeatAck {}),
                );
                self.send_to_bridge(device, ack);
            }
            Some(P::Subscribed(sub)) => {
                let mut g = self.inner.lock().unwrap();
                Self::handle_upstream_subscribed(&mut g, &upstream_id, &env, sub);
            }
            Some(P::RuntimeSnapshot(_)) => {
                let mut g = self.inner.lock().unwrap();
                Self::handle_upstream_snapshot(&mut g, &upstream_id, &env);
            }
            Some(P::SessionSummaryBatch(batch)) => {
                // 先落库(不持锁;摘要持久化,§11),再扇出。
                if let Err(e) =
                    crate::sessions::store::upsert_summary_batch(&app.db, device, batch).await
                {
                    tracing::warn!(error = %e, "summary upsert failed");
                }
                let triggers = {
                    let mut g = self.inner.lock().unwrap();
                    if batch.snapshot {
                        Self::handle_upstream_snapshot(&mut g, &upstream_id, &env);
                        Vec::new()
                    } else {
                        let events: Vec<domain_event::Event> = batch
                            .summaries
                            .iter()
                            .map(|summary| {
                                domain_event::Event::SessionSummaryChanged(summary.clone())
                            })
                            .collect();
                        Self::fan_out_events(&mut g, &upstream_id, &env, events)
                    }
                };
                crate::push::notify(app, &triggers).await;
            }
            Some(P::EventBatch(_)) => {
                let events: Vec<domain_event::Event> = match env.payload.as_ref() {
                    Some(envelope::Payload::EventBatch(batch)) => batch
                        .events
                        .iter()
                        .filter_map(|e| e.event.clone())
                        .collect(),
                    _ => return,
                };
                let triggers = {
                    let mut g = self.inner.lock().unwrap();
                    Self::fan_out_events(&mut g, &upstream_id, &env, events)
                };
                crate::push::notify(app, &triggers).await;
            }
            Some(P::CommandAccepted(accepted)) => {
                self.on_command_accepted(app, device, accepted).await;
            }
            Some(P::CommandResult(result)) => {
                self.on_command_result(app, device, result).await;
            }
            Some(P::QueryResponse(resp)) => {
                let correlation = resp.request_id.clone();
                let resp = resp.clone();
                let sender = self.inner.lock().unwrap().queries.remove(&correlation);
                if let Some(tx) = sender {
                    let _ = tx.send(resp);
                }
            }
            Some(P::CapabilitySnapshot(cap)) => {
                let cap = cap.clone();
                let _ =
                    crate::devices::update_device_capability_summary(&app.db, device, &cap).await;
            }
            Some(P::DevicePresence(_)) => { /* presence 由连接层维护 */ }
            Some(P::ResyncRequired(_)) => {
                // Bridge 要求重同步:重新发起上游订阅。
                let mut g = self.inner.lock().unwrap();
                let Some(key) = g.upstream_index.get(&upstream_id).cloned() else {
                    return;
                };
                let env = {
                    let Some(ds) = g.streams.get_mut(&key) else {
                        return;
                    };
                    let Some(binding) = ds.upstreams.get_mut(&upstream_id) else {
                        return;
                    };
                    binding.snapshot_received = false;
                    Self::upstream_subscribe_env(binding.device, &key.1)
                };
                Self::send_to_bridge_locked(&g, device, env);
            }
            Some(P::TransferOffer(_)) => {
                // Bridge 不会发送 TransferOffer;忽略(文件面协调由 Relay 发起)。
                tracing::debug!(target: "relay::realtime", "unexpected TransferOffer from bridge");
            }
            Some(P::TransferReady(ready)) => {
                // Bridge 拒绝 offer(如 FILE_HANDLE_INVALID):取消 transfer 并透传拒绝码。
                crate::transfers::on_bridge_ready(app, device, ready).await;
            }
            Some(P::TransferResult(result)) => {
                // upload 终态(含 upload handle 结果)路由给等待的 browser。
                crate::transfers::on_bridge_result(app, device, result).await;
            }
            Some(P::ProtocolError(e)) => {
                let code = StableErrorCode::try_from(e.error_code)
                    .unwrap_or(StableErrorCode::InternalError);
                tracing::warn!(target: "relay::realtime", code = stable_code_ref_name(&code), "bridge protocol error");
            }
            Some(
                P::Ack(_)
                | P::ResyncRequest(_)
                | P::Subscribe(_)
                | P::Unsubscribe(_)
                | P::ClientHello(_)
                | P::ServerHello(_)
                | P::HeartbeatAck(_)
                | P::QueryRequest(_)
                | P::CommandRequest(_),
            ) => {
                tracing::debug!(target: "relay::realtime", "unexpected payload from bridge");
            }
            None => {}
        }
    }

    async fn on_command_accepted(
        &self,
        app: &AppState,
        device: uuid::Uuid,
        accepted: &CommandAccepted,
    ) {
        let status = Receipt::try_from(accepted.status).unwrap_or(Receipt::Unspecified);
        {
            let g = self.inner.lock().unwrap();
            if let Some(p) = g.commands.get(&accepted.request_id) {
                if let Some(bc) = g.browsers.get(&p.conn_id) {
                    let env = base_envelope(
                        &device.to_string(),
                        envelope::Payload::CommandAccepted(accepted.clone()),
                    );
                    bc.outbox.push_direct(encode_direct(&env));
                }
            }
        }
        if status == Receipt::ReceiptAcceptedByBridge {
            let _ = crate::sessions::store::update_receipt(
                &app.db,
                &accepted.request_id,
                "ACCEPTED_BY_BRIDGE",
                "",
            )
            .await;
        }
    }

    async fn on_command_result(&self, app: &AppState, device: uuid::Uuid, result: &CommandResult) {
        let audit_info: Option<(uuid::Uuid, u64)> = {
            let mut g = self.inner.lock().unwrap();
            let pending = g.commands.get(&result.request_id);
            if let Some(p) = pending {
                let elapsed = p.started_at.elapsed().as_millis() as u64;
                let conn_id = p.conn_id;
                if let Some(bc) = g.browsers.get(&conn_id) {
                    let env = base_envelope(
                        &device.to_string(),
                        envelope::Payload::CommandResult(result.clone()),
                    );
                    bc.outbox.push_direct(encode_direct(&env));
                }
                g.commands.remove(&result.request_id);
                Some((conn_id, elapsed))
            } else {
                None
            }
        };
        let status = Receipt::try_from(result.status).unwrap_or(Receipt::Unspecified);
        let status_name = match status {
            Receipt::ReceiptCompleted => "COMPLETED",
            Receipt::ReceiptRejected => "REJECTED",
            Receipt::ReceiptOutcomeUnknown => "OUTCOME_UNKNOWN",
            Receipt::ReceiptDispatchedToCodex => "DISPATCHED_TO_CODEX",
            Receipt::ReceiptAcceptedByBridge => "ACCEPTED_BY_BRIDGE",
            _ => "RECEIVED",
        };
        let error = stable_code_ref_name(
            &StableErrorCode::try_from(result.error_code).unwrap_or(StableErrorCode::Unspecified),
        );
        let _ =
            crate::sessions::store::update_receipt(&app.db, &result.request_id, status_name, error)
                .await;
        if let Some((conn_id, elapsed)) = audit_info {
            if let Some(owner) = self.browser_identity(conn_id).map(|i| i.owner_id) {
                // 审计调用点(§18.7;落库由 relay-data 实现)。
                crate::state::record_audit(
                    &app.db,
                    crate::state::AuditEvent {
                        owner_id: owner,
                        device_id: Some(device),
                        session_id: None,
                        request_id: Some(result.request_id.clone()),
                        operation: "command_result".into(),
                        result: format!("{status_name}:{error}"),
                        latency_ms: Some(elapsed as i64),
                    },
                )
                .await;
            }
        }
    }

    /// Browser 上行 Envelope 处理。
    pub async fn handle_browser_envelope(
        &self,
        app: &AppState,
        conn_id: uuid::Uuid,
        env: Envelope,
    ) {
        use envelope::Payload as P;
        match env.payload.as_ref() {
            Some(P::Subscribe(subscribe)) => {
                self.on_browser_subscribe(app, conn_id, subscribe).await;
            }
            Some(P::Unsubscribe(unsub)) => {
                self.browser_unsubscribe(conn_id, &unsub.stream_id);
            }
            Some(P::Ack(ack)) => {
                self.browser_ack(conn_id, &ack.stream_id, ack.sequence);
            }
            Some(P::ResyncRequest(ResyncRequest { stream_id })) => {
                self.browser_resync(conn_id, stream_id);
            }
            Some(P::CommandRequest(request)) => {
                self.on_browser_command(app, conn_id, request).await;
            }
            Some(P::Heartbeat(_)) => {
                if let Some(outbox) = self.browser_outbox(conn_id) {
                    let ack = base_envelope("", envelope::Payload::HeartbeatAck(HeartbeatAck {}));
                    outbox.push_direct(encode_direct(&ack));
                }
            }
            Some(P::ClientHello(_) | P::ServerHello(_)) => { /* 握手阶段已处理 */ }
            _ => {
                if let Some(outbox) = self.browser_outbox(conn_id) {
                    Self::send_protocol_error(
                        &outbox,
                        StableErrorCode::InternalError,
                        "",
                        "",
                        "不支持的消息类型",
                    );
                }
            }
        }
    }

    async fn on_browser_subscribe(
        &self,
        app: &AppState,
        conn_id: uuid::Uuid,
        subscribe: &Subscribe,
    ) {
        let Some(ident) = self.browser_identity(conn_id) else {
            return;
        };
        let Some(target) = subscribe.target.as_ref() else {
            return;
        };
        match target {
            subscribe::Target::List(_) => {
                let devices = self.online_devices_of_owner(ident.owner_id);
                let needs: Vec<(uuid::Uuid, TargetTag)> =
                    devices.into_iter().map(|d| (d, TargetTag::List)).collect();
                self.browser_subscribe(conn_id, (ident.owner_id, TargetTag::List), needs);
            }
            subscribe::Target::Session(key) => {
                let reply_error = |code: StableErrorCode, msg: &str| {
                    if let Some(outbox) = self.browser_outbox(conn_id) {
                        Self::send_protocol_error(&outbox, code, "", "", msg);
                    }
                };
                let Ok(device) = uuid::Uuid::parse_str(&key.device_id) else {
                    reply_error(StableErrorCode::SessionNotFound, "会话不存在");
                    return;
                };
                // 归属校验:设备存在、未撤销且属于该 owner(离线设备也允许先校验归属)。
                let owned = sqlx::query(
                    "SELECT 1 FROM devices WHERE id = $1 AND owner_id = $2 AND revoked_at IS NULL",
                )
                .bind(device)
                .bind(ident.owner_id)
                .fetch_optional(&app.db)
                .await
                .ok()
                .flatten()
                .is_some();
                if !owned {
                    reply_error(StableErrorCode::SessionNotFound, "会话不存在");
                    return;
                }
                if !self.is_online(device) {
                    reply_error(StableErrorCode::DeviceOffline, "设备离线");
                    return;
                }
                let tag = TargetTag::Session {
                    device,
                    native_session_id: key.native_session_id.clone(),
                };
                self.browser_subscribe(conn_id, (ident.owner_id, tag.clone()), vec![(device, tag)]);
            }
        }
    }

    async fn on_browser_command(
        &self,
        app: &AppState,
        conn_id: uuid::Uuid,
        request: &CommandRequest,
    ) {
        let Some(ident) = self.browser_identity(conn_id) else {
            return;
        };
        let reply = |result: CommandResult| {
            if let Some(outbox) = self.browser_outbox(conn_id) {
                outbox.push_direct(encode_direct(&base_envelope(
                    "",
                    envelope::Payload::CommandResult(result),
                )));
            }
        };
        // 1. 停机:停止接受新命令(§26.4);不落回执,便于重启后重试。
        if self.is_shutdown() {
            reply(command_rejected(
                &request.request_id,
                StableErrorCode::InternalError,
            ));
            return;
        }
        // 2. dev-toolbox 不可达宽限期内拒绝写命令(§20.5)。
        if !self.writes_allowed() {
            let mut result = command_rejected(&request.request_id, StableErrorCode::InternalError);
            result
                .details
                .insert("reason".to_string(), "AUTH_BACKEND_UNAVAILABLE".to_string());
            reply(result);
            return;
        }
        // 3. 本地过期判定(§20.5:达到原 expires_at 即关)。
        if ident.expires_at <= chrono::Utc::now() {
            self.close_browser(conn_id, "AUTH_EXPIRED");
            return;
        }

        // 4. 解析目标设备与会话。
        let (device, session_id): (uuid::Uuid, Option<uuid::Uuid>) =
            match request.session_key.as_ref() {
                Some(session_key) => {
                    let Ok(device) = uuid::Uuid::parse_str(&session_key.device_id) else {
                        reply(command_rejected(
                            &request.request_id,
                            StableErrorCode::SessionNotFound,
                        ));
                        return;
                    };
                    let owned = sqlx::query(
                    "SELECT 1 FROM devices WHERE id = $1 AND owner_id = $2 AND revoked_at IS NULL",
                )
                .bind(device)
                .bind(ident.owner_id)
                .fetch_optional(&app.db)
                .await
                .ok()
                .flatten()
                .is_some();
                    if !owned {
                        reply(command_rejected(
                            &request.request_id,
                            StableErrorCode::SessionNotFound,
                        ));
                        return;
                    }
                    if !self.is_online(device) {
                        // 设备离线 → 拒绝写命令 DEVICE_OFFLINE。
                        reply(command_rejected(
                            &request.request_id,
                            StableErrorCode::DeviceOffline,
                        ));
                        return;
                    }
                    let session = crate::sessions::store::find_session(
                        &app.db,
                        device,
                        &crate::realtime::agent_kind_name(session_key.agent_kind),
                        &session_key.native_session_id,
                    )
                    .await
                    .ok()
                    .flatten();
                    match session {
                        Some(s) => (device, Some(s.id)),
                        None => {
                            reply(command_rejected(
                                &request.request_id,
                                StableErrorCode::SessionNotFound,
                            ));
                            return;
                        }
                    }
                }
                None => {
                    // 无会话目标(如 CREATE_TASK):M1 单设备,取该 owner 唯一在线设备。
                    let devices = self.online_devices_of_owner(ident.owner_id);
                    match devices.len() {
                        1 => (devices[0], None),
                        0 => {
                            reply(command_rejected(
                                &request.request_id,
                                StableErrorCode::DeviceOffline,
                            ));
                            return;
                        }
                        _ => {
                            reply(command_rejected(
                                &request.request_id,
                                StableErrorCode::CapabilityUnsupported,
                            ));
                            return;
                        }
                    }
                }
            };

        // 5. 重试去重:相同 request_id 返回已有回执,不重发(§15.2)。
        let operation = agent_console_protocol::v1::Operation::try_from(request.operation)
            .map(|o| operation_name(&o))
            .unwrap_or_else(|_| "OPERATION_UNSPECIFIED".to_string());
        // 不信任 Browser 自报的 payload_digest。Relay 以收到的 Protobuf
        // CommandRequest（清空 request_id/自报摘要后）计算稳定摘要，用于在
        // 请求到达 Bridge 前识别同 ID 不同内容。
        let payload_digest = canonical_browser_command_digest(request);
        let inserted = crate::sessions::store::insert_receipt(
            &app.db,
            &request.request_id,
            ident.owner_id,
            session_id,
            &operation,
            &payload_digest,
        )
        .await;
        match inserted {
            Ok(false) => {
                // 已存在:返回当前回执状态。
                if let Ok(Some(existing)) =
                    crate::sessions::store::get_receipt(&app.db, &request.request_id).await
                {
                    if existing.owner_id != ident.owner_id
                        || existing.session_id != session_id
                        || existing.operation != operation
                        || existing.payload_digest != payload_digest
                    {
                        reply(command_rejected(
                            &request.request_id,
                            StableErrorCode::DuplicateRequestMismatch,
                        ));
                        return;
                    }
                    let mut result = command_rejected(
                        &request.request_id,
                        crate::state::stable_code_from_name(&existing.error_code),
                    );
                    result.status = crate::sessions::store::receipt_status_code(&existing.status)
                        .map(|s| s as i32)
                        .unwrap_or(0);
                    reply(result);
                }
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "receipt insert failed");
                reply(command_rejected(
                    &request.request_id,
                    StableErrorCode::InternalError,
                ));
                return;
            }
            Ok(true) => {}
        }

        // 6. 注册 pending 并转发 Bridge;ACCEPTED_BY_BRIDGE 才回执(任务约定)。
        self.register_command(&request.request_id, conn_id, device, &operation);
        let mut fwd = base_envelope(
            &device.to_string(),
            envelope::Payload::CommandRequest(request.clone()),
        );
        fwd.correlation_id = request.request_id.clone();
        if !self.send_to_bridge(device, fwd) {
            // 转发失败(连接刚断):REJECTED DEVICE_OFFLINE。
            self.take_command(&request.request_id);
            let _ = crate::sessions::store::update_receipt(
                &app.db,
                &request.request_id,
                "REJECTED",
                "DEVICE_OFFLINE",
            )
            .await;
            reply(command_rejected(
                &request.request_id,
                StableErrorCode::DeviceOffline,
            ));
            return;
        }
        // 审计调用点(§18.7;落库由 relay-data 实现)。
        crate::state::record_audit(
            &app.db,
            crate::state::AuditEvent {
                owner_id: ident.owner_id,
                device_id: Some(device),
                session_id,
                request_id: Some(request.request_id.clone()),
                operation: "command_request".into(),
                result: "RECEIVED".into(),
                latency_ms: None,
            },
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// 辅助构造
// ---------------------------------------------------------------------------

fn command_rejected(request_id: &str, code: StableErrorCode) -> CommandResult {
    CommandResult {
        request_id: request_id.to_string(),
        status: Receipt::ReceiptRejected as i32,
        error_code: code as i32,
        details: Default::default(),
        duration_ms: None,
    }
}

fn canonical_browser_command_digest(request: &CommandRequest) -> String {
    use prost::Message;

    let mut normalized = request.clone();
    normalized.request_id.clear();
    normalized.payload_digest.clear();
    crate::state::sha256_hex(&normalized.encode_to_vec())
}

fn subscribe_target_list() -> subscribe::Target {
    subscribe::Target::List(SessionList {})
}

fn subscribe_target_session(device: uuid::Uuid, native_session_id: &str) -> subscribe::Target {
    subscribe::Target::Session(SessionKey {
        device_id: device.to_string(),
        agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
        native_session_id: native_session_id.to_string(),
        relay_session_uuid: String::new(),
    })
}

pub fn agent_kind_name(kind: i32) -> String {
    agent_console_protocol::v1::AgentKind::try_from(kind)
        .map(|k| k.as_str_name().to_string())
        .unwrap_or_else(|_| format!("AGENT_KIND_{kind}"))
}

pub fn operation_name(op: &agent_console_protocol::v1::Operation) -> String {
    op.as_str_name().to_string()
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}
