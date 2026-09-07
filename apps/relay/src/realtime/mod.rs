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
    /// 详情订阅:单会话流。agent_kind 为 proto 枚举数值(未知值原样保留,
    /// ZC-02:不同 Agent 的同 native id 会话不共享上游流)。
    Session {
        device: uuid::Uuid,
        agent_kind: i32,
        native_session_id: String,
    },
}

type DKey = (uuid::Uuid, TargetTag);

/// 下游 subscriber 状态。
struct SubscriberState {
    /// 尚未收到首个 snapshot:事件只入缓冲,不发往订阅者(§17.4 步骤 4/5)。
    awaiting_snapshot: bool,
    acked: u64,
    /// 该订阅者最近一次进入 awaiting 所对应 Subscribe 的 correlation_id:
    /// 快照 flush 时回显到 Subscribed,浏览器据此做请求级关联(§17.2),
    /// 不把未知响应猜成某个等待中的目标。
    subscribe_correlation: String,
}

struct UpstreamBinding {
    device: uuid::Uuid,
    epoch: u64,
    /// 已处理到的上游批 sequence(去重水位;0 表示未定)。
    last_seq: u64,
    /// 当前在途快照覆盖的上游水位(§17.4 步骤 3:Subscribed.base_sequence)。
    /// 快照到达时据此只清除确实被覆盖的缓冲事件,内容层不会回退。
    snapshot_covers: u64,
    snapshot_received: bool,
}

/// 流尚无任何快照期间暂存的上游事件批(§17.4 步骤 4):不分配下游序号、
/// 不扇出;首个快照定序后按上游批号依序 flush,保证重放/实时顺序都是
/// 快照先于其后事件。多设备列表流同理按"流级首快照"判定。
struct PendingUpstreamBatch {
    /// 来源上游流 id(flush 时按其 binding 的 covers 过滤)。
    upstream_id: String,
    /// 上游批级 sequence;0 表示上游未提供。
    upstream_seq: u64,
    sent_at: Option<prost_types::Timestamp>,
    device_id: String,
    events: Vec<domain_event::Event>,
}

struct DStream {
    #[allow(dead_code)] // 调试/未来路由使用;当前 stream_id 即唯一标识。
    key: DKey,
    stream_id: String,
    epoch: u64,
    next_seq: u64,
    /// 各上游设备最新缓存的 snapshot 帧(会话流 1 条;列表流每设备一条)。
    snapshots: VecDeque<Arc<Frame>>,
    /// 流尚无快照期间暂存的上游事件批(§17.4 步骤 4;事件数/字节双上限,
    /// 压力下只挤出非第 1 类,见 `push_pending`)。
    pending_batches: VecDeque<PendingUpstreamBatch>,
    /// 暂存事件的序列化字节与条数(事件 encoded_len 之和;计入单流预算与
    /// 全局内存统计,§17.6)。
    pending_bytes: usize,
    pending_events: usize,
    buffer: StreamBuffer,
    subscribers: HashMap<uuid::Uuid, SubscriberState>,
    /// 上游绑定:upstream_stream_id → binding。
    upstreams: HashMap<String, UpstreamBinding>,
}

/// 单流快照缓存总量硬上限:设备换 UUID 重桥会累积旧设备快照,超过上限时
/// 淘汰最旧快照;淘汰产生空洞时 full_replay 落空,由 anchored_replay 兜底。
const SNAPSHOT_CACHE_MAX: usize = 16;

impl DStream {
    fn new(key: DKey) -> Self {
        Self {
            stream_id: format!("s-{}", uuid::Uuid::new_v4()),
            key,
            epoch: 1,
            next_seq: 1,
            snapshots: VecDeque::new(),
            pending_batches: VecDeque::new(),
            pending_bytes: 0,
            pending_events: 0,
            buffer: StreamBuffer::new(
                limits::BUFFER_MAX_EVENTS,
                limits::BUFFER_MAX_BYTES,
                limits::BUFFER_RETENTION,
            ),
            subscribers: HashMap::new(),
            upstreams: HashMap::new(),
        }
    }

    /// 暂存一批"流尚无快照期间"到达的上游事件(§17.4 步骤 4)。字节计入
    /// 单流缓冲预算(`BUFFER_MAX_BYTES`),条数计入 `SNAPSHOT_PENDING_MAX_EVENTS`,
    /// 两者都计入全局内存统计。压力策略比 StreamBuffer 更保守:暂存窗口没有
    /// 序号缺口检测可以触发恢复,快照水位又固定在订阅时——水位之后第 1 类
    /// (问题/审批/生命周期)与第 2 类(最终回复等 item 内容,需触发恢复才能
    /// 经历史重读)都丢了就无任何自动路径补回。因此只允许挤出第 3 类可合并
    /// 事件(高频输出增量,LIVE_PREVIEW 最佳努力,§13.1);仅剩第 1/第 2 类
    /// 仍超限时返回 true,由调用方复用显式恢复流程(向订阅者发 ResyncRequired
    /// 并强制重取快照),不得丢事件后继续宣称同步正常。被挤空的批次立即
    /// 整体移除,空壳不残留、不占内存。
    fn push_pending(&mut self, batch: PendingUpstreamBatch) -> bool {
        self.pending_events += batch.events.len();
        self.pending_bytes += batch.events.iter().map(|e| e.encoded_len()).sum::<usize>();
        self.pending_batches.push_back(batch);
        while self.pending_events > limits::SNAPSHOT_PENDING_MAX_EVENTS
            || self.pending_bytes > limits::BUFFER_MAX_BYTES
        {
            let mut evicted_bytes = 0usize;
            let mut evicted_batch: Option<usize> = None;
            let mut evicted = false;
            'drop: for (index, batch) in self.pending_batches.iter_mut().enumerate() {
                let Some(pos) = batch
                    .events
                    .iter()
                    .position(|e| classify(e).0 == FrameKind::Mergeable)
                else {
                    continue;
                };
                let event = batch.events.remove(pos);
                evicted_bytes = event.encoded_len();
                if batch.events.is_empty() {
                    evicted_batch = Some(index);
                }
                evicted = true;
                break 'drop;
            }
            if !evicted {
                return true;
            }
            self.pending_events -= 1;
            self.pending_bytes = self.pending_bytes.saturating_sub(evicted_bytes);
            if let Some(index) = evicted_batch {
                self.pending_batches.remove(index);
            }
        }
        false
    }

    /// 清空暂存并归零计数(快照 flush/epoch 变化/内存压力等重置路径共用)。
    fn clear_pending(&mut self) {
        self.pending_batches.clear();
        self.pending_bytes = 0;
        self.pending_events = 0;
    }

    /// 缓存该设备的最新快照(替换同设备旧快照;列表聚合按设备维护,一台设备
    /// 的新快照不充当其他设备事件的覆盖锚)。总量超过 `SNAPSHOT_CACHE_MAX`
    /// 时淘汰最旧快照,缓存有界(§17.6)。
    fn cache_snapshot(&mut self, frame: Arc<Frame>) {
        self.snapshots
            .retain(|s| s.env.device_id != frame.env.device_id);
        self.snapshots.push_back(frame);
        while self.snapshots.len() > SNAPSHOT_CACHE_MAX {
            self.snapshots.pop_front();
        }
    }

    fn latest_snapshot_seq(&self) -> Option<u64> {
        self.snapshots.back().map(|f| f.env.sequence)
    }

    /// 存活帧全集(各设备最新快照 + 缓冲事件帧),按下游序号升序。
    fn surviving_frames(&self) -> Vec<Arc<Frame>> {
        let mut frames: Vec<Arc<Frame>> = self.snapshots.iter().cloned().collect();
        frames.extend(self.buffer.items.iter().cloned());
        frames.sort_by_key(|f| f.env.sequence);
        frames
    }

    /// 重放窗口首帧的 Subscribed.base(§17.4 步骤 5):窗口以快照帧开头时,
    /// 快照即已应用水位,base = 快照帧序号(快照占据 base,其后事件从
    /// base+1 连续);以事件帧开头(如列表流他设备未覆盖帧在前)时,
    /// base = 首帧序号-1,首帧事件从 base+1 连续。
    fn replay_base(&self, first: &Arc<Frame>) -> u64 {
        let lo = first.env.sequence;
        if self.snapshots.iter().any(|s| Arc::ptr_eq(s, first)) {
            lo
        } else {
            lo.saturating_sub(1)
        }
    }

    /// 完整恢复重放:存活帧序号必须连续覆盖 `lo..next_seq-1`(lo 为首帧
    /// 序号),返回 `(base, frames)`,base 语义见 `replay_base`。覆盖删除、
    /// 合并或淘汰产生过空洞时返回 None,调用方改用锚定重放,不能把不连续
    /// 窗口交给客户端。
    fn full_replay(&self) -> Option<(u64, Vec<Arc<Frame>>)> {
        let frames = self.surviving_frames();
        let first = frames.first()?;
        let lo = first.env.sequence;
        if frames.len() as u64 != self.next_seq.checked_sub(lo)? {
            return None;
        }
        for (i, f) in frames.iter().enumerate() {
            if f.env.sequence != lo + i as u64 {
                return None;
            }
        }
        let base = self.replay_base(first);
        Some((base, frames))
    }

    /// 锚定最新快照的保底重放:frames = 快照 + 缓冲中位于快照之后且仍连续
    /// 的帧。缓冲段不连续(合并/淘汰只作用于可由快照恢复的类别)时整段
    /// 丢弃,恢复者从快照重建。恒可用,保证恢复路径始终有序收尾。
    /// base 由窗口首帧推导(`replay_base`;缓冲尾帧序号恒大于各快照,
    /// 窗口非空时必以快照帧开头)。
    fn anchored_replay(&self) -> (u64, Vec<Arc<Frame>>) {
        let anchor = self.latest_snapshot_seq().unwrap_or(0);
        let mut frames: Vec<Arc<Frame>> = self.snapshots.iter().cloned().collect();
        let tail: Vec<Arc<Frame>> = self
            .buffer
            .items
            .iter()
            .filter(|f| f.env.sequence > anchor)
            .cloned()
            .collect();
        let expected_tail_len = self.next_seq.checked_sub(anchor + 1).unwrap_or(0);
        let contiguous = tail.len() as u64 == expected_tail_len
            && tail
                .iter()
                .enumerate()
                .all(|(i, f)| f.env.sequence == anchor + 1 + i as u64);
        if contiguous {
            frames.extend(tail);
        }
        frames.sort_by_key(|f| f.env.sequence);
        let base = frames.first().map(|f| self.replay_base(f)).unwrap_or(0);
        (base, frames)
    }

    /// 该流的全部上游绑定重取快照;清空缓存并让订阅者回到 awaiting 状态。
    /// `force=false`:已有 fresh snapshot 在途的绑定不重复订阅,避免 epoch 抖动。
    /// `force=true`:暂存压力恢复用——在途快照的水位固定在订阅时,无法覆盖
    /// 已超出暂存预算的关键事件,必须重订阅取得更高水位的新快照。
    fn refresh_snapshot_envs(&mut self, force: bool) -> Vec<(uuid::Uuid, Envelope)> {
        self.snapshots.clear();
        self.clear_pending();
        self.buffer.clear();
        for subscriber in self.subscribers.values_mut() {
            subscriber.awaiting_snapshot = true;
        }
        let tag = self.key.1.clone();
        self.upstreams
            .values_mut()
            .filter_map(|binding| {
                if !force && !binding.snapshot_received {
                    // 同一流已有 fresh snapshot 在途:只等待,不再发 Subscribe。
                    return None;
                }
                binding.epoch = 0;
                binding.last_seq = 0;
                binding.snapshot_covers = 0;
                binding.snapshot_received = false;
                Some((
                    binding.device,
                    Hub::upstream_subscribe_env(binding.device, &tag),
                ))
            })
            .collect()
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
            agent_kind,
            native_session_id,
        } => {
            // agent_kind 计入上游流键:同机双 Agent 同 native id 不串流(ZC-02)。
            format!("u-{device}-sess-{agent_kind}-{native_session_id}")
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

fn subscribed_env(
    stream_id: &str,
    epoch: u64,
    base_sequence: u64,
    correlation: &str,
) -> Envelope {
    let mut env = base_envelope(
        "",
        envelope::Payload::Subscribed(Subscribed {
            stream_id: stream_id.to_string(),
            stream_epoch: epoch,
            base_sequence,
        }),
    );
    // 回显发起订阅的 correlation_id:浏览器用它把响应关联到确定的订阅
    // 请求,消息载荷类型只能区分列表/详情,无法确认具体任务。
    env.correlation_id = correlation.to_string();
    env
}

/// 慢 consumer / 快照回放入队失败的统一收尾(§17.6):尽力送达 ResyncRequired、
/// 以稳定 reason 断开并摘除订阅。实时帧慢 consumer 与快照/回放 push 失败
/// 复用同一策略,绝不"let _ 之后宣称恢复完成"。
fn detach_failed_subscriber(
    streams: &mut HashMap<DKey, DStream>,
    browsers: &mut HashMap<uuid::Uuid, BrowserConn>,
    browser_streams: &mut HashMap<uuid::Uuid, HashSet<DKey>>,
    key: &DKey,
    conn_id: uuid::Uuid,
) {
    if let Some(ds) = streams.get_mut(key) {
        ds.subscribers.remove(&conn_id);
        if let Some(bc) = browsers.get(&conn_id) {
            let env = resync_required_env(&ds.stream_id);
            bc.outbox.push_direct(encode_direct(&env));
            bc.outbox.close("RESYNC_REQUIRED");
        }
    }
    if let Some(set) = browser_streams.get_mut(&conn_id) {
        set.remove(key);
    }
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
                device,
                agent_kind,
                native_session_id,
            } => subscribe_target_session(*device, *agent_kind, native_session_id),
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
                            snapshot_covers: 0,
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
    /// `correlation`:本次 Subscribe 信封的 correlation_id,flush/重放的
    /// Subscribed 依此回显(§17.2 请求级关联)。
    pub fn browser_subscribe(
        &self,
        conn_id: uuid::Uuid,
        key: DKey,
        needs_binding: Vec<(uuid::Uuid, TargetTag)>,
        correlation: String,
    ) {
        let refresh_session = matches!(key.1, TargetTag::Session { .. });
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
                            snapshot_covers: 0,
                            snapshot_received: false,
                        },
                    );
                    upstream_index.insert(uid.clone(), key.clone());
                    sub_envs.push((device, Self::upstream_subscribe_env(device, &tag)));
                } else if refresh_session
                    && ds
                        .upstreams
                        .get(&uid)
                        .is_some_and(|binding| binding.snapshot_received)
                {
                    // 单会话快照包含完整当前状态；新详情订阅直接向 Bridge
                    // 取新快照，避免重放经过合并后的旧事件窗口产生 sequence gap。
                    if let Some(binding) = ds.upstreams.get_mut(&uid) {
                        binding.epoch = 0;
                        binding.last_seq = 0;
                        binding.snapshot_covers = 0;
                        binding.snapshot_received = false;
                    }
                    ds.snapshots.clear();
                    ds.clear_pending();
                    ds.buffer.clear();
                    for subscriber in ds.subscribers.values_mut() {
                        subscriber.awaiting_snapshot = true;
                    }
                    sub_envs.push((device, Self::upstream_subscribe_env(device, &tag)));
                }
            }
        }
        for (device, env) in sub_envs {
            Self::send_to_bridge_locked(&g1, device, env);
        }
        drop(g1);
        // 挂接 subscriber(§17.4 步骤 2):有 snapshot 则立刻按顺序回放。
        {
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
                    subscribe_correlation: correlation.clone(),
                });
                sub.awaiting_snapshot = !has_snapshot;
                // 记录本次订阅的 correlation:flush 到达的 Subscribed 依此回显。
                sub.subscribe_correlation = correlation.clone();
            }
            browser_streams.entry(conn_id).or_default().insert(key.clone());

            if has_snapshot {
                // 恢复重放:优先完整存活窗口;存在空洞(覆盖删除/合并/淘汰)
                // 时锚定最新快照。序号即全局序号,不为单个订阅者改写坐标;
                // 全部入队成功才转入活跃,失败复用重同步/断开收尾(R2-AC01)。
                let (base, frames) = match ds.full_replay() {
                    Some(replay) => replay,
                    None => ds.anchored_replay(),
                };
                let (epoch, stream_id) = (ds.epoch, ds.stream_id.clone());
                let subscribed =
                    encode_direct(&subscribed_env(&stream_id, epoch, base, &correlation));
                let mut ok = outbox.push_direct(subscribed);
                if ok {
                    for frame in &frames {
                        if outbox.push_frame(frame.clone()).is_err() {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    if let Some(sub) = ds.subscribers.get_mut(&conn_id) {
                        sub.awaiting_snapshot = false;
                    }
                } else {
                    detach_failed_subscriber(streams, browsers, browser_streams, &key, conn_id);
                }
            }
            // 尚无 snapshot 时不预发 Subscribed:到达后由 flush 统一按
            // Subscribed → snapshot → 缓冲事件发送(§17.4 顺序唯一)。
        }
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

    /// Browser ResyncRequest(§17.5):列表流按全局序号重放存活帧(优先完整
    /// 窗口,空洞时锚定最新快照),不为单个订阅者改写坐标;单会话详情始终
    /// 向 Bridge 重取完整 RuntimeSnapshot,避免合并窗口的序号空洞反复触发
    /// resync。回放入队失败复用重同步/断开收尾。
    pub fn browser_resync(&self, conn_id: uuid::Uuid, stream_id: &str, correlation: &str) {
        let mut g = self.inner.lock().unwrap();
        let Some(key) = g
            .streams
            .iter()
            .find(|(_, ds)| ds.stream_id == stream_id)
            .map(|(key, _)| key.clone())
        else {
            return;
        };

        let refreshes: Vec<(uuid::Uuid, Envelope)> = {
            let HubInner {
                streams,
                browsers,
                browser_streams,
                ..
            } = &mut *g;
            let Some(ds) = streams.get_mut(&key) else {
                return;
            };
            if !ds.subscribers.contains_key(&conn_id) {
                return;
            }
            if matches!(key.1, TargetTag::Session { .. }) || ds.snapshots.is_empty() {
                // 详情流:权威快照重取(全部订阅者回到 awaiting)。
                // 列表流尚无快照(如内存压力清理后)同样重取上游权威快照。
                ds.refresh_snapshot_envs(false)
            } else {
                // 列表流恢复:优先完整存活窗口;空洞时锚定最新快照。序号即
                // 全局序号,不为单个订阅者改写坐标;全部入队成功才宣告恢复
                // 完成,失败复用重同步/断开收尾(R2-AC01)。
                let (base, frames) = match ds.full_replay() {
                    Some(replay) => replay,
                    None => ds.anchored_replay(),
                };
                let (epoch, stream_id) = (ds.epoch, ds.stream_id.clone());
                let Some(outbox) = browsers.get(&conn_id).map(|b| b.outbox.clone()) else {
                    return;
                };
                // resync 是订阅者对已知流的恢复,Subscribed 走已知流重绑路径;
                // 仍回显 ResyncRequest 的 correlation 保持请求级关联语义。
                let subscribed =
                    encode_direct(&subscribed_env(&stream_id, epoch, base, correlation));
                let mut ok = outbox.push_direct(subscribed);
                if ok {
                    for frame in &frames {
                        if outbox.push_frame(frame.clone()).is_err() {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    if let Some(sub) = ds.subscribers.get_mut(&conn_id) {
                        sub.awaiting_snapshot = false;
                    }
                } else {
                    detach_failed_subscriber(streams, browsers, browser_streams, &key, conn_id);
                }
                Vec::new()
            }
        };
        for (device, env) in refreshes {
            Self::send_to_bridge_locked(&g, device, env);
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
                // 快照覆盖水位(§17.4 步骤 3):快照内容锚定该 base_sequence,
                // 之后据此只清除确实被覆盖的缓冲事件。
                binding.snapshot_covers = sub.base_sequence;
                binding.snapshot_received = false;
                c
            };
            let other_upstreams = ds
                .upstreams
                .iter()
                .filter(|(uid, _)| uid.as_str() != upstream_id)
                .map(|(_, binding)| (binding.device, key.1.clone()))
                .collect::<Vec<_>>();
            (changed, other_upstreams)
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
            ds.clear_pending();
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
        // 当前 upstream 已用这个 Subscribed 宣告新 epoch，其新 snapshot 正在
        // 到达；只刷新聚合流中的其他 upstream。若再次订阅当前 upstream，
        // 每个响应都会再生成一个 epoch，形成自激重订阅循环。
        for (device, tag) in rebinding {
            let uid = upstream_stream_id(device, &tag);
            if let Some(ds) = g.streams.get_mut(&key) {
                if let Some(b) = ds.upstreams.get_mut(&uid) {
                    b.snapshot_received = false;
                    b.epoch = 0;
                    b.last_seq = 0;
                    b.snapshot_covers = 0;
                }
            }
            sub_envs.push((device, Self::upstream_subscribe_env(device, &tag)));
        }
        for (device, e) in sub_envs {
            Self::send_to_bridge_locked(g, device, e);
        }
    }

    /// 上游 snapshot(RuntimeSnapshot 或 snapshot=true 的 SessionSummaryBatch)。
    ///
    /// 覆盖语义(R2-AC01):快照锚定上游 `Subscribed.base_sequence` 水位,
    /// 只清除该设备确实被覆盖的缓冲事件——内容已在快照中,不再以新序号
    /// 重新应用,内容层绝不把新状态改回旧状态;其他设备与水位之后的事件
    /// 保持原下游序号,一台设备的新快照不覆盖、不重编号他人事件。快照帧
    /// 占用下一个全局序号(即活跃订阅者的下一期待帧),活跃坐标不被扰动;
    /// 恢复订阅者按存活帧重放(完整窗口,空洞时锚定最新快照),与活跃订阅
    /// 者交付同一序号含义。
    fn handle_upstream_snapshot(g: &mut HubInner, upstream_id: &str, env: &Envelope) -> Vec<crate::push::PushTrigger> {
        let Some(key) = g.upstream_index.get(upstream_id).cloned() else {
            return Vec::new();
        };
        let HubInner {
            streams,
            browsers,
            browser_streams,
            ..
        } = &mut *g;
        let Some(ds) = streams.get_mut(&key) else {
            return Vec::new();
        };
        let Some(binding) = ds.upstreams.get_mut(upstream_id) else {
            return Vec::new();
        };
        binding.snapshot_received = true;
        let covered = binding.snapshot_covers;
        let device = env.device_id.clone();
        // 暂存批按来源上游的 covers 过滤:本快照设备的批号 ≤ covers 已被
        // 覆盖,其他设备的暂存批按各自 binding 的水位判定。计数随 take 归零,
        // flush 重新入下游缓冲时按帧正常计量。
        let pending = std::mem::take(&mut ds.pending_batches);
        ds.pending_bytes = 0;
        ds.pending_events = 0;
        ds.buffer.retain_uncovered(&device, covered);
        // 下游 snapshot 帧:占用下一个全局序号,按设备替换缓存快照。
        let seq = ds.next_seq;
        ds.next_seq += 1;
        let mut frame_env = env.clone();
        frame_env.stream_id = ds.stream_id.clone();
        frame_env.stream_epoch = ds.epoch;
        frame_env.sequence = seq;
        let frame = Frame::new(frame_env, FrameKind::Critical, None);
        ds.cache_snapshot(frame.clone());
        // flush(§17.4 步骤 5):等待 snapshot 的订阅者按 Subscribed → 存活帧
        // 序列恢复;已活跃订阅者把 snapshot 当普通帧(恰为其下一期待序号)。
        let (stream_id, epoch) = (ds.stream_id.clone(), ds.epoch);
        let (base, replay_frames) = match ds.full_replay() {
            Some(replay) => replay,
            None => ds.anchored_replay(),
        };
        let conn_ids: Vec<uuid::Uuid> = ds.subscribers.keys().cloned().collect();
        let mut to_detach: Vec<uuid::Uuid> = Vec::new();
        for conn_id in conn_ids {
            let awaiting = ds.subscribers.get(&conn_id).map(|s| s.awaiting_snapshot);
            let Some(outbox) = browsers.get(&conn_id).map(|b| b.outbox.clone()) else {
                continue;
            };
            match awaiting {
                Some(true) => {
                    // 逐订阅者回显其 Subscribe 的 correlation_id(§17.2):
                    // 浏览器据此做请求级关联,不把响应猜成别的等待中目标。
                    let correlation = ds
                        .subscribers
                        .get(&conn_id)
                        .map(|s| s.subscribe_correlation.clone())
                        .unwrap_or_default();
                    let subscribed =
                        encode_direct(&subscribed_env(&stream_id, epoch, base, &correlation));
                    let mut ok = outbox.push_direct(subscribed);
                    if ok {
                        for f in &replay_frames {
                            if outbox.push_frame(f.clone()).is_err() {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        // 快照与回放全部成功入队后才恢复接收后续事件;
                        // 失败复用重同步/断开收尾,不宣称恢复完成。
                        if let Some(s) = ds.subscribers.get_mut(&conn_id) {
                            s.awaiting_snapshot = false;
                        }
                    } else {
                        to_detach.push(conn_id);
                    }
                }
                Some(false) => {
                    if outbox.push_frame(frame.clone()).is_err() {
                        to_detach.push(conn_id);
                    }
                }
                None => {}
            }
        }
        // 入队失败收尾(借用结束后统一执行):ResyncRequired + 稳定断开。
        for conn_id in to_detach {
            detach_failed_subscriber(streams, browsers, browser_streams, &key, conn_id);
        }
        drop((streams, browsers, browser_streams));

        // §17.4 步骤 5:快照已定序并交付后,流尚无快照期间暂存的上游事件
        // 按到达顺序依序进入下游——每批按其来源上游 binding 的 covers 过滤
        // (批号 ≤ covers 的已被对应快照覆盖,直接丢弃),保证订阅者看到的
        // 顺序是快照 → 其后事件;push 触发照常收集(§24)。
        let mut triggers = Vec::new();
        for batch in pending {
            let batch_covered = g
                .streams
                .get(&key)
                .and_then(|ds| ds.upstreams.get(&batch.upstream_id))
                .map(|binding| binding.snapshot_covers)
                .unwrap_or(0);
            if batch.upstream_seq != 0 && batch.upstream_seq <= batch_covered {
                continue;
            }
            let mut batch_env = base_envelope(
                &batch.device_id,
                envelope::Payload::EventBatch(EventBatch {
                    stream_id: String::new(),
                    events: Vec::new(),
                }),
            );
            batch_env.sequence = batch.upstream_seq;
            batch_env.sent_at = batch.sent_at;
            triggers.extend(Self::fan_out_events(g, &batch.upstream_id, &batch_env, batch.events));
        }
        triggers
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
            let HubInner {
                streams, browsers, bridges, ..
            } = &mut *g;
            let Some(ds) = streams.get_mut(&key) else {
                return push_triggers;
            };
            let should_stash = {
                let Some(binding) = ds.upstreams.get_mut(upstream_id) else {
                    return push_triggers;
                };
                // 上游批级 sequence(批级去重;0 表示未提供,跳过去重)。水位只在
                // 事件真正进入下游时推进;暂存批不推进,由 flush 时同一检查兜底。
                if env.sequence != 0 && env.sequence <= binding.last_seq {
                    tracing::debug!(target: "relay::realtime", "duplicate upstream batch sequence ignored");
                    return push_triggers;
                }
                !binding.snapshot_received && ds.snapshots.is_empty()
            };
            if events.is_empty() {
                return push_triggers;
            }
            if should_stash {
                // §17.4 步骤 4:流尚无任何快照(首订阅握手)期间,新事件只入
                // 暂存,不分配下游序号、不扇出——否则事件会落在首快照之前,
                // 违反"先发快照,再发 base_sequence 之后的事件"的握手顺序。
                // 首个快照到达后定序快照帧、再依序 flush(见
                // handle_upstream_snapshot)。流已有快照后(如多设备列表流的
                // 其他设备)事件按到达定序扇出,跨设备窗口由锚定重放维持。
                if ds.push_pending(PendingUpstreamBatch {
                    upstream_id: upstream_id.to_string(),
                    upstream_seq: env.sequence,
                    sent_at: env.sent_at,
                    device_id: env.device_id.clone(),
                    events,
                }) {
                    // §17.6:暂存仅剩第 1 类仍超限,不能丢关键事件后继续宣称
                    // 同步——复用显式恢复流程:向订阅者发 ResyncRequired,清空并
                    // 强制重取快照(在途快照水位固定在订阅时,覆盖不了这些
                    // 事件)。重订阅 env 必须在本分支内发出:分支结尾统一 return,
                    // events 已移动,锁外无法再访问。
                    let stream_id = ds.stream_id.clone();
                    for conn_id in ds.subscribers.keys().cloned().collect::<Vec<_>>() {
                        if let Some(bc) = browsers.get(&conn_id) {
                            bc.outbox
                                .push_direct(encode_direct(&resync_required_env(&stream_id)));
                        }
                    }
                    let refresh = ds.refresh_snapshot_envs(true);
                    for (device, env) in refresh {
                        if let Some(conn) = bridges.get(&device) {
                            let _ = conn.sink.try_send(encode_direct(&env));
                        }
                    }
                    tracing::debug!(target: "relay::realtime", "pending overflow forced snapshot refresh");
                }
                return push_triggers;
            }
            if let Some(binding) = ds.upstreams.get_mut(upstream_id) {
                if env.sequence != 0 {
                    binding.last_seq = env.sequence;
                }
            }
        }

        // push 触发映射(§24):turn 终态 / 等待问题 / 等待审批。
        // 列表流上仅摘要事件携带目标会话;详情流用 tag 目标(含 agent_kind)。
        let (tag_device, tag_native, tag_agent_kind) = match &key.1 {
            TargetTag::Session {
                device,
                agent_kind,
                native_session_id,
            } => (
                Some(*device),
                Some(native_session_id.clone()),
                agent_kind_name(*agent_kind),
            ),
            TargetTag::List => (None, None, String::new()),
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
                _ => (tag_device, tag_native.clone(), tag_agent_kind.clone()),
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
        // 合并 key 附加流身份(stream/epoch/上游设备):同一列表流聚合多台
        // 设备、同一浏览器聚合多个流,同 item ID 不得交叉合并或替换(§17.6)。
        let merge_scope = format!("{}/{}/{}", ds.stream_id, ds.epoch, device);
        let mut slow_all: Vec<uuid::Uuid> = Vec::new();
        for event in events {
            let (kind, merge) = classify(&event);
            let merge = merge.map(|m| m.scoped(&merge_scope));
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
            // 记录上游批号:快照覆盖判定只清除确实被覆盖的事件(R2-AC01);
            // 上游未提供批号(0)时视为未知,任何快照都不得删除该帧。
            let upstream_seq = if env.sequence == 0 {
                None
            } else {
                Some(env.sequence)
            };
            let frame = Frame::with_upstream_seq(frame_env, kind, merge, upstream_seq);

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
            for conn_id in &slow_all {
                detach_failed_subscriber(streams, browsers, browser_streams, &key, *conn_id);
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
            // 首个快照尚未缓存且全部订阅者仍在等待快照时,本地 presence 帧
            // 不入队:它只会落在首快照之前的回放窗口里,把首订阅重放变成
            // "事件帧在前"(base=首帧-1),违反 §17.4 步骤 5 的握手顺序
            //(快照占据 base,先于快照交付)。此状态下无人需要实时帧,直接
            // 跳过且不消耗下游序号;已有活跃订阅者(如内存压力清理后)时
            // 仍按原路径缓冲并直收。
            if ds.snapshots.is_empty() && ds.subscribers.values().all(|s| s.awaiting_snapshot) {
                continue;
            }
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
                detach_failed_subscriber(streams, browsers, browser_streams, &key, conn_id);
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
            // 暂存(快照在途事件)计入全局内存统计与单流压力排名(§17.6)。
            total += ds.buffer.bytes as u64 + ds.pending_bytes as u64;
            let charged = ds.buffer.bytes + ds.pending_bytes;
            if worst
                .as_ref()
                .map(|(_, b)| charged > *b)
                .unwrap_or(true)
            {
                worst = Some((key.clone(), charged));
            }
        }
        let mut queued = 0u64;
        for bc in g.browsers.values() {
            queued += bc.outbox.queued_bytes() as u64;
        }
        total += queued;
        self.global_buffer_bytes.store(total, Ordering::Relaxed);
        tracing::debug!(
            target: "relay::realtime",
            browsers = g.browsers.len(),
            streams = g.streams.len(),
            queued_bytes = queued,
            overload_closes = buffer::OUTBOX_OVERLOAD_CLOSES.load(Ordering::Relaxed),
            write_timeouts = buffer::WS_WRITE_TIMEOUTS.load(Ordering::Relaxed),
            "relay realtime diagnostics"
        );
        if total > limits::GLOBAL_BUFFER_MAX_BYTES as u64 {
            if let Some((key, _)) = worst {
                if let Some(ds) = g.streams.get_mut(&key) {
                    ds.clear_pending();
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
                let triggers = {
                    let mut g = self.inner.lock().unwrap();
                    Self::handle_upstream_snapshot(&mut g, &upstream_id, &env)
                };
                crate::push::notify(app, &triggers).await;
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
                        Self::handle_upstream_snapshot(&mut g, &upstream_id, &env)
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
        let status = Receipt::try_from(result.status).unwrap_or(Receipt::Unspecified);
        let terminal = matches!(
            status,
            Receipt::ReceiptCompleted | Receipt::ReceiptRejected | Receipt::ReceiptOutcomeUnknown
        );
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
                // DISPATCHED_TO_CODEX 是中间态；保留跟踪直到终态，才能把
                // COMPLETED/REJECTED/OUTCOME_UNKNOWN 继续送达同一浏览器。
                if terminal {
                    g.commands.remove(&result.request_id);
                }
                Some((conn_id, elapsed))
            } else {
                None
            }
        };
        let status_name = match status {
            Receipt::ReceiptCompleted => "COMPLETED",
            Receipt::ReceiptRejected => "REJECTED",
            Receipt::ReceiptOutcomeUnknown => "OUTCOME_UNKNOWN",
            Receipt::ReceiptDispatchedToCodex => "DISPATCHED_TO_CODEX",
            Receipt::ReceiptAcceptedByBridge => "ACCEPTED_BY_BRIDGE",
            _ => "RECEIVED",
        };
        let error = if result.error_code == StableErrorCode::Unspecified as i32 {
            ""
        } else {
            stable_code_ref_name(
                &StableErrorCode::try_from(result.error_code)
                    .unwrap_or(StableErrorCode::InternalError),
            )
        };
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
                        result: if error.is_empty() {
                            status_name.to_string()
                        } else {
                            format!("{status_name}:{error}")
                        },
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
                self.on_browser_subscribe(app, conn_id, subscribe, &env.correlation_id)
                    .await;
            }
            Some(P::Unsubscribe(unsub)) => {
                self.browser_unsubscribe(conn_id, &unsub.stream_id);
            }
            Some(P::Ack(ack)) => {
                self.browser_ack(conn_id, &ack.stream_id, ack.sequence);
            }
            Some(P::ResyncRequest(ResyncRequest { stream_id })) => {
                self.browser_resync(conn_id, stream_id, &env.correlation_id);
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
        correlation: &str,
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
                self.browser_subscribe(
                    conn_id,
                    (ident.owner_id, TargetTag::List),
                    needs,
                    correlation.to_string(),
                );
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
                    // 原样保留浏览器的 agent_kind(未知值不默认转 Codex,ZC-02;
                    // 由 Bridge 显式拒绝)。
                    agent_kind: key.agent_kind,
                    native_session_id: key.native_session_id.clone(),
                };
                self.browser_subscribe(
                    conn_id,
                    (ident.owner_id, tag.clone()),
                    vec![(device, tag)],
                    correlation.to_string(),
                );
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
                    let stored_error = if existing.error_code.is_empty() {
                        StableErrorCode::Unspecified
                    } else {
                        crate::state::stable_code_from_name(&existing.error_code)
                    };
                    let mut result = command_rejected(&request.request_id, stored_error);
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

fn subscribe_target_session(
    device: uuid::Uuid,
    agent_kind: i32,
    native_session_id: &str,
) -> subscribe::Target {
    subscribe::Target::Session(SessionKey {
        device_id: device.to_string(),
        // 浏览器声明的 agent_kind 原样透传(ZC-02;未知值由 Bridge 拒绝)。
        agent_kind,
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

#[cfg(test)]
mod tests {
    use super::*;
    use buffer::{BROWSER_QUEUE_MAX_BYTES, OutItem};

    fn test_browser(outbox: Arc<Outbox>) -> (uuid::Uuid, BrowserConn) {
        let conn_id = uuid::Uuid::new_v4();
        (
            conn_id,
            BrowserConn {
                outbox,
                ident: BrowserIdentity {
                    auth_session_id: uuid::Uuid::new_v4(),
                    owner_id: uuid::Uuid::new_v4(),
                    expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
                },
            },
        )
    }

    fn list_stream_fixture(device: uuid::Uuid) -> (DKey, String) {
        let key = (uuid::Uuid::new_v4(), TargetTag::List);
        let uid = upstream_stream_id(device, &TargetTag::List);
        (key, uid)
    }

    /// 构造上游摘要事件(批 sequence 编码在 envelope 上,fan_out 据此记录
    /// 帧的 upstream_seq)。
    fn summary_events(device: uuid::Uuid, title: &str) -> Vec<domain_event::Event> {
        vec![domain_event::Event::SessionSummaryChanged(
            agent_console_protocol::v1::SessionSummary {
                session_key: Some(agent_console_protocol::v1::SessionKey {
                    device_id: device.to_string(),
                    agent_kind: 1,
                    native_session_id: "native-x".to_string(),
                    relay_session_uuid: String::new(),
                }),
                title: title.to_string(),
                ..Default::default()
            },
        )]
    }

    fn upstream_batch_env(device: uuid::Uuid, uid: &str, batch_seq: u64) -> Envelope {
        let mut env = base_envelope(
            &device.to_string(),
            envelope::Payload::EventBatch(EventBatch {
                stream_id: uid.to_string(),
                events: vec![],
            }),
        );
        env.stream_id = uid.to_string();
        env.sequence = batch_seq;
        env
    }

    fn snapshot_env(device: uuid::Uuid, uid: &str) -> Envelope {
        let mut env = base_envelope(
            &device.to_string(),
            envelope::Payload::RuntimeSnapshot(
                agent_console_protocol::v1::RuntimeSnapshot::default(),
            ),
        );
        env.stream_id = uid.to_string();
        env.sequence = 50;
        env
    }

    /// R2-AC01 单元回归:
    /// 1. 快照在途期间该上游的事件只入暂存,不分配下游序号(§17.4 步骤 4);
    ///    快照到达后先定序快照帧、再 flush 暂存事件,重放顺序恒为
    ///    快照 → 其后事件,批号 ≤ Subscribed.base 的暂存批被快照覆盖丢弃。
    /// 2. awaiting 订阅者快照/回放全部成功入队后才转活跃。
    /// 3. 入队失败(慢 consumer)复用重同步/断开收尾,不宣称恢复完成。
    #[tokio::test]
    async fn snapshot_flush_commits_active_state_and_detaches_on_push_failure() {
        let device = uuid::Uuid::new_v4();
        let (key, uid) = list_stream_fixture(device);
        let mut g = HubInner::default();
        g.upstream_index.insert(uid.clone(), key.clone());
        {
            let ds = g
                .streams
                .entry(key.clone())
                .or_insert_with(|| DStream::new(key.clone()));
            ds.upstreams.insert(
                uid.clone(),
                UpstreamBinding {
                    device,
                    epoch: 7,
                    last_seq: 0,
                    snapshot_covers: 10,
                    snapshot_received: false,
                },
            );
        }

        let outbox_full = Arc::new(Outbox::new(1, BROWSER_QUEUE_MAX_BYTES));
        let outbox_ok = Arc::new(Outbox::new(64, BROWSER_QUEUE_MAX_BYTES));
        let (conn_full, conn) = test_browser(outbox_full.clone());
        let (conn_ok, conn2) = test_browser(outbox_ok.clone());
        g.browsers.insert(conn_full, conn);
        g.browsers.insert(conn_ok, conn2);
        let ds = g.streams.get_mut(&key).unwrap();
        ds.subscribers
            .insert(conn_full, SubscriberState { awaiting_snapshot: true, acked: 0, subscribe_correlation: String::new() });
        ds.subscribers
            .insert(conn_ok, SubscriberState { awaiting_snapshot: true, acked: 0, subscribe_correlation: String::new() });

        // 快照前到达两个事件批:批 5(被 covers=10 覆盖)、批 15(未覆盖)。
        // awaiting 订阅者不直收;快照在途时两批都只入暂存(§17.4 步骤 4),
        // 不占用下游序号。
        let env5 = upstream_batch_env(device, &uid, 5);
        Hub::fan_out_events(&mut g, &uid, &env5, summary_events(device, "covered"));
        let env15 = upstream_batch_env(device, &uid, 15);
        Hub::fan_out_events(&mut g, &uid, &env15, summary_events(device, "uncovered"));
        {
            let ds = g.streams.get(&key).unwrap();
            assert!(
                ds.buffer.is_empty(),
                "快照在途事件必须暂存,不得提前定序入缓冲"
            );
            assert_eq!(ds.next_seq, 1, "暂存批不消耗下游序号");
        }

        // 填满慢订阅者队列:快照回放入队必然 SlowConsumer。
        let filler = Frame::new(
            upstream_batch_env(device, &uid, 999),
            FrameKind::Recoverable,
            None,
        );
        outbox_full.push_frame(filler).unwrap();

        Hub::handle_upstream_snapshot(&mut g, &uid, &snapshot_env(device, &uid));

        // 快照占用 seq1 定序;批 5(≤ covers=10)被快照覆盖丢弃;批 15 在
        // 快照之后定序为 seq2 并扇出——顺序恒为快照 → 其后事件(§17.4 步骤 5)。
        {
            let ds = g.streams.get(&key).unwrap();
            assert_eq!(ds.buffer.len(), 1);
            assert_eq!(ds.buffer.items[0].env.sequence, 2);
            assert_eq!(ds.buffer.items[0].upstream_seq, Some(15));
            assert_eq!(ds.next_seq, 3, "snapshot seq1 + flushed event seq2");
        }
        // 慢订阅者:收尾 = ResyncRequired + 稳定断开 + 订阅摘除。
        assert!(
            !g.streams[&key].subscribers.contains_key(&conn_full),
            "failed flush must not keep the subscriber"
        );
        assert!(outbox_full.is_closed());
        let mut saw_resync = false;
        loop {
            match outbox_full.recv().await {
                Some(OutItem::Direct(bytes)) => {
                    let env = agent_console_protocol::codec::decode_envelope(&bytes)
                        .expect("decode direct");
                    match env.payload {
                        Some(envelope::Payload::ResyncRequired(_)) => saw_resync = true,
                        Some(envelope::Payload::Subscribed(_)) => {}
                        other => panic!("unexpected direct frame: {other:?}"),
                    }
                }
                Some(OutItem::Close(reason)) => {
                    assert_eq!(reason, "RESYNC_REQUIRED");
                    break;
                }
                Some(OutItem::Frame(_)) => panic!("unexpected stream frame"),
                None => break,
            }
        }
        assert!(saw_resync, "resync hint must be delivered on flush failure");

        // 正常订阅者:快照+回放成功入队后转活跃,重放按全局序号连续。
        {
            let ds = g.streams.get(&key).unwrap();
            let sub = ds.subscribers.get(&conn_ok).unwrap();
            assert!(!sub.awaiting_snapshot, "successful flush must go live");
        }
        let mut seqs = Vec::new();
        let mut saw_subscribed = false;
        for _ in 0..3 {
            match outbox_ok.recv().await {
                Some(OutItem::Direct(bytes)) => {
                    let env = agent_console_protocol::codec::decode_envelope(&bytes)
                        .expect("decode direct");
                    assert!(matches!(
                        env.payload,
                        Some(envelope::Payload::Subscribed(ref s)) if s.base_sequence == 1
                    ));
                    saw_subscribed = true;
                }
                Some(OutItem::Frame(f)) => seqs.push(f.env.sequence),
                _ => panic!("unexpected outbox item"),
            }
        }
        assert!(saw_subscribed);
        assert_eq!(
            seqs,
            vec![1, 2],
            "delivery = snapshot(seq1) → flushed uncovered event(seq2), base=1"
        );
    }

    /// R2-AC01 e2e 回归锁定(e2e_no_ui 场景 4,§17.4 步骤 5):重放窗口以
    /// 快照帧开头时(首次订阅的 fresh 流、清空重取后的详情流),Subscribed.
    /// base_sequence 必须等于快照帧的下游序号——快照占据 base,其后事件从
    /// base+1 连续;不得退化为"首帧序号-1"。
    #[tokio::test]
    async fn snapshot_first_window_anchors_base_at_snapshot_sequence() {
        let device = uuid::Uuid::new_v4();
        let (key, uid) = list_stream_fixture(device);
        let mut g = HubInner::default();
        g.upstream_index.insert(uid.clone(), key.clone());
        {
            let ds = g
                .streams
                .entry(key.clone())
                .or_insert_with(|| DStream::new(key.clone()));
            ds.upstreams.insert(
                uid.clone(),
                UpstreamBinding {
                    device,
                    epoch: 7,
                    last_seq: 0,
                    snapshot_covers: 0,
                    snapshot_received: false,
                },
            );
        }
        let outbox = Arc::new(Outbox::new(64, BROWSER_QUEUE_MAX_BYTES));
        let (conn, browser) = test_browser(outbox.clone());
        g.browsers.insert(conn, browser);
        g.streams.get_mut(&key).unwrap().subscribers.insert(
            conn,
            SubscriberState {
                awaiting_snapshot: true,
                acked: 0,
                subscribe_correlation: String::new(),
            },
        );

        Hub::handle_upstream_snapshot(&mut g, &uid, &snapshot_env(device, &uid));

        // fresh 流:快照帧占用下游序号 1;订阅者按 Subscribed → 快照收到。
        let mut base: Option<u64> = None;
        let mut snap_seq: Option<u64> = None;
        for _ in 0..2 {
            match outbox.recv().await {
                Some(OutItem::Direct(bytes)) => {
                    let env = agent_console_protocol::codec::decode_envelope(&bytes)
                        .expect("decode direct");
                    match env.payload {
                        Some(envelope::Payload::Subscribed(s)) => base = Some(s.base_sequence),
                        other => panic!("expected Subscribed, got {other:?}"),
                    }
                }
                Some(OutItem::Frame(f)) => snap_seq = Some(f.env.sequence),
                _ => panic!("unexpected outbox item"),
            }
        }
        assert_eq!(snap_seq, Some(1), "fresh 流首帧快照占用序号 1");
        assert_eq!(
            base, snap_seq,
            "快照 sequence 必须等于 Subscribed.base_sequence(§17.4 步骤 5)"
        );
    }

    /// ZC-02:同机双 Agent 同 nativeSessionId 时,上游流键与发给 Bridge 的
    /// 订阅目标必须按 agent_kind 区分,详情流/事件/命令路由互不串线。
    #[test]
    fn upstream_keys_distinguish_agent_kind_for_same_native_id() {
        let device = uuid::Uuid::new_v4();
        let codex = TargetTag::Session {
            device,
            agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
            native_session_id: "dup-1".to_string(),
        };
        let zcode = TargetTag::Session {
            device,
            agent_kind: agent_console_protocol::v1::AgentKind::ZcodeDesktop as i32,
            native_session_id: "dup-1".to_string(),
        };
        assert_ne!(codex, zcode, "DKey 必须按 agent_kind 区分");
        assert_ne!(
            upstream_stream_id(device, &codex),
            upstream_stream_id(device, &zcode),
            "上游流键必须按 agent_kind 区分"
        );
        // 上游 Subscribe 信封携带各自真实 agent_kind(Relay → Bridge)。
        let env = Hub::upstream_subscribe_env(device, &zcode);
        match env.payload {
            Some(envelope::Payload::Subscribe(sub)) => match sub.target {
                Some(subscribe::Target::Session(key)) => {
                    assert_eq!(
                        key.agent_kind,
                        agent_console_protocol::v1::AgentKind::ZcodeDesktop as i32
                    );
                    assert_eq!(key.native_session_id, "dup-1");
                }
                other => panic!("unexpected target {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        }
    }

    /// 未知 agent_kind 数值在流键中同样彼此隔离(不默认折叠为 Codex)。
    #[test]
    fn unknown_agent_kind_values_stay_isolated() {
        let device = uuid::Uuid::new_v4();
        let codex = TargetTag::Session {
            device,
            agent_kind: 1,
            native_session_id: "dup-1".to_string(),
        };
        let unknown = TargetTag::Session {
            device,
            agent_kind: 9,
            native_session_id: "dup-1".to_string(),
        };
        assert_ne!(upstream_stream_id(device, &codex), upstream_stream_id(device, &unknown));
    }

    /// 快照缓存总量硬上限:缓存超过 SNAPSHOT_CACHE_MAX 台设备的快照后,
    /// 最旧快照被淘汰、长度有界;淘汰路径不 panic,锚定重放兜底仍可用。
    #[test]
    fn snapshot_cache_evicts_oldest_beyond_hard_cap_and_anchored_replay_survives() {
        let mut ds = DStream::new((uuid::Uuid::new_v4(), TargetTag::List));
        let devices: Vec<uuid::Uuid> = (0..17).map(|_| uuid::Uuid::new_v4()).collect();
        for device in &devices {
            let mut env = base_envelope(
                &device.to_string(),
                envelope::Payload::RuntimeSnapshot(
                    agent_console_protocol::v1::RuntimeSnapshot::default(),
                ),
            );
            env.stream_id = ds.stream_id.clone();
            env.stream_epoch = ds.epoch;
            env.sequence = ds.next_seq;
            ds.next_seq += 1;
            ds.cache_snapshot(Frame::new(env, FrameKind::Critical, None));
        }
        assert_eq!(
            ds.snapshots.len(),
            SNAPSHOT_CACHE_MAX,
            "cache must stay bounded at the hard cap"
        );
        assert_eq!(
            ds.snapshots.front().unwrap().env.device_id,
            devices[1].to_string(),
            "oldest device snapshot evicted first"
        );
        assert_eq!(
            ds.snapshots.back().unwrap().env.device_id,
            devices[16].to_string(),
            "newest device snapshot retained"
        );
        // 锚定重放兜底:窗口非空、以缓存的快照帧开头,base 与首帧一致。
        let (base, frames) = ds.anchored_replay();
        assert!(
            frames
                .iter()
                .any(|f| f.env.device_id == devices[16].to_string()),
            "latest snapshot must be replayable"
        );
        assert_eq!(
            base,
            frames.first().unwrap().env.sequence,
            "window anchored at its first frame"
        );
    }

    /// 复核回归 1:快照水位(covers=10)之后到达的关键事件(问题,上游批
    /// 11)进入暂存后,再被 1024 个输出事件施加压力——暂存只允许挤出非第
    /// 1 类;快照到达后订阅者必须收到该问题(或显式 ResyncRequired),不得
    /// 静默丢失后继续宣称同步正常(§17.6)。
    #[tokio::test]
    async fn review_pending_overflow_must_deliver_uncovered_question_or_resync() {
        use agent_console_protocol::v1 as pb;
        let device = uuid::Uuid::new_v4();
        let tag = TargetTag::Session {
            device,
            agent_kind: 1,
            native_session_id: "review-session".into(),
        };
        let key = (uuid::Uuid::new_v4(), tag.clone());
        let uid = upstream_stream_id(device, &tag);
        let mut g = HubInner::default();
        g.upstream_index.insert(uid.clone(), key.clone());
        let mut ds = DStream::new(key.clone());
        ds.upstreams.insert(
            uid.clone(),
            UpstreamBinding {
                device,
                epoch: 1,
                last_seq: 9,
                snapshot_covers: 10,
                snapshot_received: false,
            },
        );
        let outbox = Arc::new(Outbox::new(2048, BROWSER_QUEUE_MAX_BYTES));
        let (conn_id, conn) = test_browser(outbox.clone());
        g.browsers.insert(conn_id, conn);
        ds.subscribers
            .insert(conn_id, SubscriberState { awaiting_snapshot: true, acked: 0, subscribe_correlation: String::new() });
        g.streams.insert(key.clone(), ds);
        let question = domain_event::Event::PendingAttentionAdded(pb::PendingAttentionAdded {
            attention: Some(pb::pending_attention_added::Attention::Question(
                pb::PendingAttentionQuestion {
                    question_id: "review-uncovered-question".into(),
                    title: "Synthetic question".into(),
                    valid: true,
                    ..Default::default()
                },
            )),
        });
        Hub::fan_out_events(&mut g, &uid, &upstream_batch_env(device, &uid, 11), vec![question]);
        for seq in 12..=1035 {
            let output = domain_event::Event::OutputAppend(pb::OutputAppend {
                item_id: Some(pb::ItemId {
                    id: format!("item-{seq}"),
                    ..Default::default()
                }),
                bytes: vec![b'x'],
                ..Default::default()
            });
            Hub::fan_out_events(&mut g, &uid, &upstream_batch_env(device, &uid, seq), vec![output]);
        }
        let pending_question = g.streams[&key]
            .pending_batches
            .iter()
            .flat_map(|b| &b.events)
            .any(|e| matches!(e, domain_event::Event::PendingAttentionAdded(_)));
        assert!(
            pending_question,
            "压力挤出只允许作用于非第 1 类,问题事件必须留在暂存"
        );
        Hub::handle_upstream_snapshot(&mut g, &uid, &snapshot_env(device, &uid));
        let mut saw_question = false;
        let mut saw_resync = false;
        while let Ok(Some(item)) =
            tokio::time::timeout(std::time::Duration::from_millis(1), outbox.recv()).await
        {
            let env = match item {
                OutItem::Direct(bytes) => {
                    agent_console_protocol::codec::decode_envelope(&bytes).unwrap()
                }
                OutItem::Frame(frame) => frame.env.clone(),
                OutItem::Close(_) => {
                    saw_resync = true;
                    break;
                }
            };
            match env.payload {
                Some(envelope::Payload::ResyncRequired(_)) => saw_resync = true,
                Some(envelope::Payload::EventBatch(batch)) => {
                    saw_question |= batch
                        .events
                        .iter()
                        .any(|e| matches!(e.event, Some(domain_event::Event::PendingAttentionAdded(_))));
                }
                _ => {}
            }
        }
        assert!(
            saw_question || saw_resync,
            "question at upstream seq=11 (snapshot covers=10) was lost; delivered_question={}, resync={}, subscriber_still_active={}",
            saw_question,
            saw_resync,
            g.streams[&key].subscribers.contains_key(&conn_id)
        );
    }

    /// 复核回归 2:暂存正文计入现有单流字节预算——257 个 64 KiB 输出块
    /// (≈16.8 MB)超过 16 MiB 上限时,压力挤出必须把暂存压回预算内(§17.6)。
    #[test]
    fn review_pending_queue_must_respect_existing_stream_byte_limit() {
        use agent_console_protocol::v1 as pb;
        let device = uuid::Uuid::new_v4();
        let (key, uid) = list_stream_fixture(device);
        let mut ds = DStream::new(key);
        for seq in 1..=257 {
            ds.push_pending(PendingUpstreamBatch {
                upstream_id: uid.clone(),
                upstream_seq: seq,
                sent_at: None,
                device_id: device.to_string(),
                events: vec![domain_event::Event::OutputAppend(pb::OutputAppend {
                    bytes: vec![b'x'; 64 * 1024],
                    ..Default::default()
                })],
            });
        }
        let bytes: usize = ds
            .pending_batches
            .iter()
            .flat_map(|batch| &batch.events)
            .map(|event| {
                if let domain_event::Event::OutputAppend(output) = event {
                    output.bytes.len()
                } else {
                    0
                }
            })
            .sum();
        assert!(
            bytes <= limits::BUFFER_MAX_BYTES,
            "pending output bytes={} exceed per-stream limit={}, but existing accounted buffer.bytes={}",
            bytes,
            limits::BUFFER_MAX_BYTES,
            ds.buffer.bytes
        );
        assert!(
            ds.pending_bytes > 0,
            "暂存字节必须计入预算统计(此处计入 {} 字节)",
            ds.pending_bytes
        );
    }

    /// 复核回归 3:暂存仅剩第 1 类仍超限(1025 个问题事件,无可挤出项)时,
    /// 必须走显式恢复:订阅者收到 ResyncRequired、暂存清空、binding 重置,
    /// 且强制重取快照的重订阅 env 真正发往 Bridge(不是只改本地状态)。
    #[tokio::test]
    async fn review_pending_overflow_with_only_critical_events_forces_explicit_recovery() {
        use agent_console_protocol::v1 as pb;
        let device = uuid::Uuid::new_v4();
        let tag = TargetTag::Session {
            device,
            agent_kind: 1,
            native_session_id: "review-crit".into(),
        };
        let key = (uuid::Uuid::new_v4(), tag.clone());
        let uid = upstream_stream_id(device, &tag);
        let mut g = HubInner::default();
        g.upstream_index.insert(uid.clone(), key.clone());
        let mut ds = DStream::new(key.clone());
        ds.upstreams.insert(
            uid.clone(),
            UpstreamBinding {
                device,
                epoch: 1,
                last_seq: 0,
                snapshot_covers: 10,
                snapshot_received: false,
            },
        );
        let outbox = Arc::new(Outbox::new(2048, BROWSER_QUEUE_MAX_BYTES));
        let (conn_id, conn) = test_browser(outbox.clone());
        g.browsers.insert(conn_id, conn);
        ds.subscribers
            .insert(conn_id, SubscriberState { awaiting_snapshot: true, acked: 0, subscribe_correlation: String::new() });
        g.streams.insert(key.clone(), ds);
        // 注册 Bridge 连接:恢复路径必须把重订阅 env 发往它的 sink。
        let (sink, mut bridge_rx) = tokio::sync::mpsc::channel::<WireBytes>(64);
        let (close, _close_rx) = tokio::sync::watch::channel(false);
        g.bridges.insert(
            device,
            BridgeConn {
                device,
                owner: uuid::Uuid::new_v4(),
                sink,
                close,
            },
        );

        for seq in 1..=(limits::SNAPSHOT_PENDING_MAX_EVENTS as u64 + 1) {
            let question = domain_event::Event::PendingAttentionAdded(pb::PendingAttentionAdded {
                attention: Some(pb::pending_attention_added::Attention::Question(
                    pb::PendingAttentionQuestion {
                        question_id: format!("review-crit-{seq}"),
                        title: "Critical only".into(),
                        valid: true,
                        ..Default::default()
                    },
                )),
            });
            Hub::fan_out_events(
                &mut g,
                &uid,
                &upstream_batch_env(device, &uid, seq),
                vec![question],
            );
        }

        {
            let ds = g.streams.get(&key).unwrap();
            assert!(ds.pending_batches.is_empty(), "显式恢复必须清空暂存");
            assert_eq!(ds.pending_bytes, 0);
            assert_eq!(ds.pending_events, 0);
            assert!(
                !ds.upstreams.get(&uid).unwrap().snapshot_received,
                "binding 必须重置等待新快照"
            );
        }
        let mut saw_resync = false;
        while let Ok(Some(item)) =
            tokio::time::timeout(std::time::Duration::from_millis(1), outbox.recv()).await
        {
            if let OutItem::Direct(bytes) = item {
                let env = agent_console_protocol::codec::decode_envelope(&bytes).unwrap();
                if matches!(env.payload, Some(envelope::Payload::ResyncRequired(_))) {
                    saw_resync = true;
                }
            }
        }
        assert!(saw_resync, "订阅者必须收到 ResyncRequired");
        let mut saw_subscribe = false;
        while let Ok(Some(bytes)) =
            tokio::time::timeout(std::time::Duration::from_millis(1), bridge_rx.recv()).await
        {
            let env = agent_console_protocol::codec::decode_envelope(&bytes).unwrap();
            if matches!(env.payload, Some(envelope::Payload::Subscribe(_))) {
                saw_subscribe = true;
            }
        }
        assert!(
            saw_subscribe,
            "强制重取快照的重订阅必须真正发往 Bridge,不能只改本地状态"
        );
    }

    /// round3 回归:快照水位(covers=10)之后的最终回复(ItemUpsert /
    /// AssistantMessage,第 2 类)进入暂存后,再被 1024 个输出事件施压——
    /// 暂存只允许挤出第 3 类;快照到达后订阅者必须收到该回复(或显式
    /// ResyncRequired)。"可经历史重读"必须先活着送达或触发恢复,不能静默丢。
    #[tokio::test]
    async fn round3_uncovered_assistant_message_must_be_delivered_or_resynced() {
        use agent_console_protocol::v1 as pb;
        let device = uuid::Uuid::new_v4();
        let tag = TargetTag::Session {
            device,
            agent_kind: 1,
            native_session_id: "round3-message".into(),
        };
        let key = (uuid::Uuid::new_v4(), tag.clone());
        let uid = upstream_stream_id(device, &tag);
        let mut g = HubInner::default();
        g.upstream_index.insert(uid.clone(), key.clone());
        let mut ds = DStream::new(key.clone());
        ds.upstreams.insert(
            uid.clone(),
            UpstreamBinding {
                device,
                epoch: 1,
                last_seq: 9,
                snapshot_covers: 10,
                snapshot_received: false,
            },
        );
        let outbox = Arc::new(Outbox::new(2048, BROWSER_QUEUE_MAX_BYTES));
        let (conn_id, conn) = test_browser(outbox.clone());
        g.browsers.insert(conn_id, conn);
        ds.subscribers.insert(
            conn_id,
            SubscriberState { awaiting_snapshot: true, acked: 0, subscribe_correlation: String::new() },
        );
        g.streams.insert(key.clone(), ds);
        let message = domain_event::Event::ItemUpsert(pb::ItemUpsert {
            item: Some(pb::Item {
                item_id: Some(pb::ItemId {
                    id: "uncovered-message".into(),
                    ..Default::default()
                }),
                content: Some(pb::item::Content::AssistantMessage(pb::AssistantMessage {
                    text: "synthetic final answer".into(),
                    r#final: true,
                    ..Default::default()
                })),
                ..Default::default()
            }),
        });
        Hub::fan_out_events(&mut g, &uid, &upstream_batch_env(device, &uid, 11), vec![message]);
        for seq in 12..=1035 {
            let output = domain_event::Event::OutputAppend(pb::OutputAppend {
                item_id: Some(pb::ItemId {
                    id: format!("item-{seq}"),
                    ..Default::default()
                }),
                bytes: vec![b'x'],
                ..Default::default()
            });
            Hub::fan_out_events(&mut g, &uid, &upstream_batch_env(device, &uid, seq), vec![output]);
        }
        Hub::handle_upstream_snapshot(&mut g, &uid, &snapshot_env(device, &uid));
        let mut saw_message = false;
        let mut saw_resync = false;
        while let Ok(Some(item)) =
            tokio::time::timeout(std::time::Duration::from_millis(1), outbox.recv()).await
        {
            let env = match item {
                OutItem::Direct(bytes) => {
                    agent_console_protocol::codec::decode_envelope(&bytes).unwrap()
                }
                OutItem::Frame(frame) => frame.env.clone(),
                OutItem::Close(_) => {
                    saw_resync = true;
                    break;
                }
            };
            match env.payload {
                Some(envelope::Payload::ResyncRequired(_)) => saw_resync = true,
                Some(envelope::Payload::EventBatch(batch)) => {
                    saw_message |= batch
                        .events
                        .iter()
                        .any(|e| matches!(e.event, Some(domain_event::Event::ItemUpsert(_))));
                }
                _ => {}
            }
        }
        assert!(
            saw_message || saw_resync,
            "assistant message at upstream seq=11 (snapshot covers=10) was lost; delivered={}, resync={}, subscriber_still_active={}",
            saw_message,
            saw_resync,
            g.streams[&key].subscribers.contains_key(&conn_id)
        );
    }

    /// round3 回归:队首保留一个第 1 类事件后,后续批次被挤空——空批次壳
    /// 必须立即移除,不得在暂存队列中累积占内存(实测曾残留 3073 个空壳)。
    #[test]
    fn round3_evicted_batch_shells_must_not_accumulate_behind_a_question() {
        use agent_console_protocol::v1 as pb;
        let device = uuid::Uuid::new_v4();
        let (key, uid) = list_stream_fixture(device);
        let mut ds = DStream::new(key);
        let question = domain_event::Event::PendingAttentionAdded(pb::PendingAttentionAdded {
            attention: Some(pb::pending_attention_added::Attention::Question(
                pb::PendingAttentionQuestion {
                    question_id: "kept-question".into(),
                    valid: true,
                    ..Default::default()
                },
            )),
        });
        assert!(!ds.push_pending(PendingUpstreamBatch {
            upstream_id: uid.clone(),
            upstream_seq: 1,
            sent_at: None,
            device_id: device.to_string(),
            events: vec![question],
        }));
        for seq in 2..=4097 {
            assert!(!ds.push_pending(PendingUpstreamBatch {
                upstream_id: uid.clone(),
                upstream_seq: seq,
                sent_at: None,
                device_id: device.to_string(),
                events: vec![domain_event::Event::OutputAppend(pb::OutputAppend {
                    bytes: vec![b'x'],
                    ..Default::default()
                })],
            }));
        }
        let empty = ds.pending_batches.iter().filter(|b| b.events.is_empty()).count();
        assert_eq!(
            empty, 0,
            "pending_batches={}, pending_events={}, pending_bytes={}, empty batch shells remain allocated",
            ds.pending_batches.len(),
            ds.pending_events,
            ds.pending_bytes
        );
    }
}
