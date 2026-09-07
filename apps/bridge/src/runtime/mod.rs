//! BridgeRuntime:headless Bridge 运行时集成层(权威规格 §11/§13/§14/§15/
//! §17/§22/§26)。
//!
//! 职责与边界:
//! - **装配**:把 adapter(事件投影)、CommandGateway/QueueManager(写命令与
//!   单条队列)、FileGrantManager/files transfer(文件数据面)、GitService
//!   (只读 Git)、UploadLifecycle/PowerCoordinator(生命周期与电源)、
//!   LocalStore(本地 SQLite)与 Relay 传输(出站 sink 注入)接成一体。
//! - **入站 Envelope 分发**(§17.3/§27.3/§27.4):Subscribe/Unsubscribe/
//!   ResyncRequest/QueryRequest/CommandRequest/TransferOffer/Heartbeat,按
//!   correlation_id 回对应消息;响应只带 payload 变体名日志,不带正文
//!   (§25.3)。
//! - **出站流**(§17.4/§17.5):每 session 一个 stream;epoch 为订阅纪元,
//!   重连/重建流时新 epoch 并先发 snapshot;sequence 同流内单调;list 流只发
//!   SessionSummary(Batch);事件直接转发(adapter 已做 64KiB/75ms 合并,
//!   §26.3)。出站失败(队列满/停止)即丢弃未确认事件,不自动重试非幂等
//!   命令(§26.4)。
//! - **观察策略**:委托 [`observe`](§14 逐字);UploadLifecycle 与
//!   PowerCoordinator 接到同一 adapter 事件流。
//! - **隐私**:输出正文只进 EventBatch payload 与 command_output_page;会话
//!   cwd 仅在本机内部用于 Git/文件授权,绝不序列化出 Bridge(§12/§17.2)。

pub mod observe;
pub mod proto_mapping;
pub mod transfers;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::codec::{new_message_id, payload_kind, PROTOCOL_VERSION};
use parking_lot::Mutex;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::adapter::codex::CodexAdapter;
use crate::commands::{
    CommandGateway, PowerCoordinator, QueueManager, Submission, UploadCleaner, UploadLifecycle,
};
use crate::config::BridgeConfig;
use crate::domain as dm;
use crate::files::{FileGrantManager, GrantAction, TransferConfig};
use crate::git::GitService;
use crate::local_store::LocalStore;
use crate::power::PowerSource;
use crate::transport::{DeviceCredential, RelayHandle, TransportError};

use observe::ObservePressure;
use proto_mapping as pm;

/// 完整命令输出单页上限(§27.5:单页最大 256 KiB)。
pub const OUTPUT_PAGE_MAX: usize = 256 * 1024;
/// runtime 内存输出缓冲上限(§26.4 有界;超出部分不再保留,分页只覆盖保留区)。
pub const OUTPUT_BUFFER_MAX: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// 出站 sink(注入式;生产 = RelayHandle,测试 = 回环通道)
// ---------------------------------------------------------------------------

/// 出站 Envelope sink。实现不得阻塞(满即错,§17.6)。
pub trait OutboundSink: Send + Sync + std::fmt::Debug {
    fn send_envelope(&self, envelope: &pb::Envelope) -> Result<(), TransportError>;
}

impl OutboundSink for RelayHandle {
    fn send_envelope(&self, envelope: &pb::Envelope) -> Result<(), TransportError> {
        RelayHandle::send(self, envelope)
    }
}

// ---------------------------------------------------------------------------
// 装配
// ---------------------------------------------------------------------------

/// BridgeRuntime 的全部注入件(生产由 `bridge run` 组装;测试可整体替换)。
#[derive(Debug, Clone)]
pub struct RuntimeParts {
    pub config: BridgeConfig,
    pub device_id: String,
    pub store: LocalStore,
    pub adapter: CodexAdapter,
    pub gateway: CommandGateway,
    pub grants: FileGrantManager,
    /// None = 本机无可执行 git:查询回 INTERNAL_ERROR,不影响其他能力。
    pub git: Option<Arc<GitService>>,
    pub uploads: Arc<UploadLifecycle>,
    pub power: PowerCoordinator,
    pub credential: Arc<dyn DeviceCredential>,
    pub outbound: Arc<dyn OutboundSink>,
    pub http: reqwest::Client,
    /// 上传临时根目录(config.uploads_dir();测试可覆盖)。
    pub upload_root: std::path::PathBuf,
    pub transfer_config: TransferConfig,
}

/// 单会话流(§17.4/§17.5)。
struct StreamState {
    stream_id: String,
    /// 所属会话槽位(列表流为 None;resync 重建用,ZC-02)。
    slot: Option<SessionSlot>,
    epoch: u64,
    next_sequence: u64,
    /// snapshot 未发出前事件先缓冲(§17.4 步骤 4)。
    buffering: bool,
    buffer: Vec<pb::DomainEvent>,
}

#[derive(Default)]
struct RuntimeState {
    epoch_counter: u64,
    list_stream: Option<StreamState>,
    /// 会话详情流:按 (agentKind, nativeSessionId) 槽位隔离(ZC-02)。
    session_streams: HashMap<SessionSlot, StreamState>,
}

/// 运行时内存 map 的会话槽位(ZC-02):所有缓存、输出、watcher、详情流
/// 均以 (agentKind, nativeSessionId) 区分,同机同 native id 的双 Agent
/// 会话互不串线。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionSlot(dm::AgentKind, String);

impl SessionSlot {
    fn of(key: &dm::SessionKey) -> Self {
        Self(key.agent_kind, key.native_session_id.clone())
    }

    fn key(&self, device_id: &str) -> dm::SessionKey {
        dm::SessionKey {
            device_id: device_id.to_owned(),
            agent_kind: self.0,
            native_session_id: self.1.clone(),
            relay_session_uuid: None,
        }
    }
}

/// 单会话缓存:只记稳定维度(ID/revision/phase/计数,§14/§25.3)。
#[derive(Debug, Clone, Copy, Default)]
struct SessionCache {
    turn_active: bool,
    attention_count: usize,
    runtime_revision: u64,
    /// adapter 投影已建立快照(观察前提;避免为未跟随会话触发快照等待)。
    has_snapshot: bool,
}

/// item 输出缓冲(command_output_page 数据源;只存内存,不落盘、不进日志)。
#[derive(Debug, Default)]
struct OutputBuffer {
    data: Vec<u8>,
    is_final: bool,
}

struct RuntimeInner {
    config: BridgeConfig,
    device_id: String,
    store: LocalStore,
    adapter: CodexAdapter,
    gateway: CommandGateway,
    grants: FileGrantManager,
    git: Option<Arc<GitService>>,
    uploads: Arc<UploadLifecycle>,
    power: PowerCoordinator,
    credential: Arc<dyn DeviceCredential>,
    outbound: Arc<dyn OutboundSink>,
    http: reqwest::Client,
    upload_root: std::path::PathBuf,
    transfer_config: TransferConfig,

    state: Mutex<RuntimeState>,
    /// (agentKind, native) → 会话缓存(观察与电源输入)。
    sessions: Mutex<HashMap<SessionSlot, SessionCache>>,
    /// (agentKind, native) → item 输出缓冲(command_output_page)。
    outputs: Mutex<HashMap<SessionSlot, HashMap<String, OutputBuffer>>>,
    /// 已登记 adapter watcher 的会话(仅 Codex 槽位)。
    watchers: Mutex<HashSet<SessionSlot>>,
    /// 已接 UploadLifecycle/PowerCoordinator watcher 的会话(每会话一次;仅 Codex)。
    lifecycle_wired: Mutex<HashSet<SessionSlot>>,
    /// 活跃 transfer → 取消令牌(三端联动,§22.4)。
    transfers: Mutex<HashMap<String, CancellationToken>>,
    /// 上传句柄登记(transfer_id → upload handle token;v1 TransferResult 无
    /// 句柄字段,句柄留在 Bridge,由上层经 CommandRequest 附件路径消费)。
    upload_handles: Mutex<HashMap<String, String>>,
    /// 列表流低频刷新 deadline(§14 idle 30s 摘要级)。
    list_refresh_at: Mutex<Option<std::time::Instant>>,
    battery_powered: Mutex<bool>,
    stop: CancellationToken,
    /// ZCode Hook 审批通路(ZC-01 原型;attach 后生效,未 attach 时零开销)。
    zcode_hooks: std::sync::OnceLock<Arc<crate::zcode::ZcodeHooks>>,
}

/// Bridge 运行时。克隆廉价(内部全为句柄)。
#[derive(Clone)]
pub struct BridgeRuntime {
    inner: Arc<RuntimeInner>,
}

impl std::fmt::Debug for BridgeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeRuntime")
            .field("device_id", &self.inner.device_id)
            .finish_non_exhaustive()
    }
}

impl BridgeRuntime {
    pub fn new(parts: RuntimeParts) -> Self {
        Self {
            inner: Arc::new(RuntimeInner {
                config: parts.config,
                device_id: parts.device_id,
                store: parts.store,
                adapter: parts.adapter,
                gateway: parts.gateway,
                grants: parts.grants,
                git: parts.git,
                uploads: parts.uploads,
                power: parts.power,
                credential: parts.credential,
                outbound: parts.outbound,
                http: parts.http,
                upload_root: parts.upload_root,
                transfer_config: parts.transfer_config,
                state: Mutex::new(RuntimeState::default()),
                sessions: Mutex::new(HashMap::new()),
                outputs: Mutex::new(HashMap::new()),
                watchers: Mutex::new(HashSet::new()),
                lifecycle_wired: Mutex::new(HashSet::new()),
                transfers: Mutex::new(HashMap::new()),
                upload_handles: Mutex::new(HashMap::new()),
                list_refresh_at: Mutex::new(None),
                battery_powered: Mutex::new(false),
                stop: CancellationToken::new(),
                zcode_hooks: std::sync::OnceLock::new(),
            }),
        }
    }

    pub fn device_id(&self) -> &str {
        &self.inner.device_id
    }

    pub fn grants(&self) -> &FileGrantManager {
        &self.inner.grants
    }

    pub fn queue(&self) -> &Arc<QueueManager> {
        self.inner.gateway.queue()
    }

    pub fn adapter(&self) -> &CodexAdapter {
        &self.inner.adapter
    }

    /// 关联 ZCode Hook 审批通路(幂等;首次关联生效)。
    pub fn attach_zcode_hooks(&self, hooks: Arc<crate::zcode::ZcodeHooks>) {
        let _ = self.inner.zcode_hooks.set(hooks);
    }

    /// 外部事件源(ZCode Hook 等)经现有事件通道分发(§17.4 语义不变:
    /// 无订阅者时事件丢弃,不缓冲)。
    pub async fn publish_external_event(
        self: &Arc<Self>,
        key: &dm::SessionKey,
        event: dm::DomainEvent,
    ) {
        self.note_event(key, &event);
        self.dispatch_domain_event(key, event).await;
    }

    /// 已签发的上传句柄(transfer_id → handle token;上层交给 Codex 的
    /// 附件路径由调用方决定,§22.5 第 6 步)。
    pub fn upload_handle(&self, transfer_id: &str) -> Option<String> {
        self.inner.upload_handles.lock().get(transfer_id).cloned()
    }

    // -----------------------------------------------------------------
    // transfer 支撑(transfers.rs 使用)
    // -----------------------------------------------------------------

    pub(crate) fn relay_url(&self) -> Option<String> {
        self.inner.config.relay_url.clone()
    }

    pub(crate) fn upload_root(&self) -> std::path::PathBuf {
        self.inner.upload_root.clone()
    }

    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.inner.http
    }

    pub(crate) fn transfer_config(&self) -> TransferConfig {
        self.inner.transfer_config
    }

    pub(crate) fn credential_token(&self) -> String {
        self.inner.credential.bearer_token()
    }

    /// per-transfer HTTP 客户端:把 §22.3 信息性头作为 default headers,
    /// produce/consume 自身追加 Content-Type/Range/Length 等正文头。
    pub(crate) fn http_client_with_headers(
        &self,
        headers: &reqwest::header::HeaderMap,
    ) -> Result<reqwest::Client, reqwest::Error> {
        let mut builder = reqwest::Client::builder();
        for (name, value) in headers.iter() {
            builder = builder.default_headers({
                let mut map = reqwest::header::HeaderMap::new();
                map.insert(name, value.clone());
                map
            });
        }
        builder.build()
    }

    pub(crate) fn register_transfer(&self, transfer_id: String, token: CancellationToken) {
        self.inner.transfers.lock().insert(transfer_id, token);
    }

    pub(crate) fn remove_transfer(&self, transfer_id: &str) {
        self.inner.transfers.lock().remove(transfer_id);
    }

    pub(crate) fn store_upload_handle(&self, transfer_id: &str, token: String) {
        self.inner
            .upload_handles
            .lock()
            .insert(transfer_id.to_owned(), token);
    }

    pub(crate) fn send_transfer_ready(
        &self,
        transfer_id: &str,
        ready: bool,
        rejection: Option<dm::StableErrorCode>,
    ) {
        let payload = pb::envelope::Payload::TransferReady(pb::TransferReady {
            transfer_id: transfer_id.to_owned(),
            ready,
            rejection_code: rejection
                .map(pm::stable_error_to_proto)
                .map(|c| c as i32)
                .unwrap_or(0),
        });
        self.send_connection("", payload);
    }

    pub(crate) fn send_transfer_result(
        &self,
        transfer_id: &str,
        outcome: pb::TransferOutcome,
        error: Option<dm::StableErrorCode>,
    ) {
        self.send_transfer_result_with_handle(transfer_id, outcome, error, None);
    }

    pub(crate) fn send_transfer_result_with_handle(
        &self,
        transfer_id: &str,
        outcome: pb::TransferOutcome,
        error: Option<dm::StableErrorCode>,
        upload_file_handle: Option<&str>,
    ) {
        let payload = pb::envelope::Payload::TransferResult(pb::TransferResult {
            transfer_id: transfer_id.to_owned(),
            outcome: outcome as i32,
            error_code: error
                .map(pm::stable_error_to_proto)
                .map(|c| c as i32)
                .unwrap_or(0),
            upload_file_handle: upload_file_handle.unwrap_or_default().to_owned(),
        });
        self.send_connection("", payload);
    }

    // -----------------------------------------------------------------
    // 生命周期
    // -----------------------------------------------------------------

    /// 启动观察循环(§14)。随 [`BridgeRuntime::shutdown`] 结束。
    pub fn start_observation(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        observe::spawn_observer(self.clone())
    }

    /// 优雅停机(§26.4):停止观察;gateway 停止接受新命令并对未完成命令补
    /// `OUTCOME_UNKNOWN`;释放电源断言。Relay 断开由调用方执行。
    pub async fn shutdown(&self) {
        self.inner.stop.cancel();
        let finalized = self.inner.gateway.graceful_shutdown().await;
        for request_id in finalized {
            tracing::info!(request_id = %request_id, "inflight command finalized OUTCOME_UNKNOWN");
        }
        self.inner.power.release().await;
    }

    /// 注入电源来源(main 的 pmset 轮询;§14 电池降频 + §19 唤醒断言)。
    pub async fn set_power_source(&self, source: PowerSource) {
        *self.inner.battery_powered.lock() = source == PowerSource::Battery;
        let _ = self.inner.power.set_power_source(source).await;
    }

    // -----------------------------------------------------------------
    // 重连重同步(§26.2)
    // -----------------------------------------------------------------

    /// 连接成功回调入口(同步、非阻塞;实际重同步在后台任务执行):
    /// capability → SessionSummaryBatch → 有订阅/活跃会话的 RuntimeSnapshot。
    pub fn resync(self: &Arc<Self>) {
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.resync_inner().await;
        });
    }

    async fn resync_inner(self: &Arc<Self>) {
        // ① capability。
        let caps = self.inner.adapter.capabilities();
        self.send_connection(
            "",
            pb::envelope::Payload::CapabilitySnapshot(pm::capability_set_to_proto(&caps)),
        );
        // ② 列表流:重建(epoch++ → Subscribed → 全量 batch)。
        let has_list = self.inner.state.lock().list_stream.is_some();
        if has_list {
            self.establish_list_stream(String::new(), None).await;
        }
        // ③ 详情流重建;活跃但无详情订阅的会话登记观察(§14 完成检测)。
        let stream_slots: Vec<SessionSlot> = {
            let state = self.inner.state.lock();
            state.session_streams.keys().cloned().collect()
        };
        for slot in stream_slots {
            self.establish_session_stream(&slot, String::new(), None, true)
                .await;
        }
        let active_slots: Vec<SessionSlot> = {
            self.inner
                .sessions
                .lock()
                .iter()
                .filter(|(slot, c)| c.turn_active && slot.0 == dm::AgentKind::CodexDesktop)
                .map(|(k, _)| k.clone())
                .collect()
        };
        for slot in active_slots {
            let key = slot.key(&self.inner.device_id);
            self.attach_session(&key).await;
        }
    }

    // -----------------------------------------------------------------
    // 入站分发
    // -----------------------------------------------------------------

    /// 处理一条入站 Envelope(生产由 main 的 inbound 循环喂;测试直接调用)。
    pub async fn handle_envelope(self: &Arc<Self>, envelope: pb::Envelope) {
        use pb::envelope::Payload;
        let correlation = if envelope.correlation_id.is_empty() {
            envelope.message_id.clone()
        } else {
            envelope.correlation_id.clone()
        };
        let Some(payload) = envelope.payload else {
            return;
        };
        match payload {
            Payload::Subscribe(subscribe) => {
                // 上游流 id 以 Relay 在 Subscribe 上的 stream_id 为准
                // (Bridge 回包必须回显,Routing 由 Relay upstream_index 完成)。
                let upstream_id = (!envelope.stream_id.is_empty()).then_some(envelope.stream_id);
                let runtime = self.clone();
                tokio::spawn(async move {
                    runtime
                        .handle_subscribe(subscribe, upstream_id, correlation)
                        .await;
                });
            }
            Payload::Unsubscribe(unsubscribe) => {
                self.handle_unsubscribe(&unsubscribe.stream_id);
            }
            Payload::ResyncRequest(request) => {
                let runtime = self.clone();
                tokio::spawn(async move {
                    runtime
                        .handle_stream_resync(&request.stream_id, correlation)
                        .await;
                });
            }
            // Relay 侧 sequence 缺口/重启:重建该流(新 epoch + snapshot,§17.5)。
            Payload::ResyncRequired(required) => {
                let runtime = self.clone();
                tokio::spawn(async move {
                    runtime
                        .handle_stream_resync(&required.stream_id, correlation)
                        .await;
                });
            }
            Payload::QueryRequest(query) => {
                // Desktop 查询可能等待 IPC；独立执行，不能阻塞本连接的心跳和
                // 其他入站消息。
                let runtime = self.clone();
                tokio::spawn(async move {
                    runtime.handle_query(query, correlation).await;
                });
            }
            Payload::CommandRequest(request) => self.handle_command(request, correlation).await,
            Payload::TransferOffer(offer) => {
                transfers::handle_transfer_offer(self, offer, correlation).await;
            }
            // 入站 TransferResult:任一端取消 → 本端联动取消(§22.4)。
            Payload::TransferResult(result) => {
                if result.outcome != pb::TransferOutcome::Completed as i32 {
                    if let Some(token) = self.inner.transfers.lock().get(&result.transfer_id) {
                        token.cancel();
                    }
                }
            }
            Payload::Heartbeat(_) => {
                let ack = pb::envelope::Payload::HeartbeatAck(pb::HeartbeatAck {});
                self.send_connection(&correlation, ack);
            }
            // Ack/others:Bridge 侧无动作(重复 sequence 幂等忽略由 Relay 保证)。
            _ => {}
        }
    }

    async fn handle_subscribe(
        self: &Arc<Self>,
        subscribe: pb::Subscribe,
        upstream_id: Option<String>,
        correlation: String,
    ) {
        match subscribe.target {
            Some(pb::subscribe::Target::List(_)) => {
                self.establish_list_stream(correlation, upstream_id).await;
            }
            Some(pb::subscribe::Target::Session(key)) => {
                // 未知/缺失 agent_kind 显式拒绝,不默认当作 Codex(ZC-02)。
                let Some(key) = pm::session_key_from_proto(&key) else {
                    self.send_protocol_error(
                        &correlation,
                        dm::StableErrorCode::CapabilityUnsupported,
                        "subscribe target has unknown agent kind",
                    );
                    return;
                };
                let slot = SessionSlot::of(&key);
                self.establish_session_stream(&slot, correlation, upstream_id, false)
                    .await;
            }
            None => {
                self.send_protocol_error(
                    &correlation,
                    dm::StableErrorCode::InternalError,
                    "subscribe target missing",
                );
            }
        }
    }

    fn handle_unsubscribe(&self, stream_id: &str) {
        let mut state = self.inner.state.lock();
        if state
            .list_stream
            .as_ref()
            .is_some_and(|s| s.stream_id == stream_id)
        {
            state.list_stream = None;
            return;
        }
        state
            .session_streams
            .retain(|_, s| s.stream_id != stream_id);
    }

    async fn handle_stream_resync(self: &Arc<Self>, stream_id: &str, correlation: String) {
        let target = {
            let state = self.inner.state.lock();
            if state
                .list_stream
                .as_ref()
                .is_some_and(|s| s.stream_id == stream_id)
            {
                Some(None)
            } else {
                // 直接回推流所属槽位(含 agentKind;ZC-02),不再解析 stream_id。
                state
                    .session_streams
                    .values()
                    .find(|s| s.stream_id == stream_id)
                    .map(|s| s.slot.clone())
            }
        };
        match target {
            Some(None) => self.establish_list_stream(correlation, None).await,
            Some(Some(slot)) => {
                self.establish_session_stream(&slot, correlation, None, true)
                    .await
            }
            None => {}
        }
    }

    // -----------------------------------------------------------------
    // 流建立(订阅/重同步共用;新 epoch + 先 snapshot,§17.4/§17.5)
    // -----------------------------------------------------------------

    /// 流建立(订阅/重同步共用;新 epoch + 先 snapshot,§17.4/§17.5)。
    ///
    /// `upstream_id`:Relay 在 Subscribe 上分配的上游流 id;提供时本流后续
    /// 出站(Subscribed/快照/事件)一律回显该 id,Relay 依此路由(§17.4)。
    /// `None`(重连 resync)沿用既有 id;流尚不存在时退回本地约定名。
    async fn establish_list_stream(
        self: &Arc<Self>,
        correlation: String,
        upstream_id: Option<String>,
    ) {
        let (stream_id, epoch) = {
            let mut state = self.inner.state.lock();
            state.epoch_counter += 1;
            let epoch = state.epoch_counter;
            let stream = state.list_stream.get_or_insert_with(|| StreamState {
                stream_id: "list".to_owned(),
                slot: None,
                epoch,
                next_sequence: 1,
                buffering: false,
                buffer: Vec::new(),
            });
            if let Some(id) = upstream_id {
                stream.stream_id = id;
            } else if stream.stream_id.is_empty() {
                stream.stream_id = "list".to_owned();
            }
            stream.epoch = epoch;
            stream.next_sequence = 1;
            stream.buffering = true;
            stream.buffer.clear();
            (stream.stream_id.clone(), epoch)
        };
        self.send_subscribed(&correlation, &stream_id, epoch);

        // 列表快照(§11.1):摘要 + 队列状态覆盖(§10.6;队列正文不出 Bridge)。
        // ZC-02:ZCode 会话摘要(Hook 观察 + pending 注册表)一并纳入列表。
        let page = self
            .inner
            .adapter
            .list_sessions(None, 50, false)
            .await
            .unwrap_or_default();
        let mut summaries = Vec::with_capacity(page.sessions.len());
        for mut summary in page.sessions {
            if let Ok(queue) = self.queue().queue_status(&summary.session_key).await {
                summary.queue_state = queue.state;
            }
            // 列表流只发布 catalog 摘要，不为每个任务建立详情 following。
            // 完整 history snapshot 只在详情订阅/运行态查询时读取，避免一个
            // 大历史任务阻塞其他任务的首屏与控制命令。
            summaries.push(pm::session_summary_to_proto(&summary));
        }
        if let Some(zcode) = self.inner.zcode_hooks.get() {
            for summary in zcode.list_summaries() {
                let key = summary.session_key.clone();
                let mut proto = pm::session_summary_to_proto(&summary);
                if let Ok(queue) = self.queue().queue_status(&key).await {
                    proto.queue_state = pm::queue_state_to_proto(queue.state) as i32;
                }
                summaries.push(proto);
            }
        }
        let batch = pb::SessionSummaryBatch {
            summaries,
            snapshot: true,
        };
        // 锁内:关缓冲、按序发送(snapshot + 缓冲事件)。try_send 非阻塞,
        // 锁内发送保证同流 sequence 与实际发送顺序一致(§17.5)。
        {
            let mut state = self.inner.state.lock();
            let Some(stream) = state.list_stream.as_mut() else {
                return;
            };
            stream.buffering = false;
            let sequence = stream.assign_sequence();
            self.send_stream_locked(
                &correlation,
                &stream.stream_id,
                stream.epoch,
                sequence,
                pb::envelope::Payload::SessionSummaryBatch(batch),
            );
            for event in std::mem::take(&mut stream.buffer) {
                self.flush_list_event_locked(&correlation, stream, event);
            }
        }
        *self.inner.list_refresh_at.lock() =
            Some(std::time::Instant::now() + observe::IDLE_SUMMARY_INTERVAL);
    }

    async fn establish_session_stream(
        self: &Arc<Self>,
        slot: &SessionSlot,
        correlation: String,
        upstream_id: Option<String>,
        force_refresh: bool,
    ) {
        let key = slot.key(&self.inner.device_id);
        self.attach_session(&key).await;

        let (stream_id, epoch) = {
            let mut state = self.inner.state.lock();
            state.epoch_counter += 1;
            let epoch = state.epoch_counter;
            let stream_id = format!("session:{}:{}", slot.0.kind_name(), slot.1);
            let stream = state
                .session_streams
                .entry(slot.clone())
                .or_insert_with(|| StreamState {
                    stream_id: stream_id.clone(),
                    slot: Some(slot.clone()),
                    epoch,
                    next_sequence: 1,
                    buffering: false,
                    buffer: Vec::new(),
                });
            if let Some(id) = upstream_id {
                stream.stream_id = id;
            } else if stream.stream_id.is_empty() {
                stream.stream_id = stream_id.clone();
            }
            stream.epoch = epoch;
            stream.next_sequence = 1;
            stream.buffering = true;
            stream.buffer.clear();
            (stream.stream_id.clone(), epoch)
        };
        self.send_subscribed(&correlation, &stream_id, epoch);

        // RuntimeSnapshot(§11.2):按 agentKind 选择权威来源(ZC-02)。
        // Codex = adapter 投影;ZCode = Hook 观察 + pending 注册表镜像。
        let snapshot_result: Result<dm::RuntimeSnapshot, dm::BridgeError> = match slot.0 {
            dm::AgentKind::CodexDesktop => {
                let mut snapshot = if force_refresh {
                    self.inner.adapter.refresh_snapshot(&key).await
                } else {
                    self.inner.adapter.runtime_snapshot(&key).await
                }
                .map_err(adapter_err);
                if let Ok(snapshot) = snapshot.as_mut() {
                    if let Ok(queue) = self.queue().queue_status(&key).await {
                        snapshot.queue = queue;
                    }
                }
                snapshot
            }
            dm::AgentKind::ZcodeDesktop => match self.inner.zcode_hooks.get() {
                Some(zcode) => {
                    let mut snapshot = zcode.runtime_snapshot(&key);
                    if let Ok(queue) = self.queue().queue_status(&key).await {
                        snapshot.queue = queue;
                    }
                    Ok(snapshot)
                }
                None => Err(dm::BridgeError::new(
                    dm::StableErrorCode::SessionNotFound,
                    "zcode hook path is not attached",
                )),
            },
        };
        match snapshot_result {
            Ok(snapshot) => {
                self.note_snapshot(slot, &snapshot);
                let proto = pm::runtime_snapshot_to_proto(&snapshot);
                let mut state = self.inner.state.lock();
                if let Some(stream) = state.session_streams.get_mut(slot) {
                    stream.buffering = false;
                    let sequence = stream.assign_sequence();
                    self.send_stream_locked(
                        &correlation,
                        &stream.stream_id,
                        stream.epoch,
                        sequence,
                        pb::envelope::Payload::RuntimeSnapshot(proto),
                    );
                    for event in std::mem::take(&mut stream.buffer) {
                        let sequence = stream.assign_sequence();
                        self.send_stream_locked(
                            "",
                            &stream.stream_id,
                            stream.epoch,
                            sequence,
                            pb::envelope::Payload::EventBatch(pb::EventBatch {
                                stream_id: stream.stream_id.clone(),
                                events: vec![event],
                            }),
                        );
                    }
                }
            }
            Err(err) => {
                // 快照不可得:关缓冲并以 ProtocolError 说明(§27.6)。
                tracing::debug!(code = %err.code, "runtime snapshot unavailable for stream");
                self.send_protocol_error(&correlation, err.code, err.to_string());
                let mut state = self.inner.state.lock();
                if let Some(stream) = state.session_streams.get_mut(slot) {
                    stream.buffering = false;
                }
            }
        }
    }

    fn send_subscribed(&self, correlation: &str, stream_id: &str, epoch: u64) {
        let mut envelope = self.base_envelope(correlation);
        envelope.stream_id = stream_id.to_owned();
        envelope.stream_epoch = epoch;
        envelope.sequence = 0;
        envelope.payload = Some(pb::envelope::Payload::Subscribed(pb::Subscribed {
            stream_id: stream_id.to_owned(),
            stream_epoch: epoch,
            base_sequence: 1,
        }));
        self.send_envelope(envelope);
    }

    // -----------------------------------------------------------------
    // adapter 事件接入(pump)
    // -----------------------------------------------------------------

    /// 登记 adapter watcher(每会话一次)并把事件泵入出站流;
    /// UploadLifecycle 与 PowerCoordinator 接同一 adapter 事件流(§14/§19)。
    /// ZC-02:仅 Codex 槽位接 adapter;ZCode 会话为事件驱动(外部事件源),
    /// 只登记缓存,不进 Codex 观察/上传/电源链路。
    pub async fn attach_session(self: &Arc<Self>, key: &dm::SessionKey) {
        let slot = SessionSlot::of(key);
        self.inner
            .sessions
            .lock()
            .entry(slot.clone())
            .or_default();
        if slot.0 != dm::AgentKind::CodexDesktop || self.inner.watchers.lock().contains(&slot) {
            return;
        }
        let native = key.native_session_id.clone();
        let (tx, mut rx) = mpsc::channel::<dm::DomainEvent>(1024);
        if self.inner.adapter.subscribe(key, tx).await.is_err() {
            return;
        }
        self.inner.watchers.lock().insert(slot.clone());
        if self.inner.lifecycle_wired.lock().insert(slot.clone()) {
            let _ = self
                .inner
                .uploads
                .watch_session(&self.inner.adapter, key)
                .await;
            let _ = self
                .inner
                .power
                .watch_session(&self.inner.adapter, key)
                .await;
        }
        let runtime = self.clone();
        let pump_key = key.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                runtime.note_event(&pump_key, &event);
                runtime.dispatch_domain_event(&pump_key, event).await;
            }
            runtime.inner.watchers.lock().remove(&SessionSlot(
                dm::AgentKind::CodexDesktop,
                native,
            ));
        });
    }

    async fn dispatch_domain_event(self: &Arc<Self>, key: &dm::SessionKey, event: dm::DomainEvent) {
        let slot = SessionSlot::of(key);
        // 摘要事件:并入 Bridge 本地队列状态(§10.6)。
        let mut proto_event = match pm::domain_event_to_proto(&event) {
            Some(event) => event,
            None => return, // Opaque item:proto 边界不可表达,跳过(§17.3)。
        };
        if let Some(pb::domain_event::Event::SessionSummaryChanged(ref mut summary)) =
            proto_event.event
        {
            if let Ok(queue) = self.queue().queue_status(key).await {
                summary.queue_state = pm::queue_state_to_proto(queue.state) as i32;
            }
        }
        // 会话详情流(缓冲或直接发送;锁内分配 sequence 并发送,保序 §17.5)。
        let is_summary = matches!(event, dm::DomainEvent::SessionSummaryChanged { .. });
        {
            let mut state = self.inner.state.lock();
            if let Some(stream) = state.session_streams.get_mut(&slot) {
                if stream.buffering {
                    stream.buffer.push(proto_event.clone());
                } else {
                    let sequence = stream.assign_sequence();
                    self.send_stream_locked(
                        "",
                        &stream.stream_id,
                        stream.epoch,
                        sequence,
                        pb::envelope::Payload::EventBatch(pb::EventBatch {
                            stream_id: stream.stream_id.clone(),
                            events: vec![proto_event.clone()],
                        }),
                    );
                }
            }
            // 列表流:只发 SessionSummaryChanged(§17.4)。
            if is_summary {
                if let Some(stream) = state.list_stream.as_mut() {
                    if stream.buffering {
                        stream.buffer.push(proto_event);
                    } else {
                        self.flush_list_event_locked("", stream, proto_event);
                    }
                }
            }
        }
    }

    /// 锁内:把缓冲的 list 事件(SessionSummaryChanged)转成增量 batch 发送。
    fn flush_list_event_locked(
        &self,
        correlation: &str,
        stream: &mut StreamState,
        event: pb::DomainEvent,
    ) {
        let summary = match event.event {
            Some(pb::domain_event::Event::SessionSummaryChanged(summary)) => summary,
            _ => return,
        };
        let sequence = stream.assign_sequence();
        self.send_stream_locked(
            correlation,
            &stream.stream_id,
            stream.epoch,
            sequence,
            pb::envelope::Payload::SessionSummaryBatch(pb::SessionSummaryBatch {
                summaries: vec![summary],
                snapshot: false,
            }),
        );
    }

    // -----------------------------------------------------------------
    // QueryRequest(§11.1 数据接口 4/4;§23.1;§22.2)
    // -----------------------------------------------------------------

    async fn handle_query(self: &Arc<Self>, request: pb::QueryRequest, correlation: String) {
        use pb::query_request::Query;
        let session_key = request
            .session_key
            .as_ref()
            .and_then(pm::session_key_from_proto);
        let result: Result<pb::query_response::Result, dm::BridgeError> = match request.query {
            Some(Query::RuntimeSnapshot(_)) => self.query_runtime_snapshot(session_key).await,
            Some(Query::HistoryPage(page)) => {
                self.query_history_page(session_key, &page.cursor, page.page_size)
                    .await
            }
            Some(Query::CommandOutputPage(page)) => {
                self.query_command_output_page(session_key, &page)
            }
            Some(Query::GitSummary(_)) => self
                .query_git_summary(session_key)
                .await
                .map(pb::query_response::Result::GitSummary),
            Some(Query::GitFileDiff(diff)) => self
                .query_git_file_diff(session_key, &diff)
                .await
                .map(pb::query_response::Result::GitFileDiff),
            Some(Query::FileMetadata(meta)) => self
                .query_file_metadata(session_key, &meta)
                .map(pb::query_response::Result::FileMetadata),
            None => Err(dm::BridgeError::new(
                dm::StableErrorCode::InternalError,
                "query without target",
            )),
        };
        let response = match result {
            Ok(result) => pb::QueryResponse {
                request_id: correlation.clone(),
                result: Some(result),
                error_code: 0,
            },
            Err(err) => {
                tracing::debug!(code = %err.code, operation = "query", "query rejected");
                pb::QueryResponse {
                    request_id: correlation.clone(),
                    result: None,
                    error_code: pm::stable_error_to_proto(err.code) as i32,
                }
            }
        };
        self.send_connection(&correlation, pb::envelope::Payload::QueryResponse(response));
    }

    fn require_key(
        &self,
        session_key: Option<dm::SessionKey>,
    ) -> Result<dm::SessionKey, dm::BridgeError> {
        session_key.ok_or_else(|| {
            dm::BridgeError::new(
                dm::StableErrorCode::InternalError,
                "query requires session key",
            )
        })
    }

    async fn query_runtime_snapshot(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
    ) -> Result<pb::query_response::Result, dm::BridgeError> {
        let key = self.require_key(session_key)?;
        // 按 agentKind 选择权威来源(ZC-02);ZCode 无 Hook 通路时明确拒绝。
        let mut snapshot = match key.agent_kind {
            dm::AgentKind::CodexDesktop => self
                .inner
                .adapter
                .refresh_snapshot(&key)
                .await
                .map_err(adapter_err)?,
            dm::AgentKind::ZcodeDesktop => {
                let zcode = self.inner.zcode_hooks.get().ok_or_else(|| {
                    dm::BridgeError::new(
                        dm::StableErrorCode::SessionNotFound,
                        "zcode hook path is not attached",
                    )
                })?;
                zcode.runtime_snapshot(&key)
            }
        };
        if let Ok(queue) = self.queue().queue_status(&key).await {
            snapshot.queue = queue;
        }
        self.note_snapshot(&SessionSlot::of(&key), &snapshot);
        Ok(pb::query_response::Result::RuntimeSnapshot(
            pm::runtime_snapshot_to_proto(&snapshot),
        ))
    }

    async fn query_history_page(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
        cursor: &str,
        page_size: u32,
    ) -> Result<pb::query_response::Result, dm::BridgeError> {
        let key = self.require_key(session_key)?;
        // ZC-02:ZCode Hook 通路无历史能力,明确 NOT_SUPPORTED,
        // 不以 200 空页伪装(04 §8.9)。
        if key.agent_kind != dm::AgentKind::CodexDesktop {
            return Err(dm::BridgeError::new(
                dm::StableErrorCode::CapabilityUnsupported,
                "history is not available for zcode hook sessions",
            ));
        }
        // §27.5:默认 50、最大 200(0 → 默认)。
        let page_size = if page_size == 0 {
            50
        } else {
            page_size.min(200)
        };
        let page = self
            .inner
            .adapter
            .history_page(&key, non_empty(cursor).as_deref(), page_size)
            .await
            .map_err(adapter_err)?;
        Ok(pb::query_response::Result::HistoryPage(
            pm::history_page_to_proto(&page),
        ))
    }

    /// 完整命令输出分页:数据来自 runtime 内存投影(详情订阅以来累计的
    /// OutputAppend/Replace/Final),按字节 offset 游标分页,单页 ≤256KiB。
    fn query_command_output_page(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
        page: &pb::CommandOutputPageQuery,
    ) -> Result<pb::query_response::Result, dm::BridgeError> {
        let key = self.require_key(session_key)?;
        // ZC-02:ZCode Hook 通路无输出流,明确不支持。
        if key.agent_kind != dm::AgentKind::CodexDesktop {
            return Err(dm::BridgeError::new(
                dm::StableErrorCode::CapabilityUnsupported,
                "command output is not available for zcode hook sessions",
            ));
        }
        let item = page.item_id.as_ref().ok_or_else(|| {
            dm::BridgeError::new(dm::StableErrorCode::InternalError, "missing item id")
        })?;
        let offset: u64 = page.cursor.parse().unwrap_or(0);
        let limit = if page.page_size == 0 {
            OUTPUT_PAGE_MAX
        } else {
            (page.page_size as usize).min(OUTPUT_PAGE_MAX)
        };
        let buffered = {
            let outputs = self.inner.outputs.lock();
            outputs
                .get(&SessionSlot::of(&key))
                .and_then(|items| items.get(&item.id))
                .map(|buffer| {
                    let start = (offset as usize).min(buffer.data.len());
                    let end = start.saturating_add(limit).min(buffer.data.len());
                    (
                        buffer.data[start..end].to_vec(),
                        buffer.data.len(),
                        buffer.is_final,
                    )
                })
        };
        let (chunk, total_len, output_final) = buffered
            .or_else(|| {
                self.inner.adapter.output_page(
                    &key,
                    &dm::ItemId {
                        id: item.id.clone(),
                        synthetic: item.synthetic,
                    },
                    offset as usize,
                    limit,
                )
            })
            .ok_or_else(|| {
                dm::BridgeError::new(
                    dm::StableErrorCode::ResyncRequired,
                    "output unavailable in current authoritative snapshot",
                )
            })?;
        let end = (offset as usize).min(total_len).saturating_add(chunk.len());
        let next_cursor = (end < total_len)
            .then(|| end.to_string())
            .unwrap_or_default();
        Ok(pb::query_response::Result::CommandOutputPage(
            pb::CommandOutputPage {
                item_id: Some(pb::ItemId {
                    id: item.id.clone(),
                    synthetic: item.synthetic,
                }),
                next_cursor,
                bytes: chunk,
                is_final: output_final && end >= total_len,
                channel: pb::OutputChannel::Combined as i32,
            },
        ))
    }

    async fn query_git_summary(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
    ) -> Result<pb::GitSummaryData, dm::BridgeError> {
        let (git, _key, roots, cwd) = self.git_inputs(session_key).await?;
        let summary = git.read_summary(&roots, &cwd).await.map_err(git_err)?;
        debug_assert!(
            summary.entries_all_relative(),
            "git 条目必须为相对路径(§23.1)"
        );
        Ok(pb::GitSummaryData {
            branch: summary.branch.clone().unwrap_or_default(),
            detached_head: summary.detached_head,
            head_short: summary.head_short.clone().unwrap_or_default(),
            head_full: summary.head_full.clone().unwrap_or_default(),
            root_display_name: summary
                .root
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            entries: git_entries(&summary),
            insertions: summary.total_added_lines as i64,
            deletions: summary.total_deleted_lines as i64,
            binary_files: summary.binary_files.clone(),
        })
    }

    async fn query_git_file_diff(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
        diff: &pb::GitFileDiffQuery,
    ) -> Result<pb::GitFileDiffData, dm::BridgeError> {
        let (git, _key, roots, cwd) = self.git_inputs(session_key).await?;
        let result = git
            .read_file_diff(&roots, &cwd, &diff.relative_path, diff.staged)
            .await
            .map_err(git_err)?;
        let binary = std::str::from_utf8(&result.content).is_err();
        Ok(pb::GitFileDiffData {
            relative_path: result.relative_path,
            staged: result.staged,
            patch_text: if binary {
                String::new()
            } else {
                String::from_utf8_lossy(&result.content).into_owned()
            },
            truncated: result.truncated,
            total_bytes: result.content.len() as u64,
            binary,
        })
    }

    /// Git 查询公共输入:git 服务、授权根(本地库)与会话 cwd(仅本机使用,
    /// §23.1;cwd 绝不进响应)。
    async fn git_inputs(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
    ) -> Result<
        (
            Arc<GitService>,
            dm::SessionKey,
            Vec<std::path::PathBuf>,
            std::path::PathBuf,
        ),
        dm::BridgeError,
    > {
        let key = self.require_key(session_key)?;
        // ZC-02:Git 只读能力仅对 Codex 会话;ZCode 无 cwd 投影,明确拒绝,
        // 不允许同 native id 时误读 Codex 会话的工作区。
        if key.agent_kind != dm::AgentKind::CodexDesktop {
            return Err(dm::BridgeError::new(
                dm::StableErrorCode::SessionNotFound,
                "git data is not available for this agent kind",
            ));
        }
        let git = self.inner.git.clone().ok_or_else(|| {
            dm::BridgeError::new(
                dm::StableErrorCode::InternalError,
                "git executable unavailable",
            )
        })?;
        let cwd = self.inner.adapter.session_cwd(&key).ok_or_else(|| {
            dm::BridgeError::new(
                dm::StableErrorCode::SessionNotFound,
                "session cwd unknown (no runtime snapshot)",
            )
        })?;
        let roots = self
            .inner
            .store
            .list_workspaces()
            .await
            .map_err(store_err)?
            .into_iter()
            .map(|ws| ws.root_path)
            .collect();
        Ok((git, key, roots, cwd))
    }

    /// file_metadata(§22.2/§22.3):复验 handle → 嗅探分类;不可预览返回
    /// 原因(允许下载),handle 失效/变更返回稳定错误码。
    fn query_file_metadata(
        self: &Arc<Self>,
        session_key: Option<dm::SessionKey>,
        meta: &pb::FileMetadataQuery,
    ) -> Result<pb::FileMetadataData, dm::BridgeError> {
        let key = self.require_key(session_key)?;
        // 文件授权按原生会话 ID 绑定;非 Codex 会话不进入文件面(ZC-02,
        // 不复制 ZCode 专用版本),避免同 native id 时误用 Codex 授权。
        if key.agent_kind != dm::AgentKind::CodexDesktop {
            return Err(dm::BridgeError::new(
                dm::StableErrorCode::FileOutsideScope,
                "file transfers are not available for this agent kind",
            ));
        }
        let verified = self
            .inner
            .grants
            .resolve_for_read(
                &self.inner.device_id,
                &key.native_session_id,
                &meta.file_handle,
                GrantAction::Preview,
            )
            .or_else(|_| {
                self.inner.grants.resolve_for_read(
                    &self.inner.device_id,
                    &key.native_session_id,
                    &meta.file_handle,
                    GrantAction::Download,
                )
            })
            .map_err(files_err)?;
        let mut file = verified.file;
        let want = crate::files::SNIFF_PREFIX_BYTES.min(verified.size as usize);
        let mut prefix = vec![0u8; want];
        {
            use std::io::Read;
            file.read_exact(&mut prefix).map_err(|e| {
                dm::BridgeError::new(dm::StableErrorCode::InternalError, e.to_string())
            })?;
        }
        let classification = crate::files::classify(&prefix);
        let preview = crate::files::check_preview(&classification, verified.size);
        let preview_kind = if preview.is_err() {
            "none"
        } else {
            match classification {
                crate::files::Classification::InlineText => "text",
                crate::files::Classification::InlineImage(_) => "image",
                crate::files::Classification::InlinePdf => "pdf",
                crate::files::Classification::Attachment(_) => "none",
            }
        };
        let not_previewable_reason = if preview.is_err() {
            dm::StableErrorCode::FileTypeNotPreviewable
                .as_str()
                .to_string()
        } else {
            String::new()
        };
        let display_name = verified
            .handle
            .relative_path
            .rsplit('/')
            .next()
            .unwrap_or(&verified.handle.relative_path)
            .to_string();
        Ok(pb::FileMetadataData {
            display_name,
            mime_type: classification.content_type().to_string(),
            size_bytes: verified.size,
            preview_kind: preview_kind.to_string(),
            not_previewable_reason,
            file_handle: verified.handle.token.clone(),
        })
    }

    // -----------------------------------------------------------------
    // CommandRequest(§15)
    // -----------------------------------------------------------------

    async fn handle_command(self: &Arc<Self>, request: pb::CommandRequest, correlation: String) {
        // ZCode Hook 审批/问答分派(ZC-01 原型):approval/question id 命中
        // 本机 pending 注册表时由 Bridge 原子决定并直达等待中的 helper,
        // 不经 Codex adapter 链路(能力 gate 与快照均属 Codex 语义)。
        // 未命中则保持原 Codex 路径,行为零变化。
        if let Some(zcode) = self.inner.zcode_hooks.get() {
            // P1-6:分派前核对设备/agentKind/nativeSessionId 与登记绑定一致。
            if zcode.matches_command(request.session_key.as_ref(), request.payload.as_ref()) {
                let resolved = zcode.resolve_command(request.payload.as_ref()).await;
                self.send_connection(
                    &correlation,
                    pb::envelope::Payload::CommandAccepted(pb::CommandAccepted {
                        request_id: request.request_id.clone(),
                        status: pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32,
                        accepted_at: Some(now_timestamp()),
                    }),
                );
                match resolved {
                    Some(Ok((invoke_id, _reply))) => {
                        // R2-ZC01:oneshot 成功只表示 Bridge 接受了决定。
                        // ReceiptCompleted 在该路径只表示「已确认输出原生协议
                        // 结果」(helper stdout / MCP JSON-RPC 实际写出后的
                        // 交付确认);写回失败、确认缺失或超时一律
                        // OUTCOME_UNKNOWN —— 不报成功、不自动重新投递。
                        let status = match zcode.registry().take_delivery(&invoke_id) {
                            Some(mut delivery) => {
                                // 有限等待:略大于 socket 侧 ack 等待窗口。
                                let wait = std::time::Duration::from_millis(
                                    crate::zcode::contract::DELIVERY_ACK_WAIT_MS + 2_000,
                                );
                                match tokio::time::timeout(wait, &mut delivery).await {
                                    Ok(Ok(crate::zcode::pending::DeliveryOutcome::Delivered)) => {
                                        pb::CommandReceiptStatus::ReceiptCompleted
                                    }
                                    _ => pb::CommandReceiptStatus::ReceiptOutcomeUnknown,
                                }
                            }
                            // 记录已不在/通道已取走:结果未知。
                            None => pb::CommandReceiptStatus::ReceiptOutcomeUnknown,
                        };
                        self.send_command_result(&correlation, &request.request_id, status, None);
                    }
                    Some(Err(err)) => {
                        self.send_command_result(
                            &correlation,
                            &request.request_id,
                            pb::CommandReceiptStatus::ReceiptRejected,
                            Some(pm::stable_error_to_proto(err.code)),
                        );
                    }
                    // resolve_command 只在有命中时返回 Some;命中后不会为 None。
                    None => {
                        self.send_command_result(
                            &correlation,
                            &request.request_id,
                            pb::CommandReceiptStatus::ReceiptRejected,
                            Some(pm::stable_error_to_proto(dm::StableErrorCode::InternalError)),
                        );
                    }
                }
                return;
            }
        }
        let caps = self.inner.gateway.current_capabilities();
        let domain_request = match pm::command_request_from_proto(&request, &caps) {
            Ok(request) => request,
            Err(err) => {
                self.send_command_result(
                    &correlation,
                    &request.request_id,
                    pb::CommandReceiptStatus::ReceiptRejected,
                    Some(pm::stable_error_to_proto(err.code)),
                );
                return;
            }
        };
        // ZC-02 正式路由:ZCode 会话只支持 Hook 审批/问答决定(上方按
        // invoke_id 命中注册表处理);未命中(已过期/未知)或其余操作明确
        // 拒绝,绝不落入 Codex 链路。
        if domain_request.session_key.agent_kind == dm::AgentKind::ZcodeDesktop {
            let is_answer = matches!(
                request.payload,
                Some(pb::command_request::Payload::AnswerApproval(_))
                    | Some(pb::command_request::Payload::AnswerQuestion(_))
            );
            let (code, message) = if is_answer {
                (
                    dm::StableErrorCode::ApprovalExpired,
                    "zcode decision window has closed (unknown or expired invoke)",
                )
            } else {
                (
                    dm::StableErrorCode::CapabilityUnsupported,
                    "operation not supported for zcode hook sessions",
                )
            };
            self.send_command_result(
                &correlation,
                &request.request_id,
                pb::CommandReceiptStatus::ReceiptRejected,
                Some(pm::stable_error_to_proto(code)),
            );
            tracing::debug!(code = %code, message, "zcode session command rejected");
            return;
        }
        let request_id = domain_request.request_id;
        // §15.3 队列替换:set/replace 语义已随 payload 进入 gateway——先
        // requestId 去重与快照校验,确认可替换后再单行事务更新同一条队列
        // 记录;校验不过或写失败时旧队列保留。此处绝不预取消(旧链路的
        // "先删再排队"会在新请求被拒时丢掉仍有价值的旧队列)。
        match self.inner.gateway.submit(domain_request).await {
            Ok(Submission::Accepted {
                accepted_at,
                receipts,
            }) => {
                let accepted = pb::CommandAccepted {
                    request_id: request_id.to_string(),
                    status: pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32,
                    accepted_at: Some(pm::timestamp_to_proto(accepted_at)),
                };
                self.send_connection(
                    &correlation,
                    pb::envelope::Payload::CommandAccepted(accepted),
                );
                // 回执流 → CommandResult(§15.2);出站失败即丢弃(§26.4:
                // 不自动重试,Browser 以同 request_id 重试时由 gateway 重放)。
                let runtime = self.clone();
                tokio::spawn(async move {
                    let mut receipts = receipts;
                    while let Some(receipt) = receipts.recv().await {
                        let (status, error_code) = pm::receipt_state_to_status(&receipt.state);
                        runtime.send_command_result(
                            &correlation,
                            &receipt.request_id.to_string(),
                            status,
                            error_code,
                        );
                        if matches!(
                            receipt.state,
                            dm::ReceiptState::Completed
                                | dm::ReceiptState::Rejected { .. }
                                | dm::ReceiptState::OutcomeUnknown
                        ) {
                            break;
                        }
                    }
                });
            }
            Ok(Submission::Replayed { request_id, state }) => {
                // 同 ID 同 payload 重试:重放既有回执,不重复执行(§15.2)。
                let accepted = pb::CommandAccepted {
                    request_id: request_id.to_string(),
                    status: pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32,
                    accepted_at: Some(now_timestamp()),
                };
                self.send_connection(
                    &correlation,
                    pb::envelope::Payload::CommandAccepted(accepted),
                );
                self.send_command_result(
                    &correlation,
                    &request_id.to_string(),
                    pm::stored_state_to_status(state),
                    None,
                );
            }
            Err(err) => {
                self.send_command_result(
                    &correlation,
                    &request_id.to_string(),
                    pb::CommandReceiptStatus::ReceiptRejected,
                    Some(pm::stable_error_to_proto(err.code)),
                );
            }
        }
    }

    fn send_command_result(
        &self,
        correlation: &str,
        request_id: &str,
        status: pb::CommandReceiptStatus,
        error_code: Option<pb::StableErrorCode>,
    ) {
        let result = pb::CommandResult {
            request_id: request_id.to_owned(),
            status: status as i32,
            error_code: error_code.map(|c| c as i32).unwrap_or(0),
            details: Default::default(),
            duration_ms: None,
        };
        self.send_connection(correlation, pb::envelope::Payload::CommandResult(result));
    }

    fn send_protocol_error(
        &self,
        correlation: &str,
        code: dm::StableErrorCode,
        message: impl Into<String>,
    ) {
        let error = pb::ProtocolError {
            error_code: pm::stable_error_to_proto(code) as i32,
            message: message.into(),
            request_id: String::new(),
            stream_id: String::new(),
            details: Default::default(),
        };
        self.send_connection(correlation, pb::envelope::Payload::ProtocolError(error));
    }

    // -----------------------------------------------------------------
    // 观察回调(observe.rs 调用;全部为缓存/轻量操作)
    // -----------------------------------------------------------------

    /// 已登记观察的 Codex 会话 ID(§14 观察循环只轮询 Codex adapter 投影;
    /// ZCode 会话为事件驱动,不轮询)。
    pub(crate) fn known_session_ids(&self) -> Vec<String> {
        let sessions = self.inner.sessions.lock();
        let mut ids: Vec<String> = sessions
            .keys()
            .filter(|slot| slot.0 == dm::AgentKind::CodexDesktop)
            .map(|slot| slot.1.clone())
            .collect();
        ids.sort();
        ids
    }

    pub(crate) fn observe_pressure(&self) -> ObservePressure {
        let battery = *self.inner.battery_powered.lock();
        let active = self
            .inner
            .sessions
            .lock()
            .values()
            .filter(|c| c.turn_active)
            .count();
        ObservePressure {
            battery_powered: battery,
            active_tasks: active,
        }
    }

    pub(crate) fn observe_state_of(&self, native: &str) -> Option<observe::SessionObserveState> {
        let slot = SessionSlot(dm::AgentKind::CodexDesktop, native.to_string());
        let cache = *self.inner.sessions.lock().get(&slot)?;
        let state = self.inner.state.lock();
        Some(observe::SessionObserveState {
            has_detail_subscription: state.session_streams.contains_key(&slot),
            has_list_subscription: state.list_stream.is_some(),
            turn_active: cache.turn_active,
        })
    }

    pub(crate) async fn observe_stopped(&self) {
        self.inner.stop.cancelled().await;
    }

    /// 单次轻量观察(§14;仅 Codex 会话):投影快照(不读完整历史)→
    /// 差分缓存 → 详情流低频确认 / idle 列表摘要刷新 / 电源状态推进。
    pub(crate) async fn observe_session(self: &Arc<Self>, native: &str) {
        let slot = SessionSlot(dm::AgentKind::CodexDesktop, native.to_string());
        let key = slot.key(&self.inner.device_id);
        // 只对已有投影的会话观察:避免为未跟随会话触发 adapter 的快照等待。
        if !self
            .inner
            .sessions
            .lock()
            .get(&slot)
            .copied()
            .unwrap_or_default()
            .has_snapshot
        {
            return;
        }
        let Ok(mut snapshot) = self.inner.adapter.runtime_snapshot(&key).await else {
            return;
        };
        if let Ok(queue) = self.queue().queue_status(&key).await {
            snapshot.queue = queue;
        }
        let previous = self.note_snapshot(&slot, &snapshot);
        let changed = previous.map(|p| p.runtime_revision) != Some(snapshot.runtime_revision);

        // 详情订阅:revision 变化 → 低频确认快照(§14 snapshot 只作补偿/确认)。
        {
            let mut state = self.inner.state.lock();
            if let Some(stream) = state.session_streams.get_mut(&slot) {
                if !stream.buffering && changed {
                    let sequence = stream.assign_sequence();
                    self.send_stream_locked(
                        "",
                        &stream.stream_id,
                        stream.epoch,
                        sequence,
                        pb::envelope::Payload::RuntimeSnapshot(pm::runtime_snapshot_to_proto(
                            &snapshot,
                        )),
                    );
                }
            }
        }
        // idle + 列表订阅:摘要级刷新(起点约 30s,受压力降频)。
        if snapshot.current_turn.is_none() {
            let due = {
                let mut deadline = self.inner.list_refresh_at.lock();
                match *deadline {
                    Some(at) if at <= std::time::Instant::now() => {
                        *deadline = Some(
                            std::time::Instant::now()
                                + observe::next_interval(
                                    observe::SessionObserveState {
                                        has_detail_subscription: false,
                                        has_list_subscription: true,
                                        turn_active: false,
                                    },
                                    self.observe_pressure(),
                                )
                                .unwrap_or(observe::IDLE_SUMMARY_INTERVAL),
                        );
                        true
                    }
                    _ => false,
                }
            };
            if due {
                self.refresh_list_summary().await;
            }
        }
        self.update_power_state().await;
    }

    async fn refresh_list_summary(self: &Arc<Self>) {
        let (stream_id, epoch) = {
            let state = self.inner.state.lock();
            match &state.list_stream {
                Some(stream) => (stream.stream_id.clone(), stream.epoch),
                None => return,
            }
        };
        let Ok(page) = self.inner.adapter.list_sessions(None, 50, false).await else {
            return;
        };
        let mut summaries = Vec::with_capacity(page.sessions.len());
        for mut summary in page.sessions {
            if let Ok(queue) = self.queue().queue_status(&summary.session_key).await {
                summary.queue_state = queue.state;
            }
            summaries.push(pm::session_summary_to_proto(&summary));
        }
        // 全量快照必须包含 ZCode 会话,否则会把它们从列表中"刷掉"(ZC-02)。
        if let Some(zcode) = self.inner.zcode_hooks.get() {
            for summary in zcode.list_summaries() {
                let key = summary.session_key.clone();
                let mut proto = pm::session_summary_to_proto(&summary);
                if let Ok(queue) = self.queue().queue_status(&key).await {
                    proto.queue_state = pm::queue_state_to_proto(queue.state) as i32;
                }
                summaries.push(proto);
            }
        }
        let batch = pb::SessionSummaryBatch {
            summaries,
            snapshot: true,
        };
        let mut state = self.inner.state.lock();
        if let Some(stream) = state.list_stream.as_mut() {
            // 流未在快照建立中且纪元未变才刷新(重连重建期间不发旧数据)。
            if stream.stream_id == stream_id && stream.epoch == epoch && !stream.buffering {
                let sequence = stream.assign_sequence();
                self.send_stream_locked(
                    "",
                    &stream.stream_id,
                    stream.epoch,
                    sequence,
                    pb::envelope::Payload::SessionSummaryBatch(batch),
                );
            }
        }
    }

    /// 聚合会话缓存 → 电源断言输入(§19:turn 活跃或 attention 挂起时保持)。
    async fn update_power_state(&self) {
        let (turn_active, attention_pending) = {
            let sessions = self.inner.sessions.lock();
            (
                sessions.values().any(|c| c.turn_active),
                sessions.values().any(|c| c.attention_count > 0),
            )
        };
        let _ = self.inner.power.set_turn_active(turn_active).await;
        let _ = self
            .inner
            .power
            .set_attention_pending(attention_pending)
            .await;
    }

    // -----------------------------------------------------------------
    // 事件缓存(观察差分 + 输出缓冲;只存稳定维度与输出正文)
    // -----------------------------------------------------------------

    fn note_event(&self, key: &dm::SessionKey, event: &dm::DomainEvent) {
        let slot = SessionSlot::of(key);
        {
            let mut sessions = self.inner.sessions.lock();
            let cache = sessions.entry(slot.clone()).or_default();
            match event {
                dm::DomainEvent::TurnLifecycle { phase, .. } => {
                    cache.turn_active = matches!(
                        phase,
                        dm::ActiveTurnPhase::Running | dm::ActiveTurnPhase::Finishing
                    );
                }
                dm::DomainEvent::PendingAttentionAdded { .. } => cache.attention_count += 1,
                dm::DomainEvent::PendingAttentionRemoved { .. } => {
                    cache.attention_count = cache.attention_count.saturating_sub(1)
                }
                _ => {}
            }
        }
        // 输出缓冲(command_output_page 数据源;§26.4 有界)。
        match event {
            dm::DomainEvent::OutputAppend {
                item_id,
                expected_offset,
                bytes,
                ..
            } => {
                let mut outputs = self.inner.outputs.lock();
                let buffer = outputs
                    .entry(slot)
                    .or_default()
                    .entry(item_id.id.clone())
                    .or_default();
                let expected = *expected_offset as usize;
                // offset 与本地长度一致才拼接;不一致不盲目拼(§13.2),
                // 等待 OutputReplace 权威校正。
                if expected == buffer.data.len() && buffer.data.len() < OUTPUT_BUFFER_MAX {
                    let take = (OUTPUT_BUFFER_MAX - buffer.data.len()).min(bytes.len());
                    buffer.data.extend_from_slice(&bytes.as_bytes()[..take]);
                }
            }
            dm::DomainEvent::OutputReplace { item_id, bytes, .. } => {
                let mut outputs = self.inner.outputs.lock();
                let buffer = outputs
                    .entry(slot)
                    .or_default()
                    .entry(item_id.id.clone())
                    .or_default();
                let take = bytes.len().min(OUTPUT_BUFFER_MAX);
                buffer.data = bytes.as_bytes()[..take].to_vec();
            }
            dm::DomainEvent::OutputFinal { item_id, .. } => {
                let mut outputs = self.inner.outputs.lock();
                if let Some(buffer) = outputs
                    .get_mut(&slot)
                    .and_then(|items| items.get_mut(&item_id.id))
                {
                    buffer.is_final = true;
                }
            }
            _ => {}
        }
    }

    /// 记录快照缓存;返回先前缓存(观察差分用)。
    fn note_snapshot(&self, slot: &SessionSlot, snapshot: &dm::RuntimeSnapshot) -> Option<SessionCache> {
        let mut sessions = self.inner.sessions.lock();
        let cache = sessions.entry(slot.clone()).or_default();
        let previous = *cache;
        cache.runtime_revision = snapshot.runtime_revision;
        cache.has_snapshot = true;
        cache.turn_active = snapshot
            .current_turn
            .as_ref()
            .is_some_and(|t| t.phase != dm::ActiveTurnPhase::Idle);
        cache.attention_count = snapshot.pending_questions.len() + snapshot.pending_approvals.len();
        Some(previous)
    }

    // -----------------------------------------------------------------
    // 出站
    // -----------------------------------------------------------------

    fn base_envelope(&self, correlation: &str) -> pb::Envelope {
        pb::Envelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: new_message_id(),
            correlation_id: correlation.to_owned(),
            sent_at: Some(now_timestamp()),
            device_id: self.inner.device_id.clone(),
            agent_kind: pb::AgentKind::CodexDesktop as i32,
            stream_id: String::new(),
            stream_epoch: 0,
            sequence: 0,
            payload: None,
            provider_extension: None,
        }
    }

    /// 连接级(无流)出站。
    fn send_connection(&self, correlation: &str, payload: pb::envelope::Payload) {
        let mut envelope = self.base_envelope(correlation);
        envelope.payload = Some(payload);
        self.send_envelope(envelope);
    }

    /// 流内出站(锁内调用:try_send 非阻塞,保证 sequence 与发送顺序一致)。
    fn send_stream_locked(
        &self,
        correlation: &str,
        stream_id: &str,
        epoch: u64,
        sequence: u64,
        payload: pb::envelope::Payload,
    ) {
        let mut envelope = self.base_envelope(correlation);
        envelope.stream_id = stream_id.to_owned();
        envelope.stream_epoch = epoch;
        envelope.sequence = sequence;
        envelope.payload = Some(payload);
        self.send_envelope(envelope);
    }

    fn send_envelope(&self, envelope: pb::Envelope) {
        // §26.4:出站失败(队列满/停止)→ 丢弃未确认事件,不阻塞、不重试;
        // 只记录 payload 变体名(§25.3 日志白名单)。
        if let Err(err) = self.inner.outbound.send_envelope(&envelope) {
            let kind = envelope
                .payload
                .as_ref()
                .map(payload_kind)
                .unwrap_or("none");
            tracing::debug!(kind, error = %err, "outbound envelope dropped");
        }
    }
}

// ---------------------------------------------------------------------------
// StreamState / 辅助
// ---------------------------------------------------------------------------

impl StreamState {
    fn assign_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        sequence
    }
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn adapter_err(err: crate::adapter::codex::AdapterError) -> dm::BridgeError {
    dm::BridgeError::new(err.code(), err.to_string())
}

fn store_err(err: crate::local_store::StoreError) -> dm::BridgeError {
    dm::BridgeError::new(dm::StableErrorCode::InternalError, err.to_string())
}

fn git_err(err: crate::git::GitError) -> dm::BridgeError {
    dm::BridgeError::new(stable_code_from_str(err.stable_code()), err.to_string())
}

fn files_err(err: crate::files::FilesError) -> dm::BridgeError {
    dm::BridgeError::new(stable_code_from_str(err.stable_code()), err.to_string())
}

/// 稳定码字符串 → 枚举(files/git 子系统以字符串暴露稳定码)。
fn stable_code_from_str(code: &str) -> dm::StableErrorCode {
    use dm::StableErrorCode as E;
    match code {
        "FILE_HANDLE_INVALID" => E::FileHandleInvalid,
        "FILE_OUTSIDE_SCOPE" => E::FileOutsideScope,
        "FILE_CHANGED" => E::FileChanged,
        "FILE_TYPE_NOT_PREVIEWABLE" => E::FileTypeNotPreviewable,
        "TRANSFER_EXPIRED" => E::TransferExpired,
        "TRANSFER_TOO_LARGE" => E::TransferTooLarge,
        "TRANSFER_RANGE_INVALID" => E::TransferRangeInvalid,
        "RATE_LIMITED" => E::RateLimited,
        "DIFF_TOO_LARGE" => E::DiffTooLarge,
        _ => E::InternalError,
    }
}

/// GitSummary → GitStatusEntry(§23.1 状态词:staged/unstaged/untracked/
/// added/modified/deleted/renamed)。
fn git_entries(summary: &crate::git::GitSummary) -> Vec<pb::GitStatusEntry> {
    let mut entries = Vec::new();
    for e in &summary.untracked {
        entries.push(pb::GitStatusEntry {
            relative_path: e.path.clone(),
            status: "untracked".to_owned(),
            staged: false,
        });
    }
    for e in &summary.staged {
        let status = match e.index_status {
            'A' => "added",
            'M' => "modified",
            'D' => "deleted",
            'R' | 'C' => "renamed",
            _ => "staged",
        };
        entries.push(pb::GitStatusEntry {
            relative_path: e.path.clone(),
            status: status.to_owned(),
            staged: true,
        });
    }
    for e in &summary.unstaged {
        let status = match e.worktree_status {
            'M' => "modified",
            'D' => "deleted",
            _ => "unstaged",
        };
        entries.push(pb::GitStatusEntry {
            relative_path: e.path.clone(),
            status: status.to_owned(),
            staged: false,
        });
    }
    entries
}

fn now_timestamp() -> Timestamp {
    let now = chrono::Utc::now();
    Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    }
}

// ---------------------------------------------------------------------------
// 上传清理的生产实现(§22.5;包装 files::upload,只删 Bridge 临时目录)
// ---------------------------------------------------------------------------

/// [`UploadCleaner`] 生产实现:包装 `files::upload` 清理入口。
/// `directory` 必须是上传根下的单级目录名(拒绝分隔符与 `..`)。
#[derive(Debug, Clone)]
pub struct FsUploadCleaner {
    root: std::path::PathBuf,
}

impl FsUploadCleaner {
    pub fn new(root: std::path::PathBuf) -> Self {
        Self { root }
    }
}

#[async_trait::async_trait]
impl UploadCleaner for FsUploadCleaner {
    async fn remove_upload_dir(&self, directory: &str) -> std::io::Result<()> {
        if directory.is_empty()
            || directory.contains('/')
            || directory.contains('\\')
            || directory.contains("..")
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid upload directory name",
            ));
        }
        crate::files::upload::remove_upload_dir(&crate::files::upload::UploadCleanup {
            dir: self.root.join(directory),
            expires_at: std::time::SystemTime::UNIX_EPOCH,
        })
        .await
    }

    async fn sweep_expired(
        &self,
        older_than: std::time::SystemTime,
    ) -> std::io::Result<Vec<String>> {
        // 与 files::upload::sweep_expired_uploads 同规则,但返回被删目录的
        // 相对名(清理记录 §22.5;目录名为相对 data_dir 名,不含绝对路径)。
        let mut removed = Vec::new();
        let mut reader = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = reader.next_entry().await? {
            let md = entry.metadata().await?;
            if !md.is_dir() || md.modified()? >= older_than {
                continue;
            }
            tokio::fs::remove_dir_all(entry.path()).await?;
            removed.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(removed)
    }
}
