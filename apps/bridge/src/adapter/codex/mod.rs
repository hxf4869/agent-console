//! Codex Adapter(权威规格 §12):Desktop IPC + SQLite 只读目录 + 投影映射
//! + 能力探针的公共入口。
//!
//! 消费方:runtime / commands 工作流(并发多任务 tokio 环境)。调用约定:
//! - [`CodexAdapter::connect`] 建立连接并完成能力 probe;IPC 不可用时降级为
//!   catalog-only 只读,不 panic(§12:连接失败不得终止/重启 Desktop)。
//! - **attach 生命周期**:首次未建立(如 Bridge 先于 Desktop 启动)或中途
//!   断开的 IPC,由 [`attach_supervisor`] 按 1s/2s/5s/10s→30s 退避低频重试;
//!   成功后原子替换内部 IPC 句柄与 pump(代次递增、旧任务回收),并重新
//!   探测版本/重估能力/重放 following。外部持有的 Adapter 句柄保持稳定,
//!   同一原生事件只经单一 pump 代次分发(§12/§5:未知版本保持只读)。
//! - [`CodexAdapter::list_sessions`] / [`CodexAdapter::runtime_snapshot`] /
//!   [`CodexAdapter::subscribe`] / [`CodexAdapter::execute_command`] 是
//!   runtime 工作流的唯一入口;原生 IPC/SQL 数据不出本模块(§12)。
//! - 写命令按 capability gate:READ_ONLY → `CONTROL_READ_ONLY`,无原生方法
//!   的操作 → `CAPABILITY_UNSUPPORTED`(§5/§12)。
//!
//! 下一轮队列(§15.3)的正文与持久化归 commands 工作流(Bridge SQLite);
//! 本模块只执行 start-turn 的发送路径,队列命令返回
//! `CAPABILITY_UNSUPPORTED`(见 execute_command 注释)。

pub mod catalog;
pub mod fake_owner;
pub mod ipc;
pub mod mapper;
pub mod projection;

pub use catalog::{
    fixture as catalog_fixture, CatalogCapabilities, CatalogConfig, CatalogError, CatalogThread,
    CodexCatalog,
};
pub use mapper::{OutputMergePolicy, PatchOutcome, SessionMapper};
pub use projection::{apply_immer_patches, PatchError};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::capabilities::{probe, CapabilityProbeInput, WriteMethodProbes};
use crate::domain::{
    CapabilitySet, CommandPayload, CommandReceipt, CommandRequest, DomainEvent, ItemId, Operation,
    OutputText, ReceiptState, RuntimeSnapshot, SessionKey, SessionSummary, StableErrorCode,
};

use ipc::client::{connect, ConnectionState, IpcClient, IpcClientConfig, IpcError, IpcEvent};
use ipc::messages::{
    FollowingChangedParams, InputBlock, InterruptMode, StartTurnParams, SteerTurnParams, TurnStart,
    TurnStartRequest,
};

/// 连接默认 hostId(本地 Desktop)。
pub const LOCAL_HOST_ID: &str = "local";
/// runtime_snapshot 首次订阅等待快照的超时。
pub const SNAPSHOT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("ipc: {0}")]
    Ipc(String),
    #[error("catalog: {0}")]
    Catalog(String),
    #[error("{code}: {message}")]
    Stable {
        code: StableErrorCode,
        message: String,
    },
}

impl AdapterError {
    pub fn code(&self) -> StableErrorCode {
        match self {
            AdapterError::Stable { code, .. } => *code,
            AdapterError::Ipc(_) | AdapterError::Catalog(_) => StableErrorCode::CodexUnavailable,
        }
    }

    fn stable(code: StableErrorCode, message: impl Into<String>) -> Self {
        AdapterError::Stable {
            code,
            message: message.into(),
        }
    }
}

impl From<IpcError> for AdapterError {
    fn from(value: IpcError) -> Self {
        AdapterError::Ipc(value.to_string())
    }
}

/// 会话列表页(领域摘要形态,§11.1)。
#[derive(Debug, Clone, Default)]
pub struct SessionPage {
    pub sessions: Vec<SessionSummary>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// 发现报告(`discover()`;doctor/兼容文档用,无会话内容)。
#[derive(Debug, Clone, Default)]
pub struct DiscoveryReport {
    pub socket_path: Option<PathBuf>,
    pub ipc_connected: bool,
    pub client_id: Option<String>,
    pub catalog: Option<CatalogCapabilities>,
    pub codex_version: Option<String>,
}

/// Adapter 配置。
#[derive(Debug, Clone)]
pub struct CodexAdapterConfig {
    /// 设备 ID(设备绑定的内部 ID;进入 SessionKey,§9.1)。
    pub device_id: String,
    /// IPC socket 覆盖路径;None → discovery 默认探测。
    pub ipc_socket: Option<PathBuf>,
    /// Codex home 覆盖;None → 默认 `~/.codex`。
    pub codex_home: Option<PathBuf>,
    /// 版本报告注入(None 时不探测 CLI,保持测试确定性;
    /// 生产路径由调用方先跑 `ipc::discovery::probe_version`)。
    pub version_report: Option<String>,
    /// attach/重新 attach 时 `codex --version` 探测所用二进制
    /// (None:保持 version_report 冻结,不重新探测)。
    pub version_binary: Option<PathBuf>,
    /// 写方法 probe 注入(真实 Desktop 无法无副作用 probe;
    /// 未知/未 probe 一律只读,§5)。
    pub write_method_probes: WriteMethodProbes,
    /// 无 catalog 时的静态会话种子(fake owner / e2e 使用;
    /// 生产环境留空,列表来自 SQLite 目录)。
    pub static_sessions: Vec<CatalogThread>,
    /// 目录查询配置;None → 默认。
    pub catalog: Option<CatalogConfig>,
}

impl CodexAdapterConfig {
    pub fn new(device_id: impl Into<String>) -> Self {
        Self {
            device_id: device_id.into(),
            ipc_socket: None,
            codex_home: None,
            version_report: None,
            version_binary: None,
            write_method_probes: WriteMethodProbes::default(),
            static_sessions: Vec::new(),
            catalog: None,
        }
    }
}

/// 单会话运行时状态。
struct SessionRuntime {
    key: SessionKey,
    mapper: SessionMapper,
    watchers: Vec<mpsc::Sender<DomainEvent>>,
    following: bool,
    owner_client_id: Option<String>,
}

impl SessionRuntime {
    fn new(key: SessionKey) -> Self {
        Self {
            mapper: SessionMapper::new(key.clone()),
            key,
            watchers: Vec::new(),
            following: false,
            owner_client_id: None,
        }
    }
}

/// Codex Adapter 公共入口。克隆廉价(内部全为句柄);ipc/pump 由后台
/// attach 生命周期在内部替换,外部(Gateway/Runtime)持有的句柄保持稳定。
#[derive(Clone)]
pub struct CodexAdapter {
    inner: Arc<Inner>,
}

struct Inner {
    device_id: String,
    /// 当前 IPC 客户端(可被 attach 生命周期原子替换;None = 尚未建立或
    /// 已断开等待重试)。
    ipc: parking_lot::RwLock<Option<Arc<IpcClient>>>,
    /// IPC 连接参数(connect/connect 重试用;None = 无可用 socket 路径)。
    ipc_config: Option<IpcClientConfig>,
    /// 启动时的版本报告(attach 版本重估的比对基准)。
    version_report: Option<String>,
    /// attach 时 `codex --version` 探测二进制(None:冻结 version_report)。
    version_binary: Option<PathBuf>,
    write_method_probes: WriteMethodProbes,
    catalog: Option<CodexCatalog>,
    static_sessions: Vec<CatalogThread>,
    sessions: Arc<parking_lot::Mutex<HashMap<String, SessionRuntime>>>,
    caps_tx: watch::Sender<CapabilitySet>,
    /// 常驻 receiver:保证 watch 通道永不关闭,capabilities 广播在
    /// 任意 attach 时刻都可写入(空 pump 分支不再丢 receiver)。
    _caps_keepalive: watch::Receiver<CapabilitySet>,
    /// 当前 pump 任务(attach 换代时 abort 旧任务,§12:不并行多 pump)。
    pump: parking_lot::Mutex<Option<JoinHandle<()>>>,
    /// attach 代次计数(单调递增;旧连接/旧 pump 的迟到事件无法覆盖新代次)。
    generation: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for CodexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexAdapter")
            .field("device_id", &self.inner.device_id)
            .field(
                "ipc",
                &self
                    .inner
                    .ipc
                    .read()
                    .as_ref()
                    .map(|c| c.client_id().to_string()),
            )
            .field("catalog", &self.inner.catalog.is_some())
            .finish_non_exhaustive()
    }
}

impl CodexAdapter {
    /// 连接 Desktop 并完成能力 probe。IPC 失败不致命:降级 catalog-only 只读,
    /// 并由 attach 生命周期低频重试(§12:连接失败只记录,不删除 socket、
    /// 不重启 Desktop;Bridge 先于 Desktop 启动时不得永久只读)。
    pub async fn connect(config: CodexAdapterConfig) -> Result<Self, AdapterError> {
        // ---- IPC(可选;失败由 attach 生命周期重试) ----
        let socket =
            ipc::discovery::socket_path(config.ipc_socket.clone(), config.codex_home.clone());
        let mut ipc_events: Option<mpsc::Receiver<IpcEvent>> = None;
        let mut ipc: Option<Arc<IpcClient>> = None;
        let ipc_config = socket.map(|path| IpcClientConfig::new(path, "agent-console-bridge"));
        if let Some(client_config) = ipc_config.clone() {
            match connect(client_config).await {
                Ok((client, events)) => {
                    ipc_events = Some(events);
                    ipc = Some(Arc::new(client));
                }
                Err(err) => {
                    // §12:连接失败只记录,不删除 socket、不重启 Desktop。
                    tracing::warn!(
                        error = %err,
                        "codex ipc unavailable at startup; attach supervisor will retry"
                    );
                }
            }
        }

        // ---- catalog(可选) ----
        let catalog =
            match CodexCatalog::open(config.catalog.clone().unwrap_or_else(|| CatalogConfig {
                codex_home: config.codex_home.clone(),
                ..CatalogConfig::default()
            }))
            .await
            {
                Ok(catalog)
                    if catalog.capabilities().sessions_listable
                        || catalog.capabilities().history_readable =>
                {
                    Some(catalog)
                }
                Ok(_) => None,
                Err(err) => {
                    tracing::warn!(error = %err, "codex catalog unavailable");
                    None
                }
            };

        // ---- capability probe(§5/§10.2/§10.3) ----
        let input = capability_probe_input(
            config.version_report.as_deref(),
            config.write_method_probes,
            ipc.is_some(),
            catalog.as_ref(),
        );
        let capabilities = probe(&input);

        // ---- pump + attach 生命周期 ----
        let sessions: Arc<parking_lot::Mutex<HashMap<String, SessionRuntime>>> = Default::default();
        let (caps_tx, _caps_keepalive) = watch::channel(capabilities.clone());
        let generation = if ipc.is_some() { 1 } else { 0 };
        let pump = match (&ipc, ipc_events.take()) {
            (Some(client), Some(events)) => tokio::spawn(pump_loop(
                client.clone(),
                sessions.clone(),
                caps_tx.subscribe(),
                events,
            )),
            _ => tokio::spawn(async {}),
        };

        let inner = Arc::new(Inner {
            device_id: config.device_id,
            ipc: parking_lot::RwLock::new(ipc),
            ipc_config,
            version_report: config.version_report,
            version_binary: config.version_binary,
            write_method_probes: config.write_method_probes,
            catalog,
            static_sessions: config.static_sessions,
            sessions,
            caps_tx,
            _caps_keepalive,
            pump: parking_lot::Mutex::new(Some(pump)),
            generation: std::sync::atomic::AtomicU64::new(generation),
        });
        // attach 生命周期:未连接时按 1s/2s/5s/10s→30s 退避重试;已连接时
        // 低频确认。句柄全部丢弃(Weak 升级失败)后自动退出。
        if inner.ipc_config.is_some() {
            tokio::spawn(attach_supervisor(Arc::downgrade(&inner)));
        }

        Ok(Self { inner })
    }

    /// 当前能力集合。
    pub fn capabilities(&self) -> crate::domain::CapabilitySet {
        self.inner.caps_tx.borrow().clone()
    }

    /// 发现报告(只读;无会话内容)。
    pub async fn discover(&self) -> DiscoveryReport {
        let ipc = self.inner.ipc.read().clone();
        DiscoveryReport {
            socket_path: ipc.as_ref().map(|c| c.config().socket_path.clone()),
            ipc_connected: matches!(
                ipc.as_ref().map(|c| c.connection_state().borrow().clone()),
                Some(ConnectionState::Connected { .. })
            ),
            client_id: ipc.as_ref().map(|c| c.client_id().to_string()),
            catalog: self
                .inner
                .catalog
                .as_ref()
                .map(|c| c.capabilities().clone()),
            codex_version: self.capabilities().codex_version,
        }
    }

    /// 会话 cwd(仅本机 Git 授权与文件授权使用;绝不序列化出 Bridge ——
    /// 不进 Envelope、日志、摘要或任何 IPC/网络载荷,§12/§17.2/§23.1)。
    /// 仅在 adapter 已收到该会话投影快照后可用。
    pub fn session_cwd(&self, key: &SessionKey) -> Option<PathBuf> {
        let sessions = self.inner.sessions.lock();
        sessions
            .get(&key.native_session_id)
            .and_then(|runtime| runtime.mapper.cwd())
    }

    /// 会话列表分页(§11.1 SessionSummary):catalog + 运行时已知会话合并。
    pub async fn list_sessions(
        &self,
        cursor: Option<&str>,
        limit: u32,
        include_archived: bool,
    ) -> Result<SessionPage, AdapterError> {
        let mut summaries: Vec<SessionSummary> = Vec::new();
        let mut next_cursor = None;
        let mut has_more = false;

        // catalog 侧(生产路径)。
        if let Some(catalog) = &self.inner.catalog {
            match catalog.list_sessions(cursor, limit, include_archived).await {
                Ok(page) => {
                    for thread in &page.threads {
                        summaries.push(self.summary_from_catalog(thread));
                    }
                    next_cursor = page.next_cursor;
                    has_more = page.has_more;
                }
                Err(CatalogError::Unavailable(_)) => {}
                Err(CatalogError::Timeout { .. }) => {
                    return Err(AdapterError::stable(
                        StableErrorCode::RateLimited,
                        "catalog query timed out",
                    ))
                }
                Err(err) => return Err(AdapterError::Catalog(err.to_string())),
            }
        } else {
            // 无 catalog:静态种子(fake owner / e2e 路径),单页返回。
            for thread in &self.inner.static_sessions {
                if !include_archived && thread.archived {
                    continue;
                }
                summaries.push(self.summary_from_catalog(thread));
            }
            has_more = false;
        }

        // 运行时已知但目录缺失的会话(已订阅的)补充进列表。
        {
            let sessions = self.inner.sessions.lock();
            for runtime in sessions.values() {
                if !summaries
                    .iter()
                    .any(|s| s.session_key.native_session_id == runtime.key.native_session_id)
                {
                    summaries.push(runtime.mapper.session_summary());
                }
            }
        }

        Ok(SessionPage {
            sessions: summaries,
            next_cursor,
            has_more,
        })
    }

    /// 当前运行快照(§11.2)。未订阅过的会话:临时 following 并等待首个快照。
    pub async fn runtime_snapshot(
        &self,
        key: &SessionKey,
    ) -> Result<RuntimeSnapshot, AdapterError> {
        let runtime_key = key.native_session_id.clone();
        {
            let mut sessions = self.inner.sessions.lock();
            let runtime = sessions.entry(runtime_key.clone()).or_insert_with(|| {
                SessionRuntime::new(SessionKey::codex(
                    self.inner.device_id.clone(),
                    runtime_key.clone(),
                ))
            });
            if let Some(snapshot) = runtime.mapper.runtime_snapshot() {
                return Ok(snapshot);
            }
            // 需要 following 才能拿到首个快照(Desktop 只向 follower 推送)。
            self.follow_locked(&mut sessions, &runtime_key)?;
        }
        // 等待快照到达(pump 处理)。
        let deadline = tokio::time::Instant::now() + SNAPSHOT_WAIT;
        loop {
            {
                let sessions = self.inner.sessions.lock();
                if let Some(runtime) = sessions.get(&runtime_key) {
                    if let Some(snapshot) = runtime.mapper.runtime_snapshot() {
                        return Ok(snapshot);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AdapterError::stable(
                    StableErrorCode::CodexUnavailable,
                    "no snapshot received before timeout",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 通过 Desktop 已验证的 `load-complete-history` 重读权威快照。
    /// owner 返回的 revision 是本次读取的权威水位；若现有投影已经达到
    /// 该水位，Desktop 可以不重复推送内容未变化的 snapshot。
    pub async fn refresh_snapshot(
        &self,
        key: &SessionKey,
    ) -> Result<RuntimeSnapshot, AdapterError> {
        let ipc = self.inner.ipc.read().clone().ok_or_else(|| {
            AdapterError::stable(StableErrorCode::CodexUnavailable, "ipc offline")
        })?;
        {
            let mut sessions = self.inner.sessions.lock();
            let runtime = sessions
                .entry(key.native_session_id.clone())
                .or_insert_with(|| SessionRuntime::new(key.clone()));
            runtime.following = true;
        }

        // 与后续 request 共用同一 IPC 出站队列；先声明 following，保证
        // owner 处理 load-complete-history 时已知道回发目标。
        ipc.set_following(
            FollowingChangedParams {
                conversation_id: key.native_session_id.clone(),
                host_id: LOCAL_HOST_ID.to_string(),
                following: true,
            },
            None,
        )
        .await
        .map_err(AdapterError::from)?;
        let expected_revision = ipc
            .load_complete_history(LOCAL_HOST_ID, &key.native_session_id)
            .await
            .map_err(AdapterError::from)?;
        let deadline = tokio::time::Instant::now() + SNAPSHOT_WAIT;
        loop {
            let snapshot = {
                let sessions = self.inner.sessions.lock();
                sessions
                    .get(&key.native_session_id)
                    .and_then(|runtime| runtime.mapper.runtime_snapshot())
            };
            if let Some(snapshot) = snapshot {
                if snapshot.runtime_revision >= expected_revision {
                    return Ok(snapshot);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AdapterError::stable(
                    StableErrorCode::CodexUnavailable,
                    "authoritative snapshot not received before timeout",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 从 Adapter 的当前内存投影按需读取历史命令输出，不触发完整
    /// 会话落盘或 Relay 持久化。
    pub fn output_page(
        &self,
        key: &SessionKey,
        item_id: &ItemId,
        offset: usize,
        limit: usize,
    ) -> Option<(Vec<u8>, usize, bool)> {
        self.inner
            .sessions
            .lock()
            .get(&key.native_session_id)?
            .mapper
            .output_page(item_id, offset, limit)
    }

    /// 订阅会话详情事件(§17.4:详情订阅才包含 item 变化、输出和 attention)。
    /// 调用方提供 watcher;adapter 推送 [`DomainEvent`]。
    pub async fn subscribe(
        &self,
        key: &SessionKey,
        watcher: mpsc::Sender<DomainEvent>,
    ) -> Result<(), AdapterError> {
        let runtime_key = key.native_session_id.clone();
        let intro_events;
        {
            let mut sessions = self.inner.sessions.lock();
            let runtime = sessions.entry(runtime_key.clone()).or_insert_with(|| {
                SessionRuntime::new(SessionKey::codex(
                    self.inner.device_id.clone(),
                    runtime_key.clone(),
                ))
            });
            runtime.watchers.push(watcher.clone());
            // 新 watcher 立即获得当前摘要(快照本体走 runtime_snapshot)。
            intro_events = if runtime.mapper.has_snapshot() {
                vec![DomainEvent::SessionSummaryChanged {
                    summary: runtime.mapper.session_summary(),
                }]
            } else {
                Vec::new()
            };
            self.follow_locked(&mut sessions, &runtime_key)?;
        }
        for event in intro_events {
            let _ = watcher.send(event).await;
        }
        Ok(())
    }

    /// 执行写命令(§15):capability gate → owner 确认 → 原生操作 → 回执流。
    ///
    /// 队列命令(QueueNextTurn/CancelQueue/PauseQueue)与停止后台命令的
    /// 持久化与调度归 commands 工作流(§15.3 正文只存 Bridge SQLite;
    /// 当前 IPC 无停止后台命令方法),此处返回 CAPABILITY_UNSUPPORTED。
    pub async fn execute_command(
        &self,
        key: &SessionKey,
        request: CommandRequest,
    ) -> Result<mpsc::Receiver<CommandReceipt>, AdapterError> {
        let (tx, rx) = mpsc::channel(8);
        let caps = self.capabilities();

        // ---- capability gate(§5) ----
        let gate = |op: &Operation| -> Result<(), AdapterError> {
            if caps.supports(*op) {
                Ok(())
            } else if caps.control_mode == crate::domain::ControlMode::ReadOnly {
                Err(AdapterError::stable(
                    StableErrorCode::ControlReadOnly,
                    "desktop capability probe did not enable this write",
                ))
            } else {
                Err(AdapterError::stable(
                    StableErrorCode::CapabilityUnsupported,
                    "operation not supported by current desktop version",
                ))
            }
        };

        let conversation = key.native_session_id.clone();
        let owner = self.ensure_owner(&conversation).await?;
        let ipc = self.inner.ipc.read().clone().ok_or_else(|| {
            AdapterError::stable(StableErrorCode::CodexUnavailable, "ipc offline")
        })?;

        // ---- expected turn / revision 校验(§15.1/§15.4) ----
        {
            let sessions = self.inner.sessions.lock();
            if let Some(runtime) = sessions.get(&conversation) {
                let current_turn = runtime.current_turn_id();
                if let (Some(expected), Some(current)) = (
                    request.expected_runtime_revision,
                    runtime.mapper.runtime_revision(),
                ) {
                    if expected != current && !request.permits_revision_drift(current_turn.as_ref())
                    {
                        return Err(AdapterError::stable(
                            StableErrorCode::StaleTurn,
                            format!("runtime revision moved {expected} -> {current}"),
                        ));
                    }
                }
                if let Some(expected_turn) = request.expected_turn_id.as_ref() {
                    if let Some(current_turn) = current_turn {
                        if expected_turn.id != current_turn.id {
                            return Err(AdapterError::stable(
                                StableErrorCode::StaleTurn,
                                "target turn is no longer current",
                            ));
                        }
                    }
                }
            }
        }

        macro_rules! receipt {
            ($state:expr) => {{
                let _ = tx
                    .send(CommandReceipt {
                        request_id: request.request_id,
                        state: $state,
                        at: Some(chrono::Utc::now()),
                    })
                    .await;
            }};
        }

        match &request.payload {
            CommandPayload::StartTurn { input } => {
                gate(&Operation::StartTurn)?;
                receipt!(ReceiptState::DispatchedToCodex);
                ipc.start_turn(
                    &owner,
                    StartTurnParams {
                        conversation_id: conversation.clone(),
                        turn_start: TurnStart {
                            request: TurnStartRequest {
                                thread_id: conversation.clone(),
                                input: vec![InputBlock::text(input.as_str())],
                                extra: Default::default(),
                            },
                            context: None,
                        },
                    },
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::Steer { input } => {
                gate(&Operation::SteerTurn)?;
                let restore_message = self
                    .inner
                    .sessions
                    .lock()
                    .get(&conversation)
                    .and_then(|runtime| runtime.mapper.steer_restore_message())
                    .ok_or_else(|| {
                        AdapterError::stable(
                            StableErrorCode::CodexUnavailable,
                            "steer requires current session context",
                        )
                    })?;
                receipt!(ReceiptState::DispatchedToCodex);
                ipc.steer_turn(
                    &owner,
                    SteerTurnParams {
                        conversation_id: conversation.clone(),
                        input: vec![InputBlock::text(input.as_str())],
                        client_user_message_id: None,
                        service_tier: None,
                        attachments: None,
                        additional_context: None,
                        tool_output: None,
                        restore_message: Some(restore_message),
                    },
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::Interrupt => {
                gate(&Operation::InterruptTurn)?;
                // §15.4:interrupt 必须带 expected turn。
                let Some(expected_turn) = request.expected_turn_id.clone() else {
                    return Err(AdapterError::stable(
                        StableErrorCode::InternalError,
                        "interrupt requires expected_turn_id",
                    ));
                };
                receipt!(ReceiptState::DispatchedToCodex);
                ipc.interrupt_turn(
                    &owner,
                    ipc::client::IpcClient::interrupt_params(
                        conversation.clone(),
                        InterruptMode::UserStop,
                        Some(expected_turn.id),
                    ),
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::AnswerQuestion {
                question_id,
                option_ids,
                free_text,
            } => {
                gate(&Operation::AnswerQuestion)?;
                receipt!(ReceiptState::DispatchedToCodex);
                // 0.153.4 回答路由与 payload 形状(asar V9t/
                // replyWithUserInputResponse 链,2026-09-05 复核):外层
                // request.id 与题目自身 id 是两个身份——
                // `thread-follower-submit-user-input` params.requestId 用
                // 外层 request.id 路由;response 归一化为
                // `{answers: {<题目自身id>: {answers: [...]}}}`,内层键用
                // 每题自身 id 且全部题目进 payload(结构化投影见
                // projection::extract_requests_questions_detail)。legacy
                // `pendingQuestions` 路径题目身份即路由 ID,单键回退不变。
                // answers 元素与 option_id 的精确原生类型待 N01 真机采样
                // 校正;free_text 以字符串元素追加(原生 isOther 输入的
                // 接受形态未验证)。
                let request_questions = {
                    let sessions = self.inner.sessions.lock();
                    sessions
                        .get(&conversation)
                        .map(|runtime| runtime.mapper.pending_request_questions())
                        .unwrap_or_default()
                };
                let (request_id, response) = answer_response_for(
                    &request_questions,
                    question_id.as_str(),
                    option_ids,
                    free_text.as_ref().map(|text| text.as_str()),
                );
                generic_write(
                    &ipc,
                    &owner,
                    ipc::messages::method::THREAD_FOLLOWER_SUBMIT_USER_INPUT,
                    serde_json::json!({
                        "conversationId": conversation,
                        "requestId": request_id,
                        "response": response,
                    }),
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::SubmitApproval {
                approval_id,
                decision_id,
            } => {
                gate(&Operation::SubmitApproval)?;
                receipt!(ReceiptState::DispatchedToCodex);
                generic_write(
                    &ipc,
                    &owner,
                    ipc::messages::method::THREAD_FOLLOWER_COMMAND_APPROVAL_DECISION,
                    serde_json::json!({
                        "conversationId": conversation,
                        "requestId": approval_id,
                        "decision": decision_id,
                    }),
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::UpdateSettings { values } => {
                gate(&Operation::UpdateSettings)?;
                receipt!(ReceiptState::DispatchedToCodex);
                let mut thread_settings = serde_json::Map::new();
                for value in values {
                    thread_settings.insert(
                        value.kind.native_key().to_string(),
                        serde_json::Value::String(value.value.clone()),
                    );
                }
                generic_write(
                    &ipc,
                    &owner,
                    ipc::messages::method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                    serde_json::json!({
                        "conversationId": conversation,
                        "threadSettings": thread_settings,
                    }),
                )
                .await
                .map_err(map_ipc_write_error)?;
                receipt!(ReceiptState::Completed);
            }
            CommandPayload::QueueNextTurn { .. }
            | CommandPayload::CancelQueue
            | CommandPayload::PauseQueue
            | CommandPayload::StopBackgroundCommand { .. }
            | CommandPayload::StopAllBackgroundCommands => {
                let _ = tx
                    .send(CommandReceipt {
                        request_id: request.request_id,
                        state: ReceiptState::Rejected {
                            code: StableErrorCode::CapabilityUnsupported,
                            message: "queue lifecycle is owned by the commands layer; ".to_string()
                                + "background-command stop has no verified native method",
                        },
                        at: Some(chrono::Utc::now()),
                    })
                    .await;
            }
        }
        Ok(rx)
    }

    /// 单会话历史分页(§11.1 HistoryPage;catalog 只读)。
    pub async fn history_page(
        &self,
        key: &SessionKey,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<crate::domain::HistoryPage, AdapterError> {
        let Some(catalog) = &self.inner.catalog else {
            return Err(AdapterError::stable(
                StableErrorCode::CodexUnavailable,
                "no catalog source configured",
            ));
        };
        match catalog
            .session_history(&key.native_session_id, cursor, limit)
            .await
        {
            Ok(page) => Ok(crate::domain::HistoryPage {
                entries: page.entries,
                next_cursor: page.next_cursor,
                has_more: page.has_more,
            }),
            Err(CatalogError::Unavailable(_)) => Err(AdapterError::stable(
                StableErrorCode::CapabilityUnsupported,
                "history reading disabled by schema probe",
            )),
            Err(CatalogError::Timeout { .. }) => Err(AdapterError::stable(
                StableErrorCode::RateLimited,
                "catalog query timed out",
            )),
            Err(err) => Err(AdapterError::Catalog(err.to_string())),
        }
    }

    // -----------------------------------------------------------------
    // 内部
    // -----------------------------------------------------------------

    /// 锁内调用:发送 following broadcast 并登记状态。
    /// (broadcast 是同步入队;发送失败仅意味着 IPC 断开,由 pump/上层恢复。)
    fn follow_locked(
        &self,
        sessions: &mut HashMap<String, SessionRuntime>,
        conversation: &str,
    ) -> Result<(), AdapterError> {
        let Some(ipc) = self.inner.ipc.read().clone() else {
            return Err(AdapterError::stable(
                StableErrorCode::CodexUnavailable,
                "ipc offline; cannot follow session",
            ));
        };
        let runtime = sessions.entry(conversation.to_string()).or_insert_with(|| {
            SessionRuntime::new(SessionKey::codex(
                self.inner.device_id.clone(),
                conversation.to_string(),
            ))
        });
        if runtime.following {
            return Ok(());
        }
        let params = FollowingChangedParams {
            conversation_id: conversation.to_string(),
            host_id: LOCAL_HOST_ID.to_string(),
            following: true,
        };
        runtime.following = true;
        tokio::spawn(async move {
            if let Err(err) = ipc.set_following(params, None).await {
                tracing::warn!(error = %err, "following broadcast failed");
            }
        });
        Ok(())
    }

    /// owner 确认(§12:所有写操作先确认 owner)。
    async fn ensure_owner(&self, conversation: &str) -> Result<String, AdapterError> {
        let ipc = self.inner.ipc.read().clone().ok_or_else(|| {
            AdapterError::stable(StableErrorCode::CodexUnavailable, "ipc offline")
        })?;
        // 缓存的 owner 仍然有效 → 直接用。
        {
            let sessions = self.inner.sessions.lock();
            if let Some(runtime) = sessions.get(conversation) {
                if let Some(owner) = &runtime.owner_client_id {
                    return Ok(owner.clone());
                }
            }
        }
        let owner = ipc
            .discover_owner(LOCAL_HOST_ID, conversation)
            .await
            .map_err(AdapterError::from)?
            .ok_or_else(|| {
                AdapterError::stable(
                    StableErrorCode::SessionNotFound,
                    "session has no desktop owner (not open)",
                )
            })?;
        let mut sessions = self.inner.sessions.lock();
        let runtime = sessions.entry(conversation.to_string()).or_insert_with(|| {
            SessionRuntime::new(SessionKey::codex(
                self.inner.device_id.clone(),
                conversation.to_string(),
            ))
        });
        runtime.owner_client_id = Some(owner.clone());
        Ok(owner)
    }

    fn summary_from_catalog(&self, thread: &CatalogThread) -> SessionSummary {
        let caps = self.capabilities();
        let sessions = self.inner.sessions.lock();
        let runtime = sessions.get(&thread.id);
        let mapper_summary = runtime.map(|r| r.mapper.session_summary());
        SessionSummary {
            session_key: SessionKey::codex(self.inner.device_id.clone(), thread.id.clone()),
            title: thread.title.clone().map(OutputText::new),
            agent_kind: crate::domain::AgentKind::CodexDesktop,
            project_display_name: thread.project_display_name.clone(),
            current_branch: thread.git_branch.clone(),
            updated_at: thread.updated_at,
            device_connection: if self.inner.ipc.read().is_some() {
                crate::domain::DeviceConnection::Online
            } else {
                crate::domain::DeviceConnection::Degraded
            },
            device_last_seen_at: Some(chrono::Utc::now()),
            degraded_reason: None,
            control_mode: caps.control_mode,
            compatibility_state: caps.compatibility_state,
            active_turn_phase: mapper_summary
                .as_ref()
                .map(|s| s.active_turn_phase)
                .unwrap_or(crate::domain::ActiveTurnPhase::Idle),
            pending_attention_count: mapper_summary
                .as_ref()
                .map(|s| s.pending_attention_count)
                .unwrap_or(0),
            pending_attention_kinds: mapper_summary
                .as_ref()
                .map(|s| s.pending_attention_kinds.clone())
                .unwrap_or_default(),
            queue_state: mapper_summary
                .as_ref()
                .map(|s| s.queue_state)
                .unwrap_or(crate::domain::QueueState::Empty),
            last_turn_outcome: mapper_summary
                .as_ref()
                .map(|s| s.last_turn_outcome)
                .unwrap_or(crate::domain::LastTurnOutcome::Unknown),
            pinned: false,
            muted: false,
            archived: thread.archived,
        }
    }
}

// SessionRuntime 的小辅助(放在 impl 外避免与 HashMap entry 借用纠缠)。
impl SessionRuntime {
    fn current_turn_id(&self) -> Option<crate::domain::TurnId> {
        self.mapper
            .runtime_snapshot()
            .and_then(|s| s.current_turn.map(|t| t.turn))
    }
}

/// AnswerQuestion 的回复组装(requestId 路由 + response 归一化形状)。
///
/// requests[] 详情命中该 question_id(即外层 request.id)→ 按每题自身身份
/// 组装,见 [`projection::build_request_answer_response`];未命中(legacy
/// `pendingQuestions` 路径:题目身份即路由 ID,不在 requests[] 中)→ 单键
/// 回退,question_id 同时作 requestId 与 answers 内层键,行为与该投影约定
/// 一致(fake owner / e2e 依赖)。
fn answer_response_for(
    request_questions: &[projection::RequestsQuestion],
    question_id: &str,
    answered_labels: &[String],
    free_text: Option<&str>,
) -> (String, serde_json::Value) {
    match request_questions
        .iter()
        .find(|request| request.request_id == question_id)
    {
        Some(request) => (
            request.request_id.clone(),
            projection::build_request_answer_response(request, answered_labels, free_text),
        ),
        None => (
            question_id.to_string(),
            serde_json::json!({
                "answers": {
                    question_id: {
                        "answers": projection::answer_entries(answered_labels, free_text)
                    },
                },
            }),
        ),
    }
}

/// 通用 request 写(submit-user-input / approval / settings)。
async fn generic_write(
    ipc: &IpcClient,
    owner: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<ipc::messages::ResponseFrame, IpcError> {
    ipc.request(
        method,
        params,
        ipc::client::RequestOptions {
            target_client_id: Some(owner.to_string()),
            host_id: Some(LOCAL_HOST_ID.to_string()),
            ..Default::default()
        },
    )
    .await
}

/// 原生写错误 → 稳定错误码(§12:原生错误 → 稳定错误码)。
fn map_ipc_write_error(err: IpcError) -> AdapterError {
    match &err {
        IpcError::Peer(msg)
            if msg.contains("NoActiveTurn") || msg.contains("SteerTurnInactive") =>
        {
            AdapterError::stable(StableErrorCode::StaleTurn, msg.clone())
        }
        IpcError::Peer(msg) if msg.contains("no-client-found") => AdapterError::stable(
            StableErrorCode::SessionNotFound,
            "owner disappeared before write",
        ),
        IpcError::Peer(msg) if msg.contains("request-version-mismatch") => AdapterError::stable(
            StableErrorCode::CodexVersionUnverified,
            "desktop rejected method version",
        ),
        IpcError::ConnectionLost(_) | IpcError::NotConnected => AdapterError::stable(
            StableErrorCode::OutcomeUnknown,
            "connection lost during write; outcome unknown",
        ),
        other => AdapterError::Ipc(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// pump:IPC 事件 → mapper → watcher 分发
// ---------------------------------------------------------------------------

async fn pump_loop(
    client: Arc<IpcClient>,
    sessions: Arc<parking_lot::Mutex<HashMap<String, SessionRuntime>>>,
    caps_rx: watch::Receiver<CapabilitySet>,
    mut events: mpsc::Receiver<IpcEvent>,
) {
    // 输出合并的定时冲刷(§26.3:max_delay 内未被 64KiB 触发的缓冲在此发出)。
    let mut flush_ticker = tokio::time::interval(Duration::from_millis(25));
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                Some(event) => event,
                None => break,
            },
            _ = flush_ticker.tick() => {
                flush_due_outputs_all(&sessions).await;
                continue;
            }
        };
        match event {
            IpcEvent::StreamChanged(params) => {
                let conversation = params.conversation_id.clone();
                let (events, watchers) = {
                    let mut sessions = sessions.lock();
                    // Desktop 建连时可能补发多个未订阅会话的完整历史快照。
                    // 列表页只需要 catalog 摘要；详情会先登记 runtime 再
                    // following。忽略未登记流，避免单个大历史阻塞目标详情。
                    let Some(runtime) = sessions.get_mut(&conversation) else {
                        continue;
                    };
                    runtime.mapper.set_capabilities(caps_rx.borrow().clone());
                    let events = match &params.change {
                        ipc::messages::StreamChange::Snapshot {
                            revision,
                            conversation_state,
                        } => runtime.mapper.apply_snapshot(*revision, conversation_state),
                        ipc::messages::StreamChange::Patches {
                            base_revision,
                            revision,
                            patches,
                        } => match runtime
                            .mapper
                            .apply_patches(*base_revision, *revision, patches)
                        {
                            mapper::PatchOutcome::Applied { events } => events,
                            mapper::PatchOutcome::ResyncNeeded => {
                                // §12/§14:未识别/错序 patch → 快照补偿。
                                let client = client.clone();
                                let conversation = conversation.clone();
                                tokio::spawn(async move {
                                    let _ = client
                                        .load_complete_history(LOCAL_HOST_ID, &conversation)
                                        .await;
                                });
                                Vec::new()
                            }
                        },
                    };
                    (events, runtime.watchers.clone())
                };
                dispatch_to_watchers(&watchers, events).await;
            }
            IpcEvent::Broadcast { method, .. } => {
                // owner 切换信号:client-status-changed / following-status-requested
                // → 重新发送 following,新 owner 会回发快照(协议文档 §5)。
                if method == ipc::messages::method::CLIENT_STATUS_CHANGED
                    || method == ipc::messages::method::THREAD_STREAM_FOLLOWING_STATUS_REQUESTED
                {
                    let following: Vec<String> = {
                        let sessions = sessions.lock();
                        sessions
                            .iter()
                            .filter(|(_, r)| r.following)
                            .map(|(k, _)| k.clone())
                            .collect()
                    };
                    for conversation in following {
                        let _ = client
                            .set_following(
                                FollowingChangedParams {
                                    conversation_id: conversation,
                                    host_id: LOCAL_HOST_ID.to_string(),
                                    following: true,
                                },
                                None,
                            )
                            .await;
                    }
                }
            }
            IpcEvent::UnknownBroadcast { .. } => {
                // 未识别广播:忽略并计数由 doctor 层负责;不猜测语义(§12)。
            }
        }
    }
}

/// 定时冲刷所有会话到期的待合并输出。
async fn flush_due_outputs_all(
    sessions: &Arc<parking_lot::Mutex<HashMap<String, SessionRuntime>>>,
) {
    let conversations: Vec<String> = {
        let sessions = sessions.lock();
        sessions.keys().cloned().collect()
    };
    for conversation in conversations {
        let (flushed, watchers) = {
            let mut sessions = sessions.lock();
            match sessions.get_mut(&conversation) {
                Some(runtime) => (
                    runtime.mapper.flush_due_outputs(std::time::Instant::now()),
                    runtime.watchers.clone(),
                ),
                None => continue,
            }
        };
        dispatch_to_watchers(&watchers, flushed).await;
    }
}

async fn dispatch_to_watchers(watchers: &[mpsc::Sender<DomainEvent>], events: Vec<DomainEvent>) {
    for event in events {
        for watcher in watchers {
            let _ = watcher.send(event.clone()).await;
        }
    }
}

// ---------------------------------------------------------------------------
// attach 生命周期(AC-05):首次未建立 / 中途断开的 IPC 由本模块低频重试,
// Gateway/Runtime 持有的 Adapter 句柄保持稳定
// ---------------------------------------------------------------------------

/// attach 失败退避序列(先密后疏,上限 30s;连接尝试本身廉价且幂等)。
const ATTACH_BACKOFF_STEPS: &[Duration] = &[
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];
/// attach 退避上限。
const ATTACH_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// 已连接时的低频确认周期(检测对端退出等断开,不做高频轮询)。
const ATTACH_CONNECTED_POLL: Duration = Duration::from_secs(30);

fn attach_backoff(failures: u32) -> Duration {
    let idx = failures.saturating_sub(1) as usize;
    ATTACH_BACKOFF_STEPS
        .get(idx)
        .copied()
        .unwrap_or(ATTACH_MAX_BACKOFF)
        .min(ATTACH_MAX_BACKOFF)
}

/// capability probe 输入(connect 与 attach 共用;§5:未知版本/未 probe
/// 一律只读,写能力由逐版本矩阵决定)。
fn capability_probe_input(
    version_report: Option<&str>,
    write_methods: WriteMethodProbes,
    ipc_connected: bool,
    catalog: Option<&CodexCatalog>,
) -> CapabilityProbeInput {
    let version_verified = version_report
        .map(ipc::discovery::version_is_verified)
        .unwrap_or(false);
    CapabilityProbeInput {
        version_report: version_report.map(str::to_string),
        version_verified,
        confirmed_incompatible: false,
        ipc_handshake_ok: ipc_connected,
        owner_confirmed: ipc_connected,
        write_methods,
        catalog_sessions_listable: catalog
            .map(|c| c.capabilities().sessions_listable)
            .unwrap_or(false),
        catalog_history_readable: catalog
            .map(|c| c.capabilities().history_readable)
            .unwrap_or(false),
        snapshot_observed: ipc_connected,
        settings: Vec::new(),
    }
}

/// 当前 IPC 是否处于已连接状态。
fn ipc_connected(inner: &Inner) -> bool {
    matches!(
        inner
            .ipc
            .read()
            .as_ref()
            .map(|c| c.connection_state().borrow().clone()),
        Some(ipc::client::ConnectionState::Connected { .. })
    )
}

/// attach/重新 attach 一次:连接 + 握手 → 代次切换(旧 pump 回收、owner
/// 缓存失效)→ 能力重估 → 重放 following(新 owner 回发权威快照)。
async fn try_attach(inner: &Inner) -> Result<(), IpcError> {
    let Some(client_config) = inner.ipc_config.clone() else {
        return Err(IpcError::NotConnected);
    };
    let (client, events) = connect(client_config).await?;
    let client = Arc::new(client);
    let generation = inner
        .generation
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;

    // ---- 能力重估(§5):重新发现实际运行时版本 ----
    // version_binary 未配置:冻结启动时信任基础(调用方自行负责版本发现);
    // 已配置:重新探测,版本与启动基准不一致 → 写矩阵全部失效(NotProbed),
    // 只恢复可证明的读取能力,不凭"以前版本验证过"自动开放新版本写。
    let fresh_version = ipc::discovery::probe_version(inner.version_binary.clone())
        .await
        .ok();
    let version_unchanged = match (&fresh_version, &inner.version_report) {
        (Some(fresh), Some(configured)) => {
            ipc::discovery::normalized_version(fresh)
                == ipc::discovery::normalized_version(configured)
        }
        _ => true,
    };
    let (version_report, write_probes) = if version_unchanged {
        (
            fresh_version.or_else(|| inner.version_report.clone()),
            inner.write_method_probes,
        )
    } else {
        tracing::warn!(
            fresh = fresh_version.as_deref().unwrap_or("unknown"),
            "codex version changed since startup; write capabilities stay closed"
        );
        (fresh_version, WriteMethodProbes::default())
    };
    let input = capability_probe_input(
        version_report.as_deref(),
        write_probes,
        true,
        inner.catalog.as_ref(),
    );
    let capabilities = probe(&input);

    // ---- 代次切换:旧 pump/旧连接/owner 缓存一并回收 ----
    // 旧 IpcClient drop 时 abort 其 reader/writer,旧事件通道随之关闭:
    // 旧连接的迟到消息无法进入新代次(同一原生事件只处理一次)。
    if let Some(old_pump) = inner.pump.lock().take() {
        old_pump.abort();
    }
    {
        let mut sessions = inner.sessions.lock();
        for runtime in sessions.values_mut() {
            // 旧 client 上的 owner clientId 全部失效;写路径经 ensure_owner
            // 重新发现 owner(§12:所有写操作先确认 owner)。
            runtime.owner_client_id = None;
        }
    }
    *inner.ipc.write() = Some(client.clone());
    // send_replace:无条件更新当前值(不依赖 receiver 存活)。
    inner.caps_tx.send_replace(capabilities);
    let pump = tokio::spawn(pump_loop(
        client.clone(),
        inner.sessions.clone(),
        inner.caps_tx.subscribe(),
        events,
    ));
    *inner.pump.lock() = Some(pump);

    // ---- 重放 following:新连接重新登记,owner 回发权威快照 ----
    // 不重放任何写命令/队列:following 只是只读订阅语义(§12/§15)。
    let following: Vec<String> = {
        let sessions = inner.sessions.lock();
        sessions
            .iter()
            .filter(|(_, r)| r.following)
            .map(|(k, _)| k.clone())
            .collect()
    };
    for conversation in following {
        let _ = client
            .set_following(
                FollowingChangedParams {
                    conversation_id: conversation,
                    host_id: LOCAL_HOST_ID.to_string(),
                    following: true,
                },
                None,
            )
            .await;
    }
    tracing::info!(generation, "codex ipc attached");
    Ok(())
}

/// attach supervisor:未连接时按退避重试,已连接时低频确认;Adapter 句柄
/// 全部丢弃(Weak 升级失败)后退出。不删除 socket、不重启 Desktop(§12)。
async fn attach_supervisor(inner: std::sync::Weak<Inner>) {
    let mut failures: u32 = 0;
    let mut next_wait = ATTACH_BACKOFF_STEPS[0];
    loop {
        tokio::time::sleep(next_wait).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if ipc_connected(&inner) {
            failures = 0;
            next_wait = ATTACH_CONNECTED_POLL;
            continue;
        }
        match try_attach(&inner).await {
            Ok(()) => {
                failures = 0;
                next_wait = ATTACH_CONNECTED_POLL;
            }
            Err(err) => {
                failures = failures.saturating_add(1);
                next_wait = attach_backoff(failures);
                tracing::warn!(
                    failures,
                    next_retry_in_secs = next_wait.as_secs(),
                    error = %err,
                    "codex ipc attach failed; will retry"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request_fixture(question_ids: &[&str]) -> projection::RequestsQuestion {
        projection::RequestsQuestion {
            request_id: "req-q-1".to_string(),
            turn: None,
            questions: question_ids
                .iter()
                .map(|id| projection::RequestQuestion {
                    question_id: (*id).to_string(),
                    header: "Plan".to_string(),
                    question: "Which option?".to_string(),
                    allow_free_text: true,
                    option_labels: vec!["Option A".to_string(), "Option B".to_string()],
                })
                .collect(),
        }
    }

    fn answers_of(response: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
        response
            .get("answers")
            .and_then(serde_json::Value::as_object)
            .expect("normalized answers map")
    }

    // P2-10 回归①:外层/内层 ID 不同(req-q-1 vs q1)→ requestId 用外层
    // request.id 路由,answers 内层键用题目自身 id,不复用 request.id。
    #[test]
    fn answer_response_routes_by_request_id_and_keys_answers_by_question_id() {
        let detail = vec![request_fixture(&["q1"])];
        let (request_id, response) =
            answer_response_for(&detail, "req-q-1", &["Option A".to_string()], None);
        assert_eq!(request_id, "req-q-1");
        let answers = answers_of(&response);
        assert!(
            answers.contains_key("q1"),
            "inner key must be the question id: {response}"
        );
        assert!(
            !answers.contains_key("req-q-1"),
            "request.id must not leak into the answers keys"
        );
        assert_eq!(answers.get("q1"), Some(&json!({"answers": ["Option A"]})));
    }

    // P2-10 回归②:多题 → 每题都进 payload;命令是单卡回答,所选选项与
    // 自由文本归属第一题(与展示卡一致),其余题目显式空 answers。
    #[test]
    fn answer_response_includes_every_question_of_multi_question_request() {
        let detail = vec![request_fixture(&["q1", "q2"])];
        let (request_id, response) = answer_response_for(
            &detail,
            "req-q-1",
            &["Option A".to_string()],
            Some("custom"),
        );
        assert_eq!(request_id, "req-q-1");
        let answers = answers_of(&response);
        assert_eq!(answers.len(), 2, "all questions must be assembled");
        assert_eq!(
            answers.get("q1"),
            Some(&json!({"answers": ["Option A", "custom"]}))
        );
        assert_eq!(answers.get("q2"), Some(&json!({"answers": []})));
    }

    // P2-10 回归③:legacy `pendingQuestions` 路径(题目身份即路由 ID)在
    // 详情未命中时单键回退,行为不变(fake owner / e2e 回归)。
    #[test]
    fn answer_response_legacy_path_keeps_single_key_fallback() {
        let (request_id, response) =
            answer_response_for(&[], "legacy-q", &["yes".to_string()], None);
        assert_eq!(request_id, "legacy-q");
        let answers = answers_of(&response);
        assert_eq!(answers.get("legacy-q"), Some(&json!({"answers": ["yes"]})));
    }
}
