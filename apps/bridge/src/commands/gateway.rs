//! CommandGateway(权威规格 §15.1/§15.2/§26.4):写命令统一入口。
//!
//! 处理顺序(§15):
//! 1. capability gate:Desktop 介导操作不在 [`CapabilitySet::supported`]
//!    → `CAPABILITY_UNSUPPORTED`;`READ_ONLY` → `CONTROL_READ_ONLY`。
//!    队列操作(QueueNextTurn/CancelQueue/PauseQueue)由 Bridge 本地管理
//!    (§15.3 正文只存 Bridge SQLite),不走 Desktop 能力 gate。
//! 2. request_id 去重(`local_store::record_receipt`):同 ID 同 digest →
//!    重放既有回执,不重复执行;同 ID 不同 digest →
//!    `DUPLICATE_REQUEST_MISMATCH`。
//! 3. expected turn/revision 与当前快照不符 → `STALE_TURN`(§15.2)。
//! 4. 持久化 `ACCEPTED_BY_BRIDGE`(与第 2 步同一条幂等 upsert)。
//! 5. 交给 adapter/Desktop;回执(DispatchedToCodex/Completed/Rejected/
//!    OutcomeUnknown)按序透传调用方并落库。
//!
//! §26.4:连接中断无法证明 Desktop 是否执行 → `OUTCOME_UNKNOWN`,绝不自动
//! 换新 ID 重试;shutdown 后拒绝新命令,并对未完成命令补 `OUTCOME_UNKNOWN`
//! 终态(`graceful_shutdown`)。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::adapter::codex::{AdapterError, CodexAdapter};
use crate::domain::{
    BridgeError, CapabilitySet, CommandPayload, CommandReceipt, CommandRequest, ReceiptState,
    SessionKey, StableErrorCode,
};
use crate::local_store::{LocalStore, RequestReceipt, SessionKeyRef};

use super::queue::QueueManager;
use super::{canonical_payload_digest, operation_name, receipt_status_str, store_error_code};

/// 命令执行器 seam:生产实现即 [`CodexAdapter`];测试可注入以确定性地驱动
/// 回执流语义(shutdown、无终态流等),不改变 gateway 流程。
#[async_trait]
pub trait CommandExecutor: Send + Sync {
    async fn execute_command(
        &self,
        key: &SessionKey,
        request: CommandRequest,
    ) -> Result<mpsc::Receiver<CommandReceipt>, AdapterError>;
}

#[async_trait]
impl CommandExecutor for CodexAdapter {
    async fn execute_command(
        &self,
        key: &SessionKey,
        request: CommandRequest,
    ) -> Result<mpsc::Receiver<CommandReceipt>, AdapterError> {
        CodexAdapter::execute_command(self, key, request).await
    }
}

/// 提交结果(§15.2)。
#[derive(Debug)]
pub enum Submission {
    /// `ACCEPTED_BY_BRIDGE`:已持久化去重记录并接受。`receipts` 是后续回执流
    /// (DispatchedToCodex/Completed/Rejected/OutcomeUnknown);Browser 只有
    /// 收到本结果才可显示"已发送/已排队"。
    Accepted {
        accepted_at: chrono::DateTime<chrono::Utc>,
        receipts: mpsc::Receiver<CommandReceipt>,
    },
    /// 同 ID 同 payload 重试:返回既有回执状态,不重复执行(§15.2)。
    Replayed {
        request_id: uuid::Uuid,
        state: StoredReceiptState,
    },
}

impl Submission {
    /// 是否为新接受(用于断言重试只执行一次)。
    pub fn is_accepted(&self) -> bool {
        matches!(self, Submission::Accepted { .. })
    }

    /// 取出后续回执流(Accepted 时)。
    pub fn into_receipts(self) -> Option<mpsc::Receiver<CommandReceipt>> {
        match self {
            Submission::Accepted { receipts, .. } => Some(receipts),
            Submission::Replayed { .. } => None,
        }
    }
}

/// 相等性只比较语义(Accepted 携带通道,彼此视为相等)。
impl PartialEq for Submission {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Submission::Replayed {
                    request_id: a,
                    state: sa,
                },
                Submission::Replayed {
                    request_id: b,
                    state: sb,
                },
            ) => a == b && sa == sb,
            (Submission::Accepted { .. }, Submission::Accepted { .. }) => true,
            _ => false,
        }
    }
}

/// 本地库中既有回执的状态(§15.2 全集;存储层只保留状态串,不含拒绝码)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredReceiptState {
    /// Relay 语义的 RECEIVED(Bridge 从不写入;重放时可能读到)。
    Received,
    AcceptedByBridge,
    DispatchedToCodex,
    Completed,
    Rejected,
    OutcomeUnknown,
}

fn stored_state_from_status(status: &str) -> StoredReceiptState {
    match status {
        "RECEIVED" => StoredReceiptState::Received,
        "DISPATCHED_TO_CODEX" => StoredReceiptState::DispatchedToCodex,
        "COMPLETED" => StoredReceiptState::Completed,
        "REJECTED" => StoredReceiptState::Rejected,
        "OUTCOME_UNKNOWN" => StoredReceiptState::OutcomeUnknown,
        _ => StoredReceiptState::AcceptedByBridge,
    }
}

/// 进行中命令的调用方通道句柄(shutdown 补终态用)。
struct Inflight {
    tx: mpsc::Sender<CommandReceipt>,
}

/// 写命令网关。克隆廉价(内部全为句柄)。
#[derive(Clone)]
pub struct CommandGateway {
    adapter: CodexAdapter,
    store: LocalStore,
    queue: Arc<QueueManager>,
    caps_provider: Arc<dyn Fn() -> CapabilitySet + Send + Sync>,
    executor: Arc<dyn CommandExecutor>,
    closed: Arc<AtomicBool>,
    /// request_id → 调用方通道;终态回执后移除(shutdown 只补未完成命令)。
    inflight: Arc<Mutex<HashMap<uuid::Uuid, Inflight>>>,
}

impl std::fmt::Debug for CommandGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandGateway")
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .field("inflight", &self.inflight.lock().len())
            .finish_non_exhaustive()
    }
}

impl CommandGateway {
    /// 生产构造:executor 与能力来源均为 adapter。
    pub fn new(adapter: CodexAdapter, store: LocalStore) -> Self {
        let caps_adapter = adapter.clone();
        let executor: Arc<dyn CommandExecutor> = Arc::new(adapter.clone());
        Self::with_parts(
            adapter,
            store,
            executor,
            Arc::new(move || caps_adapter.capabilities()),
        )
    }

    /// 注入执行器(能力来源仍为 adapter;测试 shutdown 语义用)。
    pub fn with_executor(
        adapter: CodexAdapter,
        store: LocalStore,
        executor: Arc<dyn CommandExecutor>,
    ) -> Self {
        let caps_adapter = adapter.clone();
        Self::with_parts(
            adapter,
            store,
            executor,
            Arc::new(move || caps_adapter.capabilities()),
        )
    }

    /// 注入能力来源(测试后台命令能力组合用;§23.2)。
    pub fn with_capability_provider(
        adapter: CodexAdapter,
        store: LocalStore,
        caps_provider: Arc<dyn Fn() -> CapabilitySet + Send + Sync>,
    ) -> Self {
        let executor: Arc<dyn CommandExecutor> = Arc::new(adapter.clone());
        Self::with_parts(adapter, store, executor, caps_provider)
    }

    pub fn with_parts(
        adapter: CodexAdapter,
        store: LocalStore,
        executor: Arc<dyn CommandExecutor>,
        caps_provider: Arc<dyn Fn() -> CapabilitySet + Send + Sync>,
    ) -> Self {
        let queue = QueueManager::new(adapter.clone(), store.clone());
        Self {
            adapter,
            store,
            queue,
            caps_provider,
            executor,
            closed: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 单条下一轮队列管理器(§15.3;队列操作经此或 QueueNextTurn 等 payload)。
    pub fn queue(&self) -> &Arc<QueueManager> {
        &self.queue
    }

    /// 底层 adapter(commands 子模块做快照校验用;不对外暴露原生数据)。
    pub(crate) fn adapter(&self) -> &CodexAdapter {
        &self.adapter
    }

    /// 当前能力集合(submit gate 与 §23.2 能力判断的同一来源)。
    pub fn current_capabilities(&self) -> CapabilitySet {
        (self.caps_provider)()
    }

    /// 是否已进入 shutdown(不再接受新命令)。
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// 提交一条写命令(§15.1/§15.2 全流程)。见模块级文档。
    pub async fn submit(&self, req: CommandRequest) -> Result<Submission, BridgeError> {
        // shutdown:停止接受新命令(§26.4)。对 Browser 语义即设备不再可达。
        if self.closed.load(Ordering::SeqCst) {
            return Err(BridgeError::new(
                StableErrorCode::DeviceOffline,
                "bridge is shutting down; not accepting new commands",
            ));
        }
        let key = req.session_key.clone();
        let request_id = req.request_id;

        // ---- ⓪ agentKind 分发(ZC-02):ZCode 会话只放行 Hook 审批/问答
        // (通常由 runtime 直接决定,不会到这里);其余写能力明确
        // CAPABILITY_UNSUPPORTED,绝不借道 Codex Desktop。
        if key.agent_kind == crate::domain::AgentKind::ZcodeDesktop
            && !matches!(
                req.payload,
                CommandPayload::AnswerQuestion { .. } | CommandPayload::SubmitApproval { .. }
            )
        {
            return Err(BridgeError::new(
                StableErrorCode::CapabilityUnsupported,
                "operation not supported for zcode hook sessions",
            ));
        }

        // ---- ① capability gate(队列操作为 Bridge 本地,豁免) ----
        // Operation::QueueNextTurn 的文档即涵盖设置/替换/取消(§15.3);
        // CancelQueue/PauseQueue 只是 payload 形态,操作层面同属队列命令。
        if req.operation != crate::domain::Operation::QueueNextTurn {
            let caps = (self.caps_provider)();
            if !caps.supports(req.operation) {
                // 与 adapter gate 一致:READ_ONLY → CONTROL_READ_ONLY(§5/§15)。
                if caps.control_mode == crate::domain::ControlMode::ReadOnly {
                    return Err(BridgeError::new(
                        StableErrorCode::ControlReadOnly,
                        "desktop capability probe did not enable this write",
                    ));
                }
                return Err(BridgeError::new(
                    StableErrorCode::CapabilityUnsupported,
                    "operation not supported by current desktop version",
                ));
            }
        }

        // ---- ②/④ request_id 去重 + ACCEPTED_BY_BRIDGE 持久化(幂等 upsert) ----
        let digest = canonical_payload_digest(&req.payload);
        let now = crate::local_store::now_rfc3339();
        let row = RequestReceipt {
            request_id: request_id.to_string(),
            session: SessionKeyRef {
                device_id: key.device_id.clone(),
                agent_kind: key.agent_kind.proto_value() as i64,
                native_session_id: key.native_session_id.clone(),
            },
            operation: operation_name(req.operation),
            status: "ACCEPTED_BY_BRIDGE".to_string(),
            payload_digest: digest.clone(),
            created_at: now.clone(),
            updated_at: now,
        };
        match self.store.record_receipt(&row).await {
            Ok(crate::local_store::ReceiptUpsertOutcome::Inserted) => {}
            Ok(crate::local_store::ReceiptUpsertOutcome::Existing) => {
                // 同 ID 同 payload 重放:返回既有回执,不重复执行(§15.2)。
                let state = self
                    .store
                    .get_receipt(&request_id.to_string())
                    .await
                    .ok()
                    .flatten()
                    .map(|r| stored_state_from_status(&r.status))
                    .unwrap_or(StoredReceiptState::AcceptedByBridge);
                return Ok(Submission::Replayed { request_id, state });
            }
            Err(err) => {
                let code = store_error_code(&err);
                return Err(BridgeError::new(code, err.to_string()));
            }
        }

        // 调用方回执通道与在途登记(shutdown 可补终态)。
        let (tx, rx) = mpsc::channel::<CommandReceipt>(16);
        self.inflight
            .lock()
            .insert(request_id, Inflight { tx: tx.clone() });

        // ---- ③ expected turn/revision 校验(§15.2 STALE_TURN) ----
        // 仅当请求携带期望值或 payload 需要快照(队列绑定)时才取快照,
        // 避免无谓的快照依赖。
        let needs_snapshot = req.expected_turn_id.is_some()
            || req.expected_runtime_revision.is_some()
            || matches!(req.payload, CommandPayload::QueueNextTurn { .. });
        if needs_snapshot {
            match self.adapter.runtime_snapshot(&key).await {
                Err(err) => {
                    // 设备离线/快照不可得:拒绝(队列语义 = CODEX_UNAVAILABLE,
                    // §15.3 设备离线时禁止创建或替换队列)。
                    self.finalize_rejected(request_id, &tx, err.code(), err.to_string())
                        .await;
                    return Ok(accepted(rx));
                }
                Ok(snapshot) => {
                    let current_turn = snapshot.current_turn.as_ref().map(|turn| &turn.turn);
                    if let Some(expected) = req.expected_runtime_revision {
                        if expected != snapshot.runtime_revision
                            && !req.permits_revision_drift(current_turn)
                        {
                            tracing::debug!(
                                operation = ?req.operation,
                                reason = "revision",
                                expected,
                                current = snapshot.runtime_revision,
                                "command precondition stale"
                            );
                            self.finalize_rejected(
                                request_id,
                                &tx,
                                StableErrorCode::StaleTurn,
                                format!(
                                    "runtime revision moved {expected} -> {}",
                                    snapshot.runtime_revision
                                ),
                            )
                            .await;
                            return Ok(accepted(rx));
                        }
                    }
                    if let Some(expected_turn) = req.expected_turn_id.as_ref() {
                        // §15.2:目标 turn 已变化(包括已无当前 turn)即 stale。
                        let still_current = current_turn == Some(expected_turn);
                        if !still_current {
                            tracing::debug!(
                                operation = ?req.operation,
                                reason = "turn",
                                "command precondition stale"
                            );
                            self.finalize_rejected(
                                request_id,
                                &tx,
                                StableErrorCode::StaleTurn,
                                "target turn is no longer current",
                            )
                            .await;
                            return Ok(accepted(rx));
                        }
                    }
                }
            }
        }

        // ---- ⑤/⑥ 分发:队列/后台命令本地处理,其余交给 executor ----
        match req.payload.clone() {
            CommandPayload::QueueNextTurn { input } => {
                let result = self.queue.set(&key, input, false, request_id).await;
                self.finalize_local(request_id, &tx, result).await;
            }
            CommandPayload::CancelQueue => {
                let result = self.queue.cancel(&key).await.map(|_| ());
                self.finalize_local(request_id, &tx, result).await;
            }
            CommandPayload::PauseQueue => {
                let result = self.queue.pause(&key).await.map(|_| ());
                self.finalize_local(request_id, &tx, result).await;
            }
            other => {
                let request = CommandRequest {
                    request_id,
                    operation: req.operation,
                    session_key: key.clone(),
                    expected_turn_id: req.expected_turn_id.clone(),
                    expected_runtime_revision: req.expected_runtime_revision,
                    payload_digest: Some(digest),
                    payload: other,
                };
                match self.executor.execute_command(&key, request).await {
                    Ok(receipts) => {
                        self.spawn_forward(request_id, receipts, tx);
                        return Ok(accepted(rx));
                    }
                    Err(err) => {
                        // §26.4:连接中断无法证明执行 → OUTCOME_UNKNOWN;
                        // 其他分发期错误 → REJECTED{稳定码}。绝不换新 ID 重试。
                        let code = err.code();
                        let state = if code == StableErrorCode::OutcomeUnknown {
                            ReceiptState::OutcomeUnknown
                        } else {
                            ReceiptState::Rejected {
                                code,
                                message: adapter_message(&err),
                            }
                        };
                        self.finalize_receipt(request_id, &tx, state).await;
                    }
                }
            }
        }
        Ok(accepted(rx))
    }

    /// 优雅停机(§26.4):停止接受新命令;对仍未完成的在途命令补
    /// `OUTCOME_UNKNOWN` 终态并落库(无法证明 Desktop 是否执行)。
    /// 返回被补终态的 request_id 列表。
    pub async fn graceful_shutdown(&self) -> Vec<uuid::Uuid> {
        self.closed.store(true, Ordering::SeqCst);
        let pending: Vec<(uuid::Uuid, mpsc::Sender<CommandReceipt>)> = {
            let mut inflight = self.inflight.lock();
            inflight.drain().map(|(id, entry)| (id, entry.tx)).collect()
        };
        let mut finalized = Vec::with_capacity(pending.len());
        for (request_id, tx) in pending {
            let receipt = CommandReceipt {
                request_id,
                state: ReceiptState::OutcomeUnknown,
                at: Some(Utc::now()),
            };
            let _ = self
                .store
                .update_receipt_status(&request_id.to_string(), "OUTCOME_UNKNOWN")
                .await;
            let _ = tx.send(receipt).await;
            finalized.push(request_id);
        }
        finalized
    }

    // -----------------------------------------------------------------
    // 内部
    // -----------------------------------------------------------------

    /// adapter 回执流 → 调用方透传 + 落库;终态后注销在途登记。
    /// 流提前结束且无终态(如 adapter 任务被中止)→ 补 OUTCOME_UNKNOWN。
    fn spawn_forward(
        &self,
        request_id: uuid::Uuid,
        mut receipts: mpsc::Receiver<CommandReceipt>,
        caller: mpsc::Sender<CommandReceipt>,
    ) {
        let store = self.store.clone();
        let inflight = self.inflight.clone();
        tokio::spawn(async move {
            let mut terminal = false;
            while let Some(receipt) = receipts.recv().await {
                terminal = matches!(
                    receipt.state,
                    ReceiptState::Completed
                        | ReceiptState::Rejected { .. }
                        | ReceiptState::OutcomeUnknown
                );
                let _ = store
                    .update_receipt_status(
                        &request_id.to_string(),
                        receipt_status_str(&receipt.state),
                    )
                    .await;
                // 调用方离开也不停:必须把最终状态落库。
                let _ = caller.send(receipt).await;
                if terminal {
                    break;
                }
            }
            if !terminal {
                let _ = store
                    .update_receipt_status(&request_id.to_string(), "OUTCOME_UNKNOWN")
                    .await;
                let _ = caller
                    .send(CommandReceipt {
                        request_id,
                        state: ReceiptState::OutcomeUnknown,
                        at: Some(Utc::now()),
                    })
                    .await;
            }
            inflight.lock().remove(&request_id);
        });
    }

    /// 本地操作(队列)的终态落库与回执。
    async fn finalize_local<T>(
        &self,
        request_id: uuid::Uuid,
        tx: &mpsc::Sender<CommandReceipt>,
        result: Result<T, BridgeError>,
    ) {
        let state = match result {
            Ok(_) => ReceiptState::Completed,
            Err(err) => ReceiptState::Rejected {
                code: err.code,
                message: err.message,
            },
        };
        self.finalize_receipt(request_id, tx, state).await;
    }

    /// 拒绝终态(校验失败/快照不可得)。
    async fn finalize_rejected(
        &self,
        request_id: uuid::Uuid,
        tx: &mpsc::Sender<CommandReceipt>,
        code: StableErrorCode,
        message: impl Into<String>,
    ) {
        self.finalize_receipt(
            request_id,
            tx,
            ReceiptState::Rejected {
                code,
                message: message.into(),
            },
        )
        .await;
    }

    /// 写终态回执:落库 + 发给调用方 + 注销在途登记。
    async fn finalize_receipt(
        &self,
        request_id: uuid::Uuid,
        tx: &mpsc::Sender<CommandReceipt>,
        state: ReceiptState,
    ) {
        let _ = self
            .store
            .update_receipt_status(&request_id.to_string(), receipt_status_str(&state))
            .await;
        let _ = tx
            .send(CommandReceipt {
                request_id,
                state,
                at: Some(Utc::now()),
            })
            .await;
        self.inflight.lock().remove(&request_id);
    }
}

/// ACCEPTED_BY_BRIDGE 提交结果(回执状态由 Accepted 变体本身表达;
/// CommandReceipt 流仅承载 §15.2 的 DispatchedToCodex 及其后续状态)。
fn accepted(receipts: mpsc::Receiver<CommandReceipt>) -> Submission {
    Submission::Accepted {
        accepted_at: Utc::now(),
        receipts,
    }
}

fn adapter_message(err: &AdapterError) -> String {
    match err {
        AdapterError::Stable { message, .. } => message.clone(),
        other => other.to_string(),
    }
}
