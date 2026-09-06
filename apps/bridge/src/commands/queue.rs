//! 单条下一轮队列(权威规格 §15.3/§15.4)。
//!
//! - 每 session 最多一条(存储层约束);正文只存 Bridge SQLite。
//! - `set`/`replace`/`cancel`/`pause`/`resume`/`get`;队列绑定
//!   `after_turn_id` 与接受时的 runtime revision。
//! - 自动发送:监听 adapter 事件——当前 turn 正常 COMPLETED、快照无
//!   PendingAttention、Desktop 未自行启动新 turn → 取队列 → 以**新
//!   request_id** 发送 StartTurn(§26.4:这不是对旧命令换 ID 重试,而是
//!   队列触发的独立新命令)→ 成功后删除队列。
//! - 改 `PAUSED`:turn FAILED/INTERRUPTED、设备断线(发送失败)、Desktop
//!   用户抢先开始新 turn(turn id 变化非我方发起)、ownership 变化
//!   (表现为非我方的新 turn)。`PAUSED` 只能由用户显式
//!   [`QueueManager::resume`] 重新绑定当前 turn/revision,绝不自动改绑。
//! - Browser 关闭不影响已接受队列(队列在 Bridge;watcher 与连接无关)。
//! - 设备离线/adapter 断开:`set`/`replace`/`resume` 拒绝并返回
//!   `CODEX_UNAVAILABLE`(Relay 不代存)。
//!
//! 边界说明(§15.4):发送语义由上层决定的 operation 表达——IDLE 默认
//! `StartTurn`,RUNNING 显式 `QueueNextTurn`;本模块不新增 Operation。
//! 条目的 request_id 存于 cursors 表(`queue-request:{session}`),仅作
//! 回执关联,正文与状态仍在 next_turn_queue。
//!
//! 每个进程每个 store 只应创建一个 QueueManager(以 `Arc` 持有);watcher
//! 任务串行消费单 session 事件,天然避免双发。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::adapter::codex::{AdapterError, CodexAdapter};
use crate::domain::{
    ActiveTurnPhase, BridgeError, CommandPayload, CommandRequest, DomainEvent, LastTurnOutcome,
    Operation, OutputText, QueueState, QueuedTurn, ReceiptState, RuntimeSnapshot, SessionKey,
    StableErrorCode, TurnId,
};
use crate::local_store::{
    LocalStore, NextTurnEntry, QueueStatus as StoreQueueStatus, SessionKeyRef,
};

/// idle 时队列绑定的 after_turn_id 标记(无当前 turn;下一个完成的 turn
/// 触发自动发送;任何新 turn 启动都会使该队列转 PAUSED)。
pub(crate) const IDLE_AFTER_TURN: &str = "idle";

/// 队列管理器。
pub struct QueueManager {
    adapter: CodexAdapter,
    store: LocalStore,
    watchers: Mutex<HashMap<String, ()>>,
    /// 我方自动发送在途的 session(防止把自身触发的 Running 当作抢跑)。
    auto_send_inflight: Mutex<HashSet<String>>,
}

impl std::fmt::Debug for QueueManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueManager")
            .field("watchers", &self.watchers.lock().len())
            .field("auto_send_inflight", &self.auto_send_inflight.lock().len())
            .finish_non_exhaustive()
    }
}

impl QueueManager {
    /// 构造(以 `Arc` 持有,watcher 任务需要共享引用)。
    pub fn new(adapter: CodexAdapter, store: LocalStore) -> Arc<Self> {
        Arc::new(Self {
            adapter,
            store,
            watchers: Mutex::new(HashMap::new()),
            auto_send_inflight: Mutex::new(HashSet::new()),
        })
    }

    /// 设置/排队(§15.3)。`replace = false` 且已存在 QUEUED 条目 →
    /// `QUEUE_ALREADY_EXISTS`;已存在 PAUSED 条目 → `QUEUE_PAUSED`
    /// (须显式 resume 或 replace)。设备离线 → `CODEX_UNAVAILABLE`。
    pub async fn set(
        self: &Arc<Self>,
        key: &SessionKey,
        input: OutputText,
        replace: bool,
        request_id: uuid::Uuid,
    ) -> Result<QueuedTurn, BridgeError> {
        let snapshot = self.snapshot(key).await?;
        let session = session_ref(key);
        if let Some(existing) = self
            .store
            .get_next_turn(&session)
            .await
            .map_err(store_err)?
        {
            if !replace {
                return Err(match existing.status {
                    StoreQueueStatus::Paused => BridgeError::new(
                        StableErrorCode::QueuePaused,
                        "queue entry is paused; resume or replace explicitly",
                    ),
                    _ => BridgeError::new(
                        StableErrorCode::QueueAlreadyExists,
                        "session already has a queued next turn",
                    ),
                });
            }
        }
        let entry = self.entry_from_snapshot(key, &snapshot, input.as_str());
        retry_busy(|| self.store.set_next_turn(&entry, replace))
            .await
            .map_err(store_err)?;
        self.remember_request_id(key, request_id).await;
        self.ensure_watcher(key).await;
        Ok(self.domain_turn(&entry, request_id, input))
    }

    /// 替换既有条目(§15.3 支持替换)。等价 `set(replace = true)`。
    pub async fn replace(
        self: &Arc<Self>,
        key: &SessionKey,
        input: OutputText,
        request_id: uuid::Uuid,
    ) -> Result<QueuedTurn, BridgeError> {
        self.set(key, input, true, request_id).await
    }

    /// 读取当前队列条目。
    pub async fn get(
        self: &Arc<Self>,
        key: &SessionKey,
    ) -> Result<Option<QueuedTurn>, BridgeError> {
        let session = session_ref(key);
        let Some(entry) = self
            .store
            .get_next_turn(&session)
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        let request_id = self.stored_request_id(key).await;
        Ok(Some(self.domain_turn(
            &entry,
            request_id,
            OutputText::new(entry.prompt.clone()),
        )))
    }

    /// 摘要形态的队列状态(§10.6)。
    pub async fn queue_status(
        self: &Arc<Self>,
        key: &SessionKey,
    ) -> Result<crate::domain::QueueStatus, BridgeError> {
        let session = session_ref(key);
        let Some(entry) = self
            .store
            .get_next_turn(&session)
            .await
            .map_err(store_err)?
        else {
            return Ok(crate::domain::QueueStatus::default());
        };
        Ok(crate::domain::QueueStatus {
            state: store_state_to_domain(entry.status),
            after_turn_id: Some(turn_id_of_str(&entry.after_turn_id)),
            accepted_runtime_revision: Some(entry.runtime_revision as u64),
        })
    }

    /// 取消/清除队列;返回是否确有删除。
    pub async fn cancel(self: &Arc<Self>, key: &SessionKey) -> Result<bool, BridgeError> {
        let session = session_ref(key);
        retry_busy(|| self.store.clear_next_turn(&session))
            .await
            .map_err(store_err)
    }

    /// 用户显式暂停(保留绑定);返回是否确有暂停。幂等。
    pub async fn pause(self: &Arc<Self>, key: &SessionKey) -> Result<bool, BridgeError> {
        let session = session_ref(key);
        let Some(mut entry) = self
            .store
            .get_next_turn(&session)
            .await
            .map_err(store_err)?
        else {
            return Ok(false);
        };
        if entry.status == StoreQueueStatus::Paused {
            return Ok(false);
        }
        entry.status = StoreQueueStatus::Paused;
        entry.updated_at = crate::local_store::now_rfc3339();
        retry_busy(|| self.store.set_next_turn(&entry, true))
            .await
            .map_err(store_err)?;
        Ok(true)
    }

    /// 用户显式重新确认(§15.3:PAUSED 只能由用户重新确认,不能自动改绑):
    /// 重新绑定当前快照的 turn/revision 后回到 QUEUED;无队列条目返回 None。
    pub async fn resume(
        self: &Arc<Self>,
        key: &SessionKey,
    ) -> Result<Option<QueuedTurn>, BridgeError> {
        let session = session_ref(key);
        let Some(mut entry) = self
            .store
            .get_next_turn(&session)
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        let snapshot = self.snapshot(key).await?;
        let rebound = self.entry_from_snapshot(key, &snapshot, &entry.prompt);
        entry.after_turn_id = rebound.after_turn_id;
        entry.runtime_revision = rebound.runtime_revision;
        entry.status = StoreQueueStatus::Queued;
        entry.updated_at = crate::local_store::now_rfc3339();
        retry_busy(|| self.store.set_next_turn(&entry, true))
            .await
            .map_err(store_err)?;
        self.ensure_watcher(key).await;
        let request_id = self.stored_request_id(key).await;
        let queued = self.domain_turn(&entry, request_id, OutputText::new(entry.prompt.clone()));
        // 空闲且无 attention 时,重新确认即满足自动发送条件,立即发送。
        self.maybe_auto_send(key).await;
        Ok(Some(queued))
    }

    // -----------------------------------------------------------------
    // 事件驱动(自动发送 / 暂停)
    // -----------------------------------------------------------------

    /// 登记事件 watcher(每 session 一次;同步完成订阅,保证 set/resume
    /// 返回后该 session 的事件必达;Browser 断开不影响)。
    async fn ensure_watcher(self: &Arc<Self>, key: &SessionKey) {
        let native = key.native_session_id.clone();
        if self.watchers.lock().contains_key(&native) {
            return;
        }
        self.watchers.lock().insert(native.clone(), ());
        let (tx, mut rx) = mpsc::channel::<DomainEvent>(512);
        if self.adapter.subscribe(key, tx.clone()).await.is_err() {
            self.watchers.lock().remove(&native);
            return;
        }
        let manager = Arc::clone(self);
        let watch_key = key.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                manager.on_event(&watch_key, event).await;
            }
            manager.watchers.lock().remove(&native);
        });
    }

    /// 事件分派:终态 → 自动发送/暂停;Running → 抢跑检测。
    async fn on_event(self: &Arc<Self>, key: &SessionKey, event: DomainEvent) {
        match event {
            DomainEvent::TurnLifecycle {
                phase: ActiveTurnPhase::Idle,
                outcome: Some(LastTurnOutcome::Completed),
                ..
            } => {
                self.auto_send_inflight
                    .lock()
                    .remove(&key.native_session_id);
                self.maybe_auto_send(key).await;
            }
            DomainEvent::TurnLifecycle {
                phase: ActiveTurnPhase::Idle,
                outcome: Some(LastTurnOutcome::Failed | LastTurnOutcome::Interrupted),
                ..
            } => {
                self.auto_send_inflight
                    .lock()
                    .remove(&key.native_session_id);
                self.mark_paused(&session_ref(key)).await;
            }
            DomainEvent::TurnLifecycle {
                turn,
                phase: ActiveTurnPhase::Running | ActiveTurnPhase::Finishing,
                ..
            } => {
                // 我方自动发送触发的第一个 Running 事件:放行。
                if self
                    .auto_send_inflight
                    .lock()
                    .remove(&key.native_session_id)
                {
                    return;
                }
                // 抢跑/ownership 变化:绑定之外的 turn 启动 → PAUSED(§15.3)。
                let session = session_ref(key);
                let Ok(Some(entry)) = self.store.get_next_turn(&session).await else {
                    return;
                };
                if entry.status == StoreQueueStatus::Queued && entry.after_turn_id != turn.id {
                    self.mark_paused(&session).await;
                }
            }
            _ => {}
        }
    }

    /// 自动发送判定:条目 QUEUED + 快照 idle + 无 attention → 发送。
    async fn maybe_auto_send(self: &Arc<Self>, key: &SessionKey) {
        let session = session_ref(key);
        let Ok(Some(entry)) = self.store.get_next_turn(&session).await else {
            return;
        };
        if entry.status != StoreQueueStatus::Queued {
            return;
        }
        let Ok(snapshot) = self.adapter.runtime_snapshot(key).await else {
            // 设备断线:改 PAUSED(§15.3),只能用户重新确认。
            self.mark_paused(&session).await;
            return;
        };
        if snapshot.current_turn.is_some() {
            // Desktop 已启动新 turn(非我方):Running 事件路径负责 PAUSED。
            return;
        }
        if !snapshot.pending_questions.is_empty() || !snapshot.pending_approvals.is_empty() {
            return; // 有 attention:等待。
        }
        // 以新 request_id 发送队列正文(§26.4:非重试,是队列触发的新命令)。
        let request_id = uuid::Uuid::new_v4();
        let request = CommandRequest {
            request_id,
            operation: Operation::StartTurn,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: Some(super::canonical_payload_digest(
                &CommandPayload::StartTurn {
                    input: OutputText::new(entry.prompt.clone()),
                },
            )),
            payload: CommandPayload::StartTurn {
                input: OutputText::new(entry.prompt.clone()),
            },
        };
        self.auto_send_inflight
            .lock()
            .insert(key.native_session_id.clone());
        match self.adapter.execute_command(key, request).await {
            Ok(mut receipts) => {
                let mut sent = false;
                while let Some(receipt) = receipts.recv().await {
                    let _ = self
                        .store
                        .update_receipt_status(
                            &request_id.to_string(),
                            receipt_status_str(&receipt.state),
                        )
                        .await;
                    match receipt.state {
                        ReceiptState::DispatchedToCodex => {}
                        ReceiptState::Completed => {
                            sent = true;
                            break;
                        }
                        ReceiptState::Rejected { .. } | ReceiptState::OutcomeUnknown => break,
                    }
                }
                if sent {
                    // 成功后删除队列(§15.3 自动发送语义);删除失败必须重试,
                    // 否则队列残留会在下个事件造成二次发送。
                    if let Err(err) = retry_busy(|| self.store.clear_next_turn(&session)).await {
                        tracing::error!(error = %err, "queue clear after auto-send failed");
                    }
                } else {
                    self.mark_paused(&session).await;
                }
            }
            Err(err) => {
                let status = if err.code() == StableErrorCode::OutcomeUnknown {
                    "OUTCOME_UNKNOWN"
                } else {
                    "REJECTED"
                };
                let _ = self
                    .store
                    .update_receipt_status(&request_id.to_string(), status)
                    .await;
                self.mark_paused(&session).await;
            }
        }
        self.auto_send_inflight
            .lock()
            .remove(&key.native_session_id);
    }

    /// QUEUED → PAUSED(只能用户显式重新确认;§15.3)。
    async fn mark_paused(&self, session: &SessionKeyRef) {
        let Ok(Some(mut entry)) = self.store.get_next_turn(session).await else {
            return;
        };
        if entry.status != StoreQueueStatus::Queued {
            return;
        }
        entry.status = StoreQueueStatus::Paused;
        entry.updated_at = crate::local_store::now_rfc3339();
        // PAUSED 必须落库:busy 时有界重试,失败仅告警(下次事件重试)。
        if let Err(err) = retry_busy(|| self.store.set_next_turn(&entry, true)).await {
            tracing::warn!(error = %err, "queue mark_paused failed");
        }
    }

    // -----------------------------------------------------------------
    // 条目/形态辅助
    // -----------------------------------------------------------------

    fn entry_from_snapshot(
        &self,
        key: &SessionKey,
        snapshot: &RuntimeSnapshot,
        prompt: &str,
    ) -> NextTurnEntry {
        let now = crate::local_store::now_rfc3339();
        NextTurnEntry {
            session: session_ref(key),
            prompt: prompt.to_string(),
            after_turn_id: snapshot
                .current_turn
                .as_ref()
                .map(|t| t.turn.id.clone())
                .unwrap_or_else(|| IDLE_AFTER_TURN.to_string()),
            runtime_revision: snapshot.runtime_revision as i64,
            status: StoreQueueStatus::Queued,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    fn domain_turn(
        &self,
        entry: &NextTurnEntry,
        request_id: uuid::Uuid,
        input: OutputText,
    ) -> QueuedTurn {
        QueuedTurn {
            request_id,
            input,
            after_turn_id: turn_id_of_str(&entry.after_turn_id),
            accepted_runtime_revision: entry.runtime_revision as u64,
            state: store_state_to_domain(entry.status),
            accepted_at: entry.updated_at.parse::<chrono::DateTime<Utc>>().ok(),
        }
    }

    async fn snapshot(&self, key: &SessionKey) -> Result<RuntimeSnapshot, BridgeError> {
        self.adapter
            .runtime_snapshot(key)
            .await
            .map_err(|err| BridgeError::new(err.code(), adapter_message(&err)))
    }

    fn cursor_name(key: &SessionKey) -> String {
        format!("queue-request:{}", key.native_session_id)
    }

    async fn remember_request_id(&self, key: &SessionKey, request_id: uuid::Uuid) {
        let _ = self
            .store
            .put_cursor(&Self::cursor_name(key), &request_id.to_string())
            .await;
    }

    async fn stored_request_id(&self, key: &SessionKey) -> uuid::Uuid {
        self.store
            .get_cursor(&Self::cursor_name(key))
            .await
            .ok()
            .flatten()
            .and_then(|v| uuid::Uuid::parse_str(&v).ok())
            .unwrap_or_else(uuid::Uuid::nil)
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn session_ref(key: &SessionKey) -> SessionKeyRef {
    SessionKeyRef {
        device_id: key.device_id.clone(),
        agent_kind: key.agent_kind.proto_value() as i64,
        native_session_id: key.native_session_id.clone(),
    }
}

fn store_err(err: crate::local_store::StoreError) -> BridgeError {
    let code = super::store_error_code(&err);
    BridgeError::new(code, err.to_string())
}

fn adapter_message(err: &AdapterError) -> String {
    match err {
        AdapterError::Stable { message, .. } => message.clone(),
        other => other.to_string(),
    }
}

fn receipt_status_str(state: &ReceiptState) -> &'static str {
    match state {
        ReceiptState::DispatchedToCodex => "DISPATCHED_TO_CODEX",
        ReceiptState::Completed => "COMPLETED",
        ReceiptState::Rejected { .. } => "REJECTED",
        ReceiptState::OutcomeUnknown => "OUTCOME_UNKNOWN",
    }
}

fn store_state_to_domain(status: StoreQueueStatus) -> QueueState {
    match status {
        StoreQueueStatus::Queued => QueueState::Queued,
        StoreQueueStatus::Paused => QueueState::Paused,
        StoreQueueStatus::Empty => QueueState::Empty,
    }
}

fn turn_id_of_str(raw: &str) -> TurnId {
    if raw == IDLE_AFTER_TURN {
        TurnId::synthetic(IDLE_AFTER_TURN)
    } else {
        TurnId::native(raw)
    }
}

/// 有界 busy 重试:SQLite deferred 事务在并发写下会返回
/// `database is locked`(读后写快照升级冲突,busy_timeout 不覆盖);
/// 队列状态写入(排队/暂停/清除)必须成功,对这一类错误做小退避重试。
async fn retry_busy<T, F, Fut>(mut op: F) -> Result<T, crate::local_store::StoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::local_store::StoreError>>,
{
    const MAX_ATTEMPTS: usize = 10;
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let busy = matches!(&err, crate::local_store::StoreError::Sqlx(e)
                    if e.to_string().contains("database is locked"));
                attempt += 1;
                if !busy || attempt >= MAX_ATTEMPTS {
                    return Err(err);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10 * attempt as u64)).await;
            }
        }
    }
}
