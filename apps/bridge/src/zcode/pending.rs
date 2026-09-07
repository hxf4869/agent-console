//! Bridge 内存 pending 注册表(04 §8.5/§8.6)。
//!
//! 状态至少区分:等待(Waiting)→ 提交中(Decided,原子锁定决定)→
//! 已返回运行时(ReturnedToRuntime)→ 运行时已处理(RuntimeProcessed,
//! 可由 PostToolUse 观察核实时);过期(Expired)/已在本机处理
//! (HandledLocally)为终态。
//!
//! 并发语义:
//! - `resolve` 是唯一决定入口;单一 Mutex 内完成「仍存活校验 + 状态迁移 +
//!   取走应答通道」,第一个仍有效的决定成功,迟到回复一律拒绝。
//! - deadline 到期后 `resolve` 一律失败(超时与点击同时到达以本注册表的
//!   单一原子状态判断,不以浏览器倒计时为准)。
//! - 注册表只在内存:Bridge 重启后旧 invoke 全部失效,不存在复活路径。
//!
//! 隐私:记录不持有 toolInput/ask 正文;Debug 实现只输出稳定维度(§25.3)。

use std::collections::HashMap;
use std::fmt;
use parking_lot::Mutex;
use std::time::Instant;

use tokio::sync::oneshot;

use super::contract::{HookReply, AskRequest};

/// invoke 种类(决定应答语义与远程卡片类型)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvokeKind {
    /// ZCode 原生 PermissionRequest。
    PermissionRequest,
    /// MCP `agent_console.ask_user`。
    AskUser,
}

/// pending 生命周期状态(04 §8.6 UI 状态最小集)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingState {
    /// 等待用户决定。
    Waiting,
    /// 提交中:决定已原子锁定,尚未送达 helper。
    Decided,
    /// 决定已写回 helper(stdout 已输出/已送达)。
    ReturnedToRuntime,
    /// 运行时已处理(PostToolUse 等原生事件可核实时)。
    RuntimeProcessed,
    /// 过期(deadline 到期未决定)。
    Expired,
    /// 已在本机处理:原生取消 / helper 消失 / Bridge 停机前失效。
    HandledLocally,
}

impl PendingState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            PendingState::ReturnedToRuntime
                | PendingState::RuntimeProcessed
                | PendingState::Expired
                | PendingState::HandledLocally
        )
    }
}

/// 注册表内的 pending 记录:只存稳定维度,不存 toolInput 正文。
pub struct PendingRecord {
    pub invoke_id: String,
    pub kind: InvokeKind,
    pub native_session_id: Option<String>,
    pub tool_name: Option<String>,
    pub tool_use_id: Option<String>,
    /// ask 卡片的问题原文(快照镜像重建用;内存内,不进日志)。
    pub(crate) ask_question: Option<String>,
    /// ask 卡片的选项原文(决定回传映射用;内存内,不进日志)。
    pub(crate) ask_options: Vec<String>,
    /// ask 是否允许补充文本。
    pub(crate) ask_allow_free_text: bool,
    /// 审批展示用的操作摘要(登记时有界化,≤400+40 字符;仅本机内存、
    /// 不进日志/不落盘;快照与实时卡片一致,P1-5)。Debug 不输出。
    pub(crate) action_summary: Option<String>,
    pub state: PendingState,
    pub created_at: Instant,
    pub deadline: Instant,
    /// 唯一回复通道;resolve 时取走。
    responder: Option<oneshot::Sender<HookReply>>,
    /// 交付确认通道(R2-ZC01):resolve 锁定决定后,命令面经
    /// `take_delivery` 取走接收端有限等待;socket 服务在写回+确认结局
    /// 明确后经 `complete_delivery` 恰好发送一次。
    delivery_tx: Option<oneshot::Sender<DeliveryOutcome>>,
    delivery_rx: Option<oneshot::Receiver<DeliveryOutcome>>,
}

impl fmt::Debug for PendingRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 手写 Debug:不输出 ask_options/正文(§25.3)。
        f.debug_struct("PendingRecord")
            .field("invoke_id", &self.invoke_id)
            .field("kind", &self.kind)
            .field(
                "native_session_id",
                &self.native_session_id.as_deref().map(|_| "set"),
            )
            .field("tool_name", &self.tool_name)
            .field("tool_use_id", &self.tool_use_id)
            .field("state", &self.state)
            .field("deadline_in_ms", &(self.deadline.saturating_duration_since(Instant::now()).as_millis() as i64))
            .finish_non_exhaustive()
    }
}

/// 注册表操作错误(稳定维度;不携带正文)。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PendingError {
    #[error("unknown or stale invoke (bridge may have restarted)")]
    Unknown,
    #[error("invoke already decided")]
    AlreadyDecided,
    #[error("invoke expired")]
    Expired,
    #[error("invoke was cancelled or its helper is gone")]
    Cancelled,
    #[error("duplicate invoke id")]
    Duplicate,
    #[error("invoke payload rejected: {0}")]
    Rejected(String),
}

/// 本地决定的交付确认结果(R2-ZC01)。确认只证明「原生协议结果已确认
/// 输出」(helper stdout / MCP JSON-RPC 写出+flush),不证明原生工具已执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// helper 已确认完成原生协议输出。
    Delivered,
    /// 写回失败 / 确认缺失、迟到、绑定不符或超时:结果不确定,
    /// 不得报成功(命令面按 OUTCOME_UNKNOWN 处理)。
    Unconfirmed,
}

/// 内存 pending 注册表。
#[derive(Default)]
pub struct PendingRegistry {
    inner: Mutex<HashMap<String, PendingRecord>>,
}

impl PendingRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记 invoke,返回决定接收通道。同一 invoke_id 重复登记拒绝
    /// (防同一 Hook 重复注册导致的双卡片在 Bridge 侧兜底)。
    pub fn register(
        &self,
        invoke_id: &str,
        kind: InvokeKind,
        native_session_id: Option<String>,
        tool_name: Option<String>,
        tool_use_id: Option<String>,
        ask: Option<&AskRequest>,
        action_summary: Option<String>,
        wait: std::time::Duration,
    ) -> Result<oneshot::Receiver<HookReply>, PendingError> {
        let now = Instant::now();
        let (tx, rx) = oneshot::channel();
        let (delivery_tx, delivery_rx) = oneshot::channel();
        let mut inner = self.inner.lock();
        if inner.contains_key(invoke_id) {
            return Err(PendingError::Duplicate);
        }
        // 顺带清理已过期终态记录,限制内存(过期即移除,迟到查询得 Unknown)。
        inner.retain(|_, record| record.deadline > now || !record.state.is_terminal());
        inner.insert(
            invoke_id.to_string(),
            PendingRecord {
                invoke_id: invoke_id.to_string(),
                kind,
                native_session_id,
                tool_name,
                tool_use_id,
                ask_question: ask.map(|a| a.question.clone()),
                ask_options: ask.map(|a| a.options.clone()).unwrap_or_default(),
                ask_allow_free_text: ask.map(|a| a.allow_free_text).unwrap_or(false),
                action_summary,
                state: PendingState::Waiting,
                created_at: now,
                deadline: now + wait,
                responder: Some(tx),
                delivery_tx: Some(delivery_tx),
                delivery_rx: Some(delivery_rx),
            },
        );
        Ok(rx)
    }

    /// 原子决定:仅在 Waiting 且未过 deadline 时成功;第一个仍有效决定胜出。
    pub fn resolve(&self, invoke_id: &str, reply: HookReply) -> Result<PendingState, PendingError> {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        let record = inner.get_mut(invoke_id).ok_or(PendingError::Unknown)?;
        match record.state {
            PendingState::Waiting => {}
            PendingState::Decided => return Err(PendingError::AlreadyDecided),
            PendingState::Expired => return Err(PendingError::Expired),
            PendingState::HandledLocally => return Err(PendingError::Cancelled),
            PendingState::ReturnedToRuntime | PendingState::RuntimeProcessed => {
                return Err(PendingError::AlreadyDecided)
            }
        }
        if now >= record.deadline {
            record.state = PendingState::Expired;
            return Err(PendingError::Expired);
        }
        let Some(responder) = record.responder.take() else {
            // 通道已消失(helper 连接断开且被 cancel 清理)→ 本机已处理。
            record.state = PendingState::HandledLocally;
            return Err(PendingError::Cancelled);
        };
        record.state = PendingState::Decided;
        // 发送失败即对端已消失:标记本机已处理,调用方不得视为已授权。
        if responder.send(reply).is_err() {
            record.state = PendingState::HandledLocally;
            return Err(PendingError::Cancelled);
        }
        Ok(record.state)
    }

    /// 取走交付确认接收端(命令面在 `resolve` 成功后调用一次;二次调用
    /// 返回 None,按结果未知处理)。
    pub fn take_delivery(
        &self,
        invoke_id: &str,
    ) -> Option<oneshot::Receiver<DeliveryOutcome>> {
        let mut inner = self.inner.lock();
        let record = inner.get_mut(invoke_id)?;
        record.delivery_rx.take()
    }

    /// 交付确认完成(R2-ZC01,socket 服务恰好调用一次):
    /// - `Delivered`:Decided → ReturnedToRuntime(既有语义:已确认输出
    ///   原生协议结果);
    /// - `Unconfirmed`:Decided → HandledLocally(写回失败/确认缺失或
    ///   超时,结果不确定,不得报成功)。
    /// 同时向命令面等待者发送结果;记录已不在/状态不合法时返回错误并
    /// 忽略(确认迟到不影响既有终态)。
    pub fn complete_delivery(
        &self,
        invoke_id: &str,
        outcome: DeliveryOutcome,
    ) -> Result<PendingState, PendingError> {
        let mut inner = self.inner.lock();
        let record = inner.get_mut(invoke_id).ok_or(PendingError::Unknown)?;
        if record.state != PendingState::Decided {
            return Err(PendingError::AlreadyDecided);
        }
        record.state = match outcome {
            DeliveryOutcome::Delivered => PendingState::ReturnedToRuntime,
            DeliveryOutcome::Unconfirmed => PendingState::HandledLocally,
        };
        if let Some(tx) = record.delivery_tx.take() {
            let _ = tx.send(outcome);
        }
        Ok(record.state)
    }

    /// 运行时已处理(PostToolUse 等原生事件核实后)。
    pub fn mark_runtime_processed(&self, invoke_id: &str) -> Result<(), PendingError> {
        self.transition(
            invoke_id,
            PendingState::ReturnedToRuntime,
            PendingState::RuntimeProcessed,
        )
    }

    /// deadline 到期:仅 Waiting → Expired(Decided 之后到达的到期不影响
    /// 已锁定的决定)。
    pub fn expire(&self, invoke_id: &str) -> Result<PendingState, PendingError> {
        let mut inner = self.inner.lock();
        let record = inner.get_mut(invoke_id).ok_or(PendingError::Unknown)?;
        if record.state != PendingState::Waiting {
            return Err(PendingError::AlreadyDecided);
        }
        record.state = PendingState::Expired;
        Ok(PendingState::Expired)
    }

    /// 超时收尾的原子判定:「检查 + 迁移」在同一把锁内完成,消除探针与
    /// `expire` 两步之间被迟到 `resolve` 插入的竞态窗口:
    /// - Waiting → 置 Expired,返回 true(正常超时过期);
    /// - 已是 Expired(`resolve` 恰在 deadline 之后到达时已置终态并拒绝
    ///   决定,该路径不摘卡)→ 返回 true,卡片/等待标记仍须由调用方清理
    ///   恰好一次;
    /// - 其余状态(Decided/HandledLocally/ReturnedToRuntime/
    ///   RuntimeProcessed)或记录不存在 → false,由调用方竞态兜底
    ///   (`finalize_decided_after_race`)或既有 Unknown 语义处理。
    pub fn expire_or_already_expired(&self, invoke_id: &str) -> bool {
        let mut inner = self.inner.lock();
        match inner.get_mut(invoke_id) {
            Some(record) => match record.state {
                PendingState::Waiting | PendingState::Expired => {
                    record.state = PendingState::Expired;
                    true
                }
                _ => false,
            },
            None => false,
        }
    }

    /// 决定与超时/连接消失竞态(`tokio::select!` 同时就绪时随机分支)的
    /// 终态化:决定已被 `resolve` 原子锁定(Decided),但应答通道已被 select
    /// 落选分支丢弃,决定无法再送达 helper。把停留在 Decided 的记录转入
    /// Expired 终态:
    /// - 迟到查询/决定与「超时后点击」走同一条 Expired 拒绝路径(统一
    ///   APPROVAL_EXPIRED),不弱化原子决定/迟到拒绝语义;
    /// - 记录可被 register 的既有 retain 清理(此前过期 Decided 记录因
    ///   非 terminal 且 deadline 已过而永久滞留内存)。
    /// 仅命中 Decided;其余状态(含 Waiting)返回 false,由调用方原路径处理。
    pub fn finalize_decided_after_race(&self, invoke_id: &str) -> bool {
        let mut inner = self.inner.lock();
        match inner.get_mut(invoke_id) {
            Some(record) if record.state == PendingState::Decided => {
                record.state = PendingState::Expired;
                true
            }
            _ => false,
        }
    }

    /// 原生取消 / helper 消失 / Bridge 停机:Waiting → HandledLocally,
    /// 撤销远程请求并拒绝一切迟到回复。
    pub fn cancel(&self, invoke_id: &str) -> Result<PendingState, PendingError> {
        let mut inner = self.inner.lock();
        let record = inner.get_mut(invoke_id).ok_or(PendingError::Unknown)?;
        if record.state != PendingState::Waiting {
            return Err(PendingError::AlreadyDecided);
        }
        record.state = PendingState::HandledLocally;
        record.responder.take(); // 丢弃通道:后续 resolve 走 Cancelled。
        Ok(PendingState::HandledLocally)
    }

    /// 查询(稳定维度快照)。
    pub fn get(&self, invoke_id: &str) -> Option<PendingSnapshot> {
        let inner = self.inner.lock();
        inner
            .get(invoke_id)
            .map(|record| PendingSnapshot {
                invoke_id: record.invoke_id.clone(),
                kind: record.kind,
                state: record.state,
                native_session_id: record.native_session_id.clone(),
            })
    }

    /// 全部等待决定中的卡片(稳定维度;ZC-02 RuntimeSnapshot/摘要镜像用)。
    pub fn waiting_cards(&self) -> Vec<WaitingCard> {
        let inner = self.inner.lock();
        let mut cards: Vec<WaitingCard> = inner
            .values()
            .filter(|record| record.state == PendingState::Waiting)
            .map(|record| WaitingCard {
                invoke_id: record.invoke_id.clone(),
                kind: record.kind,
                native_session_id: record.native_session_id.clone(),
                tool_name: record.tool_name.clone(),
                question: record.ask_question.clone(),
                options: record.ask_options.clone(),
                allow_free_text: record.ask_allow_free_text,
                action_summary: record.action_summary.clone(),
            })
            .collect();
        cards.sort_by(|a, b| a.invoke_id.cmp(&b.invoke_id));
        cards
    }

    /// ask 是否允许补充文本。
    pub fn ask_allows_free_text(&self, invoke_id: &str) -> Option<bool> {
        let inner = self.inner.lock();
        let record = inner.get(invoke_id)?;
        if record.kind != InvokeKind::AskUser {
            return None;
        }
        Some(record.ask_allow_free_text)
    }

    /// 按 tool_use_id 找已返回运行时的 invoke(PostToolUse 核实用)。
    pub fn find_returned_by_tool_use(&self, tool_use_id: &str) -> Option<String> {
        let inner = self.inner.lock();
        inner
            .values()
            .find(|record| {
                record.tool_use_id.as_deref() == Some(tool_use_id)
                    && record.state == PendingState::ReturnedToRuntime
            })
            .map(|record| record.invoke_id.clone())
    }

    /// ask 选项原文映射(option_id = "option-{i+1}")。
    pub fn ask_option_label(&self, invoke_id: &str, option_id: &str) -> Option<String> {
        let inner = self.inner.lock();
        let record = inner.get(invoke_id)?;
        if record.kind != InvokeKind::AskUser {
            return None;
        }
        let index: usize = option_id.strip_prefix("option-")?.parse().ok()?;
        let label = record.ask_options.get(index.checked_sub(1)?)?;
        Some(label.clone())
    }

    /// 当前记录数(测试/诊断)。
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    fn transition(
        &self,
        invoke_id: &str,
        from: PendingState,
        to: PendingState,
    ) -> Result<(), PendingError> {
        let mut inner = self.inner.lock();
        let record = inner.get_mut(invoke_id).ok_or(PendingError::Unknown)?;
        if record.state != from {
            return Err(PendingError::AlreadyDecided);
        }
        record.state = to;
        Ok(())
    }
}

/// 注册表快照(稳定维度;供测试与诊断)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSnapshot {
    pub invoke_id: String,
    pub kind: InvokeKind,
    pub state: PendingState,
    pub native_session_id: Option<String>,
}

/// 等待决定中的卡片(稳定维度;ask 选项/问题原文仅用于重建远程卡片,
/// 不进日志)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingCard {
    pub invoke_id: String,
    pub kind: InvokeKind,
    pub native_session_id: Option<String>,
    pub tool_name: Option<String>,
    pub question: Option<String>,
    pub options: Vec<String>,
    pub allow_free_text: bool,
    /// 审批操作摘要(登记时有界化;仅快照/卡片重建用,不进日志)。
    pub action_summary: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn short_wait() -> Duration {
        Duration::from_millis(50)
    }

    fn ask() -> AskRequest {
        AskRequest {
            question: "继续?".to_string(),
            options: vec!["是".to_string(), "否".to_string()],
            allow_free_text: true,
            call_id: Some("call-1".to_string()),
        }
    }

    #[tokio::test]
    async fn resolve_is_atomic_first_valid_decision_wins() {
        let registry = PendingRegistry::new();
        let rx = registry
            .register("i1", InvokeKind::PermissionRequest, None, Some("Bash".into()), None, None, None, short_wait())
            .unwrap();
        registry.resolve("i1", HookReply::allowed()).unwrap();
        // 第二个决定必须失败(原子锁定)。
        assert_eq!(
            registry.resolve("i1", HookReply::denied("late")).unwrap_err(),
            PendingError::AlreadyDecided
        );
        assert_eq!(rx.await.unwrap().status, "allowed");
    }

    #[tokio::test]
    async fn resolve_after_deadline_is_expired() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i2", InvokeKind::PermissionRequest, None, None, None, None, None, short_wait())
            .unwrap();
        tokio::time::sleep(short_wait() + Duration::from_millis(30)).await;
        assert_eq!(
            registry.resolve("i2", HookReply::allowed()).unwrap_err(),
            PendingError::Expired
        );
        // 到期同时点击:以注册表单一原子状态为准 → Expired。
        assert_eq!(registry.expire("i2").unwrap_err(), PendingError::AlreadyDecided);
    }

    #[test]
    fn expire_then_resolve_rejected_and_states_visible() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i3", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        assert_eq!(registry.expire("i3").unwrap(), PendingState::Expired);
        assert_eq!(
            registry.resolve("i3", HookReply::allowed()).unwrap_err(),
            PendingError::Expired
        );
        assert_eq!(
            registry.get("i3").unwrap().state,
            PendingState::Expired
        );
    }

    /// 超时收尾原子判定的全状态矩阵:Waiting → true 且置 Expired;
    /// 已是 Expired(resolve 在 deadline 后置终态、决定被拒且不摘卡的
    /// 路径)→ true,收尾仍须恰好一次;Decided/HandledLocally → false
    /// (Decided 交给 raced 兜底);记录不存在 → false。
    #[test]
    fn expire_or_already_expired_is_atomic_per_state() {
        // Waiting → true 且状态迁移为 Expired。
        let waiting = PendingRegistry::new();
        let _rx = waiting
            .register("eoa-waiting", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        assert!(waiting.expire_or_already_expired("eoa-waiting"));
        assert_eq!(
            waiting.get("eoa-waiting").unwrap().state,
            PendingState::Expired
        );

        // 已被置为 Expired(resolve-after-deadline 终态等价:Expired 且
        // responder 未取走)→ true,且状态保持 Expired 不再变化。
        let expired = PendingRegistry::new();
        let _rx = expired
            .register("eoa-expired", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        expired.expire("eoa-expired").unwrap();
        assert!(expired.expire_or_already_expired("eoa-expired"));
        assert_eq!(
            expired.get("eoa-expired").unwrap().state,
            PendingState::Expired
        );

        // Decided → false(由 raced 兜底路径收尾)。
        let decided = PendingRegistry::new();
        let _rx = decided
            .register("eoa-decided", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        decided.resolve("eoa-decided", HookReply::allowed()).unwrap();
        assert!(!decided.expire_or_already_expired("eoa-decided"));
        assert_eq!(
            decided.get("eoa-decided").unwrap().state,
            PendingState::Decided
        );

        // HandledLocally → false。
        let handled = PendingRegistry::new();
        let _rx = handled
            .register("eoa-handled", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        handled.cancel("eoa-handled").unwrap();
        assert!(!handled.expire_or_already_expired("eoa-handled"));
        assert_eq!(
            handled.get("eoa-handled").unwrap().state,
            PendingState::HandledLocally
        );

        // 记录不存在(Unknown)→ false。
        let unknown = PendingRegistry::new();
        assert!(!unknown.expire_or_already_expired("eoa-missing"));
    }

    #[test]
    fn cancel_rejects_late_replies() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i4", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        assert_eq!(registry.cancel("i4").unwrap(), PendingState::HandledLocally);
        assert_eq!(
            registry.resolve("i4", HookReply::allowed()).unwrap_err(),
            PendingError::Cancelled
        );
    }

    #[test]
    fn unknown_invoke_after_restart_is_unknown() {
        // Bridge 重启 = 新注册表:旧 invoke 一律失效,不存在复活路径。
        let old = PendingRegistry::new();
        let _rx = old
            .register("i5", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        let restarted = PendingRegistry::new();
        assert_eq!(
            restarted.resolve("i5", HookReply::allowed()).unwrap_err(),
            PendingError::Unknown
        );
        assert_eq!(restarted.get("i5"), None);
    }

    #[test]
    fn duplicate_invoke_id_rejected() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i6", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            registry
                .register("i6", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
                .unwrap_err(),
            PendingError::Duplicate
        );
    }

    #[test]
    fn same_tool_input_twice_is_two_independent_invokes() {
        // 同一命令两次执行:两个 invokeId,互相独立决定。
        let registry = PendingRegistry::new();
        let rx1 = registry
            .register("i7a", InvokeKind::PermissionRequest, None, Some("Bash".into()), None, None, None, Duration::from_secs(10))
            .unwrap();
        let rx2 = registry
            .register("i7b", InvokeKind::PermissionRequest, None, Some("Bash".into()), None, None, None, Duration::from_secs(10))
            .unwrap();
        registry.resolve("i7a", HookReply::allowed()).unwrap();
        registry.resolve("i7b", HookReply::denied("no")).unwrap();
        assert!(matches!(
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                tokio::join!(rx1, rx2)
            })
            .0,
            Ok(HookReply { status, .. }) if status == "allowed"
        ));
    }

    /// Decided → ReturnedToRuntime → RuntimeProcessed 状态链:决定经交付
    /// 确认(complete_delivery(Delivered))返回运行时,PostToolUse 核实后
    /// 进入 RuntimeProcessed;随后不再是 ReturnedToRuntime,不会被二次匹配。
    #[test]
    fn lifecycle_complete_delivery_and_runtime_processed() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i8", InvokeKind::PermissionRequest, None, None, Some("tool-1".into()), None, None, Duration::from_secs(10))
            .unwrap();
        registry.resolve("i8", HookReply::allowed()).unwrap();
        registry
            .complete_delivery("i8", DeliveryOutcome::Delivered)
            .unwrap();
        assert_eq!(
            registry.find_returned_by_tool_use("tool-1").as_deref(),
            Some("i8")
        );
        registry.mark_runtime_processed("i8").unwrap();
        assert_eq!(
            registry.get("i8").unwrap().state,
            PendingState::RuntimeProcessed
        );
        // 不再是 ReturnedToRuntime:不会被二次匹配。
        assert_eq!(registry.find_returned_by_tool_use("tool-1"), None);
    }

    /// R2-ZC01:交付确认只允许从 Decided 出发恰好一次;Delivered →
    /// ReturnedToRuntime,Unconfirmed → HandledLocally;命令面等待者收到
    /// 对应结果;二次确认/重复取通道均不得成功(迟到确认不算送达)。
    #[test]
    fn delivery_confirmation_is_once_and_phase_bound() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("d1", InvokeKind::PermissionRequest, None, Some("Bash".into()), None, None, None, Duration::from_secs(10))
            .unwrap();
        // 决定前交付确认不得生效(合法阶段绑定)。
        assert_eq!(
            registry.complete_delivery("d1", DeliveryOutcome::Delivered).unwrap_err(),
            PendingError::AlreadyDecided
        );
        registry.resolve("d1", HookReply::allowed()).unwrap();
        let wait = registry.take_delivery("d1").expect("接收端恰可取一次");
        assert!(
            registry.take_delivery("d1").is_none(),
            "二次取走按未知处理"
        );
        assert_eq!(
            registry
                .complete_delivery("d1", DeliveryOutcome::Delivered)
                .unwrap(),
            PendingState::ReturnedToRuntime
        );
        assert_eq!(
            tokio::runtime::Runtime::new().unwrap().block_on(wait).unwrap(),
            DeliveryOutcome::Delivered
        );
        // 迟到/重复确认不再改变终态。
        assert_eq!(
            registry.complete_delivery("d1", DeliveryOutcome::Unconfirmed).unwrap_err(),
            PendingError::AlreadyDecided
        );

        // Unconfirmed 路径:决定已锁定但交付不确定 → HandledLocally。
        let registry2 = PendingRegistry::new();
        let _rx2 = registry2
            .register("d2", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        registry2.resolve("d2", HookReply::denied("no")).unwrap();
        let wait2 = registry2.take_delivery("d2").unwrap();
        assert_eq!(
            registry2
                .complete_delivery("d2", DeliveryOutcome::Unconfirmed)
                .unwrap(),
            PendingState::HandledLocally
        );
        assert_eq!(
            tokio::runtime::Runtime::new().unwrap().block_on(wait2).unwrap(),
            DeliveryOutcome::Unconfirmed
        );
        // Unconfirmed 后迟到决定仍被拒(不得二次决定)。
        assert_eq!(
            registry2.resolve("d2", HookReply::allowed()).unwrap_err(),
            PendingError::Cancelled
        );
    }

    #[test]
    fn ask_option_label_maps_option_ids() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i9", InvokeKind::AskUser, None, None, None, Some(&ask()), None, Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            registry.ask_option_label("i9", "option-1").as_deref(),
            Some("是")
        );
        assert_eq!(
            registry.ask_option_label("i9", "option-2").as_deref(),
            Some("否")
        );
        assert_eq!(registry.ask_option_label("i9", "option-9"), None);
        assert_eq!(registry.ask_option_label("i9", "bogus"), None);
    }

    /// M2 回归:决定与超时同时就绪、select 选中 Timeout 分支的竞态 ——
    /// expire 对已 Decided 记录失败(既有语义),竞态兜底终态化后:迟到
    /// 决定与「超时后点击」同路径被拒(Expired),记录可被既有 retain
    /// 清理(修复前:过期 Decided 非 terminal,永久滞留内存)。
    #[tokio::test]
    async fn decided_record_after_timeout_race_is_terminalized_and_cleaned() {
        let registry = PendingRegistry::new();
        let rx = registry
            .register(
                "race-1",
                InvokeKind::PermissionRequest,
                None,
                Some("Bash".into()),
                None,
                None,
                None,
                Duration::from_millis(60),
            )
            .unwrap();
        // 决定先赢得原子锁定(竞态中 resolve 先于 Timeout 分支收尾)。
        registry.resolve("race-1", HookReply::allowed()).unwrap();
        drop(rx); // 应答无人送达(等价 select 落选分支丢弃 rx)。
        assert_eq!(
            registry.expire("race-1").unwrap_err(),
            PendingError::AlreadyDecided,
            "expire 对 Decided 仍必须拒绝(既有语义不放宽)"
        );
        assert!(registry.finalize_decided_after_race("race-1"));
        assert_eq!(registry.get("race-1").unwrap().state, PendingState::Expired);
        assert_eq!(
            registry.resolve("race-1", HookReply::allowed()).unwrap_err(),
            PendingError::Expired,
            "终态化后迟到决定仍被拒"
        );
        // 记录最终被清理:deadline 过后,新 register 的 retain 回收该记录。
        let _rx2 = registry
            .register("race-2", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _rx3 = registry
            .register("race-3", InvokeKind::PermissionRequest, None, None, None, None, None, Duration::from_secs(10))
            .unwrap();
        assert_eq!(registry.get("race-1"), None, "过期终态记录必须被 retain 清理");
    }

    #[test]
    fn record_debug_hides_ask_content() {
        let registry = PendingRegistry::new();
        let _rx = registry
            .register("i10", InvokeKind::AskUser, None, None, None, Some(&ask()), None, Duration::from_secs(10))
            .unwrap();
        let debug = format!("{:?}", registry.get("i10"));
        assert!(!debug.contains("继续"), "ask 正文不得进 Debug: {debug}");
    }
}
