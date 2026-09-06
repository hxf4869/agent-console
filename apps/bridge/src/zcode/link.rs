//! ZCode Hooks 与 BridgeRuntime 的接线(04 §8.3 数据通路;ZC-02 正式路由)。
//!
//! - 远程卡片经现有事件通道表达:`PendingAttentionAdded` /
//!   `PendingAttentionRemoved`(不改 proto;浏览器卡片渲染归 web 任务)。
//! - 会话键:正式 `SessionKey { agent_kind: ZcodeDesktop, native_session_id }`,
//!   与同机 Codex 会话按 agent_kind 维度隔离(不再用 `zcode:` 前缀)。
//! - 会话可见:决定/状态变化时发布最小 `SessionSummaryChanged`(能力来源
//!   官方 Hook),使 ZCode 会话进入列表流;`runtime_snapshot()` 提供最小
//!   快照(仅 Hook 决定能力),供详情订阅/查询恢复卡片。
//! - 浏览器命令 `AnswerApproval` / `AnswerQuestion` 命中本注册表时由
//!   Bridge 原子决定并回 `COMPLETED`;不命中则保持原 Codex 链路不变。
//! - MCP 问答走同一注册表(独立 `plugin-ask` 会话键,不冒充原生会话的
//!   native question)。

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock, Weak,
};

use agent_console_protocol::agent_console::v1 as pb;

use crate::domain as dm;
use crate::domain::{
    ApprovalDecision, OutputText, PendingApproval, PendingAttention, PendingQuestion,
    QuestionOption,
};

use super::contract::{
    HookInvoke, HookReply,
};
use super::observe::ObservationStore;
use super::pending::{InvokeKind, PendingError, PendingRegistry};

/// 审批决定 ID(Bridge 侧固定二元决定;不创造"永远允许",§16.4)。
pub const DECISION_ALLOW: &str = "allow";
pub const DECISION_DENY: &str = "deny";

/// 单条授权摘要上限(展示用;不进日志)。
const SUMMARY_MAX_CHARS: usize = 400;

/// MCP 插件问答的独立会话键(不冒充原生会话的 native question,04 §8.8)。
pub const ASK_SESSION_NATIVE: &str = "plugin-ask";

/// ZCode Hooks 侧持有物。克隆廉价。
pub struct ZcodeHooks {
    device_id: String,
    socket_path: std::path::PathBuf,
    registry: Arc<PendingRegistry>,
    observations: ObservationStore,
    /// 摘要/快照 revision(Hook 观察或决定面变化时递增)。
    revision: AtomicU64,
    runtime: OnceLock<Weak<crate::runtime::BridgeRuntime>>,
}

impl std::fmt::Debug for ZcodeHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZcodeHooks")
            .field("device_id", &self.device_id)
            .field("socket_path", &self.socket_path)
            .field("runtime_attached", &self.runtime.get().is_some())
            .finish_non_exhaustive()
    }
}

impl ZcodeHooks {
    pub fn new(device_id: impl Into<String>, socket_path: std::path::PathBuf) -> Self {
        Self {
            device_id: device_id.into(),
            socket_path,
            registry: Arc::new(PendingRegistry::new()),
            observations: ObservationStore::new(),
            revision: AtomicU64::new(1),
            runtime: OnceLock::new(),
        }
    }

    fn bump_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn current_revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    pub fn registry(&self) -> &Arc<PendingRegistry> {
        &self.registry
    }

    /// 关联运行时(Weak:不延长 runtime 生命周期)。
    pub fn set_runtime(&self, runtime: &Arc<crate::runtime::BridgeRuntime>) {
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }

    // -----------------------------------------------------------------
    // 会话键(正式 agent_kind 隔离,ZC-02)
    // -----------------------------------------------------------------

    fn session_key_for(&self, native_session_id: Option<&str>) -> dm::SessionKey {
        dm::SessionKey::zcode(
            self.device_id.clone(),
            native_session_id.unwrap_or("unknown").to_string(),
        )
    }

    fn ask_session_key(&self) -> dm::SessionKey {
        dm::SessionKey::zcode(self.device_id.clone(), ASK_SESSION_NATIVE.to_string())
    }

    // -----------------------------------------------------------------
    // 状态观察(最小元数据;web 展示归后续)
    // -----------------------------------------------------------------

    pub fn note_status(&self, invoke: &HookInvoke) {
        self.observations.apply(invoke);
        self.bump_revision();
    }

    pub fn observed_session(&self, native_session_id: &str) -> Option<crate::zcode::observe::ZcodeSessionMeta> {
        self.observations.get(native_session_id)
    }

    /// 审批等待开始/结束(仅已发现会话;未发现则忽略)。
    pub fn mark_awaiting_approval(&self, native_session_id: Option<&str>, awaiting: bool) {
        if let Some(session) = native_session_id {
            self.observations.set_awaiting_approval(session, awaiting);
            self.bump_revision();
        }
    }

    /// 插件问答会话已实际使用(L1:首次 AskUser 登记时调用)。观察镜像
    /// 据此把该虚拟会话列入列表;无实际问答时不列出(空会话不再常驻)。
    pub fn note_ask_session_used(&self) {
        self.observations.ensure_session(ASK_SESSION_NATIVE);
        self.bump_revision();
    }

    // -----------------------------------------------------------------
    // 远程卡片事件(现有事件通道;无订阅者时事件被 runtime 丢弃)
    // -----------------------------------------------------------------

    async fn publish(&self, key: &dm::SessionKey, event: dm::DomainEvent) {
        let Some(runtime) = self.runtime.get().and_then(Weak::upgrade) else {
            return;
        };
        runtime.publish_external_event(key, event).await;
    }

    /// 发布会话摘要变化(列表可见性 + attentionCount;ZC-02 会话可见)。
    async fn publish_summary(&self, key: &dm::SessionKey) {
        self.publish(
            key,
            dm::DomainEvent::SessionSummaryChanged {
                summary: self.session_summary(key),
            },
        )
        .await;
    }

    /// 远程卡片:审批 / 问答。
    pub async fn publish_attention_added(&self, invoke: &HookInvoke, kind: InvokeKind) {
        if !publishes_remote_attention(kind, invoke.native_session_id.as_deref()) {
            return;
        }
        let attention = match kind {
            InvokeKind::PermissionRequest => approval_card(invoke),
            InvokeKind::AskUser => ask_card(invoke),
        };
        let key = if kind == InvokeKind::AskUser {
            self.ask_session_key()
        } else {
            self.session_key_for(invoke.native_session_id.as_deref())
        };
        self.publish(
            &key,
            dm::DomainEvent::PendingAttentionAdded { attention },
        )
        .await;
        self.publish_summary(&key).await;
    }

    /// 决定已返回运行时 / 过期 / 撤销:卡片移除(浏览器侧不再可点)。
    pub async fn publish_attention_removed(
        &self,
        invoke: &HookInvoke,
        kind: InvokeKind,
        _reason: &str,
    ) {
        if !publishes_remote_attention(kind, invoke.native_session_id.as_deref()) {
            return;
        }
        let key = if kind == InvokeKind::AskUser {
            self.ask_session_key()
        } else {
            self.session_key_for(invoke.native_session_id.as_deref())
        };
        self.publish_removed(&key, kind, &invoke.invoke_id).await;
        self.publish_summary(&key).await;
    }

    pub async fn publish_attention_returned(&self, invoke: &HookInvoke, kind: InvokeKind) {
        self.publish_attention_removed(invoke, kind, "returned").await;
    }

    /// cancel 事件只有 invoke_id:从注册表快照还原会话/种类。
    pub async fn publish_removal_for_cancelled(&self, invoke_id: &str) {
        let Some(snapshot) = self.registry.get(invoke_id) else {
            return;
        };
        let key = if snapshot.kind == InvokeKind::AskUser {
            self.ask_session_key()
        } else {
            self.session_key_for(snapshot.native_session_id.as_deref())
        };
        self.publish_removed(&key, snapshot.kind, invoke_id).await;
        self.publish_summary(&key).await;
    }

    /// 状态事件(会话发现/轮次开始/结束)后的摘要刷新(轮次 phase 变化)。
    pub async fn publish_summary_after_status(&self, native_session_id: Option<&str>) {
        if let Some(session) = native_session_id {
            self.publish_summary(&self.session_key_for(Some(session)))
                .await;
        }
    }

    async fn publish_removed(&self, key: &dm::SessionKey, kind: InvokeKind, invoke_id: &str) {
        let attention_kind = match kind {
            InvokeKind::PermissionRequest => dm::PendingAttentionKind::RiskApproval,
            InvokeKind::AskUser => dm::PendingAttentionKind::UserQuestion,
        };
        self.publish(
            key,
            dm::DomainEvent::PendingAttentionRemoved {
                kind: attention_kind,
                native_id: invoke_id.to_string(),
                turn: None,
            },
        )
        .await;
    }

    // -----------------------------------------------------------------
    // 最小摘要 / RuntimeSnapshot(能力来源:官方 Hook;ZC-02)
    // -----------------------------------------------------------------

    /// 该会话键当前等待决定中的卡片。
    fn waiting_for(&self, key: &dm::SessionKey) -> Vec<super::pending::WaitingCard> {
        self.registry
            .waiting_cards()
            .into_iter()
            .filter(|card| match card.kind {
                InvokeKind::AskUser => {
                    key.native_session_id == ASK_SESSION_NATIVE
                }
                InvokeKind::PermissionRequest => {
                    card.native_session_id.as_deref() == Some(key.native_session_id.as_str())
                }
            })
            .collect()
    }

    fn capability_set() -> dm::CapabilitySet {
        dm::CapabilitySet {
            control_mode: dm::ControlMode::LimitedControl,
            compatibility_state: dm::CompatibilityState::Degraded,
            codex_version: None,
            // 官方 Hook 通路只验证过审批/问答决定;其余写能力如实关闭。
            supported_operations: vec![
                dm::Operation::AnswerQuestion,
                dm::Operation::SubmitApproval,
            ],
            settings: Vec::new(),
            transfer_limits: dm::TransferLimits::default(),
        }
    }

    fn phase_of(meta: &crate::zcode::observe::ZcodeSessionMeta) -> dm::ActiveTurnPhase {
        if meta.turn_running {
            dm::ActiveTurnPhase::Running
        } else {
            dm::ActiveTurnPhase::Idle
        }
    }

    /// 最小会话摘要(列表流可见;title 只含会话标识,不含路径)。
    pub fn session_summary(&self, key: &dm::SessionKey) -> dm::SessionSummary {
        let meta = self
            .observations
            .get(&key.native_session_id)
            .unwrap_or_default();
        let waiting = self.waiting_for(key);
        let mut kinds: Vec<dm::PendingAttentionKind> = waiting
            .iter()
            .map(|card| match card.kind {
                InvokeKind::PermissionRequest => dm::PendingAttentionKind::RiskApproval,
                InvokeKind::AskUser => dm::PendingAttentionKind::UserQuestion,
            })
            .collect();
        kinds.dedup();
        let title = if key.native_session_id == ASK_SESSION_NATIVE {
            "ZCode 插件问答"
        } else {
            "ZCode 会话"
        };
        dm::SessionSummary {
            session_key: key.clone(),
            title: Some(OutputText::new(title.to_string())),
            agent_kind: dm::AgentKind::ZcodeDesktop,
            project_display_name: None,
            current_branch: None,
            updated_at: Some(chrono::Utc::now()),
            device_connection: dm::DeviceConnection::Online,
            device_last_seen_at: None,
            degraded_reason: None,
            control_mode: dm::ControlMode::LimitedControl,
            compatibility_state: dm::CompatibilityState::Degraded,
            active_turn_phase: Self::phase_of(&meta),
            pending_attention_count: waiting.len() as u32,
            pending_attention_kinds: kinds,
            queue_state: dm::QueueState::Empty,
            last_turn_outcome: dm::LastTurnOutcome::Unknown,
            pinned: false,
            muted: false,
            archived: false,
        }
    }

    /// 最小 RuntimeSnapshot(详情订阅/查询恢复卡片;无输出/队列/后台命令)。
    pub fn runtime_snapshot(&self, key: &dm::SessionKey) -> dm::RuntimeSnapshot {
        let meta = self
            .observations
            .get(&key.native_session_id)
            .unwrap_or_default();
        let waiting = self.waiting_for(key);
        let mut pending_questions = Vec::new();
        let mut pending_approvals = Vec::new();
        for card in waiting {
            match card.kind {
                InvokeKind::AskUser => pending_questions.push(PendingQuestion {
                    question_id: card.invoke_id.clone(),
                    title: OutputText::new(card.question.unwrap_or_default()),
                    description: OutputText::new(String::new()),
                    options: card
                        .options
                        .iter()
                        .enumerate()
                        .map(|(index, label)| QuestionOption {
                            option_id: format!("option-{}", index + 1),
                            label: OutputText::new(label.clone()),
                        })
                        .collect(),
                    allow_multiple: false,
                    allow_free_text: card.allow_free_text,
                    turn: None,
                    created_at: None,
                    valid: true,
                }),
                InvokeKind::PermissionRequest => {
                    let tool = card.tool_name.unwrap_or_else(|| "unknown".to_string());
                    pending_approvals.push(PendingApproval {
                        approval_id: card.invoke_id,
                        risk_description: OutputText::new(format!(
                            "ZCode 请求执行工具 {tool}"
                        )),
                        // 登记时保留的操作摘要(P1-5):恢复快照与实时卡片一致。
                        requested_action: OutputText::new(
                            card.action_summary.unwrap_or_default(),
                        ),
                        decisions: vec![
                            ApprovalDecision {
                                decision_id: DECISION_ALLOW.to_string(),
                                label: OutputText::new("允许"),
                            },
                            ApprovalDecision {
                                decision_id: DECISION_DENY.to_string(),
                                label: OutputText::new("拒绝"),
                            },
                        ],
                        scope: None,
                        turn: None,
                        created_at: None,
                        valid: true,
                    });
                }
            }
        }
        dm::RuntimeSnapshot {
            session_key: key.clone(),
            runtime_revision: self.current_revision(),
            current_turn: meta.turn_running.then(|| dm::CurrentTurn {
                turn: dm::TurnId::synthetic("zcode-hook-turn"),
                phase: dm::ActiveTurnPhase::Running,
                started_at: None,
            }),
            plan: dm::Plan::default(),
            pending_questions,
            pending_approvals,
            running_commands: Vec::new(),
            background_commands: Vec::new(),
            background_command_count: 0,
            queue: dm::QueueStatus::default(),
            capabilities: Self::capability_set(),
            recent_output_cursors: Vec::new(),
        }
    }

    /// 已观察到的 ZCode 会话列表摘要(列表流快照重建用)。来源包括:
    /// 状态事件观察到的会话 + 注册表中仍在等待决定的会话(审批请求可能
    /// 先于 SessionStart 状态事件到达,04 §8.5)+ 实际发生过问答的插件
    /// 问答会话(L1:无任何问答时该虚拟会话不常驻列表;首次 AskUser 登记
    /// 经 `note_ask_session_used` 登记进观察镜像,此后一直保留)。
    pub fn list_summaries(&self) -> Vec<dm::SessionSummary> {
        let mut natives: Vec<String> = self.observations.known_sessions();
        for card in self.registry.waiting_cards() {
            if let Some(native) = card.native_session_id {
                natives.push(native);
            }
        }
        natives.sort();
        natives.dedup();
        natives
            .iter()
            .map(|native| self.session_summary(&self.session_key_for(Some(native))))
            .collect()
    }

    // -----------------------------------------------------------------
    // 浏览器命令 → 原子决定
    // -----------------------------------------------------------------

    /// 命令是否命中 ZCode pending(命中才走本模块;否则保持 Codex 链路)。
    /// 决定前必须核对目标绑定完全一致(P1-6):设备、agentKind 与
    /// nativeSessionId 都要和登记请求一致 —— 任何会话/任何 agentKind 的
    /// AnswerApproval/AnswerQuestion 不得仅凭 invokeId 处理 ZCode pending。
    /// 审批绑定登记的 nativeSessionId;MCP 问答绑定 `plugin-ask` 会话键。
    pub fn matches_command(
        &self,
        session_key: Option<&pb::SessionKey>,
        payload: Option<&pb::command_request::Payload>,
    ) -> bool {
        let Some(key) = session_key else {
            return false;
        };
        if key.device_id != self.device_id {
            return false;
        }
        if key.agent_kind != pb::AgentKind::ZcodeDesktop as i32 {
            return false;
        }
        use pb::command_request::Payload;
        let Some(payload) = payload else {
            return false;
        };
        match payload {
            Payload::AnswerApproval(a) => {
                matches!(
                    self.registry
                        .get(&a.approval_id)
                        .map(|s| (s.kind, s.native_session_id)),
                    Some((InvokeKind::PermissionRequest, Some(native)))
                        if native == key.native_session_id
                )
            }
            Payload::AnswerQuestion(q) => {
                key.native_session_id == ASK_SESSION_NATIVE
                    && matches!(
                        self.registry.get(&q.question_id).map(|s| s.kind),
                        Some(InvokeKind::AskUser)
                    )
            }
            _ => false,
        }
    }

    /// 原子决定。Ok((invoke_id, 回复)) 时回复会经 socket 送达等待中的
    /// helper;迟到/过期/已决定返回 `APPROVAL_EXPIRED`(或契约违规
    /// `INTERNAL_ERROR`)。
    pub async fn resolve_command(
        &self,
        payload: Option<&pb::command_request::Payload>,
    ) -> Option<Result<(String, HookReply), dm::BridgeError>> {
        use pb::command_request::Payload;
        let Some(payload) = payload else {
            return None;
        };
        let (invoke_id, reply) = match payload {
            Payload::AnswerApproval(a) => {
                let decision = a.decision_id.as_str();
                if decision != DECISION_ALLOW && decision != DECISION_DENY {
                    return Some(Err(invalid_request(format!(
                        "decision {decision} is not a native decision of approval {}",
                        a.approval_id
                    ))));
                }
                (
                    a.approval_id.clone(),
                    if decision == DECISION_ALLOW {
                        HookReply::allowed()
                    } else {
                        HookReply::denied("User declined this action in Agent Console")
                    },
                )
            }
            Payload::AnswerQuestion(q) => {
                let label = match q.option_ids.first() {
                    Some(option_id) => match self.registry.ask_option_label(&q.question_id, option_id) {
                        Some(label) => Some(label),
                        None => {
                            return Some(Err(invalid_request(format!(
                                "option {option_id} is not a native option of question {}",
                                q.question_id
                            ))))
                        }
                    },
                    None => None,
                };
                let text = if q.free_text.is_empty() {
                    None
                } else {
                    Some(q.free_text.clone())
                };
                if text.is_some()
                    && !self.registry.ask_allows_free_text(&q.question_id).unwrap_or(false)
                {
                    return Some(Err(invalid_request(
                        "free text is not allowed for this question",
                    )));
                }
                if label.is_none() && text.is_none() {
                    return Some(Err(invalid_request(
                        "answer requires an option id or free text",
                    )));
                }
                (q.question_id.clone(), HookReply::answered(label, text))
            }
            _ => return None,
        };
        let result = self.registry.resolve(&invoke_id, reply.clone());
        Some(
            result
                .map(|_| (invoke_id, reply))
                .map_err(pending_error),
        )
    }
}

/// PendingError → 稳定错误(过期/迟到/重启失效统一 `APPROVAL_EXPIRED`)。
fn pending_error(err: PendingError) -> dm::BridgeError {
    dm::BridgeError::new(dm::StableErrorCode::ApprovalExpired, err.to_string())
}

fn invalid_request(message: impl Into<String>) -> dm::BridgeError {
    dm::BridgeError::new(dm::StableErrorCode::InternalError, message)
}

/// 该 invoke 是否发布远程卡片/摘要事件(M3):缺 native_session_id 的
/// 审批不发布 —— matches_command 的绑定校验要求 `Some(native_session_id)`,
/// 这类审批永远无法被远程决定,发布可见卡片只会造成"可见但永远点不动"
/// 的死路(点击必然 APPROVAL_EXPIRED)。其决定的完成路径是 helper 等待
/// 预算超时后回退本机原生确认;期间不发布通知性事件 —— 以 "unknown"
/// 幽灵键进入列表/详情同样无法操作,只会制造新的死路。MCP 问答绑定
/// `plugin-ask` 固定会话键,不受影响。
fn publishes_remote_attention(kind: InvokeKind, native_session_id: Option<&str>) -> bool {
    !(kind == InvokeKind::PermissionRequest && native_session_id.is_none())
}

/// 审批卡片(toolInput 只做有界摘要;原始输入不上行)。
fn approval_card(invoke: &HookInvoke) -> PendingAttention {    let tool = invoke.tool_name.clone().unwrap_or_else(|| "unknown".to_string());
    let summary = summarize_tool_input(invoke.tool_input.as_ref());
    PendingAttention::Approval(PendingApproval {
        approval_id: invoke.invoke_id.clone(),
        risk_description: OutputText::new(format!("ZCode 请求执行工具 {tool}")),
        requested_action: OutputText::new(summary),
        decisions: vec![
            ApprovalDecision {
                decision_id: DECISION_ALLOW.to_string(),
                label: OutputText::new("允许"),
            },
            ApprovalDecision {
                decision_id: DECISION_DENY.to_string(),
                label: OutputText::new("拒绝"),
            },
        ],
        scope: None,
        turn: None,
        created_at: Some(chrono::Utc::now()),
        valid: true,
    })
}

/// 测试入口:实时事件卡片构造的只读暴露(快照一致性断言用)。
#[cfg(test)]
pub(crate) fn approval_card_for_test(invoke: &HookInvoke) -> PendingAttention {
    approval_card(invoke)
}

/// 问答卡片(选项 id = "option-{n}",原文经注册表映射回传)。
fn ask_card(invoke: &HookInvoke) -> PendingAttention {
    let Some(ask) = &invoke.ask else {
        // 校验已保证存在;防御式回退为空问题(不 panic)。
        return PendingAttention::Question(PendingQuestion {
            question_id: invoke.invoke_id.clone(),
            title: OutputText::new(String::new()),
            description: OutputText::new(String::new()),
            options: Vec::new(),
            allow_multiple: false,
            allow_free_text: true,
            turn: None,
            created_at: Some(chrono::Utc::now()),
            valid: true,
        });
    };
    PendingAttention::Question(PendingQuestion {
        question_id: invoke.invoke_id.clone(),
        title: OutputText::new(ask.question.clone()),
        description: OutputText::new(String::new()),
        options: ask
            .options
            .iter()
            .enumerate()
            .map(|(index, label)| QuestionOption {
                option_id: format!("option-{}", index + 1),
                label: OutputText::new(label.clone()),
            })
            .collect(),
        allow_multiple: false,
        allow_free_text: ask.allow_free_text,
        turn: None,
        created_at: Some(chrono::Utc::now()),
        valid: true,
    })
}

/// toolInput 有界摘要:键名 + 截断值,绝不拼接执行(04 §8.5)。
pub(crate) fn summarize_tool_input(input: Option<&serde_json::Value>) -> String {
    let Some(input) = input else {
        return "(无工具输入)".to_string();
    };
    let raw = match input {
        serde_json::Value::String(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "?".to_string()),
    };
    let truncated: String = raw.chars().take(SUMMARY_MAX_CHARS).collect();
    if raw.chars().count() > SUMMARY_MAX_CHARS {
        format!("{truncated}…(已截断展示,完整输入仅本机可见)")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zcode::contract::{self, AskRequest, EVENT_ASK_USER};
    use crate::zcode::server::status_invoke_from_line;

    fn hooks() -> ZcodeHooks {
        ZcodeHooks::new("device-test", std::env::temp_dir().join("unused.sock"))
    }

    fn ask_invoke(id: &str) -> HookInvoke {
        HookInvoke {
            version: contract::CONTRACT_VERSION,
            agent_kind: "zcode".to_string(),
            invoke_id: id.to_string(),
            event: EVENT_ASK_USER.to_string(),
            native_session_id: None,
            tool_name: None,
            tool_use_id: None,
            requested_wait_ms: 10_000,
            tool_input: None,
            ask: Some(AskRequest {
                question: "继续吗?".to_string(),
                options: vec!["继续".to_string(), "取消".to_string()],
                allow_free_text: true,
                call_id: Some("1".to_string()),
            }),
            status_event: None,
            status_input: None,
        }
    }

    /// 会话键隔离(ZC-02):正式 agent_kind = ZcodeDesktop,与同机同 native id
    /// 的 Codex 会话天然隔离;插件问答为独立会话键。
    #[test]
    fn session_keys_namespaced() {
        let hooks = hooks();
        let key = hooks.session_key_for(Some("sess-1"));
        assert_eq!(key.agent_kind, dm::AgentKind::ZcodeDesktop);
        assert_eq!(key.native_session_id, "sess-1");
        assert_eq!(
            hooks.ask_session_key().native_session_id,
            super::ASK_SESSION_NATIVE
        );
        // 同 native id 的双 Agent 键不相等(隔离语义)。
        assert_ne!(key, dm::SessionKey::codex("device-test", "sess-1"));
    }

    /// 摘要/快照镜像 waiting 注册表:计数与卡片如实;能力只含 Hook 决定。
    #[test]
    fn summary_and_snapshot_mirror_waiting_cards() {
        let hooks = hooks();
        let key = hooks.session_key_for(Some("sess-9"));
        let summary = hooks.session_summary(&key);
        assert_eq!(summary.agent_kind, dm::AgentKind::ZcodeDesktop);
        assert_eq!(summary.pending_attention_count, 0);
        assert_eq!(summary.control_mode, dm::ControlMode::LimitedControl);
        assert_eq!(summary.compatibility_state, dm::CompatibilityState::Degraded);
        let snapshot = hooks.runtime_snapshot(&key);
        assert!(snapshot.pending_approvals.is_empty());
        assert_eq!(snapshot.capabilities.supported_operations.len(), 2);

        let _rx = hooks
            .registry
            .register(
                "a-9",
                InvokeKind::PermissionRequest,
                Some("sess-9".into()),
                Some("Bash".into()),
                None,
                None,
                None,
                std::time::Duration::from_secs(10),
            )
            .unwrap();
        let summary = hooks.session_summary(&key);
        assert_eq!(summary.pending_attention_count, 1);
        assert_eq!(
            summary.pending_attention_kinds,
            vec![dm::PendingAttentionKind::RiskApproval]
        );
        let snapshot = hooks.runtime_snapshot(&key);
        assert_eq!(snapshot.pending_approvals.len(), 1);
        assert_eq!(snapshot.pending_approvals[0].decisions.len(), 2);
        assert!(snapshot.pending_questions.is_empty());
    }

    /// 卡片构造:审批二元决定 + 问答选项映射;摘要有界。
    #[tokio::test]
    async fn cards_and_command_resolution_flow() {
        let hooks = hooks();
        let invoke = ask_invoke("q-1");
        let card = ask_card(&invoke);
        let PendingAttention::Question(question) = &card else {
            panic!("ask 卡片类型错误");
        };
        assert_eq!(question.options.len(), 2);
        assert_eq!(question.options[0].option_id, "option-1");
        assert!(question.allow_free_text);

        // 命令命中与原子决定。
        let _rx = hooks.registry.register(
            "q-1",
            InvokeKind::AskUser,
            None,
            None,
            None,
            invoke.ask.as_ref(),
            None,
            std::time::Duration::from_secs(10),
        ).unwrap();
        let payload = pb::command_request::Payload::AnswerQuestion(pb::AnswerQuestionPayload {
            question_id: "q-1".into(),
            option_ids: vec!["option-2".into()],
            free_text: String::new(),
        });
        let key = pb::SessionKey { device_id: "device-test".into(), agent_kind: pb::AgentKind::ZcodeDesktop as i32, native_session_id: super::ASK_SESSION_NATIVE.into(), relay_session_uuid: String::new() };
        assert!(hooks.matches_command(Some(&key), Some(&payload)));
        let (id, reply) = hooks.resolve_command(Some(&payload)).await.unwrap().unwrap();
        assert_eq!(id, "q-1");
        assert_eq!(reply.status, contract::STATUS_ANSWERED);
        assert_eq!(reply.option.as_deref(), Some("取消"));
        // 未命中:不拦截。
        let other = pb::command_request::Payload::AnswerQuestion(pb::AnswerQuestionPayload {
            question_id: "q-other".into(),
            option_ids: vec![],
            free_text: "x".into(),
        });
        assert!(!hooks.matches_command(Some(&key), Some(&other)));
        // 反例(P1-6):同 payload 但会话键不一致不得命中。
        let other_key = pb::SessionKey { native_session_id: "sess-x".into(), ..key.clone() };
        assert!(!hooks.matches_command(Some(&other_key), Some(&payload)));
        let codex_key = pb::SessionKey { agent_kind: pb::AgentKind::CodexDesktop as i32, ..key.clone() };
        assert!(!hooks.matches_command(Some(&codex_key), Some(&payload)));
    }

    /// 非原生决定与迟到决定都拒绝。
    #[tokio::test]
    async fn invalid_and_late_decisions_rejected() {
        let hooks = hooks();
        let _rx = hooks.registry.register(
            "a-1",
            InvokeKind::PermissionRequest,
            None,
            Some("Bash".into()),
            None,
            None,
            None,
            std::time::Duration::from_millis(30),
        ).unwrap();
        // 非原生决定。
        let payload = pb::command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
            approval_id: "a-1".into(),
            decision_id: "always-allow".into(),
        });
        let err = hooks.resolve_command(Some(&payload)).await.unwrap().unwrap_err();
        assert_eq!(err.code, dm::StableErrorCode::InternalError);
        // 到期后决定。
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let payload = pb::command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
            approval_id: "a-1".into(),
            decision_id: DECISION_ALLOW.into(),
        });
        let err = hooks.resolve_command(Some(&payload)).await.unwrap().unwrap_err();
        assert_eq!(err.code, dm::StableErrorCode::ApprovalExpired);
    }

    /// M3 回归:无 session_id 的审批不发布远程卡片(无 runtime 时事件本就
    /// 丢弃,这里锁定判定本身);有 session_id 的审批与 MCP 问答不受影响;
    /// 决定命令对该类审批不命中(绑定语义保持)。
    #[tokio::test]
    async fn approval_without_session_not_published_and_not_matchable() {
        let hooks = hooks();
        assert!(!publishes_remote_attention(InvokeKind::PermissionRequest, None));
        assert!(publishes_remote_attention(InvokeKind::PermissionRequest, Some("sess-1")));
        // MCP 问答绑定 plugin-ask 固定会话键,不受此判定影响。
        assert!(publishes_remote_attention(InvokeKind::AskUser, None));

        let _rx = hooks.registry.register(
            "a-nosess",
            InvokeKind::PermissionRequest,
            None,
            Some("Bash".into()),
            None,
            None,
            None,
            std::time::Duration::from_secs(10),
        ).unwrap();
        let ghost_key = pb::SessionKey {
            device_id: "device-test".into(),
            agent_kind: pb::AgentKind::ZcodeDesktop as i32,
            native_session_id: "unknown".into(),
            relay_session_uuid: String::new(),
        };
        let payload = pb::command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
            approval_id: "a-nosess".into(),
            decision_id: DECISION_ALLOW.into(),
        });
        assert!(!hooks.matches_command(Some(&ghost_key), Some(&payload)));
    }

    /// L1 回归:冷启动列表不含空"ZCode 插件问答"会话;实际发生过一次
    /// 问答(note_ask_session_used 登记镜像)后列出并保留。
    #[test]
    fn ask_session_listed_only_when_used() {
        let hooks = hooks();
        assert!(
            !hooks
                .list_summaries()
                .iter()
                .any(|s| s.session_key.native_session_id == ASK_SESSION_NATIVE),
            "无任何问答时不得常驻空问答会话"
        );
        hooks.note_ask_session_used();
        assert!(
            hooks
                .list_summaries()
                .iter()
                .any(|s| s.session_key.native_session_id == ASK_SESSION_NATIVE),
            "有过问答后会话必须列出"
        );
    }

    /// 状态观察经 hooks 登记。
    #[test]
    fn status_observation_via_hooks() {
        let hooks = hooks();
        let invoke = status_invoke_from_line(
            r#"{"hook_event_name":"SessionStart","session_id":"s1","source":"startup"}"#,
        )
        .unwrap();
        hooks.note_status(&invoke);
        assert!(hooks.observed_session("s1").unwrap().discovered);
    }

    /// toolInput 摘要有界且不执行。
    #[test]
    fn tool_input_summary_bounded() {
        let long = "x".repeat(1_000);
        let summary = summarize_tool_input(Some(&serde_json::json!({"command": long})));
        assert!(summary.chars().count() <= SUMMARY_MAX_CHARS + 40);
        assert_eq!(summarize_tool_input(None), "(无工具输入)");
    }
}
