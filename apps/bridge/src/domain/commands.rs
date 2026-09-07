//! 写命令领域形态(§15):公共字段 + 载荷 + 回执。
//!
//! Bridge 与 runtime/commands 工作流之间的合同;payload 在此层是领域形态,
//! 原生 IPC 参数由 adapter 翻译(§12)。

use super::capability::{Operation, SettingUpdate};
use super::error::StableErrorCode;
use super::ids::{SessionKey, TurnId};
use super::states::QueueState;
use super::text::OutputText;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// serde `skip_serializing_if`:`replace = false` 不出现在规范化序列化中,
/// 既有 QueueSet 回执摘要不因新增默认字段变成 payload mismatch。
fn is_false(value: &bool) -> bool {
    !*value
}

/// 写命令请求(§15.1 公共字段)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CommandRequest {
    /// 调用方生成的 UUID。
    pub request_id: Uuid,
    pub operation: Operation,
    pub session_key: SessionKey,
    /// 目标 turn;不适用时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_turn_id: Option<TurnId>,
    /// 接受命令时的运行时修订号;变化即 STALE_TURN 判定依据。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_runtime_revision: Option<u64>,
    /// payload 的本地规范化摘要(§15.1;仅用于同 ID 不同内容检测)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_digest: Option<String>,
    pub payload: CommandPayload,
}

impl CommandRequest {
    /// Interrupt 已用强目标 `expected_turn_id` 绑定用户意图。同一 turn
    /// 运行期间的输出 patch 会推进 Desktop revision，不应单独使中断
    /// 变成 STALE_TURN；turn 已变化时仍严格拒绝。
    pub fn permits_revision_drift(&self, current_turn: Option<&TurnId>) -> bool {
        matches!(self.payload, CommandPayload::Interrupt)
            && self
                .expected_turn_id
                .as_ref()
                .zip(current_turn)
                .is_some_and(|(expected, current)| expected == current)
    }
}

/// 写命令载荷(领域形态)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "payload", rename_all = "snake_case")]
pub enum CommandPayload {
    /// 开始下一轮(IDLE 默认路径,§15.4)。
    StartTurn { input: OutputText },
    /// steer 当前 turn(RUNNING 且用户显式选择)。
    Steer { input: OutputText },
    /// interrupt 当前 turn;要求 expected_turn_id(§15.4)。
    Interrupt,
    /// 按原生 question ID 回答;发送 option ID 或明确文本(§16.3)。
    AnswerQuestion {
        question_id: String,
        #[serde(default)]
        option_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        free_text: Option<OutputText>,
    },
    /// 按原生 approval ID + 原生 decision ID 审批(§16.4)。
    SubmitApproval {
        approval_id: String,
        decision_id: String,
    },
    /// 更新会话设置(作用于下一次 turn,§16.2)。
    UpdateSettings { values: Vec<SettingUpdate> },
    /// 设置/替换单条下一轮队列正文(正文只落 Bridge SQLite,§15.3)。
    /// `replace = true` 即 QueueReplace 语义:更新同一条队列记录而非拒绝;
    /// `false` 序列化时不出现该字段,与历史 QueueSet 摘要保持兼容,
    /// set/replace 借此在去重摘要(§15.1)中可区分。
    QueueNextTurn {
        input: OutputText,
        #[serde(default, skip_serializing_if = "is_false")]
        replace: bool,
    },
    /// 取消已排队条目。
    CancelQueue,
    /// 暂停队列条目(FAILED/INTERRUPTED 后;只能由用户重新确认,§15.3)。
    PauseQueue,
    /// 停止单个后台命令(仅在能力暴露单项停止时可用)。
    StopBackgroundCommand { command_id: String },
    /// 停止全部受支持的后台命令(仅全局能力时;带 warning 语义,§23.2)。
    StopAllBackgroundCommands,
}

/// 回执状态(§15.2)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReceiptState {
    /// 已交给 Desktop owner。
    DispatchedToCodex,
    /// 操作有明确最终结果。
    Completed,
    /// 拒绝,附稳定错误码。
    Rejected {
        code: StableErrorCode,
        #[serde(default)]
        message: String,
    },
    /// 连接中断且无法证明 Desktop 是否执行。
    OutcomeUnknown,
}

/// 命令回执(流式;一条命令按序产生多个状态)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CommandReceipt {
    pub request_id: Uuid,
    pub state: ReceiptState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
}

/// 队列条目(§15.3;正文只保存在 Bridge SQLite,该结构由 commands 工作流
/// 持久化,adapter 只消费队列头部触发 StartTurn)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QueuedTurn {
    pub request_id: Uuid,
    pub input: OutputText,
    /// 队列绑定的目标 turn 与接受时的 runtime revision。
    pub after_turn_id: TurnId,
    pub accepted_runtime_revision: u64,
    pub state: QueueState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(payload: CommandPayload, expected_turn_id: Option<TurnId>) -> CommandRequest {
        CommandRequest {
            request_id: Uuid::nil(),
            operation: Operation::InterruptTurn,
            session_key: SessionKey::codex("device", "session"),
            expected_turn_id,
            expected_runtime_revision: Some(1),
            payload_digest: None,
            payload,
        }
    }

    #[test]
    fn interrupt_only_tolerates_revision_drift_for_the_same_turn() {
        let current = TurnId::native("turn-current");
        assert!(request(CommandPayload::Interrupt, Some(current.clone()))
            .permits_revision_drift(Some(&current)));
        assert!(!request(
            CommandPayload::Interrupt,
            Some(TurnId::native("turn-stale"))
        )
        .permits_revision_drift(Some(&current)));
        assert!(!request(
            CommandPayload::StartTurn {
                input: OutputText::new("next")
            },
            Some(current.clone())
        )
        .permits_revision_drift(Some(&current)));
    }

    /// §15.1 去重摘要兼容性:`replace = false` 不改变既有 QueueSet 的
    /// 规范化序列化;`replace = true`(QueueReplace)与 set 摘要可区分;
    /// 旧形态 JSON(无 replace 字段)仍可反序列化且摘要一致。
    #[test]
    fn queue_next_turn_replace_flag_keeps_set_digest_compatible() {
        let set = CommandPayload::QueueNextTurn {
            input: OutputText::new("body"),
            replace: false,
        };
        let replace = CommandPayload::QueueNextTurn {
            input: OutputText::new("body"),
            replace: true,
        };

        let set_value = serde_json::to_value(&set).unwrap();
        assert!(
            set_value.get("replace").is_none(),
            "set 的默认 replace 字段不得进入摘要序列化: {set_value}"
        );
        let replace_value = serde_json::to_value(&replace).unwrap();
        assert_eq!(replace_value.get("replace"), Some(&serde_json::json!(true)));

        let digest = |payload: &CommandPayload| crate::commands::canonical_payload_digest(payload);
        assert_ne!(digest(&set), digest(&replace), "set/replace 摘要必须可区分");

        // 历史形态(修复前 QueueSet/QueueReplace 同形)反序列化回 set,
        // 摘要与新增省略默认字段后的序列化完全一致。
        let legacy: CommandPayload = serde_json::from_value(serde_json::json!({
            "payload": "queue_next_turn",
            "input": "body"
        }))
        .unwrap();
        assert_eq!(legacy, set);
        assert_eq!(digest(&legacy), digest(&set));
    }
}
