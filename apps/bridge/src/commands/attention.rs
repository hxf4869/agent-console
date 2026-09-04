//! 问题/审批回答(权威规格 §16.3/§16.4)。
//!
//! 只按原生 question/approval ID 与原生 option/decision ID 提交;快照中
//! 不存在或已失效 → `QUESTION_EXPIRED`/`APPROVAL_EXPIRED`;多选约束与
//! 自由文本约束在 Bridge 侧校验;审批决定必须是 Desktop 提供的原生决定,
//! 绝不构造"永远允许"或扩大范围选项(§16.4)。
//!
//! 说明:多选/自由文本/未知选项/未知决定属于调用方契约违规;§27.6 稳定
//! 错误码集合中没有通用"请求无效"码,以 `INTERNAL_ERROR` + 明确 message
//! 表达(登记于实现报告),不静默改写请求。

use super::gateway::{CommandGateway, Submission};
use crate::domain::{
    BridgeError, CommandPayload, CommandRequest, OutputText, SessionKey, StableErrorCode,
};

impl CommandGateway {
    /// 按原生 question ID 回答(§16.3)。发送 option ID;自由文本仅在
    /// `allow_free_text` 时允许;单选问题至多一个 option。
    pub async fn answer_question(
        &self,
        key: &SessionKey,
        question_id: &str,
        option_ids: &[String],
        free_text: Option<OutputText>,
    ) -> Result<Submission, BridgeError> {
        let snapshot = self
            .adapter()
            .runtime_snapshot(key)
            .await
            .map_err(adapter_err)?;
        let Some(question) = snapshot
            .pending_questions
            .iter()
            .find(|q| q.question_id == question_id)
        else {
            return Err(BridgeError::new(
                StableErrorCode::QuestionExpired,
                format!("no pending question with native id {question_id}"),
            ));
        };
        if !question.valid {
            return Err(BridgeError::new(
                StableErrorCode::QuestionExpired,
                "question is no longer valid",
            ));
        }
        if !question.allow_multiple && option_ids.len() > 1 {
            return Err(invalid_request(
                "multiple options submitted for a single-choice question",
            ));
        }
        for option_id in option_ids {
            if !question.options.iter().any(|o| &o.option_id == option_id) {
                return Err(invalid_request(format!(
                    "option {option_id} is not a native option of question {question_id}"
                )));
            }
        }
        if free_text.is_some() && !question.allow_free_text {
            return Err(invalid_request(
                "free text is not allowed for this question",
            ));
        }
        if option_ids.is_empty() && free_text.is_none() {
            return Err(invalid_request("answer requires an option id or free text"));
        }

        let request = CommandRequest {
            request_id: uuid::Uuid::new_v4(),
            operation: crate::domain::Operation::AnswerQuestion,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: CommandPayload::AnswerQuestion {
                question_id: question_id.to_string(),
                option_ids: option_ids.to_vec(),
                free_text,
            },
        };
        self.submit(request).await
    }

    /// 按原生 approval ID + 原生 decision ID 审批(§16.4)。决定必须是
    /// Desktop 原生提供项;绝不构造"永远允许"或扩大范围选项。
    pub async fn answer_approval(
        &self,
        key: &SessionKey,
        approval_id: &str,
        decision_id: &str,
    ) -> Result<Submission, BridgeError> {
        let snapshot = self
            .adapter()
            .runtime_snapshot(key)
            .await
            .map_err(adapter_err)?;
        let Some(approval) = snapshot
            .pending_approvals
            .iter()
            .find(|a| a.approval_id == approval_id)
        else {
            return Err(BridgeError::new(
                StableErrorCode::ApprovalExpired,
                format!("no pending approval with native id {approval_id}"),
            ));
        };
        if !approval.valid {
            return Err(BridgeError::new(
                StableErrorCode::ApprovalExpired,
                "approval is no longer valid",
            ));
        }
        if !approval
            .decisions
            .iter()
            .any(|d| d.decision_id == decision_id)
        {
            // 不转发非原生决定(§16.4)。
            return Err(invalid_request(format!(
                "decision {decision_id} is not a native decision of approval {approval_id}"
            )));
        }

        let request = CommandRequest {
            request_id: uuid::Uuid::new_v4(),
            operation: crate::domain::Operation::SubmitApproval,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: CommandPayload::SubmitApproval {
                approval_id: approval_id.to_string(),
                decision_id: decision_id.to_string(),
            },
        };
        self.submit(request).await
    }
}

fn invalid_request(message: impl Into<String>) -> BridgeError {
    BridgeError::new(StableErrorCode::InternalError, message)
}

fn adapter_err(err: crate::adapter::codex::AdapterError) -> BridgeError {
    BridgeError::new(err.code(), err.to_string())
}
