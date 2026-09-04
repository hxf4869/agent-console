//! 待处理关注(§10.5)与问题/审批全字段(§16.3/§16.4)。
//!
//! 生命周期必须可靠恢复,不能按最佳努力处理(§13.1);回答只允许按原生
//! question/approval ID 与原生 option/decision ID,不能只发展示文案。

use super::ids::TurnId;
use super::states::PendingAttentionKind;
use super::text::OutputText;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 问题选项(§16.3):Browser 回答必须发送 option ID。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QuestionOption {
    pub option_id: String,
    pub label: OutputText,
}

/// 原生问题(§16.3 全字段)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PendingQuestion {
    /// 原生 question ID。
    pub question_id: String,
    pub title: OutputText,
    #[serde(default)]
    pub description: OutputText,
    pub options: Vec<QuestionOption>,
    pub allow_multiple: bool,
    pub allow_free_text: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// 失效状态:回答后、turn 结束或超时后为 false,前端不得再提交。
    pub valid: bool,
}

/// 审批可用的原生决定(§16.4):Bridge 只能调用 Desktop 提供的原生决定,
/// 不得创造"永远允许"或扩大范围的选项。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ApprovalDecision {
    pub decision_id: String,
    pub label: OutputText,
}

/// 原生审批(§16.4 全字段)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PendingApproval {
    pub approval_id: String,
    pub risk_description: OutputText,
    pub requested_action: OutputText,
    pub decisions: Vec<ApprovalDecision>,
    /// 作用范围(如本次命令/本会话),由 Desktop 原生提供。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<OutputText>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    pub valid: bool,
}

/// 待处理关注(§10.5 列表类型)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "attention", rename_all = "snake_case")]
pub enum PendingAttention {
    Question(PendingQuestion),
    Approval(PendingApproval),
}

impl PendingAttention {
    pub fn kind(&self) -> PendingAttentionKind {
        match self {
            PendingAttention::Question(_) => PendingAttentionKind::UserQuestion,
            PendingAttention::Approval(_) => PendingAttentionKind::RiskApproval,
        }
    }

    /// 原生 ID(question_id / approval_id)。
    pub fn native_id(&self) -> &str {
        match self {
            PendingAttention::Question(q) => &q.question_id,
            PendingAttention::Approval(a) => &a.approval_id,
        }
    }

    pub fn turn(&self) -> Option<&TurnId> {
        match self {
            PendingAttention::Question(q) => q.turn.as_ref(),
            PendingAttention::Approval(a) => a.turn.as_ref(),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            PendingAttention::Question(q) => q.valid,
            PendingAttention::Approval(a) => a.valid,
        }
    }
}
