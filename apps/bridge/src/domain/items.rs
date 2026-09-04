//! 可见内容条目(§9.3)与历史分页(§11)。
//!
//! Item 的内容枚举与 §9.3 的 11 类可见内容一一对应;未识别的原生类型保留
//! 类型名落入 [`ItemContent::Opaque`],不崩溃、不猜测语义。

use super::ids::{ItemId, TurnId};
use super::states::{ActiveTurnPhase, LastTurnOutcome, PlanStepStatus};
use super::text::OutputText;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Turn(§9.2):稳定 ID + 时间 + 状态推导。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Turn {
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
    pub phase: ActiveTurnPhase,
    /// 终态结果;未结束时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<LastTurnOutcome>,
}

/// 单个可见内容条目(§9.3)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Item {
    pub item_id: ItemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnId>,
    /// 条目修订号;输出类条目随内容追加递增(§13.2)。
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    pub content: ItemContent,
}

/// §9.3 的 11 类可见内容 + 未识别类型的 opaque 保留类。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ItemContent {
    /// 用户消息。
    UserMessage { text: OutputText },
    /// Assistant commentary/final(`final=true` 区分最终回复)。
    AssistantMessage {
        text: OutputText,
        final_message: bool,
    },
    /// Codex 提供给用户的可见 reasoning summary;不含隐藏思维链。
    ReasoningSummary { text: OutputText },
    /// Plan 与步骤状态。
    Plan { plan: Plan },
    /// 工具调用(MCP/web/协作/动态工具)记录。
    ToolCall {
        tool_call_id: String,
        name: String,
        status: PlanStepStatus,
        /// 用户可见参数摘要;不承载原始 arguments 全文。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<OutputText>,
        /// 执行时长(毫秒);未结束时为 0。
        #[serde(default)]
        duration_ms: u64,
    },
    /// 命令状态(§10.8 状态层面;输出游标见 RuntimeSnapshot/输出事件)。
    CommandStatus {
        command_id: String,
        state: CommandStatusState,
        /// 命令显示(§23.2);不含 cwd 等本机绝对路径。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display: Option<OutputText>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i64>,
        #[serde(default)]
        duration_ms: u64,
    },
    /// 文件变化集合(时间线只传 metadata;正文/Diff 走 OnDemandDetail)。
    FileChange { changes: Vec<FileChange> },
    /// 子 Agent 状态(必须带父 turn 关系,§9.2)。
    SubAgentStatus {
        subagent_turn: Option<TurnId>,
        label: String,
        phase: ActiveTurnPhase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<LastTurnOutcome>,
    },
    /// 问题出现(§16.3)。
    Question {
        question_id: String,
        title: OutputText,
    },
    /// 审批出现(§16.4)。
    Approval {
        approval_id: String,
        requested_action: OutputText,
    },
    /// Token/context 用量。
    TokenUsage {
        input_tokens: u64,
        output_tokens: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_used_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_window_tokens: Option<u64>,
    },
    /// 未识别的原生类型:保留类型名,不崩溃、不猜测(§12)。
    Opaque { native_type: String },
}

/// 命令在条目层面的状态(运行时派生自原生 status)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatusState {
    Running,
    Completed,
    Failed,
    Interrupted,
    Unknown,
}

/// Plan(§9.3)。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Plan {
    #[serde(default)]
    pub steps: Vec<PlanStep>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PlanStep {
    pub step_id: String,
    pub title: OutputText,
    pub status: PlanStepStatus,
}

/// 文件变化(§9.3);路径必须是相对授权根的安全显示,禁止绝对路径(§17.2)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileChange {
    pub path: String,
    pub change: FileChangeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Unknown,
}

/// 历史条目:turn 内的可见 item(§11)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HistoryEntry {
    pub turn: TurnId,
    pub item: Item,
}

/// 历史分页(§11):按稳定游标从最新向前读取;禁止页码加可变排序(§27.5)。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HistoryPage {
    #[serde(default)]
    pub entries: Vec<HistoryEntry>,
    /// 继续向前读取的稳定 cursor;None 表示已到最早端。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
