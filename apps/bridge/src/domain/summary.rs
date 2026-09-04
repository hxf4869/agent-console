//! 会话摘要(§11.1)、运行快照(§11.2)与输出游标(§13.2)。

use super::attention::{PendingApproval, PendingQuestion};
use super::capability::CapabilitySet;
use super::ids::{AgentKind, ItemId, SessionKey, TurnId};
use super::items::Plan;
use super::states::{
    ActiveTurnPhase, BackgroundCommandState, CompatibilityState, ControlMode, DeviceConnection,
    LastTurnOutcome, OutputChannel, PendingAttentionKind, QueueState,
};
use super::text::OutputText;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 当前 turn(§11.2)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CurrentTurn {
    pub turn: TurnId,
    pub phase: ActiveTurnPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// 会话摘要(§11.1 数据接口 1/4):唯一允许 Relay 从 PostgreSQL 直接返回的数据。
/// 多维状态分开携带,禁止合并成单一状态枚举让前端猜。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionSummary {
    pub session_key: SessionKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<OutputText>,
    pub agent_kind: AgentKind,
    /// 项目显示名(目录名尾段或 projects.name);绝不是本机绝对路径(§12)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,

    // ---- 多维状态摘要(§10) ----
    pub device_connection: DeviceConnection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_last_seen_at: Option<DateTime<Utc>>,
    /// DEGRADED 原因;OFFLINE 时为空(不猜测)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<String>,
    pub control_mode: ControlMode,
    pub compatibility_state: CompatibilityState,
    pub active_turn_phase: ActiveTurnPhase,
    /// 待处理关注数量(问题 + 审批),供列表角标显示。
    pub pending_attention_count: u32,
    pub pending_attention_kinds: Vec<PendingAttentionKind>,
    pub queue_state: QueueState,
    pub last_turn_outcome: LastTurnOutcome,

    // ---- 用户标记(§27.2;Agent Console 自身偏好,保存在 Relay) ----
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub muted: bool,
    #[serde(default)]
    pub archived: bool,
}

/// 正在运行的主 turn 命令(状态层面;输出走输出事件/游标)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunningCommand {
    /// 稳定 command ID(§9.2);无法稳定识别时不提供条目,只提供总数。
    pub command_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// 后台命令(§10.8)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BackgroundCommand {
    /// 稳定 command ID;无法稳定识别时为 None(仅计数 + 全局停止 capability)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<ItemId>,
    pub state: BackgroundCommandState,
    /// 命令显示(§23.2)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<OutputText>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

/// 单条下一轮队列状态(§15.3)。正文只保存在 Bridge SQLite,不进摘要/事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QueueStatus {
    pub state: QueueState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_turn_id: Option<TurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_runtime_revision: Option<u64>,
}

impl Default for QueueStatus {
    fn default() -> Self {
        Self {
            state: QueueState::Empty,
            after_turn_id: None,
            accepted_runtime_revision: None,
        }
    }
}

/// 输出游标(§13.2):每个可输出 item 至少维护的字段。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OutputCursor {
    pub item_id: ItemId,
    pub revision: u64,
    pub byte_length: u64,
    pub is_final: bool,
    /// 原生可区分时为 STDOUT/STDERR;不可区分时只能 COMBINED,不得猜测。
    pub channel: OutputChannel,
    /// §13.3:终态后权威最终结果仍无法读取时置 true
    /// (FINAL_OUTPUT_UNAVAILABLE 标记),不得把预览冒充完整结果。
    #[serde(default)]
    pub final_unavailable: bool,
}

/// 运行快照(§11.2 数据接口 2/4):必须由在线 Bridge 提供;
/// 设备离线时上层返回稳定 DEVICE_OFFLINE。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RuntimeSnapshot {
    pub session_key: SessionKey,
    /// 运行时修订号;写命令的 expected_runtime_revision 与之比较。
    pub runtime_revision: u64,
    /// 当前 turn;idle 时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_turn: Option<CurrentTurn>,
    pub plan: Plan,
    /// 待处理关注(§10.5);生命周期必须可靠恢复(§13.1)。
    #[serde(default)]
    pub pending_questions: Vec<PendingQuestion>,
    #[serde(default)]
    pub pending_approvals: Vec<PendingApproval>,
    #[serde(default)]
    pub running_commands: Vec<RunningCommand>,
    /// 后台命令(单项条目仅在 command ID 稳定可识别时提供)。
    #[serde(default)]
    pub background_commands: Vec<BackgroundCommand>,
    /// 后台命令总数(§9.2:无法稳定识别单项时只提供总数)。
    pub background_command_count: u32,
    pub queue: QueueStatus,
    /// 能力摘要。
    pub capabilities: CapabilitySet,
    /// 最近输出 item 的游标列表,供前端对齐本地状态(§13.2)。
    #[serde(default)]
    pub recent_output_cursors: Vec<OutputCursor>,
}
