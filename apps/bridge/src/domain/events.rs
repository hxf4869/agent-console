//! 领域事件(§13.2/§14)。命名与 proto/agent_console/v1/events.proto 对齐,
//! 但这是 Bridge 内部类型,不依赖 proto。

use super::attention::PendingAttention;
use super::capability::CapabilitySet;
use super::ids::{ItemId, TurnId};
use super::items::Item;
use super::states::{ActiveTurnPhase, LastTurnOutcome, OutputChannel, PendingAttentionKind};
use super::summary::{BackgroundCommand, QueueStatus, SessionSummary};
use super::text::OutputBytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 单个领域事件。顺序即同一会话流内的应用顺序;输出正文字段一律使用
/// [`OutputBytes`],Debug 输出只含 ID/状态/长度。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum DomainEvent {
    /// Turn 生命周期(§10.4);终态时携带上一轮结果。
    TurnLifecycle {
        turn: TurnId,
        phase: ActiveTurnPhase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<LastTurnOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finished_at: Option<DateTime<Utc>>,
    },
    /// Item 新建或内容更新(§9.3)。
    ItemUpsert { item: Item },
    /// 输出增量追加(§13.2):前端本地 offset 与 expected_offset 不一致时
    /// 不得拼接,应请求 item snapshot 或 RESYNC_REQUIRED。
    OutputAppend {
        item_id: ItemId,
        expected_offset: u64,
        bytes: OutputBytes,
        channel: OutputChannel,
    },
    /// 修正/替换(§13.3 结束校正或快照补偿):内容为完整替换。
    OutputReplace {
        item_id: ItemId,
        revision: u64,
        bytes: OutputBytes,
        channel: OutputChannel,
    },
    /// 输出定稿(§13.3)。
    OutputFinal {
        item_id: ItemId,
        revision: u64,
        byte_length: u64,
        channel: OutputChannel,
        /// 权威最终结果不可读取时的 FINAL_OUTPUT_UNAVAILABLE 标记。
        #[serde(default)]
        final_unavailable: bool,
    },
    /// 摘要变化,直接携带完整 SessionSummary(第 1 优先级,不可丢弃)。
    SessionSummaryChanged { summary: SessionSummary },
    /// 问题/审批出现(第 1 优先级)。
    PendingAttentionAdded { attention: PendingAttention },
    /// 问题/审批移除(回答、失效或 turn 结束)。
    PendingAttentionRemoved {
        kind: PendingAttentionKind,
        native_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
    },
    /// 队列状态变化(§15.3;正文不进入本事件)。
    QueueStateChanged { queue: QueueStatus },
    /// 后台命令变化(§10.8)。
    BackgroundCommandChanged { command: BackgroundCommand },
    /// 能力变化(probe 结果或设置可选值变化)。
    CapabilityChanged { capabilities: CapabilitySet },
}
