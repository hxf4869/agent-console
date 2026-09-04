//! Bridge 内部强类型领域模型(权威规格 §9-§16)。
//!
//! - 所有类型 serde Serialize/Deserialize;`Debug` 输出脱敏:只含 ID/状态/长度,
//!   正文全文不出现(正文类字段用 [`text::OutputText`]/[`text::OutputBytes`])。
//! - 语义与 proto/agent_console/v1 对齐,但这是 Bridge 内部类型,不依赖 proto。
//! - 事件枚举 [`events::DomainEvent`] 与队列/回执类型供 runtime/commands
//!   工作流直接消费。

pub mod attention;
pub mod capability;
pub mod commands;
pub mod error;
pub mod events;
pub mod ids;
pub mod items;
pub mod states;
pub mod summary;
pub mod text;

pub use attention::{
    ApprovalDecision, PendingApproval, PendingAttention, PendingQuestion, QuestionOption,
};
pub use capability::{
    CapabilitySet, Operation, SettingKind, SettingOption, SettingUpdate, SettingValue,
    TransferLimits,
};
pub use commands::{CommandPayload, CommandReceipt, CommandRequest, QueuedTurn, ReceiptState};
pub use error::{BridgeError, StableErrorCode};
pub use events::DomainEvent;
pub use ids::{AgentKind, ItemId, SessionKey, TurnId};
pub use items::{
    CommandStatusState, FileChange, FileChangeKind, HistoryEntry, HistoryPage, Item, ItemContent,
    Plan, PlanStep, Turn,
};
pub use states::{
    ActiveTurnPhase, BackgroundCommandState, CompatibilityState, ControlMode, DeviceConnection,
    LastTurnOutcome, OutputChannel, PendingAttentionKind, PlanStepStatus, QueueState,
};
pub use summary::{
    BackgroundCommand, CurrentTurn, OutputCursor, QueueStatus, RunningCommand, RuntimeSnapshot,
    SessionSummary,
};
pub use text::{OutputBytes, OutputText};
