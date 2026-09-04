//! 多维状态模型(§10)。禁止单一 `session_status`;各维度独立表达。

use serde::{Deserialize, Serialize};

/// §10.1 设备连接状态。无法区分关机/休眠/断网时统一为 OFFLINE,不猜测原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeviceConnection {
    Connecting,
    Online,
    Degraded,
    Offline,
}

/// §10.2 控制模式,由 Adapter capability probe 决定,不由网页自行推断。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlMode {
    FullControl,
    LimitedControl,
    ReadOnly,
    Unavailable,
}

/// §10.3 兼容状态。未知版本默认 DEGRADED + READ_ONLY(§5)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityState {
    Verified,
    Degraded,
    Unsupported,
}

/// §10.4 当前 turn 阶段。等待用户/审批不作为互斥 phase,放入 PendingAttention。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActiveTurnPhase {
    Idle,
    Running,
    Finishing,
}

/// §10.5 待处理关注类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PendingAttentionKind {
    UserQuestion,
    RiskApproval,
}

/// §10.6 单条下一轮队列状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueueState {
    Empty,
    Queued,
    Paused,
}

/// §10.7 上一轮结果。只描述上一轮,不把任务永久标成失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LastTurnOutcome {
    Completed,
    Failed,
    Interrupted,
    Unknown,
}

/// §10.8 后台命令状态。主 turn 完成后允许仍显示后台命令,不得发明
/// "所有 Agent 完成"之类的综合状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackgroundCommandState {
    Running,
    Completed,
    Failed,
    Stopped,
    Unknown,
}

/// Plan 步骤状态(§9.3 Plan 与步骤状态;工具调用状态复用)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PlanStepStatus {
    Pending,
    InProgress,
    Completed,
}

/// §13.2 输出通道。原生无法区分 stdout/stderr 时只能 COMBINED,不得猜测。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutputChannel {
    Stdout,
    Stderr,
    Combined,
}
