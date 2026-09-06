//! domain ↔ proto 映射(Bridge runtime 集成层;权威规格 §9-§17)。
//!
//! 职责:把 Bridge 内部领域类型与 `agent_console_protocol::v1` 消息互相
//! 转换,供 Envelope 出入站使用。规则:
//! - `OutputText`/`OutputBytes` 正文只进对应 payload 字段;本模块不打印、
//!   不记录任何正文(§13/§25.3)。
//! - turn/item 的 `synthetic` 标记原样保留(§9.2)。
//! - `agent_kind` 按会话键如实映射(CODEX_DESKTOP / ZCODE_DESKTOP,ZC-02);
//!   未知 proto 数值显式拒绝,不默认当作 Codex。
//! - 领域 `ItemContent::Opaque` 在 v1 proto 无对应 variant(§17.3 禁止为
//!   未消化原生数据开通道):映射返回 `None`,由调用方跳过该事件;领域层
//!   仍保留完整信息。
//! - 领域 `OutputCursor.final_unavailable` 在 proto OutputCursor 无字段,
//!   只经事件/查询错误码表达(§13.3),映射时丢弃。

use agent_console_protocol::agent_console::v1 as pb;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use uuid::Uuid;

use crate::domain as dm;

// ---------------------------------------------------------------------------
// 时间与基础标识
// ---------------------------------------------------------------------------

pub fn timestamp_to_proto(value: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

pub fn timestamp_from_proto(value: &Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(value.seconds, value.nanos.max(0) as u32).unwrap_or_else(Utc::now)
}

pub fn opt_timestamp_to_proto(value: Option<DateTime<Utc>>) -> Option<Timestamp> {
    value.map(timestamp_to_proto)
}

pub fn opt_timestamp_from_proto(value: &Option<Timestamp>) -> Option<DateTime<Utc>> {
    value.as_ref().map(timestamp_from_proto)
}

pub fn session_key_to_proto(key: &dm::SessionKey) -> pb::SessionKey {
    pb::SessionKey {
        device_id: key.device_id.clone(),
        agent_kind: key.agent_kind.proto_value(),
        native_session_id: key.native_session_id.clone(),
        relay_session_uuid: key
            .relay_session_uuid
            .map(|u| u.to_string())
            .unwrap_or_default(),
    }
}

/// proto SessionKey → 领域。`agent_kind` 未知/未指定时返回 None:
/// 调用方必须显式拒绝,不得默认当作 Codex(ZC-02 路由约束)。
pub fn session_key_from_proto(key: &pb::SessionKey) -> Option<dm::SessionKey> {
    Some(dm::SessionKey {
        device_id: key.device_id.clone(),
        agent_kind: dm::AgentKind::from_proto_value(key.agent_kind)?,
        native_session_id: key.native_session_id.clone(),
        relay_session_uuid: Uuid::parse_str(&key.relay_session_uuid).ok(),
    })
}

pub fn turn_id_to_proto(id: &dm::TurnId) -> pb::TurnId {
    pb::TurnId {
        id: id.id.clone(),
        synthetic: id.synthetic,
    }
}

pub fn turn_id_from_proto(id: &pb::TurnId) -> dm::TurnId {
    dm::TurnId {
        id: id.id.clone(),
        synthetic: id.synthetic,
    }
}

pub fn opt_turn_id_to_proto(id: Option<&dm::TurnId>) -> Option<pb::TurnId> {
    id.map(turn_id_to_proto)
}

pub fn opt_turn_id_from_proto(id: &Option<pb::TurnId>) -> Option<dm::TurnId> {
    id.as_ref().map(turn_id_from_proto)
}

pub fn item_id_to_proto(id: &dm::ItemId) -> pb::ItemId {
    pb::ItemId {
        id: id.id.clone(),
        synthetic: id.synthetic,
    }
}

pub fn item_id_from_proto(id: &pb::ItemId) -> dm::ItemId {
    dm::ItemId {
        id: id.id.clone(),
        synthetic: id.synthetic,
    }
}

// ---------------------------------------------------------------------------
// §10 多维状态枚举
// ---------------------------------------------------------------------------

pub fn device_connection_to_proto(value: dm::DeviceConnection) -> pb::DeviceConnection {
    match value {
        dm::DeviceConnection::Connecting => pb::DeviceConnection::ConnectionConnecting,
        dm::DeviceConnection::Online => pb::DeviceConnection::ConnectionOnline,
        dm::DeviceConnection::Degraded => pb::DeviceConnection::ConnectionDegraded,
        dm::DeviceConnection::Offline => pb::DeviceConnection::ConnectionOffline,
    }
}

pub fn device_connection_from_proto(value: pb::DeviceConnection) -> dm::DeviceConnection {
    match value {
        pb::DeviceConnection::ConnectionConnecting => dm::DeviceConnection::Connecting,
        pb::DeviceConnection::ConnectionOnline => dm::DeviceConnection::Online,
        pb::DeviceConnection::ConnectionDegraded => dm::DeviceConnection::Degraded,
        pb::DeviceConnection::ConnectionOffline => dm::DeviceConnection::Offline,
        _ => dm::DeviceConnection::Offline,
    }
}

pub fn control_mode_to_proto(value: dm::ControlMode) -> pb::ControlMode {
    match value {
        dm::ControlMode::FullControl => pb::ControlMode::FullControl,
        dm::ControlMode::LimitedControl => pb::ControlMode::LimitedControl,
        dm::ControlMode::ReadOnly => pb::ControlMode::ReadOnly,
        dm::ControlMode::Unavailable => pb::ControlMode::Unavailable,
    }
}

pub fn control_mode_from_proto(value: pb::ControlMode) -> dm::ControlMode {
    match value {
        pb::ControlMode::FullControl => dm::ControlMode::FullControl,
        pb::ControlMode::LimitedControl => dm::ControlMode::LimitedControl,
        pb::ControlMode::ReadOnly => dm::ControlMode::ReadOnly,
        pb::ControlMode::Unavailable => dm::ControlMode::Unavailable,
        _ => dm::ControlMode::Unavailable,
    }
}

pub fn compatibility_state_to_proto(value: dm::CompatibilityState) -> pb::CompatibilityState {
    match value {
        dm::CompatibilityState::Verified => pb::CompatibilityState::CompatibilityVerified,
        dm::CompatibilityState::Degraded => pb::CompatibilityState::CompatibilityDegraded,
        dm::CompatibilityState::Unsupported => pb::CompatibilityState::CompatibilityUnsupported,
    }
}

pub fn compatibility_state_from_proto(value: pb::CompatibilityState) -> dm::CompatibilityState {
    match value {
        pb::CompatibilityState::CompatibilityVerified => dm::CompatibilityState::Verified,
        pb::CompatibilityState::CompatibilityDegraded => dm::CompatibilityState::Degraded,
        pb::CompatibilityState::CompatibilityUnsupported => dm::CompatibilityState::Unsupported,
        _ => dm::CompatibilityState::Degraded,
    }
}

pub fn turn_phase_to_proto(value: dm::ActiveTurnPhase) -> pb::ActiveTurnPhase {
    match value {
        dm::ActiveTurnPhase::Idle => pb::ActiveTurnPhase::TurnPhaseIdle,
        dm::ActiveTurnPhase::Running => pb::ActiveTurnPhase::TurnPhaseRunning,
        dm::ActiveTurnPhase::Finishing => pb::ActiveTurnPhase::TurnPhaseFinishing,
    }
}

pub fn turn_phase_from_proto(value: pb::ActiveTurnPhase) -> dm::ActiveTurnPhase {
    match value {
        pb::ActiveTurnPhase::TurnPhaseIdle => dm::ActiveTurnPhase::Idle,
        pb::ActiveTurnPhase::TurnPhaseRunning => dm::ActiveTurnPhase::Running,
        pb::ActiveTurnPhase::TurnPhaseFinishing => dm::ActiveTurnPhase::Finishing,
        _ => dm::ActiveTurnPhase::Idle,
    }
}

pub fn attention_kind_to_proto(value: dm::PendingAttentionKind) -> pb::PendingAttentionKind {
    match value {
        dm::PendingAttentionKind::UserQuestion => pb::PendingAttentionKind::AttentionUserQuestion,
        dm::PendingAttentionKind::RiskApproval => pb::PendingAttentionKind::AttentionRiskApproval,
    }
}

pub fn attention_kind_from_proto(value: pb::PendingAttentionKind) -> dm::PendingAttentionKind {
    match value {
        pb::PendingAttentionKind::AttentionUserQuestion => dm::PendingAttentionKind::UserQuestion,
        pb::PendingAttentionKind::AttentionRiskApproval => dm::PendingAttentionKind::RiskApproval,
        _ => dm::PendingAttentionKind::UserQuestion,
    }
}

pub fn queue_state_to_proto(value: dm::QueueState) -> pb::QueueState {
    match value {
        dm::QueueState::Empty => pb::QueueState::Empty,
        dm::QueueState::Queued => pb::QueueState::Queued,
        dm::QueueState::Paused => pb::QueueState::Paused,
    }
}

pub fn queue_state_from_proto(value: pb::QueueState) -> dm::QueueState {
    match value {
        pb::QueueState::Empty => dm::QueueState::Empty,
        pb::QueueState::Queued => dm::QueueState::Queued,
        pb::QueueState::Paused => dm::QueueState::Paused,
        _ => dm::QueueState::Empty,
    }
}

pub fn turn_outcome_to_proto(value: dm::LastTurnOutcome) -> pb::LastTurnOutcome {
    match value {
        dm::LastTurnOutcome::Completed => pb::LastTurnOutcome::TurnOutcomeCompleted,
        dm::LastTurnOutcome::Failed => pb::LastTurnOutcome::TurnOutcomeFailed,
        dm::LastTurnOutcome::Interrupted => pb::LastTurnOutcome::TurnOutcomeInterrupted,
        dm::LastTurnOutcome::Unknown => pb::LastTurnOutcome::TurnOutcomeUnknown,
    }
}

pub fn turn_outcome_from_proto(value: pb::LastTurnOutcome) -> dm::LastTurnOutcome {
    match value {
        pb::LastTurnOutcome::TurnOutcomeCompleted => dm::LastTurnOutcome::Completed,
        pb::LastTurnOutcome::TurnOutcomeFailed => dm::LastTurnOutcome::Failed,
        pb::LastTurnOutcome::TurnOutcomeInterrupted => dm::LastTurnOutcome::Interrupted,
        pb::LastTurnOutcome::TurnOutcomeUnknown => dm::LastTurnOutcome::Unknown,
        _ => dm::LastTurnOutcome::Unknown,
    }
}

pub fn background_state_to_proto(value: dm::BackgroundCommandState) -> pb::BackgroundCommandState {
    match value {
        dm::BackgroundCommandState::Running => pb::BackgroundCommandState::BackgroundCmdRunning,
        dm::BackgroundCommandState::Completed => pb::BackgroundCommandState::BackgroundCmdCompleted,
        dm::BackgroundCommandState::Failed => pb::BackgroundCommandState::BackgroundCmdFailed,
        dm::BackgroundCommandState::Stopped => pb::BackgroundCommandState::BackgroundCmdStopped,
        dm::BackgroundCommandState::Unknown => pb::BackgroundCommandState::BackgroundCmdUnknown,
    }
}

pub fn background_state_from_proto(
    value: pb::BackgroundCommandState,
) -> dm::BackgroundCommandState {
    match value {
        pb::BackgroundCommandState::BackgroundCmdRunning => dm::BackgroundCommandState::Running,
        pb::BackgroundCommandState::BackgroundCmdCompleted => dm::BackgroundCommandState::Completed,
        pb::BackgroundCommandState::BackgroundCmdFailed => dm::BackgroundCommandState::Failed,
        pb::BackgroundCommandState::BackgroundCmdStopped => dm::BackgroundCommandState::Stopped,
        pb::BackgroundCommandState::BackgroundCmdUnknown => dm::BackgroundCommandState::Unknown,
        _ => dm::BackgroundCommandState::Unknown,
    }
}

pub fn output_channel_to_proto(value: dm::OutputChannel) -> pb::OutputChannel {
    match value {
        dm::OutputChannel::Stdout => pb::OutputChannel::Stdout,
        dm::OutputChannel::Stderr => pb::OutputChannel::Stderr,
        dm::OutputChannel::Combined => pb::OutputChannel::Combined,
    }
}

pub fn output_channel_from_proto(value: pb::OutputChannel) -> dm::OutputChannel {
    match value {
        pb::OutputChannel::Stdout => dm::OutputChannel::Stdout,
        pb::OutputChannel::Stderr => dm::OutputChannel::Stderr,
        pb::OutputChannel::Combined => dm::OutputChannel::Combined,
        _ => dm::OutputChannel::Combined,
    }
}

pub fn plan_step_status_to_proto(value: dm::PlanStepStatus) -> pb::PlanStepStatus {
    match value {
        dm::PlanStepStatus::Pending => pb::PlanStepStatus::PlanStepPending,
        dm::PlanStepStatus::InProgress => pb::PlanStepStatus::PlanStepInProgress,
        dm::PlanStepStatus::Completed => pb::PlanStepStatus::PlanStepCompleted,
    }
}

pub fn plan_step_status_from_proto(value: pb::PlanStepStatus) -> dm::PlanStepStatus {
    match value {
        pb::PlanStepStatus::PlanStepPending => dm::PlanStepStatus::Pending,
        pb::PlanStepStatus::PlanStepInProgress => dm::PlanStepStatus::InProgress,
        pb::PlanStepStatus::PlanStepCompleted => dm::PlanStepStatus::Completed,
        _ => dm::PlanStepStatus::Pending,
    }
}

pub fn file_change_kind_to_proto(value: dm::FileChangeKind) -> pb::FileChangeKind {
    match value {
        dm::FileChangeKind::Added => pb::FileChangeKind::FileChangeAdded,
        dm::FileChangeKind::Modified => pb::FileChangeKind::FileChangeModified,
        dm::FileChangeKind::Deleted => pb::FileChangeKind::FileChangeDeleted,
        dm::FileChangeKind::Renamed => pb::FileChangeKind::FileChangeRenamed,
        dm::FileChangeKind::Unknown => pb::FileChangeKind::Unspecified,
    }
}

pub fn file_change_kind_from_proto(value: pb::FileChangeKind) -> dm::FileChangeKind {
    match value {
        pb::FileChangeKind::FileChangeAdded => dm::FileChangeKind::Added,
        pb::FileChangeKind::FileChangeModified => dm::FileChangeKind::Modified,
        pb::FileChangeKind::FileChangeDeleted => dm::FileChangeKind::Deleted,
        pb::FileChangeKind::FileChangeRenamed => dm::FileChangeKind::Renamed,
        _ => dm::FileChangeKind::Unknown,
    }
}

pub fn stable_error_to_proto(value: dm::StableErrorCode) -> pb::StableErrorCode {
    match value {
        dm::StableErrorCode::AuthRequired => pb::StableErrorCode::AuthRequired,
        dm::StableErrorCode::AuthExpired => pb::StableErrorCode::AuthExpired,
        dm::StableErrorCode::CsrfInvalid => pb::StableErrorCode::CsrfInvalid,
        dm::StableErrorCode::WsTicketInvalid => pb::StableErrorCode::WsTicketInvalid,
        dm::StableErrorCode::WsTicketExpired => pb::StableErrorCode::WsTicketExpired,
        dm::StableErrorCode::WsTicketConsumed => pb::StableErrorCode::WsTicketConsumed,
        dm::StableErrorCode::DeviceOffline => pb::StableErrorCode::DeviceOffline,
        dm::StableErrorCode::DeviceRevoked => pb::StableErrorCode::DeviceRevoked,
        dm::StableErrorCode::CodexUnavailable => pb::StableErrorCode::CodexUnavailable,
        dm::StableErrorCode::CodexVersionUnverified => pb::StableErrorCode::CodexVersionUnverified,
        dm::StableErrorCode::ControlReadOnly => pb::StableErrorCode::ControlReadOnly,
        dm::StableErrorCode::CapabilityUnsupported => pb::StableErrorCode::CapabilityUnsupported,
        dm::StableErrorCode::SessionNotFound => pb::StableErrorCode::SessionNotFound,
        dm::StableErrorCode::StaleTurn => pb::StableErrorCode::StaleTurn,
        dm::StableErrorCode::DuplicateRequestMismatch => {
            pb::StableErrorCode::DuplicateRequestMismatch
        }
        dm::StableErrorCode::OutcomeUnknown => pb::StableErrorCode::OutcomeUnknown,
        dm::StableErrorCode::ResyncRequired => pb::StableErrorCode::ResyncRequired,
        dm::StableErrorCode::QueueAlreadyExists => pb::StableErrorCode::QueueAlreadyExists,
        dm::StableErrorCode::QueuePaused => pb::StableErrorCode::QueuePaused,
        dm::StableErrorCode::QuestionExpired => pb::StableErrorCode::QuestionExpired,
        dm::StableErrorCode::ApprovalExpired => pb::StableErrorCode::ApprovalExpired,
        dm::StableErrorCode::SettingCombinationUnsupported => {
            pb::StableErrorCode::SettingCombinationUnsupported
        }
        dm::StableErrorCode::FileHandleInvalid => pb::StableErrorCode::FileHandleInvalid,
        dm::StableErrorCode::FileOutsideScope => pb::StableErrorCode::FileOutsideScope,
        dm::StableErrorCode::FileChanged => pb::StableErrorCode::FileChanged,
        dm::StableErrorCode::FileTypeNotPreviewable => pb::StableErrorCode::FileTypeNotPreviewable,
        dm::StableErrorCode::TransferExpired => pb::StableErrorCode::TransferExpired,
        dm::StableErrorCode::TransferTooLarge => pb::StableErrorCode::TransferTooLarge,
        dm::StableErrorCode::TransferRangeInvalid => pb::StableErrorCode::TransferRangeInvalid,
        dm::StableErrorCode::DiffTooLarge => pb::StableErrorCode::DiffTooLarge,
        dm::StableErrorCode::RateLimited => pb::StableErrorCode::RateLimited,
        dm::StableErrorCode::InternalError => pb::StableErrorCode::InternalError,
    }
}

pub fn stable_error_from_proto(value: pb::StableErrorCode) -> dm::StableErrorCode {
    match value {
        pb::StableErrorCode::AuthRequired => dm::StableErrorCode::AuthRequired,
        pb::StableErrorCode::AuthExpired => dm::StableErrorCode::AuthExpired,
        pb::StableErrorCode::CsrfInvalid => dm::StableErrorCode::CsrfInvalid,
        pb::StableErrorCode::WsTicketInvalid => dm::StableErrorCode::WsTicketInvalid,
        pb::StableErrorCode::WsTicketExpired => dm::StableErrorCode::WsTicketExpired,
        pb::StableErrorCode::WsTicketConsumed => dm::StableErrorCode::WsTicketConsumed,
        pb::StableErrorCode::DeviceOffline => dm::StableErrorCode::DeviceOffline,
        pb::StableErrorCode::DeviceRevoked => dm::StableErrorCode::DeviceRevoked,
        pb::StableErrorCode::CodexUnavailable => dm::StableErrorCode::CodexUnavailable,
        pb::StableErrorCode::CodexVersionUnverified => dm::StableErrorCode::CodexVersionUnverified,
        pb::StableErrorCode::ControlReadOnly => dm::StableErrorCode::ControlReadOnly,
        pb::StableErrorCode::CapabilityUnsupported => dm::StableErrorCode::CapabilityUnsupported,
        pb::StableErrorCode::SessionNotFound => dm::StableErrorCode::SessionNotFound,
        pb::StableErrorCode::StaleTurn => dm::StableErrorCode::StaleTurn,
        pb::StableErrorCode::DuplicateRequestMismatch => {
            dm::StableErrorCode::DuplicateRequestMismatch
        }
        pb::StableErrorCode::OutcomeUnknown => dm::StableErrorCode::OutcomeUnknown,
        pb::StableErrorCode::ResyncRequired => dm::StableErrorCode::ResyncRequired,
        pb::StableErrorCode::QueueAlreadyExists => dm::StableErrorCode::QueueAlreadyExists,
        pb::StableErrorCode::QueuePaused => dm::StableErrorCode::QueuePaused,
        pb::StableErrorCode::QuestionExpired => dm::StableErrorCode::QuestionExpired,
        pb::StableErrorCode::ApprovalExpired => dm::StableErrorCode::ApprovalExpired,
        pb::StableErrorCode::SettingCombinationUnsupported => {
            dm::StableErrorCode::SettingCombinationUnsupported
        }
        pb::StableErrorCode::FileHandleInvalid => dm::StableErrorCode::FileHandleInvalid,
        pb::StableErrorCode::FileOutsideScope => dm::StableErrorCode::FileOutsideScope,
        pb::StableErrorCode::FileChanged => dm::StableErrorCode::FileChanged,
        pb::StableErrorCode::FileTypeNotPreviewable => dm::StableErrorCode::FileTypeNotPreviewable,
        pb::StableErrorCode::TransferExpired => dm::StableErrorCode::TransferExpired,
        pb::StableErrorCode::TransferTooLarge => dm::StableErrorCode::TransferTooLarge,
        pb::StableErrorCode::TransferRangeInvalid => dm::StableErrorCode::TransferRangeInvalid,
        pb::StableErrorCode::DiffTooLarge => dm::StableErrorCode::DiffTooLarge,
        pb::StableErrorCode::RateLimited => dm::StableErrorCode::RateLimited,
        pb::StableErrorCode::InternalError => dm::StableErrorCode::InternalError,
        _ => dm::StableErrorCode::InternalError,
    }
}

// ---------------------------------------------------------------------------
// §11 SessionSummary / RuntimeSnapshot / HistoryPage
// ---------------------------------------------------------------------------

pub fn session_summary_to_proto(summary: &dm::SessionSummary) -> pb::SessionSummary {
    pb::SessionSummary {
        session_key: Some(session_key_to_proto(&summary.session_key)),
        title: summary
            .title
            .as_ref()
            .map(|t| t.as_str().to_owned())
            .unwrap_or_default(),
        agent_kind: summary.session_key.agent_kind.proto_value(),
        project_display_name: summary.project_display_name.clone().unwrap_or_default(),
        current_branch: summary.current_branch.clone().unwrap_or_default(),
        updated_at: opt_timestamp_to_proto(summary.updated_at),
        device_connection: device_connection_to_proto(summary.device_connection) as i32,
        device_last_seen_at: opt_timestamp_to_proto(summary.device_last_seen_at),
        degraded_reason: summary.degraded_reason.clone().unwrap_or_default(),
        control_mode: control_mode_to_proto(summary.control_mode) as i32,
        compatibility_state: compatibility_state_to_proto(summary.compatibility_state) as i32,
        active_turn_phase: turn_phase_to_proto(summary.active_turn_phase) as i32,
        pending_attention_count: summary.pending_attention_count,
        pending_attention_kinds: summary
            .pending_attention_kinds
            .iter()
            .map(|k| attention_kind_to_proto(*k) as i32)
            .collect(),
        queue_state: queue_state_to_proto(summary.queue_state) as i32,
        last_turn_outcome: turn_outcome_to_proto(summary.last_turn_outcome) as i32,
        pinned: summary.pinned,
        muted: summary.muted,
        archived: summary.archived,
    }
}

pub fn session_summary_from_proto(p: &pb::SessionSummary) -> Option<dm::SessionSummary> {
    let session_key = p
        .session_key
        .as_ref()
        .and_then(session_key_from_proto)?;
    Some(dm::SessionSummary {
        session_key: session_key.clone(),
        title: if p.title.is_empty() {
            None
        } else {
            Some(dm::OutputText::new(p.title.clone()))
        },
        agent_kind: session_key.agent_kind,
        project_display_name: if p.project_display_name.is_empty() {
            None
        } else {
            Some(p.project_display_name.clone())
        },
        current_branch: if p.current_branch.is_empty() {
            None
        } else {
            Some(p.current_branch.clone())
        },
        updated_at: opt_timestamp_from_proto(&p.updated_at),
        device_connection: p
            .device_connection
            .try_into()
            .map(device_connection_from_proto)
            .unwrap_or(dm::DeviceConnection::Offline),
        device_last_seen_at: opt_timestamp_from_proto(&p.device_last_seen_at),
        degraded_reason: if p.degraded_reason.is_empty() {
            None
        } else {
            Some(p.degraded_reason.clone())
        },
        control_mode: p
            .control_mode
            .try_into()
            .map(control_mode_from_proto)
            .unwrap_or(dm::ControlMode::Unavailable),
        compatibility_state: p
            .compatibility_state
            .try_into()
            .map(compatibility_state_from_proto)
            .unwrap_or(dm::CompatibilityState::Degraded),
        active_turn_phase: p
            .active_turn_phase
            .try_into()
            .map(turn_phase_from_proto)
            .unwrap_or(dm::ActiveTurnPhase::Idle),
        pending_attention_count: p.pending_attention_count,
        pending_attention_kinds: p
            .pending_attention_kinds
            .iter()
            .filter_map(|k| (*k).try_into().ok().map(attention_kind_from_proto))
            .collect(),
        queue_state: p
            .queue_state
            .try_into()
            .map(queue_state_from_proto)
            .unwrap_or(dm::QueueState::Empty),
        last_turn_outcome: p
            .last_turn_outcome
            .try_into()
            .map(turn_outcome_from_proto)
            .unwrap_or(dm::LastTurnOutcome::Unknown),
        pinned: p.pinned,
        muted: p.muted,
        archived: p.archived,
    })
}

pub fn queue_status_to_proto(queue: &dm::QueueStatus) -> pb::QueueStatus {
    pb::QueueStatus {
        state: queue_state_to_proto(queue.state) as i32,
        after_turn_id: opt_turn_id_to_proto(queue.after_turn_id.as_ref()),
        accepted_runtime_revision: queue.accepted_runtime_revision.unwrap_or(0),
    }
}

pub fn queue_status_from_proto(p: &pb::QueueStatus) -> dm::QueueStatus {
    dm::QueueStatus {
        state: p
            .state
            .try_into()
            .map(queue_state_from_proto)
            .unwrap_or(dm::QueueState::Empty),
        after_turn_id: opt_turn_id_from_proto(&p.after_turn_id),
        accepted_runtime_revision: if p.accepted_runtime_revision == 0 {
            None
        } else {
            Some(p.accepted_runtime_revision)
        },
    }
}

pub fn output_cursor_to_proto(cursor: &dm::OutputCursor) -> pb::OutputCursor {
    pb::OutputCursor {
        item_id: Some(item_id_to_proto(&cursor.item_id)),
        revision: cursor.revision,
        byte_length: cursor.byte_length,
        is_final: cursor.is_final,
        channel: output_channel_to_proto(cursor.channel) as i32,
    }
}

pub fn output_cursor_from_proto(p: &pb::OutputCursor) -> dm::OutputCursor {
    dm::OutputCursor {
        item_id: p
            .item_id
            .as_ref()
            .map(item_id_from_proto)
            .unwrap_or_else(|| dm::ItemId::synthetic("")),
        revision: p.revision,
        byte_length: p.byte_length,
        is_final: p.is_final,
        channel: p
            .channel
            .try_into()
            .map(output_channel_from_proto)
            .unwrap_or(dm::OutputChannel::Combined),
        final_unavailable: false,
    }
}

pub fn current_turn_to_proto(turn: &dm::CurrentTurn) -> pb::CurrentTurn {
    pb::CurrentTurn {
        turn: Some(turn_id_to_proto(&turn.turn)),
        phase: turn_phase_to_proto(turn.phase) as i32,
        started_at: opt_timestamp_to_proto(turn.started_at),
    }
}

pub fn current_turn_from_proto(p: &pb::CurrentTurn) -> dm::CurrentTurn {
    dm::CurrentTurn {
        turn: p
            .turn
            .as_ref()
            .map(turn_id_from_proto)
            .unwrap_or_else(|| dm::TurnId::synthetic("")),
        phase: p
            .phase
            .try_into()
            .map(turn_phase_from_proto)
            .unwrap_or(dm::ActiveTurnPhase::Idle),
        started_at: opt_timestamp_from_proto(&p.started_at),
    }
}

pub fn plan_to_proto(plan: &dm::Plan) -> pb::Plan {
    pb::Plan {
        steps: plan
            .steps
            .iter()
            .map(|step| pb::PlanStep {
                step_id: step.step_id.clone(),
                title: step.title.as_str().to_owned(),
                status: plan_step_status_to_proto(step.status) as i32,
            })
            .collect(),
    }
}

pub fn plan_from_proto(p: &pb::Plan) -> dm::Plan {
    dm::Plan {
        steps: p
            .steps
            .iter()
            .map(|step| dm::PlanStep {
                step_id: step.step_id.clone(),
                title: dm::OutputText::new(step.title.clone()),
                status: step
                    .status
                    .try_into()
                    .map(plan_step_status_from_proto)
                    .unwrap_or(dm::PlanStepStatus::Pending),
            })
            .collect(),
    }
}

pub fn running_command_to_proto(cmd: &dm::RunningCommand) -> pb::RunningCommand {
    pb::RunningCommand {
        command_id: cmd.command_id.clone(),
        item_id: cmd.item_id.as_ref().map(item_id_to_proto),
        started_at: opt_timestamp_to_proto(cmd.started_at),
    }
}

pub fn running_command_from_proto(p: &pb::RunningCommand) -> dm::RunningCommand {
    dm::RunningCommand {
        command_id: p.command_id.clone(),
        item_id: p.item_id.as_ref().map(item_id_from_proto),
        started_at: opt_timestamp_from_proto(&p.started_at),
    }
}

pub fn background_command_to_proto(cmd: &dm::BackgroundCommand) -> pb::BackgroundCommand {
    pb::BackgroundCommand {
        command_id: cmd.command_id.clone().unwrap_or_default(),
        item_id: cmd.item_id.as_ref().map(item_id_to_proto),
        state: background_state_to_proto(cmd.state) as i32,
        started_at: opt_timestamp_to_proto(cmd.started_at),
        finished_at: opt_timestamp_to_proto(cmd.finished_at),
    }
}

pub fn background_command_from_proto(p: &pb::BackgroundCommand) -> dm::BackgroundCommand {
    dm::BackgroundCommand {
        command_id: if p.command_id.is_empty() {
            None
        } else {
            Some(p.command_id.clone())
        },
        item_id: p.item_id.as_ref().map(item_id_from_proto),
        state: p
            .state
            .try_into()
            .map(background_state_from_proto)
            .unwrap_or(dm::BackgroundCommandState::Unknown),
        display: None,
        started_at: opt_timestamp_from_proto(&p.started_at),
        finished_at: opt_timestamp_from_proto(&p.finished_at),
    }
}

// ---------------------------------------------------------------------------
// §16 能力 / 设置 / §10.5 attention
// ---------------------------------------------------------------------------

pub fn setting_kind_to_proto(value: dm::SettingKind) -> pb::SettingKind {
    match value {
        dm::SettingKind::Model => pb::SettingKind::Model,
        dm::SettingKind::ReasoningEffort => pb::SettingKind::ReasoningEffort,
        dm::SettingKind::ServiceTier => pb::SettingKind::ServiceTier,
        dm::SettingKind::PermissionMode => pb::SettingKind::PermissionMode,
        dm::SettingKind::CollaborationMode => pb::SettingKind::CollaborationMode,
    }
}

pub fn setting_kind_from_proto(value: pb::SettingKind) -> Option<dm::SettingKind> {
    match value {
        pb::SettingKind::Model => Some(dm::SettingKind::Model),
        pb::SettingKind::ReasoningEffort => Some(dm::SettingKind::ReasoningEffort),
        pb::SettingKind::ServiceTier => Some(dm::SettingKind::ServiceTier),
        pb::SettingKind::PermissionMode => Some(dm::SettingKind::PermissionMode),
        pb::SettingKind::CollaborationMode => Some(dm::SettingKind::CollaborationMode),
        _ => None,
    }
}

pub fn transfer_limits_to_proto(limits: &dm::TransferLimits) -> pb::TransferLimits {
    pb::TransferLimits {
        text_inline_max_bytes: limits.text_inline_max_bytes,
        image_inline_max_bytes: limits.image_inline_max_bytes,
        pdf_range_max_bytes: limits.pdf_range_max_bytes,
        download_max_bytes: limits.download_max_bytes,
        upload_max_bytes: limits.upload_max_bytes,
        max_concurrent_per_browser: limits.max_concurrent_per_browser,
        max_concurrent_per_device: limits.max_concurrent_per_device,
        previewable_mime_prefixes: limits.previewable_mime_prefixes.clone(),
    }
}

pub fn transfer_limits_from_proto(p: &pb::TransferLimits) -> dm::TransferLimits {
    // 数值为 0 视为未填,回退首版默认(§22.3 集中定义)。
    let defaults = dm::TransferLimits::default();
    dm::TransferLimits {
        text_inline_max_bytes: if p.text_inline_max_bytes == 0 {
            defaults.text_inline_max_bytes
        } else {
            p.text_inline_max_bytes
        },
        image_inline_max_bytes: if p.image_inline_max_bytes == 0 {
            defaults.image_inline_max_bytes
        } else {
            p.image_inline_max_bytes
        },
        pdf_range_max_bytes: if p.pdf_range_max_bytes == 0 {
            defaults.pdf_range_max_bytes
        } else {
            p.pdf_range_max_bytes
        },
        download_max_bytes: if p.download_max_bytes == 0 {
            defaults.download_max_bytes
        } else {
            p.download_max_bytes
        },
        upload_max_bytes: if p.upload_max_bytes == 0 {
            defaults.upload_max_bytes
        } else {
            p.upload_max_bytes
        },
        max_concurrent_per_browser: if p.max_concurrent_per_browser == 0 {
            defaults.max_concurrent_per_browser
        } else {
            p.max_concurrent_per_browser
        },
        max_concurrent_per_device: if p.max_concurrent_per_device == 0 {
            defaults.max_concurrent_per_device
        } else {
            p.max_concurrent_per_device
        },
        previewable_mime_prefixes: if p.previewable_mime_prefixes.is_empty() {
            defaults.previewable_mime_prefixes
        } else {
            p.previewable_mime_prefixes.clone()
        },
    }
}

pub fn setting_option_to_proto(option: &dm::SettingOption) -> pb::SettingOption {
    pb::SettingOption {
        option_id: option.option_id.clone(),
        kind: setting_kind_to_proto(option.kind) as i32,
        label: option.label.as_str().to_owned(),
        current_value: option.current_value.clone().unwrap_or_default(),
        available_values: option
            .available_values
            .iter()
            .map(|v| pb::SettingValue {
                value: v.value.clone(),
                label: v.label.as_str().to_owned(),
            })
            .collect(),
        mutable: option.mutable,
    }
}

pub fn setting_option_from_proto(p: &pb::SettingOption) -> Option<dm::SettingOption> {
    let kind = setting_kind_from_proto(p.kind.try_into().unwrap_or(pb::SettingKind::Unspecified))?;
    Some(dm::SettingOption {
        option_id: p.option_id.clone(),
        kind,
        label: dm::OutputText::new(p.label.clone()),
        current_value: if p.current_value.is_empty() {
            None
        } else {
            Some(p.current_value.clone())
        },
        available_values: p
            .available_values
            .iter()
            .map(|v| dm::SettingValue {
                value: v.value.clone(),
                label: dm::OutputText::new(v.label.clone()),
            })
            .collect(),
        mutable: p.mutable,
    })
}

pub fn operation_to_proto(value: dm::Operation) -> pb::Operation {
    match value {
        dm::Operation::CreateSession => pb::Operation::CreateTask,
        dm::Operation::StartTurn => pb::Operation::StartTurn,
        dm::Operation::QueueNextTurn => pb::Operation::QueueSet,
        dm::Operation::SteerTurn => pb::Operation::Steer,
        dm::Operation::InterruptTurn => pb::Operation::Interrupt,
        dm::Operation::AnswerQuestion => pb::Operation::AnswerQuestion,
        dm::Operation::SubmitApproval => pb::Operation::AnswerApproval,
        dm::Operation::UpdateSettings => pb::Operation::UpdateSettings,
        dm::Operation::RenameSession => pb::Operation::RenameTask,
        dm::Operation::ArchiveSession => pb::Operation::ArchiveTask,
        dm::Operation::UnarchiveSession => pb::Operation::UnarchiveTask,
        dm::Operation::ForkSession => pb::Operation::ForkTask,
        dm::Operation::StopBackgroundCommand => pb::Operation::StopBackgroundCommand,
        dm::Operation::StopAllBackgroundCommands => pb::Operation::StopAllBackgroundCommands,
    }
}

pub fn capability_set_to_proto(caps: &dm::CapabilitySet) -> pb::CapabilitySnapshot {
    pb::CapabilitySnapshot {
        control_mode: control_mode_to_proto(caps.control_mode) as i32,
        compatibility_state: compatibility_state_to_proto(caps.compatibility_state) as i32,
        codex_version: caps.codex_version.clone().unwrap_or_default(),
        supported_operations: caps
            .supported_operations
            .iter()
            .flat_map(|op| match op {
                // 领域层把单条下一轮队列建模为一个能力；浏览器协议把设置、
                // 替换和取消建模为三个独立操作。capability 必须完整展开，
                // 否则前端在已有队列时会错误隐藏 replace/cancel。
                dm::Operation::QueueNextTurn => vec![
                    pb::Operation::QueueSet as i32,
                    pb::Operation::QueueReplace as i32,
                    pb::Operation::QueueCancel as i32,
                ],
                other => vec![operation_to_proto(*other) as i32],
            })
            .collect(),
        settings: caps.settings.iter().map(setting_option_to_proto).collect(),
        transfer_limits: Some(transfer_limits_to_proto(&caps.transfer_limits)),
    }
}

pub fn capability_set_from_proto(p: &pb::CapabilitySnapshot) -> dm::CapabilitySet {
    dm::CapabilitySet {
        control_mode: p
            .control_mode
            .try_into()
            .map(control_mode_from_proto)
            .unwrap_or(dm::ControlMode::Unavailable),
        compatibility_state: p
            .compatibility_state
            .try_into()
            .map(compatibility_state_from_proto)
            .unwrap_or(dm::CompatibilityState::Degraded),
        codex_version: if p.codex_version.is_empty() {
            None
        } else {
            Some(p.codex_version.clone())
        },
        supported_operations: Vec::new(),
        settings: p
            .settings
            .iter()
            .filter_map(setting_option_from_proto)
            .collect(),
        transfer_limits: p
            .transfer_limits
            .as_ref()
            .map(transfer_limits_from_proto)
            .unwrap_or_default(),
    }
}

pub fn question_to_proto(q: &dm::PendingQuestion) -> pb::PendingAttentionQuestion {
    pb::PendingAttentionQuestion {
        question_id: q.question_id.clone(),
        title: q.title.as_str().to_owned(),
        description: q.description.as_str().to_owned(),
        options: q
            .options
            .iter()
            .map(|o| pb::QuestionOption {
                option_id: o.option_id.clone(),
                label: o.label.as_str().to_owned(),
            })
            .collect(),
        allow_multiple: q.allow_multiple,
        allow_free_text: q.allow_free_text,
        turn: opt_turn_id_to_proto(q.turn.as_ref()),
        created_at: opt_timestamp_to_proto(q.created_at),
        valid: q.valid,
    }
}

pub fn question_from_proto(p: &pb::PendingAttentionQuestion) -> dm::PendingQuestion {
    dm::PendingQuestion {
        question_id: p.question_id.clone(),
        title: dm::OutputText::new(p.title.clone()),
        description: dm::OutputText::new(p.description.clone()),
        options: p
            .options
            .iter()
            .map(|o| dm::QuestionOption {
                option_id: o.option_id.clone(),
                label: dm::OutputText::new(o.label.clone()),
            })
            .collect(),
        allow_multiple: p.allow_multiple,
        allow_free_text: p.allow_free_text,
        turn: opt_turn_id_from_proto(&p.turn),
        created_at: opt_timestamp_from_proto(&p.created_at),
        valid: p.valid,
    }
}

pub fn approval_to_proto(a: &dm::PendingApproval) -> pb::PendingAttentionApproval {
    pb::PendingAttentionApproval {
        approval_id: a.approval_id.clone(),
        risk_description: a.risk_description.as_str().to_owned(),
        requested_action: a.requested_action.as_str().to_owned(),
        decisions: a
            .decisions
            .iter()
            .map(|d| pb::ApprovalDecision {
                decision_id: d.decision_id.clone(),
                label: d.label.as_str().to_owned(),
            })
            .collect(),
        scope: a
            .scope
            .as_ref()
            .map(|s| s.as_str().to_owned())
            .unwrap_or_default(),
        turn: opt_turn_id_to_proto(a.turn.as_ref()),
        created_at: opt_timestamp_to_proto(a.created_at),
        valid: a.valid,
    }
}

pub fn approval_from_proto(p: &pb::PendingAttentionApproval) -> dm::PendingApproval {
    dm::PendingApproval {
        approval_id: p.approval_id.clone(),
        risk_description: dm::OutputText::new(p.risk_description.clone()),
        requested_action: dm::OutputText::new(p.requested_action.clone()),
        decisions: p
            .decisions
            .iter()
            .map(|d| dm::ApprovalDecision {
                decision_id: d.decision_id.clone(),
                label: dm::OutputText::new(d.label.clone()),
            })
            .collect(),
        scope: if p.scope.is_empty() {
            None
        } else {
            Some(dm::OutputText::new(p.scope.clone()))
        },
        turn: opt_turn_id_from_proto(&p.turn),
        created_at: opt_timestamp_from_proto(&p.created_at),
        valid: p.valid,
    }
}

pub fn pending_attention_to_proto(
    attention: &dm::PendingAttention,
) -> pb::pending_attention_added::Attention {
    match attention {
        dm::PendingAttention::Question(q) => {
            pb::pending_attention_added::Attention::Question(question_to_proto(q))
        }
        dm::PendingAttention::Approval(a) => {
            pb::pending_attention_added::Attention::Approval(approval_to_proto(a))
        }
    }
}

// ---------------------------------------------------------------------------
// §9.3 Item / §11 HistoryPage
// ---------------------------------------------------------------------------

/// 领域 Item → proto Item。`Opaque` 无 proto 对应 variant,返回 `None`
/// (§17.3:不透传未消化原生数据)。
pub fn item_to_proto(item: &dm::Item) -> Option<pb::Item> {
    let content = match &item.content {
        dm::ItemContent::UserMessage { text } => pb::item::Content::UserMessage(pb::UserMessage {
            text: text.as_str().to_owned(),
        }),
        dm::ItemContent::AssistantMessage {
            text,
            final_message,
        } => pb::item::Content::AssistantMessage(pb::AssistantMessage {
            text: text.as_str().to_owned(),
            r#final: *final_message,
        }),
        dm::ItemContent::ReasoningSummary { text } => {
            pb::item::Content::ReasoningSummary(pb::ReasoningSummary {
                text: text.as_str().to_owned(),
            })
        }
        dm::ItemContent::Plan { plan } => pb::item::Content::Plan(plan_to_proto(plan)),
        dm::ItemContent::ToolCall {
            tool_call_id,
            name,
            status,
            summary,
            duration_ms,
        } => pb::item::Content::ToolCall(pb::ToolCall {
            tool_call_id: tool_call_id.clone(),
            name: name.clone(),
            status: plan_step_status_to_proto(*status) as i32,
            summary: summary
                .as_ref()
                .map(|s| s.as_str().to_owned())
                .unwrap_or_default(),
            duration_ms: *duration_ms,
        }),
        dm::ItemContent::CommandStatus {
            command_id,
            state,
            duration_ms,
            ..
        } => pb::item::Content::CommandStatus(pb::CommandStatusItem {
            command_id: command_id.clone(),
            state: background_state_to_proto(match state {
                dm::CommandStatusState::Running => dm::BackgroundCommandState::Running,
                dm::CommandStatusState::Completed => dm::BackgroundCommandState::Completed,
                dm::CommandStatusState::Failed => dm::BackgroundCommandState::Failed,
                dm::CommandStatusState::Interrupted | dm::CommandStatusState::Unknown => {
                    dm::BackgroundCommandState::Unknown
                }
            }) as i32,
            started_at: None,
            finished_at: None,
            duration_ms: *duration_ms,
        }),
        dm::ItemContent::FileChange { changes } => {
            pb::item::Content::FileChange(pb::FileChangeSet {
                files: changes
                    .iter()
                    .map(|change| pb::FileChange {
                        path: change.path.clone(),
                        kind: file_change_kind_to_proto(change.change) as i32,
                        new_path: String::new(),
                        inline_diff: Vec::new(),
                        diff_truncated: false,
                    })
                    .collect(),
            })
        }
        dm::ItemContent::SubAgentStatus {
            subagent_turn,
            label,
            phase,
            outcome,
        } => pb::item::Content::SubagentStatus(pb::SubAgentStatus {
            subagent_turn: opt_turn_id_to_proto(subagent_turn.as_ref()),
            parent_turn: None,
            label: label.clone(),
            phase: turn_phase_to_proto(*phase) as i32,
            outcome: outcome
                .map(turn_outcome_to_proto)
                .unwrap_or(pb::LastTurnOutcome::Unspecified) as i32,
        }),
        dm::ItemContent::Question { question_id, title } => {
            pb::item::Content::Question(pb::PendingAttentionQuestion {
                question_id: question_id.clone(),
                title: title.as_str().to_owned(),
                ..Default::default()
            })
        }
        dm::ItemContent::Approval {
            approval_id,
            requested_action,
        } => pb::item::Content::Approval(pb::PendingAttentionApproval {
            approval_id: approval_id.clone(),
            requested_action: requested_action.as_str().to_owned(),
            ..Default::default()
        }),
        dm::ItemContent::TokenUsage {
            input_tokens,
            output_tokens,
            context_used_tokens,
            context_window_tokens,
        } => pb::item::Content::TokenUsage(pb::TokenUsage {
            input_tokens: *input_tokens,
            output_tokens: *output_tokens,
            context_used_tokens: context_used_tokens.unwrap_or(0),
            context_window_tokens: context_window_tokens.unwrap_or(0),
        }),
        dm::ItemContent::Opaque { .. } => return None,
    };
    Some(pb::Item {
        item_id: Some(item_id_to_proto(&item.item_id)),
        turn: opt_turn_id_to_proto(item.turn.as_ref()),
        revision: item.revision,
        created_at: opt_timestamp_to_proto(item.created_at),
        content: Some(content),
    })
}

pub fn item_from_proto(p: &pb::Item) -> dm::Item {
    let content = match p.content.as_ref() {
        Some(pb::item::Content::UserMessage(m)) => dm::ItemContent::UserMessage {
            text: dm::OutputText::new(m.text.clone()),
        },
        Some(pb::item::Content::AssistantMessage(m)) => dm::ItemContent::AssistantMessage {
            text: dm::OutputText::new(m.text.clone()),
            final_message: m.r#final,
        },
        Some(pb::item::Content::ReasoningSummary(m)) => dm::ItemContent::ReasoningSummary {
            text: dm::OutputText::new(m.text.clone()),
        },
        Some(pb::item::Content::Plan(plan)) => dm::ItemContent::Plan {
            plan: plan_from_proto(plan),
        },
        Some(pb::item::Content::ToolCall(t)) => dm::ItemContent::ToolCall {
            tool_call_id: t.tool_call_id.clone(),
            name: t.name.clone(),
            status: t
                .status
                .try_into()
                .map(plan_step_status_from_proto)
                .unwrap_or(dm::PlanStepStatus::Pending),
            summary: if t.summary.is_empty() {
                None
            } else {
                Some(dm::OutputText::new(t.summary.clone()))
            },
            duration_ms: t.duration_ms,
        },
        Some(pb::item::Content::CommandStatus(c)) => dm::ItemContent::CommandStatus {
            command_id: c.command_id.clone(),
            state: dm::CommandStatusState::Unknown,
            display: None,
            exit_code: None,
            duration_ms: c.duration_ms,
        },
        Some(pb::item::Content::FileChange(f)) => dm::ItemContent::FileChange {
            changes: f
                .files
                .iter()
                .map(|change| dm::FileChange {
                    path: change.path.clone(),
                    change: change
                        .kind
                        .try_into()
                        .map(file_change_kind_from_proto)
                        .unwrap_or(dm::FileChangeKind::Unknown),
                })
                .collect(),
        },
        Some(pb::item::Content::SubagentStatus(s)) => dm::ItemContent::SubAgentStatus {
            subagent_turn: opt_turn_id_from_proto(&s.subagent_turn),
            label: s.label.clone(),
            phase: s
                .phase
                .try_into()
                .map(turn_phase_from_proto)
                .unwrap_or(dm::ActiveTurnPhase::Idle),
            outcome: match pb::LastTurnOutcome::try_from(s.outcome) {
                Ok(v) if v != pb::LastTurnOutcome::Unspecified => Some(turn_outcome_from_proto(v)),
                _ => None,
            },
        },
        Some(pb::item::Content::Question(q)) => dm::ItemContent::Question {
            question_id: q.question_id.clone(),
            title: dm::OutputText::new(q.title.clone()),
        },
        Some(pb::item::Content::Approval(a)) => dm::ItemContent::Approval {
            approval_id: a.approval_id.clone(),
            requested_action: dm::OutputText::new(a.requested_action.clone()),
        },
        Some(pb::item::Content::TokenUsage(t)) => dm::ItemContent::TokenUsage {
            input_tokens: t.input_tokens,
            output_tokens: t.output_tokens,
            context_used_tokens: if t.context_used_tokens == 0 {
                None
            } else {
                Some(t.context_used_tokens)
            },
            context_window_tokens: if t.context_window_tokens == 0 {
                None
            } else {
                Some(t.context_window_tokens)
            },
        },
        None => dm::ItemContent::Opaque {
            native_type: "proto-item-without-content".to_string(),
        },
    };
    dm::Item {
        item_id: p
            .item_id
            .as_ref()
            .map(item_id_from_proto)
            .unwrap_or_else(|| dm::ItemId::synthetic("")),
        turn: opt_turn_id_from_proto(&p.turn),
        revision: p.revision,
        created_at: opt_timestamp_from_proto(&p.created_at),
        content,
    }
}

pub fn history_page_to_proto(page: &dm::HistoryPage) -> pb::HistoryPage {
    pb::HistoryPage {
        entries: page
            .entries
            .iter()
            .filter_map(|entry| {
                let item = item_to_proto(&entry.item)?;
                Some(pb::HistoryEntry {
                    turn: Some(turn_id_to_proto(&entry.turn)),
                    item: Some(item),
                })
            })
            .collect(),
        next_cursor: page.next_cursor.clone().unwrap_or_default(),
        has_more: page.has_more,
    }
}

pub fn history_page_from_proto(p: &pb::HistoryPage) -> dm::HistoryPage {
    dm::HistoryPage {
        entries: p
            .entries
            .iter()
            .map(|entry| dm::HistoryEntry {
                turn: entry
                    .turn
                    .as_ref()
                    .map(turn_id_from_proto)
                    .unwrap_or_else(|| dm::TurnId::synthetic("")),
                item: item_from_proto(entry.item.as_ref().expect("history entry carries item")),
            })
            .collect(),
        next_cursor: if p.next_cursor.is_empty() {
            None
        } else {
            Some(p.next_cursor.clone())
        },
        has_more: p.has_more,
    }
}

// ---------------------------------------------------------------------------
// RuntimeSnapshot(§11.2;proto 侧不含 session_key,流上下文携带)
// ---------------------------------------------------------------------------

pub fn runtime_snapshot_to_proto(snapshot: &dm::RuntimeSnapshot) -> pb::RuntimeSnapshot {
    pb::RuntimeSnapshot {
        runtime_revision: snapshot.runtime_revision,
        current_turn: snapshot.current_turn.as_ref().map(current_turn_to_proto),
        plan: Some(plan_to_proto(&snapshot.plan)),
        pending_questions: snapshot
            .pending_questions
            .iter()
            .map(question_to_proto)
            .collect(),
        pending_approvals: snapshot
            .pending_approvals
            .iter()
            .map(approval_to_proto)
            .collect(),
        running_commands: snapshot
            .running_commands
            .iter()
            .map(running_command_to_proto)
            .collect(),
        background_commands: snapshot
            .background_commands
            .iter()
            .map(background_command_to_proto)
            .collect(),
        background_command_count: snapshot.background_command_count,
        queue: Some(queue_status_to_proto(&snapshot.queue)),
        capabilities: Some(capability_set_to_proto(&snapshot.capabilities)),
        recent_output_cursors: snapshot
            .recent_output_cursors
            .iter()
            .map(output_cursor_to_proto)
            .collect(),
    }
}

pub fn runtime_snapshot_from_proto(
    p: &pb::RuntimeSnapshot,
    session_key: dm::SessionKey,
) -> dm::RuntimeSnapshot {
    dm::RuntimeSnapshot {
        session_key,
        runtime_revision: p.runtime_revision,
        current_turn: p.current_turn.as_ref().map(current_turn_from_proto),
        plan: p.plan.as_ref().map(plan_from_proto).unwrap_or_default(),
        pending_questions: p
            .pending_questions
            .iter()
            .map(question_from_proto)
            .collect(),
        pending_approvals: p
            .pending_approvals
            .iter()
            .map(approval_from_proto)
            .collect(),
        running_commands: p
            .running_commands
            .iter()
            .map(running_command_from_proto)
            .collect(),
        background_commands: p
            .background_commands
            .iter()
            .map(background_command_from_proto)
            .collect(),
        background_command_count: p.background_command_count,
        queue: p
            .queue
            .as_ref()
            .map(queue_status_from_proto)
            .unwrap_or_default(),
        capabilities: p
            .capabilities
            .as_ref()
            .map(capability_set_from_proto)
            .unwrap_or_else(|| dm::CapabilitySet::read_only_degraded(None)),
        recent_output_cursors: p
            .recent_output_cursors
            .iter()
            .map(output_cursor_from_proto)
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// §13/§17.4 领域事件 → proto DomainEvent
// ---------------------------------------------------------------------------

/// 领域事件 → proto。`ItemUpsert`/`HistoryPage` 携带 `Opaque` item 时返回
/// `None`(事件在 proto 边界不可表达,跳过;其余事件不受影响)。
pub fn domain_event_to_proto(event: &dm::DomainEvent) -> Option<pb::DomainEvent> {
    use pb::domain_event::Event;
    let event = match event {
        dm::DomainEvent::TurnLifecycle {
            turn,
            phase,
            outcome,
            finished_at,
        } => Event::TurnLifecycle(pb::TurnLifecycle {
            turn: Some(turn_id_to_proto(turn)),
            phase: turn_phase_to_proto(*phase) as i32,
            outcome: outcome
                .map(turn_outcome_to_proto)
                .unwrap_or(pb::LastTurnOutcome::Unspecified) as i32,
            finished_at: opt_timestamp_to_proto(*finished_at),
        }),
        dm::DomainEvent::ItemUpsert { item } => {
            // Opaque item 无 proto 对应:整个事件在 proto 边界跳过。
            let item = item_to_proto(item)?;
            Event::ItemUpsert(pb::ItemUpsert { item: Some(item) })
        }
        dm::DomainEvent::OutputAppend {
            item_id,
            expected_offset,
            bytes,
            channel,
        } => Event::OutputAppend(pb::OutputAppend {
            item_id: Some(item_id_to_proto(item_id)),
            expected_offset: *expected_offset,
            bytes: bytes.as_bytes().to_vec(),
            channel: output_channel_to_proto(*channel) as i32,
        }),
        dm::DomainEvent::OutputReplace {
            item_id,
            revision,
            bytes,
            channel,
        } => Event::OutputReplace(pb::OutputReplace {
            item_id: Some(item_id_to_proto(item_id)),
            revision: *revision,
            content: Some(pb::output_replace::Content::Bytes(
                bytes.as_bytes().to_vec(),
            )),
            channel: output_channel_to_proto(*channel) as i32,
        }),
        dm::DomainEvent::OutputFinal {
            item_id,
            revision,
            byte_length,
            channel,
            ..
        } => Event::OutputFinal(pb::OutputFinal {
            item_id: Some(item_id_to_proto(item_id)),
            revision: *revision,
            byte_length: *byte_length,
            channel: output_channel_to_proto(*channel) as i32,
        }),
        dm::DomainEvent::SessionSummaryChanged { summary } => {
            Event::SessionSummaryChanged(session_summary_to_proto(summary))
        }
        dm::DomainEvent::PendingAttentionAdded { attention } => {
            Event::PendingAttentionAdded(pb::PendingAttentionAdded {
                attention: Some(pending_attention_to_proto(attention)),
            })
        }
        dm::DomainEvent::PendingAttentionRemoved {
            kind,
            native_id,
            turn,
        } => Event::PendingAttentionRemoved(pb::PendingAttentionRemoved {
            kind: attention_kind_to_proto(*kind) as i32,
            native_id: native_id.clone(),
            turn: opt_turn_id_to_proto(turn.as_ref()),
        }),
        dm::DomainEvent::QueueStateChanged { queue } => {
            Event::QueueStateChanged(pb::QueueStateChanged {
                queue: Some(queue_status_to_proto(queue)),
            })
        }
        dm::DomainEvent::BackgroundCommandChanged { command } => {
            Event::BackgroundCommandChanged(pb::BackgroundCommandChanged {
                command: Some(background_command_to_proto(command)),
            })
        }
        dm::DomainEvent::CapabilityChanged { capabilities } => {
            Event::CapabilityChanged(pb::CapabilityChanged {
                capabilities: Some(capability_set_to_proto(capabilities)),
            })
        }
    };
    Some(pb::DomainEvent {
        emitted_at: Some(timestamp_to_proto(Utc::now())),
        event: Some(event),
    })
}

// ---------------------------------------------------------------------------
// §15 命令:proto CommandRequest → domain;回执 → CommandAccepted/Result
// ---------------------------------------------------------------------------

/// proto CommandRequest → 领域 CommandRequest。
///
/// - `request_id` 必须是 UUID(§15.1),否则返回 `INTERNAL_ERROR`。
/// - proto 未提供领域 payload 的操作(Create/Rename/Archive/Unarchive/Fork;
///   capability probe 永不开启,§5)→ `CAPABILITY_UNSUPPORTED`。
/// - 设置更新的 kind 按当前能力快照的 option_id 动态解析(§16.1);未知
///   option → `SETTING_COMBINATION_UNSUPPORTED`。
/// - 队列族操作统一映射 `Operation::QueueNextTurn`(gateway 据此豁免
///   Desktop 能力 gate,§15.3 队列由 Bridge 本地管理)。
pub fn command_request_from_proto(
    p: &pb::CommandRequest,
    caps: &dm::CapabilitySet,
) -> Result<dm::CommandRequest, dm::BridgeError> {
    use pb::command_request::Payload;
    let request_id = Uuid::parse_str(&p.request_id).map_err(|_| {
        dm::BridgeError::new(
            dm::StableErrorCode::InternalError,
            "command request_id is not a UUID",
        )
    })?;
    let operation = pb::Operation::try_from(p.operation).map_err(|_| {
        dm::BridgeError::new(
            dm::StableErrorCode::CapabilityUnsupported,
            "unknown operation",
        )
    })?;
    let session_key = p
        .session_key
        .as_ref()
        .and_then(session_key_from_proto)
        .ok_or_else(|| {
            dm::BridgeError::new(
                dm::StableErrorCode::CapabilityUnsupported,
                "command request has unknown or missing agent kind",
            )
        })?;
    let unsupported = |what: &str| {
        Err(dm::BridgeError::new(
            dm::StableErrorCode::CapabilityUnsupported,
            format!("{what} is not supported by current desktop version"),
        ))
    };
    let payload = match p.payload.as_ref() {
        Some(Payload::StartTurn(start)) => dm::CommandPayload::StartTurn {
            input: dm::OutputText::new(start.prompt.clone()),
        },
        Some(Payload::Steer(steer)) => dm::CommandPayload::Steer {
            input: dm::OutputText::new(steer.prompt.clone()),
        },
        Some(Payload::Interrupt(_)) => dm::CommandPayload::Interrupt,
        Some(Payload::AnswerQuestion(q)) => dm::CommandPayload::AnswerQuestion {
            question_id: q.question_id.clone(),
            option_ids: q.option_ids.clone(),
            free_text: if q.free_text.is_empty() {
                None
            } else {
                Some(dm::OutputText::new(q.free_text.clone()))
            },
        },
        Some(Payload::AnswerApproval(a)) => dm::CommandPayload::SubmitApproval {
            approval_id: a.approval_id.clone(),
            decision_id: a.decision_id.clone(),
        },
        Some(Payload::UpdateSettings(settings)) => {
            let mut values = Vec::with_capacity(settings.updates.len());
            for update in &settings.updates {
                let kind = caps
                    .settings
                    .iter()
                    .find(|option| option.option_id == update.option_id)
                    .map(|option| option.kind)
                    .ok_or_else(|| {
                        dm::BridgeError::new(
                            dm::StableErrorCode::SettingCombinationUnsupported,
                            "unknown setting option",
                        )
                    })?;
                values.push(dm::SettingUpdate {
                    kind,
                    value: update.value.clone(),
                });
            }
            dm::CommandPayload::UpdateSettings { values }
        }
        Some(Payload::QueueSet(queue)) => dm::CommandPayload::QueueNextTurn {
            input: dm::OutputText::new(queue.prompt.clone()),
        },
        Some(Payload::QueueReplace(queue)) => dm::CommandPayload::QueueNextTurn {
            input: dm::OutputText::new(queue.prompt.clone()),
        },
        Some(Payload::QueueCancel(_)) => dm::CommandPayload::CancelQueue,
        Some(Payload::StopBackgroundCommand(stop)) => dm::CommandPayload::StopBackgroundCommand {
            command_id: stop.command_id.clone(),
        },
        Some(Payload::StopAllBackgroundCommands(_)) => {
            dm::CommandPayload::StopAllBackgroundCommands
        }
        Some(Payload::CreateTask(_)) => return unsupported("create task"),
        Some(Payload::RenameTask(_)) => return unsupported("rename task"),
        Some(Payload::ArchiveTask(_)) => return unsupported("archive task"),
        Some(Payload::UnarchiveTask(_)) => return unsupported("unarchive task"),
        Some(Payload::ForkTask(_)) => return unsupported("fork task"),
        None => {
            return Err(dm::BridgeError::new(
                dm::StableErrorCode::InternalError,
                "command request without payload",
            ))
        }
    };
    // 操作归一:队列族(SET/REPLACE/CANCEL)统一 QueueNextTurn。
    let operation = match operation {
        pb::Operation::QueueSet | pb::Operation::QueueReplace | pb::Operation::QueueCancel => {
            dm::Operation::QueueNextTurn
        }
        pb::Operation::StartTurn => dm::Operation::StartTurn,
        pb::Operation::Steer => dm::Operation::SteerTurn,
        pb::Operation::Interrupt => dm::Operation::InterruptTurn,
        pb::Operation::AnswerQuestion => dm::Operation::AnswerQuestion,
        pb::Operation::AnswerApproval => dm::Operation::SubmitApproval,
        pb::Operation::UpdateSettings => dm::Operation::UpdateSettings,
        pb::Operation::StopBackgroundCommand => dm::Operation::StopBackgroundCommand,
        pb::Operation::StopAllBackgroundCommands => dm::Operation::StopAllBackgroundCommands,
        _ => {
            return unsupported("operation");
        }
    };
    Ok(dm::CommandRequest {
        request_id,
        operation,
        session_key,
        expected_turn_id: opt_turn_id_from_proto(&p.expected_turn_id),
        expected_runtime_revision: p.expected_runtime_revision,
        payload_digest: None,
        payload,
    })
}

/// 领域回执状态 → (proto 回执状态, 稳定错误码)。
pub fn receipt_state_to_status(
    state: &dm::ReceiptState,
) -> (pb::CommandReceiptStatus, Option<pb::StableErrorCode>) {
    match state {
        dm::ReceiptState::DispatchedToCodex => {
            (pb::CommandReceiptStatus::ReceiptDispatchedToCodex, None)
        }
        dm::ReceiptState::Completed => (pb::CommandReceiptStatus::ReceiptCompleted, None),
        dm::ReceiptState::Rejected { code, .. } => (
            pb::CommandReceiptStatus::ReceiptRejected,
            Some(stable_error_to_proto(*code)),
        ),
        dm::ReceiptState::OutcomeUnknown => (pb::CommandReceiptStatus::ReceiptOutcomeUnknown, None),
    }
}

/// 本地既有回执(重放)→ proto 回执状态(§15.2 全集)。
pub fn stored_state_to_status(
    state: crate::commands::StoredReceiptState,
) -> pb::CommandReceiptStatus {
    use crate::commands::StoredReceiptState as S;
    match state {
        S::Received => pb::CommandReceiptStatus::ReceiptReceived,
        S::AcceptedByBridge => pb::CommandReceiptStatus::ReceiptAcceptedByBridge,
        S::DispatchedToCodex => pb::CommandReceiptStatus::ReceiptDispatchedToCodex,
        S::Completed => pb::CommandReceiptStatus::ReceiptCompleted,
        S::Rejected => pb::CommandReceiptStatus::ReceiptRejected,
        S::OutcomeUnknown => pb::CommandReceiptStatus::ReceiptOutcomeUnknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::query_request;
    use pb::query_response;

    fn sample_session_key() -> dm::SessionKey {
        dm::SessionKey::codex("device-1", "native-1")
    }

    fn sample_summary() -> dm::SessionSummary {
        dm::SessionSummary {
            session_key: sample_session_key(),
            title: Some(dm::OutputText::new("fixture-title")),
            agent_kind: dm::AgentKind::CodexDesktop,
            project_display_name: Some("fixture-project".to_string()),
            current_branch: Some("fixture-branch".to_string()),
            updated_at: Some(Utc::now()),
            device_connection: dm::DeviceConnection::Online,
            device_last_seen_at: Some(Utc::now()),
            degraded_reason: None,
            control_mode: dm::ControlMode::FullControl,
            compatibility_state: dm::CompatibilityState::Verified,
            active_turn_phase: dm::ActiveTurnPhase::Running,
            pending_attention_count: 1,
            pending_attention_kinds: vec![dm::PendingAttentionKind::UserQuestion],
            queue_state: dm::QueueState::Queued,
            last_turn_outcome: dm::LastTurnOutcome::Completed,
            pinned: true,
            muted: false,
            archived: false,
        }
    }

    #[test]
    fn session_summary_roundtrip() {
        let summary = sample_summary();
        let proto = session_summary_to_proto(&summary);
        assert_eq!(proto.agent_kind, pb::AgentKind::CodexDesktop as i32);
        assert_eq!(proto.title, "fixture-title");
        assert_eq!(proto.pending_attention_kinds.len(), 1);
        assert_eq!(proto.queue_state, pb::QueueState::Queued as i32);
        let back = session_summary_from_proto(&proto).unwrap();
        assert_eq!(back.session_key, summary.session_key);
        assert_eq!(back.title, summary.title);
        assert_eq!(back.control_mode, summary.control_mode);
        assert_eq!(back.queue_state, summary.queue_state);
        assert_eq!(back.pinned, true);
    }

    /// ZC-02:ZCode 摘要 kind 往返保真;未知 kind 不默认转换为 Codex。
    #[test]
    fn session_summary_kind_roundtrip_and_unknown_rejected() {
        let mut summary = sample_summary();
        summary.session_key = dm::SessionKey::zcode("device-1", "native-1");
        summary.agent_kind = dm::AgentKind::ZcodeDesktop;
        let proto = session_summary_to_proto(&summary);
        assert_eq!(proto.agent_kind, pb::AgentKind::ZcodeDesktop as i32);
        let back = session_summary_from_proto(&proto).unwrap();
        assert_eq!(back.session_key.agent_kind, dm::AgentKind::ZcodeDesktop);

        let mut unknown = proto.clone();
        unknown.session_key.as_mut().unwrap().agent_kind = 99;
        assert!(session_summary_from_proto(&unknown).is_none());
        let mut unspecified = proto.clone();
        unspecified.session_key.as_mut().unwrap().agent_kind = 0;
        assert!(session_summary_from_proto(&unspecified).is_none());
    }

    #[test]
    fn runtime_snapshot_roundtrip() {
        let mut snapshot = dm::RuntimeSnapshot {
            session_key: sample_session_key(),
            runtime_revision: 42,
            current_turn: Some(dm::CurrentTurn {
                turn: dm::TurnId::native("turn-1"),
                phase: dm::ActiveTurnPhase::Running,
                started_at: Some(Utc::now()),
            }),
            plan: dm::Plan {
                steps: vec![dm::PlanStep {
                    step_id: "s1".to_string(),
                    title: dm::OutputText::new("step"),
                    status: dm::PlanStepStatus::InProgress,
                }],
            },
            pending_questions: vec![dm::PendingQuestion {
                question_id: "q-1".to_string(),
                title: dm::OutputText::new("pick"),
                description: dm::OutputText::new("d"),
                options: vec![dm::QuestionOption {
                    option_id: "opt-a".to_string(),
                    label: dm::OutputText::new("A"),
                }],
                allow_multiple: false,
                allow_free_text: true,
                turn: Some(dm::TurnId::native("turn-1")),
                created_at: Some(Utc::now()),
                valid: true,
            }],
            pending_approvals: vec![],
            running_commands: vec![dm::RunningCommand {
                command_id: "cmd-1".to_string(),
                item_id: Some(dm::ItemId::native("item-1")),
                started_at: None,
            }],
            background_commands: vec![],
            background_command_count: 0,
            queue: dm::QueueStatus::default(),
            capabilities: dm::CapabilitySet::read_only_degraded(Some("0.153.0-alpha.5".into())),
            recent_output_cursors: vec![dm::OutputCursor {
                item_id: dm::ItemId::native("item-1"),
                revision: 3,
                byte_length: 12,
                is_final: false,
                channel: dm::OutputChannel::Combined,
                final_unavailable: false,
            }],
        };
        snapshot.capabilities.supported_operations = vec![dm::Operation::StartTurn];
        let proto = runtime_snapshot_to_proto(&snapshot);
        assert_eq!(proto.runtime_revision, 42);
        assert_eq!(proto.pending_questions.len(), 1);
        assert_eq!(proto.pending_questions[0].options[0].option_id, "opt-a");
        let back = runtime_snapshot_from_proto(&proto, sample_session_key());
        assert_eq!(back.runtime_revision, 42);
        assert_eq!(back.current_turn.as_ref().unwrap().turn.id, "turn-1");
        assert_eq!(back.pending_questions[0].question_id, "q-1");
        assert_eq!(back.recent_output_cursors[0].byte_length, 12);
        assert_eq!(back.plan.steps.len(), 1);
    }

    #[test]
    fn history_page_roundtrip_preserves_synthetic_flags() {
        let page = dm::HistoryPage {
            entries: vec![dm::HistoryEntry {
                turn: dm::TurnId::synthetic("t-syn"),
                item: dm::Item {
                    item_id: dm::ItemId::synthetic("i-syn"),
                    turn: Some(dm::TurnId::native("t-native")),
                    revision: 2,
                    created_at: None,
                    content: dm::ItemContent::AssistantMessage {
                        text: dm::OutputText::new("fixture-reply"),
                        final_message: true,
                    },
                },
            }],
            next_cursor: Some("cursor-1".to_string()),
            has_more: true,
        };
        let proto = history_page_to_proto(&page);
        let turn = proto.entries[0].turn.as_ref().unwrap();
        assert!(turn.synthetic, "synthetic 标记保留(§9.2)");
        let item = proto.entries[0].item.as_ref().unwrap();
        assert!(item.item_id.as_ref().unwrap().synthetic);
        let back = history_page_from_proto(&proto);
        assert!(back.entries[0].turn.synthetic);
        assert!(back.entries[0].item.item_id.synthetic);
        assert_eq!(back.next_cursor.as_deref(), Some("cursor-1"));
    }

    #[test]
    fn domain_events_map_and_opaque_item_drops() {
        let append = dm::DomainEvent::OutputAppend {
            item_id: dm::ItemId::native("item-1"),
            expected_offset: 5,
            bytes: dm::OutputBytes::from_text("fixture-chunk"),
            channel: dm::OutputChannel::Combined,
        };
        let proto = domain_event_to_proto(&append).expect("append maps");
        match proto.event {
            Some(pb::domain_event::Event::OutputAppend(a)) => {
                assert_eq!(a.expected_offset, 5);
                assert_eq!(a.bytes, b"fixture-chunk");
            }
            other => panic!("unexpected: {other:?}"),
        }

        let lifecycle = dm::DomainEvent::TurnLifecycle {
            turn: dm::TurnId::native("turn-1"),
            phase: dm::ActiveTurnPhase::Idle,
            outcome: Some(dm::LastTurnOutcome::Failed),
            finished_at: Some(Utc::now()),
        };
        assert!(matches!(
            domain_event_to_proto(&lifecycle).unwrap().event,
            Some(pb::domain_event::Event::TurnLifecycle(_))
        ));

        let attention = dm::DomainEvent::PendingAttentionAdded {
            attention: dm::PendingAttention::Approval(dm::PendingApproval {
                approval_id: "ap-1".to_string(),
                risk_description: dm::OutputText::new("risk"),
                requested_action: dm::OutputText::new("act"),
                decisions: vec![],
                scope: None,
                turn: None,
                created_at: None,
                valid: true,
            }),
        };
        match domain_event_to_proto(&attention).unwrap().event {
            Some(pb::domain_event::Event::PendingAttentionAdded(a)) => match a.attention {
                Some(pb::pending_attention_added::Attention::Approval(_)) => {}
                other => panic!("unexpected: {other:?}"),
            },
            other => panic!("unexpected: {other:?}"),
        }

        let opaque = dm::DomainEvent::ItemUpsert {
            item: dm::Item {
                item_id: dm::ItemId::native("item-x"),
                turn: None,
                revision: 1,
                created_at: None,
                content: dm::ItemContent::Opaque {
                    native_type: "brandNewType".to_string(),
                },
            },
        };
        assert!(
            domain_event_to_proto(&opaque).is_none(),
            "Opaque item 在 proto 边界跳过,不透传未消化数据"
        );
    }

    #[test]
    fn command_request_proto_to_domain() {
        let mut caps = dm::CapabilitySet::read_only_degraded(None);
        caps.settings = vec![dm::SettingOption {
            option_id: "model".to_string(),
            kind: dm::SettingKind::Model,
            label: dm::OutputText::new("model"),
            current_value: None,
            available_values: vec![],
            mutable: true,
        }];
        let request_id = Uuid::new_v4();
        let proto = pb::CommandRequest {
            request_id: request_id.to_string(),
            operation: pb::Operation::StartTurn as i32,
            session_key: Some(session_key_to_proto(&sample_session_key())),
            expected_turn_id: Some(pb::TurnId {
                id: "turn-1".to_string(),
                synthetic: false,
            }),
            expected_runtime_revision: Some(7),
            payload_digest: "deadbeef".to_string(),
            payload: Some(pb::command_request::Payload::StartTurn(
                pb::StartTurnPayload {
                    prompt: "fixture-prompt".to_string(),
                },
            )),
        };
        let domain = command_request_from_proto(&proto, &caps).unwrap();
        assert_eq!(domain.request_id, request_id);
        assert_eq!(domain.operation, dm::Operation::StartTurn);
        assert_eq!(domain.expected_runtime_revision, Some(7));
        match domain.payload {
            dm::CommandPayload::StartTurn { input } => assert_eq!(input.as_str(), "fixture-prompt"),
            other => panic!("unexpected: {other:?}"),
        }

        // 队列族操作归一 QueueNextTurn(gateway 豁免 Desktop 能力 gate)。
        let queue = pb::CommandRequest {
            request_id: request_id.to_string(),
            operation: pb::Operation::QueueCancel as i32,
            session_key: Some(session_key_to_proto(&sample_session_key())),
            payload: Some(pb::command_request::Payload::QueueCancel(
                pb::QueueCancelPayload {},
            )),
            ..Default::default()
        };
        let domain = command_request_from_proto(&queue, &caps).unwrap();
        assert_eq!(domain.operation, dm::Operation::QueueNextTurn);
        assert!(matches!(domain.payload, dm::CommandPayload::CancelQueue));

        // 设置:kind 按 option_id 动态解析。
        let settings = pb::CommandRequest {
            request_id: request_id.to_string(),
            operation: pb::Operation::UpdateSettings as i32,
            session_key: Some(session_key_to_proto(&sample_session_key())),
            payload: Some(pb::command_request::Payload::UpdateSettings(
                pb::UpdateSettingsPayload {
                    updates: vec![pb::SettingUpdate {
                        option_id: "model".to_string(),
                        value: "gpt-5.3-fixture".to_string(),
                    }],
                },
            )),
            ..Default::default()
        };
        let domain = command_request_from_proto(&settings, &caps).unwrap();
        match domain.payload {
            dm::CommandPayload::UpdateSettings { values } => {
                assert_eq!(values[0].kind, dm::SettingKind::Model)
            }
            other => panic!("unexpected: {other:?}"),
        }

        // 不支持的操作(caps 永不开启)→ CAPABILITY_UNSUPPORTED。
        let create = pb::CommandRequest {
            request_id: request_id.to_string(),
            operation: pb::Operation::CreateTask as i32,
            session_key: Some(session_key_to_proto(&sample_session_key())),
            payload: Some(pb::command_request::Payload::CreateTask(
                pb::CreateTaskPayload::default(),
            )),
            ..Default::default()
        };
        assert_eq!(
            command_request_from_proto(&create, &caps).unwrap_err().code,
            dm::StableErrorCode::CapabilityUnsupported
        );
    }

    #[test]
    fn receipt_status_mapping_covers_all_states() {
        let (status, code) = receipt_state_to_status(&dm::ReceiptState::DispatchedToCodex);
        assert_eq!(status, pb::CommandReceiptStatus::ReceiptDispatchedToCodex);
        assert!(code.is_none());
        let (status, code) = receipt_state_to_status(&dm::ReceiptState::Rejected {
            code: dm::StableErrorCode::StaleTurn,
            message: String::new(),
        });
        assert_eq!(status, pb::CommandReceiptStatus::ReceiptRejected);
        assert_eq!(code, Some(pb::StableErrorCode::StaleTurn));
        assert_eq!(
            stored_state_to_status(crate::commands::StoredReceiptState::AcceptedByBridge),
            pb::CommandReceiptStatus::ReceiptAcceptedByBridge
        );
        assert_eq!(
            stored_state_to_status(crate::commands::StoredReceiptState::Received),
            pb::CommandReceiptStatus::ReceiptReceived
        );
    }

    #[test]
    fn capability_and_query_roundtrips() {
        let mut caps =
            dm::CapabilitySet::read_only_degraded(Some("codex-cli 0.153.0-alpha.5".into()));
        caps.supported_operations = vec![dm::Operation::StartTurn, dm::Operation::QueueNextTurn];
        caps.settings = vec![dm::SettingOption {
            option_id: "effort".to_string(),
            kind: dm::SettingKind::ReasoningEffort,
            label: dm::OutputText::new("effort"),
            current_value: Some("medium".to_string()),
            available_values: vec![dm::SettingValue {
                value: "medium".to_string(),
                label: dm::OutputText::new("medium"),
            }],
            mutable: true,
        }];
        let proto = capability_set_to_proto(&caps);
        assert_eq!(
            proto.supported_operations,
            vec![
                pb::Operation::StartTurn as i32,
                pb::Operation::QueueSet as i32,
                pb::Operation::QueueReplace as i32,
                pb::Operation::QueueCancel as i32,
            ]
        );
        let back = capability_set_from_proto(&proto);
        assert_eq!(back.control_mode, caps.control_mode);
        assert_eq!(back.codex_version, caps.codex_version);
        assert_eq!(back.settings.len(), 1);
        assert_eq!(back.settings[0].kind, dm::SettingKind::ReasoningEffort);

        // QueryResponse result oneof 覆盖 runtime_snapshot 形态。
        let snapshot_proto = runtime_snapshot_to_proto(&dm::RuntimeSnapshot {
            session_key: sample_session_key(),
            runtime_revision: 3,
            current_turn: None,
            plan: dm::Plan::default(),
            pending_questions: vec![],
            pending_approvals: vec![],
            running_commands: vec![],
            background_commands: vec![],
            background_command_count: 0,
            queue: dm::QueueStatus::default(),
            capabilities: caps.clone(),
            recent_output_cursors: vec![],
        });
        let response = pb::QueryResponse {
            request_id: "req-1".to_string(),
            result: Some(query_response::Result::RuntimeSnapshot(snapshot_proto)),
            error_code: 0,
        };
        assert!(matches!(
            response.result,
            Some(query_response::Result::RuntimeSnapshot(_))
        ));
        // QueryRequest history_page 形态可构造。
        let request = pb::QueryRequest {
            session_key: Some(session_key_to_proto(&sample_session_key())),
            query: Some(query_request::Query::HistoryPage(pb::HistoryPageQuery {
                cursor: String::new(),
                page_size: 50,
            })),
        };
        assert!(matches!(
            request.query,
            Some(query_request::Query::HistoryPage(_))
        ));
    }
}
