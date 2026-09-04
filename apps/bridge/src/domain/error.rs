//! 必需稳定错误码(§27.6)。与 HTTP JSON 错误的 `code` 及 Protobuf
//! `ProtocolError` 使用同一稳定集合;前端不得依赖自由文本判断流程。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StableErrorCode {
    AuthRequired,
    AuthExpired,
    CsrfInvalid,
    WsTicketInvalid,
    WsTicketExpired,
    WsTicketConsumed,
    DeviceOffline,
    DeviceRevoked,
    CodexUnavailable,
    CodexVersionUnverified,
    ControlReadOnly,
    CapabilityUnsupported,
    SessionNotFound,
    StaleTurn,
    DuplicateRequestMismatch,
    OutcomeUnknown,
    ResyncRequired,
    QueueAlreadyExists,
    QueuePaused,
    QuestionExpired,
    ApprovalExpired,
    SettingCombinationUnsupported,
    FileHandleInvalid,
    FileOutsideScope,
    FileChanged,
    FileTypeNotPreviewable,
    TransferExpired,
    TransferTooLarge,
    TransferRangeInvalid,
    DiffTooLarge,
    RateLimited,
    InternalError,
}

impl StableErrorCode {
    /// 稳定字符串形式(与 §27.6 完全一致的全大写)。
    pub fn as_str(self) -> &'static str {
        match self {
            StableErrorCode::AuthRequired => "AUTH_REQUIRED",
            StableErrorCode::AuthExpired => "AUTH_EXPIRED",
            StableErrorCode::CsrfInvalid => "CSRF_INVALID",
            StableErrorCode::WsTicketInvalid => "WS_TICKET_INVALID",
            StableErrorCode::WsTicketExpired => "WS_TICKET_EXPIRED",
            StableErrorCode::WsTicketConsumed => "WS_TICKET_CONSUMED",
            StableErrorCode::DeviceOffline => "DEVICE_OFFLINE",
            StableErrorCode::DeviceRevoked => "DEVICE_REVOKED",
            StableErrorCode::CodexUnavailable => "CODEX_UNAVAILABLE",
            StableErrorCode::CodexVersionUnverified => "CODEX_VERSION_UNVERIFIED",
            StableErrorCode::ControlReadOnly => "CONTROL_READ_ONLY",
            StableErrorCode::CapabilityUnsupported => "CAPABILITY_UNSUPPORTED",
            StableErrorCode::SessionNotFound => "SESSION_NOT_FOUND",
            StableErrorCode::StaleTurn => "STALE_TURN",
            StableErrorCode::DuplicateRequestMismatch => "DUPLICATE_REQUEST_MISMATCH",
            StableErrorCode::OutcomeUnknown => "OUTCOME_UNKNOWN",
            StableErrorCode::ResyncRequired => "RESYNC_REQUIRED",
            StableErrorCode::QueueAlreadyExists => "QUEUE_ALREADY_EXISTS",
            StableErrorCode::QueuePaused => "QUEUE_PAUSED",
            StableErrorCode::QuestionExpired => "QUESTION_EXPIRED",
            StableErrorCode::ApprovalExpired => "APPROVAL_EXPIRED",
            StableErrorCode::SettingCombinationUnsupported => "SETTING_COMBINATION_UNSUPPORTED",
            StableErrorCode::FileHandleInvalid => "FILE_HANDLE_INVALID",
            StableErrorCode::FileOutsideScope => "FILE_OUTSIDE_SCOPE",
            StableErrorCode::FileChanged => "FILE_CHANGED",
            StableErrorCode::FileTypeNotPreviewable => "FILE_TYPE_NOT_PREVIEWABLE",
            StableErrorCode::TransferExpired => "TRANSFER_EXPIRED",
            StableErrorCode::TransferTooLarge => "TRANSFER_TOO_LARGE",
            StableErrorCode::TransferRangeInvalid => "TRANSFER_RANGE_INVALID",
            StableErrorCode::DiffTooLarge => "DIFF_TOO_LARGE",
            StableErrorCode::RateLimited => "RATE_LIMITED",
            StableErrorCode::InternalError => "INTERNAL_ERROR",
        }
    }
}

impl std::fmt::Display for StableErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 携带稳定错误码的领域错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct BridgeError {
    pub code: StableErrorCode,
    pub message: String,
}

impl BridgeError {
    pub fn new(code: StableErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
