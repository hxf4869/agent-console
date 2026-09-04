//! 写命令管线与单条下一轮队列(权威规格 §15/§16/§23.2/§26.4)。
//!
//! - [`CommandGateway`]:写命令唯一入口。capability gate → request_id 去重
//!   (同 ID 同 payload 重放既有回执,不重复执行;同 ID 不同 digest →
//!   `DUPLICATE_REQUEST_MISMATCH`)→ expected turn/revision 校验(`STALE_TURN`)
//!   → 持久化 `ACCEPTED_BY_BRIDGE` → 交给 adapter/Desktop → 回执透传落库。
//!   摘要 = payload 规范化 JSON(键排序)的 SHA-256,仅去重用(§15.1)。
//! - [`QueueManager`]:单条下一轮队列(§15.3)。正文只存 Bridge SQLite;
//!   每 session 最多一条;turn 正常完成且无 attention 时自动发送,
//!   failed/interrupted/断线/Desktop 抢先/ownership 变化改 `PAUSED`;
//!   `PAUSED` 只能由用户显式 resume 重新绑定,绝不自动改绑。
//! - attention/settings/background:问题/审批只按原生 ID(§16.3/§16.4);
//!   设置组合不支持 → `SETTING_COMBINATION_UNSUPPORTED`,不静默降级(§16.2);
//!   后台命令只暴露原生能力,全局停止带 warning code(§23.2)。
//! - [`PowerCoordinator`]/[`UploadLifecycle`]:turn 活动到电源断言与上传
//!   清理的接线点(§19/§22.5)。
//!
//! §26.4:连接中断无法证明执行 → `OUTCOME_UNKNOWN`,绝不对非幂等 Desktop
//! 命令自动换新 ID 重试;shutdown 停止接受新命令并给未完成命令明确终态。

pub mod attention;
pub mod background;
pub mod gateway;
pub mod power;
pub mod queue;
pub mod settings;
pub mod upload_lifecycle;

pub use gateway::{CommandExecutor, CommandGateway, StoredReceiptState, Submission};
pub use power::PowerCoordinator;
pub use queue::QueueManager;
pub use settings::{SettingEffective, SettingsSubmission};
pub use upload_lifecycle::{UploadCleaner, UploadLifecycle};

use crate::domain::{CommandPayload, Operation, ReceiptState, StableErrorCode};
use crate::local_store::StoreError;

/// payload 规范化摘要(§15.1):serde_json 值形态(对象键排序)再序列化后
/// SHA-256 hex。仅用于同 request_id 去重比较,不用于内容真实性证明。
pub fn canonical_payload_digest(payload: &CommandPayload) -> String {
    let value = serde_json::to_value(payload).unwrap_or(serde_json::Value::Null);
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    crate::local_store::payload_digest(&bytes)
}

/// Operation 的稳定名(request_receipts.operation 列;serde snake_case)。
pub fn operation_name(op: Operation) -> String {
    serde_json::to_value(op)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

/// [`ReceiptState`] → 回执存储状态串(§15.2 全集,models.rs 契约)。
pub fn receipt_status_str(state: &ReceiptState) -> &'static str {
    match state {
        ReceiptState::DispatchedToCodex => "DISPATCHED_TO_CODEX",
        ReceiptState::Completed => "COMPLETED",
        ReceiptState::Rejected { .. } => "REJECTED",
        ReceiptState::OutcomeUnknown => "OUTCOME_UNKNOWN",
    }
}

pub(crate) fn store_error_code(err: &StoreError) -> StableErrorCode {
    match err {
        StoreError::DuplicateRequestMismatch { .. } => StableErrorCode::DuplicateRequestMismatch,
        StoreError::QueueAlreadyExists => StableErrorCode::QueueAlreadyExists,
        _ => StableErrorCode::InternalError,
    }
}
