//! local_store 数据模型(§19/§15)。
//!
//! 字段与 §25.3 日志白名单对齐:模型 Debug 输出只含 ID、状态、时间、摘要,
//! `NextTurnEntry.prompt` 是唯一正文字段(§15.3:正文只存 Bridge),其 Debug
//! 做脱敏,避免误入日志。

use std::path::PathBuf;

/// 会话原生复合键 (device_id, agent_kind, native_session_id)(§9.1)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKeyRef {
    pub device_id: String,
    /// proto AgentKind 数值(首版恒为 CODEX_DESKTOP)。
    pub agent_kind: i64,
    pub native_session_id: String,
}

/// 非敏感绑定状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindingStatus {
    #[default]
    Unbound,
    Pairing,
    Paired,
}

impl BindingStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BindingStatus::Unbound => "UNBOUND",
            BindingStatus::Pairing => "PAIRING",
            BindingStatus::Paired => "PAIRED",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "PAIRING" => BindingStatus::Pairing,
            "PAIRED" => BindingStatus::Paired,
            _ => BindingStatus::Unbound,
        }
    }
}

/// 绑定行(不含凭据;凭据只在 Keychain)。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Binding {
    pub relay_url: String,
    pub device_id: String,
    pub status: BindingStatus,
}

/// 已授权工作区根目录(canonical path)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedWorkspace {
    pub root_path: PathBuf,
    pub display_name: String,
    pub authorized_at: String,
}

/// request 回执(§15.1/§15.2):只有元数据与摘要,无正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestReceipt {
    pub request_id: String,
    pub session: SessionKeyRef,
    /// operation 稳定名(如 "start_turn")。
    pub operation: String,
    /// RECEIVED/ACCEPTED_BY_BRIDGE/DISPATCHED_TO_CODEX/COMPLETED/REJECTED/OUTCOME_UNKNOWN。
    pub status: String,
    /// payload 规范化摘要(去重用途,§15.1)。
    pub payload_digest: String,
    pub created_at: String,
    pub updated_at: String,
}

/// 回执写入结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptUpsertOutcome {
    /// 新 request_id,已插入。
    Inserted,
    /// 同 ID 同 digest 重试,返回既有回执语义(不重复执行,§15.2)。
    Existing,
}

/// 下一轮队列状态(§15.3)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueStatus {
    #[default]
    Queued,
    Paused,
    Empty,
}

impl QueueStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            QueueStatus::Queued => "QUEUED",
            QueueStatus::Paused => "PAUSED",
            QueueStatus::Empty => "EMPTY",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "PAUSED" => QueueStatus::Paused,
            "EMPTY" => QueueStatus::Empty,
            _ => QueueStatus::Queued,
        }
    }
}

/// 队列项。`prompt` 是正文,只存 Bridge SQLite(§15.3)。
pub struct NextTurnEntry {
    pub session: SessionKeyRef,
    pub prompt: String,
    pub after_turn_id: String,
    pub runtime_revision: i64,
    pub status: QueueStatus,
    pub created_at: String,
    pub updated_at: String,
}

impl std::fmt::Debug for NextTurnEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 正文绝不进日志/调试输出:只输出长度。
        f.debug_struct("NextTurnEntry")
            .field("session", &self.session)
            .field("prompt_len", &self.prompt.len())
            .field("after_turn_id", &self.after_turn_id)
            .field("runtime_revision", &self.runtime_revision)
            .field("status", &self.status.as_str())
            .field("created_at", &self.created_at)
            .finish()
    }
}

/// capability probe 缓存(JSON:仅 schema/能力,无会话内容)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCacheEntry {
    pub probe_json: String,
    pub schema_version: i64,
    pub updated_at: String,
}
