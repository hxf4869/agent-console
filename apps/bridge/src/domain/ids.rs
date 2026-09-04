//! 统一领域标识(§9)。

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Agent 种类;首版只有 CODEX_DESKTOP(§9.1)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentKind {
    CodexDesktop,
}

/// 复合原生会话键(§9.1):`(device_id, agent_kind, native_session_id)`。
/// `relay_session_uuid` 是 Relay 内部 UUID,仅用于 URL 与日志关联,
/// 不得覆盖原生键。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionKey {
    pub device_id: String,
    pub agent_kind: AgentKind,
    pub native_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_session_uuid: Option<Uuid>,
}

impl SessionKey {
    /// 构造 Codex Desktop 会话键(首版唯一 agent kind)。
    pub fn codex(device_id: impl Into<String>, native_session_id: impl Into<String>) -> Self {
        Self {
            device_id: device_id.into(),
            agent_kind: AgentKind::CodexDesktop,
            native_session_id: native_session_id.into(),
            relay_session_uuid: None,
        }
    }
}

impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{:?}",
            self.device_id,
            self.agent_kind.kind_name(),
            self.native_session_id
        )
    }
}

impl AgentKind {
    /// 稳定的种类名(URL/日志使用)。
    pub fn kind_name(self) -> &'static str {
        match self {
            AgentKind::CodexDesktop => "codex-desktop",
        }
    }
}

/// 稳定 turn ID(§9.2):优先 Codex 原生 ID;无法取得时使用 Adapter scoped
/// 稳定 ID 并置 `synthetic=true`。禁止用消息数组下标充当稳定 ID。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TurnId {
    pub id: String,
    pub synthetic: bool,
}

impl TurnId {
    pub fn native(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            synthetic: false,
        }
    }

    pub fn synthetic(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            synthetic: true,
        }
    }
}

impl std::fmt::Display for TurnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.synthetic {
            write!(f, "{}~synthetic", self.id)
        } else {
            write!(f, "{}", self.id)
        }
    }
}

/// 稳定 item ID(§9.2),语义同 [`TurnId`]。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ItemId {
    pub id: String,
    pub synthetic: bool,
}

impl ItemId {
    pub fn native(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            synthetic: false,
        }
    }

    pub fn synthetic(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            synthetic: true,
        }
    }
}

impl std::fmt::Display for ItemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.synthetic {
            write!(f, "{}~synthetic", self.id)
        } else {
            write!(f, "{}", self.id)
        }
    }
}
