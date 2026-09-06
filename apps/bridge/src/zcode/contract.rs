//! helper ↔ Bridge 本机合同(04 §8.5:项目自有本机合同,非公网 API)。
//!
//! - 一条 helper 连接对应一个 invoke;请求/应答各为一行 JSON + `\n`。
//! - `invoke_id` 由 helper 生成(UUID v4):同一命令两次执行 = 两个请求。
//! - `tool_use_id` 仅在原生输入实际携带时转发;不得假定存在。
//! - 原始 `tool_input` 仅本机传输与授权展示,不进日志;体积超限整单拒绝,
//!   不截断后批准未知动作。

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 合同版本(当前唯一版本)。
pub const CONTRACT_VERSION: u32 = 1;
/// ZCode 官方 Hook 根级默认超时 60000ms;远程等待预算约 45s(04 §8.6)。
pub const DEFAULT_REMOTE_WAIT_MS: u64 = 45_000;
/// 远程等待下限(测试/异常输入保护)。
pub const MIN_REMOTE_WAIT_MS: u64 = 500;
/// 远程等待上限:不得吃满官方 60s Hook 预算,给清理与原生回复留余量。
pub const MAX_REMOTE_WAIT_MS: u64 = DEFAULT_REMOTE_WAIT_MS;
/// helper stdin / socket 单帧上限(256 KiB):超限整单拒绝,不截断批准。
pub const MAX_FRAME_BYTES: usize = 256 * 1024;
/// 状态事件确认等待(ms;远小于决策预算)。
pub const STATUS_WAIT_MS: u64 = 5_000;

/// invoke 事件类型。
pub const EVENT_PERMISSION_REQUEST: &str = "permission_request";
pub const EVENT_ASK_USER: &str = "ask_user";
pub const EVENT_STATUS: &str = "status";
pub const EVENT_CANCEL: &str = "cancel";

/// 应答状态(`HookReply::status`)。
pub const STATUS_ALLOWED: &str = "allowed";
pub const STATUS_DENIED: &str = "denied";
pub const STATUS_ANSWERED: &str = "answered";
pub const STATUS_EXPIRED: &str = "expired";
pub const STATUS_CANCELLED: &str = "cancelled";
pub const STATUS_REJECTED: &str = "rejected";
pub const STATUS_ACCEPTED: &str = "accepted";

/// helper → Bridge 的 invoke 消息(连接后第一行)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookInvoke {
    pub version: u32,
    /// 固定 "zcode"(预留给未来本机 agent 种类)。
    pub agent_kind: String,
    /// helper 生成的 UUID;唯一回复通道标识。
    pub invoke_id: String,
    /// [`EVENT_PERMISSION_REQUEST`] 等常量之一。
    pub event: String,
    /// 原生 session_id(Hook 输入携带时);MCP 问答侧为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    /// 期望远程等待时长(ms;Bridge 侧收敛到 [MIN, MAX])。
    pub requested_wait_ms: u64,
    /// 原始工具输入(仅 permission_request;内存内授权展示,不进日志)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_input: Option<serde_json::Value>,
    /// MCP 问答请求(仅 ask_user)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask: Option<AskRequest>,
    /// 状态观察事件名(仅 status;官方事件名,如 SessionStart/Stop)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_event: Option<String>,
    /// 状态事件的原始输入(仅 status;最小元数据映射用)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_input: Option<serde_json::Value>,
}

/// MCP 问答请求(04 §8.8:仅 question、有限选项、是否允许补充文本、
/// 调用级标识)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AskRequest {
    pub question: String,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default = "default_true")]
    pub allow_free_text: bool,
    /// MCP JSON-RPC 调用 id(调用级标识;取消传播用)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
}

fn default_true() -> bool {
    true
}

impl AskRequest {
    /// 问答长度/选项数按手机展示收敛(04 §8.8);超限整单拒绝。
    pub const MAX_QUESTION_CHARS: usize = 2_000;
    pub const MAX_OPTION_CHARS: usize = 200;
    pub const MAX_OPTIONS: usize = 8;

    pub fn validate(&self) -> Result<(), String> {
        if self.question.trim().is_empty() {
            return Err("question is empty".to_string());
        }
        if self.question.chars().count() > Self::MAX_QUESTION_CHARS {
            return Err(format!(
                "question exceeds {} chars",
                Self::MAX_QUESTION_CHARS
            ));
        }
        if self.options.len() > Self::MAX_OPTIONS {
            return Err(format!("more than {} options", Self::MAX_OPTIONS));
        }
        for option in &self.options {
            if option.chars().count() > Self::MAX_OPTION_CHARS {
                return Err(format!(
                    "option exceeds {} chars",
                    Self::MAX_OPTION_CHARS
                ));
            }
        }
        Ok(())
    }
}

/// Bridge → helper 的应答(一行 JSON)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookReply {
    /// [`STATUS_ALLOWED`] 等常量之一。
    pub status: String,
    /// deny 时面向 ZCode 原生确认的消息。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// answered:选中的选项原文(选项无独立 id 时以原文为回传值)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub option: Option<String>,
    /// answered:补充文本(allow_free_text 时)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl HookReply {
    pub fn allowed() -> Self {
        Self {
            status: STATUS_ALLOWED.to_string(),
            message: None,
            option: None,
            text: None,
        }
    }

    pub fn denied(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_DENIED.to_string(),
            message: Some(message.into()),
            option: None,
            text: None,
        }
    }

    pub fn answered(option: Option<String>, text: Option<String>) -> Self {
        Self {
            status: STATUS_ANSWERED.to_string(),
            message: None,
            option,
            text,
        }
    }

    pub fn expired() -> Self {
        Self {
            status: STATUS_EXPIRED.to_string(),
            message: None,
            option: None,
            text: None,
        }
    }

    pub fn cancelled() -> Self {
        Self {
            status: STATUS_CANCELLED.to_string(),
            message: None,
            option: None,
            text: None,
        }
    }

    pub fn rejected(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_REJECTED.to_string(),
            message: Some(message.into()),
            option: None,
            text: None,
        }
    }

    pub fn accepted() -> Self {
        Self {
            status: STATUS_ACCEPTED.to_string(),
            message: None,
            option: None,
            text: None,
        }
    }
}

/// 收敛等待时长到合同范围。
pub fn clamp_wait_ms(requested: u64) -> Duration {
    Duration::from_millis(requested.clamp(MIN_REMOTE_WAIT_MS, MAX_REMOTE_WAIT_MS))
}

// ---------------------------------------------------------------------------
// 原生 Hook stdin 输入解析(官方文档字段;snake_case 为主,容忍 camelCase)
// ---------------------------------------------------------------------------

/// ZCode Hook stdin 输入(官方:单行 JSON + 换行)。
#[derive(Debug, Clone, PartialEq)]
pub struct NativeHookInput {
    /// 官方 `hook_event_name`(如 PermissionRequest/SessionStart/Stop)。
    pub hook_event_name: String,
    /// 原生会话 ID(Hook 输入携带时;不得猜测)。
    pub session_id: Option<String>,
    pub tool_name: Option<String>,
    pub tool_input: Option<serde_json::Value>,
    /// 仅当原生输入实际携带时存在(04 §8.5)。
    pub tool_use_id: Option<String>,
    /// 工作区 cwd(仅状态元数据;不进日志)。
    pub cwd: Option<String>,
}

/// 解析错误(不含输入原文,避免把 toolInput 带进错误链路)。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("hook input is empty")]
    Empty,
    #[error("hook input is not valid UTF-8 JSON")]
    NotObject,
    #[error("hook input io failure")]
    Io,
    #[error("hook_event_name is missing")]
    MissingEventName,
    #[error("hook input exceeds {0} bytes")]
    Oversize(usize),
}

fn field(value: &serde_json::Value, snake: &str, camel: &str) -> Option<serde_json::Value> {
    let obj = value.as_object()?;
    obj.get(snake)
        .filter(|v| !v.is_null())
        .or_else(|| obj.get(camel).filter(|v| !v.is_null()))
        .cloned()
}

fn string_field(value: &serde_json::Value, snake: &str, camel: &str) -> Option<String> {
    field(value, snake, camel)
        .and_then(|v| v.as_str().map(str::to_string))
        .filter(|s| !s.is_empty())
}

/// 解析原生 Hook 单行输入。行长度超 [`MAX_FRAME_BYTES`] 时返回
/// [`ParseError::Oversize`](不截断、不解析)。
pub fn parse_native_input(line: &str) -> Result<NativeHookInput, ParseError> {
    if line.len() > MAX_FRAME_BYTES {
        return Err(ParseError::Oversize(line.len()));
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }
    let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|_| ParseError::NotObject)?;
    if !value.is_object() {
        return Err(ParseError::NotObject);
    }
    let hook_event_name = string_field(&value, "hook_event_name", "hookEventName")
        .ok_or(ParseError::MissingEventName)?;
    Ok(NativeHookInput {
        hook_event_name,
        session_id: string_field(&value, "session_id", "sessionId"),
        tool_name: string_field(&value, "tool_name", "toolName"),
        tool_input: field(&value, "tool_input", "toolInput"),
        tool_use_id: string_field(&value, "tool_use_id", "toolUseId"),
        cwd: string_field(&value, "cwd", "cwd"),
    })
}

// ---------------------------------------------------------------------------
// ZCode decision stdout(官方 PermissionRequest 输出协议;stdout 仅协议)
// ---------------------------------------------------------------------------

/// allow 决定(官方 schema,固定字段)。
pub fn allow_decision_json() -> String {
    r#"{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}"#
        .to_string()
}

/// deny 决定(带面向用户的消息)。
pub fn deny_decision_json(message: &str) -> String {
    let mut value = serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": {
                "behavior": "deny",
                "message": message,
            }
        }
    });
    // 与 allow 同样固定字段:去掉可能混入的空键。
    if let Some(extra) = value
        .get_mut("hookSpecificOutput")
        .and_then(|h| h.get_mut("decision"))
        .and_then(|d| d.as_object_mut())
    {
        if extra.get("message").and_then(|m| m.as_str()) == Some("") {
            extra.remove("message");
        }
    }
    serde_json::to_string(&value).expect("decision json serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 官方 PermissionRequest 样例(含全部常用字段)可解析,别名容忍。
    #[test]
    fn parses_official_permission_request_shape() {
        let line = r#"{"session_id":"session-123","transcript_path":"/tmp/t.jsonl",
            "cwd":"/workspace/demo","permission_mode":"default",
            "hook_event_name":"PermissionRequest","tool_name":"Bash",
            "tool_input":{"command":"ls"},"tool_use_id":"tool-123"}"#;
        let parsed = parse_native_input(line).unwrap();
        assert_eq!(parsed.hook_event_name, "PermissionRequest");
        assert_eq!(parsed.session_id.as_deref(), Some("session-123"));
        assert_eq!(parsed.tool_name.as_deref(), Some("Bash"));
        assert_eq!(parsed.tool_use_id.as_deref(), Some("tool-123"));
        assert_eq!(parsed.tool_input.as_ref().unwrap()["command"], "ls");
    }

    #[test]
    fn parses_camel_case_aliases_and_missing_optional_fields() {
        let parsed = parse_native_input(
            r#"{"hookEventName":"Stop","sessionId":"s1","stop_hook_active":false}"#,
        )
        .unwrap();
        assert_eq!(parsed.hook_event_name, "Stop");
        assert_eq!(parsed.session_id.as_deref(), Some("s1"));
        assert!(parsed.tool_name.is_none());
        assert!(parsed.tool_use_id.is_none(), "tool_use_id 不得假定存在");
    }

    #[test]
    fn rejects_empty_nonobject_missing_event_and_oversize() {
        assert_eq!(parse_native_input("").unwrap_err(), ParseError::Empty);
        assert_eq!(
            parse_native_input("[1,2]").unwrap_err(),
            ParseError::NotObject
        );
        assert_eq!(
            parse_native_input(r#"{"tool_name":"Bash"}"#).unwrap_err(),
            ParseError::MissingEventName
        );
        let big = format!(r#"{{"hook_event_name":"Stop","pad":"{}"}}"#, "x".repeat(MAX_FRAME_BYTES));
        assert!(matches!(
            parse_native_input(&big).unwrap_err(),
            ParseError::Oversize(_)
        ));
    }

    /// allow/deny stdout 与官方文档 schema 逐字节一致(allow 为固定串)。
    #[test]
    fn decision_json_matches_official_schema() {
        assert_eq!(
            allow_decision_json(),
            r#"{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}"#
        );
        let deny = deny_decision_json("User declined this action in Agent Console");
        let value: serde_json::Value = serde_json::from_str(&deny).unwrap();
        let out = &value["hookSpecificOutput"];
        assert_eq!(out["hookEventName"], "PermissionRequest");
        assert_eq!(out["decision"]["behavior"], "deny");
        assert_eq!(
            out["decision"]["message"],
            "User declined this action in Agent Console"
        );
        // 输出体积远小于官方 stdout 32KiB 上限。
        assert!(deny.len() < 32 * 1024);
    }

    #[test]
    fn ask_request_validation_bounds() {
        let ok = AskRequest {
            question: "继续执行吗?".to_string(),
            options: vec!["是".to_string(), "否".to_string()],
            allow_free_text: true,
            call_id: Some("1".to_string()),
        };
        assert!(ok.validate().is_ok());
        let empty = AskRequest {
            question: "  ".to_string(),
            options: vec![],
            allow_free_text: false,
            call_id: None,
        };
        assert!(empty.validate().is_err());
        let too_many = AskRequest {
            question: "q".to_string(),
            options: vec!["o".to_string(); AskRequest::MAX_OPTIONS + 1],
            allow_free_text: false,
            call_id: None,
        };
        assert!(too_many.validate().is_err());
    }

    #[test]
    fn wait_clamped_to_contract_range() {
        assert_eq!(clamp_wait_ms(0), Duration::from_millis(MIN_REMOTE_WAIT_MS));
        assert_eq!(clamp_wait_ms(10_000), Duration::from_millis(10_000));
        assert_eq!(
            clamp_wait_ms(u64::MAX),
            Duration::from_millis(MAX_REMOTE_WAIT_MS)
        );
    }
}
