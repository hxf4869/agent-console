//! Codex Desktop IPC 强类型消息。
//!
//! 只覆盖 `docs/CODEX-IPC-PROTOCOL.md` 中记录的方法;字段名与 Desktop 实现一致
//! (serde rename camelCase)。未识别的信封类型解析为 [`IncomingMessage::Unknown`],
//! 未识别的写方法由 [`method_version`] 返回版本 0 并被上层拒绝,绝不猜测字段开启写能力。

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod method {
    /// 已在 asar 逆向中确认存在的方法名(协议文档 §4/§6/§7)。
    pub const INITIALIZE: &str = "initialize";
    pub const THREAD_OWNER_DISCOVERY: &str = "thread-owner-discovery";
    pub const THREAD_FOLLOWER_START_TURN: &str = "thread-follower-start-turn";
    pub const THREAD_FOLLOWER_LOAD_COMPLETE_HISTORY: &str = "thread-follower-load-complete-history";
    pub const THREAD_FOLLOWER_COMPACT_THREAD: &str = "thread-follower-compact-thread";
    pub const THREAD_FOLLOWER_STEER_TURN: &str = "thread-follower-steer-turn";
    pub const THREAD_FOLLOWER_INTERRUPT_TURN: &str = "thread-follower-interrupt-turn";
    pub const THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS: &str =
        "thread-follower-update-thread-settings";
    pub const THREAD_FOLLOWER_EDIT_LAST_USER_TURN: &str = "thread-follower-edit-last-user-turn";
    pub const THREAD_FOLLOWER_COMMAND_APPROVAL_DECISION: &str =
        "thread-follower-command-approval-decision";
    pub const THREAD_FOLLOWER_FILE_APPROVAL_DECISION: &str =
        "thread-follower-file-approval-decision";
    pub const THREAD_FOLLOWER_PERMISSIONS_REQUEST_APPROVAL_RESPONSE: &str =
        "thread-follower-permissions-request-approval-response";
    pub const THREAD_FOLLOWER_SUBMIT_USER_INPUT: &str = "thread-follower-submit-user-input";
    pub const THREAD_FOLLOWER_SUBMIT_MCP_SERVER_ELICITATION_RESPONSE: &str =
        "thread-follower-submit-mcp-server-elicitation-response";
    pub const THREAD_FOLLOWER_SET_QUEUED_FOLLOW_UPS_STATE: &str =
        "thread-follower-set-queued-follow-ups-state";
    pub const IDE_CONTEXT: &str = "ide-context";

    pub const THREAD_STREAM_STATE_CHANGED: &str = "thread-stream-state-changed";
    pub const THREAD_STREAM_FOLLOWING_CHANGED: &str = "thread-stream-following-changed";
    pub const THREAD_STREAM_FOLLOWING_STATUS_REQUESTED: &str =
        "thread-stream-following-status-requested";
    pub const THREAD_READ_STATE_CHANGED: &str = "thread-read-state-changed";
    pub const THREAD_ARCHIVED: &str = "thread-archived";
    pub const THREAD_UNARCHIVED: &str = "thread-unarchived";
    pub const THREAD_QUEUED_FOLLOWUPS_CHANGED: &str = "thread-queued-followups-changed";
    pub const CLIENT_STATUS_CHANGED: &str = "client-status-changed";
    pub const QUERY_CACHE_INVALIDATE: &str = "query-cache-invalidate";
}

/// asar 版本表(152.0.7977.64 实测值;协议文档 §4)。
fn table_version(method: &str) -> u32 {
    match method {
        method::THREAD_STREAM_STATE_CHANGED => 11,
        method::THREAD_STREAM_FOLLOWING_CHANGED
        | method::THREAD_STREAM_FOLLOWING_STATUS_REQUESTED
        | method::THREAD_OWNER_DISCOVERY
        | method::THREAD_FOLLOWER_LOAD_COMPLETE_HISTORY
        | method::THREAD_FOLLOWER_COMPACT_THREAD
        | method::THREAD_FOLLOWER_STEER_TURN
        | method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS
        | method::THREAD_FOLLOWER_COMMAND_APPROVAL_DECISION
        | method::THREAD_FOLLOWER_FILE_APPROVAL_DECISION
        | method::THREAD_FOLLOWER_PERMISSIONS_REQUEST_APPROVAL_RESPONSE
        | method::THREAD_FOLLOWER_SUBMIT_USER_INPUT
        | method::THREAD_FOLLOWER_SUBMIT_MCP_SERVER_ELICITATION_RESPONSE
        | method::THREAD_FOLLOWER_SET_QUEUED_FOLLOW_UPS_STATE
        | method::THREAD_UNARCHIVED => 1,
        method::THREAD_READ_STATE_CHANGED
        | method::THREAD_ARCHIVED
        | method::THREAD_QUEUED_FOLLOWUPS_CHANGED => 2,
        method::THREAD_FOLLOWER_START_TURN => 2,
        method::THREAD_FOLLOWER_INTERRUPT_TURN => 4,
        method::THREAD_FOLLOWER_EDIT_LAST_USER_TURN => 2,
        _ => 0,
    }
}

/// 是否为 Desktop owner 处理的 follower 写/读方法。
pub fn is_follower_method(method: &str) -> bool {
    method.starts_with("thread-follower-")
}

/// 计算发送 request 的 `version` 字段(协议文档 §4 规则,已实测)。
pub fn request_version(method: &str, params: &Value, host_id: Option<&str>) -> u32 {
    let base = table_version(method);
    if host_id.is_some() && is_follower_method(method) {
        return base + 1;
    }
    if method == method::THREAD_FOLLOWER_INTERRUPT_TURN {
        let has_expected_turn = params.get("expectedTurnId").is_some_and(|v| !v.is_null());
        if host_id.is_none() && !has_expected_turn {
            return 3;
        }
    }
    base
}

/// 计算 broadcast 的 `version` 字段。
pub fn broadcast_version(method: &str) -> u32 {
    table_version(method)
}

// ---------------------------------------------------------------------------
// 信封
// ---------------------------------------------------------------------------

pub const TYPE_REQUEST: &str = "request";
pub const TYPE_RESPONSE: &str = "response";
pub const TYPE_BROADCAST: &str = "broadcast";
pub const TYPE_CLIENT_DISCOVERY_REQUEST: &str = "client-discovery-request";
pub const TYPE_CLIENT_DISCOVERY_RESPONSE: &str = "client-discovery-response";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestFrame {
    pub request_id: String,
    #[serde(default)]
    pub source_client_id: Option<String>,
    #[serde(default)]
    pub version: u32,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub target_client_id: Option<String>,
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultType {
    Success,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseFrame {
    pub request_id: String,
    pub result_type: ResultType,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub handled_by_client_id: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFrame {
    pub method: String,
    #[serde(default)]
    pub source_client_id: Option<String>,
    #[serde(default)]
    pub target_client_ids: Option<Vec<String>>,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientDiscoveryRequestFrame {
    pub request_id: String,
    pub request: RequestFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientDiscoveryResponseFrame {
    pub request_id: String,
    pub response: DiscoveryAnswer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryAnswer {
    #[serde(rename = "canHandle")]
    pub can_handle: bool,
}

/// 入站消息:强类型信封;未知 `type` 落入 `Unknown`(不 panic,交上层策略)。
#[derive(Debug, Clone)]
pub enum IncomingMessage {
    Request(RequestFrame),
    Response(ResponseFrame),
    Broadcast(BroadcastFrame),
    ClientDiscoveryRequest(ClientDiscoveryRequestFrame),
    ClientDiscoveryResponse(ClientDiscoveryResponseFrame),
    Unknown(Value),
}

impl IncomingMessage {
    pub fn parse(value: Value) -> Self {
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return IncomingMessage::Unknown(value);
        };
        // 反序列化失败的消息保留原样落入 Unknown:不 panic、不丢帧,
        // 由上层决定是否触发 snapshot 补偿(§12:未识别关键 patch 触发 snapshot)。
        match kind {
            TYPE_REQUEST => match serde_json::from_value::<RequestFrame>(value.clone()) {
                Ok(frame) => IncomingMessage::Request(frame),
                Err(_) => IncomingMessage::Unknown(value),
            },
            TYPE_RESPONSE => match serde_json::from_value::<ResponseFrame>(value.clone()) {
                Ok(frame) => IncomingMessage::Response(frame),
                Err(_) => IncomingMessage::Unknown(value),
            },
            TYPE_BROADCAST => match serde_json::from_value::<BroadcastFrame>(value.clone()) {
                Ok(frame) => IncomingMessage::Broadcast(frame),
                Err(_) => IncomingMessage::Unknown(value),
            },
            TYPE_CLIENT_DISCOVERY_REQUEST => {
                match serde_json::from_value::<ClientDiscoveryRequestFrame>(value.clone()) {
                    Ok(frame) => IncomingMessage::ClientDiscoveryRequest(frame),
                    Err(_) => IncomingMessage::Unknown(value),
                }
            }
            TYPE_CLIENT_DISCOVERY_RESPONSE => {
                match serde_json::from_value::<ClientDiscoveryResponseFrame>(value.clone()) {
                    Ok(frame) => IncomingMessage::ClientDiscoveryResponse(frame),
                    Err(_) => IncomingMessage::Unknown(value),
                }
            }
            _ => IncomingMessage::Unknown(value),
        }
    }
}

/// 出站消息(发送前编码)。
#[derive(Debug, Clone)]
pub enum OutgoingMessage {
    Request { frame: RequestFrame },
    Response { frame: ResponseFrame },
    Broadcast { frame: BroadcastFrame },
    ClientDiscoveryResponse { frame: ClientDiscoveryResponseFrame },
}

impl OutgoingMessage {
    pub fn to_value(&self) -> Value {
        match self {
            OutgoingMessage::Request { frame } => {
                let mut v = serde_json::to_value(frame).expect("request frame serializes");
                v["type"] = Value::String(TYPE_REQUEST.to_string());
                v
            }
            OutgoingMessage::Response { frame } => {
                let mut v = serde_json::to_value(frame).expect("response frame serializes");
                v["type"] = Value::String(TYPE_RESPONSE.to_string());
                v
            }
            OutgoingMessage::Broadcast { frame } => {
                let mut v = serde_json::to_value(frame).expect("broadcast frame serializes");
                v["type"] = Value::String(TYPE_BROADCAST.to_string());
                v
            }
            OutgoingMessage::ClientDiscoveryResponse { frame } => {
                let mut v = serde_json::to_value(frame).expect("discovery response serializes");
                v["type"] = Value::String(TYPE_CLIENT_DISCOVERY_RESPONSE.to_string());
                v
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 具体方法的 params / result
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub client_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerDiscoveryParams {
    pub host_id: String,
    pub conversation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OwnerDiscoveryResult {
    #[serde(default)]
    pub supports_untrusted_app_input: Option<bool>,
}

/// 会话流变更:`snapshot` 全量 / `patches` 增量(Immer patch,非 RFC6902)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StreamChange {
    #[serde(rename_all = "camelCase")]
    Snapshot {
        revision: u64,
        /// 渲染进程会话状态对象。Bridge 只做受控投影,不透传原始 JSON(§25)。
        conversation_state: Value,
    },
    #[serde(rename_all = "camelCase")]
    Patches {
        base_revision: u64,
        revision: u64,
        #[serde(default)]
        patches: Vec<Value>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamStateChangedParams {
    pub conversation_id: String,
    pub host_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_client_ids: Option<Vec<String>>,
    pub change: StreamChange,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowingChangedParams {
    pub conversation_id: String,
    pub host_id: String,
    pub following: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowingStatusRequestedParams {
    pub conversation_id: String,
    pub host_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientStatusChangedParams {
    pub client_id: String,
    #[serde(default)]
    pub client_type: Option<String>,
    #[serde(default)]
    pub is_self: Option<bool>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadCompleteHistoryParams {
    pub conversation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadCompleteHistoryResult {
    pub revision: u64,
}

/// 文本输入块(start/steer 的 `input` 数组元素;协议文档 §7.1)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputBlock {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
    #[serde(default, rename = "text_elements")]
    pub text_elements: Vec<Value>,
}

impl InputBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            kind: "text".to_string(),
            text: text.into(),
            text_elements: Vec::new(),
        }
    }
}

/// start-turn 的 `turnStart` 载荷:`{ request: <turn 请求>, context: <可选> }`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStart {
    pub request: TurnStartRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<TurnStartContext>,
}

/// start-turn 的 `turnStart.request`。已知字段强类型,其余保留原样
/// (`#[serde(flatten)]`),避免把未知字段静默丢弃。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartRequest {
    pub thread_id: String,
    pub input: Vec<InputBlock>,
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, Value>,
}

/// start-turn 的 `turnStart.context`:首版全部可选,整块保真传递。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnStartContext(pub Value);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartTurnParams {
    pub conversation_id: String,
    pub turn_start: TurnStart,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartTurnResult {
    #[serde(default)]
    pub result: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SteerTurnParams {
    pub conversation_id: String,
    pub input: Vec<InputBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_user_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_message: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SteerTurnResult {
    #[serde(default)]
    pub result: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InterruptMode {
    UserStop,
    System,
    DescendantCleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterruptTurnParams {
    pub conversation_id: String,
    pub mode: InterruptMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_turn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterruptTurnResult {
    pub interrupted_turn_id: String,
    #[serde(default = "default_true", rename = "ok")]
    pub ok: bool,
    #[serde(default, rename = "goalPauseError")]
    pub goal_pause_error: Option<String>,
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_version_follows_table_and_host_rule() {
        let p = json!({});
        assert_eq!(request_version(method::INITIALIZE, &p, None), 0);
        assert_eq!(request_version(method::THREAD_OWNER_DISCOVERY, &p, None), 1);
        assert_eq!(
            request_version(method::THREAD_OWNER_DISCOVERY, &p, Some("local")),
            1,
            "非 follower 方法不受 hostId 影响"
        );
        assert_eq!(
            request_version(
                method::THREAD_FOLLOWER_LOAD_COMPLETE_HISTORY,
                &p,
                Some("local")
            ),
            2,
            "hostId != null 时 follower 方法 Ev+1"
        );
        assert_eq!(
            request_version(method::THREAD_FOLLOWER_LOAD_COMPLETE_HISTORY, &p, None),
            1
        );
        assert_eq!(
            request_version(method::THREAD_FOLLOWER_START_TURN, &p, Some("local")),
            3
        );
        assert_eq!(
            request_version(
                method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                &p,
                Some("local")
            ),
            2,
            "update-settings Ev=1(§4),hostId → 2;错发 1 时 owner 回 no-client-found(0.153.1 实测)"
        );
        // interrupt:无 expectedTurnId 且无 hostId → legacy 3;带 hostId → 5;带 expectedTurnId 且无 hostId → 4
        assert_eq!(
            request_version(method::THREAD_FOLLOWER_INTERRUPT_TURN, &p, None),
            3
        );
        assert_eq!(
            request_version(method::THREAD_FOLLOWER_INTERRUPT_TURN, &p, Some("local")),
            5
        );
        let with_turn = json!({"expectedTurnId": "01a"});
        assert_eq!(
            request_version(method::THREAD_FOLLOWER_INTERRUPT_TURN, &with_turn, None),
            4
        );
    }

    #[test]
    fn parses_snapshot_change() {
        let raw = json!({
            "conversationId": "01a",
            "hostId": "local",
            "change": {"type": "snapshot", "revision": 7, "conversationState": {"id": "01a", "title": null}}
        });
        let params: StreamStateChangedParams = serde_json::from_value(raw).unwrap();
        match params.change {
            StreamChange::Snapshot { revision, .. } => assert_eq!(revision, 7),
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn parses_patches_change() {
        let raw = json!({
            "conversationId": "01a",
            "hostId": "local",
            "change": {"type": "patches", "baseRevision": 3, "revision": 4, "patches": [
                {"op": "replace", "path": ["title"], "value": "t"}
            ]}
        });
        let params: StreamStateChangedParams = serde_json::from_value(raw).unwrap();
        match params.change {
            StreamChange::Patches {
                base_revision,
                revision,
                patches,
            } => {
                assert_eq!((base_revision, revision), (3, 4));
                assert_eq!(patches.len(), 1);
            }
            _ => panic!("expected patches"),
        }
    }

    #[test]
    fn unknown_type_falls_back_to_unknown() {
        let msg = IncomingMessage::parse(json!({"type": "future-frame", "x": 1}));
        assert!(matches!(msg, IncomingMessage::Unknown(_)));
        let msg = IncomingMessage::parse(json!({"no_type": true}));
        assert!(matches!(msg, IncomingMessage::Unknown(_)));
    }

    #[test]
    fn start_turn_params_serialize_with_native_names() {
        let params = StartTurnParams {
            conversation_id: "conv".into(),
            turn_start: TurnStart {
                request: TurnStartRequest {
                    thread_id: "conv".into(),
                    input: vec![InputBlock::text("回复 ok 即可")],
                    extra: Default::default(),
                },
                context: None,
            },
        };
        let v = serde_json::to_value(&params).unwrap();
        assert_eq!(v["conversationId"], "conv");
        assert_eq!(v["turnStart"]["request"]["threadId"], "conv");
        assert_eq!(v["turnStart"]["request"]["input"][0]["type"], "text");
        assert_eq!(
            v["turnStart"]["request"]["input"][0]["text_elements"],
            json!([])
        );
    }

    #[test]
    fn interrupt_result_tolerates_missing_ok() {
        let r: InterruptTurnResult =
            serde_json::from_value(json!({"interruptedTurnId": "01a"})).unwrap();
        assert!(r.ok);
        assert_eq!(r.interrupted_turn_id, "01a");
    }
}
