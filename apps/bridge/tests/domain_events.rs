//! 领域模型集成测试:serde 往返、Debug 脱敏、稳定错误码、能力默认值(§9-§16、§27.6)。

use bridge::domain::{
    AgentKind, CapabilitySet, CommandPayload, CommandRequest, ControlMode, DomainEvent, Item,
    ItemContent, ItemId, LastTurnOutcome, OutputBytes, OutputText, PendingApproval,
    PendingAttention, PendingQuestion, QueueState, RuntimeSnapshot, SessionKey, StableErrorCode,
    TransferLimits, TurnId,
};
use uuid::Uuid;

#[test]
fn session_key_serde_roundtrip() {
    let key = SessionKey {
        device_id: "device-1".to_string(),
        agent_kind: AgentKind::CodexDesktop,
        native_session_id: "11111111-1111-4111-8111-111111111111".to_string(),
        relay_session_uuid: Some(Uuid::nil()),
    };
    let text = serde_json::to_string(&key).unwrap();
    assert!(text.contains("CODEX_DESKTOP"), "actual: {text}");
    let back: SessionKey = serde_json::from_str(&text).unwrap();
    assert_eq!(back, key);
}

#[test]
fn item_debug_does_not_leak_text() {
    let item = Item {
        item_id: ItemId::native("item-1"),
        turn: Some(TurnId::native("turn-1")),
        revision: 3,
        created_at: None,
        content: ItemContent::UserMessage {
            text: OutputText::new("机密用户输入-secret-prompt-body"),
        },
    };
    let debug = format!("{item:?}");
    assert!(debug.contains("item-1"), "ID 允许出现: {debug}");
    assert!(!debug.contains("机密用户输入"), "正文不得出现: {debug}");

    // 序列化保留原文(内部 JSON 调试需要)。
    let json = serde_json::to_string(&item).unwrap();
    assert!(json.contains("机密用户输入"));
}

#[test]
fn pending_attention_events_debug_redacted() {
    let attention = PendingAttention::Question(PendingQuestion {
        question_id: "q-1".to_string(),
        title: OutputText::new("机密问题标题-confidential"),
        description: OutputText::default(),
        options: vec![],
        allow_multiple: false,
        allow_free_text: true,
        turn: Some(TurnId::native("turn-1")),
        created_at: None,
        valid: true,
    });
    let event = DomainEvent::PendingAttentionAdded { attention };
    let debug = format!("{event:?}");
    assert!(debug.contains("q-1"));
    assert!(!debug.contains("confidential"), "actual: {debug}");

    let approval = PendingAttention::Approval(PendingApproval {
        approval_id: "ap-1".to_string(),
        risk_description: OutputText::new("风险说明-secret-risk"),
        requested_action: OutputText::new("动作-secret-action"),
        decisions: vec![],
        scope: None,
        turn: None,
        created_at: None,
        valid: true,
    });
    let debug = format!(
        "{:?}",
        DomainEvent::PendingAttentionAdded {
            attention: approval,
        }
    );
    assert!(!debug.contains("secret-risk"), "actual: {debug}");
}

#[test]
fn output_event_debug_shows_length_only() {
    let event = DomainEvent::OutputAppend {
        item_id: ItemId::native("item-cmd"),
        expected_offset: 0,
        bytes: OutputBytes::from_text("命令输出正文-secret-output"),
        channel: bridge::domain::OutputChannel::Combined,
    };
    let debug = format!("{event:?}");
    assert!(debug.contains("expected_offset: 0"));
    assert!(!debug.contains("secret-output"), "actual: {debug}");
    // serde 往返。
    let text = serde_json::to_string(&event).unwrap();
    let back: DomainEvent = serde_json::from_str(&text).unwrap();
    assert_eq!(back, event);
}

#[test]
fn stable_error_code_strings_match_spec() {
    assert_eq!(
        StableErrorCode::ControlReadOnly.as_str(),
        "CONTROL_READ_ONLY"
    );
    assert_eq!(StableErrorCode::StaleTurn.as_str(), "STALE_TURN");
    assert_eq!(StableErrorCode::InternalError.as_str(), "INTERNAL_ERROR");
    // §27.6 全集存在且互异。
    let all = [
        StableErrorCode::AuthRequired,
        StableErrorCode::AuthExpired,
        StableErrorCode::CsrfInvalid,
        StableErrorCode::WsTicketInvalid,
        StableErrorCode::WsTicketExpired,
        StableErrorCode::WsTicketConsumed,
        StableErrorCode::DeviceOffline,
        StableErrorCode::DeviceRevoked,
        StableErrorCode::CodexUnavailable,
        StableErrorCode::CodexVersionUnverified,
        StableErrorCode::ControlReadOnly,
        StableErrorCode::CapabilityUnsupported,
        StableErrorCode::SessionNotFound,
        StableErrorCode::StaleTurn,
        StableErrorCode::DuplicateRequestMismatch,
        StableErrorCode::OutcomeUnknown,
        StableErrorCode::ResyncRequired,
        StableErrorCode::QueueAlreadyExists,
        StableErrorCode::QueuePaused,
        StableErrorCode::QuestionExpired,
        StableErrorCode::ApprovalExpired,
        StableErrorCode::SettingCombinationUnsupported,
        StableErrorCode::FileHandleInvalid,
        StableErrorCode::FileOutsideScope,
        StableErrorCode::FileChanged,
        StableErrorCode::FileTypeNotPreviewable,
        StableErrorCode::TransferExpired,
        StableErrorCode::TransferTooLarge,
        StableErrorCode::TransferRangeInvalid,
        StableErrorCode::DiffTooLarge,
        StableErrorCode::RateLimited,
        StableErrorCode::InternalError,
    ];
    let mut names: Vec<&str> = all.iter().map(|c| c.as_str()).collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "错误码字符串必须互异(§27.6)");
}

#[test]
fn capability_set_defaults_are_read_only_degraded() {
    let caps = CapabilitySet::read_only_degraded(Some("codex-cli 9.9.9".to_string()));
    assert_eq!(caps.control_mode, ControlMode::ReadOnly);
    assert!(caps.supported_operations.is_empty());
    assert_eq!(caps.transfer_limits, TransferLimits::default());
    assert_eq!(caps.transfer_limits.upload_max_bytes, 20 * 1024 * 1024);
    let text = serde_json::to_string(&caps).unwrap();
    assert!(text.contains("READ_ONLY"));
}

#[test]
fn command_request_roundtrip_with_queue_payload() {
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: bridge::domain::Operation::QueueNextTurn,
        session_key: SessionKey::codex("device-1", "conv-1"),
        expected_turn_id: Some(TurnId::native("turn-1")),
        expected_runtime_revision: Some(7),
        payload_digest: Some("sha256:fixture".to_string()),
        payload: CommandPayload::QueueNextTurn {
            input: OutputText::new("下一条指令"),
        },
    };
    let text = serde_json::to_string(&request).unwrap();
    let back: CommandRequest = serde_json::from_str(&text).unwrap();
    assert_eq!(back.request_id, request.request_id);
    assert_eq!(back.expected_runtime_revision, Some(7));
}

#[test]
fn runtime_snapshot_defaults_queue_empty() {
    let snapshot = RuntimeSnapshot {
        session_key: SessionKey::codex("device-1", "conv-1"),
        runtime_revision: 4,
        current_turn: None,
        plan: Default::default(),
        pending_questions: vec![],
        pending_approvals: vec![],
        running_commands: vec![],
        background_commands: vec![],
        background_command_count: 0,
        queue: Default::default(),
        capabilities: CapabilitySet::read_only_degraded(None),
        recent_output_cursors: vec![],
    };
    assert_eq!(snapshot.queue.state, QueueState::Empty);
    let text = serde_json::to_string(&snapshot).unwrap();
    assert!(text.contains("EMPTY"));
    let _last: Option<LastTurnOutcome> = None;
}
