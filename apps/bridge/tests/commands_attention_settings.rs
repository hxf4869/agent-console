//! 问题/审批/设置命令集成测试(权威规格 §16.2/§16.3/§16.4/§29.1):
//! question 过期 → QUESTION_EXPIRED、approval 过期 → APPROVAL_EXPIRED、
//! 多选与自由文本约束、设置组合不支持 → SETTING_COMBINATION_UNSUPPORTED、
//! READ_ONLY → CONTROL_READ_ONLY、生效范围标记。

use std::path::PathBuf;
use std::time::Duration;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::{CommandGateway, SettingEffective};
use bridge::domain::{
    CommandPayload, CommandRequest, DomainEvent, Operation, OutputText, SessionKey, SettingKind,
    SettingUpdate, StableErrorCode,
};
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-cmd-attn";
const CONV_QUESTION: &str = "dddddddd-1111-4111-8111-111111111111";
const CONV_APPROVAL: &str = "dddddddd-2222-4222-8222-222222222222";
const CONV_SETTINGS: &str = "dddddddd-3333-4333-8333-333333333333";
const CONV_NO_MODEL: &str = "dddddddd-4444-4444-8444-444444444444";
const CONV_RO: &str = "dddddddd-5555-4555-8555-555555555555";
const CONV_NO_DYN: &str = "dddddddd-6666-4666-8666-666666666666";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-cmd-attn-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn session_node(conversation: &str, turn_extra: serde_json::Value) -> serde_json::Value {
    let mut turn = json!({
        "outputLines": ["a-1", "a-2"],
        "lineDelayMs": 60,
        "questionAfterLine": 1
    });
    if let (Some(base), Some(extra)) = (turn.as_object_mut(), turn_extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    json!({
        "conversationId": conversation,
        "title": format!("fixture-{conversation}"),
        "cwd": format!("/tmp/fixture-{conversation}"),
        "model": "gpt-5.3-fixture",
        "approvalPolicy": "untrusted",
        "turn": turn
    })
}

/// 无 model 键的会话(Desktop 未提供 → extract_settings 不产生 model 选项)。
fn session_node_without_model(conversation: &str) -> serde_json::Value {
    let mut node = session_node(conversation, json!({}));
    node["model"] = serde_json::Value::Null;
    node
}

/// 有 effort 当前值 + 动态可选值的会话(fixture 数据,验证 Desktop 提供动态
/// availableValues 时的正路径;真机 0.153.1 不携带该字段)。
fn session_node_with_effort(conversation: &str) -> serde_json::Value {
    let mut node = session_node(conversation, json!({}));
    node["reasoningEffort"] = json!("max");
    node["latestCollaborationMode"] = json!({
        "mode": "fixture",
        "settings": {"effortAvailableValues": ["high", "max"]}
    });
    node
}

/// 有 effort 当前值但无动态可选值的会话(真机形状 → 写入被拒)。
fn session_node_effort_without_values(conversation: &str) -> serde_json::Value {
    let mut node = session_node(conversation, json!({}));
    node["reasoningEffort"] = json!("high");
    node
}

async fn spawn_fake(dir: &PathBuf, nodes: &[serde_json::Value]) -> (PathBuf, std::process::Child) {
    let socket = PathBuf::from(format!(
        "/tmp/ac-cmd-attn-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = json!({ "sessions": nodes });
    let script_path = dir.join("script.json");
    std::fs::write(&script_path, serde_json::to_string(&script).unwrap()).unwrap();
    let stderr_log = std::fs::File::create(dir.join("fake-owner.log")).expect("log file");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fake-codex-owner"))
        .arg(socket.clone())
        .arg(script_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log))
        .spawn()
        .expect("spawn fake-codex-owner");
    for _ in 0..250 {
        if socket.exists() {
            break;
        }
        if let Ok(Some(_)) = child.try_wait() {
            panic!("fake-codex-owner exited before binding socket");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");
    (socket, child)
}

fn adapter_config(socket: PathBuf, conversations: &[&str]) -> CodexAdapterConfig {
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(socket);
    config.codex_home = Some(temp_dir("empty-home"));
    config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    config.write_method_probes = WriteMethodProbes {
        start_turn: ProbeResult::Passed,
        steer_turn: ProbeResult::Passed,
        interrupt_turn: ProbeResult::Passed,
        answer_question: ProbeResult::Passed,
        update_settings: ProbeResult::Passed,
    };
    config.static_sessions = conversations
        .iter()
        .map(|c| CatalogThread {
            id: c.to_string(),
            title: Some(format!("fixture-{c}")),
            project_display_name: None,
            model: None,
            reasoning_effort: None,
            git_branch: None,
            created_at: None,
            updated_at: None,
            archived: false,
            agent_nickname: None,
            agent_role: None,
        })
        .collect();
    config
}

fn key(conversation: &str) -> SessionKey {
    SessionKey::codex(DEVICE, conversation)
}

fn start_turn_request(session: &SessionKey) -> CommandRequest {
    CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: session.clone(),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("fixture-go"),
        },
    }
}

async fn drain(
    mut receipts: tokio::sync::mpsc::Receiver<bridge::domain::CommandReceipt>,
) -> Vec<bridge::domain::ReceiptState> {
    let mut states = Vec::new();
    while let Some(receipt) = receipts.recv().await {
        let terminal = matches!(
            receipt.state,
            bridge::domain::ReceiptState::Completed
                | bridge::domain::ReceiptState::Rejected { .. }
                | bridge::domain::ReceiptState::OutcomeUnknown
        );
        states.push(receipt.state);
        if terminal {
            break;
        }
    }
    states
}

async fn collect_until(
    rx: &mut tokio::sync::mpsc::Receiver<DomainEvent>,
    deadline: Duration,
    mut done: impl FnMut(&[DomainEvent]) -> bool,
) -> Vec<DomainEvent> {
    let mut events = Vec::new();
    let end = tokio::time::Instant::now() + deadline;
    loop {
        if done(&events) {
            return events;
        }
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return events;
        }
        match tokio::time::timeout(remaining.min(Duration::from_millis(50)), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => return events,
            Err(_) => {}
        }
    }
}

struct FakeGuard(std::process::Child);
impl Drop for FakeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn question_expired_and_answer_constraints() {
    let dir = temp_dir("question");
    let (socket, fake) = spawn_fake(
        &dir,
        &[session_node(
            CONV_QUESTION,
            json!({
                "question": {
                    "id": "q-attn-1",
                    "title": "fixture question",
                    "options": [
                        {"id": "opt-a", "label": "A"},
                        {"id": "opt-b", "label": "B"}
                    ],
                    "allowMultiple": false,
                    "allowFreeText": false
                }
            }),
        )],
    )
    .await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_QUESTION]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        bridge::local_store::LocalStore::open(&temp_dir("store"))
            .await
            .unwrap(),
    );
    let session = key(CONV_QUESTION);

    // 快照中不存在的原生 question ID → QUESTION_EXPIRED。
    let err = gateway
        .answer_question(&session, "no-such-question", &["opt-a".into()], None)
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::QuestionExpired);

    // 开始 turn,等待问题出现。
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&session, tx).await.unwrap();
    drain(
        gateway
            .submit(start_turn_request(&session))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    collect_until(&mut rx, Duration::from_secs(10), |events| {
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::PendingAttentionAdded { attention } if attention.native_id() == "q-attn-1"))
    })
    .await;

    // 单选问题提交两个 option → 拒绝(不透传)。
    let err = gateway
        .answer_question(
            &session,
            "q-attn-1",
            &["opt-a".into(), "opt-b".into()],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);

    // 自由文本不被允许 → 拒绝。
    let err = gateway
        .answer_question(
            &session,
            "q-attn-1",
            &["opt-a".into()],
            Some(OutputText::new("free text")),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);

    // 未知 option ID → 拒绝。
    let err = gateway
        .answer_question(&session, "q-attn-1", &["opt-fake".into()], None)
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);

    // 按原生 question ID + option ID 回答 → Completed;问题移除。
    let submission = gateway
        .answer_question(&session, "q-attn-1", &["opt-a".into()], None)
        .await
        .unwrap();
    let states = drain(submission.into_receipts().unwrap()).await;
    assert!(states
        .last()
        .is_some_and(|s| *s == bridge::domain::ReceiptState::Completed));

    collect_until(&mut rx, Duration::from_secs(10), |events| {
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::PendingAttentionRemoved { native_id, .. } if native_id == "q-attn-1"))
    })
    .await;

    // 回答后(问题已移除)再次回答 → QUESTION_EXPIRED。
    let err = gateway
        .answer_question(&session, "q-attn-1", &["opt-a".into()], None)
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::QuestionExpired);
}

#[tokio::test]
async fn approval_expired_and_native_decision_only() {
    let dir = temp_dir("approval");
    let (socket, fake) = spawn_fake(
        &dir,
        &[session_node(
            CONV_APPROVAL,
            json!({
                "approval": {
                    "id": "ap-1",
                    "riskDescription": "fixture risk",
                    "requestedAction": "fixture action",
                    "decisions": [
                        {"id": "allow", "label": "Allow"},
                        {"id": "deny", "label": "Deny"}
                    ]
                }
            }),
        )],
    )
    .await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_APPROVAL]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        bridge::local_store::LocalStore::open(&temp_dir("store"))
            .await
            .unwrap(),
    );
    let session = key(CONV_APPROVAL);

    // 不存在的 approval → APPROVAL_EXPIRED。
    let err = gateway
        .answer_approval(&session, "no-such-approval", "allow")
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::ApprovalExpired);

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&session, tx).await.unwrap();
    drain(
        gateway
            .submit(start_turn_request(&session))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    collect_until(&mut rx, Duration::from_secs(10), |events| {
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::PendingAttentionAdded { attention } if attention.native_id() == "ap-1"))
    })
    .await;

    // 非原生决定("永远允许"式扩大范围)→ 拒绝,绝不透传(§16.4)。
    let err = gateway
        .answer_approval(&session, "ap-1", "always-allow-forever")
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);

    // 原生决定 → Completed;审批移除。
    let submission = gateway
        .answer_approval(&session, "ap-1", "allow")
        .await
        .unwrap();
    let states = drain(submission.into_receipts().unwrap()).await;
    assert!(states
        .last()
        .is_some_and(|s| *s == bridge::domain::ReceiptState::Completed));
    collect_until(&mut rx, Duration::from_secs(10), |events| {
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::PendingAttentionRemoved { native_id, .. } if native_id == "ap-1"))
    })
    .await;

    // 已移除 → APPROVAL_EXPIRED。
    let err = gateway
        .answer_approval(&session, "ap-1", "allow")
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::ApprovalExpired);
}

#[tokio::test]
async fn settings_combination_and_effective_scope() {
    let dir = temp_dir("settings");
    let (socket, fake) = spawn_fake(
        &dir,
        &[
            session_node_with_effort(CONV_SETTINGS),
            // 无 model 键 → latestModel 为空 → model 类别未提供。
            session_node_without_model(CONV_NO_MODEL),
            // 有 effort 当前值但快照未提供动态可选值(真机形状)。
            session_node_effort_without_values(CONV_NO_DYN),
        ],
    )
    .await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(
        socket,
        &[CONV_SETTINGS, CONV_NO_MODEL, CONV_NO_DYN],
    ))
    .await
    .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        bridge::local_store::LocalStore::open(&temp_dir("store"))
            .await
            .unwrap(),
    );
    let session = key(CONV_SETTINGS);

    // 快照携带动态设置选项(§16.1;投影仍输出 Model,但产品仅开放 effort 写)。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    assert!(snapshot
        .capabilities
        .settings
        .iter()
        .any(|o| o.kind == SettingKind::Model));

    // kind 白名单:仅 ReasoningEffort 逐项真机验证,Model 一律拒绝。
    let err = gateway
        .update_settings(
            &session,
            vec![SettingUpdate {
                kind: SettingKind::Model,
                value: "gpt-5.3-fixture-turbo".to_string(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::SettingCombinationUnsupported);

    // 真机形状:Desktop 未提供动态可选值(空列表)→ 拒绝,绝不放行任意字符串。
    let no_dyn = key(CONV_NO_DYN);
    let _ = adapter.runtime_snapshot(&no_dyn).await.unwrap();
    let err = gateway
        .update_settings(
            &no_dyn,
            vec![SettingUpdate {
                kind: SettingKind::ReasoningEffort,
                value: "high".to_string(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::SettingCombinationUnsupported);

    // 正路径:快照提供 effort 动态可选值 → 值在其中即可写;生效范围 NextTurn。
    let outcome = gateway
        .update_settings(
            &session,
            vec![SettingUpdate {
                kind: SettingKind::ReasoningEffort,
                value: "high".to_string(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(outcome.effective, SettingEffective::NextTurn);
    let states = drain(outcome.submission.into_receipts().unwrap()).await;
    assert!(states
        .last()
        .is_some_and(|s| *s == bridge::domain::ReceiptState::Completed));

    // 原生值已生效(snapshot current_value 更新;§16.2 生效范围明确为下一次 turn)。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    assert_eq!(
        snapshot
            .capabilities
            .settings
            .iter()
            .find(|o| o.kind == SettingKind::ReasoningEffort)
            .and_then(|o| o.current_value.clone()),
        Some("high".to_string())
    );

    // RUNNING 中:设置锁定 → SETTING_COMBINATION_UNSUPPORTED,不静默降级。
    drain(
        gateway
            .submit(start_turn_request(&session))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    // 等 turn 进入 Running(快照传播),保证 mutable=false 判定成立。
    let mut running = false;
    for _ in 0..200 {
        if adapter
            .runtime_snapshot(&session)
            .await
            .unwrap()
            .current_turn
            .is_some()
        {
            running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(running, "turn 未进入 Running");
    // kind/option/值域均合法(值在动态可选值中),拒绝只能来自锁定。
    let err = gateway
        .update_settings(
            &session,
            vec![SettingUpdate {
                kind: SettingKind::ReasoningEffort,
                value: "max".to_string(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::SettingCombinationUnsupported);

    // 类别未提供(快照无该 option)→ SETTING_COMBINATION_UNSUPPORTED。
    let no_model = key(CONV_NO_MODEL);
    let _ = adapter.runtime_snapshot(&no_model).await.unwrap();
    let err = gateway
        .update_settings(
            &no_model,
            vec![SettingUpdate {
                kind: SettingKind::ReasoningEffort,
                value: "high".to_string(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::SettingCombinationUnsupported);
}

#[tokio::test]
async fn read_only_mode_rejects_settings_update() {
    let dir = temp_dir("ro");
    // 组合校验所需全部就绪:effort 当前值 + 动态可选值(校验可全通过)。
    let mut ro_node = session_node(CONV_RO, json!({}));
    ro_node["reasoningEffort"] = json!("high");
    ro_node["latestCollaborationMode"] = json!({
        "mode": "fixture",
        "settings": {"effortAvailableValues": ["high", "max"]}
    });
    let (socket, fake) = spawn_fake(&dir, &[ro_node]).await;
    let _guard = FakeGuard(fake);
    let mut config = adapter_config(socket, &[CONV_RO]);
    config.version_report = Some("codex-cli 9.9.9-unknown".to_string());
    let adapter = CodexAdapter::connect(config).await.unwrap();
    let gateway = CommandGateway::new(
        adapter,
        bridge::local_store::LocalStore::open(&temp_dir("store"))
            .await
            .unwrap(),
    );
    let err = gateway
        .update_settings(
            &key(CONV_RO),
            vec![SettingUpdate {
                kind: SettingKind::ReasoningEffort,
                value: "high".to_string(),
            }],
        )
        .await
        .unwrap_err();
    // READ_ONLY 由 submit 的 capability gate 拒绝(§16.2 设置写入仍走 gate)。
    assert_eq!(err.code, StableErrorCode::ControlReadOnly);
}
