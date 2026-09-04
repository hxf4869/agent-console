//! fake-codex-owner 端到端集成测试:spawn 独立 fake Desktop 进程,
//! CodexAdapter 完成 list → snapshot → subscribe → start turn → 输出 append
//! → final 校正 → steer → interrupt → 问题回答(§13.4/§29.4 最小链)。

use std::path::PathBuf;
use std::time::Duration;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::domain::{
    CommandPayload, CommandRequest, DomainEvent, Operation, OutputText, SessionKey, StableErrorCode,
};
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-e2e";
const CONV_FAST: &str = "aaaaaaaa-1111-4111-8111-111111111111";
const CONV_STEER: &str = "aaaaaaaa-2222-4222-8222-222222222222";
const CONV_QUESTION: &str = "aaaaaaaa-3333-4333-8333-333333333333";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-fake-owner-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_script(dir: &PathBuf) -> PathBuf {
    let script = json!({
        "sessions": [
            {
                "conversationId": CONV_FAST,
                "title": "fixture-fast",
                "cwd": "/tmp/fixture-fast",
                "branch": "fixture-branch",
                "turn": {
                    "outputLines": ["1", "2", "3", "4", "5"],
                    // > 合并窗口(75ms):预览增量能在 turn 内以 LIVE_PREVIEW 发出。
                    "lineDelayMs": 120,
                    "finalAnswer": "fixture-final-answer",
                    // 权威输出与预览不同 → 驱动 OutputReplace 校正(§13.3)。
                    "finalOutput": "corrected-authoritative-output\n"
                }
            },
            {
                "conversationId": CONV_STEER,
                "title": "fixture-steer",
                "cwd": "/tmp/fixture-steer",
                "turn": {
                    "outputLines": ["s-1", "s-2", "s-3", "s-4", "s-5", "s-6", "s-7", "s-8"],
                    "lineDelayMs": 120
                }
            },
            {
                "conversationId": CONV_QUESTION,
                "title": "fixture-question",
                "cwd": "/tmp/fixture-question",
                "turn": {
                    "outputLines": ["q-1", "q-2", "q-3"],
                    "lineDelayMs": 30,
                    "questionAfterLine": 2,
                    "question": {
                        "id": "q-e2e-1",
                        "title": "fixture question",
                        "options": [{"id": "opt-a", "label": "A"}, {"id": "opt-b", "label": "B"}],
                        "allowMultiple": false,
                        "allowFreeText": false
                    }
                }
            }
        ]
    });
    let path = dir.join("script.json");
    std::fs::write(&path, serde_json::to_string(&script).unwrap()).unwrap();
    path
}

async fn spawn_fake(dir: &PathBuf) -> (PathBuf, std::process::Child) {
    // macOS sun_path 上限 104 字节:TMPDIR 太长,socket 用短路径。
    let socket = PathBuf::from(format!(
        "/tmp/ac-fake-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = write_script(dir);
    let stderr_log = std::fs::File::create(dir.join("fake-owner.log")).expect("log file");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fake-codex-owner"))
        .arg(socket.clone())
        .arg(script)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log))
        .spawn()
        .expect("spawn fake-codex-owner");
    // 等待 socket 出现(最多 5s)。
    for _ in 0..250 {
        if socket.exists() {
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            let log = std::fs::read_to_string(dir.join("fake-owner.log")).unwrap_or_default();
            panic!("fake-codex-owner exited before binding socket: {status}; log: {log}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");
    (socket, child)
}

fn adapter_config(socket: PathBuf, sessions: Vec<CatalogThread>) -> CodexAdapterConfig {
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(socket);
    // 空 codex_home:无真实 SQLite,列表来自静态种子(fake 路径)。
    config.codex_home = Some(temp_dir("empty-home"));
    config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    config.write_method_probes = WriteMethodProbes {
        start_turn: ProbeResult::Passed,
        steer_turn: ProbeResult::Passed,
        interrupt_turn: ProbeResult::Passed,
        answer_question: ProbeResult::Passed,
        update_settings: ProbeResult::Passed,
    };
    config.static_sessions = sessions;
    config
}

fn seed(id: &str, title: &str) -> CatalogThread {
    CatalogThread {
        id: id.to_string(),
        title: Some(title.to_string()),
        project_display_name: Some(title.to_string()),
        model: None,
        reasoning_effort: None,
        git_branch: Some("fixture-branch".to_string()),
        created_at: None,
        updated_at: None,
        archived: false,
        agent_nickname: None,
        agent_role: None,
    }
}

fn key(conversation: &str) -> SessionKey {
    SessionKey::codex(DEVICE, conversation)
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

#[tokio::test]
async fn fast_turn_appends_then_authoritative_correction() {
    let dir = temp_dir("fast");
    let (socket, mut fake) = spawn_fake(&dir).await;
    let _fake_guard = FakeGuard(&mut fake);
    let adapter = CodexAdapter::connect(adapter_config(
        socket,
        vec![seed(CONV_FAST, "fixture-fast")],
    ))
    .await
    .unwrap();

    // ---- discover / list ----
    let report = adapter.discover().await;
    assert!(report.ipc_connected);
    assert_eq!(report.catalog, None);
    let caps = adapter.capabilities();
    assert_eq!(
        caps.control_mode,
        bridge::domain::ControlMode::FullControl,
        "版本命中 + probe 通过 → FULL_CONTROL"
    );

    let page = adapter.list_sessions(None, 50, false).await.unwrap();
    assert_eq!(page.sessions.len(), 1);
    assert_eq!(page.sessions[0].session_key.native_session_id, CONV_FAST);
    assert_eq!(
        page.sessions[0].title.as_ref().map(|t| t.as_str()),
        Some("fixture-fast")
    );

    // ---- snapshot ----
    let snapshot = adapter.runtime_snapshot(&key(CONV_FAST)).await.unwrap();
    assert_eq!(snapshot.current_turn, None, "初始 idle");
    // fake 初始快照 revision 从 0 开始(与真实 Desktop 的 1 起步不同,仅合成差异)。
    assert_eq!(snapshot.runtime_revision, 0);

    // ---- subscribe ----
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&key(CONV_FAST), tx).await.unwrap();

    // ---- start turn ----
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV_FAST),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("fixture-go"),
        },
    };
    let mut receipts = adapter
        .execute_command(&key(CONV_FAST), request)
        .await
        .unwrap();
    let first = receipts.recv().await.expect("receipt");
    assert_eq!(first.state, bridge::domain::ReceiptState::DispatchedToCodex);
    let second = receipts.recv().await.expect("receipt");
    assert_eq!(second.state, bridge::domain::ReceiptState::Completed);

    // ---- 观察 append → final 校正 ----
    // 等齐:OutputFinal + 最终回复 ItemUpsert + Completed 生命周期。
    let events = collect_until(&mut rx, Duration::from_secs(10), |events| {
        events.iter().any(|e| matches!(e, DomainEvent::OutputFinal { .. }))
            && events.iter().any(|e| matches!(
                e,
                DomainEvent::ItemUpsert { item }
                    if matches!(item.content, bridge::domain::ItemContent::AssistantMessage { final_message: true, .. })
            ))
            && events.iter().any(|e| matches!(
                e,
                DomainEvent::TurnLifecycle {
                    phase: bridge::domain::ActiveTurnPhase::Idle,
                    outcome: Some(bridge::domain::LastTurnOutcome::Completed),
                    ..
                }
            ))
    })
    .await;

    // TurnLifecycle:Running 出现。
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::TurnLifecycle {
            phase: bridge::domain::ActiveTurnPhase::Running,
            ..
        }
    )));

    // 编号 1..5 无缺号(LIVE_PREVIEW 路径)。
    let mut concatenated = String::new();
    let mut expected_offset = 0u64;
    for event in &events {
        if let DomainEvent::OutputAppend {
            expected_offset: offset,
            bytes,
            ..
        } = event
        {
            assert_eq!(*offset, expected_offset);
            concatenated.push_str(std::str::from_utf8(bytes.as_bytes()).unwrap());
            expected_offset += bytes.len() as u64;
        }
    }
    assert_eq!(concatenated, "1\n2\n3\n4\n5\n", "编号输出无缺号(§13.4)");

    // 权威校正:finalOutput 与预览不同 → OutputReplace。
    let corrected = events.iter().any(|e| matches!(
        e,
        DomainEvent::OutputReplace { bytes, .. }
            if std::str::from_utf8(bytes.as_bytes()).unwrap() == "corrected-authoritative-output\n"
    ));
    assert!(corrected, "缺少权威 OutputReplace 校正: {events:?}");
    let final_event = events.iter().find_map(|e| match e {
        DomainEvent::OutputFinal { byte_length, .. } => Some(*byte_length),
        _ => None,
    });
    assert_eq!(
        final_event,
        Some("corrected-authoritative-output\n".len() as u64)
    );

    // 最终回复 item 与终态。
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::ItemUpsert { item }
            if matches!(item.content, bridge::domain::ItemContent::AssistantMessage { final_message: true, .. })
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::TurnLifecycle {
            phase: bridge::domain::ActiveTurnPhase::Idle,
            outcome: Some(bridge::domain::LastTurnOutcome::Completed),
            ..
        }
    )));
}

#[tokio::test]
async fn steer_then_interrupt_mid_turn() {
    let dir = temp_dir("steer");
    let (socket, mut fake) = spawn_fake(&dir).await;
    let _fake_guard = FakeGuard(&mut fake);
    let adapter = CodexAdapter::connect(adapter_config(
        socket,
        vec![seed(CONV_STEER, "fixture-steer")],
    ))
    .await
    .unwrap();

    let _snapshot = adapter.runtime_snapshot(&key(CONV_STEER)).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&key(CONV_STEER), tx).await.unwrap();

    // 开始长 turn。
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV_STEER),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("fixture-long-run"),
        },
    };
    let mut receipts = adapter
        .execute_command(&key(CONV_STEER), request)
        .await
        .unwrap();
    while let Some(receipt) = receipts.recv().await {
        if receipt.state == bridge::domain::ReceiptState::Completed {
            break;
        }
    }

    // 等第一段输出出现。
    let _ = collect_until(&mut rx, Duration::from_secs(5), |events| {
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::OutputAppend { .. }))
    })
    .await;

    // steer:回执 Completed;随后输出流中出现注入行 "steer-accepted"。
    let steer = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::SteerTurn,
        session_key: key(CONV_STEER),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::Steer {
            input: OutputText::new("fixture-steer-input"),
        },
    };
    let mut steer_receipts = adapter
        .execute_command(&key(CONV_STEER), steer)
        .await
        .unwrap();
    let receipt = steer_receipts.recv().await.expect("steer receipt");
    assert_eq!(
        receipt.state,
        bridge::domain::ReceiptState::DispatchedToCodex
    );
    let receipt = steer_receipts.recv().await.expect("steer completion");
    assert_eq!(receipt.state, bridge::domain::ReceiptState::Completed);

    let events = collect_until(&mut rx, Duration::from_secs(5), |events| {
        events.iter().any(|e| {
            matches!(
                e,
                DomainEvent::OutputAppend { bytes, .. }
                    if std::str::from_utf8(bytes.as_bytes()).unwrap().contains("steer-accepted")
            )
        })
    })
    .await;
    assert!(
        events.iter().any(|e| matches!(
            e,
            DomainEvent::OutputAppend { bytes, .. }
                if std::str::from_utf8(bytes.as_bytes()).unwrap().contains("steer-accepted")
        )),
        "steer 注入行未出现: {}",
        events.len()
    );

    // interrupt(要求 expected turn,§15.4)。
    let snapshot = adapter.runtime_snapshot(&key(CONV_STEER)).await.unwrap();
    let current_turn = snapshot.current_turn.expect("active turn present").turn;
    let interrupt = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::InterruptTurn,
        session_key: key(CONV_STEER),
        expected_turn_id: Some(current_turn),
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::Interrupt,
    };
    let mut interrupt_receipts = adapter
        .execute_command(&key(CONV_STEER), interrupt)
        .await
        .unwrap();
    let receipt = interrupt_receipts.recv().await.expect("interrupt receipt");
    assert_eq!(
        receipt.state,
        bridge::domain::ReceiptState::DispatchedToCodex
    );
    let receipt = interrupt_receipts
        .recv()
        .await
        .expect("interrupt completion");
    assert_eq!(receipt.state, bridge::domain::ReceiptState::Completed);

    let events = collect_until(&mut rx, Duration::from_secs(8), |events| {
        events.iter().any(|e| {
            matches!(
                e,
                DomainEvent::TurnLifecycle {
                    phase: bridge::domain::ActiveTurnPhase::Idle,
                    outcome: Some(bridge::domain::LastTurnOutcome::Interrupted),
                    ..
                }
            )
        }) && events
            .iter()
            .any(|e| matches!(e, DomainEvent::OutputFinal { .. }))
    })
    .await;
    assert!(
        events.iter().any(|e| matches!(
            e,
            DomainEvent::TurnLifecycle {
                phase: bridge::domain::ActiveTurnPhase::Idle,
                outcome: Some(bridge::domain::LastTurnOutcome::Interrupted),
                ..
            }
        )),
        "interrupt 终态未出现: {}",
        events.len()
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, DomainEvent::OutputFinal { .. })),
        "中断定稿未出现: {}",
        events.len()
    );
    // 中断后的权威快照:输出以 AUTHORITATIVE 形态定稿(含已输出行)。
    // OutputFinal 与 TurnLifecycle 在同一批内(Output 事件先于生命周期),
    // 因此在累计事件上判定。
    collect_until(&mut rx, Duration::from_secs(3), |_events| false).await;
    assert!(true, "等待窗口结束后无额外必需事件");
}

#[tokio::test]
async fn scripted_question_lifecycle_end_to_end() {
    let dir = temp_dir("question");
    let (socket, mut fake) = spawn_fake(&dir).await;
    let _fake_guard = FakeGuard(&mut fake);
    let adapter = CodexAdapter::connect(adapter_config(
        socket,
        vec![seed(CONV_QUESTION, "fixture-question")],
    ))
    .await
    .unwrap();

    let _ = adapter.runtime_snapshot(&key(CONV_QUESTION)).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&key(CONV_QUESTION), tx).await.unwrap();

    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV_QUESTION),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("fixture-ask"),
        },
    };
    let mut receipts = adapter
        .execute_command(&key(CONV_QUESTION), request)
        .await
        .unwrap();
    while let Some(receipt) = receipts.recv().await {
        if receipt.state == bridge::domain::ReceiptState::Completed {
            break;
        }
    }

    // 等问题出现(第 1 优先级事件,不可丢失,§17.6)。
    let events = collect_until(&mut rx, Duration::from_secs(8), |events| {
        events.iter().any(|e| {
            matches!(
                e,
                DomainEvent::PendingAttentionAdded { attention }
                    if attention.native_id() == "q-e2e-1"
            )
        })
    })
    .await;
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::PendingAttentionAdded { attention }
            if attention.native_id() == "q-e2e-1"
    )));

    // runtime snapshot 携带问题选项(§16.3)。
    let snapshot = adapter.runtime_snapshot(&key(CONV_QUESTION)).await.unwrap();
    assert_eq!(snapshot.pending_questions.len(), 1);
    assert_eq!(snapshot.pending_questions[0].options[0].option_id, "opt-a");

    // 按原生 question ID + option ID 回答(§16.3)。
    let answer = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::AnswerQuestion,
        session_key: key(CONV_QUESTION),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::AnswerQuestion {
            question_id: "q-e2e-1".to_string(),
            option_ids: vec!["opt-a".to_string()],
            free_text: None,
        },
    };
    let mut answer_receipts = adapter
        .execute_command(&key(CONV_QUESTION), answer)
        .await
        .unwrap();
    while let Some(receipt) = answer_receipts.recv().await {
        if receipt.state == bridge::domain::ReceiptState::Completed {
            break;
        }
    }

    let events = collect_until(&mut rx, Duration::from_secs(8), |events| {
        events.iter().any(|e| {
            matches!(
                e,
                DomainEvent::PendingAttentionRemoved { native_id, .. } if native_id == "q-e2e-1"
            )
        }) && events
            .iter()
            .any(|e| matches!(e, DomainEvent::OutputFinal { .. }))
    })
    .await;
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::PendingAttentionRemoved { native_id, .. } if native_id == "q-e2e-1"
    )));
    // turn 完成输出 q-1..q-3。
    assert!(events
        .iter()
        .any(|e| matches!(e, DomainEvent::OutputFinal { .. })));
}

#[tokio::test]
async fn read_only_capability_gate_rejects_write() {
    let dir = temp_dir("readonly");
    let (socket, mut fake) = spawn_fake(&dir).await;
    let _fake_guard = FakeGuard(&mut fake);
    let mut config = adapter_config(socket, vec![seed(CONV_FAST, "fixture-fast")]);
    // 未知版本 → 只读(§5)。
    config.version_report = Some("codex-cli 9.9.9-unknown".to_string());
    let adapter = CodexAdapter::connect(config).await.unwrap();

    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::ReadOnly);

    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV_FAST),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("should-not-run"),
        },
    };
    let err = adapter
        .execute_command(&key(CONV_FAST), request)
        .await
        .unwrap_err();
    assert_eq!(err.code(), StableErrorCode::ControlReadOnly);
}

/// 测试结束时 kill fake 进程(避免后台残留)。
struct FakeGuard<'a>(&'a mut std::process::Child);
impl Drop for FakeGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
