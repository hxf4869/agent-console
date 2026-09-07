//! 单条下一轮队列集成测试(权威规格 §15.3/§29.1):替换/取消/
//! QUEUE_ALREADY_EXISTS/QUEUE_PAUSED、turn 正常完成+无 attention → 自动发送
//! 并清空(final 校正之后)、turn failed → PAUSED、用户抢先新 turn → PAUSED、
//! PAUSED 后 resume 需显式、设备离线时 set 拒绝。

use std::path::PathBuf;
use std::time::Duration;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::{CommandGateway, Submission};
use bridge::domain::{
    CommandPayload, CommandRequest, DomainEvent, Operation, OutputText, QueueState, SessionKey,
    StableErrorCode,
};
use bridge::local_store::LocalStore;
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-cmd-q";
const CONV_OK: &str = "cccccccc-1111-4111-8111-111111111111";
const CONV_FAIL: &str = "cccccccc-2222-4222-8222-222222222222";
const CONV_RESUME: &str = "cccccccc-3333-4333-8333-333333333333";
const CONV_PREEMPT: &str = "cccccccc-4444-4444-8444-444444444444";
const CONV_OFFLINE: &str = "cccccccc-5555-4555-8555-555555555555";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-cmd-q-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn session_node(conversation: &str, fail: bool, line_delay: u64) -> serde_json::Value {
    json!({
        "conversationId": conversation,
        "title": format!("fixture-{conversation}"),
        "cwd": format!("/tmp/fixture-{conversation}"),
        "model": "gpt-5.3-fixture",
        "turn": {
            "outputLines": ["1", "2", "3", "4", "5"],
            "lineDelayMs": line_delay,
            "fail": fail,
            // 权威输出与预览不同 → OutputReplace 校正(§13.3)。
            "finalOutput": "corrected-authoritative-output\n"
        }
    })
}

fn write_script(dir: &PathBuf, nodes: &[serde_json::Value]) -> PathBuf {
    let script = json!({ "sessions": nodes });
    let path = dir.join("script.json");
    std::fs::write(&path, serde_json::to_string(&script).unwrap()).unwrap();
    path
}

/// 返回 (socket, log, child);等待 socket 就绪。
async fn spawn_fake(
    dir: &PathBuf,
    nodes: &[serde_json::Value],
) -> (PathBuf, PathBuf, std::process::Child) {
    let socket = PathBuf::from(format!(
        "/tmp/ac-cmd-q-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = write_script(dir, nodes);
    let log_path = dir.join("fake-owner.log");
    let stderr_log = std::fs::File::create(&log_path).expect("log file");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fake-codex-owner"))
        .arg(socket.clone())
        .arg(script)
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
    (socket, log_path, child)
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

fn start_turn_request(session: &SessionKey, input: &str) -> CommandRequest {
    CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: session.clone(),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new(input),
        },
    }
}

fn queue_set_request(
    session: &SessionKey,
    request_id: Uuid,
    input: &str,
    replace: bool,
) -> CommandRequest {
    CommandRequest {
        request_id,
        operation: Operation::QueueNextTurn,
        session_key: session.clone(),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::QueueNextTurn {
            input: OutputText::new(input),
            replace,
        },
    }
}

async fn drain(mut receipts: tokio::sync::mpsc::Receiver<bridge::domain::CommandReceipt>) {
    while let Some(receipt) = receipts.recv().await {
        if matches!(
            receipt.state,
            bridge::domain::ReceiptState::Completed
                | bridge::domain::ReceiptState::Rejected { .. }
                | bridge::domain::ReceiptState::OutcomeUnknown
        ) {
            break;
        }
    }
}

async fn drain_states(
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

fn count_start_turns(log_path: &PathBuf) -> usize {
    std::fs::read_to_string(log_path)
        .unwrap_or_default()
        .lines()
        .filter(|l| l.contains("request thread-follower-start-turn"))
        .count()
}

struct FakeGuard(std::process::Child);
impl Drop for FakeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn queue_set_replace_cancel_and_duplicate_semantics() {
    let dir = temp_dir("lifecycle");
    let (socket, _log, fake) = spawn_fake(
        &dir,
        &[
            session_node(CONV_OK, false, 80),
            session_node(CONV_FAIL, true, 30),
        ],
    )
    .await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_OK, CONV_FAIL]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_OK);
    let queue = gateway.queue();

    // 空队列状态。
    assert_eq!(queue.get(&session).await.unwrap(), None);
    let status = queue.queue_status(&session).await.unwrap();
    assert_eq!(status.state, QueueState::Empty);

    // 排队(经 gateway 命令管线:去重 + ACCEPTED → Completed)。
    let request_id = Uuid::new_v4();
    let submission = gateway
        .submit(queue_set_request(&session, request_id, "queued-body-1", false))
        .await
        .unwrap();
    assert!(submission.is_accepted());
    drain(submission.into_receipts().unwrap()).await;
    let queued = queue.get(&session).await.unwrap().expect("queue entry");
    assert_eq!(queued.input.as_str(), "queued-body-1");
    assert_eq!(queued.state, QueueState::Queued);
    // idle 排队:after_turn_id 为 idle 标记。
    assert_eq!(queued.after_turn_id.id, "idle");

    // 重复排队 → ACCEPTED 后回执流给出 REJECTED{QUEUE_ALREADY_EXISTS};
    // 重复 request_id → 重放。
    let dup = gateway
        .submit(queue_set_request(&session, Uuid::new_v4(), "queued-body-2", false))
        .await
        .unwrap();
    let dup_states = drain_states(dup.into_receipts().unwrap()).await;
    assert!(
        matches!(
            &dup_states[..],
            [bridge::domain::ReceiptState::Rejected {
                code: StableErrorCode::QueueAlreadyExists,
                ..
            }]
        ),
        "实际 {dup_states:?}"
    );
    let replay = gateway
        .submit(queue_set_request(&session, request_id, "queued-body-1", false))
        .await
        .unwrap();
    assert!(matches!(replay, Submission::Replayed { .. }));

    // 替换:正文与绑定 revision 更新。
    let replaced = queue
        .replace(&session, OutputText::new("queued-body-3"), Uuid::new_v4())
        .await
        .unwrap();
    assert_eq!(replaced.input.as_str(), "queued-body-3");
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "queued-body-3"
    );

    // 取消;幂等。
    assert!(queue.cancel(&session).await.unwrap());
    assert_eq!(queue.get(&session).await.unwrap(), None);
    assert!(!queue.cancel(&session).await.unwrap());
}

#[tokio::test]
async fn completed_turn_auto_sends_queue_after_final_correction() {
    let dir = temp_dir("autosend");
    let (socket, log_path, fake) = spawn_fake(&dir, &[session_node(CONV_OK, false, 120)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_OK]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_OK);
    let queue = gateway.queue();

    // 观察事件(校正 vs 抢跑 turn 的顺序断言)。
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&session, tx).await.unwrap();

    // turn-1 开始。
    drain(
        gateway
            .submit(start_turn_request(&session, "first-turn"))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    let running = collect_until(&mut rx, Duration::from_secs(10), |events| {
        events.iter().any(|e| matches!(
            e,
            DomainEvent::TurnLifecycle { phase: bridge::domain::ActiveTurnPhase::Running, turn, .. }
                if turn.id == "turn-1"
        ))
    })
    .await;
    assert!(!running.is_empty(), "turn-1 Running 未出现");

    // turn-1 运行中排队:绑定 turn-1 与当前 revision。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    let queued = queue
        .set(
            &session,
            OutputText::new("queued-next"),
            false,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap();
    assert_eq!(queued.after_turn_id.id, "turn-1");
    assert_eq!(queued.state, QueueState::Queued);

    // turn-1 完成(含权威校正)→ 自动发送 turn-2 → 队列清空。
    let mut cleared = false;
    for _ in 0..300 {
        if queue.get(&session).await.unwrap().is_none() {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(cleared, "turn 完成后队列应被自动发送并清空");

    // 顺序断言:OutputReplace(权威校正)先于 Idle(Completed),再先于
    // 自动发送的 turn-2 Running(§15.3:校正完成才算 COMPLETED)。
    let events = collect_until(&mut rx, Duration::from_secs(10), |events| {
        events.iter().any(|e| matches!(
            e,
            DomainEvent::TurnLifecycle { phase: bridge::domain::ActiveTurnPhase::Running, turn, .. }
                if turn.id == "turn-2"
        ))
    })
    .await;
    let combined: Vec<&DomainEvent> = running.iter().chain(events.iter()).collect();
    let idx_replace = combined.iter().position(|e| matches!(
        e,
        DomainEvent::OutputReplace { bytes, .. }
            if std::str::from_utf8(bytes.as_bytes()).unwrap() == "corrected-authoritative-output\n"
    ));
    let idx_idle = combined.iter().position(|e| {
        matches!(
            e,
            DomainEvent::TurnLifecycle {
                phase: bridge::domain::ActiveTurnPhase::Idle,
                outcome: Some(bridge::domain::LastTurnOutcome::Completed),
                ..
            }
        )
    });
    let idx_turn2 = combined.iter().position(|e| {
        matches!(
            e,
            DomainEvent::TurnLifecycle { phase: bridge::domain::ActiveTurnPhase::Running, turn, .. }
                if turn.id == "turn-2"
        )
    });
    assert!(idx_replace.is_some(), "缺少权威校正事件: {combined:?}");
    assert!(idx_idle.is_some(), "缺少 turn-1 终态事件");
    assert!(idx_turn2.is_some(), "自动发送的 turn-2 未出现");
    assert!(
        idx_replace.unwrap() < idx_idle.unwrap() && idx_idle.unwrap() < idx_turn2.unwrap(),
        "自动发送必须在权威校正与 COMPLETED 之后: replace={idx_replace:?} idle={idx_idle:?} turn2={idx_turn2:?}"
    );
    assert_eq!(count_start_turns(&log_path), 2, "首 turn + 队列自动发送");
}

#[tokio::test]
async fn failed_turn_pauses_queue_without_autosend() {
    let dir = temp_dir("paused");
    let (socket, log_path, fake) = spawn_fake(&dir, &[session_node(CONV_FAIL, true, 40)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_FAIL]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_FAIL);
    let queue = gateway.queue();

    drain(
        gateway
            .submit(start_turn_request(&session, "doomed-turn"))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    // 等 Running 出现后排队。
    let mut running_seen = false;
    for _ in 0..100 {
        let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
        if snapshot.current_turn.is_some() {
            running_seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(running_seen, "turn 未进入 Running");

    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    queue
        .set(
            &session,
            OutputText::new("paused-body"),
            false,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap();

    // turn 失败 → PAUSED。
    let mut paused = false;
    for _ in 0..200 {
        if queue.queue_status(&session).await.unwrap().state == QueueState::Paused {
            paused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(paused, "turn 失败后队列应为 PAUSED");

    // 失败后等待一段时间:无自动发送(仍 PAUSED)。
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        queue.queue_status(&session).await.unwrap().state,
        QueueState::Paused
    );
    assert_eq!(count_start_turns(&log_path), 1, "失败不得触发自动发送");
}

#[tokio::test]
async fn paused_queue_requires_explicit_resume_then_autosends() {
    let dir = temp_dir("resume");
    let (socket, log_path, fake) = spawn_fake(&dir, &[session_node(CONV_RESUME, true, 40)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_RESUME]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_RESUME);
    let queue = gateway.queue();

    // 第一轮失败 → PAUSED。
    drain(
        gateway
            .submit(start_turn_request(&session, "failing-first"))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    for _ in 0..100 {
        if adapter
            .runtime_snapshot(&session)
            .await
            .unwrap()
            .current_turn
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    queue
        .set(
            &session,
            OutputText::new("resume-body"),
            false,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap();
    for _ in 0..200 {
        if queue.queue_status(&session).await.unwrap().state == QueueState::Paused {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        queue.queue_status(&session).await.unwrap().state,
        QueueState::Paused
    );

    // PAUSED 下 set(不 replace)→ QUEUE_PAUSED。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    let err = queue
        .set(
            &session,
            OutputText::new("another"),
            false,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::QueuePaused);

    // resume:显式重新确认 → 重新绑定 → QUEUED → 空闲且无 attention → 自动发送。
    let resumed = queue
        .resume(&session)
        .await
        .unwrap()
        .expect("resumed entry");
    assert_eq!(resumed.state, QueueState::Queued);
    let mut cleared = false;
    for _ in 0..200 {
        if queue.get(&session).await.unwrap().is_none() {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(cleared, "resume 后队列应自动发送并清空");
    assert_eq!(count_start_turns(&log_path), 2, "首 turn + resume 自动发送");

    // 无条目时 resume → None。
    assert!(queue.resume(&session).await.unwrap().is_none());
}

#[tokio::test]
async fn foreign_new_turn_pauses_queue() {
    let dir = temp_dir("preempt");
    let (socket, _log, fake) = spawn_fake(&dir, &[session_node(CONV_PREEMPT, false, 300)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_PREEMPT]))
        .await
        .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_PREEMPT);
    let queue = gateway.queue();

    // idle 时排队(绑定 idle 标记)。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    queue
        .set(
            &session,
            OutputText::new("preempt-body"),
            false,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap();

    // 非队列来源的新 turn(用户/桌面抢先)→ PAUSED。
    drain(
        gateway
            .submit(start_turn_request(&session, "user-started"))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    let mut paused = false;
    for _ in 0..200 {
        if queue.queue_status(&session).await.unwrap().state == QueueState::Paused {
            paused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(paused, "Desktop 抢先开始新 turn 后队列应 PAUSED");
}

#[tokio::test]
async fn offline_device_rejects_queue_set() {
    // 无 IPC:adapter 降级 catalog-only,快照不可得。
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(
        std::env::temp_dir().join(format!("ac-cmd-q-missing-{}.sock", Uuid::new_v4().simple())),
    );
    config.codex_home = Some(temp_dir("empty-home"));
    config.static_sessions = vec![CatalogThread {
        id: CONV_OFFLINE.to_string(),
        title: None,
        project_display_name: None,
        model: None,
        reasoning_effort: None,
        git_branch: None,
        created_at: None,
        updated_at: None,
        archived: false,
        agent_nickname: None,
        agent_role: None,
    }];
    let adapter = CodexAdapter::connect(config).await.unwrap();
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let gateway = CommandGateway::new(adapter, store.clone());
    let session = key(CONV_OFFLINE);

    // 经 gateway 命令管线:快照不可得 → ACCEPTED 后回执 REJECTED{CODEX_UNAVAILABLE}。
    let submission = gateway
        .submit(queue_set_request(&session, Uuid::new_v4(), "offline-body", false))
        .await
        .unwrap();
    let states = drain_states(submission.into_receipts().unwrap()).await;
    assert!(
        matches!(
            &states[..],
            [bridge::domain::ReceiptState::Rejected {
                code: StableErrorCode::CodexUnavailable,
                ..
            }]
        ),
        "实际 {states:?}"
    );
    assert_eq!(gateway.queue().get(&session).await.unwrap(), None);

    // 已有队列时同样拒绝且保留旧队列(§15.3:离线禁止创建或替换)。
    let stale = bridge::local_store::NextTurnEntry {
        session: bridge::local_store::SessionKeyRef {
            device_id: DEVICE.to_string(),
            agent_kind: 1,
            native_session_id: CONV_OFFLINE.to_string(),
        },
        prompt: "old-queued-body".to_string(),
        after_turn_id: "turn-old".to_string(),
        runtime_revision: 7,
        status: bridge::local_store::QueueStatus::Queued,
        created_at: "2026-01-01T00:00:00+00:00".to_string(),
        updated_at: "2026-01-01T00:00:00+00:00".to_string(),
    };
    store.set_next_turn(&stale, false).await.unwrap();
    let submission = gateway
        .submit(queue_set_request(&session, Uuid::new_v4(), "offline-body", true))
        .await
        .unwrap();
    let states = drain_states(submission.into_receipts().unwrap()).await;
    assert!(
        matches!(
            &states[..],
            [bridge::domain::ReceiptState::Rejected {
                code: StableErrorCode::CodexUnavailable,
                ..
            }]
        ),
        "实际 {states:?}"
    );
    assert_eq!(
        gateway.queue().get(&session).await.unwrap().unwrap().input,
        bridge::domain::OutputText::new("old-queued-body")
    );
}

/// §15.3 R2-AC02:替换请求经 gateway 全链路,校验不过(过期 revision /
/// 错误 turn)时旧队列必须保留——不再有 runtime 预删除。
#[tokio::test]
async fn replace_via_gateway_keeps_old_queue_when_preconditions_fail() {
    let dir = temp_dir("replace-stale");
    let (socket, _log, fake) = spawn_fake(&dir, &[session_node(CONV_OK, false, 80)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_OK])).await.unwrap();
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let session = key(CONV_OK);
    let queue = gateway.queue();

    // 预置旧队列(经 gateway set)。
    drain(
        gateway
            .submit(queue_set_request(&session, Uuid::new_v4(), "old-body", false))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();

    // 过期 revision → STALE_TURN,旧队列保留。
    let stale_revision = CommandRequest {
        expected_runtime_revision: Some(snapshot.runtime_revision + 100),
        ..queue_set_request(&session, Uuid::new_v4(), "new-body", true)
    };
    let states = drain_states(
        gateway
            .submit(stale_revision)
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    assert!(
        matches!(
            &states[..],
            [bridge::domain::ReceiptState::Rejected {
                code: StableErrorCode::StaleTurn,
                ..
            }]
        ),
        "实际 {states:?}"
    );
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "old-body"
    );

    // 错误 turn → STALE_TURN,旧队列保留。
    let stale_turn = CommandRequest {
        expected_turn_id: Some(bridge::domain::TurnId::native("turn-gone")),
        ..queue_set_request(&session, Uuid::new_v4(), "new-body", true)
    };
    let states = drain_states(
        gateway
            .submit(stale_turn)
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    assert!(
        matches!(
            &states[..],
            [bridge::domain::ReceiptState::Rejected {
                code: StableErrorCode::StaleTurn,
                ..
            }]
        ),
        "实际 {states:?}"
    );
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "old-body"
    );

    // 同一 request_id 断线重连后重试:重放既有回执(REJECTED),不另起新任务。
    let stale_id = Uuid::new_v4();
    let first = CommandRequest {
        expected_runtime_revision: Some(snapshot.runtime_revision + 100),
        ..queue_set_request(&session, stale_id, "new-body", true)
    };
    let states = drain_states(gateway.submit(first).await.unwrap().into_receipts().unwrap()).await;
    assert!(matches!(
        &states[..],
        [bridge::domain::ReceiptState::Rejected {
            code: StableErrorCode::StaleTurn,
            ..
        }]
    ));
    let replay = gateway
        .submit(queue_set_request(&session, stale_id, "new-body", true))
        .await
        .unwrap();
    match replay {
        Submission::Replayed { state, .. } => {
            assert_eq!(state, bridge::commands::StoredReceiptState::Rejected)
        }
        other => panic!("应重放既有回执,实际 {other:?}"),
    }
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "old-body"
    );
}

/// §15.3 R2-AC02:合法替换仅留新队列;同 requestId 重放不重复替换;
/// 不同 payload 复用 requestId 明确拒绝;set/replace 去重摘要可区分。
#[tokio::test]
async fn replace_via_gateway_replay_and_mismatch_semantics() {
    let dir = temp_dir("replace-replay");
    let (socket, _log, fake) = spawn_fake(&dir, &[session_node(CONV_OK, false, 80)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_OK])).await.unwrap();
    let gateway = CommandGateway::new(
        adapter,
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = key(CONV_OK);
    let queue = gateway.queue();

    // 预置旧队列并合法替换:仅留新队列。
    drain(
        gateway
            .submit(queue_set_request(&session, Uuid::new_v4(), "old-body", false))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    let replace_id = Uuid::new_v4();
    let states = drain_states(
        gateway
            .submit(queue_set_request(&session, replace_id, "new-body", true))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    assert!(matches!(
        &states[..],
        [bridge::domain::ReceiptState::Completed]
    ));
    let entry = queue.get(&session).await.unwrap().unwrap();
    assert_eq!(entry.input.as_str(), "new-body");

    // 同 requestId 同 payload 重放:既有回执,队列不再变化。
    let replay = gateway
        .submit(queue_set_request(&session, replace_id, "new-body", true))
        .await
        .unwrap();
    match replay {
        Submission::Replayed { state, .. } => {
            assert_eq!(state, bridge::commands::StoredReceiptState::Completed)
        }
        other => panic!("应重放既有回执,实际 {other:?}"),
    }
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "new-body"
    );

    // 同 requestId 不同 payload:DUPLICATE_REQUEST_MISMATCH,队列保留。
    let err = gateway
        .submit(queue_set_request(&session, replace_id, "yet-another-body", true))
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::DuplicateRequestMismatch);
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "new-body"
    );

    // set 与 replace 的去重摘要可区分:同 requestId 下先 set 再 replace
    // (正文相同)因语义不同被拒,不发生静默替换。
    assert!(queue.cancel(&session).await.unwrap());
    let set_then_replace = Uuid::new_v4();
    let states = drain_states(
        gateway
            .submit(queue_set_request(&session, set_then_replace, "same-body", false))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;
    assert!(matches!(
        &states[..],
        [bridge::domain::ReceiptState::Completed]
    ));
    let err = gateway
        .submit(queue_set_request(&session, set_then_replace, "same-body", true))
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::DuplicateRequestMismatch);
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "same-body"
    );
}

/// §15.3 R2-AC02:存储写失败(并发写锁)→ 拒绝且旧队列保留。
/// 覆盖两条真实失败路径:
/// 1. gateway.submit 内去重落库(INSERT)失败 → Err,未触达队列;
/// 2. QueueManager 直写 set_next_turn 的 UPDATE 重试耗尽失败 →
///    单行事务回滚,旧行保留。
#[tokio::test]
async fn replace_keeps_old_queue_when_store_write_fails() {
    let dir = temp_dir("replace-lock");
    let (socket, _log, fake) = spawn_fake(&dir, &[session_node(CONV_OK, false, 80)]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_OK])).await.unwrap();
    let store_dir = temp_dir("store");
    let store = LocalStore::open(&store_dir).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let session = key(CONV_OK);
    let queue = gateway.queue();

    drain(
        gateway
            .submit(queue_set_request(&session, Uuid::new_v4(), "old-body", false))
            .await
            .unwrap()
            .into_receipts()
            .unwrap(),
    )
    .await;

    // 独立连接持有 SQLite 写锁,使后续写操作失败。
    let lock = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(store_dir.join("bridge.sqlite3"))
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(lock)
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();

    // 路径 1:gateway 去重落库失败 → Err{InternalError};队列未被动过。
    let err = gateway
        .submit(queue_set_request(&session, Uuid::new_v4(), "new-body", true))
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "old-body"
    );

    // 路径 2:QueueManager 直写(绑定快照由调用方提供,不经 gateway)→
    // set_next_turn 在 retry_busy 重试耗尽后失败;旧行保留。
    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    let err = queue
        .set(
            &session,
            OutputText::new("new-body"),
            true,
            Uuid::new_v4(),
            &snapshot,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::InternalError);

    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();
    // 先归还持锁连接再关池:close 会等待所有连接归还。
    drop(conn);
    assert_eq!(
        queue.get(&session).await.unwrap().unwrap().input.as_str(),
        "old-body",
        "存储写失败时旧队列必须保留"
    );
    pool.close().await;
}
