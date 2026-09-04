//! CommandGateway 集成测试(权威规格 §15.1/§15.2/§26.4/§29.1):
//! request_id 重试重放不重复执行、同 ID 不同 digest →
//! DUPLICATE_REQUEST_MISMATCH、stale revision/turn → STALE_TURN、
//! READ_ONLY gate、回执透传落库、shutdown 语义。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::{CommandGateway, StoredReceiptState, Submission};
use bridge::domain::{
    CommandPayload, CommandReceipt, CommandRequest, Operation, OutputText, ReceiptState,
    SessionKey, StableErrorCode, TurnId,
};
use bridge::local_store::LocalStore;
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-cmd-gw";
const CONV_RETRY: &str = "bbbbbbbb-1111-4111-8111-111111111111";
const CONV_STALE: &str = "bbbbbbbb-2222-4222-8222-222222222222";
const CONV_RO: &str = "bbbbbbbb-3333-4333-8333-333333333333";
const CONV_HELD: &str = "bbbbbbbb-4444-4444-8444-444444444444";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-cmd-gw-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_script(dir: &PathBuf, conversations: &[&str]) -> PathBuf {
    let sessions: Vec<_> = conversations
        .iter()
        .map(|c| {
            json!({
                "conversationId": c,
                "title": format!("fixture-{c}"),
                "cwd": format!("/tmp/fixture-{c}"),
                "model": "gpt-5.3-fixture",
                "turn": {
                    "outputLines": ["1", "2", "3"],
                    "lineDelayMs": 80
                }
            })
        })
        .collect();
    let script = json!({ "sessions": sessions });
    let path = dir.join("script.json");
    std::fs::write(&path, serde_json::to_string(&script).unwrap()).unwrap();
    path
}

async fn spawn_fake(
    dir: &PathBuf,
    conversations: &[&str],
) -> (PathBuf, PathBuf, std::process::Child) {
    // macOS sun_path 上限:socket 用短路径;stderr 落 log 供执行次数断言。
    let socket = PathBuf::from(format!(
        "/tmp/ac-cmd-gw-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = write_script(dir, conversations);
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

fn start_turn_request(session: &SessionKey, request_id: Uuid, input: &str) -> CommandRequest {
    CommandRequest {
        request_id,
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

async fn drain_receipts(mut receipts: mpsc::Receiver<CommandReceipt>) -> Vec<ReceiptState> {
    let mut states = Vec::new();
    while let Some(receipt) = receipts.recv().await {
        let terminal = matches!(
            receipt.state,
            ReceiptState::Completed | ReceiptState::Rejected { .. } | ReceiptState::OutcomeUnknown
        );
        states.push(receipt.state);
        if terminal {
            break;
        }
    }
    states
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
async fn same_request_id_replays_without_reexecution() {
    let dir = temp_dir("replay");
    let (socket, log_path, fake) = spawn_fake(&dir, &[CONV_RETRY]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_RETRY]))
        .await
        .unwrap();
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let session = key(CONV_RETRY);

    // 首次提交:ACCEPTED → DispatchedToCodex → Completed,落库 COMPLETED。
    let request_id = Uuid::new_v4();
    let first = gateway
        .submit(start_turn_request(&session, request_id, "fixture-input"))
        .await
        .unwrap();
    assert!(first.is_accepted(), "首次提交应为 Accepted");
    let states = drain_receipts(first.into_receipts().unwrap()).await;
    assert_eq!(
        states,
        vec![ReceiptState::DispatchedToCodex, ReceiptState::Completed]
    );
    let stored = store
        .get_receipt(&request_id.to_string())
        .await
        .unwrap()
        .expect("receipt persisted");
    assert_eq!(stored.status, "COMPLETED");

    // 同 ID 同 payload 重试:重放既有回执,fake owner 只执行一次。
    let replay = gateway
        .submit(start_turn_request(&session, request_id, "fixture-input"))
        .await
        .unwrap();
    assert_eq!(
        replay,
        Submission::Replayed {
            request_id,
            state: StoredReceiptState::Completed
        }
    );

    // 进行中的重试(不等回执)同样只重放,不重复执行。
    let second_id = Uuid::new_v4();
    let second = gateway
        .submit(start_turn_request(&session, second_id, "fixture-input-2"))
        .await
        .unwrap();
    assert!(second.is_accepted());
    let second_replay = gateway
        .submit(start_turn_request(&session, second_id, "fixture-input-2"))
        .await
        .unwrap();
    assert!(!second_replay.is_accepted(), "进行中重试应重放而非再次执行");
    let _ = drain_receipts(second.into_receipts().unwrap()).await;

    // 同 ID 不同 payload → DUPLICATE_REQUEST_MISMATCH。
    let mismatch = gateway
        .submit(start_turn_request(
            &session,
            request_id,
            "different-payload",
        ))
        .await
        .unwrap_err();
    assert_eq!(mismatch.code, StableErrorCode::DuplicateRequestMismatch);

    // fake owner 只看到两次 start_turn(request_id 各一次)。
    assert_eq!(
        count_start_turns(&log_path),
        2,
        "每个 request_id 只执行一次"
    );
}

#[tokio::test]
async fn stale_expected_revision_or_turn_is_rejected() {
    let dir = temp_dir("stale");
    let (socket, _log, fake) = spawn_fake(&dir, &[CONV_STALE]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_STALE]))
        .await
        .unwrap();
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let session = key(CONV_STALE);

    let snapshot = adapter.runtime_snapshot(&session).await.unwrap();
    let wrong_revision = snapshot.runtime_revision.wrapping_add(1_000);

    // expected revision 不符 → ACCEPTED 后 REJECTED{STALE_TURN}(§15.2);
    // 拒绝状态落库,重试同 ID 返回 REJECTED 重放,不重复执行。
    let stale_id = Uuid::new_v4();
    let mut request = start_turn_request(&session, stale_id, "fixture-stale");
    request.expected_runtime_revision = Some(wrong_revision);
    let submission = gateway.submit(request.clone()).await.unwrap();
    assert!(submission.is_accepted(), "先接受再拒绝");
    let states = drain_receipts(submission.into_receipts().unwrap()).await;
    assert!(
        matches!(
            &states[..],
            [ReceiptState::Rejected {
                code: StableErrorCode::StaleTurn,
                ..
            }]
        ),
        "期望单个 STALE_TURN 拒绝回执,实际 {states:?}"
    );
    assert_eq!(
        store
            .get_receipt(&stale_id.to_string())
            .await
            .unwrap()
            .expect("receipt persisted")
            .status,
        "REJECTED"
    );
    assert_eq!(
        gateway.submit(request).await.unwrap(),
        Submission::Replayed {
            request_id: stale_id,
            state: StoredReceiptState::Rejected
        }
    );

    // expected turn 不符 → STALE_TURN。
    let mut request = start_turn_request(&session, Uuid::new_v4(), "fixture-stale-2");
    request.expected_turn_id = Some(TurnId::native("turn-does-not-exist"));
    let submission = gateway.submit(request).await.unwrap();
    let states = drain_receipts(submission.into_receipts().unwrap()).await;
    assert!(matches!(
        &states[..],
        [ReceiptState::Rejected {
            code: StableErrorCode::StaleTurn,
            ..
        }]
    ));

    assert_eq!(count_start_turns(&_log), 0, "被拒命令不得下发到 Desktop");
}

#[tokio::test]
async fn read_only_capability_gate_rejects_submission() {
    let dir = temp_dir("readonly");
    let (socket, _log, fake) = spawn_fake(&dir, &[CONV_RO]).await;
    let _guard = FakeGuard(fake);
    let mut config = adapter_config(socket, &[CONV_RO]);
    config.version_report = Some("codex-cli 9.9.9-unknown".to_string());
    let adapter = CodexAdapter::connect(config).await.unwrap();
    let gateway = CommandGateway::new(adapter, LocalStore::open(&temp_dir("store")).await.unwrap());

    let err = gateway
        .submit(start_turn_request(&key(CONV_RO), Uuid::new_v4(), "nope"))
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::ControlReadOnly);
}

// ---------------------------------------------------------------------------
// shutdown 语义(§26.4):停止接受新命令;在途命令补 OUTCOME_UNKNOWN 终态。
// ---------------------------------------------------------------------------

/// 测试执行器:回执流由测试控制,用于确定性构造"进行中"命令。
struct HeldExecutor {
    sender: Mutex<Option<mpsc::Sender<CommandReceipt>>>,
}

impl HeldExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            sender: Mutex::new(None),
        })
    }
}

#[async_trait]
impl bridge::commands::CommandExecutor for HeldExecutor {
    async fn execute_command(
        &self,
        _key: &SessionKey,
        request: CommandRequest,
    ) -> Result<mpsc::Receiver<CommandReceipt>, bridge::adapter::codex::AdapterError> {
        let (tx, rx) = mpsc::channel(8);
        *self.sender.lock() = Some(tx.clone());
        let _ = tx
            .send(CommandReceipt {
                request_id: request.request_id,
                state: ReceiptState::DispatchedToCodex,
                at: None,
            })
            .await;
        Ok(rx)
    }
}

#[tokio::test]
async fn graceful_shutdown_finalizes_inflight_and_rejects_new() {
    let dir = temp_dir("shutdown");
    let (socket, _log, fake) = spawn_fake(&dir, &[CONV_HELD]).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect(adapter_config(socket, &[CONV_HELD]))
        .await
        .unwrap();
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let held = HeldExecutor::new();
    let gateway = CommandGateway::with_executor(adapter, store.clone(), held.clone());
    let session = key(CONV_HELD);

    let request_id = Uuid::new_v4();
    let submission = gateway
        .submit(start_turn_request(&session, request_id, "fixture-held"))
        .await
        .unwrap();
    let mut receipts = submission.into_receipts().unwrap();
    let first = receipts.recv().await.expect("dispatched receipt");
    assert_eq!(first.state, ReceiptState::DispatchedToCodex);

    // 停机:在途命令补 OUTCOME_UNKNOWN 并落库。
    assert!(!gateway.is_closed());
    let finalized = gateway.graceful_shutdown().await;
    assert_eq!(finalized, vec![request_id]);
    assert!(gateway.is_closed());

    let receipt = receipts.recv().await.expect("finalized receipt");
    assert_eq!(receipt.state, ReceiptState::OutcomeUnknown);
    let stored = store
        .get_receipt(&request_id.to_string())
        .await
        .unwrap()
        .expect("receipt persisted");
    assert_eq!(stored.status, "OUTCOME_UNKNOWN");

    // 停机后拒绝新命令。
    let err = gateway
        .submit(start_turn_request(
            &session,
            Uuid::new_v4(),
            "after-shutdown",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code, StableErrorCode::DeviceOffline);

    // 释放测试执行器:转发流无终态结束 → 不得 panic、不改变已落库终态。
    *held.sender.lock() = None;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stored = store
        .get_receipt(&request_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, "OUTCOME_UNKNOWN");
}
