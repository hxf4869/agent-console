//! PowerCoordinator 与上传生命周期钩子集成测试(权威规格 §19/§22.5):
//! turn 活跃或存在 pending attention → hold;结束且无 attention → release;
//! turn 终态触发上传过期清理并写清理记录。

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use parking_lot::Mutex;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::upload_lifecycle::finished_reason;
use bridge::commands::{PowerCoordinator, UploadCleaner, UploadLifecycle};
use bridge::domain::{
    CommandPayload, CommandRequest, LastTurnOutcome, Operation, OutputText, SessionKey,
};
use bridge::local_store::LocalStore;
use bridge::power::{PowerSource, WakePolicy};
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-cmd-pu";
const CONV_TURN: &str = "ffffffff-1111-4111-8111-111111111111";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-cmd-pu-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn wait_until(pred: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while !pred() {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

// ---------------------------------------------------------------------------
// PowerCoordinator
// ---------------------------------------------------------------------------

/// fake caffeinate:驻留进程(exec sleep),供断言生命周期验证。
fn write_staying_program(dir: &PathBuf) -> PathBuf {
    let path = dir.join("fake-caffeinate");
    std::fs::write(&path, "#!/bin/sh\nexec sleep 60\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[tokio::test]
async fn power_coordinator_holds_on_activity_releases_on_idle() {
    let dir = temp_dir("power");
    let program = write_staying_program(&dir);
    let policy = Arc::new(WakePolicy::with_program(program));
    let coordinator = PowerCoordinator::new(Arc::clone(&policy), PowerSource::Battery);
    assert!(!coordinator.assertion_held(), "初始空闲不持有断言");

    // 电池 + turn 活跃 → hold。
    coordinator.set_turn_active(true).await.unwrap();
    assert!(
        wait_until(|| coordinator.assertion_held(), Duration::from_secs(3)).await,
        "turn 活跃应持有断言"
    );

    // turn 结束且无 attention → release。
    coordinator.set_turn_active(false).await.unwrap();
    assert!(
        wait_until(|| !coordinator.assertion_held(), Duration::from_secs(3)).await,
        "turn 结束应释放断言"
    );

    // 电池 + pending attention → hold;解除 → release。
    coordinator.set_attention_pending(true).await.unwrap();
    assert!(
        wait_until(|| coordinator.assertion_held(), Duration::from_secs(3)).await,
        "pending attention 应持有断言"
    );
    coordinator.set_attention_pending(false).await.unwrap();
    assert!(
        wait_until(|| !coordinator.assertion_held(), Duration::from_secs(3)).await,
        "attention 解除应释放断言"
    );

    // 电源来源切换由外部传入:AC 下即使空闲也持有(§19)。
    coordinator.set_power_source(PowerSource::Ac).await.unwrap();
    assert!(
        wait_until(|| coordinator.assertion_held(), Duration::from_secs(3)).await,
        "AC 供电应恒持有断言"
    );
    coordinator.release().await;
    assert!(!coordinator.assertion_held());
}

// ---------------------------------------------------------------------------
// UploadLifecycle
// ---------------------------------------------------------------------------

/// fake 清理入口:记录调用并返回固定清理结果(解耦 files::upload)。
#[derive(Default)]
struct FakeCleaner {
    sweep_calls: AtomicUsize,
    last_cutoff: Mutex<Option<SystemTime>>,
    removed: Mutex<Vec<String>>,
}

#[async_trait]
impl UploadCleaner for FakeCleaner {
    async fn remove_upload_dir(&self, directory: &str) -> std::io::Result<()> {
        self.removed.lock().push(directory.to_string());
        Ok(())
    }

    async fn sweep_expired(&self, older_than: SystemTime) -> std::io::Result<Vec<String>> {
        self.sweep_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_cutoff.lock() = Some(older_than);
        Ok(vec!["upload-finished".to_string()])
    }
}

#[tokio::test]
async fn turn_finished_triggers_upload_sweep_with_record() {
    let store = LocalStore::open(&temp_dir("store")).await.unwrap();
    let cleaner = Arc::new(FakeCleaner::default());
    let lifecycle =
        UploadLifecycle::with_ttl(cleaner.clone(), store.clone(), Duration::from_secs(3600));

    // 直接触发:failed 终态 → sweep + 清理记录(reason 入库)。
    let removed = lifecycle
        .on_turn_finished(finished_reason(LastTurnOutcome::Failed))
        .await;
    assert_eq!(removed, vec!["upload-finished".to_string()]);
    assert_eq!(cleaner.sweep_calls.load(Ordering::SeqCst), 1);
    let cutoff = cleaner.last_cutoff.lock().expect("cutoff recorded");
    let now = SystemTime::now();
    assert!(cutoff <= now, "cutoff 应为 now - ttl");
    let records = store.list_upload_cleanups().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].directory, "upload-finished");
    assert_eq!(records[0].reason, "turn_failed");

    // 显式删除一次上传目录:入口调用 + 记录。
    lifecycle
        .remove_upload("upload-cancelled", "user_cancelled")
        .await
        .unwrap();
    assert_eq!(
        *cleaner.removed.lock(),
        vec!["upload-cancelled".to_string()]
    );
    let records = store.list_upload_cleanups().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].reason, "user_cancelled");
}

#[tokio::test]
async fn watch_session_sweeps_on_turn_terminal_event() {
    // fake owner + adapter:turn 完成后 Idle 终态事件 → 清理被触发一次。
    let dir = temp_dir("watch");
    let socket = PathBuf::from(format!(
        "/tmp/ac-cmd-pu-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = json!({
        "sessions": [{
            "conversationId": CONV_TURN,
            "title": "fixture-turn",
            "cwd": "/tmp/fixture-turn",
            "model": "gpt-5.3-fixture",
            "turn": {"outputLines": ["t-1", "t-2"], "lineDelayMs": 40}
        }]
    });
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
    let _guard = Guard(&mut child);
    for _ in 0..250 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists());

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
    config.static_sessions = vec![CatalogThread {
        id: CONV_TURN.to_string(),
        title: Some("fixture-turn".to_string()),
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
    let cleaner = Arc::new(FakeCleaner::default());
    let lifecycle = Arc::new(UploadLifecycle::new(cleaner.clone(), store.clone()));

    let session = SessionKey::codex(DEVICE, CONV_TURN);
    lifecycle.watch_session(&adapter, &session).await.unwrap();

    // 启动并等待 turn 完成。
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: session.clone(),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("fixture-run"),
        },
    };
    let mut receipts = adapter.execute_command(&session, request).await.unwrap();
    while let Some(receipt) = receipts.recv().await {
        if receipt.state == bridge::domain::ReceiptState::Completed {
            break;
        }
    }

    assert!(
        wait_until(
            || cleaner.sweep_calls.load(Ordering::SeqCst) > 0,
            Duration::from_secs(10)
        )
        .await,
        "turn 终态应触发上传清理"
    );
    let records = store.list_upload_cleanups().await.unwrap();
    assert_eq!(records[0].reason, "turn_completed");
}

struct Guard<'a>(&'a mut std::process::Child);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
