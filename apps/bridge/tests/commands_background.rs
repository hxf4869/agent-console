//! 后台命令集成测试(权威规格 §23.2/§29.1):列表来自 snapshot、
//! 无能力 → CAPABILITY_UNSUPPORTED、仅有全局 clean 能力 →
//! 停止全部带 BACKGROUND_STOP_GLOBAL_ONLY warning。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::background::BACKGROUND_STOP_GLOBAL_ONLY;
use bridge::commands::CommandGateway;
use bridge::domain::{CapabilitySet, Operation, ReceiptState, SessionKey};
use bridge::local_store::LocalStore;
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-cmd-bg";
const CONV_BG: &str = "eeeeeeee-1111-4111-8111-111111111111";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-cmd-bg-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_fake(dir: &PathBuf) -> (PathBuf, std::process::Child) {
    let socket = PathBuf::from(format!(
        "/tmp/ac-cmd-bg-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    let script = json!({
        "sessions": [{
            "conversationId": CONV_BG,
            "title": "fixture-bg",
            "cwd": "/tmp/fixture-bg",
            "model": "gpt-5.3-fixture",
            "turn": {"outputLines": ["b-1"], "lineDelayMs": 30}
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

fn full_control_caps(operations: &[Operation]) -> CapabilitySet {
    CapabilitySet {
        control_mode: bridge::domain::ControlMode::FullControl,
        compatibility_state: bridge::domain::CompatibilityState::Verified,
        codex_version: Some("codex-cli 0.153.0-alpha.5".to_string()),
        supported_operations: operations.to_vec(),
        settings: Vec::new(),
        transfer_limits: Default::default(),
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
async fn background_list_and_capability_gates() {
    let dir = temp_dir("bg");
    let (socket, fake) = spawn_fake(&dir).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect({
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
            id: CONV_BG.to_string(),
            title: Some("fixture-bg".to_string()),
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
        config
    })
    .await
    .unwrap();
    let gateway = CommandGateway::new(
        adapter.clone(),
        LocalStore::open(&temp_dir("store")).await.unwrap(),
    );
    let session = SessionKey::codex(DEVICE, CONV_BG);

    // 列表来自 snapshot(fixed 脚本无后台命令 → 空列表,不猜测)。
    let list = gateway.background_commands(&session).await.unwrap();
    assert!(list.is_empty());

    // 单项停止:probe 不开启该能力 → CAPABILITY_UNSUPPORTED(§23.2)。
    let err = gateway
        .stop_background_command(&session, "cmd-1")
        .await
        .unwrap_err();
    assert_eq!(
        err.code,
        bridge::domain::StableErrorCode::CapabilityUnsupported
    );

    // 全局停止:无任何能力 → CAPABILITY_UNSUPPORTED。
    let err = gateway
        .stop_all_background_commands(&session)
        .await
        .unwrap_err();
    assert_eq!(
        err.code,
        bridge::domain::StableErrorCode::CapabilityUnsupported
    );
}

#[tokio::test]
async fn global_only_capability_carries_warning_and_honest_rejection() {
    let dir = temp_dir("bg-global");
    let (socket, fake) = spawn_fake(&dir).await;
    let _guard = FakeGuard(fake);
    let adapter = CodexAdapter::connect({
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
            id: CONV_BG.to_string(),
            title: Some("fixture-bg".to_string()),
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
        config
    })
    .await
    .unwrap();

    // 注入"仅有全局 clean 能力"的能力来源(§23.2 响应契约)。
    let caps = full_control_caps(&[Operation::StopAllBackgroundCommands]);
    let gateway = CommandGateway::with_capability_provider(
        adapter,
        LocalStore::open(&temp_dir("store")).await.unwrap(),
        Arc::new(move || caps.clone()),
    );
    let session = SessionKey::codex(DEVICE, CONV_BG);

    let outcome = gateway
        .stop_all_background_commands(&session)
        .await
        .unwrap();
    // 无单项能力 → 响应带全局停止 warning(登记常量,§23.2)。
    assert_eq!(outcome.warnings, vec![BACKGROUND_STOP_GLOBAL_ONLY]);

    // v1 无已验证的原生停止方法:adapter 如实回 REJECTED,不伪造成功
    // (本地命令无 DispatchedToCodex 前置回执,单条 Rejected)。
    let mut receipts = outcome.submission.into_receipts().unwrap();
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
    assert!(
        matches!(
            &states[..],
            [ReceiptState::Rejected {
                code: bridge::domain::StableErrorCode::CapabilityUnsupported,
                ..
            }]
        ),
        "实际 {states:?}"
    );
    let _ = Uuid::new_v4();
}
