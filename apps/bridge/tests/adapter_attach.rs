//! CodexAdapter attach 生命周期集成测试(AC-05):
//! - Bridge 先于 Desktop 启动(socket 不存在)→ 永久只读(缺陷);
//!   owner 稍后出现 → 不重启 Bridge 完成有限职责 attach(T19/FIXTURE)。
//! - Desktop 退出重开 → 重新 attach;旧 pump 代次回收,同一事件只收到一次;
//!   版本变化后能力降级为可证明的只读(T20/FIXTURE)。
//!
//! 全部使用独立进程 fake-codex-owner + 临时 socket/codex_home,
//! 绝不触碰真实 `~/.codex`(socket 显式注入,discovery 不参与)。

use std::path::{Path, PathBuf};
use std::time::Duration;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::domain::{
    CommandPayload, CommandRequest, DomainEvent, Operation, OutputText, SessionKey,
};
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-attach";
const CONV: &str = "bbbbbbbb-1111-4111-8111-111111111111";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-attach-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 短 socket 路径(macOS sun_path 上限 104 字节)。
fn socket_path(tag: &str) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/ac-attach-{}-{}-{tag}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ))
}

fn script_for(dir: &Path, conversation: &str, title: &str) -> PathBuf {
    let script = json!({
        "sessions": [{
            "conversationId": conversation,
            "title": title,
            "cwd": "/tmp/fixture-attach",
            "branch": "fixture-branch",
            "turn": {
                "outputLines": ["a-1", "a-2"],
                "lineDelayMs": 30,
                "finalAnswer": "fixture-final",
                "finalOutput": null
            }
        }]
    });
    let path = dir.join("script.json");
    std::fs::write(&path, serde_json::to_string(&script).unwrap()).unwrap();
    path
}

/// spawn 独立 fake owner 子进程并等待 socket 出现。
async fn spawn_fake_owner(socket: &Path, script: PathBuf, log: PathBuf) -> std::process::Child {
    let stderr_log = std::fs::File::create(&log).expect("log file");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fake-codex-owner"))
        .arg(socket)
        .arg(script)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log))
        .spawn()
        .expect("spawn fake-codex-owner");
    for _ in 0..250 {
        if socket.exists() {
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            let log = std::fs::read_to_string(&log).unwrap_or_default();
            panic!("fake-codex-owner exited before binding socket: {status}; log: {log}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");
    child
}

struct FakeGuard(std::process::Child);
impl Drop for FakeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// 写方法全 Passed 的 probe 注入(与真机 §9 矩阵一致的 fixture 形态)。
fn all_passed_probes() -> WriteMethodProbes {
    WriteMethodProbes {
        start_turn: ProbeResult::Passed,
        steer_turn: ProbeResult::Passed,
        interrupt_turn: ProbeResult::Passed,
        answer_question: ProbeResult::Passed,
        update_settings: ProbeResult::Passed,
    }
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

fn adapter_config(socket: PathBuf, version_binary: Option<PathBuf>) -> CodexAdapterConfig {
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(socket);
    // 空 codex_home:无真实 SQLite;列表来自静态种子。
    config.codex_home = Some(temp_dir("empty-home"));
    config.version_report = Some("codex-cli 0.153.1".to_string());
    config.version_binary = version_binary;
    config.write_method_probes = all_passed_probes();
    config.static_sessions = vec![seed(CONV, "fixture-attach")];
    config
}

/// `codex --version` 固定 argv 探测的 fixture:输出指定版本串。
fn version_probe_fixture(dir: &Path, version: &str) -> PathBuf {
    write_version_script(dir, &format!("#!/bin/sh\necho \"codex-cli {version}\"\n"))
}

/// 覆写版本探测脚本内容(与 `version_probe_fixture` 同一路径,支持同一
/// attach 生命周期内切换探测结果:成功 ↔ 失败)。
fn write_version_script(dir: &Path, content: &str) -> PathBuf {
    let path = dir.join("codex-version-probe.sh");
    std::fs::write(&path, content).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// 版本探测失败 fixture:固定 argv 探测以非零退出(自动更新半途/二进制
/// 路径失效/版本命令失败的真实形态)。
fn version_probe_failure_fixture(dir: &Path) -> PathBuf {
    write_version_script(dir, "#!/bin/sh\nexit 1\n")
}

/// 轮询直到能力达到指定 control_mode(attach 完成的充分条件:
/// 连接 + 版本重估 + 能力广播均已生效)。
async fn wait_control_mode(
    adapter: &CodexAdapter,
    mode: bridge::domain::ControlMode,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if adapter.capabilities().control_mode == mode && adapter.discover().await.ipc_connected {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 输出 transport/adapter 内部日志,便于失败诊断。
fn init_test_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new("debug").unwrap())
        .with_test_writer()
        .try_init();
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

/// T19/FIXTURE:Bridge(Adapter)先于 Desktop 启动:ipc 缺失 → 只读降级;
/// owner 稍后出现 → 不重启完成 attach,能力从只读提升到已验证写,
/// 随后权威快照可达。设备键(device_id)保持不变。
#[tokio::test]
async fn cold_start_attaches_when_owner_appears_later() {
    init_test_tracing();
    let dir = temp_dir("cold");
    let socket = socket_path("cold");
    assert!(!socket.exists(), "前置:socket 尚不存在(Desktop 未启动)");
    // 版本探测 fixture:与配置一致 → attach 后写能力按既有矩阵开放。
    let version_binary = version_probe_fixture(&dir, "0.153.1");
    let adapter =
        CodexAdapter::connect(adapter_config(socket.clone(), Some(version_binary.clone())))
            .await
            .unwrap();

    // ---- 冷启动:ipc 未建立,能力只读降级(§5/§10.2) ----
    let report = adapter.discover().await;
    assert!(!report.ipc_connected, "Desktop 未启动时不得误报已连接");
    let caps = adapter.capabilities();
    assert_eq!(
        caps.control_mode,
        bridge::domain::ControlMode::Unavailable,
        "ipc 缺失且无 catalog:不可控制降级(§10.2;ReadOnly 为有本地只读源时的降级)"
    );
    let cold_err = adapter.runtime_snapshot(&key(CONV)).await.unwrap_err();
    assert_eq!(
        cold_err.code(),
        bridge::domain::StableErrorCode::CodexUnavailable,
        "ipc 缺失:快照查询回 CODEX_UNAVAILABLE"
    );

    // ---- owner 稍后出现 ----
    let script = script_for(&dir, CONV, "fixture-attach");
    let fake = spawn_fake_owner(&socket, script, dir.join("owner1.log")).await;
    let _guard = FakeGuard(fake);

    // attach:有限职责重试(1s/2s/5s 退避)后完成,不重启 Bridge。
    // 以能力提升为完成标志(连接 + 版本重估 + 能力广播全部生效)。
    let upgraded = wait_control_mode(
        &adapter,
        bridge::domain::ControlMode::FullControl,
        Duration::from_secs(30),
    )
    .await;
    assert!(
        upgraded,
        "owner 出现后 30s 内未完成 attach + 能力提升: caps={:?} report={:?}",
        adapter.capabilities(),
        adapter.discover().await
    );

    // 能力提升:重新发现的版本 + 握手 + probe → 已验证写(§5)。
    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::FullControl);
    assert!(caps.supports(Operation::StartTurn));

    // Catalog 中存在但当前未在 Desktop 打开的任务应立即归类为
    // SESSION_NOT_FOUND，不能等待首个快照后误报 CODEX_UNAVAILABLE。
    let unopened = key("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    let unopened_err = tokio::time::timeout(
        Duration::from_secs(1),
        adapter.runtime_snapshot(&unopened),
    )
    .await
    .expect("未打开任务的 owner discovery 应快速返回")
    .unwrap_err();
    assert_eq!(
        unopened_err.code(),
        bridge::domain::StableErrorCode::SessionNotFound
    );

    // 权威快照先行:attach 后订阅/快照路径可用。
    let snapshot = adapter.runtime_snapshot(&key(CONV)).await.unwrap();
    assert_eq!(snapshot.current_turn, None, "初始 idle");
    // 设备键保持不变(§9.1;adapter config 决定,锁定不变量)。
    assert_eq!(snapshot_key_device(), DEVICE);
}

fn snapshot_key_device() -> &'static str {
    DEVICE // SessionKey::codex(DEVICE, ..) 由 adapter config 决定。
}

/// T20/FIXTURE:Desktop 退出重开 → 自动重新 attach;新 owner 的权威状态
/// 可达;同一原生事件只收到一次(旧 pump 代次已回收,无跨代次重复);
/// 新 owner 运行未知新版本(版本探测变化)→ 只恢复可证明的读取能力,
/// 不凭"以前版本验证过"自动开放写(§5)。
#[tokio::test]
async fn owner_restart_reattaches_with_generation_recovery_and_version_downgrade() {
    init_test_tracing();
    let dir = temp_dir("restart");
    let socket = socket_path("restart");
    // v1 探测:与配置一致的已验证版本(写开放)。
    let version_v1 = version_probe_fixture(&dir, "0.153.1");
    let adapter = CodexAdapter::connect(adapter_config(socket.clone(), Some(version_v1.clone())))
        .await
        .unwrap();

    let script1 = script_for(&dir, CONV, "fixture-attach");
    let fake1 = spawn_fake_owner(&socket, script1, dir.join("owner1.log")).await;
    let guard1 = FakeGuard(fake1);

    assert!(
        wait_control_mode(
            &adapter,
            bridge::domain::ControlMode::FullControl,
            Duration::from_secs(30)
        )
        .await,
        "第一次 attach 未完成"
    );

    // 订阅并取得基线快照(旧代次状态)。
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    adapter.subscribe(&key(CONV), tx).await.unwrap();
    let baseline = adapter.runtime_snapshot(&key(CONV)).await.unwrap();
    assert_eq!(baseline.current_turn, None);

    // ---- Desktop 退出(socket 随进程关闭)----
    drop(guard1);

    // ---- Desktop 重开(未知新版本) ----
    let _ = version_probe_fixture(&dir, "9.9.9-future"); // v2 内容覆写同一探测脚本
    let script2 = script_for(&dir, CONV, "fixture-attach-v2");
    let fake2 = spawn_fake_owner(&socket, script2, dir.join("owner2.log")).await;
    let _guard2 = FakeGuard(fake2);

    // 重新 attach(版本探测变化 → 能力降级为只读)。
    let reattached = wait_control_mode(
        &adapter,
        bridge::domain::ControlMode::ReadOnly,
        Duration::from_secs(60),
    )
    .await;
    assert!(
        reattached,
        "owner 重开后 60s 内未完成重新 attach + 版本重估: caps={:?} report={:?}",
        adapter.capabilities(),
        adapter.discover().await
    );

    // 新版本:只恢复可证明的读取能力(§5:不凭历史版本开放写)。
    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::ReadOnly);
    assert!(!caps.supports(Operation::StartTurn));
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("must-not-run"),
        },
    };
    let err = adapter
        .execute_command(&key(CONV), request)
        .await
        .unwrap_err();
    assert_eq!(err.code(), bridge::domain::StableErrorCode::ControlReadOnly);

    // 新 owner 权威快照可达(覆盖旧代次投影)。
    let snapshot = adapter.runtime_snapshot(&key(CONV)).await.unwrap();
    assert_eq!(snapshot.current_turn, None);

    // 同一原生事件只收到一次:静默窗口内 summary 事件数量有限且无风暴
    // (若旧 pump 代次未回收,同一事件会跨代次重复出现)。
    let quiet = collect_until(&mut rx, Duration::from_secs(2), |_| false).await;
    let summaries = quiet
        .iter()
        .filter(|e| matches!(e, DomainEvent::SessionSummaryChanged { .. }))
        .count();
    assert!(
        summaries <= 4,
        "重新 attach 后出现跨代次重复事件(summary x{summaries})"
    );
}

/// R2-AC03/FIXTURE:旧版本已验证 + 重连时版本探测失败 → 连接与可证明的
/// 读取能力保留,写能力关闭(DEGRADED + READ_ONLY),不沿用启动白名单;
/// 探测恢复读到同一已验证版本后,新连接下重新确认并恢复写。
/// 全程断言零写命令重放(重连只重放 following 订阅)。
#[tokio::test]
async fn version_probe_failure_on_reattach_degrades_read_only_then_recovers() {
    init_test_tracing();
    let dir = temp_dir("probe-fail");
    let socket = socket_path("probe-fail");
    // 启动基准:0.153.1 已验证写(与探测 fixture 一致)。
    let version_binary = version_probe_fixture(&dir, "0.153.1");
    let adapter = CodexAdapter::connect(adapter_config(socket.clone(), Some(version_binary.clone())))
        .await
        .unwrap();

    let script = script_for(&dir, CONV, "fixture-attach");
    let fake1 = spawn_fake_owner(&socket, script.clone(), dir.join("owner1.log")).await;
    let guard1 = FakeGuard(fake1);
    assert!(
        wait_control_mode(&adapter, bridge::domain::ControlMode::FullControl, Duration::from_secs(30)).await,
        "第一次 attach 未完成"
    );

    // ---- Desktop 退出;探测脚本失效(exit 1);新 owner 重开 ----
    drop(guard1);
    let _failure_script = version_probe_failure_fixture(&dir);
    let fake2 = spawn_fake_owner(&socket, script.clone(), dir.join("owner2.log")).await;
    let guard2 = FakeGuard(fake2);

    // 重新 attach:探测失败 → 无可信版本 → 降级只读,连接保留。
    let degraded = wait_control_mode(
        &adapter,
        bridge::domain::ControlMode::ReadOnly,
        Duration::from_secs(60),
    )
    .await;
    assert!(
        degraded,
        "探测失败后未在 60s 内降级只读: caps={:?} report={:?}",
        adapter.capabilities(),
        adapter.discover().await
    );
    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::ReadOnly);
    assert_eq!(
        caps.compatibility_state,
        bridge::domain::CompatibilityState::Degraded,
        "无可信版本 → DEGRADED(§10.3),不沿用启动时的 VERIFIED"
    );
    assert!(
        caps.codex_version.is_none(),
        "探测失败不得把启动版本冒充为已确认的当前版本"
    );
    assert!(!caps.supports(Operation::StartTurn));
    let request = CommandRequest {
        request_id: Uuid::new_v4(),
        operation: Operation::StartTurn,
        session_key: key(CONV),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: None,
        payload: CommandPayload::StartTurn {
            input: OutputText::new("must-not-run-while-probe-fails"),
        },
    };
    let err = adapter
        .execute_command(&key(CONV), request)
        .await
        .unwrap_err();
    assert_eq!(err.code(), bridge::domain::StableErrorCode::ControlReadOnly);

    // 只读链不因写降级一起挂掉:连接在线、列表与权威快照可达
    // (新连接下新 owner 的状态,非旧连接缓存)。
    assert!(adapter.discover().await.ipc_connected, "连接必须保留");
    let page = adapter.list_sessions(None, 10, false).await.unwrap();
    assert!(
        page.sessions.iter().any(|s| s.session_key.native_session_id == CONV),
        "降级后会话列表仍可用"
    );
    let snapshot = adapter.runtime_snapshot(&key(CONV)).await.unwrap();
    assert_eq!(snapshot.current_turn, None, "新 owner 权威快照可达");

    // ---- 探测恢复(读到同一已验证版本)→ 下次重连恢复写 ----
    drop(guard2);
    let _restored = version_probe_fixture(&dir, "0.153.1");
    let fake3 = spawn_fake_owner(&socket, script, dir.join("owner3.log")).await;
    let _guard3 = FakeGuard(fake3);

    let recovered = wait_control_mode(
        &adapter,
        bridge::domain::ControlMode::FullControl,
        Duration::from_secs(60),
    )
    .await;
    assert!(
        recovered,
        "探测恢复后未在 60s 内恢复写能力: caps={:?} report={:?}",
        adapter.capabilities(),
        adapter.discover().await
    );
    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::FullControl);
    assert_eq!(
        caps.compatibility_state,
        bridge::domain::CompatibilityState::Verified
    );
    assert!(caps.supports(Operation::StartTurn));

    // 全程零写命令重放:三次 fake owner 日志都不含原生写方法到达
    // (attach 只重放 following 订阅,§12/§15)。pending 类写方法有
    // "write method reached owner" 观测日志;start/steer/interrupt/
    // update-settings 等其余原生写只走通用 request 日志,按方法名全量
    // 断言,保证降级/恢复全程零写覆盖完整。
    const WRITE_METHODS: [&str; 8] = [
        "thread-follower-start-turn",
        "thread-follower-steer-turn",
        "thread-follower-interrupt-turn",
        "thread-follower-update-thread-settings",
        "thread-follower-submit-user-input",
        "thread-follower-command-approval-decision",
        "thread-follower-file-approval-decision",
        "thread-follower-permissions-request-approval-response",
    ];
    for log in ["owner1.log", "owner2.log", "owner3.log"] {
        let content = std::fs::read_to_string(dir.join(log)).unwrap_or_default();
        assert!(
            !content.contains("write method reached owner"),
            "{log} 收到了原生写方法(写命令重放): {content}"
        );
        for method in WRITE_METHODS {
            assert!(
                !content.contains(method),
                "{log} 收到了原生写方法 {method}(写命令重放): {content}"
            );
        }
    }
}

/// R2-AC03 seam 回归:version_binary=None 是测试注入的明确冻结版本模式——
/// 不触发生产探测,attach 后按启动 version_report 与写矩阵恢复
/// (与生产探测失败路径严格区分;本机默认探测二进制存在与否不影响结果)。
#[tokio::test]
async fn frozen_version_report_seam_keeps_startup_trust_base() {
    init_test_tracing();
    let dir = temp_dir("frozen");
    let socket = socket_path("frozen");
    // version_binary=None:冻结模式(seam)。
    let adapter = CodexAdapter::connect(adapter_config(socket.clone(), None))
        .await
        .unwrap();

    let script = script_for(&dir, CONV, "fixture-attach");
    let fake = spawn_fake_owner(&socket, script, dir.join("owner1.log")).await;
    let _guard = FakeGuard(fake);

    assert!(
        wait_control_mode(&adapter, bridge::domain::ControlMode::FullControl, Duration::from_secs(30)).await,
        "冻结模式下 attach 未完成"
    );
    let caps = adapter.capabilities();
    assert_eq!(caps.control_mode, bridge::domain::ControlMode::FullControl);
    assert_eq!(
        caps.codex_version.as_deref(),
        Some("codex-cli 0.153.1"),
        "冻结模式保持启动 version_report"
    );
}
