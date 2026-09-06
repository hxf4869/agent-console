//! ZCode Hook 审批通路端到端(FIXTURE;权威规格 §29.4 模式 + 04 §8.3):
//!
//! helper socket 客户端 → Bridge 注册表 → 现有事件通道(PendingAttention*)
//! → 命令面(`AnswerApproval` / `AnswerQuestion` 命令信封)原子决定 →
//! helper 收到应答。Codex 侧用 fake-codex-owner 撑起 adapter(零写操作),
//! ZCode 会话以正式 `agent_kind = ZCODE_DESKTOP` 隔离(ZC-02),与 Codex
//! 会话不串线。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::agent_console::v1::{command_request, envelope};
use agent_console_protocol::codec::{new_message_id, PROTOCOL_VERSION};
use bridge::adapter::codex::{CodexAdapter, CodexAdapterConfig};
use bridge::commands::{CommandGateway, PowerCoordinator, UploadLifecycle};
use bridge::config::BridgeConfig;
use bridge::files::{FileGrantManager, TransferConfig};
use bridge::git::GitService;
use bridge::local_store::LocalStore;
use bridge::runtime::{BridgeRuntime, FsUploadCleaner, OutboundSink, RuntimeParts};
use bridge::transport::{DeviceCredential, TransportError};
use bridge::zcode::contract::{self, HookInvoke, HookReply};
use bridge::zcode::pending::InvokeKind;
use bridge::zcode::{server, PendingState, ZcodeHooks};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

const DEVICE: &str = "device-zcode-e2e";

// ---------------------------------------------------------------------------
// 回环出站 sink 与收集器(与 integration_runtime 同模式)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LoopbackSink {
    tx: tokio::sync::mpsc::UnboundedSender<pb::Envelope>,
}

impl std::fmt::Debug for LoopbackSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopbackSink").finish()
    }
}

impl OutboundSink for LoopbackSink {
    fn send_envelope(&self, envelope: &pb::Envelope) -> Result<(), TransportError> {
        self.tx
            .send(envelope.clone())
            .map_err(|_| TransportError::Stopped)
    }
}

#[derive(Debug)]
struct TestCredential;

impl DeviceCredential for TestCredential {
    fn bearer_token(&self) -> String {
        "test-device-credential".to_string()
    }
}

struct Outbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<pb::Envelope>,
}

impl Outbox {
    async fn collect_until(&mut self, at_most: usize, timeout: Duration) -> Vec<pb::Envelope> {
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        while out.len() < at_most {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(env)) => out.push(env),
                Ok(None) | Err(_) => break,
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// 最小 harness(fake-codex-owner 撑起 Codex adapter;无种子、零写)
// ---------------------------------------------------------------------------

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-zcode-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct FakeGuard(std::process::Child);
impl Drop for FakeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// macOS SUN_LEN 限制:socket 路径必须短。返回专用短目录(服务端 bind
/// 会将其 chmod 0700),socket 文件为其下 hook.sock;不得把共享目录
/// (如 /tmp 本身)当作 socket 父目录。
fn short_socket_dir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(format!(
        "/tmp/ac-z-{}-{}-{tag}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("hook.sock")
}

async fn spawn_fake(dir: &Path, socket: PathBuf) -> (PathBuf, FakeGuard) {
    let script_path = dir.join("script.json");
    std::fs::write(&script_path, serde_json::json!({"sessions": []}).to_string()).unwrap();
    let stderr_log = std::fs::File::create(dir.join("fake-owner.log")).expect("log file");
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fake-codex-owner"))
        .arg(socket.clone())
        .arg(script_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log))
        .spawn()
        .expect("spawn fake-codex-owner");
    for _ in 0..250 {
        if socket.exists() {
            return (socket, FakeGuard(child));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("fake owner socket did not appear");
}

async fn setup(tag: &str) -> (Arc<BridgeRuntime>, Outbox, PathBuf) {
    let dir = temp_dir(tag);
    let ipc_socket = short_socket_dir(&format!("{tag}-ipc"));
    let (ipc_socket, _fake) = spawn_fake(&dir, ipc_socket).await;
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(ipc_socket);
    config.codex_home = Some(temp_dir(&format!("{tag}-home")));
    config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    let adapter = CodexAdapter::connect(config).await.unwrap();

    let data_dir = dir.join("data");
    let store = LocalStore::open(&data_dir).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let uploads = Arc::new(UploadLifecycle::new(
        Arc::new(FsUploadCleaner::new(data_dir.join("uploads"))),
        store.clone(),
    ));
    let power = PowerCoordinator::new(
        Arc::new(bridge::power::WakePolicy::with_program(PathBuf::from(
            "/bin/true",
        ))),
        bridge::power::PowerSource::Ac,
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Arc::new(BridgeRuntime::new(RuntimeParts {
        config: BridgeConfig::from_env()
            .with_data_dir(data_dir.clone())
            .with_relay_url("ws://127.0.0.1:1"),
        device_id: DEVICE.to_string(),
        store,
        adapter,
        gateway,
        grants: FileGrantManager::new(),
        git: GitService::new().ok().map(Arc::new),
        uploads,
        power,
        credential: Arc::new(TestCredential),
        outbound: Arc::new(LoopbackSink { tx }),
        http: reqwest::Client::new(),
        upload_root: data_dir.join("uploads"),
        transfer_config: TransferConfig::default(),
    }));
    runtime.start_observation();
    (runtime, Outbox { rx }, data_dir)
}

// ---------------------------------------------------------------------------
// socket 客户端(模拟 helper)
// ---------------------------------------------------------------------------

fn permission_invoke(id: &str) -> HookInvoke {
    HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: id.to_string(),
        event: contract::EVENT_PERMISSION_REQUEST.to_string(),
        native_session_id: Some("sess-z1".to_string()),
        tool_name: Some("Bash".to_string()),
        tool_use_id: None,
        requested_wait_ms: 15_000,
        tool_input: Some(serde_json::json!({"command": "echo fixture"})),
        ask: None,
        status_event: None,
        status_input: None,
    }
}

fn ask_invoke(id: &str) -> HookInvoke {
    HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: id.to_string(),
        event: contract::EVENT_ASK_USER.to_string(),
        native_session_id: None,
        tool_name: None,
        tool_use_id: None,
        requested_wait_ms: 15_000,
        tool_input: None,
        ask: Some(contract::AskRequest {
            question: "继续执行吗?".to_string(),
            options: vec!["继续".to_string(), "取消".to_string()],
            allow_free_text: true,
            call_id: Some("call-1".to_string()),
        }),
        status_event: None,
        status_input: None,
    }
}

async fn send_invoke(socket: &Path, invoke: &HookInvoke) -> (tokio::net::unix::OwnedWriteHalf, BufReader<tokio::net::unix::OwnedReadHalf>) {
    let client = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = client.into_split();
    let mut line = serde_json::to_string(invoke).unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    (writer, BufReader::new(reader))
}

async fn read_reply(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> HookReply {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

fn inbound_envelope(payload: pb::envelope::Payload, correlation: &str) -> pb::Envelope {
    pb::Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: correlation.to_owned(),
        sent_at: Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        device_id: String::new(),
        agent_kind: 1,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(payload),
        provider_extension: None,
    }
}

fn subscribe_session(native: &str) -> pb::Envelope {
    inbound_envelope(
        envelope::Payload::Subscribe(pb::Subscribe {
            target: Some(pb::subscribe::Target::Session(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::ZcodeDesktop as i32,
                native_session_id: native.to_string(),
                relay_session_uuid: String::new(),
            })),
        }),
        "corr-sub",
    )
}

fn approval_command(invoke_id: &str, decision: &str) -> pb::Envelope {
    inbound_envelope(
        envelope::Payload::CommandRequest(pb::CommandRequest {
            request_id: Uuid::new_v4().to_string(),
            operation: pb::Operation::AnswerApproval as i32,
            session_key: Some(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::ZcodeDesktop as i32,
                native_session_id: "sess-z1".to_string(),
                relay_session_uuid: String::new(),
            }),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: String::new(),
            payload: Some(command_request::Payload::AnswerApproval(
                pb::AnswerApprovalPayload {
                    approval_id: invoke_id.to_string(),
                    decision_id: decision.to_string(),
                },
            )),
        }),
        "corr-cmd",
    )
}

fn question_command(invoke_id: &str, option: &str, text: &str) -> pb::Envelope {
    inbound_envelope(
        envelope::Payload::CommandRequest(pb::CommandRequest {
            request_id: Uuid::new_v4().to_string(),
            operation: pb::Operation::AnswerQuestion as i32,
            session_key: Some(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::ZcodeDesktop as i32,
                native_session_id: "plugin-ask".to_string(),
                relay_session_uuid: String::new(),
            }),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: String::new(),
            payload: Some(command_request::Payload::AnswerQuestion(
                pb::AnswerQuestionPayload {
                    question_id: invoke_id.to_string(),
                    option_ids: if option.is_empty() {
                        vec![]
                    } else {
                        vec![option.to_string()]
                    },
                    free_text: text.to_string(),
                },
            )),
        }),
        "corr-cmd",
    )
}

/// 等待注册(轮询,有界)。
async fn wait_registered(hooks: &ZcodeHooks, invoke_id: &str) {
    for _ in 0..200 {
        if hooks.registry().get(invoke_id).is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("invoke {invoke_id} not registered in time");
}

// ---------------------------------------------------------------------------
// 场景
// ---------------------------------------------------------------------------

/// 手机/浏览器命令 → Bridge 原子决定 → helper 收到 allowed;卡片事件经
/// 现有事件通道(PendingAttentionAdded → Removed)到达会话流。
#[tokio::test]
async fn browser_allow_command_resolves_pending_and_publishes_events() {
    let (runtime, mut outbox, _data_dir) = setup("allow").await;
    let zcode_socket = short_socket_dir("allow-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    // 浏览器订阅 zcode 会话流(独立"会话",不与 Codex 列表混流)。
    runtime
        .handle_envelope(subscribe_session("sess-z1"))
        .await;

    // helper 发起 PermissionRequest。
    let invoke = permission_invoke("e2e-allow-1");
    let (_guard, mut reader) = send_invoke(&zcode_socket, &invoke).await;
    wait_registered(&hooks, "e2e-allow-1").await;

    // 卡片事件已发(PendingAttentionAdded;摘要不隐去工具名与输入摘要)。
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let added = envelopes.iter().find_map(|env| match &env.payload {
        Some(envelope::Payload::EventBatch(batch)) => batch.events.iter().find_map(|event| {
            matches!(
                event.event,
                Some(pb::domain_event::Event::PendingAttentionAdded(_))
            )
            .then(|| event.clone())
        }),
        _ => None,
    });
    assert!(
        added.is_some(),
        "expected PendingAttentionAdded event, got {:?}",
        envelopes.iter().map(payload_kind).collect::<Vec<_>>()
    );

    // 浏览器命令 allow。
    runtime.handle_envelope(approval_command("e2e-allow-1", "allow")).await;
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .expect("helper reply in time");
    assert_eq!(reply.status, contract::STATUS_ALLOWED);

    // 回执:CommandAccepted → CommandResult COMPLETED。
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let accepted = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::CommandAccepted(_))
        )
    });
    let completed = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
        )
    });
    assert!(accepted, "expected CommandAccepted");
    assert!(completed, "expected CommandResult COMPLETED");
    // 决定后卡片移除(PendingAttentionRemoved)。
    let removed = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::EventBatch(ref b))
                if b.events.iter().any(|e| matches!(
                    e.event,
                    Some(pb::domain_event::Event::PendingAttentionRemoved(_))
                ))
        )
    });
    assert!(removed, "expected PendingAttentionRemoved after decision");

    server_task.abort();
}

/// 浏览器拒绝 → helper 收到 denied + 默认消息。
#[tokio::test]
async fn browser_deny_command_returns_denied_to_helper() {
    let (runtime, _outbox, _data_dir) = setup("deny").await;
    let zcode_socket = short_socket_dir("deny-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();
    let (_guard, mut reader) =
        send_invoke(&zcode_socket, &permission_invoke("e2e-deny-1")).await;
    wait_registered(&hooks, "e2e-deny-1").await;
    runtime
        .handle_envelope(approval_command("e2e-deny-1", "deny"))
        .await;
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .unwrap();
    assert_eq!(reply.status, contract::STATUS_DENIED);
    assert_eq!(
        reply.message.as_deref(),
        Some("User declined this action in Agent Console")
    );
    server_task.abort();
}

/// MCP 问答经同一命令面:AnswerQuestion(option id)→ answered + 选项原文。
#[tokio::test]
async fn mcp_ask_resolved_via_answer_question_command() {
    let (runtime, _outbox, _data_dir) = setup("ask").await;
    let zcode_socket = short_socket_dir("ask-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();
    let (_guard, mut reader) = send_invoke(&zcode_socket, &ask_invoke("e2e-ask-1")).await;
    wait_registered(&hooks, "e2e-ask-1").await;
    runtime
        .handle_envelope(question_command("e2e-ask-1", "option-2", ""))
        .await;
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .unwrap();
    assert_eq!(reply.status, contract::STATUS_ANSWERED);
    assert_eq!(reply.option.as_deref(), Some("取消"));
    server_task.abort();
}

/// 过期后命令决定 → Rejected + APPROVAL_EXPIRED;helper 收到 expired。
#[tokio::test]
async fn late_command_decision_is_rejected_as_expired() {
    let (runtime, mut outbox, _data_dir) = setup("late").await;
    let zcode_socket = short_socket_dir("late-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();
    let mut invoke = permission_invoke("e2e-late-1");
    invoke.requested_wait_ms = contract::MIN_REMOTE_WAIT_MS;
    let (_guard, mut reader) = send_invoke(&zcode_socket, &invoke).await;
    wait_registered(&hooks, "e2e-late-1").await;
    let reply = tokio::time::timeout(Duration::from_secs(5), read_reply(&mut reader))
        .await
        .unwrap();
    assert_eq!(reply.status, contract::STATUS_EXPIRED);
    // 迟到命令。
    runtime
        .handle_envelope(approval_command("e2e-late-1", "allow"))
        .await;
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let rejected_expired = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptRejected as i32
                    && r.error_code == pb::StableErrorCode::ApprovalExpired as i32
        )
    });
    assert!(rejected_expired, "expected APPROVAL_EXPIRED rejection");
    server_task.abort();
}

/// 同机隔离:同 native id 的 Codex 命令与 ZCode 命令不串线 —— ZCode 审批
/// 命令不进入 Codex gateway(不会以 CAPABILITY gate 拒绝,也不会误投
/// Codex Desktop);Codex 链路的 unknown approval 仍走原路径被拒。
#[tokio::test]
async fn codex_answer_approval_path_unchanged_without_zcode_hit() {
    let (runtime, mut outbox, _data_dir) = setup("isolation").await;
    let zcode_socket = short_socket_dir("iso-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    // 未登记任何 zcode pending:同 payload 命令必须走 Codex 原路径
    // (fake owner 无此 approval → gateway 拒绝,而不是 zcode 分支 COMPLETED)。
    runtime
        .handle_envelope(approval_command("no-such-zcode-invoke", "allow"))
        .await;
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let completed = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
        )
    });
    assert!(!completed, "未命中 zcode 注册表不得返回 COMPLETED");
}

/// 真实 helper 子进程(`bridge zcode-hook`)全链路:helper 发起 PermissionRequest
/// 后保持写半连接存活(不再提前 EOF),服务端 pending 不被取消,allow 决定
/// 能送达 helper 并输出官方 decision JSON。
/// Codex 复现(修复前):invoke_once 返回 reader 时丢弃 OwnedWriteHalf →
/// 服务端立即收到 EOF → pending 以 HandledLocally 取消,决定永远到不了
/// helper(elapsed≈0,直接 expired/HandledLocally)。
#[tokio::test]
async fn real_helper_process_keeps_pending_alive_until_decided() {
    let (runtime, _outbox, _data_dir) = setup("real-helper").await;
    let zcode_socket = short_socket_dir("real-helper-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    // 经实际子进程入口驱动(helper 主路径,非模拟客户端)。
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_bridge"))
        .args([
            "zcode-hook",
            "--socket",
            zcode_socket.to_str().unwrap(),
            "--wait-ms",
            "15000",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn bridge zcode-hook helper");
    let mut stdin = helper.stdin.take().expect("helper stdin");
    stdin
        .write_all(
            br#"{"hook_event_name":"PermissionRequest","session_id":"sess-z1","tool_name":"Bash","tool_input":{"command":"echo fixture"}}"#,
        )
        .await
        .unwrap();
    stdin.write_all(b"\n").await.unwrap();
    stdin.flush().await.unwrap();
    drop(stdin); // 单行输入已读完;socket 写半连接由 helper 内部保持。

    // helper 自生成 invoke_id:轮询注册表拿首个 Waiting 卡片。
    let mut invoke_id = String::new();
    for _ in 0..200 {
        if let Some(card) = hooks.registry().waiting_cards().first() {
            invoke_id = card.invoke_id.clone();
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !invoke_id.is_empty(),
        "helper invoke 未能以 Waiting 态被观察到(修复前:登记即被 helper \
         EOF 以 HandledLocally 取消,轮询窗口内 pending 已消失;registry len={})",
        hooks.registry().len()
    );
    // 关键断言:登记后等待一段时间,pending 仍是 Waiting(修复前:helper
    // 连接建立即丢写半 → EOF → HandledLocally,此处已经取消)。
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        hooks.registry().get(&invoke_id).map(|s| s.state),
        Some(bridge::zcode::PendingState::Waiting),
        "pending 不得因 helper EOF 提前取消"
    );
    // 决定可达 helper:原子决定后 helper stdout 输出官方 allow JSON。
    hooks
        .registry()
        .resolve(&invoke_id, HookReply::allowed())
        .expect("pending must still be resolvable");
    let mut stdout = BufReader::new(helper.stdout.take().expect("helper stdout"));
    let mut line = String::new();
    let read = tokio::time::timeout(Duration::from_secs(3), stdout.read_line(&mut line))
        .await
        .expect("helper reply in time");
    assert!(read.unwrap() > 0, "helper stdout closed without decision");
    assert_eq!(
        line.trim(),
        r#"{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}"#
    );

    server_task.abort();
    let _ = helper.wait().await;
}

/// ZCode 审批分派必须核对目标绑定(Codex 复现:runtime 在解析 session_key
/// 之前,仅凭 approval/question id 命中注册表即处理 —— 任何会话/任何
/// agentKind 的 AnswerApproval/AnswerQuestion 都可能处理掉 ZCode pending)。
/// 反例:其他会话、错误设备、Codex 目标(同 native ID)、错误操作类型;
/// 正例:绑定一致仍能决定。
#[tokio::test]
async fn zcode_dispatch_requires_matching_session_binding() {
    let (runtime, mut outbox, _data_dir) = setup("dispatch").await;
    let zcode_socket = short_socket_dir("dispatch-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    let (_guard, mut reader) = send_invoke(&zcode_socket, &permission_invoke("x-1")).await;
    wait_registered(&hooks, "x-1").await;

    /// 自定义目标(设备/agentKind/native)的 AnswerApproval 信封。
    fn approval_to(
        device: &str,
        agent: pb::AgentKind,
        native: &str,
        invoke_id: &str,
        decision: &str,
    ) -> pb::Envelope {
        inbound_envelope(
            envelope::Payload::CommandRequest(pb::CommandRequest {
                request_id: Uuid::new_v4().to_string(),
                operation: pb::Operation::AnswerApproval as i32,
                session_key: Some(pb::SessionKey {
                    device_id: device.to_string(),
                    agent_kind: agent as i32,
                    native_session_id: native.to_string(),
                    relay_session_uuid: String::new(),
                }),
                expected_turn_id: None,
                expected_runtime_revision: None,
                payload_digest: String::new(),
                payload: Some(command_request::Payload::AnswerApproval(
                    pb::AnswerApprovalPayload {
                        approval_id: invoke_id.to_string(),
                        decision_id: decision.to_string(),
                    },
                )),
            }),
            "corr-cmd",
        )
    }

    /// AnswerQuestion 打审批 pending(错误操作类型)。
    fn question_on_approval(invoke_id: &str) -> pb::Envelope {
        inbound_envelope(
            envelope::Payload::CommandRequest(pb::CommandRequest {
                request_id: Uuid::new_v4().to_string(),
                operation: pb::Operation::AnswerQuestion as i32,
                session_key: Some(pb::SessionKey {
                    device_id: DEVICE.to_string(),
                    agent_kind: pb::AgentKind::ZcodeDesktop as i32,
                    native_session_id: "plugin-ask".to_string(),
                    relay_session_uuid: String::new(),
                }),
                expected_turn_id: None,
                expected_runtime_revision: None,
                payload_digest: String::new(),
                payload: Some(command_request::Payload::AnswerQuestion(
                    pb::AnswerQuestionPayload {
                        question_id: invoke_id.to_string(),
                        option_ids: vec!["option-1".to_string()],
                        free_text: String::new(),
                    },
                )),
            }),
            "corr-cmd",
        )
    }

    async fn not_completed_and_still_waiting(
        runtime: &Arc<BridgeRuntime>,
        outbox: &mut Outbox,
        hooks: &ZcodeHooks,
        envelope: pb::Envelope,
        tag: &str,
    ) {
        runtime.handle_envelope(envelope).await;
        let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
        assert!(
            !envelopes.iter().any(|env| matches!(
                env.payload,
                Some(envelope::Payload::CommandResult(ref r))
                    if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
            )),
            "[{tag}] 目标不一致不得 COMPLETED"
        );
        assert_eq!(
            hooks.registry().get("x-1").map(|s| s.state),
            Some(bridge::zcode::PendingState::Waiting),
            "[{tag}] pending 不得被无关目标消费"
        );
    }

    // ① 其他会话的同命令(ZcodeDesktop,native 不一致)。
    not_completed_and_still_waiting(
        &runtime,
        &mut outbox,
        &hooks,
        approval_to(DEVICE, pb::AgentKind::ZcodeDesktop, "sess-other", "x-1", "allow"),
        "wrong-session",
    )
    .await;
    // ② 错误设备。
    not_completed_and_still_waiting(
        &runtime,
        &mut outbox,
        &hooks,
        approval_to("device-other", pb::AgentKind::ZcodeDesktop, "sess-z1", "x-1", "allow"),
        "wrong-device",
    )
    .await;
    // ③ Codex 目标(同 native ID 的 CODEX 会话):走原 Codex 路径。
    not_completed_and_still_waiting(
        &runtime,
        &mut outbox,
        &hooks,
        approval_to(DEVICE, pb::AgentKind::CodexDesktop, "sess-z1", "x-1", "allow"),
        "codex-target",
    )
    .await;
    // ④ 错误操作类型(AnswerQuestion 打审批 pending)。
    not_completed_and_still_waiting(
        &runtime,
        &mut outbox,
        &hooks,
        question_on_approval("x-1"),
        "wrong-op-type",
    )
    .await;

    // ⑤ 正确目标:绑定完全一致仍能决定,helper 收到 allowed。
    runtime
        .handle_envelope(approval_to(
            DEVICE,
            pb::AgentKind::ZcodeDesktop,
            "sess-z1",
            "x-1",
            "allow",
        ))
        .await;
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .expect("helper reply in time");
    assert_eq!(reply.status, contract::STATUS_ALLOWED);
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    assert!(
        envelopes.iter().any(|env| matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
        )),
        "正确目标必须 COMPLETED"
    );

    server_task.abort();
}

/// Hook 状态事件必须进入实时发布(Codex 复现:server 状态分支只更新
/// ObservationStore 并 ACK,不发布摘要 —— 浏览器看不到 ZCode 会话状态
/// 实时更新)。经实际 socket 帧驱动到订阅流:UserPromptSubmit → 运行中,
/// Stop → 空闲;订阅流各收到一条 SessionSummaryChanged。
#[tokio::test]
async fn status_events_publish_session_summary_updates() {
    let (runtime, mut outbox, _data_dir) = setup("status").await;
    let zcode_socket = short_socket_dir("status-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    // 浏览器订阅 ZCode 会话详情流。
    runtime.handle_envelope(subscribe_session("sess-status-1")).await;
    let _ = outbox.collect_until(2, Duration::from_secs(3)).await;

    async fn send_status(
        socket: &Path,
        line: &str,
    ) -> contract::HookReply {
        let invoke = server::status_invoke_from_line(line).unwrap();
        let (_guard, mut reader) = send_invoke(socket, &invoke).await;
        read_reply(&mut reader).await
    }

    async fn collect_summary_updates(
        outbox: &mut Outbox,
    ) -> Vec<pb::SessionSummary> {
        let envelopes = outbox.collect_until(2, Duration::from_secs(3)).await;
        envelopes
            .iter()
            .filter_map(|env| match &env.payload {
                Some(envelope::Payload::EventBatch(b)) => Some(b),
                _ => None,
            })
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match &e.event {
                Some(pb::domain_event::Event::SessionSummaryChanged(s)) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    // ① UserPromptSubmit:助手流收到运行中摘要。
    let reply = send_status(
        &zcode_socket,
        r#"{"hook_event_name":"UserPromptSubmit","session_id":"sess-status-1","prompt":"..."}"#,
    )
    .await;
    assert_eq!(reply.status, contract::STATUS_ACCEPTED);
    let updates = collect_summary_updates(&mut outbox).await;
    let summary = updates
        .iter()
        .find(|s| {
            s.session_key
                .as_ref()
                .is_some_and(|k| k.native_session_id == "sess-status-1")
        })
        .unwrap_or_else(|| panic!("状态事件必须发布摘要更新(修复前仅写内存): {updates:?}"));
    assert_eq!(
        summary.active_turn_phase,
        pb::ActiveTurnPhase::TurnPhaseRunning as i32,
        "UserPromptSubmit 后摘要应为运行中"
    );

    // ② Stop:摘要回到空闲。
    let reply = send_status(
        &zcode_socket,
        r#"{"hook_event_name":"Stop","session_id":"sess-status-1","stop_hook_active":false}"#,
    )
    .await;
    assert_eq!(reply.status, contract::STATUS_ACCEPTED);
    let updates = collect_summary_updates(&mut outbox).await;
    let summary = updates
        .iter()
        .find(|s| {
            s.session_key
                .as_ref()
                .is_some_and(|k| k.native_session_id == "sess-status-1")
        })
        .unwrap_or_else(|| panic!("Stop 必须发布摘要更新: {updates:?}"));
    assert_eq!(
        summary.active_turn_phase,
        pb::ActiveTurnPhase::TurnPhaseIdle as i32,
        "Stop 后摘要应为空闲"
    );

    server_task.abort();
}

/// M2 回归:决定与超时同时就绪、`tokio::select!` 选中 Timeout 分支的竞态
/// (随机分支,经提取的 Timeout 收尾路径 `settle_without_decision` 确定性
/// 驱动;不经 socket 登记以免活 select 消费决定)。时序:resolve 先赢得
/// 原子锁定(Decided),应答通道随后被落选分支丢弃 —— 决定无法送达 helper。
/// 修复前:expire 对 Decided 失败 → settled=false → 不发布
/// PendingAttentionRemoved(浏览器卡片永久残留),记录停在 Decided(非
/// terminal)永久滞留。修复后:竞态兜底终态化记录并照常摘除卡片;迟到
/// 命令决定仍被拒(APPROVAL_EXPIRED)。
#[tokio::test]
async fn timeout_race_with_decided_pending_still_removes_card_and_terminalizes() {
    let (runtime, mut outbox, _data_dir) = setup("race").await;
    let zcode_socket = short_socket_dir("race-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    // 浏览器订阅会话流(removed 事件只投递给已订阅的会话详情流)。
    runtime.handle_envelope(subscribe_session("sess-z1")).await;
    let _ = outbox.collect_until(2, Duration::from_secs(3)).await;

    // 登记 + 决定先赢得原子锁定(竞态时序:resolve 先于 Timeout 分支收尾)。
    let invoke = permission_invoke("e2e-race-1");
    let rx = hooks
        .registry()
        .register(
            "e2e-race-1",
            InvokeKind::PermissionRequest,
            Some("sess-z1".into()),
            Some("Bash".into()),
            None,
            None,
            None,
            std::time::Duration::from_secs(15),
        )
        .expect("登记必须成功");
    hooks
        .registry()
        .resolve("e2e-race-1", HookReply::allowed())
        .expect("决定先于 Timeout 分支收尾,必须原子锁定成功");
    // 应答已写入通道但无人送达 —— 丢弃接收端,等价于 select 落选分支丢弃 rx。
    drop(rx);
    assert_eq!(
        hooks.registry().get("e2e-race-1").map(|s| s.state),
        Some(PendingState::Decided)
    );

    // select 选中 Timeout 分支(与 handle_pending_invoke 同一收尾实现)。
    server::settle_without_decision(&hooks, &invoke, InvokeKind::PermissionRequest, true).await;

    // ① removed 事件仍发布(卡片可摘除,指向同一 invoke)。
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let removed = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::EventBatch(ref b))
                if b.events.iter().any(|e| matches!(
                    e.event,
                    Some(pb::domain_event::Event::PendingAttentionRemoved(ref r))
                        if r.native_id == "e2e-race-1"
                ))
        )
    });
    assert!(
        removed,
        "竞态下必须仍发布 PendingAttentionRemoved(修复前卡片残留): {:?}",
        envelopes.iter().map(payload_kind).collect::<Vec<_>>()
    );

    // ② 记录已终态化(可被 register 的 retain 清理,不再滞留)。
    assert_eq!(
        hooks.registry().get("e2e-race-1").map(|s| s.state),
        Some(PendingState::Expired),
        "竞态后停留 Decided 的记录必须被终态化"
    );

    // ③ 迟到命令决定仍被拒(APPROVAL_EXPIRED;与超时后点击同路径)。
    runtime
        .handle_envelope(approval_command("e2e-race-1", "allow"))
        .await;
    let envelopes = outbox.collect_until(4, Duration::from_secs(3)).await;
    let rejected_expired = envelopes.iter().any(|env| {
        matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptRejected as i32
                    && r.error_code == pb::StableErrorCode::ApprovalExpired as i32
        )
    });
    assert!(rejected_expired, "竞态终态化后迟到决定必须仍被拒");

    server_task.abort();
}

/// M3 回归:Hook 输入缺 session_id 的 PermissionRequest,审批卡片曾被发布到
/// `session_key_for(None)`("unknown" 幽灵会话,浏览器可见可点),但
/// matches_command 的绑定校验要求 Some(native_session_id) → 点击必然
/// APPROVAL_EXPIRED,45s 窗口内死路。修复后:该类审批不发布任何远程卡片/
/// 摘要事件(决定只能由 helper 超时后回退本机原生确认完成);决定命令仍
/// 不命中(既有绑定语义保持);helper 超时回退路径正常(收到 expired)。
#[tokio::test]
async fn approval_without_session_publishes_no_remote_card() {
    let (runtime, mut outbox, _data_dir) = setup("no-session").await;
    let zcode_socket = short_socket_dir("no-session-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    // 订阅幽灵会话键(修复前卡片事件发布在这里,可观测)。
    runtime.handle_envelope(subscribe_session("unknown")).await;
    let _ = outbox.collect_until(2, Duration::from_secs(3)).await;

    // 无 session_id 的 PermissionRequest(最短等待,尽快走到超时回退)。
    let mut invoke = permission_invoke("e2e-nosess-1");
    invoke.native_session_id = None;
    invoke.requested_wait_ms = contract::MIN_REMOTE_WAIT_MS;
    let (_guard, mut reader) = send_invoke(&zcode_socket, &invoke).await;
    wait_registered(&hooks, "e2e-nosess-1").await;

    // ① 不得发布审批卡片/摘要事件(修复前 PendingAttentionAdded 可见)。
    let envelopes = outbox.collect_until(1, Duration::from_secs(1)).await;
    let attention_events: Vec<String> = envelopes
        .iter()
        .filter_map(|env| match &env.payload {
            Some(envelope::Payload::EventBatch(b)) => Some(b),
            _ => None,
        })
        .flat_map(|b| b.events.iter())
        .filter_map(|e| match &e.event {
            Some(pb::domain_event::Event::PendingAttentionAdded(_)) => {
                Some("added".to_string())
            }
            Some(pb::domain_event::Event::PendingAttentionRemoved(_)) => {
                Some("removed".to_string())
            }
            Some(pb::domain_event::Event::SessionSummaryChanged(_)) => {
                Some("summary".to_string())
            }
            _ => None,
        })
        .collect();
    assert!(
        attention_events.is_empty(),
        "无 session_id 的审批不得发布远程卡片/摘要事件(修复前可见但永远无法远程决定): {attention_events:?}"
    );

    // ② 决定命令不命中(绑定语义保持):幽灵键上的 AnswerApproval 不得
    //    消费 pending。
    let ghost_key = pb::SessionKey {
        device_id: DEVICE.to_string(),
        agent_kind: pb::AgentKind::ZcodeDesktop as i32,
        native_session_id: "unknown".to_string(),
        relay_session_uuid: String::new(),
    };
    let approval = pb::command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
        approval_id: "e2e-nosess-1".into(),
        decision_id: "allow".into(),
    });
    assert!(
        !hooks.matches_command(Some(&ghost_key), Some(&approval)),
        "无 native session 的审批永远不得命中远程决定"
    );

    // ③ helper 超时回退正常:收到 expired,pending 过期(回到原生确认)。
    let reply = tokio::time::timeout(Duration::from_secs(5), read_reply(&mut reader))
        .await
        .expect("helper reply in time");
    assert_eq!(reply.status, contract::STATUS_EXPIRED);
    assert_eq!(
        hooks.registry().get("e2e-nosess-1").map(|s| s.state),
        Some(PendingState::Expired)
    );

    server_task.abort();
}

/// L1 回归:"ZCode 插件问答"空会话不得常驻列表 —— 冷启动(无任何问答)
/// 列表不含 plugin-ask;实际发生过一次 MCP 问答后才列出,且此后保留
/// (决定后仍列出,会话保留行为不变)。仅审批(有 session)不制造问答会话。
#[tokio::test]
async fn ask_session_listed_only_after_actual_ask() {
    let (runtime, _outbox, _data_dir) = setup("ask-list").await;
    let zcode_socket = short_socket_dir("ask-list-hook");
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = server::serve(
        server::HookServerConfig {
            socket_path: zcode_socket.clone(),
        },
        hooks.clone(),
    )
    .await
    .unwrap();

    fn lists_ask_session(hooks: &ZcodeHooks) -> bool {
        hooks
            .list_summaries()
            .iter()
            .any(|s| s.session_key.native_session_id == "plugin-ask")
    }

    // ① 冷启动:无任何问答,列表不含空问答会话。
    assert!(
        !lists_ask_session(&hooks),
        "冷启动不得常驻空\"ZCode 插件问答\"会话(修复前无条件列出且无法移除)"
    );

    // ② 仅审批(有 session_id):不制造问答会话。
    let (_guard, _reader) = send_invoke(&zcode_socket, &permission_invoke("e2e-list-1")).await;
    wait_registered(&hooks, "e2e-list-1").await;
    assert!(!lists_ask_session(&hooks));

    // ③ 一次真实问答登记后列出。
    let (_guard, mut reader) = send_invoke(&zcode_socket, &ask_invoke("e2e-list-ask")).await;
    wait_registered(&hooks, "e2e-list-ask").await;
    assert!(lists_ask_session(&hooks), "实际问答后会话必须列出");

    // ④ 决定后(卡片离开 waiting)会话保留列出(保留行为不变)。
    hooks
        .registry()
        .resolve("e2e-list-ask", HookReply::answered(Some("取消".into()), None))
        .unwrap();
    assert!(
        lists_ask_session(&hooks),
        "有过问答的会话必须保留列出"
    );
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .expect("helper reply in time");
    assert_eq!(reply.status, contract::STATUS_ANSWERED);

    server_task.abort();
}

fn payload_kind(env: &pb::Envelope) -> &'static str {
    match &env.payload {
        Some(pb::envelope::Payload::EventBatch(_)) => "events",
        Some(pb::envelope::Payload::Subscribed(_)) => "subscribed",
        Some(pb::envelope::Payload::RuntimeSnapshot(_)) => "snapshot",
        Some(pb::envelope::Payload::ProtocolError(_)) => "protocol_error",
        Some(pb::envelope::Payload::CommandAccepted(_)) => "accepted",
        Some(pb::envelope::Payload::CommandResult(_)) => "result",
        _ => "other",
    }
}

/// 官方事件 fixtures(仓库 integrations/zcode/fixtures)→ 合同解析 →
/// 最小状态观察映射;PermissionRequest 样本走审批路径。
/// 三个 PermissionRequest 关键样本(有/无 tool_use_id、无工具名拒绝)已由
/// contract 单测覆盖;此处锁定仓库 fixtures 与实现的同步。
#[test]
fn repo_fixtures_parse_and_map_to_minimal_metadata() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../integrations/zcode/fixtures");
    let mut checked = 0;
    let entries = std::fs::read_dir(&fixtures).expect("fixtures dir");
    for entry in entries {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let native = bridge::zcode::contract::parse_native_input(&text)
            .unwrap_or_else(|err| panic!("fixture {} 解析失败: {err}", path.display()));
        match native.hook_event_name.as_str() {
            "PermissionRequest" => {
                assert!(
                    native.tool_name.is_some(),
                    "fixture {} 缺 tool_name",
                    path.display()
                );
                let invoke = HookInvoke {
                    version: contract::CONTRACT_VERSION,
                    agent_kind: "zcode".into(),
                    invoke_id: "f-1".into(),
                    event: contract::EVENT_PERMISSION_REQUEST.into(),
                    native_session_id: native.session_id.clone(),
                    tool_name: native.tool_name.clone(),
                    tool_use_id: native.tool_use_id.clone(),
                    requested_wait_ms: 1000,
                    tool_input: native.tool_input.clone(),
                    ask: None,
                    status_event: Some("PermissionRequest".into()),
                    status_input: None,
                };
                assert_eq!(
                    bridge::zcode::observe::map_status_event(&invoke),
                    Some(bridge::zcode::observe::HookStatusEvent::PermissionRequested {
                        tool_name: native.tool_name,
                        tool_use_id: native.tool_use_id,
                    })
                );
                // 审批事件类型可分类,但真实流程不经 status 通路。
            }
            event => {
                assert!(
                    matches!(
                        event,
                        "SessionStart" | "UserPromptSubmit" | "PostToolUse"
                            | "PostToolUseFailure" | "Stop"
                    ),
                    "fixture 含未知事件 {event}"
                );
                let invoke = server::status_invoke_from_line(&text).unwrap();
                assert!(
                    bridge::zcode::observe::map_status_event(&invoke).is_some(),
                    "fixture {event} 应映射到状态枚举"
                );
            }
        }
        checked += 1;
    }
    assert!(checked >= 7, "fixtures 应至少 7 份,实际 {checked}");
}
