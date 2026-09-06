//! 双 Agent 同机同 nativeSessionId 隔离回归(ZC-02;AUTO/FIXTURE):
//!
//! Codex(静态种子 + fake-codex-owner)与 ZCode(Hook 注册表)在同机同
//! `native_session_id = "dup-1"` 时,列表、详情流、命令与查询互不串线:
//! - 列表:两类摘要同时可见,各自携带正确 agent_kind;
//! - 详情:事件按 (agentKind, native) 槽位路由,不跨流;
//! - 命令:ZCode 决定直达注册表,StartTurn/队列明确 CAPABILITY_UNSUPPORTED;
//! - 查询:ZCode 快照/历史按其真实 capability 应答。
//!
//! 零真实 ZCode/Codex 操作(与 zcode_hooks.rs 同 harness 模式)。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::agent_console::v1::{command_request, envelope, query_request};
use agent_console_protocol::codec::{new_message_id, PROTOCOL_VERSION};
use bridge::adapter::codex::{CodexAdapter, CodexAdapterConfig, CatalogThread};
use bridge::commands::{CommandGateway, PowerCoordinator, UploadLifecycle};
use bridge::config::BridgeConfig;
use bridge::domain as dm;
use bridge::files::{FileGrantManager, TransferConfig};
use bridge::git::GitService;
use bridge::local_store::LocalStore;
use bridge::runtime::{BridgeRuntime, FsUploadCleaner, OutboundSink, RuntimeParts};
use bridge::transport::{DeviceCredential, TransportError};
use bridge::zcode::contract::{self, HookInvoke, HookReply};
use bridge::zcode::{server, ZcodeHooks};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

const DEVICE: &str = "device-dual-e2e";
/// 双 Agent 故意共用的 native session id(04 §8.9 回归场景)。
const DUP_ID: &str = "dup-1";

// ---------------------------------------------------------------------------
// 回环出站 sink(harness 同 zcode_hooks.rs)
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
// harness
// ---------------------------------------------------------------------------

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-dual-{}-{}-{tag}",
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

fn short_socket_dir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(format!(
        "/tmp/ac-d-{}-{}-{tag}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("hook.sock")
}

async fn spawn_fake(dir: &Path, socket: PathBuf) -> (PathBuf, FakeGuard) {
    // fake owner 提供与 ZCode 同 ID 的 Codex 会话 "dup-1"(快照可答)。
    let script_path = dir.join("script.json");
    std::fs::write(
        &script_path,
        serde_json::json!({
            "sessions": [{
                "conversationId": DUP_ID,
                "title": "codex-dup",
                "cwd": "/tmp/ac-dual-fixture",
                "turn": {
                    "outputLines": [],
                    "lineDelayMs": 1,
                    "finalAnswer": "fixture-final",
                    "finalOutput": null,
                    "fail": false,
                    "questionAfterLine": null,
                    "question": null,
                    "approval": null
                }
            }]
        })
        .to_string(),
    )
    .unwrap();
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

struct Dual {
    runtime: Arc<BridgeRuntime>,
    gateway: CommandGateway,
    outbox: Outbox,
    hooks: Arc<ZcodeHooks>,
    _server: tokio::task::JoinHandle<()>,
    _fake: FakeGuard,
    _data_dir: PathBuf,
    zcode_socket: PathBuf,
}

async fn setup(tag: &str) -> Dual {
    let dir = temp_dir(tag);
    let ipc_socket = short_socket_dir(&format!("{tag}-ipc"));
    let (ipc_socket, fake) = spawn_fake(&dir, ipc_socket).await;
    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(ipc_socket);
    config.codex_home = Some(temp_dir(&format!("{tag}-home")));
    config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    // Codex 侧列表种子:与 ZCode 相同 native id 的会话。
    config.static_sessions = vec![CatalogThread {
        id: DUP_ID.to_string(),
        title: Some("codex-dup".to_string()),
        project_display_name: Some("codex-dup".to_string()),
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

    let data_dir = dir.join("data");
    let store = LocalStore::open(&data_dir).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let gateway_for_test = gateway.clone();
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

    let zcode_socket = short_socket_dir(&format!("{tag}-hook"));
    let hooks = Arc::new(ZcodeHooks::new(DEVICE, zcode_socket.clone()));
    hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(hooks.clone());
    let server_task = tokio::spawn({
        let hooks = hooks.clone();
        let zcode_socket = zcode_socket.clone();
        async move {
            let _ = server::serve(
                server::HookServerConfig {
                    socket_path: zcode_socket,
                },
                hooks,
            )
            .await;
        }
    });
    // server::serve 绑定是异步的:等待 socket 文件就绪再继续。
    for _ in 0..250 {
        if zcode_socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(zcode_socket.exists(), "zcode hook socket did not appear");

    Dual {
        runtime,
        gateway: gateway_for_test,
        outbox: Outbox { rx },
        hooks,
        _server: server_task,
        _fake: fake,
        _data_dir: dir,
        zcode_socket,
    }
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
        native_session_id: Some(DUP_ID.to_string()),
        tool_name: Some("Bash".to_string()),
        tool_use_id: None,
        requested_wait_ms: 20_000,
        tool_input: Some(serde_json::json!({"command": "echo fixture"})),
        ask: None,
        status_event: None,
        status_input: None,
    }
}

async fn send_invoke(
    socket: &Path,
    invoke: &HookInvoke,
) -> (
    tokio::net::unix::OwnedWriteHalf,
    BufReader<tokio::net::unix::OwnedReadHalf>,
) {
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
// 入站信封构造
// ---------------------------------------------------------------------------

fn session_key_of(agent: pb::AgentKind, native: &str) -> pb::SessionKey {
    pb::SessionKey {
        device_id: DEVICE.to_string(),
        agent_kind: agent as i32,
        native_session_id: native.to_string(),
        relay_session_uuid: String::new(),
    }
}

fn inbound(
    stream_id: &str,
    correlation: &str,
    payload: pb::envelope::Payload,
) -> pb::Envelope {
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
        stream_id: stream_id.to_string(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(payload),
        provider_extension: None,
    }
}

fn subscribe_session(agent: pb::AgentKind, native: &str, upstream: &str) -> pb::Envelope {
    inbound(
        upstream,
        "corr-sub",
        envelope::Payload::Subscribe(pb::Subscribe {
            target: Some(pb::subscribe::Target::Session(session_key_of(agent, native))),
        }),
    )
}

fn command(
    agent: pb::AgentKind,
    native: &str,
    operation: pb::Operation,
    payload: command_request::Payload,
) -> pb::Envelope {
    inbound(
        "",
        "corr-cmd",
        envelope::Payload::CommandRequest(pb::CommandRequest {
            request_id: Uuid::new_v4().to_string(),
            operation: operation as i32,
            session_key: Some(session_key_of(agent, native)),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: String::new(),
            payload: Some(payload),
        }),
    )
}

fn query(agent: pb::AgentKind, native: &str, correlation: &str, query: query_request::Query) -> pb::Envelope {
    inbound(
        "",
        correlation,
        envelope::Payload::QueryRequest(pb::QueryRequest {
            session_key: Some(session_key_of(agent, native)),
            query: Some(query),
        }),
    )
}

// ---------------------------------------------------------------------------
// 场景 1:列表 —— 同 ID 双 Agent 摘要并存且各自携带正确 agent_kind
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_stream_keeps_dual_agent_summaries_distinct() {
    let mut dual = setup("list").await;
    // 触发 ZCode 侧同 ID 会话的审批(publish attention + summary)。
    let (_guard, _reader) = send_invoke(&dual.zcode_socket, &permission_invoke("z-list-1")).await;
    wait_registered(&dual.hooks, "z-list-1").await;
    dual.outbox.collect_until(2, Duration::from_secs(2)).await;

    dual.runtime
        .handle_envelope(inbound(
            "",
            "corr-list",
            envelope::Payload::Subscribe(pb::Subscribe {
                target: Some(pb::subscribe::Target::List(pb::SessionList {})),
            }),
        ))
        .await;
    let envelopes = dual.outbox.collect_until(4, Duration::from_secs(4)).await;
    let batch = envelopes.iter().find_map(|env| match &env.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) if b.snapshot => Some(b.clone()),
        _ => None,
    });
    let Some(batch) = batch else {
        panic!("expected list snapshot batch");
    };

    let codex_dup = batch.summaries.iter().find(|s| {
        s.agent_kind == pb::AgentKind::CodexDesktop as i32
            && s.session_key
                .as_ref()
                .is_some_and(|k| k.native_session_id == DUP_ID)
    });
    let zcode_dup = batch.summaries.iter().find(|s| {
        s.agent_kind == pb::AgentKind::ZcodeDesktop as i32
            && s.session_key
                .as_ref()
                .is_some_and(|k| k.native_session_id == DUP_ID)
    });
    let codex = codex_dup.expect("codex 同 ID 摘要必须在列表中");
    let zcode = zcode_dup.expect("zcode 同 ID 摘要必须在列表中");
    assert_ne!(
        codex.session_key.as_ref().unwrap().agent_kind,
        zcode.session_key.as_ref().unwrap().agent_kind,
    );
    // ZCode 摘要如实镜像 pending:attention=1(审批),能力来源为 Hook 通路。
    assert_eq!(zcode.pending_attention_count, 1);
    assert_eq!(
        zcode.pending_attention_kinds,
        vec![pb::PendingAttentionKind::AttentionRiskApproval as i32]
    );
    assert_eq!(
        zcode.control_mode,
        pb::ControlMode::LimitedControl as i32,
        "ZCode 控制模式必须如实 LIMITED(官方 Hook)"
    );
    dual._server.abort();
}

// ---------------------------------------------------------------------------
// 场景 2:详情 —— 事件按 (agentKind, native) 槽位路由,不跨流
// ---------------------------------------------------------------------------

#[tokio::test]
async fn detail_streams_route_events_by_agent_kind() {
    let mut dual = setup("detail").await;
    // 订阅两个同 ID 详情流:上游流 ID 分别回显,便于归属。
    dual.runtime
        .handle_envelope(subscribe_session(pb::AgentKind::CodexDesktop, DUP_ID, "u-codex-dup"))
        .await;
    dual.runtime
        .handle_envelope(subscribe_session(pb::AgentKind::ZcodeDesktop, DUP_ID, "u-zcode-dup"))
        .await;
    // 等 Subscribed×2 + 快照×2(事件归属前先建立流)。
    let envelopes = dual.outbox.collect_until(6, Duration::from_secs(6)).await;
    let subscribed: Vec<String> = envelopes
        .iter()
        .filter_map(|env| match &env.payload {
            Some(envelope::Payload::Subscribed(s)) => Some(s.stream_id.clone()),
            _ => None,
        })
        .collect();
    assert!(
        subscribed.contains(&"u-codex-dup".to_string())
            && subscribed.contains(&"u-zcode-dup".to_string()),
        "两条详情流都必须建立: {subscribed:?}"
    );

    // ZCode 侧事件:同 ID 会话的审批请求。
    let (_guard, _reader) = send_invoke(&dual.zcode_socket, &permission_invoke("z-detail-1")).await;
    wait_registered(&dual.hooks, "z-detail-1").await;
    let envelopes = dual.outbox.collect_until(6, Duration::from_secs(4)).await;
    let codex_events: Vec<_> = envelopes
        .iter()
        .filter(|env| env.stream_id == "u-codex-dup")
        .filter_map(|env| match &env.payload {
            Some(envelope::Payload::EventBatch(b)) => Some(b.clone()),
            _ => None,
        })
        .collect();
    let zcode_events: Vec<_> = envelopes
        .iter()
        .filter(|env| env.stream_id == "u-zcode-dup")
        .filter_map(|env| match &env.payload {
            Some(envelope::Payload::EventBatch(b)) => Some(b.clone()),
            _ => None,
        })
        .collect();
    assert!(
        zcode_events.iter().any(|b| b.events.iter().any(|e| matches!(
            e.event,
            Some(pb::domain_event::Event::PendingAttentionAdded(_))
        ))),
        "ZCode 流必须收到审批卡片事件"
    );
    assert!(
        codex_events.is_empty(),
        "Codex 同 ID 流不得收到 ZCode 事件: {codex_events:?}"
    );
    dual._server.abort();
}

// ---------------------------------------------------------------------------
// 场景 3:命令与查询 —— 决定直达注册表,其余明确 NOT_SUPPORTED
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commands_and_queries_are_kind_scoped() {
    let mut dual = setup("cmd").await;
    let (_guard, mut reader) =
        send_invoke(&dual.zcode_socket, &permission_invoke("z-cmd-1")).await;
    wait_registered(&dual.hooks, "z-cmd-1").await;

    // ① ZCode 决定(同 ID,kind=ZCODE_DESKTOP)→ COMPLETED,helper 收到 allowed。
    dual.runtime
        .handle_envelope(command(
            pb::AgentKind::ZcodeDesktop,
            DUP_ID,
            pb::Operation::AnswerApproval,
            command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
                approval_id: "z-cmd-1".to_string(),
                decision_id: "allow".to_string(),
            }),
        ))
        .await;
    let reply = tokio::time::timeout(Duration::from_secs(3), read_reply(&mut reader))
        .await
        .expect("helper reply in time");
    assert_eq!(reply.status, contract::STATUS_ALLOWED);
    let envelopes = dual.outbox.collect_until(4, Duration::from_secs(3)).await;
    assert!(
        envelopes.iter().any(|env| matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
        )),
        "ZCode 决定必须 COMPLETED"
    );

    // ② StartTurn on ZCode 会话 → 明确 CAPABILITY_UNSUPPORTED(不假装成功)。
    dual.runtime
        .handle_envelope(command(
            pb::AgentKind::ZcodeDesktop,
            DUP_ID,
            pb::Operation::StartTurn,
            command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: "fixture".to_string(),
            }),
        ))
        .await;
    let envelopes = dual.outbox.collect_until(2, Duration::from_secs(3)).await;
    assert!(
        envelopes.iter().any(|env| matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptRejected as i32
                    && r.error_code == pb::StableErrorCode::CapabilityUnsupported as i32
        )),
        "ZCode 会话 StartTurn 必须 CAPABILITY_UNSUPPORTED"
    );

    // ③ QueueNextTurn on ZCode 会话 → 同样明确不支持。
    dual.runtime
        .handle_envelope(command(
            pb::AgentKind::ZcodeDesktop,
            DUP_ID,
            pb::Operation::QueueSet,
            command_request::Payload::QueueSet(pb::QueueSetPayload {
                prompt: "fixture".to_string(),
                after_turn_id: None,
                runtime_revision: 0,
            }),
        ))
        .await;
    let envelopes = dual.outbox.collect_until(2, Duration::from_secs(3)).await;
    assert!(
        envelopes.iter().any(|env| matches!(
            env.payload,
            Some(envelope::Payload::CommandResult(ref r))
                if r.status == pb::CommandReceiptStatus::ReceiptRejected as i32
                    && r.error_code == pb::StableErrorCode::CapabilityUnsupported as i32
        )),
        "ZCode 会话队列必须 CAPABILITY_UNSUPPORTED"
    );

    // ④ gateway 直连兜底:绕过 runtime 分派也不得借道 Codex。
    let err = dual
        .gateway
        .submit(dm::CommandRequest {
            request_id: Uuid::new_v4(),
            operation: dm::Operation::StartTurn,
            session_key: dm::SessionKey::zcode(DEVICE, DUP_ID),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: dm::CommandPayload::StartTurn {
                input: dm::OutputText::new("fixture-go"),
            },
        })
        .await
        .expect_err("gateway must reject zcode start-turn");
    assert_eq!(err.code, dm::StableErrorCode::CapabilityUnsupported);

    // ⑤ 快照查询:ZCode 返回其真实 capability(仅 Hook 决定)。
    dual.runtime
        .handle_envelope(query(
            pb::AgentKind::ZcodeDesktop,
            DUP_ID,
            "q-zcode-snap",
            query_request::Query::RuntimeSnapshot(pb::RuntimeSnapshotQuery {}),
        ))
        .await;
    let envelopes = dual.outbox.collect_until(2, Duration::from_secs(3)).await;
    let snap = envelopes.iter().find_map(|env| match &env.payload {
        Some(envelope::Payload::QueryResponse(r)) if r.request_id == "q-zcode-snap" => {
            match r.result.as_ref() {
                Some(pb::query_response::Result::RuntimeSnapshot(snap)) => Some(snap.clone()),
                _ => None,
            }
        }
        _ => None,
    });
    let snap = snap.expect("zcode 快照必须可得(注册表镜像)");
    let supported = snap
        .capabilities
        .as_ref()
        .map(|c| c.supported_operations.clone())
        .unwrap_or_default();
    assert!(supported.contains(&(pb::Operation::AnswerApproval as i32)));
    assert!(supported.contains(&(pb::Operation::AnswerQuestion as i32)));
    assert!(!supported.contains(&(pb::Operation::StartTurn as i32)));

    // ⑥ 历史查询:明确 CAPABILITY_UNSUPPORTED,不以 200 空页伪装。
    dual.runtime
        .handle_envelope(query(
            pb::AgentKind::ZcodeDesktop,
            DUP_ID,
            "q-zcode-history",
            query_request::Query::HistoryPage(pb::HistoryPageQuery {
                cursor: String::new(),
                page_size: 0,
            }),
        ))
        .await;
    let envelopes = dual.outbox.collect_until(2, Duration::from_secs(3)).await;
    assert!(
        envelopes.iter().any(|env| matches!(
            env.payload,
            Some(envelope::Payload::QueryResponse(ref r))
                if r.request_id == "q-zcode-history"
                    && r.result.is_none()
                    && r.error_code == pb::StableErrorCode::CapabilityUnsupported as i32
        )),
        "ZCode 历史必须显式 NOT_SUPPORTED"
    );
    dual._server.abort();
}
