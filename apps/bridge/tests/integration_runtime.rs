//! BridgeRuntime 集成测试(权威规格 §29.4 最小链,fake-codex-owner + 回环
//! 出站 sink + 测试内假 Relay producer,不起真实 WSS/Codex):
//!
//! 1. 启动 → list 订阅 → Subscribed + SessionSummaryBatch 全量快照。
//! 2. 详情订阅 → RuntimeSnapshot → fake owner 编号输出 → OutputAppend →
//!    turn 完成 → final 校正 OutputReplace(脚本 finalOutput ≠ 预览);
//!    CommandRequest → ACCEPTED_BY_BRIDGE → Completed 回执序。
//! 3. 队列:RUNNING 中 QueueNextTurn → turn 完成后自动发送第二 turn。
//! 4. QueryRequest git_summary:会话 cwd 指向临时 git 仓库(已授权根)。
//! 5. 文件 preview TransferOffer 全链路:假 Relay 接 producer POST,
//!    断言 §22.3 信息性头与字节。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::agent_console::v1::{
    command_request, envelope, query_request, subscribe,
};
use agent_console_protocol::codec::{new_message_id, PROTOCOL_VERSION};
use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::commands::{CommandGateway, PowerCoordinator, UploadLifecycle};
use bridge::config::BridgeConfig;
use bridge::domain::SessionKey;
use bridge::files::{
    FileGrantManager, GrantActions, GrantSource, TransferConfig, DEFAULT_GRANT_TTL,
};
use bridge::git::GitService;
use bridge::local_store::LocalStore;
use bridge::power::WakePolicy;
use bridge::runtime::{BridgeRuntime, FsUploadCleaner, OutboundSink, RuntimeParts};
use bridge::transport::{DeviceCredential, TransportError};
use serde_json::json;
use uuid::Uuid;

const DEVICE: &str = "device-runtime";
const CONV_FAST: &str = "bbbbbbbb-1111-4111-8111-111111111111";
const CONV_QUEUE: &str = "bbbbbbbb-2222-4222-8222-222222222222";
const CONV_GIT: &str = "bbbbbbbb-3333-4333-8333-333333333333";
const CONV_FILE: &str = "bbbbbbbb-4444-4444-8444-444444444444";

// ---------------------------------------------------------------------------
// 回环出站 sink 与收集器
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

/// 静态凭据(Debug 不泄露 token;token 为测试固定值)。
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
    async fn collect_until(
        &mut self,
        deadline: Duration,
        mut done: impl FnMut(&[pb::Envelope]) -> bool,
    ) -> Vec<pb::Envelope> {
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
            match tokio::time::timeout(remaining.min(Duration::from_millis(50)), self.rx.recv())
                .await
            {
                Ok(Some(envelope)) => events.push(envelope),
                Ok(None) => return events,
                Err(_) => {}
            }
        }
    }

    fn payload_of<'a>(
        events: &'a [pb::Envelope],
    ) -> impl Iterator<Item = &'a pb::envelope::Payload> {
        events.iter().filter_map(|e| e.payload.as_ref())
    }

    fn find_subscribed<'a>(events: &'a [pb::Envelope]) -> Option<&'a pb::Subscribed> {
        Self::payload_of(events).find_map(|p| match p {
            pb::envelope::Payload::Subscribed(s) => Some(s),
            _ => None,
        })
    }

    fn find_summary_batch<'a>(events: &'a [pb::Envelope]) -> Option<&'a pb::SessionSummaryBatch> {
        Self::payload_of(events).find_map(|p| match p {
            pb::envelope::Payload::SessionSummaryBatch(b) => Some(b),
            _ => None,
        })
    }

    fn event_batches<'a>(events: &'a [pb::Envelope]) -> Vec<&'a pb::EventBatch> {
        Self::payload_of(events)
            .filter_map(|p| match p {
                pb::envelope::Payload::EventBatch(b) => Some(b),
                _ => None,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// fake owner / 装配
// ---------------------------------------------------------------------------

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ac-runtime-{}-{}-{tag}",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_fake(dir: &Path, script: serde_json::Value) -> (PathBuf, std::process::Child) {
    let socket = PathBuf::from(format!(
        "/tmp/ac-rt-{}-{}.sock",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    ));
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
        if let Ok(Some(status)) = child.try_wait() {
            panic!("fake-codex-owner exited before binding socket: {status}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");
    (socket, child)
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

struct TestCtx {
    runtime: Arc<BridgeRuntime>,
    outbox: Outbox,
    store: LocalStore,
    _fake_guard: FakeGuard,
}

struct FakeGuard(std::process::Child);
impl Drop for FakeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn setup(tag: &str, script: serde_json::Value, seeds: Vec<CatalogThread>) -> TestCtx {
    setup_with_relay(tag, script, seeds, "ws://127.0.0.1:1").await
}

async fn setup_with_relay(
    tag: &str,
    script: serde_json::Value,
    seeds: Vec<CatalogThread>,
    relay_url: &str,
) -> TestCtx {
    let dir = temp_dir(tag);
    let (socket, child) = spawn_fake(&dir, script).await;

    let mut config = CodexAdapterConfig::new(DEVICE);
    config.ipc_socket = Some(socket);
    config.codex_home = Some(temp_dir(&format!("{tag}-home")));
    config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    config.write_method_probes = bridge::capabilities::WriteMethodProbes {
        start_turn: bridge::capabilities::ProbeResult::Passed,
        steer_turn: bridge::capabilities::ProbeResult::Passed,
        interrupt_turn: bridge::capabilities::ProbeResult::Passed,
        answer_question: bridge::capabilities::ProbeResult::Passed,
        update_settings: bridge::capabilities::ProbeResult::Passed,
    };
    config.static_sessions = seeds;
    let adapter = CodexAdapter::connect(config).await.unwrap();

    let data_dir = dir.join("data");
    let store = LocalStore::open(&data_dir).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let grants = FileGrantManager::new();
    let git = GitService::new().ok().map(Arc::new);
    let uploads = Arc::new(UploadLifecycle::new(
        Arc::new(FsUploadCleaner::new(data_dir.join("uploads"))),
        store.clone(),
    ));
    // 测试注入 /bin/true 作为 caffeinate 替身:断言状态推进但无真实断言副作用。
    let power = PowerCoordinator::new(
        Arc::new(WakePolicy::with_program(PathBuf::from("/bin/true"))),
        bridge::power::PowerSource::Ac,
    );

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Arc::new(BridgeRuntime::new(RuntimeParts {
        config: BridgeConfig::from_env()
            .with_data_dir(data_dir.clone())
            .with_relay_url(relay_url),
        device_id: DEVICE.to_string(),
        store: store.clone(),
        adapter,
        gateway,
        grants,
        git,
        uploads,
        power,
        credential: Arc::new(TestCredential),
        outbound: Arc::new(LoopbackSink { tx }),
        http: reqwest::Client::new(),
        upload_root: data_dir.join("uploads"),
        transfer_config: TransferConfig::default(),
    }));
    runtime.start_observation();
    TestCtx {
        runtime,
        outbox: Outbox { rx },
        store,
        _fake_guard: FakeGuard(child),
    }
}

fn subscribe_list() -> pb::Envelope {
    inbound_envelope(
        pb::envelope::Payload::Subscribe(pb::Subscribe {
            target: Some(subscribe::Target::List(pb::SessionList {})),
        }),
        "corr-list",
    )
}

fn subscribe_session(conversation: &str) -> pb::Envelope {
    let key = key(conversation);
    inbound_envelope(
        pb::envelope::Payload::Subscribe(pb::Subscribe {
            target: Some(subscribe::Target::Session(pb::SessionKey {
                device_id: key.device_id,
                agent_kind: pb::AgentKind::CodexDesktop as i32,
                native_session_id: key.native_session_id,
                relay_session_uuid: String::new(),
            })),
        }),
        "corr-session",
    )
}

fn resync_stream(stream_id: &str) -> pb::Envelope {
    inbound_envelope(
        pb::envelope::Payload::ResyncRequest(pb::ResyncRequest {
            stream_id: stream_id.to_string(),
        }),
        "corr-resync",
    )
}

fn output_query(conversation: &str, item_id: &str, page_size: u32) -> pb::Envelope {
    inbound_envelope(
        pb::envelope::Payload::QueryRequest(pb::QueryRequest {
            session_key: Some(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::CodexDesktop as i32,
                native_session_id: conversation.to_string(),
                relay_session_uuid: String::new(),
            }),
            query: Some(query_request::Query::CommandOutputPage(
                pb::CommandOutputPageQuery {
                    item_id: Some(pb::ItemId {
                        id: item_id.to_string(),
                        synthetic: false,
                    }),
                    cursor: String::new(),
                    page_size,
                },
            )),
        }),
        "corr-output",
    )
}

fn runtime_query(conversation: &str) -> pb::Envelope {
    inbound_envelope(
        pb::envelope::Payload::QueryRequest(pb::QueryRequest {
            session_key: Some(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::CodexDesktop as i32,
                native_session_id: conversation.to_string(),
                relay_session_uuid: String::new(),
            }),
            query: Some(query_request::Query::RuntimeSnapshot(
                pb::RuntimeSnapshotQuery {},
            )),
        }),
        "corr-runtime",
    )
}

fn start_turn_request(conversation: &str, prompt: &str, correlation: &str) -> pb::Envelope {
    inbound_envelope(
        pb::envelope::Payload::CommandRequest(pb::CommandRequest {
            request_id: Uuid::new_v4().to_string(),
            operation: pb::Operation::StartTurn as i32,
            session_key: Some(pb::SessionKey {
                device_id: DEVICE.to_string(),
                agent_kind: pb::AgentKind::CodexDesktop as i32,
                native_session_id: conversation.to_string(),
                relay_session_uuid: String::new(),
            }),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: String::new(),
            payload: Some(command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: prompt.to_string(),
            })),
        }),
        correlation,
    )
}

// ---------------------------------------------------------------------------
// 1. list 订阅
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_subscribe_delivers_summary_batch() {
    let mut ctx = setup(
        "list",
        json!({"sessions": [{"conversationId": CONV_FAST, "title": "fixture-list", "cwd": "/tmp/fixture-list"}]}),
        vec![seed(CONV_FAST, "fixture-list")],
    )
    .await;
    ctx.runtime.handle_envelope(subscribe_list()).await;

    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::find_summary_batch(events).is_some()
                && Outbox::find_subscribed(events).is_some()
        })
        .await;

    let subscribed = Outbox::find_subscribed(&events).expect("Subscribed 缺失");
    assert_eq!(subscribed.stream_id, "list");
    assert_eq!(subscribed.base_sequence, 1);
    assert_eq!(subscribed.stream_epoch, 1, "首个流纪元从 1 起");

    let (batch_env, batch) = events
        .iter()
        .find_map(|e| match e.payload.as_ref() {
            Some(envelope::Payload::SessionSummaryBatch(b)) => Some((e, b)),
            _ => None,
        })
        .expect("SessionSummaryBatch 缺失");
    assert!(batch.snapshot, "首响应为全量快照");
    assert_eq!(batch.summaries.len(), 1);
    let summary = &batch.summaries[0];
    assert_eq!(
        summary.agent_kind,
        pb::AgentKind::CodexDesktop as i32,
        "agent_kind 恒 CODEX_DESKTOP"
    );
    assert_eq!(
        summary.session_key.as_ref().unwrap().native_session_id,
        CONV_FAST
    );
    assert_eq!(summary.queue_state, pb::QueueState::Empty as i32);
    // §17.4:快照序 = base_sequence;epoch 与 Subscribed 一致。
    assert_eq!(batch_env.sequence, subscribed.base_sequence);
    assert_eq!(batch_env.stream_epoch, subscribed.stream_epoch);
    assert_eq!(batch_env.stream_id, "list");
    ctx.runtime.shutdown().await;
}

// ---------------------------------------------------------------------------
// 2. 详情订阅 + 命令回执 + 编号输出 + 权威校正
// ---------------------------------------------------------------------------

#[tokio::test]
async fn detail_pipeline_outputs_and_command_receipts() {
    let mut ctx = setup(
        "detail",
        json!({"sessions": [{
            "conversationId": CONV_FAST,
            "title": "fixture-detail",
            "cwd": "/tmp/fixture-detail",
            "turn": {
                "outputLines": ["1", "2", "3", "4", "5"],
                "lineDelayMs": 120,
                "finalAnswer": "fixture-final-answer",
                "finalOutput": "corrected-authoritative-output\n"
            }
        }]}),
        vec![seed(CONV_FAST, "fixture-detail")],
    )
    .await;

    // 详情订阅 → Subscribed + RuntimeSnapshot。
    ctx.runtime
        .handle_envelope(subscribe_session(CONV_FAST))
        .await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::find_subscribed(events).is_some()
                && Outbox::payload_of(events)
                    .any(|p| matches!(p, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;
    let subscribed = Outbox::find_subscribed(&events).expect("Subscribed 缺失");
    assert_eq!(subscribed.stream_id, format!("session:{CONV_FAST}"));
    assert!(
        Outbox::payload_of(&events).any(|p| matches!(p, envelope::Payload::RuntimeSnapshot(_))),
        "订阅后先发 RuntimeSnapshot(§17.4)"
    );

    // CommandRequest → CommandAccepted(ACCEPTED_BY_BRIDGE) → 回执流。
    ctx.runtime
        .handle_envelope(start_turn_request(CONV_FAST, "fixture-go", "corr-cmd"))
        .await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(10), |events| {
            let accepted = Outbox::payload_of(events).any(|p| {
                matches!(
                    p,
                    envelope::Payload::CommandAccepted(a)
                        if a.status == pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32
                )
            });
            let completed = Outbox::payload_of(events).any(|p| {
                matches!(
                    p,
                    envelope::Payload::CommandResult(r)
                        if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
                )
            });
            accepted && completed
        })
        .await;
    // 回执序:ACCEPTED_BY_BRIDGE 先于(或同批早于)最终 Completed;correlation 回填。
    let accepted_index = events
        .iter()
        .position(|e| matches!(e.payload, Some(envelope::Payload::CommandAccepted(_))))
        .expect("CommandAccepted 缺失");
    let accepted = match events[accepted_index].payload.as_ref().unwrap() {
        envelope::Payload::CommandAccepted(a) => a,
        _ => unreachable!(),
    };
    assert_eq!(
        accepted.status,
        pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32
    );
    assert_eq!(events[accepted_index].correlation_id, "corr-cmd");
    let completed = Outbox::payload_of(&events)
        .find_map(|p| match p {
            envelope::Payload::CommandResult(r)
                if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32 =>
            {
                Some(r)
            }
            _ => None,
        })
        .expect("Completed 回执缺失");
    assert_eq!(completed.request_id, accepted.request_id);
    let dispatched = Outbox::payload_of(&events).any(|p| {
        matches!(
            p,
            envelope::Payload::CommandResult(r)
                if r.status == pb::CommandReceiptStatus::ReceiptDispatchedToCodex as i32
        )
    });
    assert!(dispatched, "应有 DISPATCHED_TO_CODEX 状态回执");

    // 事件流:编号输出 1..5 无缺号 + 权威 OutputReplace 校正。
    let events =
        ctx.outbox
            .collect_until(Duration::from_secs(12), |events| {
                let batches = Outbox::event_batches(events);
                let has_final = batches.iter().any(|b| {
                    b.events
                        .iter()
                        .any(|e| matches!(e.event, Some(pb::domain_event::Event::OutputFinal(_))))
                });
                let has_replace = batches.iter().any(|b| {
                    b.events.iter().any(|e| {
                        matches!(
                            e.event,
                            Some(pb::domain_event::Event::OutputReplace(ref r))
                                if r.content
                                    == Some(pb::output_replace::Content::Bytes(
                                        b"corrected-authoritative-output\n".to_vec(),
                                    ))
                        )
                    })
                });
                let idle_completed =
                    batches.iter().any(|b| {
                        b.events.iter().any(|e| matches!(
                    e.event,
                    Some(pb::domain_event::Event::TurnLifecycle(ref t))
                        if t.phase == pb::ActiveTurnPhase::TurnPhaseIdle as i32
                            && t.outcome == pb::LastTurnOutcome::TurnOutcomeCompleted as i32
                ))
                    });
                has_final && has_replace && idle_completed
            })
            .await;

    let mut concatenated = String::new();
    let mut expected_offset = 0u64;
    let mut final_length = None;
    for batch in Outbox::event_batches(&events) {
        for event in &batch.events {
            match event.event.as_ref().unwrap() {
                pb::domain_event::Event::OutputAppend(a) => {
                    assert_eq!(a.expected_offset, expected_offset, "编号输出无缺号(§13.4)");
                    concatenated.push_str(std::str::from_utf8(&a.bytes).unwrap());
                    expected_offset += a.bytes.len() as u64;
                }
                pb::domain_event::Event::OutputFinal(f) => {
                    final_length = Some(f.byte_length);
                }
                _ => {}
            }
        }
    }
    assert_eq!(concatenated, "1\n2\n3\n4\n5\n");
    assert_eq!(
        final_length,
        Some("corrected-authoritative-output\n".len() as u64),
        "OutputFinal 长度 = 权威校正后内容(§13.3)"
    );
    // sequence 单调性:同流内严格递增、无重复。
    let mut last_sequence = 0u64;
    for e in &events {
        if !e.stream_id.is_empty() {
            assert!(e.sequence > last_sequence, "sequence 必须单调(§17.5)");
            last_sequence = e.sequence;
        }
    }
    ctx.runtime.shutdown().await;
}

#[tokio::test]
async fn detail_resync_starts_new_epoch_with_fresh_snapshot_first() {
    let mut ctx = setup(
        "detail-resync",
        json!({"sessions": [{
            "conversationId": CONV_FAST,
            "title": "fixture-resync",
            "cwd": "/tmp/fixture-resync"
        }]}),
        vec![seed(CONV_FAST, "fixture-resync")],
    )
    .await;

    ctx.runtime
        .handle_envelope(subscribe_session(CONV_FAST))
        .await;
    let first = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::find_subscribed(events).is_some()
                && Outbox::payload_of(events)
                    .any(|payload| matches!(payload, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;
    let first_subscribed = Outbox::find_subscribed(&first).expect("first Subscribed");
    let first_revision = Outbox::payload_of(&first)
        .find_map(|payload| match payload {
            envelope::Payload::RuntimeSnapshot(snapshot) => Some(snapshot.runtime_revision),
            _ => None,
        })
        .expect("first RuntimeSnapshot");

    ctx.runtime
        .handle_envelope(resync_stream(&first_subscribed.stream_id))
        .await;
    let refreshed = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::find_subscribed(events).is_some()
                && Outbox::payload_of(events)
                    .any(|payload| matches!(payload, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;
    let refreshed_subscribed = Outbox::find_subscribed(&refreshed).expect("refreshed Subscribed");
    let (snapshot_index, snapshot_env, snapshot) = refreshed
        .iter()
        .enumerate()
        .find_map(|(index, env)| match env.payload.as_ref() {
            Some(envelope::Payload::RuntimeSnapshot(snapshot)) => Some((index, env, snapshot)),
            _ => None,
        })
        .expect("refreshed RuntimeSnapshot");

    assert!(refreshed_subscribed.stream_epoch > first_subscribed.stream_epoch);
    assert!(
        snapshot.runtime_revision > first_revision,
        "resync 必须重读 owner"
    );
    assert_eq!(snapshot_env.stream_epoch, refreshed_subscribed.stream_epoch);
    assert_eq!(snapshot_env.sequence, refreshed_subscribed.base_sequence);
    assert!(
        refreshed[..snapshot_index].iter().all(|env| {
            env.stream_epoch != refreshed_subscribed.stream_epoch || env.sequence == 0
        }),
        "新 epoch 内 RuntimeSnapshot 必须是首个有序帧"
    );
    let sequenced: Vec<u64> = refreshed
        .iter()
        .filter(|env| env.stream_epoch == refreshed_subscribed.stream_epoch && env.sequence > 0)
        .map(|env| env.sequence)
        .collect();
    assert!(
        sequenced.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "新 epoch sequence 必须连续: {sequenced:?}"
    );
    ctx.runtime.shutdown().await;
}

#[tokio::test]
async fn runtime_query_refreshes_the_desktop_owner_snapshot() {
    let mut ctx = setup(
        "runtime-query-refresh",
        json!({"sessions": [{
            "conversationId": CONV_FAST,
            "title": "fixture-query-refresh",
            "cwd": "/tmp/fixture-query-refresh"
        }]}),
        vec![seed(CONV_FAST, "fixture-query-refresh")],
    )
    .await;

    ctx.runtime
        .handle_envelope(subscribe_session(CONV_FAST))
        .await;
    let initial = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;
    let initial_revision = Outbox::payload_of(&initial)
        .find_map(|payload| match payload {
            envelope::Payload::RuntimeSnapshot(snapshot) => Some(snapshot.runtime_revision),
            _ => None,
        })
        .expect("initial RuntimeSnapshot");

    ctx.runtime.handle_envelope(runtime_query(CONV_FAST)).await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::QueryResponse(_)))
        })
        .await;
    let response = Outbox::payload_of(&events)
        .find_map(|payload| match payload {
            envelope::Payload::QueryResponse(response) => Some(response),
            _ => None,
        })
        .expect("runtime QueryResponse");
    assert_eq!(response.error_code, 0);
    let pb::query_response::Result::RuntimeSnapshot(snapshot) =
        response.result.as_ref().expect("runtime result")
    else {
        panic!("expected RuntimeSnapshot")
    };
    assert!(
        snapshot.runtime_revision > initial_revision,
        "HTTP runtime query 不得返回打开页面时的旧缓存"
    );
    ctx.runtime.shutdown().await;
}

#[tokio::test]
async fn runtime_query_accepts_unchanged_authoritative_revision() {
    let mut ctx = setup(
        "runtime-query-stable",
        json!({"sessions": [{
            "conversationId": CONV_FAST,
            "title": "fixture-query-stable",
            "cwd": "/tmp/fixture-query-stable",
            "stableHistoryRefresh": true
        }]}),
        vec![seed(CONV_FAST, "fixture-query-stable")],
    )
    .await;

    ctx.runtime
        .handle_envelope(subscribe_session(CONV_FAST))
        .await;
    let initial = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;
    let initial_revision = Outbox::payload_of(&initial)
        .find_map(|payload| match payload {
            envelope::Payload::RuntimeSnapshot(snapshot) => Some(snapshot.runtime_revision),
            _ => None,
        })
        .expect("initial RuntimeSnapshot");

    ctx.runtime.handle_envelope(runtime_query(CONV_FAST)).await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(1), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::QueryResponse(_)))
        })
        .await;
    let response = Outbox::payload_of(&events)
        .find_map(|payload| match payload {
            envelope::Payload::QueryResponse(response) => Some(response),
            _ => None,
        })
        .expect("runtime QueryResponse");
    assert_eq!(response.error_code, 0);
    let pb::query_response::Result::RuntimeSnapshot(snapshot) =
        response.result.as_ref().expect("runtime result")
    else {
        panic!("expected RuntimeSnapshot")
    };
    assert_eq!(snapshot.runtime_revision, initial_revision);
    ctx.runtime.shutdown().await;
}

#[tokio::test]
async fn preexisting_snapshot_output_is_served_without_resync_error() {
    let mut ctx = setup(
        "snapshot-output",
        json!({"sessions": [{
            "conversationId": CONV_FAST,
            "title": "fixture-output",
            "cwd": "/tmp/fixture-output",
            "turns": [{
                "turnId": "turn-existing",
                "status": "completed",
                "items": [{
                    "id": "item-existing-command",
                    "type": "commandExecution",
                    "command": "fixture-existing",
                    "status": "completed",
                    "aggregatedOutput": "abcdef"
                }]
            }]
        }]}),
        vec![seed(CONV_FAST, "fixture-output")],
    )
    .await;

    ctx.runtime
        .handle_envelope(subscribe_session(CONV_FAST))
        .await;
    let _ = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;

    ctx.runtime
        .handle_envelope(output_query(CONV_FAST, "item-existing-command", 3))
        .await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events)
                .any(|payload| matches!(payload, envelope::Payload::QueryResponse(_)))
        })
        .await;
    let response = Outbox::payload_of(&events)
        .find_map(|payload| match payload {
            envelope::Payload::QueryResponse(response) => Some(response),
            _ => None,
        })
        .expect("output QueryResponse");
    assert_eq!(response.error_code, 0);
    let pb::query_response::Result::CommandOutputPage(page) =
        response.result.as_ref().expect("output result")
    else {
        panic!("expected CommandOutputPage")
    };
    assert_eq!(page.bytes, b"abc");
    assert_eq!(page.next_cursor, "3");
    assert!(!page.is_final);
    ctx.runtime.shutdown().await;
}

// ---------------------------------------------------------------------------
// 3. 队列:RUNNING 中排队 → turn 完成自动发送
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_next_turn_auto_sends_second_turn() {
    let mut ctx = setup(
        "queue",
        json!({"sessions": [{
            "conversationId": CONV_QUEUE,
            "title": "fixture-queue",
            "cwd": "/tmp/fixture-queue",
            "turn": {
                "outputLines": ["q-1", "q-2", "q-3", "q-4", "q-5", "q-6"],
                "lineDelayMs": 150
            }
        }]}),
        vec![seed(CONV_QUEUE, "fixture-queue")],
    )
    .await;

    ctx.runtime
        .handle_envelope(subscribe_session(CONV_QUEUE))
        .await;
    // 全程累计事件(各阶段的 Running 事件可能先于/晚于回执到达)。
    let mut all: Vec<pb::Envelope> = Vec::new();
    all.extend(
        ctx.outbox
            .collect_until(Duration::from_secs(5), |events| {
                Outbox::find_subscribed(events).is_some()
                    && Outbox::payload_of(events)
                        .any(|p| matches!(p, envelope::Payload::RuntimeSnapshot(_)))
            })
            .await,
    );

    // 第一 turn。
    ctx.runtime
        .handle_envelope(start_turn_request(CONV_QUEUE, "first", "corr-first"))
        .await;
    all.extend(
        ctx.outbox
            .collect_until(Duration::from_secs(10), |events| {
                Outbox::payload_of(events).any(|p| {
                    matches!(
                        p,
                        envelope::Payload::CommandResult(r)
                            if r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
                    )
                })
            })
            .await,
    );

    // RUNNING 中排队(queue_set)。
    let queue_request_id = Uuid::new_v4();
    ctx.runtime
        .handle_envelope(inbound_envelope(
            pb::envelope::Payload::CommandRequest(pb::CommandRequest {
                request_id: queue_request_id.to_string(),
                operation: pb::Operation::QueueSet as i32,
                session_key: Some(pb::SessionKey {
                    device_id: DEVICE.to_string(),
                    agent_kind: pb::AgentKind::CodexDesktop as i32,
                    native_session_id: CONV_QUEUE.to_string(),
                    relay_session_uuid: String::new(),
                }),
                expected_turn_id: None,
                expected_runtime_revision: None,
                payload_digest: String::new(),
                payload: Some(command_request::Payload::QueueSet(pb::QueueSetPayload {
                    prompt: "queued-second-turn".to_string(),
                    after_turn_id: None,
                    runtime_revision: 0,
                })),
            }),
            "corr-queue",
        ))
        .await;
    all.extend(
        ctx.outbox
            .collect_until(Duration::from_secs(5), |events| {
                Outbox::payload_of(events).any(|p| {
                    matches!(
                        p,
                        envelope::Payload::CommandResult(r)
                            if r.request_id == queue_request_id.to_string()
                                && r.status == pb::CommandReceiptStatus::ReceiptCompleted as i32
                    )
                })
            })
            .await,
    );
    assert!(
        Outbox::payload_of(&all).any(|p| matches!(
            p,
            envelope::Payload::CommandAccepted(a)
                if a.request_id == queue_request_id.to_string()
                    && a.status == pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32
        )),
        "排队命令应先回 ACCEPTED_BY_BRIDGE"
    );

    // 等待两个不同的 Running turn(第一 turn + 队列自动发送的第二 turn),
    // 且第二 turn 最终 Idle+Completed。
    let running_of = |events: &[pb::Envelope]| {
        Outbox::event_batches(events)
            .iter()
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match e.event.as_ref() {
                Some(pb::domain_event::Event::TurnLifecycle(t))
                    if t.phase == pb::ActiveTurnPhase::TurnPhaseRunning as i32 =>
                {
                    t.turn.as_ref().map(|turn| turn.id.clone())
                }
                _ => None,
            })
            .collect::<Vec<String>>()
    };
    // 手动循环:对累计向量判定(第二 Running + 其后 Idle Completed)。
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let running = running_of(&all);
            let distinct: std::collections::HashSet<&String> = running.iter().collect();
            let second_completed = Outbox::event_batches(&all).iter().any(|b| {
                b.events.iter().any(|e| {
                    matches!(
                        e.event,
                        Some(pb::domain_event::Event::TurnLifecycle(ref t))
                            if t.phase == pb::ActiveTurnPhase::TurnPhaseIdle as i32
                                && t.outcome == pb::LastTurnOutcome::TurnOutcomeCompleted as i32
                    )
                })
            }) && distinct.len() >= 2;
            if second_completed || tokio::time::Instant::now() >= deadline {
                break;
            }
            match tokio::time::timeout(Duration::from_millis(50), ctx.outbox.rx.recv()).await {
                Ok(Some(env)) => all.push(env),
                Ok(None) => break,
                Err(_) => {}
            }
        }
    }
    let running_turns: Vec<String> = Outbox::event_batches(&all)
        .iter()
        .flat_map(|b| b.events.iter())
        .filter_map(|e| match e.event.as_ref() {
            Some(pb::domain_event::Event::TurnLifecycle(t))
                if t.phase == pb::ActiveTurnPhase::TurnPhaseRunning as i32 =>
            {
                t.turn.as_ref().map(|turn| turn.id.clone())
            }
            _ => None,
        })
        .collect();
    let distinct: std::collections::HashSet<&String> = running_turns.iter().collect();
    assert!(
        distinct.len() >= 2,
        "队列应自动发送第二 turn;实际 running turns: {running_turns:?}"
    );
    assert!(
        Outbox::event_batches(&all).iter().any(|b| {
            b.events.iter().any(|e| {
                matches!(
                    e.event,
                    Some(pb::domain_event::Event::TurnLifecycle(ref t))
                        if t.phase == pb::ActiveTurnPhase::TurnPhaseIdle as i32
                            && t.outcome == pb::LastTurnOutcome::TurnOutcomeCompleted as i32
                )
            })
        }),
        "第二 turn 应最终正常完成"
    );
    // 队列正文只在 Bridge:事件流不得出现队列 prompt 正文(§15.3)。
    let serialized = format!("{all:?}");
    assert!(
        !serialized.contains("queued-second-turn"),
        "队列正文不得进入事件流(Debug 序列化为脱敏形态)"
    );
    ctx.runtime.shutdown().await;
}

// ---------------------------------------------------------------------------
// 4. git_summary 查询(会话 cwd → 已授权 git 仓库)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_summary_query_uses_authorized_session_cwd() {
    // 临时 git 仓库:initial commit + 一次未暂存修改。
    let repo = temp_dir("git-repo");
    let repo = repo.canonicalize().unwrap();
    let run_git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .output()
            .expect("git spawn");
        assert!(
            status.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
    };
    run_git(&["init"]);
    run_git(&["config", "user.email", "fixture@example.com"]);
    run_git(&["config", "user.name", "fixture"]);
    std::fs::write(repo.join("a.txt"), "line-1\nline-2\n").unwrap();
    run_git(&["add", "."]);
    run_git(&["commit", "-m", "fixture-init"]);
    std::fs::write(repo.join("a.txt"), "line-1\nline-2-changed\n").unwrap();

    let mut ctx = setup(
        "git",
        json!({"sessions": [{
            "conversationId": CONV_GIT,
            "title": "fixture-git",
            // fake owner cwd 指向 git 仓库(测试内构造,非用户项目)。
            "cwd": repo.to_string_lossy(),
            "branch": "fixture-branch"
        }]}),
        vec![seed(CONV_GIT, "fixture-git")],
    )
    .await;
    // 授权仓库根(§23.1:cwd 所属且已授权)。
    ctx.store
        .authorize_workspace(&repo, "fixture-git-repo")
        .await
        .unwrap();

    // 先建立详情订阅(adapter 收到快照后 session_cwd 才可用)。
    ctx.runtime
        .handle_envelope(subscribe_session(CONV_GIT))
        .await;
    let _ = ctx
        .outbox
        .collect_until(Duration::from_secs(5), |events| {
            Outbox::payload_of(events).any(|p| matches!(p, envelope::Payload::RuntimeSnapshot(_)))
        })
        .await;

    ctx.runtime
        .handle_envelope(inbound_envelope(
            pb::envelope::Payload::QueryRequest(pb::QueryRequest {
                session_key: Some(pb::SessionKey {
                    device_id: DEVICE.to_string(),
                    agent_kind: pb::AgentKind::CodexDesktop as i32,
                    native_session_id: CONV_GIT.to_string(),
                    relay_session_uuid: String::new(),
                }),
                query: Some(query_request::Query::GitSummary(pb::GitSummaryQuery {})),
            }),
            "corr-git",
        ))
        .await;
    let events = ctx
        .outbox
        .collect_until(Duration::from_secs(8), |events| {
            Outbox::payload_of(events).any(|p| matches!(p, envelope::Payload::QueryResponse(_)))
        })
        .await;
    let response = Outbox::payload_of(&events)
        .find_map(|p| match p {
            envelope::Payload::QueryResponse(r) => Some(r.clone()),
            _ => None,
        })
        .expect("QueryResponse 缺失");
    assert_eq!(response.request_id, "corr-git");
    assert_eq!(response.error_code, 0, "git_summary 查询应成功");
    match response.result.expect("result oneof") {
        pb::query_response::Result::GitSummary(summary) => {
            assert!(!summary.detached_head);
            assert!(!summary.branch.is_empty());
            assert_eq!(summary.head_full.len(), 40, "HEAD 完整 ID");
            assert_eq!(summary.head_short.len(), 7);
            assert!(
                !summary.root_display_name.contains('/'),
                "只输出安全显示名,不含路径"
            );
            let modified: Vec<&pb::GitStatusEntry> = summary
                .entries
                .iter()
                .filter(|e| e.status == "modified" && e.relative_path == "a.txt")
                .collect();
            assert_eq!(modified.len(), 1, "未暂存修改应出现在 entries");
            assert!(!modified[0].staged);
            assert!(
                summary
                    .entries
                    .iter()
                    .all(|e| !e.relative_path.contains("/tmp")
                        && !Path::new(&e.relative_path).is_absolute()),
                "entries 必须为相对路径(§23.1)"
            );
        }
        other => panic!("unexpected query result: {other:?}"),
    }
    ctx.runtime.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. 文件 preview TransferOffer 全链路(假 Relay 接 producer POST)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn preview_transfer_streams_file_via_producer() {
    const FILE_BYTES: &[u8] = b"fixture-notes-content-0123456789abcdef\n";
    let workspace = temp_dir("file-ws");
    let workspace = workspace.canonicalize().unwrap();
    std::fs::write(workspace.join("notes.txt"), FILE_BYTES).unwrap();

    // 假 Relay:接 producer POST,断言头与字节;200 回复。
    let captured: Arc<
        std::sync::Mutex<Option<(String, reqwest::header::HeaderMap, bytes::Bytes)>>,
    > = Arc::new(std::sync::Mutex::new(None));
    let app = {
        let captured = captured.clone();
        axum::Router::new().route(
            "/agent-console/transfers/producer/{transfer_id}",
            axum::routing::post(
                move |path: axum::extract::Path<String>,
                      headers: axum::http::HeaderMap,
                      body: axum::body::Bytes| {
                    let captured = captured.clone();
                    async move {
                        *captured.lock().unwrap() = Some((path.0, headers, body));
                        axum::http::StatusCode::OK
                    }
                },
            ),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut ctx = setup_with_relay(
        "file",
        json!({"sessions": [{
            "conversationId": CONV_FILE,
            "title": "fixture-file",
            "cwd": "/tmp/fixture-file"
        }]}),
        vec![seed(CONV_FILE, "fixture-file")],
        &format!("ws://{relay_addr}"),
    )
    .await;
    ctx.store
        .authorize_workspace(&workspace, "fixture-file-ws")
        .await
        .unwrap();
    // 以 UserOpened 来源签发 handle(§22.2 三类来源之一;测试即用户代理)。
    let handle = ctx
        .runtime
        .grants()
        .issue(
            DEVICE,
            CONV_FILE,
            &workspace,
            Path::new("notes.txt"),
            GrantActions::PREVIEW,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap();

    ctx.runtime
        .handle_envelope(inbound_envelope(
            pb::envelope::Payload::TransferOffer(pb::TransferOffer {
                transfer_id: "t-preview-1".to_string(),
                direction: pb::TransferDirection::Download as i32,
                session_key: Some(pb::SessionKey {
                    device_id: DEVICE.to_string(),
                    agent_kind: pb::AgentKind::CodexDesktop as i32,
                    native_session_id: CONV_FILE.to_string(),
                    relay_session_uuid: String::new(),
                }),
                file_name: "notes.txt".to_string(),
                mime_type: "text/plain".to_string(),
                size_bytes: FILE_BYTES.len() as u64,
                file_handle: handle.token.clone(),
                range_start: 0,
                range_end_inclusive: None,
                expires_at: None,
            }),
            "corr-transfer",
        ))
        .await;

    // 等 TransferReady(true) + TransferResult(COMPLETED) + producer POST 落地。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut envelopes = Vec::new();
    loop {
        if let Some((_, _, _)) = captured.lock().unwrap().as_ref() {
            let ready = Outbox::payload_of(&envelopes).any(|p| {
                matches!(
                    p,
                    envelope::Payload::TransferReady(r) if r.ready
                )
            });
            let done = Outbox::payload_of(&envelopes).any(|p| {
                matches!(
                    p,
                    envelope::Payload::TransferResult(r)
                        if r.outcome == pb::TransferOutcome::Completed as i32
                )
            });
            if ready && done {
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("transfer 未完成;outbox: {envelopes:?}");
        }
        match tokio::time::timeout(Duration::from_millis(50), ctx.outbox.rx.recv()).await {
            Ok(Some(env)) => envelopes.push(env),
            Ok(None) => break,
            Err(_) => {}
        }
    }

    let (transfer_id, headers, body) = captured
        .lock()
        .unwrap()
        .clone()
        .expect("producer POST 未到达");
    assert_eq!(transfer_id, "t-preview-1");
    // 短期 transfer token + device credential(§22.4 第 4 步)。
    assert_eq!(headers.get("x-transfer-token").unwrap(), "t-preview-1");
    let authorization = headers.get("authorization").unwrap().to_str().unwrap();
    assert!(
        authorization.starts_with("Bearer "),
        "actual: {authorization}"
    );
    // §22.3 头语义:类型/内联/总长 + produce 自带的 Content-Type/Length。
    let header_str = |name: &str| headers.get(name).unwrap().to_str().unwrap().to_string();
    assert_eq!(header_str("content-type"), "text/plain; charset=utf-8");
    assert_eq!(
        header_str("x-transfer-content-type"),
        "text/plain; charset=utf-8"
    );
    assert_eq!(header_str("x-transfer-disposition"), "inline");
    assert_eq!(
        header_str("x-transfer-total-length"),
        FILE_BYTES.len().to_string()
    );
    assert_eq!(
        header_str("x-transfer-length"),
        FILE_BYTES.len().to_string()
    );
    assert_eq!(&body[..], FILE_BYTES, "producer 正文必须逐字节一致");

    assert!(
        Outbox::payload_of(&envelopes).any(|p| matches!(
            p,
            envelope::Payload::TransferReady(r)
                if r.transfer_id == "t-preview-1" && r.ready
        )),
        "应先回 TransferReady(true)"
    );
    assert!(
        Outbox::payload_of(&envelopes).any(|p| matches!(
            p,
            envelope::Payload::TransferResult(r)
                if r.transfer_id == "t-preview-1"
                    && r.outcome == pb::TransferOutcome::Completed as i32
        )),
        "完成后应回 TransferResult(COMPLETED)"
    );
    ctx.runtime.shutdown().await;
}
