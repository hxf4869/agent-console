//! 无 UI 端到端测试链(权威规格 §29.4,11 场景逐条对应)。
//!
//! 真实进程级组合:fake IPC owner(`bridge::adapter::codex::fake_owner`,进程内)
//! + Bridge(`BridgeRuntime` + 真实 transport,进程内)+ Relay(`relay::serve`
//! 真实服务器,进程内)+ PostgreSQL(docker postgres:16-alpine,独立库
//! `agent_console_e2e` / 独立用户)+ 假 dev-toolbox(测试内 axum stub)+
//! test Browser client(tokio-tungstenite + reqwest)。
//!
//! 场景 → 断言映射(§29.4 逐字;测试内以 `场景 N` 注释分节):
//!  1. 设备已绑定并上线      → 配对 v2(Bridge register→短码→浏览器
//!                            lookup/approve→claim ready→凭据 Keychain)
//!                            + bridge WSS 上线(/devices CONNECTION_ONLINE)
//!  2. Browser 取票并连接    → stub toolbox consume + subprotocol 握手
//!                            + ticket 单次消费(重放 401)
//!  3. 订阅 list 与详情      → Subscribed + 列表快照(3 会话)+ RuntimeSnapshot
//!                            + 摘要落库(session_summaries)
//!  4. snapshot 先于事件     → 同流首帧 Subscribed,快照 sequence ==
//!                            base_sequence,其后 sequence 严格递增(§17.4/§17.5)
//!  5. command → accepted    → CommandAccepted(ACCEPTED_BY_BRIDGE)
//!                            + request_receipts 元数据落库
//!  6. 断线重试只执行一次    → 同 request_id 重放既有回执,fake owner 仅一个
//!                            running turn(执行计数 = 1)
//!  7. 输出 gap → resync     → 丢弃 EventBatch 检测缺口 → ResyncRequest 重放
//!                            (Subscribed+snapshot);bridge resync 路径下发
//!                            ResyncRequired + 新 epoch snapshot;HTTP output
//!                            分页取得权威 final output
//!  8. question/approval     → 按原生 option/decision ID 回传,attention 按原生
//!                            事件移除,权威快照清空
//!  9. 后台命令跨 turn       → turn 完成后(currentTurn=null)快照仍含 1 条
//!                            BACKGROUND_CMD_RUNNING 后台命令
//! 10. 文件预览经 Relay      → preview 全链路逐字节一致;PostgreSQL 全表扫描与
//!                            临时目录扫描均无文件正文(§22.1/§25.2)
//! 11. 撤销 auth session     → introspect invalid 后 Browser WS 在窗口内以
//!                            1008/AUTH_EXPIRED 关闭(§20.5)
//!
//! 额外断言(§29.2/§25):全程 tracing 捕获无 ticket/凭据/prompt/正文;
//! 断线重连窗口后事件继续流动(bridge 未被阻塞)。

mod e2e_support;
mod support;

use std::{path::Path, sync::Arc, time::Duration};

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::agent_console::v1::{command_request, envelope, subscribe};
use agent_console_protocol::codec::{decode_envelope, new_message_id, PROTOCOL_VERSION};
use base64::Engine as _;

use bridge::adapter::codex::{CatalogThread, CodexAdapter, CodexAdapterConfig};
use bridge::capabilities::{ProbeResult, WriteMethodProbes};
use bridge::commands::{CommandGateway, PowerCoordinator, UploadLifecycle};
use bridge::config::BridgeConfig;
use bridge::files::{
    FileGrantManager, GrantActions, GrantSource, TransferConfig, DEFAULT_GRANT_TTL,
};
use bridge::keychain::{InMemoryKeychainStore, KeychainStore};
use bridge::local_store::{BindingStatus, LocalStore};
use bridge::power::WakePolicy;
use bridge::runtime::{BridgeRuntime, FsUploadCleaner, RuntimeParts};
use bridge::transport::{ConnectionState, StaticCredential};

use futures::StreamExt;
use support::{recv_close, recv_env, send_env, FakeToolbox, Ws};

// ---------------------------------------------------------------------------
// 会话与敏感常量(全部合成;marker 供正文泄漏断言)
// ---------------------------------------------------------------------------

/// 主会话(场景 3-7):长 turn + finalOutput ≠ 预览(权威校正)。
const MAIN: &str = "eeeeeeee-1111-4111-8111-111111111111";
/// 问题/审批会话(场景 8)。
const ATTN: &str = "eeeeeeee-2222-4222-8222-222222222222";
/// 后台命令会话(场景 9)。
const BG: &str = "eeeeeeee-3333-4333-8333-333333333333";

const PROMPT: &str = "e2e-prompt-MARKER-main-turn";
const FINAL_OUTPUT: &str = "AUTHORITATIVE-FINAL-OUTPUT-MARKER\n";
const QUESTION_ID: &str = "q-e2e-1";
const QUESTION_OPTION: &str = "opt-e2e-yes";
const APPROVAL_ID: &str = "ap-e2e-1";
const APPROVAL_DECISION: &str = "dec-e2e-allow";
const BG_COMMAND_ID: &str = "bg-e2e-1";
const BG_COMMAND_DISPLAY: &str = "e2e-bg-server";

fn owner_script() -> serde_json::Value {
    serde_json::json!({
        "sessions": [
            {
                "conversationId": MAIN,
                "title": "e2e-main",
                "cwd": "/tmp/e2e-main",
                "branch": "e2e-branch",
                "turn": {
                    "outputLines": (0..60).map(|i| format!("line-{i:02}")).collect::<Vec<_>>(),
                    "lineDelayMs": 150,
                    "finalAnswer": "e2e-final-answer",
                    "finalOutput": FINAL_OUTPUT,
                }
            },
            {
                "conversationId": ATTN,
                "title": "e2e-attention",
                "cwd": "/tmp/e2e-attention",
                "turn": {
                    "outputLines": ["q1", "q2", "q3"],
                    "lineDelayMs": 150,
                    "questionAfterLine": 1,
                    "question": {
                        "id": QUESTION_ID,
                        "title": "e2e question",
                        "options": [
                            {"id": QUESTION_OPTION, "label": "Yes"},
                            {"id": "opt-e2e-no", "label": "No"}
                        ],
                        "allowMultiple": false,
                        "allowFreeText": false
                    },
                    "approval": {
                        "id": APPROVAL_ID,
                        "riskDescription": "e2e risk",
                        "requestedAction": "e2e action",
                        "decisions": [
                            {"id": APPROVAL_DECISION, "label": "Allow"},
                            {"id": "dec-e2e-deny", "label": "Deny"}
                        ],
                        "scope": "this-command"
                    }
                }
            },
            {
                "conversationId": BG,
                "title": "e2e-background",
                "cwd": "/tmp/e2e-background",
                "backgroundCommands": [
                    {"id": BG_COMMAND_ID, "display": BG_COMMAND_DISPLAY, "state": "running"}
                ],
                "turn": {
                    "outputLines": ["b1", "b2", "b3"],
                    "lineDelayMs": 120
                }
            }
        ]
    })
}

fn catalog_seed(id: &str, title: &str) -> CatalogThread {
    CatalogThread {
        id: id.to_string(),
        title: Some(title.to_string()),
        project_display_name: Some(title.to_string()),
        model: None,
        reasoning_effort: None,
        git_branch: Some("e2e-branch".to_string()),
        created_at: None,
        updated_at: None,
        archived: false,
        agent_nickname: None,
        agent_role: None,
    }
}

// ---------------------------------------------------------------------------
// Browser 侧小工具
// ---------------------------------------------------------------------------

fn browser_env(payload: pb::envelope::Payload) -> pb::Envelope {
    pb::Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        device_id: String::new(),
        agent_kind: 1,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(payload),
        provider_extension: None,
    }
}

fn subscribe_list() -> pb::Envelope {
    browser_env(pb::envelope::Payload::Subscribe(pb::Subscribe {
        target: Some(subscribe::Target::List(pb::SessionList {})),
    }))
}

fn subscribe_session(device_id: &str, native: &str) -> pb::Envelope {
    browser_env(pb::envelope::Payload::Subscribe(pb::Subscribe {
        target: Some(subscribe::Target::Session(pb::SessionKey {
            device_id: device_id.to_string(),
            agent_kind: pb::AgentKind::CodexDesktop as i32,
            native_session_id: native.to_string(),
            relay_session_uuid: String::new(),
        })),
    }))
}

fn command_request(
    request_id: &str,
    native: &str,
    device_id: &str,
    payload: command_request::Payload,
) -> pb::Envelope {
    let operation = match &payload {
        command_request::Payload::StartTurn(_) => pb::Operation::StartTurn,
        command_request::Payload::AnswerQuestion(_) => pb::Operation::AnswerQuestion,
        command_request::Payload::AnswerApproval(_) => pb::Operation::AnswerApproval,
        _ => pb::Operation::Unspecified,
    };
    browser_env(pb::envelope::Payload::CommandRequest(pb::CommandRequest {
        request_id: request_id.to_string(),
        operation: operation as i32,
        session_key: Some(pb::SessionKey {
            device_id: device_id.to_string(),
            agent_kind: pb::AgentKind::CodexDesktop as i32,
            native_session_id: native.to_string(),
            relay_session_uuid: String::new(),
        }),
        expected_turn_id: None,
        expected_runtime_revision: None,
        payload_digest: String::new(),
        payload: Some(payload),
    }))
}

fn resync_request(stream_id: &str) -> pb::Envelope {
    browser_env(pb::envelope::Payload::ResyncRequest(pb::ResyncRequest {
        stream_id: stream_id.to_string(),
    }))
}

/// 顺序收集帧直到条件满足或超时;保留全部帧。
async fn collect_until(
    ws: &mut Ws,
    deadline: Duration,
    mut done: impl FnMut(&[pb::Envelope]) -> bool,
) -> Vec<pb::Envelope> {
    let mut frames = Vec::new();
    let end = tokio::time::Instant::now() + deadline;
    loop {
        if done(&frames) {
            return frames;
        }
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return frames;
        }
        match tokio::time::timeout(remaining.min(Duration::from_millis(50)), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes)))) => {
                if let Ok(env) = decode_envelope(&bytes) {
                    frames.push(env);
                }
            }
            Ok(Some(Ok(_))) => {} // Text/Ping/Pong:忽略
            Ok(Some(Err(_))) | Ok(None) => return frames,
            Err(_) => {}
        }
    }
}

fn payloads(frames: &[pb::Envelope]) -> Vec<&pb::envelope::Payload> {
    frames.iter().filter_map(|f| f.payload.as_ref()).collect()
}

/// 事件批里的全部领域事件。
fn domain_events(frames: &[pb::Envelope]) -> Vec<&pb::domain_event::Event> {
    payloads(frames)
        .into_iter()
        .filter_map(|p| match p {
            envelope::Payload::EventBatch(b) => Some(b.events.iter().collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .filter_map(|e| e.event.as_ref())
        .collect()
}

fn turn_lifecycle_events(frames: &[pb::Envelope]) -> Vec<&pb::TurnLifecycle> {
    domain_events(frames)
        .into_iter()
        .filter_map(|e| match e {
            pb::domain_event::Event::TurnLifecycle(t) => Some(t),
            _ => None,
        })
        .collect()
}

/// §17.4/§17.5 订阅顺序断言:
/// - Subscribed(payload 内 stream_id == stream)先到;
/// - 其后同流首帧为快照,且 sequence == Subscribed.base_sequence;
/// - 再往后同流帧 sequence 严格递增(均 > base)。
/// 注意:Relay 下发 Subscribed/ResyncRequired 时 Envelope.stream_id 为空,
/// 流归属以 payload 内字段为准。
fn assert_subscribe_order(frames: &[pb::Envelope], stream: &str) {
    let sub_pos = frames
        .iter()
        .position(|f| {
            matches!(
                f.payload.as_ref(),
                Some(envelope::Payload::Subscribed(s)) if s.stream_id == stream
            )
        })
        .unwrap_or_else(|| panic!("stream {stream} 的 Subscribed 缺失"));
    let base = match frames[sub_pos].payload.as_ref() {
        Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
        _ => unreachable!(),
    };
    let rest = &frames[sub_pos + 1..];
    let snap_pos = rest
        .iter()
        .position(|f| {
            f.stream_id == stream
                && matches!(
                    f.payload.as_ref(),
                    Some(envelope::Payload::RuntimeSnapshot(_))
                        | Some(envelope::Payload::SessionSummaryBatch(_))
                )
        })
        .unwrap_or_else(|| panic!("stream {stream} 的快照缺失"));
    let snapshot = &rest[snap_pos];
    assert_eq!(
        snapshot.sequence, base,
        "快照 sequence 必须等于 base_sequence(§17.4 步骤 5)"
    );
    let mut last = snapshot.sequence;
    for frame in &rest[snap_pos + 1..] {
        if frame.stream_id != stream {
            continue;
        }
        assert!(
            frame.sequence > last,
            "sequence 必须严格递增: {} <= {last}(§17.5)",
            frame.sequence
        );
        last = frame.sequence;
    }
}

// ---------------------------------------------------------------------------
// 测试主链
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_full_chain_no_ui_eleven_scenarios() {
    // =====================================================================
    // 环境装配:PostgreSQL(16-alpine) + 假 dev-toolbox + 进程内 Relay
    // =====================================================================
    let logs = e2e_support::LogCapture::init();
    let pg = e2e_support::E2ePostgres::start().await;
    let db_url = pg.db_url();
    let toolbox = FakeToolbox::start().await;
    toolbox.seed_session(support::AUTH_SESSION, chrono::Duration::hours(1));
    let relay = e2e_support::start_relay(&db_url, &toolbox).await;
    let pool = sqlx::postgres::PgPool::connect(&db_url).await.unwrap();
    let http = reqwest::Client::new();
    let identity = e2e_support::identity_headers();

    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("data");

    // fake IPC owner(进程内;脚本含主/attention/后台三会话)。
    let socket = std::env::temp_dir().join(format!(
        "ac-e2e-{}-{}.sock",
        std::process::id(),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    let owner_task = tokio::spawn(bridge::adapter::codex::fake_owner::run_server(
        socket.clone(),
        owner_script(),
    ));
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");

    // =====================================================================
    // 场景 1:设备绑定并上线(配对 v2:Bridge 发起 register→短码,浏览器
    // lookup/approve,claim ready→凭据 Keychain;bridge WSS 上线)
    // =====================================================================
    let store = LocalStore::open(&data_dir).await.unwrap();
    let keychain = Arc::new(InMemoryKeychainStore::new());

    // Bridge 发起配对(§21 步骤 1-2):本地 challenge → register → 短码。
    // 进程内直连 relay(不经网关;网关链另有 e2e_gateway)。
    let pairing = bridge::pairing::PairingClient::new(&relay.base).unwrap();
    let registration = pairing.register("e2e-mac", "0.1.0").await.unwrap();
    let short_code = registration.short_code.clone();
    assert_eq!(short_code.len(), 6, "6 位短码");

    // Bridge 轮询 claim(§21 步骤 6-7):先 pending;批准后 ready 单次交付,
    // 凭据先入 Keychain、再落绑定状态(§19)。
    let pair_base = relay.base.clone();
    let pair_store = store.clone();
    let pair_keychain = keychain.clone();
    let pair_task = tokio::spawn(async move {
        let client = bridge::pairing::PairingClient::new(&pair_base).unwrap();
        let (device_id, credential) = client
            .wait_approval(
                &registration,
                bridge::pairing::PairingOptions {
                    poll_interval: Duration::from_millis(100),
                    deadline: Duration::from_secs(30),
                },
            )
            .await
            .expect("配对批准");
        bridge::pairing::bind(
            pair_keychain,
            &pair_store,
            client.origin(),
            &device_id,
            &credential,
        )
        .await
        .expect("bind");
        (device_id, credential)
    });

    // Browser(已登录)输入短码 lookup(§21 步骤 3-4)→ 设备信息。
    let mut req = http.post(format!("{}/agent-console/api/pairing/lookup", relay.base));
    for (k, v) in &identity {
        req = req.header(*k, v);
    }
    let resp = req
        .json(&serde_json::json!({ "shortCode": short_code }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "短码 lookup 失败: {}",
        resp.status()
    );
    let lookup: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(lookup["deviceName"], "e2e-mac", "lookup 展示设备信息");

    // Browser 批准(§21 步骤 5;owner 批准时回填)。
    let mut req = http.post(format!("{}/agent-console/api/pairing/approve", relay.base));
    for (k, v) in &identity {
        req = req.header(*k, v);
    }
    let resp = req
        .json(&serde_json::json!({
            "shortCode": short_code,
            "challengeId": lookup["challengeId"],
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "批准失败: {}", resp.status());

    let (device_id, credential) = pair_task.await.unwrap();
    // 凭据进 Keychain(不进 SQLite);绑定状态 Paired。
    assert_eq!(
        keychain.get_device_credential(&device_id).await.unwrap(),
        Some(credential.clone()),
        "凭据应已写入 Keychain"
    );
    let binding = store.get_binding().await.unwrap();
    assert_eq!(binding.device_id, device_id);
    assert_eq!(binding.status, BindingStatus::Paired);
    // Relay 库存设备行(凭据只存摘要)。
    let row = sqlx::query_as::<_, (String,)>(
        "SELECT display_name FROM devices WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(uuid::Uuid::parse_str(&device_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("设备行存在");
    assert_eq!(row.0, "e2e-mac");

    // ---- Bridge 组装(adapter → fake owner;transport → 真实 relay WSS)----
    let mut adapter_config = CodexAdapterConfig::new(device_id.clone());
    adapter_config.ipc_socket = Some(socket.clone());
    // codex_home 放入 root 临时目录内(TempDrop 统一清理)。
    adapter_config.codex_home = Some(root.path().join("codex-home"));
    adapter_config.version_report = Some("codex-cli 0.153.0-alpha.5".to_string());
    adapter_config.write_method_probes = WriteMethodProbes {
        start_turn: ProbeResult::Passed,
        steer_turn: ProbeResult::Passed,
        interrupt_turn: ProbeResult::Passed,
        answer_question: ProbeResult::Passed,
        update_settings: ProbeResult::Passed,
    };
    adapter_config.static_sessions = vec![
        catalog_seed(MAIN, "e2e-main"),
        catalog_seed(ATTN, "e2e-attention"),
        catalog_seed(BG, "e2e-background"),
    ];
    let adapter = CodexAdapter::connect(adapter_config).await.unwrap();
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let grants = FileGrantManager::new();
    let uploads = Arc::new(UploadLifecycle::new(
        Arc::new(FsUploadCleaner::new(data_dir.join("uploads"))),
        store.clone(),
    ));
    let power = PowerCoordinator::new(
        Arc::new(WakePolicy::with_program(
            Path::new("/bin/true").to_path_buf(),
        )),
        bridge::power::PowerSource::Ac,
    );

    let relay_urls = bridge::config::RelayUrls::parse(&relay.base).unwrap();
    let mut options =
        bridge::transport::RelayClientOptions::new(&relay_urls).with_device_id(device_id.clone());
    options.heartbeat_interval = Duration::from_secs(1);
    options.reconnect_initial_backoff = Duration::from_millis(100);
    options.reconnect_max_backoff = Duration::from_millis(500);
    options.reconnect_jitter = Duration::ZERO;

    let runtime_slot: Arc<std::sync::OnceLock<Arc<BridgeRuntime>>> =
        Arc::new(std::sync::OnceLock::new());
    let on_connected: bridge::transport::OnConnected = {
        let slot = runtime_slot.clone();
        Arc::new(move || {
            if let Some(runtime) = slot.get() {
                runtime.resync();
            }
        })
    };
    let (handle, mut inbound) = bridge::transport::start(
        options,
        Arc::new(StaticCredential::new(credential.clone())),
        Some(on_connected),
    );
    let runtime = Arc::new(BridgeRuntime::new(RuntimeParts {
        config: BridgeConfig::from_env()
            .with_data_dir(data_dir.clone())
            .with_relay_url(relay.base.clone()),
        device_id: device_id.clone(),
        store: store.clone(),
        adapter: adapter.clone(),
        gateway,
        grants,
        git: None,
        uploads,
        power,
        credential: Arc::new(StaticCredential::new(credential.clone())),
        outbound: Arc::new(handle.clone()),
        http: reqwest::Client::new(),
        upload_root: data_dir.join("uploads"),
        transfer_config: TransferConfig::default(),
    }));
    let _ = runtime_slot.set(runtime.clone());
    let observer = runtime.start_observation();
    let pump = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            while let Some(env) = inbound.recv().await {
                runtime.handle_envelope(env).await;
            }
        }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while handle.state() != ConnectionState::Connected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge 未连上 relay"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // 设备上线(HTTP /devices 经 hub.is_online 判定;§10.1)。
    let mut online = false;
    for _ in 0..100 {
        let mut req = http.get(format!("{}/agent-console/api/devices", relay.base));
        for (k, v) in &identity {
            req = req.header(*k, v);
        }
        if let Ok(resp) = req.send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                let devices = body["devices"].as_array().cloned().unwrap_or_default();
                if devices.iter().any(|d| {
                    d["id"].as_str() == Some(device_id.as_str())
                        && d["connection"].as_str() == Some("CONNECTION_ONLINE")
                }) {
                    online = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(online, "设备应显示 CONNECTION_ONLINE(场景 1)");

    // =====================================================================
    // 场景 2:Browser 取 ticket 并连接(stub toolbox consume/introspect)
    // =====================================================================
    let ticket_1 = toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = e2e_support::connect_browser_ws(&relay.base, &ticket_1)
        .await
        .expect("浏览器 WS 连接");
    // ticket 单次消费:重放同 ticket 必须在升级前被拒(401)。
    let reused = e2e_support::connect_browser_ws(&relay.base, &ticket_1).await;
    assert_eq!(reused.unwrap_err(), 401, "已消费 ticket 必须拒绝");

    // =====================================================================
    // 场景 3:订阅 session list 和详情
    // =====================================================================
    send_env(&mut browser, &subscribe_list()).await;
    let list_frames = collect_until(&mut browser, Duration::from_secs(10), |frames| {
        payloads(frames).iter().any(|p| {
            matches!(
                p,
                envelope::Payload::SessionSummaryBatch(b) if b.snapshot
            )
        }) && payloads(frames)
            .iter()
            .any(|p| matches!(p, envelope::Payload::Subscribed(_)))
    })
    .await;
    let list_stream = list_frames
        .iter()
        .find(|f| {
            matches!(
                f.payload.as_ref(),
                Some(envelope::Payload::SessionSummaryBatch(_))
            )
        })
        .map(|f| f.stream_id.clone())
        .expect("list 流 snapshot 缺失");
    let summary_batch = payloads(&list_frames)
        .into_iter()
        .find_map(|p| match p {
            envelope::Payload::SessionSummaryBatch(b) if b.snapshot => Some(b.clone()),
            _ => None,
        })
        .unwrap();
    let natives: Vec<&str> = summary_batch
        .summaries
        .iter()
        .map(|s| s.session_key.as_ref().unwrap().native_session_id.as_str())
        .collect();
    for expected in [MAIN, ATTN, BG] {
        assert!(
            natives.contains(&expected),
            "list 快照应含 {expected};实际 {natives:?}"
        );
    }
    // 摘要已落库(§18.3;后续 command 的 session 解析依赖此行)。
    let mut session_row: Option<(uuid::Uuid,)> = None;
    for _ in 0..100 {
        match sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT id FROM session_summaries WHERE device_id = $1 AND native_session_id = $2",
        )
        .bind(uuid::Uuid::parse_str(&device_id).unwrap())
        .bind(MAIN)
        .fetch_optional(&pool)
        .await
        {
            Ok(row) if row.is_some() => {
                session_row = row;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let session_uuid = session_row.expect("会话摘要应已落库").0;

    send_env(&mut browser, &subscribe_session(&device_id, MAIN)).await;
    let mut all_frames = collect_until(&mut browser, Duration::from_secs(10), |frames| {
        payloads(frames)
            .iter()
            .any(|p| matches!(p, envelope::Payload::RuntimeSnapshot(_)))
            && payloads(frames).iter().any(|p| {
                matches!(
                    p,
                    envelope::Payload::Subscribed(s) if s.stream_id.starts_with("s-")
                )
            })
    })
    .await;
    let main_stream = all_frames
        .iter()
        .find(|f| {
            matches!(
                f.payload.as_ref(),
                Some(envelope::Payload::RuntimeSnapshot(_))
            )
        })
        .map(|f| f.stream_id.clone())
        .expect("详情流 snapshot 缺失");
    assert!(main_stream.starts_with("s-"), "详情流 id 形如 s-<uuid>");

    // =====================================================================
    // 场景 4:顺序断言 —— Subscribed → snapshot(base_sequence) → 后续事件
    // =====================================================================
    assert_subscribe_order(&all_frames, &main_stream);
    assert_subscribe_order(&list_frames, &list_stream);

    // =====================================================================
    // 场景 5:发 command → 收 ACCEPTED_BY_BRIDGE 回执
    // =====================================================================
    let r1 = uuid::Uuid::new_v4().to_string();
    send_env(
        &mut browser,
        &command_request(
            &r1,
            MAIN,
            &device_id,
            command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: PROMPT.to_string(),
            }),
        ),
    )
    .await;
    let accepted_frames = collect_until(&mut browser, Duration::from_secs(10), |frames| {
        payloads(frames).iter().any(|p| {
            matches!(
                p,
                envelope::Payload::CommandAccepted(a)
                    if a.request_id == r1
                        && a.status == pb::CommandReceiptStatus::ReceiptAcceptedByBridge as i32
            )
        })
    })
    .await;
    assert!(
        !accepted_frames.is_empty(),
        "应收到 ACCEPTED_BY_BRIDGE 回执"
    );
    all_frames.extend(accepted_frames.iter().cloned());
    // 回执落库(request_receipts 只存元数据;§18.4/§15.2)。start-turn 命令
    // 回执可能已快速走到终态(命令生命周期 ≠ turn 生命周期),断言为合法回执态。
    let mut receipt_row: Option<(String, String)> = None;
    for _ in 0..200 {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT status, operation FROM request_receipts WHERE request_id = $1")
                .bind(&r1)
                .fetch_optional(&pool)
                .await
                .unwrap();
        let accepted = row.as_ref().is_some_and(|(status, _)| {
            matches!(
                status.as_str(),
                "ACCEPTED_BY_BRIDGE" | "DISPATCHED_TO_CODEX" | "COMPLETED"
            )
        });
        if accepted {
            receipt_row = row;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let receipt = receipt_row.expect("request_receipts 应有去重记录(§29.2)");
    assert_eq!(receipt.1, "OPERATION_START_TURN");
    assert!(
        matches!(
            receipt.0.as_str(),
            "ACCEPTED_BY_BRIDGE" | "DISPATCHED_TO_CODEX" | "COMPLETED"
        ),
        "回执状态异常: {}",
        receipt.0
    );

    // =====================================================================
    // 场景 6:断线(accepted 回执丢失)→ 同 request_id 重试 → 只执行一次
    // =====================================================================
    // 回执已持久化(上面等待)后立即断开:对调用方而言 accepted 回执"丢失"。
    let _ = browser.close(None).await;
    drop(browser);

    let ticket_2 = toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = e2e_support::connect_browser_ws(&relay.base, &ticket_2)
        .await
        .expect("重连成功");
    send_env(&mut browser, &subscribe_session(&device_id, MAIN)).await;
    let resub_frames = collect_until(&mut browser, Duration::from_secs(5), |frames| {
        frames.iter().any(|frame| {
            matches!(
                frame.payload.as_ref(),
                Some(envelope::Payload::Subscribed(s)) if s.stream_id == main_stream
            )
        }) && frames.iter().any(|frame| {
            frame.stream_id == main_stream
                && matches!(
                    frame.payload.as_ref(),
                    Some(envelope::Payload::RuntimeSnapshot(_))
                )
        })
    })
    .await;
    assert_subscribe_order(&resub_frames, &main_stream);
    all_frames.extend(resub_frames);
    // 同一 request_id + 相同内容重试。
    send_env(
        &mut browser,
        &command_request(
            &r1,
            MAIN,
            &device_id,
            command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: PROMPT.to_string(),
            }),
        ),
    )
    .await;
    let retry_frames = collect_until(&mut browser, Duration::from_secs(5), |frames| {
        payloads(frames).iter().any(|p| {
            matches!(
                p,
                envelope::Payload::CommandResult(r) if r.request_id == r1
            )
        })
    })
    .await;
    all_frames.extend(retry_frames.iter().cloned());
    let replayed = payloads(&retry_frames)
        .into_iter()
        .find_map(|p| match p {
            envelope::Payload::CommandResult(r) if r.request_id == r1 => Some(r.clone()),
            _ => None,
        })
        .expect("重试应回放既有回执");
    // 回放的是既有回执的当前状态(§15.2:相同 ID 重试返回已有回执,不重发);
    // 该命令可能已到终态 COMPLETED,以库内状态为准。
    let (db_status,): (String,) =
        sqlx::query_as("SELECT status FROM request_receipts WHERE request_id = $1")
            .bind(&r1)
            .fetch_one(&pool)
            .await
            .unwrap();
    let db_status_code = match db_status.as_str() {
        "RECEIVED" => pb::CommandReceiptStatus::ReceiptReceived,
        "ACCEPTED_BY_BRIDGE" => pb::CommandReceiptStatus::ReceiptAcceptedByBridge,
        "DISPATCHED_TO_CODEX" => pb::CommandReceiptStatus::ReceiptDispatchedToCodex,
        "COMPLETED" => pb::CommandReceiptStatus::ReceiptCompleted,
        "REJECTED" => pb::CommandReceiptStatus::ReceiptRejected,
        _ => pb::CommandReceiptStatus::ReceiptOutcomeUnknown,
    };
    assert_eq!(
        replayed.status, db_status_code as i32,
        "重试必须回放库内既有回执(不重发,§15.2)"
    );

    // =====================================================================
    // 场景 7a:输出 gap(丢弃一个 EventBatch)→ ResyncRequest → 窗口内重放
    // =====================================================================
    let mut dropped = false;
    let mut gap_detected = false;
    let mut last_seq: Option<u64> = None;
    let gap_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < gap_deadline {
        let env = recv_env(&mut browser, Duration::from_secs(6)).await;
        if env.stream_id != main_stream {
            all_frames.push(env);
            continue;
        }
        if matches!(env.payload.as_ref(), Some(envelope::Payload::EventBatch(_))) {
            // 客户端视角:已见帧定义期望(last+1);丢掉一帧已建立基线后的帧,
            // 下一帧 sequence = last+2 → 检测到缺口。
            if dropped {
                if let Some(last) = last_seq {
                    if env.sequence > last + 1 {
                        gap_detected = true;
                        break;
                    }
                }
            } else if last_seq.is_some() {
                dropped = true; // 已有基线:丢弃本帧(模拟 Browser 侧丢帧)
                continue;
            }
            last_seq = Some(env.sequence);
        }
    }
    assert!(
        dropped && gap_detected,
        "应能制造并检测输出 gap(dropped={dropped}, gap={gap_detected})"
    );
    send_env(&mut browser, &resync_request(&main_stream)).await;
    let mut replay_frames: Vec<pb::Envelope> = Vec::new();
    loop {
        let env = recv_env(&mut browser, Duration::from_secs(6)).await;
        let is_replayed_snapshot = env.stream_id == main_stream
            && matches!(
                env.payload.as_ref(),
                Some(envelope::Payload::RuntimeSnapshot(_))
            );
        replay_frames.push(env);
        if is_replayed_snapshot {
            break;
        }
    }
    all_frames.extend(replay_frames.iter().cloned());
    // 重放以 Subscribed 开始,快照 sequence == base_sequence(§17.5 窗口补发)。
    let resync_base = replay_frames
        .iter()
        .find_map(|f| match f.payload.as_ref() {
            Some(envelope::Payload::Subscribed(s)) if s.stream_id == main_stream => {
                Some((s.stream_epoch, s.base_sequence))
            }
            _ => None,
        })
        .expect("重放应以 Subscribed 开始");
    let replay_snapshot = replay_frames
        .iter()
        .find(|f| {
            f.stream_id == main_stream
                && matches!(
                    f.payload.as_ref(),
                    Some(envelope::Payload::RuntimeSnapshot(_))
                )
        })
        .expect("重放应含重新 snapshot");
    assert_eq!(
        replay_snapshot.sequence, resync_base.1,
        "重放快照 sequence = base_sequence"
    );

    // 等 MAIN turn 完成且权威输出校正到达；二者属于同一终态快照产生的事件，
    // 不能只见 lifecycle 就停止收帧后再断言 OutputReplace。
    let completion = collect_until(&mut browser, Duration::from_secs(30), |frames| {
        let completed = turn_lifecycle_events(frames).iter().any(|t| {
            t.phase == pb::ActiveTurnPhase::TurnPhaseIdle as i32
                && t.outcome == pb::LastTurnOutcome::TurnOutcomeCompleted as i32
        });
        let corrected = domain_events(frames).iter().any(|event| matches!(
            event,
            pb::domain_event::Event::OutputReplace(replace)
                if replace.content == Some(pb::output_replace::Content::Bytes(FINAL_OUTPUT.as_bytes().to_vec()))
        ));
        completed && corrected
    })
    .await;
    all_frames.extend(completion.iter().cloned());
    // 执行计数断言:全链路 MAIN 只出现一个 turn id(fake owner 只执行一次)。
    // Browser 在 running 期间断线；详情流重连会取 fresh snapshot，因此不能依赖
    // 断线窗口内的 running 事件仍被重放，终态 lifecycle 仍能可靠证明执行次数。
    let executed_turns: std::collections::HashSet<String> = turn_lifecycle_events(&all_frames)
        .into_iter()
        .filter_map(|t| t.turn.as_ref().map(|x| x.id.clone()))
        .collect();
    assert_eq!(
        executed_turns.len(),
        1,
        "重试不得重复执行(§29.4 场景 6);实际 turn ids: {executed_turns:?}"
    );

    // =====================================================================
    // 场景 7b:bridge resync(重连等价路径)→ Browser 收 ResyncRequired
    // → 重新 snapshot(新 epoch)
    // =====================================================================
    runtime.resync();
    let mut saw_resync_required = false;
    let mut saw_new_epoch_snapshot = false;
    let mut first_epoch_after: Option<u64> = None;
    let resync_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < resync_deadline
        && !(saw_resync_required && saw_new_epoch_snapshot)
    {
        let env = match tokio::time::timeout(Duration::from_secs(10), browser.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes)))) => {
                decode_envelope(&bytes).expect("decode")
            }
            _ => panic!("resync 窗口内连接中断或超时"),
        };
        // ResyncRequired / Subscribed 的 Envelope.stream_id 为空(连接级帧),
        // 流归属以 payload 内字段为准;快照/事件帧带流 id。
        match env.payload.as_ref() {
            Some(envelope::Payload::ResyncRequired(r)) if r.stream_id == main_stream => {
                saw_resync_required = true;
            }
            Some(envelope::Payload::Subscribed(s)) if s.stream_id == main_stream => {
                first_epoch_after = Some(s.stream_epoch);
            }
            Some(envelope::Payload::RuntimeSnapshot(_)) if env.stream_id == main_stream => {
                if let Some(epoch) = first_epoch_after {
                    assert_eq!(env.stream_epoch, epoch, "新快照必须属于新 epoch(§17.5)");
                    saw_new_epoch_snapshot = true;
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_resync_required && saw_new_epoch_snapshot,
        "Browser 应收到 ResyncRequired + 新 epoch snapshot"
    );

    // =====================================================================
    // 场景 7c:权威 final output(HTTP output 分页,Bridge 权威缓冲)
    // =====================================================================
    let item_id = domain_events(&all_frames)
        .into_iter()
        .find_map(|e| match e {
            pb::domain_event::Event::OutputReplace(r) => r.item_id.as_ref().map(|i| i.id.clone()),
            pb::domain_event::Event::OutputAppend(a) => a.item_id.as_ref().map(|i| i.id.clone()),
            _ => None,
        })
        .expect("输出事件应携带 item id");
    // 预览事件流应出现过权威校正(OutputReplace = finalOutput,§13.3)。
    let corrected = domain_events(&all_frames).iter().any(|e| matches!(
        e,
        pb::domain_event::Event::OutputReplace(r)
            if r.content == Some(pb::output_replace::Content::Bytes(FINAL_OUTPUT.as_bytes().to_vec()))
    ));
    assert!(corrected, "事件流应含权威 OutputReplace 校正");
    let body = http_get_json(
        &http,
        &format!(
            "{}/agent-console/api/sessions/{session_uuid}/output?itemId={item_id}&cursor=",
            relay.base
        ),
        &identity,
    )
    .await;
    let page = &body["commandOutputPage"];
    let final_bytes = base64::engine::general_purpose::STANDARD
        .decode(page["bytesBase64"].as_str().expect("bytesBase64"))
        .unwrap();
    assert_eq!(
        String::from_utf8(final_bytes).unwrap(),
        FINAL_OUTPUT,
        "HTTP 分页读取的必须是权威 final output(§13.3)"
    );
    assert_eq!(page["isFinal"], serde_json::Value::Bool(true));

    // =====================================================================
    // 场景 8:question 与 approval,按原生 option/decision ID 回传,attention 移除
    // =====================================================================
    // attention 事件在 ATTN 详情流上:先订阅再启动 turn。
    send_env(&mut browser, &subscribe_session(&device_id, ATTN)).await;
    let attn_sub = collect_until(&mut browser, Duration::from_secs(10), |frames| {
        // ATTN 流 id 未知(形如 s-<uuid>):等一个非 MAIN 流的快照帧。
        frames.iter().any(|f| {
            !f.stream_id.is_empty()
                && f.stream_id != main_stream
                && matches!(
                    f.payload.as_ref(),
                    Some(envelope::Payload::RuntimeSnapshot(_))
                )
        })
    })
    .await;
    let _attn_stream = attn_sub
        .iter()
        .find(|f| {
            !f.stream_id.is_empty()
                && f.stream_id != main_stream
                && matches!(
                    f.payload.as_ref(),
                    Some(envelope::Payload::RuntimeSnapshot(_))
                )
        })
        .map(|f| f.stream_id.clone())
        .expect("ATTN 详情订阅应建立");
    let r2 = uuid::Uuid::new_v4().to_string();
    send_env(
        &mut browser,
        &command_request(
            &r2,
            ATTN,
            &device_id,
            command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: "e2e-prompt-attention".to_string(),
            }),
        ),
    )
    .await;
    let mut attention_question: Option<pb::PendingAttentionQuestion> = None;
    let mut attention_approval: Option<pb::PendingAttentionApproval> = None;
    let attn_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while (attention_question.is_none() || attention_approval.is_none())
        && tokio::time::Instant::now() < attn_deadline
    {
        let env = recv_env(&mut browser, Duration::from_secs(20)).await;
        for event in domain_events(std::slice::from_ref(&env)) {
            if let pb::domain_event::Event::PendingAttentionAdded(added) = event {
                match added.attention.as_ref() {
                    Some(pb::pending_attention_added::Attention::Question(q)) => {
                        if q.question_id == QUESTION_ID {
                            assert!(
                                q.options.iter().any(|o| o.option_id == QUESTION_OPTION),
                                "问题必须携带原生 option ID"
                            );
                            attention_question = Some(q.clone());
                        }
                    }
                    Some(pb::pending_attention_added::Attention::Approval(a)) => {
                        if a.approval_id == APPROVAL_ID {
                            assert!(
                                a.decisions
                                    .iter()
                                    .any(|d| d.decision_id == APPROVAL_DECISION),
                                "审批必须携带原生 decision ID"
                            );
                            attention_approval = Some(a.clone());
                        }
                    }
                    None => {}
                }
            }
        }
    }
    assert!(
        attention_question.is_some() && attention_approval.is_some(),
        "应收到问题与审批(原生 ID + 选项)"
    );

    // 回答问题(原生 option ID)→ PendingAttentionRemoved(question)。
    send_env(
        &mut browser,
        &command_request(
            &uuid::Uuid::new_v4().to_string(),
            ATTN,
            &device_id,
            command_request::Payload::AnswerQuestion(pb::AnswerQuestionPayload {
                question_id: QUESTION_ID.to_string(),
                option_ids: vec![QUESTION_OPTION.to_string()],
                free_text: String::new(),
            }),
        ),
    )
    .await;
    let mut question_removed = false;
    let remove_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !question_removed && tokio::time::Instant::now() < remove_deadline {
        let env = recv_env(&mut browser, Duration::from_secs(10)).await;
        for event in domain_events(std::slice::from_ref(&env)) {
            if let pb::domain_event::Event::PendingAttentionRemoved(removed) = event {
                if removed.native_id == QUESTION_ID {
                    question_removed = true;
                }
            }
        }
    }
    assert!(question_removed, "问题回答后必须按原生事件移除 attention");

    // 回答审批(原生 decision ID)→ PendingAttentionRemoved(approval)。
    send_env(
        &mut browser,
        &command_request(
            &uuid::Uuid::new_v4().to_string(),
            ATTN,
            &device_id,
            command_request::Payload::AnswerApproval(pb::AnswerApprovalPayload {
                approval_id: APPROVAL_ID.to_string(),
                decision_id: APPROVAL_DECISION.to_string(),
            }),
        ),
    )
    .await;
    let mut approval_removed = false;
    let remove_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !approval_removed && tokio::time::Instant::now() < remove_deadline {
        let env = recv_env(&mut browser, Duration::from_secs(10)).await;
        for event in domain_events(std::slice::from_ref(&env)) {
            if let pb::domain_event::Event::PendingAttentionRemoved(removed) = event {
                if removed.native_id == APPROVAL_ID {
                    approval_removed = true;
                }
            }
        }
    }
    assert!(approval_removed, "审批决定后必须按原生事件移除 attention");
    // 权威视角:HTTP runtime 快照中 attention 为空。
    let attn_row: (uuid::Uuid,) = sqlx::query_as(
        "SELECT id FROM session_summaries WHERE device_id = $1 AND native_session_id = $2",
    )
    .bind(uuid::Uuid::parse_str(&device_id).unwrap())
    .bind(ATTN)
    .fetch_one(&pool)
    .await
    .unwrap();
    let attn_snapshot = http_get_json(
        &http,
        &format!(
            "{}/agent-console/api/sessions/{}/runtime",
            relay.base, attn_row.0
        ),
        &identity,
    )
    .await;
    let snap = &attn_snapshot["runtimeSnapshot"];
    assert!(
        snap["pendingQuestions"].as_array().unwrap().is_empty()
            && snap["pendingApprovals"].as_array().unwrap().is_empty(),
        "回答后权威快照不应再有 pending attention: {snap}"
    );

    // =====================================================================
    // 场景 9:1 个后台命令在 turn 完成后仍在运行
    // =====================================================================
    let r3 = uuid::Uuid::new_v4().to_string();
    send_env(
        &mut browser,
        &command_request(
            &r3,
            BG,
            &device_id,
            command_request::Payload::StartTurn(pb::StartTurnPayload {
                prompt: "e2e-prompt-background".to_string(),
            }),
        ),
    )
    .await;
    let bg_row: (uuid::Uuid,) = sqlx::query_as(
        "SELECT id FROM session_summaries WHERE device_id = $1 AND native_session_id = $2",
    )
    .bind(uuid::Uuid::parse_str(&device_id).unwrap())
    .bind(BG)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 权威快照轮询:runtimeStatus idle(currentTurn null)且后台命令仍 RUNNING。
    let mut bg_snapshot_ok = false;
    let mut last_bg_snapshot = serde_json::Value::Null;
    for _ in 0..80 {
        let body = http_get_json(
            &http,
            &format!(
                "{}/agent-console/api/sessions/{}/runtime",
                relay.base, bg_row.0
            ),
            &identity,
        )
        .await;
        let snap = &body["runtimeSnapshot"];
        last_bg_snapshot = snap.clone();
        let idle = snap["currentTurn"].is_null();
        let count = snap["backgroundCommandCount"].as_u64().unwrap_or(0);
        let running = snap["backgroundCommands"].as_array().map(|a| {
            a.iter().any(|c| {
                c["commandId"].as_str() == Some(BG_COMMAND_ID)
                    && c["state"].as_str() == Some("BACKGROUND_CMD_RUNNING")
            })
        });
        if idle && count == 1 && running == Some(true) {
            bg_snapshot_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        bg_snapshot_ok,
        "turn 完成后应仍有 1 个后台命令在运行(currentTurn=null, count=1, \
         BACKGROUND_CMD_RUNNING);最后快照: {last_bg_snapshot}"
    );

    // =====================================================================
    // 场景 10:文件预览全链路经 Relay;Relay 临时目录与 PostgreSQL 无正文
    // =====================================================================
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let file_marker = format!("e2e-file-body-MARKER-{}", uuid::Uuid::new_v4().simple());
    let file_bytes = format!("{file_marker}\npayload-line-2\n");
    std::fs::write(workspace.join("notes.txt"), &file_bytes).unwrap();
    store
        .authorize_workspace(&workspace, "e2e-workspace")
        .await
        .unwrap();
    // 用户在会话上下文明确打开文件(§22.2 来源之一)→ Bridge 签发短期 handle。
    let file_handle = runtime
        .grants()
        .issue(
            &device_id,
            MAIN,
            &workspace,
            Path::new("notes.txt"),
            GrantActions::PREVIEW,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap();
    // Browser 已认证 preview → Relay transfer → Bridge 复验 + producer 出站 →
    // Relay 有界 pipe → Browser(§22.4)。
    let mut req = http.post(format!(
        "{}/agent-console/api/sessions/{session_uuid}/files/preview",
        relay.base
    ));
    for (k, v) in &identity {
        req = req.header(*k, v);
    }
    let resp = req
        .json(&serde_json::json!({
            "fileHandle": file_handle.token,
            "fileName": "notes.txt",
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "preview 失败: {status} {:?}",
        resp.text().await.unwrap_or_default()
    );
    assert_eq!(
        resp.headers().get("x-content-type-options").unwrap(),
        "nosniff",
        "§22.3 安全响应头"
    );
    assert_eq!(
        resp.headers().get("cache-control").unwrap(),
        "private, no-store"
    );
    let preview_bytes = resp.bytes().await.unwrap();
    assert_eq!(
        preview_bytes.as_ref(),
        file_bytes.as_bytes(),
        "producer 正文必须逐字节一致(全链路经 Relay)"
    );

    // PostgreSQL 全表/全文本列扫描:正文/prompt/ticket/凭据/短码均不得出现。
    assert_db_free_of(
        &pool,
        &[
            file_marker.as_str(),
            PROMPT,
            FINAL_OUTPUT.trim_end_matches('\n'),
            ticket_1.as_str(),
            ticket_2.as_str(),
            credential.as_str(),
            short_code.as_str(),
        ],
    )
    .await;
    // 临时目录扫描:除授权工作区源文件外,任何文件不得含文件正文
    // (Relay 全程内存 pipe,不落盘;§22.1/§25.2)。
    assert_tempdir_free_of(std::env::temp_dir(), &workspace, &file_marker);

    // =====================================================================
    // 场景 11:撤销 auth session → Browser WS 在窗口内以稳定 reason 关闭
    // =====================================================================
    toolbox.revoke_session(support::AUTH_SESSION);
    let close = recv_close(&mut browser, Duration::from_secs(10)).await;
    assert_eq!(close.0, 1008, "auth 类关闭使用 1008(§20.5)");
    assert_eq!(close.1, "AUTH_EXPIRED", "稳定 close reason");

    // =====================================================================
    // 收尾断言:日志白名单(§25.3)+ 优雅停机(§26.4)
    // =====================================================================
    logs.assert_free_of(&[
        ticket_1.as_str(),
        ticket_2.as_str(),
        credential.as_str(),
        short_code.as_str(),
        PROMPT,
        FINAL_OUTPUT.trim_end_matches('\n'),
        file_marker.as_str(),
    ]);

    runtime.shutdown().await;
    observer.abort();
    handle.shutdown();
    let _ = pump.await;
    relay.server.shutdown().await.expect("relay shutdown");
    owner_task.abort();
    pool.close().await;
    let _ = std::fs::remove_file(&socket); // fake owner socket 文件
    drop(root); // tempfile 自动清理 data_dir/workspace
    drop(pg); // docker 容器强制移除
}

// ---------------------------------------------------------------------------
// 深度断言辅助
// ---------------------------------------------------------------------------

async fn http_get_json(
    http: &reqwest::Client,
    url: &str,
    identity: &[(&str, String)],
) -> serde_json::Value {
    let mut req = http.get(url.to_string());
    for (k, v) in identity {
        req = req.header(*k, v);
    }
    let resp = req.send().await.unwrap();
    assert!(resp.status().is_success(), "GET {url} → {}", resp.status());
    resp.json().await.unwrap()
}

/// 扫描 public schema 全部表/文本列:任何列都不得包含敏感串(§25.2)。
async fn assert_db_free_of(pool: &sqlx::PgPool, secrets: &[&str]) {
    let columns = sqlx::query_as::<_, (String, String, String)>(
        "SELECT table_name, column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'public' \
           AND data_type IN ('text','bytea','character varying')",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert!(!columns.is_empty(), "应有业务表");
    for (table, column, data_type) in columns {
        for secret in secrets {
            let predicate = if data_type == "bytea" {
                format!("encode(\"{column}\", 'escape') LIKE '%{secret}%'")
            } else {
                format!("\"{column}\" LIKE '%{secret}%'")
            };
            let sql = format!("SELECT count(*) FROM \"{table}\" WHERE {predicate}");
            let (n,): (i64,) = sqlx::query_as(&sql).fetch_one(pool).await.unwrap();
            assert_eq!(
                n,
                0,
                "表 {table}.{column} 中出现敏感串(违反 §25.2,前 8 字符: {})",
                &secret[..secret.len().min(8)]
            );
        }
    }
}

/// 扫描临时目录:除授权工作区源文件外,任何文件不得含文件正文。
fn assert_tempdir_free_of(scan_root: std::path::PathBuf, workspace: &Path, marker: &str) {
    let mut stack = vec![scan_root];
    let mut checked = 0usize;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // macOS /var → /private/var 符号链接:统一经 canonicalize 比较。
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            if canonical.starts_with(workspace) {
                continue; // 工作区源文件是正文的唯一合法副本。
            }
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            if !meta.is_file() || meta.len() > 4 * 1024 * 1024 || checked > 20_000 {
                continue;
            }
            if let Ok(content) = std::fs::read(&path) {
                checked += 1;
                assert!(
                    !contains_subsequence(&content, marker.as_bytes()),
                    "临时目录文件 {:?} 中出现文件正文(违反 §22.1/§25.2)",
                    path
                );
            }
        }
    }
    assert!(checked > 0, "临时目录扫描应覆盖到文件");
}

/// 字节级子串检查(避免大 buffer String 化)。
fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
