//! 同域真实网关端到端联调(dev-toolbox 真实 API + Caddy 实际网关配置)。
//!
//! 组件(全部测试内拉起、guard 清理;编排见 gateway_support 模块文档):
//! pgvector:pg16(两库+独立用户)→ cmd/migrate 迁移 → init-owner →
//! dev-toolbox server(真实 Go 进程)→ 进程内 relay → Caddy(原 Caddyfile
//! 只读挂载 + env 适配 + socat 上游别名)→ 进程内 Bridge(fake IPC owner)。
//! Bridge 的 AGENT_CONSOLE_RELAY_URL、浏览器 HTTP/WS、数据面
//! producer/consumer 全部走 Caddy 公网 origin;relay 调 dev-toolbox 走
//! 宿主直连内部地址(生产语义:内部调用走内网,不经网关)。
//!
//! 断言 → 映射(§4/§20/§21/§22/§29.4;测试内以 `断言 X` 注释分节):
//!  A 真实登录 session+CSRF   → login→TOTP setup→本地 RFC6238 码→confirm,
//!                              服务端签发真实 auth session(非 SQL 种子)
//!  B ticket 经网关 + no-store→ POST /api/v1/agent-console/ws-tickets
//!  C 浏览器 WS 经网关        → subprotocol ticket 握手 + ServerHello
//!  D Bridge 经网关上线       → devices API CONNECTION_ONLINE
//!  E 订阅 snapshot 顺序      → Subscribed → 快照(sequence==base) → 递增事件
//!  F command+重试只执行一次  → accepted→result;断线同 request ID 重试,
//!                              fake owner running turn 计数 == 1
//!  G 配对路径(即 D 过程)   → 短码 lookup;并发 approve 单赢家(200+409)
//!  H preview/download/upload → producer/consumer 数据面全经网关,逐字节一致
//!  I 正文不落库/不落临时目录 → relay 库全表扫描 + tempdir 扫描
//!  J 撤销 auth session       → 真实 DELETE /api/v1/auth/sessions/{id} 经网关,
//!                              浏览器 WS 1008/AUTH_EXPIRED 关闭(§20.5)
//!  K 撤销设备                → DELETE /agent-console/api/devices/{id} 经网关,
//!                              Bridge WS 1008/DEVICE_REVOKED;旧凭据重连 401
//!  L 伪造身份头直连网关      → 被网关剥离,forward-auth 401(不能伪造身份)
//!  M 写 API fail-closed      → 无 session / 缺 Origin / 缺 CSRF → 一律拒绝
//!  N 数据面无 Cookie 也可走  → Bridge 本不带 Cookie(H 覆盖);错误设备凭据
//!                              或 transfer token → 401
//!  O /internal/* 经网关      → 公网 404
//!
//! 运行:`cargo test -p relay --test e2e_gateway -- --ignored`
//! (#[ignore]:整链需要 docker + go 工具链 + dev-toolbox 仓库,不随常规
//! cargo test 拉起;e2e_no_ui 保持默认运行)。

mod gateway_support;
mod support;

use std::{path::Path, sync::Arc, time::Duration};

use agent_console_protocol::agent_console::v1 as pb;
use agent_console_protocol::agent_console::v1::{command_request, envelope, subscribe};
use agent_console_protocol::codec::{decode_envelope, new_message_id, PROTOCOL_VERSION};
use futures::StreamExt;

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
use bridge::transport::{ConnectionState, RelayClientOptions, StaticCredential};

use gateway_support::{
    BrowserSession, Gateway, GatewayPostgres, GoBinaries, LogCapture, ToolboxServer,
};
use support::{free_port, recv_close, send_env, Ws};

// ---------------------------------------------------------------------------
// 合成会话与敏感常量(marker 供正文泄漏断言)
// ---------------------------------------------------------------------------

const MAIN: &str = "eeeeeeee-aaaa-4aaa-8aaa-111111111111";
const PROMPT: &str = "gw-e2e-prompt-MARKER-main-turn";
const FINAL_OUTPUT: &str = "GW-AUTHORITATIVE-FINAL-OUTPUT-MARKER\n";

fn interactive_control_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("AC_E2E_BROWSER_CONTROL").map(std::path::PathBuf::from)
}

/// 可选交互式浏览器检查点。凭据全部是本次测试生成的临时值，只写入调用方
/// 指定的 0700 临时目录，不进 stdout、日志或仓库。创建 `<stage>.done` 后继续。
async fn browser_checkpoint(stage: &str, payload: serde_json::Value) {
    let Some(dir) = interactive_control_dir() else {
        return;
    };
    std::fs::create_dir_all(&dir).expect("create browser control dir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .expect("chmod browser control dir");
    }
    let info = dir.join(format!("{stage}.json"));
    let done = dir.join(format!("{stage}.done"));
    let _ = std::fs::remove_file(&done);
    std::fs::write(&info, serde_json::to_vec(&payload).unwrap()).expect("write checkpoint info");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&info, std::fs::Permissions::from_mode(0o600))
            .expect("chmod checkpoint info");
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30 * 60);
    while !done.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "browser checkpoint {stage} timed out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_file(info);
    let _ = std::fs::remove_file(done);
}

fn owner_script(cwd: &Path) -> serde_json::Value {
    serde_json::json!({
        "sessions": [
            {
                "conversationId": MAIN,
                "title": "gw-e2e-main",
                "cwd": cwd,
                "branch": "gw-branch",
                "turn": {
                    "outputLines": (0..40).map(|i| format!("gline-{i:02}")).collect::<Vec<_>>(),
                    "lineDelayMs": 120,
                    "finalAnswer": "gw-final-answer",
                    "finalOutput": FINAL_OUTPUT,
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
        git_branch: Some("gw-branch".to_string()),
        created_at: None,
        updated_at: None,
        archived: false,
        agent_nickname: None,
        agent_role: None,
    }
}

// ---------------------------------------------------------------------------
// 协议辅助(与 e2e_no_ui 同形;自包含避免跨测试文件共享)
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
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) => return frames,
            Err(_) => {}
        }
    }
}

fn payloads(frames: &[pb::Envelope]) -> Vec<&pb::envelope::Payload> {
    frames.iter().filter_map(|f| f.payload.as_ref()).collect()
}

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

/// §17.4/§17.5 顺序断言:Subscribed → 快照(sequence==base_sequence) →
/// 后续同流帧 sequence 严格递增。
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
    assert_eq!(
        rest[snap_pos].sequence, base,
        "快照 sequence 必须等于 base_sequence(§17.4)"
    );
    let mut last = rest[snap_pos].sequence;
    for frame in &rest[snap_pos + 1..] {
        if frame.stream_id != stream {
            continue;
        }
        assert!(frame.sequence > last, "sequence 必须严格递增(§17.5)");
        last = frame.sequence;
    }
}

// ---------------------------------------------------------------------------
// 测试主链
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "gateway-e2e: 需 docker + go 工具链 + dev-toolbox 仓库;运行: cargo test -p relay --test e2e_gateway -- --ignored"]
async fn e2e_gateway_real_chain() {
    let logs = LogCapture::init();
    let interactive_browser = interactive_control_dir().is_some();
    let root = tempfile::tempdir().unwrap();
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let data_dir = root.path().join("data");

    // =====================================================================
    // 装配 1:PostgreSQL(pgvector:pg16;两库 + relay 独立用户)
    // =====================================================================
    let pg = GatewayPostgres::start().await;

    // dev-toolbox 迁移 + owner 初始化(真实 CLI;密码走受控临时文件)。
    let bins: GoBinaries = gateway_support::build_go_binaries();
    gateway_support::run_migrations(&bins.migrate, &pg.toolbox_db_url());
    let username = "gw-e2e-owner";
    let password = format!("gw-e2e-pass-{}", uuid::Uuid::new_v4().simple());
    gateway_support::init_owner(&bins.cli, &pg.toolbox_db_url(), username, &password, &work);

    // 内部服务令牌(relay / dev-toolbox / caddy 同值;≥32 字符)。
    let internal_token = format!("gw-e2e-internal-{}", uuid::Uuid::new_v4().simple());
    let api_port = free_port();
    let site_port = free_port();
    let relay_port = free_port();
    let app_origin = format!("http://127.0.0.1:{site_port}");

    // =====================================================================
    // 装配 2:Relay(进程内;introspect 1s 观测撤销窗口)。可信代理网段
    // 放宽到全网:仅测试专用 —— socat 网关容器经 OrbStack host NAT 连入,
    // 源地址不固定;生产配置必须绑定真实网关网络网段(§20.4)。
    // =====================================================================
    let relay = relay::serve(relay::state::Config {
        database_url: pg.relay_db_url(),
        bind_addr: format!("127.0.0.1:{relay_port}").parse().unwrap(),
        internal_token: Some(internal_token.clone()),
        // 生产语义:relay 调 dev-toolbox 走内部网络,不经公网网关。
        devtoolbox_base_url: Some(format!("http://127.0.0.1:{api_port}")),
        trusted_proxy_cidrs: ["0.0.0.0/0", "::/0"]
            .iter()
            .map(|c| c.parse().unwrap())
            .collect(),
        introspect_interval: Duration::from_secs(1),
        auth_grace: Duration::from_secs(120),
        heartbeat_interval: Duration::from_secs(2),
        // 人工浏览器检查点最长等待 30 分钟；该模式下原始撤销测试 WS 没有
        // BridgeRuntime 代发心跳，因此给它留出略长于检查点的存活窗口。
        offline_after: if interactive_browser {
            Duration::from_secs(35 * 60)
        } else {
            Duration::from_secs(30)
        },
    })
    .await
    .expect("relay serve");
    assert_eq!(relay.addr.port(), relay_port);

    // =====================================================================
    // 装配 3:dev-toolbox server(真实进程)+ Caddy(原 Caddyfile + env 适配)
    // =====================================================================
    let toolbox = ToolboxServer::start(
        &bins.server,
        &pg.toolbox_db_url(),
        api_port,
        &app_origin,
        &internal_token,
        &work,
    )
    .await;
    let caddyfile = gateway_support::devtoolbox_root()
        .join("deploy")
        .join("Caddyfile");
    let gateway =
        Gateway::start(&caddyfile, api_port, relay_port, site_port, &internal_token).await;

    let pool = sqlx::postgres::PgPool::connect(&pg.relay_db_url())
        .await
        .unwrap();
    let http = reqwest::Client::new();

    // =====================================================================
    // 断言 A:真实登录拿到 session+CSRF(服务端签发,非 SQL 种子)
    // =====================================================================
    let (browser, totp_secret) = BrowserSession::login(&gateway.base, username, &password).await;
    assert!(!browser.csrf.is_empty(), "登录必须下发 CSRF token");
    assert!(!browser.session_id.is_empty(), "登录必须返回 session id");
    assert_eq!(browser.owner_id.len(), 36, "owner id 为 UUID");
    // 服务端真实会话可校验(bootstrap 端点,同一 Cookie)。
    let resp = browser.get("/api/v1/auth/session").send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "登录会话应可通过真实服务端校验: {}",
        resp.status()
    );
    // 第二个独立会话(真实 mfa/verify 流程):J 撤销会话 1 后,K/M 断言
    // 仍需要一个有效浏览器会话(§20.5:重新登录必须新会话)。
    let browser_2 =
        BrowserSession::login_again(&gateway.base, username, &password, &totp_secret).await;

    // =====================================================================
    // 断言 G + D:配对 v2 全经网关(register/claim 走 Caddy 公网合同路径)
    // =====================================================================
    let pairing = bridge::pairing::PairingClient::new(&gateway.base).unwrap();
    let registration = pairing.register("gw-e2e-mac", "0.1.0").await.unwrap();
    let short_code = registration.short_code.clone();
    assert_eq!(short_code.len(), 6, "6 位短码");

    // 浏览器经网关 lookup(短码即凭证;展示设备信息)。
    let resp = browser
        .post("/agent-console/api/pairing/lookup")
        .json(&serde_json::json!({ "shortCode": short_code }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "经网关短码 lookup 失败: status={} body={}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    let lookup: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(lookup["deviceName"], "gw-e2e-mac");
    assert_eq!(lookup["platform"], std::env::consts::OS);

    // Bridge 轮询 claim(经网关),批准后 ready 单次交付;凭据先入 Keychain。
    let pair_base = gateway.base.clone();
    let pair_store = LocalStore::open(&data_dir).await.unwrap();
    let keychain = Arc::new(InMemoryKeychainStore::new());
    let pair_keychain = keychain.clone();
    let pair_task = tokio::spawn(async move {
        let client = bridge::pairing::PairingClient::new(&pair_base).unwrap();
        let (device_id, credential) = client
            .wait_approval(
                &registration,
                bridge::pairing::PairingOptions {
                    poll_interval: if interactive_browser {
                        bridge::pairing::CLAIM_POLL_INTERVAL
                    } else {
                        Duration::from_millis(100)
                    },
                    deadline: if interactive_browser {
                        Duration::from_secs(10 * 60)
                    } else {
                        Duration::from_secs(30)
                    },
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

    if interactive_browser {
        browser_checkpoint(
            "pairing",
            serde_json::json!({
                "base": gateway.base,
                "username": username,
                "password": password,
                "totpSecret": totp_secret,
                "shortCode": short_code,
            }),
        )
        .await;
    } else {
        // 并发 approve:单赢家原子批准 —— 一个 200,另一个 409 ALREADY_APPROVED。
        let approve_body = serde_json::json!({
            "shortCode": short_code,
            "challengeId": lookup["challengeId"],
        });
        let (first, second) = tokio::join!(
            browser
                .post("/agent-console/api/pairing/approve")
                .json(&approve_body)
                .send(),
            browser
                .post("/agent-console/api/pairing/approve")
                .json(&approve_body)
                .send()
        );
        let statuses = [first.unwrap().status(), second.unwrap().status()];
        let winners = statuses.iter().filter(|s| s.is_success()).count();
        let losers = statuses
            .iter()
            .filter(|s| **s == reqwest::StatusCode::CONFLICT)
            .count();
        assert_eq!(winners, 1, "并发 approve 必须恰有一个赢家: {statuses:?}");
        assert_eq!(
            losers, 1,
            "落选者必须得到 409 ALREADY_APPROVED(单赢家): {statuses:?}"
        );
    }

    let (device_id, credential) = pair_task.await.unwrap();
    assert_eq!(
        keychain.get_device_credential(&device_id).await.unwrap(),
        Some(credential.clone()),
        "凭据只进 Keychain"
    );
    let binding_store = LocalStore::open(&data_dir).await.unwrap();
    let binding = binding_store.get_binding().await.unwrap();
    assert_eq!(binding.device_id, device_id);
    assert_eq!(binding.status, BindingStatus::Paired);
    assert_eq!(
        binding.relay_url, gateway.base,
        "绑定 origin = 网关公网 origin"
    );

    // 仅测试临时目录中的 Git fixture；产品路径仍只执行 Git 读命令。
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    for args in [
        vec!["init", "-q", "-b", "fixture-main"],
        vec!["config", "user.email", "fixture@example.invalid"],
        vec!["config", "user.name", "Fixture"],
    ] {
        let status = std::process::Command::new("git")
            .current_dir(&workspace)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "prepare temporary git fixture");
    }
    std::fs::write(workspace.join("browser-git.txt"), "before\n").unwrap();
    assert!(std::process::Command::new("git")
        .current_dir(&workspace)
        .args(["add", "browser-git.txt"])
        .status()
        .unwrap()
        .success());
    assert!(std::process::Command::new("git")
        .current_dir(&workspace)
        .args(["commit", "-qm", "fixture base"])
        .status()
        .unwrap()
        .success());
    std::fs::write(workspace.join("browser-git.txt"), "before\nafter\n").unwrap();
    let workspace = workspace.canonicalize().unwrap();
    binding_store
        .authorize_workspace(&workspace, "gw-workspace")
        .await
        .unwrap();

    // ---- Bridge 组装:WS 与数据面全部指向网关公网 origin ----
    let socket = std::env::temp_dir().join(format!(
        "ac-gw-e2e-{}-{}.sock",
        std::process::id(),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    let owner_task = tokio::spawn(bridge::adapter::codex::fake_owner::run_server(
        socket.clone(),
        owner_script(&workspace),
    ));
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "fake owner socket did not appear");

    let mut adapter_config = CodexAdapterConfig::new(device_id.clone());
    adapter_config.ipc_socket = Some(socket.clone());
    adapter_config.codex_home = Some(root.path().join("codex-home"));
    adapter_config.version_report = Some("codex-cli 0.153.1".to_string());
    adapter_config.write_method_probes = WriteMethodProbes {
        start_turn: ProbeResult::Passed,
        steer_turn: ProbeResult::Passed,
        interrupt_turn: ProbeResult::Passed,
        answer_question: ProbeResult::NotProbed,
        update_settings: ProbeResult::NotProbed,
    };
    adapter_config.static_sessions = vec![catalog_seed(MAIN, "gw-e2e-main")];
    let adapter = CodexAdapter::connect(adapter_config).await.unwrap();
    let store = binding_store;
    let gateway_cmd = CommandGateway::new(adapter.clone(), store.clone());
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

    let relay_urls = bridge::config::RelayUrls::parse(&gateway.base).unwrap();
    let mut options = RelayClientOptions::new(&relay_urls).with_device_id(device_id.clone());
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
            .with_relay_url(gateway.base.clone()),
        device_id: device_id.clone(),
        store: store.clone(),
        adapter: adapter.clone(),
        gateway: gateway_cmd,
        grants,
        git: bridge::git::GitService::new().ok().map(Arc::new),
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while handle.state() != ConnectionState::Connected {
        assert!(tokio::time::Instant::now() < deadline, "bridge 未连上网关");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 断言 D:devices API(经网关 forward-auth)显示在线。
    let mut online = false;
    for _ in 0..100 {
        let resp = browser
            .get("/agent-console/api/devices")
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "devices API 经网关失败: {}",
            resp.status()
        );
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(online, "设备应经网关显示 CONNECTION_ONLINE(断言 D)");

    // =====================================================================
    // 断言 B + C:ticket 经网关(no-store)→ 浏览器 WS 经网关 + ServerHello
    // =====================================================================
    let ticket_1 = browser.request_ws_ticket().await;
    let mut browser_ws = gateway_support::connect_browser_ws(&gateway.base, &ticket_1)
        .await
        .expect("浏览器 WS 经网关连接(断言 C)");
    // ticket 单次消费:重放 401(升级前拒绝,§20.2)。
    let reused = gateway_support::connect_browser_ws(&gateway.base, &ticket_1).await;
    assert_eq!(reused.unwrap_err(), 401, "已消费 ticket 必须拒绝");

    // =====================================================================
    // 断言 E:订阅 list + 详情;snapshot 先于 base_sequence 后事件
    // =====================================================================
    send_env(&mut browser_ws, &subscribe_list()).await;
    let list_frames = collect_until(&mut browser_ws, Duration::from_secs(15), |frames| {
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
    assert!(
        summary_batch
            .summaries
            .iter()
            .any(|s| s.session_key.as_ref().unwrap().native_session_id == MAIN),
        "list 快照应含主会话"
    );

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

    send_env(&mut browser_ws, &subscribe_session(&device_id, MAIN)).await;
    let mut main_frames = collect_until(&mut browser_ws, Duration::from_secs(15), |frames| {
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
    let main_stream = main_frames
        .iter()
        .find(|f| {
            matches!(
                f.payload.as_ref(),
                Some(envelope::Payload::RuntimeSnapshot(_))
            )
        })
        .map(|f| f.stream_id.clone())
        .expect("详情流 snapshot 缺失");
    assert_subscribe_order(&main_frames, &main_stream);
    assert_subscribe_order(&list_frames, &list_stream);

    // Git 只读查询同样经过真实网关；fixture worktree 仅位于测试临时目录。
    let git_summary: serde_json::Value = browser
        .get(&format!("/agent-console/api/sessions/{session_uuid}/git"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(git_summary["gitSummary"]["branch"], "fixture-main");
    assert!(git_summary["gitSummary"]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["relativePath"] == "browser-git.txt"
                && entry["status"] == "modified"
                && entry["staged"] == false
        })));
    let git_diff: serde_json::Value = browser
        .get(&format!(
            "/agent-console/api/sessions/{session_uuid}/git/diff?path=browser-git.txt&staged=false"
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(git_diff["gitFileDiff"]["patchText"]
        .as_str()
        .is_some_and(|patch| patch.contains("+after")));

    if interactive_browser {
        browser_checkpoint(
            "online",
            serde_json::json!({
                "base": gateway.base,
                "username": username,
                "password": password,
                "totpSecret": totp_secret,
                "sessionUuid": session_uuid,
                "nativeSessionId": MAIN,
                "deviceId": device_id,
            }),
        )
        .await;

        // 交互式检查点会用第二个浏览器客户端实际发起命令。操作方只在该命令
        // 完成后放行；这里消费已经广播到本测试客户端的帧，避免它们污染下方
        // 专门验证 r1 断线重试幂等性的执行计数。
        let _ = collect_until(&mut browser_ws, Duration::from_millis(500), |_| false).await;
        main_frames.clear();
    }

    // =====================================================================
    // 断言 F:command accepted → result;断线同 request ID 重试只执行一次
    // =====================================================================
    let r1 = uuid::Uuid::new_v4().to_string();
    send_env(
        &mut browser_ws,
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
    let accepted = collect_until(&mut browser_ws, Duration::from_secs(15), |frames| {
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
    assert!(!accepted.is_empty(), "应收到 ACCEPTED_BY_BRIDGE 回执");
    main_frames.extend(accepted.iter().cloned());

    // 回执落库后再断开:对调用方而言 accepted 回执"丢失"。
    let mut receipt_db: Option<String> = None;
    for _ in 0..200 {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM request_receipts WHERE request_id = $1")
                .bind(&r1)
                .fetch_optional(&pool)
                .await
                .unwrap();
        if let Some((status,)) = row {
            if matches!(
                status.as_str(),
                "ACCEPTED_BY_BRIDGE" | "DISPATCHED_TO_CODEX" | "COMPLETED"
            ) {
                receipt_db = Some(status);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    receipt_db.as_ref().expect("回执应落库");

    let _ = browser_ws.close(None).await;
    drop(browser_ws);

    let ticket_2 = browser.request_ws_ticket().await;
    let mut browser_ws = gateway_support::connect_browser_ws(&gateway.base, &ticket_2)
        .await
        .expect("重连成功");
    send_env(&mut browser_ws, &subscribe_session(&device_id, MAIN)).await;
    let resub = collect_until(&mut browser_ws, Duration::from_secs(10), |frames| {
        payloads(frames)
            .iter()
            .any(|p| matches!(p, envelope::Payload::Subscribed(_)))
    })
    .await;
    assert!(!resub.is_empty(), "重连后重订阅应建立");
    main_frames.extend(resub.iter().cloned());
    send_env(
        &mut browser_ws,
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
    let retry = collect_until(&mut browser_ws, Duration::from_secs(10), |frames| {
        payloads(frames)
            .iter()
            .any(|p| matches!(p, envelope::Payload::CommandResult(r) if r.request_id == r1))
    })
    .await;
    main_frames.extend(retry.iter().cloned());
    let replayed = payloads(&retry)
        .into_iter()
        .find_map(|p| match p {
            envelope::Payload::CommandResult(r) if r.request_id == r1 => Some(r.clone()),
            _ => None,
        })
        .expect("重试应回放既有回执");
    let (db_status,): (String,) =
        sqlx::query_as("SELECT status FROM request_receipts WHERE request_id = $1")
            .bind(&r1)
            .fetch_one(&pool)
            .await
            .unwrap();
    let db_code = match db_status.as_str() {
        "RECEIVED" => pb::CommandReceiptStatus::ReceiptReceived,
        "ACCEPTED_BY_BRIDGE" => pb::CommandReceiptStatus::ReceiptAcceptedByBridge,
        "DISPATCHED_TO_CODEX" => pb::CommandReceiptStatus::ReceiptDispatchedToCodex,
        "COMPLETED" => pb::CommandReceiptStatus::ReceiptCompleted,
        "REJECTED" => pb::CommandReceiptStatus::ReceiptRejected,
        _ => pb::CommandReceiptStatus::ReceiptOutcomeUnknown,
    };
    assert_eq!(
        replayed.status, db_code as i32,
        "重试必须回放库内既有回执(不重发,§15.2)"
    );

    // 等 MAIN turn 完成;fake owner 执行计数 == 1(fake owner 的 turn id
    // 由每次实际执行 start_turn 递增生成:turn-1、turn-2…;重复执行必然
    // 出现第二个 turn id)。全帧(TurnLifecycle + RuntimeSnapshot.currentTurn)
    // 收集到的唯一 turn id 即执行次数。
    let completion = collect_until(&mut browser_ws, Duration::from_secs(40), |frames| {
        turn_lifecycle_events(frames).iter().any(|t| {
            t.phase == pb::ActiveTurnPhase::TurnPhaseIdle as i32
                && t.outcome == pb::LastTurnOutcome::TurnOutcomeCompleted as i32
        })
    })
    .await;
    main_frames.extend(completion.iter().cloned());
    let mut turn_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for t in turn_lifecycle_events(&main_frames) {
        if let Some(turn) = t.turn.as_ref() {
            turn_ids.insert(turn.id.clone());
        }
    }
    for f in &main_frames {
        if let Some(envelope::Payload::RuntimeSnapshot(snap)) = f.payload.as_ref() {
            if let Some(current) = snap.current_turn.as_ref() {
                if let Some(turn) = current.turn.as_ref() {
                    turn_ids.insert(turn.id.clone());
                }
            }
        }
    }
    turn_ids.retain(|id| !id.is_empty());
    assert_eq!(
        turn_ids.len(),
        1,
        "断线重试不得重复执行(断言 F);fake owner 实际执行的 turn: {turn_ids:?}"
    );

    // =====================================================================
    // 断言 H:preview / download(Range)/ upload 全经网关(producer/consumer)
    // =====================================================================
    let file_marker = format!("gw-file-body-MARKER-{}", uuid::Uuid::new_v4().simple());
    let file_bytes = format!("{file_marker}\npayload-line-2\n");
    std::fs::write(workspace.join("notes.txt"), &file_bytes).unwrap();
    let preview_handle = runtime
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

    let resp = browser
        .post(&format!(
            "/agent-console/api/sessions/{session_uuid}/files/preview"
        ))
        .json(&serde_json::json!({
            "fileHandle": preview_handle.token,
            "fileName": "notes.txt",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "preview 经网关失败: {:?}",
        resp.text().await.unwrap_or_default()
    );
    assert_eq!(
        resp.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(
        resp.headers().get("cache-control").unwrap(),
        "private, no-store"
    );
    let preview_bytes = resp.bytes().await.unwrap();
    assert_eq!(
        preview_bytes.as_ref(),
        file_bytes.as_bytes(),
        "producer 正文必须逐字节一致(全链路经网关)"
    );

    // download(attachment)+ 单 Range:bytes=0-15 → 206。
    // POST 经 forward_auth 按写请求校验:Cookie + Origin + X-CSRF-Token。
    let download_handle = runtime
        .grants()
        .issue(
            &device_id,
            MAIN,
            &workspace,
            Path::new("notes.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap();
    let resp = browser
        .post(&format!(
            "/agent-console/api/sessions/{session_uuid}/files/download"
        ))
        .header(reqwest::header::RANGE, "bytes=0-15")
        .json(&serde_json::json!({
            "fileHandle": download_handle.token,
            "fileName": "notes.txt",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::PARTIAL_CONTENT,
        "Range download 经网关失败"
    );
    let ranged = resp.bytes().await.unwrap();
    assert_eq!(
        ranged.as_ref(),
        &file_bytes.as_bytes()[..16],
        "Range 下载内容不符"
    );

    // upload:声明 → 浏览器 PUT 正文经网关 → Bridge consumer 出站接收。
    let upload_marker = format!("gw-upload-body-MARKER-{}", uuid::Uuid::new_v4().simple());
    let upload_body: Vec<u8> = {
        let line = format!("{upload_marker}\n");
        let mut out = Vec::new();
        for i in 0..800 {
            out.extend_from_slice(format!("{line:>80}{i:06}\n").as_bytes());
        }
        out
    };
    let resp = browser
        .post(&format!(
            "/agent-console/api/sessions/{session_uuid}/files/upload"
        ))
        .json(&serde_json::json!({
            "fileName": "upload.bin",
            "mime": "application/octet-stream",
            "length": upload_body.len(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "upload 声明失败"
    );
    let declared: serde_json::Value = resp.json().await.unwrap();
    let upload_url = declared["uploadUrl"]
        .as_str()
        .expect("uploadUrl")
        .to_string();

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::COOKIE, browser.cookie.parse().unwrap());
    headers.insert(reqwest::header::ORIGIN, browser.origin.parse().unwrap());
    headers.insert("X-CSRF-Token", browser.csrf.parse().unwrap());
    let resp = http
        .put(format!("{}{upload_url}", gateway.base))
        .headers(headers)
        .body(upload_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "upload 正文流经网关失败: {:?}",
        resp.text().await.unwrap_or_default()
    );
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        result["outcome"], "TRANSFER_OUTCOME_COMPLETED",
        "upload 应完成: {result}"
    );
    assert!(
        result["uploadFileHandle"]
            .as_str()
            .is_some_and(|handle| !handle.is_empty()),
        "upload 完成必须把 Bridge 签发的 session-scoped handle 返回浏览器"
    );
    // Bridge 私有上传目录是上传正文的唯一合法副本(§22.5)。
    let uploads_dir = data_dir.join("uploads");
    let mut uploaded_found = false;
    let mut stack = vec![uploads_dir.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.metadata().map(|m| m.is_dir()).unwrap_or(false) {
                stack.push(path);
                continue;
            }
            if let Ok(content) = std::fs::read(&path) {
                if content == upload_body {
                    uploaded_found = true;
                }
            }
        }
    }
    assert!(
        uploaded_found,
        "Bridge 上传目录应含与正文逐字节一致的文件(§22.5)"
    );

    // =====================================================================
    // 断言 I:transfer 正文不进 relay 库(全表扫描)与临时目录
    // =====================================================================
    assert_db_free_of(
        &pool,
        &[
            file_marker.as_str(),
            upload_marker.as_str(),
            PROMPT,
            FINAL_OUTPUT.trim_end_matches('\n'),
            ticket_1.as_str(),
            ticket_2.as_str(),
            credential.as_str(),
            short_code.as_str(),
        ],
    )
    .await;
    assert_tempdir_free_of(
        std::env::temp_dir(),
        // /var → /private/var 符号链接:允许副本一律 canonicalize 后比较。
        &[&workspace, &data_dir.canonicalize().unwrap()],
        &[&file_marker, &upload_marker],
    );

    // =====================================================================
    // 断言 J:撤销真实 auth session → 浏览器 WS 窗口内 1008/AUTH_EXPIRED
    // =====================================================================
    let resp = browser
        .delete(&format!("/api/v1/auth/sessions/{}", browser.session_id))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "经网关撤销登录会话失败: {}",
        resp.status()
    );
    let close = recv_close(&mut browser_ws, Duration::from_secs(15)).await;
    assert_eq!(close.0, 1008, "auth 类关闭使用 1008(§20.5)");
    assert_eq!(close.1, "AUTH_EXPIRED", "稳定 close reason");

    // =====================================================================
    // 断言 K:撤销设备(经网关 DELETE)→ Bridge WS 1008/DEVICE_REVOKED
    // =====================================================================
    // 停掉进程内 Bridge transport,再用原始凭据开一条经网关的设备 WS。
    handle.shutdown();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while handle.state() != ConnectionState::Disconnected {
        assert!(tokio::time::Instant::now() < deadline, "transport 未停止");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut bridge_ws = gateway_support::connect_bridge_ws(&gateway.base, &device_id, &credential)
        .await
        .expect("设备 WS 经网关重连");
    if interactive_browser {
        browser_checkpoint(
            "revoke-device",
            serde_json::json!({
                "base": gateway.base,
                "username": username,
                "password": password,
                "totpSecret": totp_secret,
                "deviceId": device_id,
            }),
        )
        .await;
        let revoked: (bool,) =
            sqlx::query_as("SELECT revoked_at IS NOT NULL FROM devices WHERE id = $1")
                .bind(uuid::Uuid::parse_str(&device_id).unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(revoked.0, "交互式浏览器必须实际撤销设备");
    } else {
        let resp = browser_2
            .delete(&format!("/agent-console/api/devices/{device_id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NO_CONTENT,
            "经网关撤销设备失败: {}",
            resp.status()
        );
    }
    let close = recv_close(&mut bridge_ws, Duration::from_secs(15)).await;
    assert_eq!(close.0, 1008, "设备撤销关闭使用 1008");
    assert_eq!(close.1, "DEVICE_REVOKED", "稳定 close reason(§21.9)");
    // 撤销后同一凭据认证失败(401,升级前拒绝)。
    let again = gateway_support::connect_bridge_ws(&gateway.base, &device_id, &credential).await;
    assert_eq!(again.unwrap_err(), 401, "已撤销设备凭据必须被拒");

    // =====================================================================
    // 断言 L:伪造 X-Agent-Console-Session-Id/Owner-Id 直连网关 → 剥离 → 401
    // =====================================================================
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "X-Agent-Console-Session-Id",
        uuid::Uuid::new_v4().to_string().parse().unwrap(),
    );
    headers.insert(
        "X-Agent-Console-Owner-Id",
        uuid::Uuid::new_v4().to_string().parse().unwrap(),
    );
    headers.insert(
        "X-Agent-Console-Session-Expires",
        "2027-01-01T00:00:00Z".parse().unwrap(),
    );
    let resp = http
        .get(format!("{}/agent-console/api/devices", gateway.base))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "伪造身份头必须以未认证拒绝"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"]["code"], "AUTH_REQUIRED",
        "伪造身份不能通过(被网关剥离后 forward-auth 拒绝)"
    );

    // =====================================================================
    // 断言 M:写 API fail-closed(无 session / 缺 Origin / 缺 CSRF)
    // =====================================================================
    // 无任何会话。
    let resp = http
        .post(format!(
            "{}/agent-console/api/pairing/approve",
            gateway.base
        ))
        .json(&serde_json::json!({"shortCode": "000000"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "无 session 的写 API 必须被拒"
    );
    // 有 Cookie 但缺 Origin(网关对写请求 fail-closed;会话本身有效,
    // 拒绝只能归因于缺失的 Origin)。
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::COOKIE, browser_2.cookie.parse().unwrap());
    let resp = http
        .post(format!(
            "{}/agent-console/api/pairing/approve",
            gateway.base
        ))
        .headers(headers)
        .json(&serde_json::json!({"shortCode": "000000"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "缺 Origin 的写请求必须 fail-closed"
    );
    // 有 Origin 但 CSRF 错误。
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::COOKIE, browser_2.cookie.parse().unwrap());
    headers.insert(reqwest::header::ORIGIN, browser.origin.parse().unwrap());
    headers.insert("X-CSRF-Token", "wrong-csrf".parse().unwrap());
    let resp = http
        .post(format!(
            "{}/agent-console/api/pairing/approve",
            gateway.base
        ))
        .headers(headers)
        .json(&serde_json::json!({"shortCode": "000000"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "CSRF 错误的写请求必须被拒"
    );

    // =====================================================================
    // 断言 N:数据面无 Cookie 可走(H 中 Bridge producer/consumer 均无
    // Cookie 已覆盖);错误设备凭据 / transfer token → 401
    // =====================================================================
    let resp = http
        .post(format!(
            "{}/agent-console/transfers/producer/{}",
            gateway.base,
            uuid::Uuid::new_v4()
        ))
        .header("Authorization", "Bearer not-a-device-credential")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "错误设备凭据必须 401"
    );
    let resp = http
        .get(format!(
            "{}/agent-console/transfers/consumer/{}",
            gateway.base,
            uuid::Uuid::new_v4()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "无凭据 consumer 必须 401"
    );

    // =====================================================================
    // 断言 O:/internal/agent-console/* 经网关 → 404
    // =====================================================================
    let resp = http
        .post(format!(
            "{}/internal/agent-console/ws-tickets/consume",
            gateway.base
        ))
        .json(&serde_json::json!({"ticket": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/internal/* 公网必须 404"
    );
    let resp = http
        .post(format!(
            "{}/internal/agent-console/auth-sessions/introspect",
            gateway.base
        ))
        .json(&serde_json::json!({"authSessionId": uuid::Uuid::new_v4().to_string()}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // =====================================================================
    // 收尾:日志白名单 + 停机 + 清理
    // =====================================================================
    logs.assert_free_of(&[
        ticket_1.as_str(),
        ticket_2.as_str(),
        credential.as_str(),
        short_code.as_str(),
        password.as_str(),
        PROMPT,
        FINAL_OUTPUT.trim_end_matches('\n'),
        file_marker.as_str(),
        upload_marker.as_str(),
    ]);

    runtime.shutdown().await;
    observer.abort();
    handle.shutdown();
    let _ = pump.await;
    relay.shutdown().await.expect("relay shutdown");
    owner_task.abort();
    pool.close().await;
    let _ = std::fs::remove_file(&socket);
    drop(toolbox);
    drop(gateway);
    drop(pg);
    drop(root);
}

// ---------------------------------------------------------------------------
// 深度断言辅助(与 e2e_no_ui 同形)
// ---------------------------------------------------------------------------

/// 扫描 relay 库 public schema 全部表/文本列:任何列都不得包含敏感串。
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

/// 扫描临时目录:除授权工作区与 Bridge 数据目录(上传唯一合法副本)外,
/// 任何文件不得含任一文件正文 marker。
fn assert_tempdir_free_of(scan_root: std::path::PathBuf, allowed: &[&Path], markers: &[&str]) {
    let mut stack = vec![scan_root];
    let mut checked = 0usize;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            if allowed.iter().any(|a| canonical.starts_with(a)) {
                continue;
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
                for marker in markers {
                    assert!(
                        !contains_subsequence(&content, marker.as_bytes()),
                        "临时目录文件 {path:?} 中出现文件正文(违反 §22.1/§25.2)"
                    );
                }
            }
        }
    }
    assert!(checked > 0, "临时目录扫描应覆盖到文件");
}

fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
