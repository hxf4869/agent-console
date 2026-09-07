//! 集成测试支撑:临时 PostgreSQL(docker,OrbStack)、fake dev-toolbox(axum)、
//! Relay 二进制进程、fake Bridge / fake Browser(tokio-tungstenite + 协议编解码)。

#![allow(dead_code)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use base64::Engine;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    WebSocketStream,
};

pub type WsStream = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
pub type Ws = WsStream;

pub const OWNER: uuid::Uuid = uuid::uuid!("11111111-1111-1111-1111-111111111111");
pub const AUTH_SESSION: uuid::Uuid = uuid::uuid!("22222222-2222-2222-2222-222222222222");
pub const INTERNAL_TOKEN: &str = "test-internal-token";

// ---------------------------------------------------------------------------
// 临时 PostgreSQL
// ---------------------------------------------------------------------------

pub struct Postgres {
    pub name: String,
    pub port: u16,
}

/// 清理此前测试进程遗留的本套件容器。共享 PG 挂在进程级 static 上,进程
/// 退出不会触发 Drop,孤儿容器只能由下一次运行防御性回收;判定标准是
/// 属主测试进程已消亡(容器已退出,或 owner 标签的宿主 PID 不存在),
/// 并发运行中的其他测试进程容器绝不动。
async fn cleanup_stale_pg_containers() {
    let Ok(out) = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "name=relay-it-pg-",
            "--filter",
            "label=agent-console-it=1",
        ])
        .output()
    else {
        return;
    };
    let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(String::from)
        .collect();
    let mut stale: Vec<String> = Vec::new();
    for id in &ids {
        // 早于标签机制创建的遗留容器没有属主信息:一律保守保留。
        let Ok(inspect) = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.State.Status}}|{{index .Config.Labels \"agent-console-it-owner\"}}",
                id,
            ])
            .output()
        else {
            continue;
        };
        let info = String::from_utf8_lossy(&inspect.stdout).trim().to_string();
        let mut parts = info.splitn(2, '|');
        let status = parts.next().unwrap_or("");
        let owner = parts.next().unwrap_or("");
        if status.is_empty() {
            continue;
        }
        if status == "exited" {
            stale.push(id.clone());
            continue;
        }
        let Some(owner) = owner.trim().parse::<u32>().ok() else {
            continue;
        };
        // 属主进程已消亡 → 孤儿。PID 复用时误判为存活只会保守保留。
        match Command::new("ps").args(["-p", &owner.to_string()]).output() {
            Ok(alive) if !alive.status.success() => stale.push(id.clone()),
            _ => {}
        }
    }
    if !stale.is_empty() {
        let _ = Command::new("docker")
            .args(["rm", "-f"])
            .args(&stale)
            .output();
    }
}

impl Postgres {
    /// 启动一次性 postgres:17-alpine 容器(随机宿主端口);Drop 时强制清理。
    /// docker run 偶发瞬时失败(并发启动/端口竞争),最多重试 3 次。
    pub async fn start() -> Postgres {
        cleanup_stale_pg_containers().await;
        let mut last_err = String::new();
        for _ in 0..3 {
            let name = format!("relay-it-pg-{}", uuid::Uuid::new_v4().simple());
            let free = free_port();
            let out = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "-d",
                    "--name",
                    &name,
                    // 属主标签:并发测试进程的容器靠它区分,防御清理绝不动。
                    "--label",
                    "agent-console-it=1",
                    "--label",
                    &format!("agent-console-it-owner={}", std::process::id()),
                    "-e",
                    "POSTGRES_PASSWORD=test",
                    "-e",
                    "POSTGRES_USER=test",
                    "-p",
                    &format!("127.0.0.1:{free}:5432"),
                    "postgres:17-alpine",
                ])
                .output()
                .expect("docker run");
            if !out.status.success() {
                last_err = String::from_utf8_lossy(&out.stderr).to_string();
                continue;
            }
            let url = format!("postgres://test:test@127.0.0.1:{free}/postgres");
            // 等待就绪。
            for _ in 0..120 {
                if let Ok(pool) = sqlx::postgres::PgPool::connect(&url).await {
                    pool.close().await;
                    return Postgres { name, port: free };
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            let _ = Command::new("docker").args(["rm", "-f", &name]).status();
            last_err = "postgres container not ready in time".into();
        }
        panic!("docker run failed after retries: {last_err}");
    }

    /// 新建空数据库并返回其 URL(每个测试独立库,迁移从空库执行)。
    pub async fn fresh_db(&self) -> String {
        let db = format!("t_{}", uuid::Uuid::new_v4().simple());
        let base = format!("postgres://test:test@127.0.0.1:{}/postgres", self.port);
        let pool = sqlx::postgres::PgPool::connect(&base)
            .await
            .expect("connect postgres");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&pool)
            .await
            .expect("create database");
        pool.close().await;
        format!("postgres://test:test@127.0.0.1:{}/{db}", self.port)
    }

    pub async fn pool(&self, url: &str) -> sqlx::PgPool {
        sqlx::postgres::PgPool::connect(url)
            .await
            .expect("connect db")
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind :0")
        .local_addr()
        .unwrap()
        .port()
}

// ---------------------------------------------------------------------------
// fake dev-toolbox(§20 内部契约)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TicketRec {
    session: uuid::Uuid,
    owner: uuid::Uuid,
    expires_at: chrono::DateTime<chrono::Utc>,
    consumed: bool,
}

#[derive(Clone)]
struct SessionRec {
    valid: bool,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
pub struct FakeToolbox {
    addr: SocketAddr,
    tickets: Arc<Mutex<HashMap<String, TicketRec>>>,
    sessions: Arc<Mutex<HashMap<uuid::Uuid, SessionRec>>>,
    unavailable: Arc<AtomicBool>,
}

impl FakeToolbox {
    pub async fn start() -> FakeToolbox {
        let tickets: Arc<Mutex<HashMap<String, TicketRec>>> = Arc::new(Mutex::new(HashMap::new()));
        let sessions: Arc<Mutex<HashMap<uuid::Uuid, SessionRec>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let unavailable = Arc::new(AtomicBool::new(false));
        let state = (tickets.clone(), sessions.clone(), unavailable.clone());

        let app = axum::Router::new()
            .route(
                "/internal/agent-console/ws-tickets/consume",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap,
                          axum::Json(body): axum::Json<serde_json::Value>| {
                        let (tickets, sessions, unavailable) = state.clone();
                        async move {
                            if !check_internal_auth(&headers) {
                                return (axum::http::StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({"error": {"code": "AUTH_REQUIRED"}})));
                            }
                            if unavailable.load(Ordering::SeqCst) {
                                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({"error": {"code": "INTERNAL_ERROR"}})));
                            }
                            let ticket = body["ticket"].as_str().unwrap_or("").to_string();
                            let mut map = tickets.lock().unwrap();
                            let Some(rec) = map.get_mut(&ticket) else {
                                return ok_json(serde_json::json!({"valid": false, "reason": "NOT_FOUND"}));
                            };
                            if rec.consumed {
                                return ok_json(serde_json::json!({"valid": false, "reason": "CONSUMED"}));
                            }
                            if rec.expires_at <= chrono::Utc::now() {
                                return ok_json(serde_json::json!({"valid": false, "reason": "EXPIRED"}));
                            }
                            let session_valid = sessions
                                .lock()
                                .unwrap()
                                .get(&rec.session)
                                .map(|s| s.valid && s.expires_at > chrono::Utc::now())
                                .unwrap_or(false);
                            if !session_valid {
                                return ok_json(serde_json::json!({"valid": false, "reason": "SESSION_INVALID"}));
                            }
                            rec.consumed = true;
                            ok_json(serde_json::json!({
                                "valid": true,
                                "authSessionId": rec.session,
                                "ownerId": rec.owner,
                                "expiresAt": rec.expires_at.to_rfc3339(),
                            }))
                        }
                    },
                ),
            )
            .route(
                "/internal/agent-console/auth-sessions/introspect",
                {
                    let tickets = tickets.clone();
                    let sessions = sessions.clone();
                    let unavailable = unavailable.clone();
                    axum::routing::post(
                        move |headers: axum::http::HeaderMap,
                              axum::Json(body): axum::Json<serde_json::Value>| {
                            let sessions = sessions.clone();
                            let unavailable = unavailable.clone();
                            async move {
                                let _ = tickets;
                                if !check_internal_auth(&headers) {
                                    return (
                                        axum::http::StatusCode::UNAUTHORIZED,
                                        axum::Json(serde_json::json!({"error": {"code": "AUTH_REQUIRED"}})),
                                    );
                                }
                                if unavailable.load(Ordering::SeqCst) {
                                    return (
                                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                        axum::Json(serde_json::json!({"error": {"code": "INTERNAL_ERROR"}})),
                                    );
                                }
                                let id = body["authSessionId"]
                                    .as_str()
                                    .and_then(|s| uuid::Uuid::parse_str(s).ok());
                                match id.and_then(|id| sessions.lock().unwrap().get(&id).cloned()) {
                                    None => ok_json(
                                        serde_json::json!({"valid": false, "revokedReason": "NOT_FOUND"}),
                                    ),
                                    Some(s) if !s.valid => ok_json(
                                        serde_json::json!({"valid": false, "revokedReason": "REVOKED"}),
                                    ),
                                    Some(s) if s.expires_at <= chrono::Utc::now() => ok_json(
                                        serde_json::json!({"valid": false, "revokedReason": "EXPIRED"}),
                                    ),
                                    Some(s) => ok_json(serde_json::json!({
                                        "valid": true,
                                        "ownerId": OWNER,
                                        "expiresAt": s.expires_at.to_rfc3339(),
                                    })),
                                }
                            }
                        },
                    )
                },
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self {
            addr,
            tickets,
            sessions,
            unavailable,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn seed_session(&self, session: uuid::Uuid, ttl: chrono::Duration) {
        self.sessions.lock().unwrap().insert(
            session,
            SessionRec {
                valid: true,
                expires_at: chrono::Utc::now() + ttl,
            },
        );
    }

    pub fn revoke_session(&self, session: uuid::Uuid) {
        if let Some(s) = self.sessions.lock().unwrap().get_mut(&session) {
            s.valid = false;
        }
    }

    pub fn set_unavailable(&self, v: bool) {
        self.unavailable.store(v, Ordering::SeqCst);
    }

    pub fn issue_ticket(&self, ttl: chrono::Duration) -> String {
        let ticket = format!("tk-{}", uuid::Uuid::new_v4().simple());
        self.insert_ticket(ticket.clone(), ttl);
        ticket
    }

    /// 为指定 auth session 签发 ticket(多会话场景:撤销隔离测试用)。
    pub fn issue_ticket_for(&self, session: uuid::Uuid, ttl: chrono::Duration) -> String {
        let ticket = format!("tk-{}", uuid::Uuid::new_v4().simple());
        self.tickets.lock().unwrap().insert(
            ticket.clone(),
            TicketRec {
                session,
                owner: OWNER,
                expires_at: chrono::Utc::now() + ttl,
                consumed: false,
            },
        );
        ticket
    }

    /// 以指定明文注入 ticket(敏感日志检查用)。
    pub fn issue_ticket_with_value(&self, ticket: &str, ttl: chrono::Duration) {
        self.insert_ticket(ticket.to_string(), ttl);
    }

    fn insert_ticket(&self, ticket: String, ttl: chrono::Duration) {
        self.tickets.lock().unwrap().insert(
            ticket,
            TicketRec {
                session: AUTH_SESSION,
                owner: OWNER,
                expires_at: chrono::Utc::now() + ttl,
                consumed: false,
            },
        );
    }
}

fn check_internal_auth(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t == INTERNAL_TOKEN)
        .unwrap_or(false)
}

fn ok_json(v: serde_json::Value) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    (axum::http::StatusCode::OK, axum::Json(v))
}

// ---------------------------------------------------------------------------
// Relay 进程
// ---------------------------------------------------------------------------

pub struct Relay {
    pub child: Child,
    pub base: String,
    pub addr: SocketAddr,
}

impl Relay {
    pub fn spawn(db_url: &str, toolbox: &FakeToolbox, env_extra: &[(&str, &str)]) -> Relay {
        Self::spawn_with_stderr(db_url, toolbox, env_extra, None)
    }

    /// stderr_path:Some 时日志写入文件(敏感日志检查用),否则丢弃。
    pub fn spawn_with_stderr(
        db_url: &str,
        toolbox: &FakeToolbox,
        env_extra: &[(&str, &str)],
        stderr_path: Option<&std::path::Path>,
    ) -> Relay {
        let port = free_port();
        let stderr = match stderr_path {
            Some(p) => Stdio::from(std::fs::File::create(p).expect("create stderr file")),
            None => {
                if std::env::var("RELAY_IT_DEBUG").is_ok() {
                    Stdio::inherit()
                } else {
                    Stdio::null()
                }
            }
        };
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_relay"));
        cmd.env("RELAY_DATABASE_URL", db_url)
            .env("RELAY_BIND_ADDR", format!("127.0.0.1:{port}"))
            .env("DEVTOOLBOX_INTERNAL_BASE_URL", toolbox.base_url())
            .env("RELAY_INTERNAL_TOKEN", INTERNAL_TOKEN)
            .env(
                "RUST_LOG",
                std::env::var("RELAY_IT_DEBUG")
                    .map(|_| "debug".to_string())
                    .unwrap_or_else(|_| "info".into()),
            )
            .stdout(Stdio::null())
            .stderr(stderr);
        for (k, v) in env_extra {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn relay");
        let relay = Relay {
            child,
            base: format!("http://127.0.0.1:{port}"),
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
        };
        relay
    }

    pub async fn wait_healthy(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        let client = reqwest::Client::new();
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("relay did not become healthy in time");
            }
            if let Ok(resp) = client.get(format!("{}/health", self.base)).send().await {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.kill();
    }
}

// ---------------------------------------------------------------------------
// HTTP 辅助
// ---------------------------------------------------------------------------

pub fn identity_headers() -> Vec<(&'static str, String)> {
    vec![
        ("X-Agent-Console-Session-Id", AUTH_SESSION.to_string()),
        ("X-Agent-Console-Owner-Id", OWNER.to_string()),
        (
            "X-Agent-Console-Session-Expires",
            (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        ),
    ]
}

pub async fn http_get(relay: &Relay, path: &str, headers: &[(&str, String)]) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut req = client.get(format!("{}{path}", relay.base));
    for (k, v) in headers {
        req = req.header(*k, v);
    }
    req.send().await.expect("http get")
}

// ---------------------------------------------------------------------------
// 协议编解码辅助
// ---------------------------------------------------------------------------

use agent_console_protocol::codec::{
    decode_envelope, encode_envelope, new_message_id, PROTOCOL_VERSION,
};
use agent_console_protocol::v1::{domain_event, envelope, DomainEvent, Envelope};

pub fn base_env(payload: envelope::Payload) -> Envelope {
    Envelope {
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

pub async fn send_env(ws: &mut Ws, env: &Envelope) {
    let bytes = encode_envelope(env).expect("encode");
    ws.send(Message::Binary(bytes)).await.expect("send");
}

pub async fn recv_raw(ws: &mut Ws, timeout: Duration) -> Message {
    tokio::time::timeout(timeout, ws.next())
        .await
        .expect("recv timeout")
        .expect("ws closed")
        .expect("ws error")
}

pub async fn recv_env(ws: &mut Ws, timeout: Duration) -> Envelope {
    let msg = recv_raw(ws, timeout).await;
    match msg {
        Message::Binary(bytes) => decode_envelope(&bytes).expect("decode"),
        other => panic!("expected binary frame, got {other:?}"),
    }
}

/// 读取一帧;跳过 Heartbeat(Relay 周期心跳)。
pub async fn recv_skip_heartbeat(ws: &mut Ws, timeout: Duration) -> Envelope {
    loop {
        let e = recv_env(ws, timeout).await;
        if !matches!(e.payload, Some(envelope::Payload::Heartbeat(_))) {
            return e;
        }
    }
}

/// 读取一帧;跳过纯 presence 的 EventBatch(设备上下线广播)。
pub async fn recv_skip_presence(ws: &mut Ws, timeout: Duration) -> Envelope {
    loop {
        let e = recv_env(ws, timeout).await;
        let all_presence = matches!(
            e.payload.as_ref(),
            Some(envelope::Payload::EventBatch(b))
            if !b.events.is_empty()
                && b.events.iter().all(|ev| {
                    matches!(ev.event.as_ref(), Some(agent_console_protocol::v1::domain_event::Event::DevicePresenceChanged(_)))
                })
        );
        if !all_presence {
            return e;
        }
    }
}

/// 非阻塞探测一帧(超时返回 None,不 panic)。
pub async fn try_recv_env(ws: &mut Ws, timeout: Duration) -> Option<Envelope> {
    let msg = tokio::time::timeout(timeout, ws.next())
        .await
        .ok()
        .flatten()?
        .ok()?;
    match msg {
        tokio_tungstenite::tungstenite::Message::Binary(bytes) => decode_envelope(&bytes).ok(),
        _ => None,
    }
}

/// 期待二进制帧但允许中间 Ping 帧。
pub async fn recv_close(ws: &mut Ws, timeout: Duration) -> (u16, String) {
    loop {
        let msg = recv_raw(ws, timeout).await;
        if let Message::Close(frame) = msg {
            let f = frame.expect("close frame");
            return (u16::from(f.code), f.reason.to_string());
        }
    }
}

pub fn client_hello(kind: i32, device_id: &str, version: u32) -> Envelope {
    let mut env = base_env(envelope::Payload::ClientHello(
        agent_console_protocol::v1::ClientHello {
            protocol_version: version,
            client_kind: kind,
            device_id: device_id.to_string(),
            auth_subject: String::new(),
            capabilities: vec![],
        },
    ));
    env.device_id = device_id.to_string();
    env
}

pub async fn handshake_bridge(ws: &mut Ws, device_id: &str) -> Envelope {
    send_env(ws, &client_hello(2, device_id, PROTOCOL_VERSION)).await;
    recv_env(ws, Duration::from_secs(5)).await
}

pub async fn handshake_browser(ws: &mut Ws) -> Envelope {
    send_env(ws, &client_hello(1, "", PROTOCOL_VERSION)).await;
    recv_env(ws, Duration::from_secs(5)).await
}

// ---------------------------------------------------------------------------
// fake Bridge / fake Browser
// ---------------------------------------------------------------------------

pub struct FakeBridge {
    pub ws: Ws,
    pub device_id: String,
}

/// 生成测试凭据及其 SHA-256 hex 摘要。
pub fn test_credential() -> (String, String) {
    let cred = format!("cred-{}", uuid::Uuid::new_v4().simple());
    let digest = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(cred.as_bytes()))
    };
    (cred, digest)
}

/// 直接向库内插入设备行(绕过 pairing;pairing 流程另有专项测试)。
pub async fn insert_device(db: &sqlx::PgPool, device_id: uuid::Uuid, credential_digest: &str) {
    sqlx::query(
        "INSERT INTO devices (id, owner_id, display_name, platform, arch, bridge_version, credential_digest) \
         VALUES ($1, $2, 'Test Mac', 'darwin', 'arm64', 'test-version', $3)",
    )
    .bind(device_id)
    .bind(OWNER)
    .bind(credential_digest)
    .execute(db)
    .await
    .expect("insert device");
}

impl FakeBridge {
    /// 连接并完成 Hello 握手;认证失败返回 HTTP 状态码。
    pub async fn connect(
        relay: &Relay,
        device_id: &str,
        credential: &str,
    ) -> Result<FakeBridge, u16> {
        let mut request = format!("ws://{}/agent-console/bridge/ws", relay.addr)
            .into_client_request()
            .expect("request");
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {credential}")
                .parse()
                .expect("header value"),
        );
        match connect_async(request).await {
            Ok((ws, _)) => {
                let mut bridge = FakeBridge {
                    ws,
                    device_id: device_id.to_string(),
                };
                let hello = handshake_bridge(&mut bridge.ws, device_id).await;
                assert!(
                    matches!(hello.payload, Some(envelope::Payload::ServerHello(_))),
                    "expected ServerHello"
                );
                Ok(bridge)
            }
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
            Err(e) => panic!("bridge connect failed: {e}"),
        }
    }

    pub async fn send(&mut self, env: &Envelope) {
        send_env(&mut self.ws, env).await;
    }

    pub async fn recv(&mut self, timeout: Duration) -> Envelope {
        recv_env(&mut self.ws, timeout).await
    }

    /// 上游订阅确认:Subscribed(epoch, base)。
    pub async fn send_subscribed(&mut self, upstream_stream_id: &str, epoch: u64, base: u64) {
        let mut env = base_env(envelope::Payload::Subscribed(
            agent_console_protocol::v1::Subscribed {
                stream_id: upstream_stream_id.to_string(),
                stream_epoch: epoch,
                base_sequence: base,
            },
        ));
        env.stream_id = upstream_stream_id.to_string();
        self.send(&env).await;
    }

    pub fn list_snapshot_env(device_id: &str, native: &str, title: &str) -> Envelope {
        let summary = session_summary(device_id, native, title);
        let mut env = base_env(envelope::Payload::SessionSummaryBatch(
            agent_console_protocol::v1::SessionSummaryBatch {
                summaries: vec![summary],
                snapshot: true,
            },
        ));
        env.stream_id = format!("u-{device_id}-list");
        env.device_id = device_id.to_string();
        env
    }

    pub fn list_delta_env(device_id: &str, native: &str, title: &str, seq: u64) -> Envelope {
        let summary = session_summary(device_id, native, title);
        let mut env = base_env(envelope::Payload::SessionSummaryBatch(
            agent_console_protocol::v1::SessionSummaryBatch {
                summaries: vec![summary],
                snapshot: false,
            },
        ));
        env.stream_id = format!("u-{device_id}-list");
        env.device_id = device_id.to_string();
        env.sequence = seq;
        env
    }

    pub fn runtime_snapshot_env(
        device_id: &str,
        _native: &str,
        upstream_stream_id: &str,
        seq: u64,
    ) -> Envelope {
        let mut env = base_env(envelope::Payload::RuntimeSnapshot(
            agent_console_protocol::v1::RuntimeSnapshot {
                runtime_revision: 42,
                ..Default::default()
            },
        ));
        env.stream_id = upstream_stream_id.to_string();
        env.device_id = device_id.to_string();
        env.sequence = seq;
        env
    }

    pub fn output_append_env(
        device_id: &str,
        upstream_stream_id: &str,
        item: &str,
        offset: u64,
        len: usize,
        seq: u64,
    ) -> Envelope {
        let event = domain_event::Event::OutputAppend(agent_console_protocol::v1::OutputAppend {
            item_id: Some(agent_console_protocol::v1::ItemId {
                id: item.to_string(),
                synthetic: false,
            }),
            expected_offset: offset,
            bytes: vec![b'x'; len],
            channel: 3,
        });
        let mut env = base_env(envelope::Payload::EventBatch(
            agent_console_protocol::v1::EventBatch {
                stream_id: upstream_stream_id.to_string(),
                events: vec![DomainEvent {
                    emitted_at: None,
                    event: Some(event),
                }],
            },
        ));
        env.stream_id = upstream_stream_id.to_string();
        env.device_id = device_id.to_string();
        env.sequence = seq;
        env
    }
}

pub fn session_summary(
    device_id: &str,
    native: &str,
    title: &str,
) -> agent_console_protocol::v1::SessionSummary {
    agent_console_protocol::v1::SessionSummary {
        session_key: Some(agent_console_protocol::v1::SessionKey {
            device_id: device_id.to_string(),
            agent_kind: 1,
            native_session_id: native.to_string(),
            relay_session_uuid: String::new(),
        }),
        title: title.to_string(),
        agent_kind: 1,
        project_display_name: "proj".to_string(),
        current_branch: "main".to_string(),
        updated_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        device_connection: 2,
        ..Default::default()
    }
}

pub struct FakeBrowser {
    pub ws: Ws,
}

/// Sec-WebSocket-Protocol 携带 ticket 的浏览器连接(§20.2)。
pub async fn connect_browser(relay: &Relay, ticket: &str) -> Result<FakeBrowser, u16> {
    let subprotocol = format!(
        "{}, agent-console.ticket-{}",
        "agent-console.v1",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ticket.as_bytes())
    );
    let mut request = format!("ws://{}/agent-console/ws", relay.addr)
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        subprotocol.parse().expect("header"),
    );
    match connect_async(request).await {
        Ok((ws, _)) => {
            let mut b = FakeBrowser { ws };
            let hello = handshake_browser(&mut b.ws).await;
            assert!(matches!(
                hello.payload,
                Some(envelope::Payload::ServerHello(_))
            ));
            Ok(b)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            // 读出响应体供断言使用由调用方决定;这里仅返回状态码。
            Err(resp.status().as_u16())
        }
        Err(e) => panic!("browser connect failed: {e}"),
    }
}

impl FakeBrowser {
    pub async fn send(&mut self, env: &Envelope) {
        send_env(&mut self.ws, env).await;
    }

    pub async fn recv(&mut self, timeout: Duration) -> Envelope {
        recv_env(&mut self.ws, timeout).await
    }
}

pub fn subscribe_list() -> Envelope {
    base_env(envelope::Payload::Subscribe(
        agent_console_protocol::v1::Subscribe {
            target: Some(agent_console_protocol::v1::subscribe::Target::List(
                agent_console_protocol::v1::SessionList {},
            )),
        },
    ))
}

pub fn subscribe_session(device_id: &str, native: &str) -> Envelope {
    base_env(envelope::Payload::Subscribe(
        agent_console_protocol::v1::Subscribe {
            target: Some(agent_console_protocol::v1::subscribe::Target::Session(
                agent_console_protocol::v1::SessionKey {
                    device_id: device_id.to_string(),
                    agent_kind: 1,
                    native_session_id: native.to_string(),
                    relay_session_uuid: String::new(),
                },
            )),
        },
    ))
}

pub const BRIDGE: i32 = 2;
pub const BROWSER: i32 = 1;

/// 共享容器:整个测试二进制共用一个 postgres(每个测试独立数据库)。
static PG: tokio::sync::OnceCell<Postgres> = tokio::sync::OnceCell::const_new();

/// 在 async 上下文中初始化共享容器(首次调用会阻塞数秒)。
pub async fn postgres() -> &'static Postgres {
    PG.get_or_init(|| Postgres::start()).await
}

// ---------------------------------------------------------------------------
// 标准测试环境
// ---------------------------------------------------------------------------

pub struct Env {
    pub db_url: String,
    pub pool: sqlx::PgPool,
    pub toolbox: FakeToolbox,
    pub relay: Relay,
}

pub async fn setup(env_extra: &[(&str, &str)]) -> Env {
    let pg = postgres().await;
    let db_url = pg.fresh_db().await;
    let toolbox = FakeToolbox::start().await;
    toolbox.seed_session(AUTH_SESSION, chrono::Duration::hours(1));
    let relay = Relay::spawn(&db_url, &toolbox, env_extra);
    relay.wait_healthy(Duration::from_secs(30)).await;
    let pool = pg.pool(&db_url).await;
    Env {
        db_url,
        pool,
        toolbox,
        relay,
    }
}

/// 带日志文件的标准环境(敏感日志检查)。
pub async fn setup_with_log_file(env_extra: &[(&str, &str)], log_path: &std::path::Path) -> Env {
    let pg = postgres().await;
    let db_url = pg.fresh_db().await;
    let toolbox = FakeToolbox::start().await;
    toolbox.seed_session(AUTH_SESSION, chrono::Duration::hours(1));
    let relay = Relay::spawn_with_stderr(&db_url, &toolbox, env_extra, Some(log_path));
    relay.wait_healthy(Duration::from_secs(30)).await;
    let pool = pg.pool(&db_url).await;
    Env {
        db_url,
        pool,
        toolbox,
        relay,
    }
}

/// 已插入设备并让 fake Bridge 完成握手的就绪环境。
pub async fn setup_with_bridge() -> (Env, FakeBridge, String) {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge connect");
    // 等设备上线注册生效。
    tokio::time::sleep(Duration::from_millis(200)).await;
    (env, bridge, device_id.to_string())
}

/// 便捷断言:等价于稳定的 retry 循环。
pub async fn until<F, T>(mut f: F) -> T
where
    F: FnMut() -> Option<T>,
{
    for _ in 0..100 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not met in time");
}

/// payload 变体名(测试输出用)。
pub fn payload_kind_name(p: &agent_console_protocol::v1::envelope::Payload) -> &'static str {
    use agent_console_protocol::codec::payload_kind;
    payload_kind(p)
}
