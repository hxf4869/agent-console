//! 无 UI e2e(§29.4)专用支撑:临时 PostgreSQL(postgres:16-alpine,独立库
//! `agent_console_e2e` + 独立用户)、进程内 Relay(`relay::serve`)、
//! tracing 全量捕获(敏感字段断言用)。
//!
//! Browser/fake owner 协议层复用 `support`(同目录)与
//! `bridge::adapter::codex::fake_owner`;BridgeRuntime 进程内装配直接使用
//! `bridge::runtime` 公共 API。

#![allow(dead_code)]

use std::{
    net::SocketAddr,
    process::{Command, Stdio},
    sync::Mutex,
    time::Duration,
};

use base64::Engine;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::support::FakeToolbox;

// ---------------------------------------------------------------------------
// 临时 PostgreSQL(postgres:16-alpine;独立库 agent_console_e2e / 独立用户)
// ---------------------------------------------------------------------------

pub struct E2ePostgres {
    pub name: String,
    pub port: u16,
}

impl E2ePostgres {
    /// 启动一次性 postgres:16-alpine 容器(随机宿主端口);Drop 时强制清理。
    pub async fn start() -> E2ePostgres {
        // 防御性清理此前测试进程异常退出遗留的容器。
        let _ = Command::new("docker")
            .args(["ps", "-aq", "--filter", "name=e2e-no-ui-pg-"])
            .output()
            .map(|o| {
                let ids = String::from_utf8_lossy(&o.stdout).to_string();
                if !ids.trim().is_empty() {
                    let _ = Command::new("docker")
                        .args(["rm", "-f"])
                        .args(ids.split_whitespace())
                        .output();
                }
            });
        let name = format!("e2e-no-ui-pg-{}", uuid::Uuid::new_v4().simple());
        let free = crate::support::free_port();
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "-e",
                "POSTGRES_PASSWORD=test",
                "-e",
                "POSTGRES_USER=test",
                "-p",
                &format!("127.0.0.1:{free}:5432"),
                "postgres:16-alpine",
            ])
            .output()
            .expect("docker run (postgres:16-alpine)");
        assert!(
            out.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let admin = format!("postgres://test:test@127.0.0.1:{free}/postgres");
        for _ in 0..240 {
            if let Ok(pool) = sqlx::postgres::PgPool::connect(&admin).await {
                // 独立数据库 + 独立用户(§18:不与既有业务共用)。
                sqlx::query("CREATE ROLE agent_console_e2e LOGIN PASSWORD 'e2e-pw'")
                    .execute(&pool)
                    .await
                    .expect("create role");
                sqlx::query("CREATE DATABASE agent_console_e2e OWNER agent_console_e2e")
                    .execute(&pool)
                    .await
                    .expect("create database");
                pool.close().await;
                return E2ePostgres { name, port: free };
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // 就绪失败:先清理容器再失败,避免泄漏。
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        panic!("postgres:16-alpine container not ready in time");
    }

    /// 独立库 `agent_console_e2e` 的连接串(独立用户)。
    pub fn db_url(&self) -> String {
        format!(
            "postgres://agent_console_e2e:e2e-pw@127.0.0.1:{}/agent_console_e2e",
            self.port
        )
    }
}

impl Drop for E2ePostgres {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

// ---------------------------------------------------------------------------
// 进程内 Relay
// ---------------------------------------------------------------------------

pub struct E2eRelay {
    pub server: relay::RelayServer,
    pub base: String,
    pub addr: SocketAddr,
}

/// 以 e2e 调优配置启动进程内 Relay(migrations 在 serve 内从空库执行)。
/// introspect_interval 压缩到 500ms(§20.5 撤销关闭窗口的 e2e 等价观测)。
pub async fn start_relay(db_url: &str, toolbox: &FakeToolbox) -> E2eRelay {
    let config = relay::state::Config {
        database_url: db_url.to_string(),
        bind_addr: format!("127.0.0.1:{}", crate::support::free_port())
            .parse()
            .unwrap(),
        internal_token: Some(crate::support::INTERNAL_TOKEN.to_string()),
        devtoolbox_base_url: Some(toolbox.base_url()),
        trusted_proxy_cidrs: vec!["127.0.0.0/8".parse().unwrap()],
        introspect_interval: Duration::from_millis(500),
        auth_grace: Duration::from_secs(120),
        heartbeat_interval: Duration::from_secs(2),
        offline_after: Duration::from_secs(30),
    };
    let server = relay::serve(config).await.expect("relay serve");
    let base = server.base_url.clone();
    let addr = server.addr;
    // 等 /health 就绪(迁移完成后才返回,这里只是确认可响应)。
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("relay did not become healthy in time");
        }
        if let Ok(resp) = client.get(format!("{base}/health")).send().await {
            if resp.status().is_success() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    E2eRelay { server, base, addr }
}

// ---------------------------------------------------------------------------
// tracing 全量捕获(§25.3 敏感字段断言;进程内 relay + bridge 全部入捕获)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedBufferWriter {
    buffer: std::sync::Arc<Mutex<Vec<u8>>>,
}

impl std::io::Write for SharedBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for SharedBufferWriter {
    type Writer = SharedBufferWriter;
    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

/// 全进程唯一 tracing 订阅:debug 级别全部写入内存缓冲,供事后断言
/// ticket/凭据/prompt 正文/文件正文(§25.3)从未进入日志。
pub struct LogCapture {
    buffer: std::sync::Arc<Mutex<Vec<u8>>>,
}

impl LogCapture {
    pub fn init() -> LogCapture {
        static INIT: std::sync::Once = std::sync::Once::new();
        let buffer = std::sync::Arc::new(Mutex::new(Vec::new()));
        INIT.call_once(|| {
            // relay/bridge 内部 debug 全收;HTTP/DB 中间件噪声压到 warn。
            let filter = tracing_subscriber::EnvFilter::new("warn,relay=debug,bridge=debug");
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(SharedBufferWriter {
                    buffer: buffer.clone(),
                })
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        LogCapture { buffer }
    }

    /// 断言敏感串全部缺席(§25.3:结构化日志白名单,不允许正文/凭据)。
    pub fn assert_free_of(&self, secrets: &[&str]) {
        let captured = String::from_utf8_lossy(&self.buffer.lock().unwrap()).into_owned();
        for secret in secrets {
            assert!(
                !captured.contains(secret),
                "日志捕获中出现敏感串(前 12 字符: {}...)",
                &secret[..secret.len().min(12)]
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Browser WS(ticket subprotocol;§20.2)
// ---------------------------------------------------------------------------

/// 以 `agent-console.v1` + `agent-console.ticket-<b64url>` subprotocol 连接
/// 浏览器 WS 并完成 ClientHello/ServerHello 握手;认证失败返回 HTTP 状态码。
pub async fn connect_browser_ws(base: &str, ticket: &str) -> Result<crate::support::Ws, u16> {
    let subprotocol = format!(
        "{}, agent-console.ticket-{}",
        "agent-console.v1",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ticket.as_bytes())
    );
    // tungstenite 只接受 ws/wss scheme;base 由 RelayServer 以 http:// 给出。
    let ws_base = base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    let mut request = format!("{ws_base}/agent-console/ws")
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        subprotocol.parse().expect("header"),
    );
    match tokio_tungstenite::connect_async(request).await {
        Ok((ws, _)) => {
            let mut ws = ws;
            let hello = crate::support::handshake_browser(&mut ws).await;
            assert!(matches!(
                hello.payload,
                Some(agent_console_protocol::v1::envelope::Payload::ServerHello(
                    _
                ))
            ));
            Ok(ws)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("browser connect failed: {e}"),
    }
}

/// 浏览器 HTTP 身份头(可信代理重建的三元组;§20.4)。
pub fn identity_headers() -> Vec<(&'static str, String)> {
    crate::support::identity_headers()
}
