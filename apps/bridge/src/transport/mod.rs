//! Relay WebSocket 传输客户端(权威规格 §17/§26.2/§26.4)。
//!
//! - 连接 `<基地址>/agent-console/bridge/ws`(由
//!   [`RelayClientOptions::ws_url`](crate::config::RelayUrls::bridge_ws_url)
//!   派生),认证头 `Authorization: Bearer <credential>`;
//!   凭据由 [`DeviceCredential`] 注入,**不进日志**(§25.3)。
//! - 帧为 `agent_console_protocol` Envelope 二进制编码(单帧 ≤ 1 MiB,
//!   由 codec 统一检查)。
//! - 发送队列有界:满时 [`RelayHandle::send`] 立即返回
//!   [`TransportError::QueueFull`],不阻塞调用方。
//! - 心跳:每 [`RelayClientOptions::heartbeat_interval`](默认 15s)发送
//!   Heartbeat;约 45s(`heartbeat_dead_after`)无任何入站帧判离线断开。
//! - 重连:指数退避 1s–30s + jitter;`ws://`(开发)与 `wss://`(生产)都支持。
//! - 每次连接成功后调用 `on_connected` 回调,由上层触发 capability/summary/
//!   snapshot 重同步(§26.2)。
//! - 入站队列有界;满时视为慢 consumer,断开重连触发重同步(§26.4)。

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use agent_console_protocol::agent_console::v1::Heartbeat;
use agent_console_protocol::agent_console::v1::{envelope, Envelope};
use agent_console_protocol::codec::{decode_envelope, encode_envelope, new_message_id};

pub use error::TransportError;
pub use options::RelayClientOptions;

mod error;
mod options;

/// 连接状态(watch 通道可供上层/测试观察)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// 设备凭据提供者。实现不得记录 token。
pub trait DeviceCredential: Send + Sync + std::fmt::Debug {
    fn bearer_token(&self) -> String;
}

/// 静态凭据(测试与已缓存凭据场景)。
#[derive(Clone)]
pub struct StaticCredential {
    /// 字段名刻意不含敏感词;Debug 手工实现避免值泄漏。
    inner: Arc<String>,
}

impl StaticCredential {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(token.into()),
        }
    }
}

impl std::fmt::Debug for StaticCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredential").finish_non_exhaustive()
    }
}

impl DeviceCredential for StaticCredential {
    fn bearer_token(&self) -> String {
        (*self.inner).clone()
    }
}

/// 连接成功后的重同步触发回调(§26.2:恢复后立刻同步 capability、summary
/// 和活跃 RuntimeSnapshot)。必须是廉价非阻塞函数。
pub type OnConnected = Arc<dyn Fn() + Send + Sync>;

// ---------------------------------------------------------------------------
// 入口
// ---------------------------------------------------------------------------

/// 启动 Relay 客户端后台任务,返回 (句柄, 入站接收端)。
///
/// 后台任务维护"连接 → 会话 → 断线退避重连"循环,直到
/// [`RelayHandle::shutdown`] 被调用或句柄全部丢弃。
pub fn start(
    options: RelayClientOptions,
    credential: Arc<dyn DeviceCredential>,
    on_connected: Option<OnConnected>,
) -> (RelayHandle, mpsc::Receiver<Envelope>) {
    let (outbound_tx, outbound_rx) = mpsc::channel(options.outbound_capacity);
    let (inbound_tx, inbound_rx) = mpsc::channel(options.inbound_capacity);
    let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
    let (stop_tx, stop_rx) = watch::channel(false);

    let handle = RelayHandle {
        outbound_tx,
        state_rx,
        stop_tx,
    };
    tokio::spawn(run_loop(RunContext {
        options,
        credential,
        on_connected,
        outbound_rx,
        inbound_tx,
        state_tx,
        stop_rx,
    }));
    (handle, inbound_rx)
}

/// 句柄:发送 Envelope、观察状态、请求停止。克隆廉价。
#[derive(Clone)]
pub struct RelayHandle {
    outbound_tx: mpsc::Sender<Vec<u8>>,
    state_rx: watch::Receiver<ConnectionState>,
    stop_tx: watch::Sender<bool>,
}

impl std::fmt::Debug for RelayHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayHandle").finish_non_exhaustive()
    }
}

impl RelayHandle {
    /// 编码并入队;队列满立即返回 [`TransportError::QueueFull`],不阻塞。
    pub fn send(&self, envelope: &Envelope) -> Result<(), TransportError> {
        // 单帧 ≤1 MiB 在编码入口统一拒绝(§17.6)。
        let frame = encode_envelope(envelope)?;
        match self.outbound_tx.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(TransportError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::Stopped),
        }
    }

    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// 等待状态变化(测试与上层监控用)。
    pub async fn state_changed(&mut self) {
        let _ = self.state_rx.changed().await;
    }

    /// 请求停止:断开当前连接并退出重连循环(幂等)。
    pub fn shutdown(&self) {
        let _ = self.stop_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// 内部运行循环
// ---------------------------------------------------------------------------

struct RunContext {
    options: RelayClientOptions,
    credential: Arc<dyn DeviceCredential>,
    on_connected: Option<OnConnected>,
    outbound_rx: mpsc::Receiver<Vec<u8>>,
    inbound_tx: mpsc::Sender<Envelope>,
    state_tx: watch::Sender<ConnectionState>,
    stop_rx: watch::Receiver<bool>,
}

enum SessionEnd {
    /// 收到停止信号或发送端全部关闭:退出整个循环。
    Shutdown,
    /// 连接丢失:按退避重连。
    Lost,
}

async fn run_loop(mut ctx: RunContext) {
    let mut attempt: u32 = 0;
    loop {
        if *ctx.stop_rx.borrow() {
            break;
        }
        let _ = ctx.state_tx.send(ConnectionState::Connecting);

        match connect_once(&ctx.options, ctx.credential.as_ref()).await {
            Ok(ws) => {
                attempt = 0;
                let _ = ctx.state_tx.send(ConnectionState::Connected);
                // 重同步点:上层在此重建订阅/重拉 capability 与 snapshot。
                if let Some(cb) = &ctx.on_connected {
                    cb();
                }
                match run_session(&mut ctx, ws).await {
                    SessionEnd::Shutdown => break,
                    SessionEnd::Lost => {}
                }
                let _ = ctx.state_tx.send(ConnectionState::Disconnected);
            }
            Err(err) => {
                // 只记录稳定错误文本,不带 URL 细节/凭据(§25.3)。
                tracing::warn!(attempt, error = %err, "relay connect failed");
                let _ = ctx.state_tx.send(ConnectionState::Disconnected);
            }
        }

        if *ctx.stop_rx.borrow() {
            break;
        }
        let delay = backoff_delay(attempt, &ctx.options);
        attempt = attempt.saturating_add(1);
        let stop = wait_stop(&mut ctx.stop_rx);
        tokio::pin!(stop);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = &mut stop => break,
        }
    }
    let _ = ctx.state_tx.send(ConnectionState::Disconnected);
}

/// 未来的 stop 等待(watch::changed 循环,取消安全)。
async fn wait_stop(stop_rx: &mut watch::Receiver<bool>) {
    loop {
        if *stop_rx.borrow() {
            return;
        }
        if stop_rx.changed().await.is_err() {
            return;
        }
    }
}

/// 指数退避:initial * 2^attempt 截断到 max,再叠加 [0, jitter) 随机量。
fn backoff_delay(attempt: u32, options: &RelayClientOptions) -> Duration {
    let exp = attempt.min(16);
    let base = options
        .reconnect_initial_backoff
        .saturating_mul(1u32 << exp)
        .min(options.reconnect_max_backoff);
    if options.reconnect_jitter.is_zero() {
        return base;
    }
    let jitter_ms = options.reconnect_jitter.as_millis() as u64;
    let extra = rand::Rng::gen_range(&mut rand::thread_rng(), 0..jitter_ms.max(1));
    base + Duration::from_millis(extra)
}

async fn connect_once(
    options: &RelayClientOptions,
    credential: &dyn DeviceCredential,
) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, TransportError> {
    // ws_url 由 RelayUrls 校验派生,此处只做握手;解析失败按连接错误处理。
    let url: url::Url = options
        .ws_url
        .parse()
        .map_err(|e| TransportError::Connect(format!("invalid ws url: {e}")))?;
    // IntoClientRequest 生成完整的 WS 握手头(Host/Connection/Upgrade/
    // Sec-WebSocket-Version/Sec-WebSocket-Key);这里只追加认证头。
    // Bearer 凭据只进请求头,绝不记录(§19/§25.3)。
    let mut request: Request = url
        .as_str()
        .into_client_request()
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&format!("Bearer {}", credential.bearer_token()))
            .map_err(|e| TransportError::Connect(format!("invalid auth header: {e}")))?,
    );
    let (ws, _response) = connect_async(request)
        .await
        .map_err(|e| TransportError::Connect(e.to_string()))?;
    Ok(ws)
}

async fn run_session(
    ctx: &mut RunContext,
    ws: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
) -> SessionEnd {
    let (mut sink, mut stream) = ws.split();

    // 握手(§17.2):升级后立即发送 ClientHello(BRIDGE);Relay 校验协议版本
    // 与 device_id 一致后回 ServerHello(由入站队列交给上层,无动作)。
    let hello = client_hello_envelope(&ctx.options.device_id);
    match encode_envelope(&hello) {
        Ok(frame) => {
            if sink.send(Message::Binary(frame)).await.is_err() {
                return SessionEnd::Lost;
            }
        }
        Err(err) => {
            tracing::error!(error = %err, "client hello encode failed");
            return SessionEnd::Lost;
        }
    }

    // 心跳:interval 首跳推迟一个周期,避免连接即发。
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + ctx.options.heartbeat_interval,
        ctx.options.heartbeat_interval,
    );
    // 断线判定:上次入站帧距今超过 dead_after(§26.2:约 45s 无有效心跳)。
    let mut last_inbound = tokio::time::Instant::now();
    let dead_deadline = tokio::time::sleep_until(last_inbound + ctx.options.heartbeat_dead_after);
    tokio::pin!(dead_deadline);

    loop {
        tokio::select! {
            _ = wait_stop(&mut ctx.stop_rx) => return SessionEnd::Shutdown,

            outbound = ctx.outbound_rx.recv() => match outbound {
                Some(frame) => {
                    if sink.send(Message::Binary(frame)).await.is_err() {
                        return SessionEnd::Lost;
                    }
                }
                // 句柄全部丢弃:视为停止。
                None => return SessionEnd::Shutdown,
            },

            _ = heartbeat.tick() => {
                let envelope = heartbeat_envelope(&ctx.options.device_id);
                match encode_envelope(&envelope) {
                    Ok(frame) => {
                        if sink.send(Message::Binary(frame)).await.is_err() {
                            return SessionEnd::Lost;
                        }
                    }
                    Err(err) => {
                        // 心跳帧不可能超限;发生即协议 bug,记错误码后跳过。
                        tracing::error!(error = %err, "heartbeat encode failed");
                    }
                }
            }

            _ = &mut dead_deadline => {
                // 约 45s 无入站帧:判离线,主动断开走重连(§26.2)。
                tracing::warn!("relay heartbeat timeout, reconnecting");
                return SessionEnd::Lost;
            }

            incoming = stream.next() => match incoming {
                Some(Ok(Message::Binary(bytes))) => {
                    last_inbound = tokio::time::Instant::now();
                    dead_deadline
                        .as_mut()
                        .reset(last_inbound + ctx.options.heartbeat_dead_after);
                    match decode_envelope(&bytes) {
                        Ok(envelope) => {
                            // 入站有界:满即慢 consumer,断开由重连触发重同步(§26.4)。
                            if ctx.inbound_tx.try_send(envelope).is_err() {
                                tracing::warn!("inbound queue full, dropping connection for resync");
                                return SessionEnd::Lost;
                            }
                        }
                        Err(err) => {
                            // 超限或解码失败属于协议违约:断开,不给截断数据放行。
                            tracing::warn!(error = %err, "invalid inbound frame, dropping connection");
                            return SessionEnd::Lost;
                        }
                    }
                }
                Some(Ok(Message::Close(_))) => return SessionEnd::Lost,
                Some(Ok(_)) => {} // Text/Ping/Pong:tungstenite 已自动处理 ping/pong。
                Some(Err(err)) => {
                    tracing::warn!(error = %err, "relay read failed");
                    return SessionEnd::Lost;
                }
                None => return SessionEnd::Lost,
            },
        }
    }
}

/// 构造最小 Heartbeat Envelope(§17.2 必需字段)。
pub fn heartbeat_envelope(device_id: &str) -> Envelope {
    Envelope {
        protocol_version: agent_console_protocol::codec::PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: Some(now_timestamp()),
        device_id: device_id.to_owned(),
        agent_kind: 1, // CODEX_DESKTOP
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(envelope::Payload::Heartbeat(Heartbeat {})),
        provider_extension: None,
    }
}

/// 构造连接握手 ClientHello(BRIDGE,§17.2):升级后首帧,
/// Relay 侧校验 protocol_version 与 device_id。
pub fn client_hello_envelope(device_id: &str) -> Envelope {
    use agent_console_protocol::agent_console::v1::{ClientHello, ClientKind};
    Envelope {
        protocol_version: agent_console_protocol::codec::PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: Some(now_timestamp()),
        device_id: device_id.to_owned(),
        agent_kind: 1, // CODEX_DESKTOP
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(envelope::Payload::ClientHello(ClientHello {
            protocol_version: agent_console_protocol::codec::PROTOCOL_VERSION,
            client_kind: ClientKind::ClientBridge as i32,
            device_id: device_id.to_owned(),
            auth_subject: String::new(),
            capabilities: vec![],
        })),
        provider_extension: None,
    }
}

fn now_timestamp() -> prost_types::Timestamp {
    let now = chrono::Utc::now();
    prost_types::Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RelayUrls;

    fn opts(base: &str) -> RelayClientOptions {
        RelayClientOptions::new(&RelayUrls::parse(base).unwrap())
    }

    #[test]
    fn options_use_relayurls_derived_ws_target() {
        // 派生合同在 config::RelayUrls 测试矩阵覆盖;此处锁定 transport 只认
        // 派生结果,不再自行拼路径。
        assert_eq!(
            opts("ws://127.0.0.1:1").ws_url,
            "ws://127.0.0.1:1/agent-console/bridge/ws"
        );
        assert_eq!(
            opts("https://relay.example.com").ws_url,
            "wss://relay.example.com/agent-console/bridge/ws"
        );
    }

    #[test]
    fn backoff_is_exponential_capped_with_jitter() {
        let mut o = opts("ws://127.0.0.1:1");
        o.reconnect_initial_backoff = Duration::from_secs(1);
        o.reconnect_max_backoff = Duration::from_secs(30);
        o.reconnect_jitter = Duration::from_millis(250);
        // 无 jitter:精确断言指数序列。
        let mut no_jitter = o.clone();
        no_jitter.reconnect_jitter = Duration::ZERO;
        assert_eq!(backoff_delay(0, &no_jitter), Duration::from_secs(1));
        assert_eq!(backoff_delay(1, &no_jitter), Duration::from_secs(2));
        assert_eq!(backoff_delay(2, &no_jitter), Duration::from_secs(4));
        assert_eq!(backoff_delay(3, &no_jitter), Duration::from_secs(8));
        // 有 jitter:base ≤ d < base + jitter;超上限截断到 max。
        for attempt in [0u32, 1, 2, 10] {
            let d = backoff_delay(attempt, &o);
            let base = o
                .reconnect_initial_backoff
                .saturating_mul(1u32 << attempt.min(16))
                .min(o.reconnect_max_backoff);
            assert!(d >= base, "attempt {attempt}: {d:?} < base {base:?}");
            assert!(
                d < base + o.reconnect_jitter,
                "attempt {attempt}: {d:?} >= base+jitter"
            );
        }
        let d = backoff_delay(10, &o); // 1<<10 截断到 30s
        assert!(d >= Duration::from_secs(30));
    }

    #[test]
    fn credential_debug_does_not_leak_token() {
        let cred = StaticCredential::new("super-secret-token");
        let rendered = format!("{cred:?}");
        assert!(!rendered.contains("super-secret-token"));
    }

    #[test]
    fn heartbeat_envelope_has_required_fields() {
        let env = heartbeat_envelope("device-1");
        assert_eq!(
            env.protocol_version,
            agent_console_protocol::codec::PROTOCOL_VERSION
        );
        assert_eq!(env.device_id, "device-1");
        assert!(matches!(env.payload, Some(envelope::Payload::Heartbeat(_))));
        assert!(encode_envelope(&env).is_ok());
    }
}
