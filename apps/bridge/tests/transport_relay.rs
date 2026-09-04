//! transport 集成测试:用测试内 axum WS 服务器验证握手、心跳超时、重连退避、
//! 有界发送队列与单帧上限(§17.6/§26.2)。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::sync::mpsc;

use agent_console_protocol::agent_console::v1::{envelope, Envelope, HeartbeatAck};
use agent_console_protocol::codec::{
    decode_envelope, encode_envelope, new_message_id, MAX_FRAME_BYTES,
};

use bridge::config::{RelayUrls, BRIDGE_WS_PATH};
use bridge::transport::{
    heartbeat_envelope, ConnectionState, RelayClientOptions, RelayHandle, StaticCredential,
};

// ---------------------------------------------------------------------------
// 测试 Relay 服务器
// ---------------------------------------------------------------------------

/// 输出 transport 内部日志,便于失败诊断。
fn init_test_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new("debug").unwrap())
        .with_test_writer()
        .try_init();
}

#[derive(Default)]
struct ServerBehavior {
    /// 要求的 Authorization 头原文(如 "Bearer token-1");None 表示不校验。
    required_auth: Option<String>,
    /// 记录最近一次请求携带的 Authorization 头。
    seen_auth: Mutex<Option<String>>,
    /// 对 Heartbeat 回 HeartbeatAck。
    reply_heartbeat: bool,
    /// 升级后完全静默(不读不写):验证心跳超时断开。
    silent: bool,
    /// 拒绝前 N 次升级:验证重连退避。
    reject_remaining: AtomicUsize,
    /// 升级后立即发送超限二进制帧:验证 §17.6 单帧上限。
    push_oversize: bool,
    /// 成功建立的连接数。
    connections: AtomicUsize,
}

async fn spawn_test_relay(behavior: std::sync::Arc<ServerBehavior>) -> SocketAddr {
    let app = Router::new()
        .route(
            BRIDGE_WS_PATH,
            get(ws_upgrade).with_state(std::sync::Arc::clone(&behavior)),
        )
        .with_state(std::sync::Arc::clone(&behavior));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

async fn ws_upgrade(
    State(behavior): State<std::sync::Arc<ServerBehavior>>,
    upgrade: WebSocketUpgrade,
    req: Request,
) -> Response {
    // 重连退避测试:前 N 次直接拒绝升级。
    if behavior.reject_remaining.load(Ordering::SeqCst) > 0 {
        behavior.reject_remaining.fetch_sub(1, Ordering::SeqCst);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    *behavior.seen_auth.lock().unwrap() = auth.clone();

    if let Some(expected) = &behavior.required_auth {
        if auth.as_deref() != Some(expected.as_str()) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }

    upgrade.on_upgrade(move |socket| ws_session(behavior, socket))
}

async fn ws_session(behavior: std::sync::Arc<ServerBehavior>, mut socket: WebSocket) {
    behavior.connections.fetch_add(1, Ordering::SeqCst);

    if behavior.silent {
        // 保持连接但不回任何协议帧:客户端心跳超时应主动断开。
        tokio::time::sleep(Duration::from_secs(30)).await;
        return;
    }
    if behavior.push_oversize {
        let big = vec![0u8; MAX_FRAME_BYTES + 1];
        let _ = socket.send(WsMessage::Binary(big.into())).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        return;
    }

    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            WsMessage::Binary(bytes) => {
                let Ok(env) = decode_envelope(&bytes) else {
                    continue;
                };
                if matches!(env.payload, Some(envelope::Payload::Heartbeat(_)))
                    && behavior.reply_heartbeat
                {
                    let ack = Envelope {
                        protocol_version: 1,
                        message_id: new_message_id(),
                        correlation_id: env.message_id.clone(),
                        device_id: env.device_id.clone(),
                        payload: Some(envelope::Payload::HeartbeatAck(HeartbeatAck {})),
                        ..Default::default()
                    };
                    if socket
                        .send(WsMessage::Binary(encode_envelope(&ack).unwrap().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

fn fast_options(relay: &str) -> RelayClientOptions {
    let mut o = RelayClientOptions::new(&RelayUrls::parse(relay).unwrap());
    o.heartbeat_interval = Duration::from_millis(80);
    o.heartbeat_dead_after = Duration::from_secs(10); // 默认不触发超时
    o.reconnect_initial_backoff = Duration::from_millis(20);
    o.reconnect_max_backoff = Duration::from_millis(50);
    o.reconnect_jitter = Duration::ZERO;
    o
}

async fn wait_state(
    handle: &mut RelayHandle,
    pred: impl Fn(ConnectionState) -> bool + Copy,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pred(handle.state()) {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        let _ = tokio::time::timeout(deadline - now, handle.state_changed()).await;
    }
}

async fn wait_for<T>(
    timeout: Duration,
    poll: Duration,
    mut f: impl FnMut() -> Option<T>,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(poll).await;
    }
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handshake_carries_bearer_and_roundtrips_heartbeat() {
    init_test_tracing();
    let behavior = std::sync::Arc::new(ServerBehavior {
        required_auth: Some("Bearer device-token-1".to_owned()),
        reply_heartbeat: true,
        ..Default::default()
    });
    let addr = spawn_test_relay(std::sync::Arc::clone(&behavior)).await;

    let on_connected_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let calls = std::sync::Arc::clone(&on_connected_calls);
    let on_connected: bridge::transport::OnConnected = std::sync::Arc::new(move || {
        calls.fetch_add(1, Ordering::SeqCst);
    });
    let (mut handle, mut inbound) = bridge::transport::start(
        fast_options(&format!("ws://{addr}")).with_device_id("device-test"),
        std::sync::Arc::new(StaticCredential::new("device-token-1")),
        Some(on_connected),
    );

    assert!(
        wait_state(
            &mut handle,
            |s| s == ConnectionState::Connected,
            Duration::from_secs(5)
        )
        .await,
        "应成功连接"
    );
    assert_eq!(
        behavior.seen_auth.lock().unwrap().as_deref(),
        Some("Bearer device-token-1"),
        "必须携带 Bearer 认证头"
    );

    // 心跳往返:发 Heartbeat → 收 HeartbeatAck。
    handle.send(&heartbeat_envelope("device-test")).unwrap();
    let ack = wait_for(
        Duration::from_secs(5),
        Duration::from_millis(20),
        || match inbound.try_recv() {
            Ok(env) => match env.payload {
                Some(envelope::Payload::HeartbeatAck(_)) => Some(()),
                _ => None,
            },
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(e) => panic!("inbound channel: {e}"),
        },
    )
    .await;
    assert!(ack.is_some(), "应收到 HeartbeatAck");

    // on_connected 至少触发一次(重同步钩子)。
    assert!(on_connected_calls.load(Ordering::SeqCst) >= 1);

    // 关闭:状态回到 Disconnected。
    handle.shutdown();
    assert!(
        wait_state(
            &mut handle,
            |s| s == ConnectionState::Disconnected,
            Duration::from_secs(3)
        )
        .await,
        "shutdown 后应断开"
    );
}

#[tokio::test]
async fn silent_relay_triggers_heartbeat_timeout_disconnect() {
    let behavior = std::sync::Arc::new(ServerBehavior {
        silent: true,
        ..Default::default()
    });
    let addr = spawn_test_relay(behavior).await;

    let mut options = fast_options(&format!("ws://{addr}"));
    options.heartbeat_dead_after = Duration::from_millis(300); // 加速超时
    let (mut handle, _inbound) = bridge::transport::start(
        options,
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    assert!(
        wait_state(
            &mut handle,
            |s| s == ConnectionState::Connected,
            Duration::from_secs(5)
        )
        .await,
        "先建立连接"
    );
    // 约 45s 语义在测试中压缩为 300ms:无入站帧必须判离线并重连。
    let saw_disconnected = wait_for(Duration::from_secs(5), Duration::from_millis(20), || {
        let s = handle.state();
        if s != ConnectionState::Connected {
            Some(s)
        } else {
            None
        }
    })
    .await;
    assert!(saw_disconnected.is_some(), "心跳超时后必须离开 Connected");
}

#[tokio::test]
async fn reconnects_with_backoff_after_rejections() {
    let behavior = std::sync::Arc::new(ServerBehavior {
        reply_heartbeat: true,
        reject_remaining: AtomicUsize::new(2),
        ..Default::default()
    });
    let addr = spawn_test_relay(std::sync::Arc::clone(&behavior)).await;

    let (mut handle, _inbound) = bridge::transport::start(
        fast_options(&format!("ws://{addr}")),
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    // 前两次 503 拒绝,退避 20–50ms 后第三次连上。
    assert!(
        wait_state(
            &mut handle,
            |s| s == ConnectionState::Connected,
            Duration::from_secs(5)
        )
        .await,
        "退避重连后应成功"
    );
    assert_eq!(behavior.reject_remaining.load(Ordering::SeqCst), 0);
    assert!(behavior.connections.load(Ordering::SeqCst) >= 1);
    handle.shutdown();
}

#[tokio::test]
async fn outbound_queue_full_returns_error_without_blocking() {
    // 得到一个确定关闭的端口:先 bind 再 drop。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = listener.local_addr().unwrap();
    drop(listener);

    let mut options = fast_options(&format!("ws://{dead_addr}"));
    options.outbound_capacity = 2;
    // 连接失败后退避 60s:测试窗口内 runner 不会排空队列。
    options.reconnect_initial_backoff = Duration::from_secs(60);
    options.reconnect_max_backoff = Duration::from_secs(60);

    let (handle, _inbound) = bridge::transport::start(
        options,
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    let heartbeat = heartbeat_envelope("device-test");
    let started = std::time::Instant::now();
    handle.send(&heartbeat).unwrap();
    handle.send(&heartbeat).unwrap();
    let err = handle.send(&heartbeat).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "队列满必须立即返回,不得阻塞"
    );
    assert!(matches!(err, bridge::transport::TransportError::QueueFull));
    handle.shutdown();
}

#[tokio::test]
async fn oversized_outbound_frame_is_rejected_locally() {
    // §17.6:单帧 >1 MiB 在编码入口拒绝,不产生截断数据、不入队。
    use agent_console_protocol::agent_console::v1::{
        domain_event, DomainEvent, EventBatch, ItemId, OutputAppend,
    };
    let mut options = fast_options("ws://127.0.0.1:1");
    options.outbound_capacity = 64;
    options.reconnect_initial_backoff = Duration::from_secs(60);
    options.reconnect_max_backoff = Duration::from_secs(60);
    let (handle, _inbound) = bridge::transport::start(
        options,
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );
    let huge = Envelope {
        message_id: new_message_id(),
        device_id: "device-test".into(),
        payload: Some(envelope::Payload::EventBatch(EventBatch {
            stream_id: String::new(),
            events: vec![DomainEvent {
                emitted_at: None,
                event: Some(domain_event::Event::OutputAppend(OutputAppend {
                    item_id: Some(ItemId {
                        id: "i".into(),
                        synthetic: false,
                    }),
                    expected_offset: 0,
                    bytes: vec![0u8; MAX_FRAME_BYTES + 1024],
                    channel: 1,
                })),
            }],
        })),
        ..Default::default()
    };
    let err = handle.send(&huge).unwrap_err();
    assert!(matches!(
        err,
        bridge::transport::TransportError::Codec(
            agent_console_protocol::codec::CodecError::FrameTooLarge { .. }
        )
    ));
    handle.shutdown();
}

#[tokio::test]
async fn oversized_inbound_frame_drops_connection() {
    let behavior = std::sync::Arc::new(ServerBehavior {
        push_oversize: true,
        ..Default::default()
    });
    let addr = spawn_test_relay(behavior).await;

    let (mut handle, _inbound) = bridge::transport::start(
        fast_options(&format!("ws://{addr}")),
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    assert!(
        wait_state(
            &mut handle,
            |s| s == ConnectionState::Connected,
            Duration::from_secs(5)
        )
        .await,
        "先建立连接"
    );
    // 收到 >1MiB 帧:协议违约,断开重连(§17.6)。
    let left_connected = wait_for(Duration::from_secs(5), Duration::from_millis(20), || {
        (handle.state() != ConnectionState::Connected).then_some(())
    })
    .await;
    assert!(left_connected.is_some(), "超限入站帧必须导致断开");
    handle.shutdown();
}
