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
    /// 升级后完全静默(不读不写):模拟黑洞链路(心跳超时/写阻塞 fixture)。
    silent: bool,
    /// 升级成功后立即 Close:应用协议从未建立(验证退避不在连接层重置)。
    close_after_upgrade: bool,
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
        // 保持连接但不读不写:客户端心跳超时/写预算必须主动断开(黑洞 fixture)。
        tokio::time::sleep(Duration::from_secs(120)).await;
        return;
    }
    if behavior.close_after_upgrade {
        // 升级成功即 Close:ClientHello 之后没有任何有效协议帧。
        let _ = socket.send(WsMessage::Close(None)).await;
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

// ---------------------------------------------------------------------------
// AC-04 回归:连接预算、写预算、退避重置时机(FIXTURE:本地假服务表达
// 真实网络黑洞/睡眠唤醒场景)
// ---------------------------------------------------------------------------

/// 原始 TCP 黑洞服务:接受连接后原样挂住(不读不写、不回握手响应),
/// 模拟网络黑洞(路由黑洞/对端假死)。返回连接计数句柄。
async fn spawn_blackhole_tcp() -> (SocketAddr, std::sync::Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&accepts);
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            // 挂住 socket:不读不写,保持 TCP 打开(不 drop,否则客户端立即
            // 收到 reset 而非黑洞)。
            tokio::spawn(async move {
                let _held = sock;
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
        }
    });
    (addr, accepts)
}

/// T16/T18/FIXTURE:服务端接受 TCP 但永不完成 WS 握手 → 连接预算(而非
/// OS 级分钟超时)限时失败进入退避;进行中 shutdown → 及时取消,无遗留任务。
#[tokio::test]
async fn stalled_handshake_retries_within_connect_budget_and_shutdown_cancels() {
    init_test_tracing();
    let (addr, accepts) = spawn_blackhole_tcp().await;

    let mut options = fast_options(&format!("ws://{addr}"));
    options.connect_budget = Duration::from_millis(400);
    options.reconnect_initial_backoff = Duration::from_millis(100);
    options.reconnect_max_backoff = Duration::from_millis(200);
    let (handle, _inbound) = bridge::transport::start(
        options,
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    // 预算尺度内必须出现第二次连接尝试(第一次失败 + 退避重试),
    // 而不是挂在分钟级 OS 连接超时上。
    let retried = wait_for(Duration::from_secs(5), Duration::from_millis(20), || {
        (accepts.load(Ordering::SeqCst) >= 2).then_some(())
    })
    .await;
    assert!(retried.is_some(), "连接预算未生效:5s 内没有第二次连接尝试");
    assert_ne!(
        handle.state(),
        ConnectionState::Connected,
        "黑洞握手不可能进入 Connected"
    );

    // T18:连接尝试进行中 shutdown → 及时退出,不再发起新连接(无遗留任务)。
    handle.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before = accepts.load(Ordering::SeqCst);
    // 等待 > 连接预算 + 最大退避(400ms + 200ms):不得再有新尝试。
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after = accepts.load(Ordering::SeqCst);
    assert_eq!(before, after, "shutdown 后仍有遗留连接任务在重试");
}

/// T17/FIXTURE:服务端升级后不再读取 → sink 写阻塞。写预算必须先于外层
/// 心跳判定生效:断开走重连,而不是等 heartbeat_dead_after。
#[tokio::test]
async fn blocked_sink_write_drops_connection_at_send_deadline() {
    init_test_tracing();
    let behavior = std::sync::Arc::new(ServerBehavior {
        silent: true,
        ..Default::default()
    });
    let addr = spawn_test_relay(behavior).await;

    let mut options = fast_options(&format!("ws://{addr}"));
    options.send_budget = Duration::from_millis(800);
    // 外层心跳判定故意放到远超测试窗口之外:若断开发生,只能是写预算。
    options.heartbeat_interval = Duration::from_secs(5);
    options.heartbeat_dead_after = Duration::from_secs(90);
    options.outbound_capacity = 64;
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

    // 大帧灌入出站队列:对端不读,内核缓冲填满后 sink 写阻塞。
    let big = Envelope {
        message_id: new_message_id(),
        device_id: "device-test".into(),
        payload: Some(envelope::Payload::EventBatch(event_batch_fixture(
            700 * 1024,
        ))),
        ..Default::default()
    };
    for _ in 0..32 {
        match handle.send(&big) {
            Ok(()) => {}
            Err(bridge::transport::TransportError::QueueFull) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(_) => break,
        }
    }

    let dropped = wait_for(Duration::from_secs(15), Duration::from_millis(20), || {
        (handle.state() != ConnectionState::Connected).then_some(())
    })
    .await;
    assert!(dropped.is_some(), "写预算未生效:15s 内未断开(在等外层心跳)");
    handle.shutdown();
}

fn event_batch_fixture(bytes_len: usize) -> agent_console_protocol::agent_console::v1::EventBatch {
    use agent_console_protocol::agent_console::v1::{
        domain_event, DomainEvent, ItemId, OutputAppend,
    };
    agent_console_protocol::agent_console::v1::EventBatch {
        stream_id: String::new(),
        events: vec![DomainEvent {
            emitted_at: None,
            event: Some(domain_event::Event::OutputAppend(OutputAppend {
                item_id: Some(ItemId {
                    id: "i".into(),
                    synthetic: false,
                }),
                expected_offset: 0,
                bytes: vec![0u8; bytes_len],
                channel: 1,
            })),
        }],
    }
}

/// FIXTURE:升级成功但应用协议从未成立(立即 Close)→ 退避不得在连接层
/// 重置;只有收到有效协议帧(握手完成)后才允许归零,避免对"仅 TCP/WS
/// 升级成功"的服务以最短间隔反复重试。
#[tokio::test]
async fn backoff_keeps_growing_until_protocol_established() {
    init_test_tracing();
    let behavior = std::sync::Arc::new(ServerBehavior {
        close_after_upgrade: true,
        ..Default::default()
    });
    let addr = spawn_test_relay(std::sync::Arc::clone(&behavior)).await;

    let mut options = fast_options(&format!("ws://{addr}"));
    options.reconnect_initial_backoff = Duration::from_millis(200);
    options.reconnect_max_backoff = Duration::from_millis(1000);
    let (handle, _inbound) = bridge::transport::start(
        options,
        std::sync::Arc::new(StaticCredential::new("t")),
        None,
    );

    // 1.5s 窗口:正确行为(200ms→400ms→800ms→1000ms)约 4 次连接;
    // 缺陷行为(每次连接成功即重置)约 8 次。窗口两侧留足裕量。
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let connections = behavior.connections.load(Ordering::SeqCst);
    assert!(
        connections <= 5,
        "协议未建立时退避被重置:1.5s 内连接 {connections} 次"
    );
    assert!(connections >= 2, "应持续按退避重试,实际 {connections} 次");
    handle.shutdown();
}
