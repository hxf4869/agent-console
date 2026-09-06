//! Relay 浏览器 WS transport 补测(T15/AC-03/AC-04;真实 TCP/WS,记 AUTO):
//! - T15:多个 ≤16KiB 分片帧聚合超限(> max_message_size = 1MiB)由 transport
//!   层终止连接,不无限聚合;限内分片重组消息正常处理(正向对照)。
//! - 写期限:真实对端完成升级后停止读取,服务端持续推送时 writer 在
//!   WS_WRITE_TIMEOUT 预算内放弃连接(WS_WRITE_TIMEOUTS 计数 +1)。
//! - 认证撤销:reader/writer 任务全部回收(无泄漏)、连接完全关闭(EOF)、
//!   后续使用被拒、Hub 仍可接受新浏览器。
//! - 慢客户端隔离:同一流上一个停止读取的客户端按自身预算被断开,
//!   不阻塞正常客户端接收,也不阻塞上游 Bridge。

mod support;

use std::{
    net::SocketAddr,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use agent_console_protocol::codec::{
    decode_envelope, encode_envelope, MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use agent_console_protocol::v1::{envelope, Heartbeat};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use relay::realtime::buffer::WS_WRITE_TIMEOUTS;
use relay::RelayServer;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::client::Request,
        protocol::{
            frame::{
                coding::{Data, OpCode},
                Frame,
            },
            Message,
        },
    },
};

use support::{
    base_env, client_hello, connect_browser, handshake_bridge, handshake_browser, insert_device,
    postgres, recv_close, recv_env, recv_skip_presence, send_env, setup, subscribe_session,
    test_credential, FakeBridge, FakeBrowser, FakeToolbox, Ws, AUTH_SESSION, BROWSER,
    INTERNAL_TOKEN,
};

// ---------------------------------------------------------------------------
// 本文件本地连接辅助(进程内 relay::serve 没有 support::Relay 结构可引用)
// ---------------------------------------------------------------------------

fn browser_request(addr: SocketAddr, ticket: &str) -> Request {
    let subprotocol = format!(
        "{}, agent-console.ticket-{}",
        "agent-console.v1",
        URL_SAFE_NO_PAD.encode(ticket.as_bytes())
    );
    let mut request = format!("ws://{addr}/agent-console/ws")
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        subprotocol.parse().expect("header"),
    );
    request
}

/// 原始连接(不握手):分片测试需手工控制首条消息。
async fn connect_browser_raw(addr: SocketAddr, ticket: &str) -> Ws {
    let (ws, _) = connect_async(browser_request(addr, ticket))
        .await
        .expect("browser connect");
    ws
}

async fn connect_browser_at(addr: SocketAddr, ticket: &str) -> Result<FakeBrowser, u16> {
    match connect_async(browser_request(addr, ticket)).await {
        Ok((ws, _)) => {
            let mut b = FakeBrowser { ws };
            let hello = handshake_browser(&mut b.ws).await;
            assert!(matches!(
                hello.payload,
                Some(envelope::Payload::ServerHello(_))
            ));
            Ok(b)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("browser connect failed: {e}"),
    }
}

async fn connect_bridge_at(addr: SocketAddr, device_id: &str, credential: &str) -> FakeBridge {
    let mut request = format!("ws://{addr}/agent-console/bridge/ws")
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().expect("header"),
    );
    let (ws, _) = connect_async(request).await.expect("bridge connect");
    let mut b = FakeBridge {
        ws,
        device_id: device_id.to_string(),
    };
    let hello = handshake_bridge(&mut b.ws, device_id).await;
    assert!(matches!(
        hello.payload,
        Some(envelope::Payload::ServerHello(_))
    ));
    b
}

/// 进程内 relay:临时 PostgreSQL + fake toolbox + `relay::serve`。
/// 返回服务器、toolbox 与数据库连接池。
async fn setup_inprocess(introspect: Duration) -> (RelayServer, FakeToolbox, sqlx::PgPool) {
    let pg = postgres().await;
    let db_url = pg.fresh_db().await;
    let toolbox = FakeToolbox::start().await;
    toolbox.seed_session(AUTH_SESSION, chrono::Duration::hours(1));
    let config = relay::state::Config {
        database_url: db_url,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        internal_token: Some(INTERNAL_TOKEN.to_string()),
        devtoolbox_base_url: Some(toolbox.base_url()),
        trusted_proxy_cidrs: vec!["127.0.0.1/8".parse().unwrap(), "::1/128".parse().unwrap()],
        introspect_interval: introspect,
        auth_grace: Duration::from_secs(120),
        heartbeat_interval: Duration::from_secs(15),
        offline_after: Duration::from_secs(45),
    };
    let pool = sqlx::postgres::PgPool::connect(&config.database_url)
        .await
        .expect("connect db");
    let server = relay::serve(config).await.expect("relay serve");
    (server, toolbox, pool)
}

async fn health_ok(server: &RelayServer) -> bool {
    reqwest::get(format!("{}/health", server.base_url))
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// 以 ≤chunk 字节的分片帧发送一条消息(首帧 Binary FIN=0,续帧 Continuation)。
async fn send_fragmented(ws: &mut Ws, payload: &[u8], chunk: usize) {
    assert!(chunk > 0 && chunk <= 16 * 1024, "fragment must be ≤16KiB");
    let mut offset = 0usize;
    loop {
        let end = (offset + chunk).min(payload.len());
        let opcode = if offset == 0 {
            OpCode::Data(Data::Binary)
        } else {
            OpCode::Data(Data::Continue)
        };
        let fin = end == payload.len();
        let frame = Frame::message(payload[offset..end].to_vec(), opcode, fin);
        ws.send(Message::Frame(frame)).await.expect("send fragment");
        offset = end;
        if fin {
            break;
        }
    }
}

/// 持续等待连接被服务端终止(Close/EOF/错误)。
/// 期间的数据帧是服务端放弃连接前已写入内核缓冲的存量帧(客户端此前
/// 停止读取),排空它们属于正常终止过程;恢复指令/心跳帧同样放行。
async fn await_termination(ws: &mut Ws, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    loop {
        let wait = deadline.saturating_duration_since(Instant::now());
        assert!(!wait.is_zero(), "connection not terminated within budget");
        match tokio::time::timeout(wait, ws.next()).await {
            Err(_) => panic!("connection not terminated in time"),
            Ok(None) => return "EOF".to_string(),
            Ok(Some(Err(e))) => return format!("ERROR: {e}"),
            Ok(Some(Ok(Message::Close(frame)))) => {
                return format!(
                    "CLOSE({})",
                    frame.map(|f| f.reason.to_string()).unwrap_or_default()
                );
            }
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                if let Ok(env) = decode_envelope(&bytes) {
                    match env.payload {
                        Some(envelope::Payload::ResyncRequired(_))
                        | Some(envelope::Payload::Heartbeat(_)) => continue,
                        _ => continue, // 存量数据帧:继续排空直到终止信号。
                    }
                }
                continue; // 同上:无法解码的存量帧不改变终止判定。
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// 从 fake Bridge 持续注入不可合并(不同 item、偏移跳变)的输出增量。
/// `pace` 为帧间间隔:零 = 全速突发(专测写期限),否则按节奏发送,
/// 保证正常客户端能实时跟上(慢端仍按自身队列预算触发 SlowConsumer)。
/// `len` 为每帧输出字节量。
async fn flood_output(
    bridge: &mut FakeBridge,
    device: &str,
    upstream: &str,
    count: u64,
    pace: Duration,
    len: usize,
) {
    let started = Instant::now();
    for i in 0..count {
        send_env(
            &mut bridge.ws,
            &FakeBridge::output_append_env(device, upstream, &format!("item-{i}"), 0, len, 100 + i),
        )
        .await;
        if !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }
    }
    eprintln!(
        "flood({count} x {len}B, {pace:?}) done in {:?}",
        started.elapsed()
    );
}

async fn basic_env() -> (support::Env, ()) {
    let env = setup(&[]).await;
    (env, ())
}

// ---------------------------------------------------------------------------
// T15:分片组成超限消息 → transport 层终止;限内分片 → 正常处理
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn fragmented_over_limit_terminates_and_within_limit_is_processed() {
    let (env, _d) = basic_env().await;
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(60));
    let mut ws = connect_browser_raw(env.relay.addr, &ticket).await;

    // 正向对照 1:分片重组后限内的 ClientHello 被正常处理(握手完成)。
    let hello = encode_envelope(&client_hello(BROWSER, "", PROTOCOL_VERSION)).expect("encode");
    send_fragmented(&mut ws, &hello, hello.len().div_ceil(3)).await;
    let hello_ack = recv_env(&mut ws, Duration::from_secs(5)).await;
    assert!(
        matches!(hello_ack.payload, Some(envelope::Payload::ServerHello(_))),
        "fragmented hello within limit must be reassembled and processed"
    );

    // 正向对照 2:握手后限内分片消息进入正常处理路径(Heartbeat → Ack)。
    let hb =
        encode_envelope(&base_env(envelope::Payload::Heartbeat(Heartbeat {}))).expect("encode");
    send_fragmented(&mut ws, &hb, hb.len().div_ceil(2)).await;
    let ack = recv_env(&mut ws, Duration::from_secs(5)).await;
    assert!(matches!(
        ack.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));

    // T15:多个 ≤16KiB 分片帧聚合约 1.0625MiB > max_message_size(1MiB)
    // → transport 层终止连接,不无限聚合。
    let total = MAX_FRAME_BYTES + 64 * 1024;
    let payload = vec![0xA5u8; total];
    // 服务端在聚合中途即失败;剩余分片可能因对端停止读取写满缓冲,发送超时不视为失败。
    let _ = tokio::time::timeout(
        Duration::from_secs(10),
        send_fragmented(&mut ws, &payload, 16 * 1024),
    )
    .await;
    let end = await_termination(&mut ws, Duration::from_secs(10)).await;
    eprintln!("T15: oversized fragmented message terminated connection via {end}");
}

// ---------------------------------------------------------------------------
// 真实阻塞 socket 写期限:停止读取 → writer 在 WS_WRITE_TIMEOUT 内弃连
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn blocked_client_write_times_out_and_connection_is_abandoned() {
    let (server, toolbox, pool) = setup_inprocess(Duration::from_secs(60)).await;
    let addr = server.addr;
    let baseline = WS_WRITE_TIMEOUTS.load(Ordering::Relaxed);

    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&pool, device_id, &digest).await;
    let device = device_id.to_string();

    let mut bridge = connect_bridge_at(addr, &device, &credential).await;
    let ticket = toolbox.issue_ticket(chrono::Duration::seconds(60));
    let mut browser = connect_browser_at(addr, &ticket).await.expect("browser");

    // 订阅并确认写入路径正常:Subscribed + snapshot 都能到达。
    browser.send(&subscribe_session(&device, "native-wt")).await;
    let sub = loop {
        let frame = bridge.recv(Duration::from_secs(5)).await;
        if matches!(frame.payload, Some(envelope::Payload::Heartbeat(_))) {
            continue;
        }
        break frame;
    };
    let upstream = sub.stream_id.clone();
    assert!(matches!(sub.payload, Some(envelope::Payload::Subscribe(_))));
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device,
            "native-wt",
            &upstream,
            0,
        ))
        .await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;

    // 客户端停止读取;服务端持续推送 → writer 阻塞在填满的内核缓冲上。
    // 单帧 ~950KiB(编码后 < 1MiB 上限):本机 loopback 内核收发缓冲可达
    // ~8MiB 以上,4KiB 小帧 × 2048 帧队列上限(约 8.6MiB)会被内核全部吸收,
    // writer 永不阻塞;大帧使队列字节预算(16MiB)在少量帧内打满,writer
    // 在内核缓冲灌满后真实阻塞,随后队列过载关闭不会先于写阻塞发生。
    let started = Instant::now();
    flood_output(
        &mut bridge,
        &device,
        &upstream,
        200,
        Duration::ZERO,
        950_000,
    )
    .await;

    // WS_WRITE_TIMEOUT = 10s;阻塞点最迟在洪泛结束时,等待超预算 + 余量后
    // 再读:连接必须已被服务端放弃,而非继续悬挂。
    tokio::time::sleep(Duration::from_secs(12)).await;
    let end = await_termination(&mut browser.ws, Duration::from_secs(10)).await;
    let elapsed = started.elapsed();
    eprintln!("write deadline: connection abandoned after {elapsed:?} via {end}");
    assert!(
        elapsed < Duration::from_secs(28),
        "writer must abandon within WS_WRITE_TIMEOUT budget, took {elapsed:?}"
    );

    // 写超时弃连路径的计数必须 +1(精确证据:走的是超时分支而非普通断开)。
    let deadline = Instant::now() + Duration::from_secs(5);
    while WS_WRITE_TIMEOUTS.load(Ordering::Relaxed) <= baseline {
        assert!(
            Instant::now() < deadline,
            "WS_WRITE_TIMEOUTS must increase after write deadline"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 弃连后 relay 仍健康并可接受新浏览器。
    assert!(health_ok(&server).await, "relay must stay healthy");
    let mut browser2 =
        connect_browser_at(addr, &toolbox.issue_ticket(chrono::Duration::seconds(60)))
            .await
            .expect("second browser after abandoned connection");
    browser2
        .send(&base_env(envelope::Payload::Heartbeat(Heartbeat {})))
        .await;
    let ack = recv_skip_presence(&mut browser2.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        ack.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));
}

// ---------------------------------------------------------------------------
// 认证撤销:reader/writer 任务回收、连接完全关闭、后续使用被拒
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn revocation_reclaims_reader_writer_tasks_and_rejects_later_use() {
    let (server, toolbox, _pool) = setup_inprocess(Duration::from_secs(1)).await;
    let addr = server.addr;
    // 等后台任务全部起齐,取稳定基线。
    tokio::time::sleep(Duration::from_millis(500)).await;
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();

    // 第二个有效会话:撤销后验证 Hub 仍能接受新浏览器。
    let session_b = uuid::Uuid::new_v4();
    toolbox.seed_session(session_b, chrono::Duration::hours(1));

    let ticket = toolbox.issue_ticket(chrono::Duration::seconds(60));
    let mut browser = connect_browser_at(addr, &ticket).await.expect("browser");
    browser
        .send(&base_env(envelope::Payload::Heartbeat(Heartbeat {})))
        .await;
    let ack = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        ack.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));

    // 连接活跃:reader/browser_connection + writer + axum 连接任务都在。
    let active = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    assert!(
        active >= baseline + 2,
        "connection must add reader/writer tasks (baseline {baseline}, active {active})"
    );

    // 撤销 auth session → 稳定 close reason 关闭。
    toolbox.revoke_session(AUTH_SESSION);
    let close = recv_close(&mut browser.ws, Duration::from_secs(10)).await;
    assert_eq!(close.1, "AUTH_EXPIRED", "stable close reason");

    // 收尾完整:Close 之后 socket 完全关闭(EOF),reader/writer 全部结束。
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "no EOF after close");
        match tokio::time::timeout(Duration::from_millis(500), browser.ws.next()).await {
            Err(_) => continue,
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(other))) => panic!("unexpected frame after close: {other:?}"),
        }
    }

    // 后续操作被拒:已关闭的连接上发送必须失败,不得有任何人应答。
    match browser.ws.send(Message::Binary(vec![0u8; 8])).await {
        Err(_) => {}
        Ok(()) => match tokio::time::timeout(Duration::from_secs(1), browser.ws.next()).await {
            Err(_) => panic!("post-close send accepted and connection stayed open"),
            Ok(Some(Ok(Message::Binary(_)))) => panic!("post-close operation was answered"),
            _ => {}
        },
    }

    // 无任务泄漏:活跃任务数相对连接活跃时回落至少 2(reader + writer)。
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let now = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        if active.saturating_sub(now) >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "reader/writer tasks not reclaimed after revocation (active {active}, now {now})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 撤销会话的新票据在升级时被拒(认证面后续操作被拒)。
    let rejected =
        connect_browser_at(addr, &toolbox.issue_ticket(chrono::Duration::seconds(60))).await;
    assert!(rejected.is_err(), "revoked session ticket must be rejected");

    // Hub 仍接受新浏览器(另一有效会话),无资源死锁。
    assert!(health_ok(&server).await);
    let mut browser2 = connect_browser_at(
        addr,
        &toolbox.issue_ticket_for(session_b, chrono::Duration::seconds(60)),
    )
    .await
    .expect("browser on other valid session");
    browser2
        .send(&base_env(envelope::Payload::Heartbeat(Heartbeat {})))
        .await;
    let ack2 = recv_skip_presence(&mut browser2.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        ack2.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));
}

// ---------------------------------------------------------------------------
// 慢客户端隔离:同流一个停止读取,另一个持续接收且上游不阻塞
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn slow_client_isolated_and_active_client_unaffected() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let device = device_id.to_string();
    let mut bridge = FakeBridge::connect(&env.relay, &device, &credential)
        .await
        .expect("bridge");

    // 两个真实 WS 客户端同时订阅同一会话流。
    let mut slow = connect_browser(
        &env.relay,
        &env.toolbox.issue_ticket(chrono::Duration::seconds(120)),
    )
    .await
    .expect("slow browser");
    let mut fast = connect_browser(
        &env.relay,
        &env.toolbox.issue_ticket(chrono::Duration::seconds(120)),
    )
    .await
    .expect("fast browser");
    for b in [&mut slow, &mut fast] {
        b.send(&subscribe_session(&device, "native-iso")).await;
    }
    let sub = loop {
        let frame = bridge.recv(Duration::from_secs(5)).await;
        if matches!(frame.payload, Some(envelope::Payload::Heartbeat(_))) {
            continue;
        }
        break frame;
    };
    let upstream = sub.stream_id.clone();
    assert!(matches!(sub.payload, Some(envelope::Payload::Subscribe(_))));
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device,
            "native-iso",
            &upstream,
            0,
        ))
        .await;

    // 双方各自消化 Subscribed + snapshot;此后慢端停止读取。
    for b in [&mut slow, &mut fast] {
        let _ = recv_skip_presence(&mut b.ws, Duration::from_secs(5)).await;
        let _ = recv_skip_presence(&mut b.ws, Duration::from_secs(5)).await;
    }

    // 洪泛按 1ms/帧 节奏发送(3000 帧 ≈ 3s,超过 2048 帧队列预算):
    // 正常客户端能实时跟上不触发自身 SlowConsumer;慢端停止读取,
    // 队列按自身预算在 ~2s 内填满并被摘除。
    let mut flood_source = bridge;
    let flood_device = device.clone();
    let flood_upstream = upstream.clone();
    let flood = tokio::spawn(async move {
        flood_output(
            &mut flood_source,
            &flood_device,
            &flood_upstream,
            3000,
            Duration::from_millis(1),
            4096,
        )
        .await;
        flood_source
    });

    // 正常客户端持续接收,不被慢端阻塞(非 panic 读取:超时/错误即停)。
    let mut fast_received = 0u32;
    let read_started = Instant::now();
    let deadline = read_started + Duration::from_secs(20);
    while fast_received < 2000 && Instant::now() < deadline {
        let Some(e) = support::try_recv_env(&mut fast.ws, Duration::from_secs(3)).await else {
            eprintln!(
                "fast stalled at {fast_received} after {:?}",
                read_started.elapsed()
            );
            break;
        };
        if matches!(e.payload, Some(envelope::Payload::EventBatch(_))) {
            fast_received += 1;
        }
    }
    let mut bridge = flood.await.expect("flood join");
    assert!(
        fast_received >= 2000,
        "fast client must keep receiving while slow client is stalled (got {fast_received})"
    );

    // 慢端按自身预算被断开(Close 或 EOF/错误,不依赖任何外层心跳)。
    let slow_end = await_termination(&mut slow.ws, Duration::from_secs(40)).await;
    eprintln!("slow client terminated via {slow_end}");

    // 慢端断开后,正常客户端继续工作。
    for i in 0..10u64 {
        bridge
            .send(&FakeBridge::output_append_env(
                &device,
                &upstream,
                &format!("post-{i}"),
                0,
                4096,
                5000 + i,
            ))
            .await;
    }
    let mut after = 0u32;
    for _ in 0..5 {
        let e = recv_skip_presence(&mut fast.ws, Duration::from_secs(5)).await;
        if matches!(e.payload, Some(envelope::Payload::EventBatch(_))) {
            after += 1;
        }
    }
    assert!(
        after >= 5,
        "fast client must keep receiving after slow client is gone (got {after})"
    );

    // 上游不阻塞:Bridge 心跳仍有应答。
    bridge
        .send(&base_env(envelope::Payload::Heartbeat(Heartbeat {})))
        .await;
    let ack = loop {
        let f = bridge.recv(Duration::from_secs(5)).await;
        if matches!(f.payload, Some(envelope::Payload::HeartbeatAck(_))) {
            break f;
        }
    };
    assert!(matches!(
        ack.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));
}
