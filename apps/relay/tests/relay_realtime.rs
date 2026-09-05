//! Relay 实时面集成测试(§29.2):
//! - Subscribe → snapshot → 缓冲事件顺序(§17.4)
//! - Ack / 重放(ResyncRequest)/ ResyncRequired / epoch change(§17.5)
//! - 上游重复 sequence 幂等忽略
//! - 有界缓冲、优先级、慢 consumer 被断开且上游不阻塞(§17.6)
//! - Relay 重启后从 fake Bridge 恢复 summary + snapshot(§11)

mod support;

use std::time::Duration;

use agent_console_protocol::codec::decode_envelope;
use agent_console_protocol::v1::{domain_event, envelope, Envelope};
use tokio_tungstenite::tungstenite::Message;

use support::*;

/// 订阅列表流并完成 snapshot 流程,返回 browser 与 bridge。
async fn subscribe_list_flow(
    env: &Env,
    device_id: &str,
    native: &str,
) -> (FakeBrowser, FakeBridge, String, String, u64) {
    let (credential, digest) = test_credential();
    insert_device(
        &env.pool,
        uuid::Uuid::parse_str(device_id).unwrap(),
        &digest,
    )
    .await;
    let mut bridge = FakeBridge::connect(&env.relay, device_id, &credential)
        .await
        .expect("bridge");
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");

    // Browser 订阅列表。
    browser.send(&subscribe_list()).await;
    // Relay 转发上游订阅:bridge 收到 Subscribe(list);跳过可能先到的心跳帧。
    let sub = loop {
        let frame = bridge.recv(Duration::from_secs(40)).await;
        if matches!(frame.payload, Some(envelope::Payload::Heartbeat(_))) {
            continue;
        }
        break frame;
    };
    let upstream_id = match sub.payload {
        Some(envelope::Payload::Subscribe(s)) => {
            assert!(matches!(
                s.target,
                Some(agent_console_protocol::v1::subscribe::Target::List(_))
            ));
            sub.stream_id.clone()
        }
        other => panic!("expected upstream Subscribe, got {other:?}"),
    };
    assert_eq!(upstream_id, format!("u-{device_id}-list"));

    // Bridge 固定 epoch/base(§17.4 步骤 3),发 snapshot。
    bridge.send_subscribed(&upstream_id, 7, 100).await;
    bridge
        .send(&FakeBridge::list_snapshot_env(device_id, native, "任务一"))
        .await;

    // Browser 依次收到:Subscribed → snapshot(顺序唯一,§17.4 步骤 5)。
    let subd = browser.recv(Duration::from_secs(5)).await;
    eprintln!(
        "BROWSER FRAME[1]: {:?}",
        subd.payload.as_ref().map(crate::payload_kind_name)
    );
    eprintln!(
        "BROWSER META[1]: stream={} epoch={} seq={}",
        subd.stream_id, subd.stream_epoch, subd.sequence
    );
    let subscribed_base;
    let downstream_stream_id = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => {
            assert_eq!(s.stream_epoch, 1, "stream epoch");
            subscribed_base = s.base_sequence;
            s.stream_id.clone()
        }
        other => panic!("expected Subscribed, got {other:?}"),
    };
    let snap = browser.recv(Duration::from_secs(5)).await;
    match &snap.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) => {
            assert!(b.snapshot, "first list frame must be snapshot");
            assert_eq!(b.summaries[0].title, "任务一");
        }
        other => panic!("expected snapshot batch, got {other:?}"),
    }
    // Subscribed.base_sequence 必须锚定 snapshot 帧的 sequence(§17.4 步骤 3/5)。
    assert_eq!(
        subscribed_base, snap.sequence,
        "Subscribed.base_sequence anchors the snapshot frame"
    );
    (
        browser,
        bridge,
        upstream_id,
        downstream_stream_id,
        subscribed_base,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_snapshot_buffered_event_order() {
    let (env, _device_id_str) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, upstream_id, _downstream, base) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;

    // snapshot 之后的增量事件按序到达(sequence 单调)。
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "任务一改",
            101,
        ))
        .await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "任务一改2",
            102,
        ))
        .await;
    let e1 = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let e2 = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert_delta(&e1, "任务一改");
    assert_delta(&e2, "任务一改2");
    assert!(
        e1.sequence > base && e2.sequence > e1.sequence,
        "sequence must increase monotonically after the snapshot anchor"
    );
    let _ = upstream_id;
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_upstream_sequence_is_idempotent() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, _upstream, _downstream, _base) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;

    // 同一上游 sequence 101 的事件批发两次:浏览器只能收到一份(§17.5)。
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-A",
            101,
        ))
        .await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-A-dup",
            101,
        ))
        .await;
    let got = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert_delta(&got, "delta-A");
    // 重放帧不应到达(带短超时验证)。
    // 重放帧不应到达:400ms 窗口内跳过 presence 帧,出现其他帧即失败。
    let deadline = std::time::Instant::now() + Duration::from_millis(400);
    loop {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            break; // 窗口内无重复帧 ✓
        }
        let msg = tokio::time::timeout(wait, futures::StreamExt::next(&mut browser.ws)).await;
        let Ok(Some(Ok(Message::Binary(bytes)))) = msg else {
            break; // 超时:窗口内没有帧 ✓
        };
        let e = decode_envelope(&bytes).expect("decode");
        let is_presence = matches!(
            e.payload.as_ref(),
            Some(envelope::Payload::EventBatch(b))
            if !b.events.is_empty()
                && b.events.iter().all(|ev| {
                    matches!(ev.event.as_ref(), Some(domain_event::Event::DevicePresenceChanged(_)))
                })
        );
        assert!(
            !is_presence,
            "presence frame in no-dup window (test artifact)"
        );
        panic!(
            "duplicate upstream sequence must be ignored, got frame seq={}",
            e.sequence
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resync_request_replays_window() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, _upstream, downstream, _base) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-1",
            101,
        ))
        .await;
    let _ = browser.recv(Duration::from_secs(5)).await;

    // Browser 发送 Ack(§17.4 步骤 6)与 ResyncRequest → 窗口内重发 snapshot+缓冲。
    browser
        .send(&base_env(envelope::Payload::Ack(
            agent_console_protocol::v1::Ack {
                stream_id: downstream.clone(),
                sequence: 2,
            },
        )))
        .await;
    browser
        .send(&base_env(envelope::Payload::ResyncRequest(
            agent_console_protocol::v1::ResyncRequest {
                stream_id: downstream,
            },
        )))
        .await;
    let subd = browser.recv(Duration::from_secs(5)).await;
    match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => assert!(s.base_sequence >= 1),
        other => panic!("expected Subscribed on resync, got {other:?}"),
    }
    let snap = browser.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        snap.payload,
        Some(envelope::Payload::SessionSummaryBatch(ref b)) if b.snapshot
    ));
    // 缓冲中的 delta 也被重发。
    let delta = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert_delta(&delta, "delta-1");
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_epoch_change_triggers_resync_and_new_snapshot() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, upstream_id, _downstream, _base) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;

    // Bridge 侧 epoch 变化:新的 Subscribed(§17.5:epoch 变化必须从新 snapshot 开始)。
    bridge.send_subscribed(&upstream_id, 8, 200).await;
    let resync = tokio::time::timeout(
        Duration::from_secs(5),
        recv_env_skip_presence(&mut browser.ws, Duration::from_secs(4)),
    )
    .await
    .unwrap_or_else(|_| panic!("R1: no ResyncRequired within 5s"));
    match resync.payload {
        Some(envelope::Payload::ResyncRequired(r)) => {
            assert_eq!(
                r.reason_code,
                agent_console_protocol::v1::StableErrorCode::ResyncRequired as i32
            );
        }
        other => panic!("expected ResyncRequired, got {other:?}"),
    }

    // Relay 重新发起上游订阅;Bridge 回应新 epoch 并给新 snapshot。
    // 跳过心跳帧,等待 Relay 的重新订阅请求。
    let sub_again = loop {
        let f = tokio::time::timeout(Duration::from_secs(5), bridge.recv(Duration::from_secs(4)))
            .await
            .expect("relay did not re-subscribe after epoch change");
        if matches!(f.payload, Some(envelope::Payload::Subscribe(_))) {
            break f;
        }
    };
    let _ = sub_again;
    bridge.send_subscribed(&upstream_id, 9, 300).await;
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_id.to_string(),
            "native-1",
            "新纪元快照",
        ))
        .await;
    let subd = tokio::time::timeout(Duration::from_secs(5), browser.recv(Duration::from_secs(4)))
        .await
        .unwrap_or_else(|_| panic!("R2: no Subscribed after epoch change"));
    let new_base = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => {
            assert_eq!(s.stream_epoch, 2, "downstream epoch bumped");
            s.base_sequence
        }
        other => panic!("expected Subscribed after epoch change, got {other:?}"),
    };
    let snap = tokio::time::timeout(Duration::from_secs(5), browser.recv(Duration::from_secs(4)))
        .await
        .unwrap_or_else(|_| panic!("R3: no snapshot after epoch change"));
    assert_eq!(snap.sequence, new_base, "snapshot anchors at new base");
    match snap.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) => {
            assert!(b.snapshot);
            assert_eq!(b.summaries[0].title, "新纪元快照");
        }
        other => panic!("expected new snapshot, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn detail_stream_snapshot_then_events_order() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge");
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");

    // 详情订阅。
    browser
        .send(&subscribe_session(&device_id.to_string(), "native-9"))
        .await;
    let sub = bridge.recv(Duration::from_secs(5)).await;
    let _upstream_id = match sub.payload {
        Some(envelope::Payload::Subscribe(s)) => match s.target {
            Some(agent_console_protocol::v1::subscribe::Target::Session(k)) => {
                assert_eq!(k.native_session_id, "native-9");
            }
            other => panic!("expected session target, got {other:?}"),
        },
        other => panic!("expected upstream Subscribe, got {other:?}"),
    };
    let upstream_id = sub.stream_id.clone();
    let _ = upstream_id;

    // snapshot 之前到达的事件必须进入缓冲(§17.4 步骤 4)。
    bridge.send_subscribed(&sub.stream_id, 3, 10).await;
    bridge
        .send(&FakeBridge::output_append_env(
            &device_id.to_string(),
            &sub.stream_id,
            "item-1",
            0,
            5,
            11,
        ))
        .await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device_id.to_string(),
            "native-9",
            &sub.stream_id,
            10,
        ))
        .await;

    // 顺序:Subscribed → RuntimeSnapshot → 缓冲的 OutputAppend 事件。
    let subd = browser.recv(Duration::from_secs(5)).await;
    let (subd_base, downstream_stream) = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => (s.base_sequence, s.stream_id),
        other => panic!("expected Subscribed, got {other:?}"),
    };
    let snap = browser.recv(Duration::from_secs(5)).await;
    eprintln!(
        "DETAIL[2] outer_seq={} payload={:?}",
        snap.sequence,
        snap.payload.as_ref().map(crate::payload_kind_name)
    );
    assert!(matches!(
        snap.payload,
        Some(envelope::Payload::RuntimeSnapshot(_))
    ));
    assert_eq!(
        snap.sequence, subd_base,
        "snapshot anchors at base sequence"
    );
    let ev = browser.recv(Duration::from_secs(5)).await;
    eprintln!(
        "DETAIL[3] outer_seq={} payload={:?}",
        ev.sequence,
        ev.payload.as_ref().map(crate::payload_kind_name)
    );
    match ev.payload {
        Some(envelope::Payload::EventBatch(b)) => {
            assert_eq!(b.events.len(), 1);
            assert!(matches!(
                b.events[0].event,
                Some(domain_event::Event::OutputAppend(_))
            ));
        }
        other => panic!("expected buffered output append, got {other:?}"),
    }
    assert_eq!(
        ev.sequence,
        snap.sequence + 1,
        "buffered event follows snapshot"
    );

    // 单会话 resync 不重放已经合并过的旧窗口，而是重新向 Bridge 取
    // 权威快照；新快照后的实时事件必须继续严格相邻。
    browser
        .send(&base_env(envelope::Payload::ResyncRequest(
            agent_console_protocol::v1::ResyncRequest {
                stream_id: downstream_stream,
            },
        )))
        .await;
    let refresh = loop {
        let frame = bridge.recv(Duration::from_secs(5)).await;
        if matches!(frame.payload, Some(envelope::Payload::Heartbeat(_))) {
            continue;
        }
        break frame;
    };
    assert!(matches!(
        refresh.payload,
        Some(envelope::Payload::Subscribe(ref subscribe))
            if matches!(subscribe.target, Some(agent_console_protocol::v1::subscribe::Target::Session(_)))
    ));
    bridge.send_subscribed(&refresh.stream_id, 4, 20).await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device_id.to_string(),
            "native-9",
            &refresh.stream_id,
            20,
        ))
        .await;

    let refreshed = browser.recv(Duration::from_secs(5)).await;
    let refreshed_base = match refreshed.payload {
        Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
        other => panic!("expected fresh Subscribed on detail resync, got {other:?}"),
    };
    let refreshed_snapshot = browser.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        refreshed_snapshot.payload,
        Some(envelope::Payload::RuntimeSnapshot(_))
    ));
    assert_eq!(refreshed_snapshot.sequence, refreshed_base);
    assert!(refreshed_snapshot.sequence > ev.sequence);

    bridge
        .send(&FakeBridge::output_append_env(
            &device_id.to_string(),
            &refresh.stream_id,
            "item-1",
            5,
            6,
            21,
        ))
        .await;
    let after_resync = browser.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        after_resync.payload,
        Some(envelope::Payload::EventBatch(_))
    ));
    assert_eq!(after_resync.sequence, refreshed_snapshot.sequence + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_gets_resync_and_disconnect_upstream_unblocked() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge");
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");

    browser
        .send(&subscribe_session(&device_id.to_string(), "native-slow"))
        .await;
    let sub = bridge.recv(Duration::from_secs(5)).await;
    let upstream = sub.stream_id.clone();
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device_id.to_string(),
            "native-slow",
            &upstream,
            0,
        ))
        .await;
    // 消化 Subscribed + snapshot。
    let _ = browser.recv(Duration::from_secs(5)).await;
    let _ = browser.recv(Duration::from_secs(5)).await;

    // Browser 停止读取;Bridge 洪泛不可合并(偏移跳变)的输出增量。
    // 队列上限 2048 帧;发送 3000 帧触发慢 consumer 处理。
    for i in 0..3000u64 {
        bridge
            .send(&FakeBridge::output_append_env(
                &device_id.to_string(),
                &upstream,
                &format!("item-{i}"),
                0,
                4096,
                100 + i,
            ))
            .await;
    }

    // Browser 端最终收到 ResyncRequired 且连接被关闭(稳定 close reason)。
    let mut saw_resync = false;
    let close = loop {
        let msg = recv_raw(&mut browser.ws, Duration::from_secs(60)).await;
        if let Message::Binary(bytes) = &msg {
            if let Ok(env) = decode_envelope(bytes) {
                if matches!(env.payload, Some(envelope::Payload::ResyncRequired(_))) {
                    saw_resync = true;
                }
            }
        }
        if let Message::Close(frame) = msg {
            let f = frame.expect("close frame");
            break (f.code, f.reason.to_string());
        }
    };
    assert!(saw_resync, "ResyncRequired must precede disconnect");
    assert_eq!(close.1, "RESYNC_REQUIRED", "stable close reason");

    // 上游不阻塞:Bridge 仍可心跳并收到 HeartbeatAck(§17.6)。
    bridge
        .send(&base_env(envelope::Payload::Heartbeat(
            agent_console_protocol::v1::Heartbeat {},
        )))
        .await;
    // 跳过可能排队的心跳/其他帧,等待 HeartbeatAck。
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

#[tokio::test(flavor = "multi_thread")]
async fn relay_restart_recovers_summary_and_snapshot_from_bridge() {
    let pg = postgres().await;
    let db_url = pg.fresh_db().await;
    let toolbox = FakeToolbox::start().await;
    toolbox.seed_session(AUTH_SESSION, chrono::Duration::hours(1));
    let mut relay = Relay::spawn(&db_url, &toolbox, &[]);
    relay.wait_healthy(Duration::from_secs(30)).await;
    let pool = pg.pool(&db_url).await;

    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&pool, device_id, &digest).await;
    let device_str = device_id.to_string();

    // 第一轮:bridge + browser 订阅并收到快照。
    let mut bridge = FakeBridge::connect(&relay, &device_str, &credential)
        .await
        .expect("bridge");
    let ticket = toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&relay, &ticket).await.expect("browser");
    browser.send(&subscribe_list()).await;
    let sub = bridge.recv(Duration::from_secs(5)).await;
    let upstream = sub.stream_id.clone();
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_str,
            "native-r",
            "重启前标题",
        ))
        .await;
    let subd = browser.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        subd.payload,
        Some(envelope::Payload::Subscribed(_))
    ));
    let snap = browser.recv(Duration::from_secs(5)).await;
    match snap.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) => {
            assert_eq!(b.summaries[0].title, "重启前标题")
        }
        other => panic!("expected snapshot, got {other:?}"),
    }

    // 重启 Relay(§11:重启后从在线 Bridge 重新拉 summary + 活跃 snapshot)。
    relay.kill();
    drop(browser);
    drop(bridge);

    let mut relay2 = Relay::spawn(&db_url, &toolbox, &[]);
    relay2.wait_healthy(Duration::from_secs(30)).await;

    // Bridge 重连,Browser 重新订阅 → 立即获得 summary 快照。
    let mut bridge2 = FakeBridge::connect(&relay2, &device_str, &credential)
        .await
        .expect("bridge reconnect");
    let ticket2 = toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser2 = connect_browser(&relay2, &ticket2)
        .await
        .expect("browser reconnect");
    browser2.send(&subscribe_list()).await;
    let sub2 = bridge2.recv(Duration::from_secs(5)).await;
    let upstream2 = sub2.stream_id.clone();
    bridge2.send_subscribed(&upstream2, 1, 0).await;
    bridge2
        .send(&FakeBridge::list_snapshot_env(
            &device_str,
            "native-r",
            "重启后标题",
        ))
        .await;
    let subd2 = browser2.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        subd2.payload,
        Some(envelope::Payload::Subscribed(_))
    ));
    let snap2 = browser2.recv(Duration::from_secs(5)).await;
    match snap2.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) => {
            assert!(b.snapshot);
            assert_eq!(b.summaries[0].title, "重启后标题");
        }
        other => panic!("expected snapshot after restart, got {other:?}"),
    }

    // 摘要持久化在 Relay PostgreSQL(重启期间也不丢)。
    let row: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM session_summaries WHERE native_session_id = 'native-r'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 1);
    relay2.kill();
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

async fn basic_env() -> (Env, ()) {
    let env = setup(&[]).await;
    (env, ())
}

/// 读取一帧;跳过 DevicePresenceChanged(设备上下线事件与测试流量竞速)。
async fn recv_env_skip_presence(ws: &mut Ws, timeout: Duration) -> Envelope {
    loop {
        let e = recv_env(ws, timeout).await;
        let all_presence = matches!(
            e.payload.as_ref(),
            Some(envelope::Payload::EventBatch(b))
            if !b.events.is_empty()
                && b.events.iter().all(|ev| {
                    matches!(ev.event.as_ref(), Some(domain_event::Event::DevicePresenceChanged(_)))
                })
        );
        if !all_presence {
            return e;
        }
    }
}

/// 断言一帧是"摘要增量"事件(Relay 把 snapshot=false 的批次转换为
/// SessionSummaryChanged 领域事件)并校验标题。
fn assert_delta(e: &Envelope, title: &str) {
    match e.payload.as_ref() {
        Some(envelope::Payload::EventBatch(b)) => {
            assert_eq!(b.events.len(), 1);
            match b.events[0].event.as_ref().unwrap() {
                domain_event::Event::SessionSummaryChanged(sum) => {
                    assert_eq!(sum.title, title);
                }
                other => panic!("expected SessionSummaryChanged, got {other:?}"),
            }
        }
        other => panic!("expected delta event batch, got {other:?}"),
    }
}
