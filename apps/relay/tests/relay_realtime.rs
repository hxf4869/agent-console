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
/// 返回的 SeqTracker 已锚定快照后的下一期待序号(后续帧序号连续性由它校验)。
async fn subscribe_list_flow(
    env: &Env,
    device_id: &str,
    native: &str,
) -> (FakeBrowser, FakeBridge, String, String, u64, SeqTracker) {
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

    // Browser 依次收到:Subscribed → 重放序列(快照前缓存的 presence 等流帧
    // 按全局序号连续)→ snapshot(§17.4 步骤 5)。
    let subd = recv_env(&mut browser.ws, Duration::from_secs(5)).await;
    let subscribed_base;
    let downstream_stream_id = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => {
            assert_eq!(s.stream_epoch, 1, "stream epoch");
            subscribed_base = s.base_sequence;
            s.stream_id.clone()
        }
        other => panic!("expected Subscribed, got {other:?}"),
    };
    let mut tracker = SeqTracker::from_base(subscribed_base);
    loop {
        let frame = tracker.recv(&mut browser.ws).await;
        match frame.payload {
            Some(envelope::Payload::SessionSummaryBatch(ref b)) if b.snapshot => {
                assert_eq!(b.summaries[0].title, "任务一");
                break;
            }
            Some(envelope::Payload::EventBatch(_)) => continue, // presence:序号已校验
            other => panic!("expected snapshot batch, got {other:?}"),
        }
    }
    // 排空快照后可能迟到的 presence 帧(序号连续性同步校验),保证返回的
    // tracker 期望值对调用方准确。
    tracker
        .expect_quiet(&mut browser.ws, Duration::from_millis(400))
        .await;
    (
        browser,
        bridge,
        upstream_id,
        downstream_stream_id,
        subscribed_base,
        tracker,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_snapshot_buffered_event_order() {
    let (env, _device_id_str) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, upstream_id, _downstream, base, _tracker) =
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
    let (mut browser, mut bridge, _upstream, _downstream, _base, _tracker) =
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
    let (mut browser, mut bridge, _upstream, downstream, _base, _tracker) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-1",
            101,
        ))
        .await;
    let _ = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;

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
    let subd = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let resync_base = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
        other => panic!("expected Subscribed on resync, got {other:?}"),
    };
    let snap = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        snap.payload,
        Some(envelope::Payload::SessionSummaryBatch(ref b)) if b.snapshot
    ));
    // base 锚定重放窗口首帧:纯快照窗口时快照占据 base(§17.4 步骤 5),
    // 快照前有未覆盖 presence 帧时首帧事件为 base+1;快照序号不得小于 base。
    assert!(
        resync_base <= snap.sequence,
        "resync base must anchor at or before the replayed snapshot"
    );
    // 缓冲中的 delta 也被重发。
    let delta = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert_delta(&delta, "delta-1");
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_epoch_change_triggers_resync_and_new_snapshot() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, upstream_id, _downstream, _base, _tracker) =
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

    // 当前 Subscribed 已宣告新 epoch，Bridge 的新 snapshot 正在到达；Relay
    // 不得再次订阅同一 upstream，否则会让每次响应继续生成新 epoch。
    let unexpected = tokio::time::timeout(
        Duration::from_millis(300),
        bridge.recv(Duration::from_secs(4)),
    )
    .await;
    if let Ok(frame) = unexpected {
        assert!(
            !matches!(frame.payload, Some(envelope::Payload::Subscribe(_))),
            "epoch change must not re-subscribe the same upstream"
        );
    }
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_id.to_string(),
            "native-1",
            "新纪元快照",
        ))
        .await;
    let subd = tokio::time::timeout(
        Duration::from_secs(5),
        recv_env_skip_presence(&mut browser.ws, Duration::from_secs(4)),
    )
    .await
    .unwrap_or_else(|_| panic!("R2: no Subscribed after epoch change"));
    let new_base = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => {
            assert_eq!(s.stream_epoch, 2, "downstream epoch bumped");
            s.base_sequence
        }
        other => panic!("expected Subscribed after epoch change, got {other:?}"),
    };
    let snap = tokio::time::timeout(
        Duration::from_secs(5),
        recv_env_skip_presence(&mut browser.ws, Duration::from_secs(4)),
    )
    .await
    .unwrap_or_else(|_| panic!("R3: no snapshot after epoch change"));
    assert_eq!(
        snap.sequence, new_base,
        "snapshot anchors at the new base (§17.4 步骤 5)"
    );
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

    // 顺序:Subscribed → 缓冲的 OutputAppend → RuntimeSnapshot。重放按
    // Bridge 实际发送顺序(上游批 11 在快照水位之后,内容不被快照覆盖),
    // 与活跃订阅者的应用顺序一致(R2-AC01:恢复者不得反向应用)。
    let subd = browser.recv(Duration::from_secs(5)).await;
    let (subd_base, downstream_stream) = match subd.payload {
        Some(envelope::Payload::Subscribed(s)) => (s.base_sequence, s.stream_id),
        other => panic!("expected Subscribed, got {other:?}"),
    };
    let ev = browser.recv(Duration::from_secs(5)).await;
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
        subd_base + 1,
        "first replayed frame follows the subscribed base"
    );
    let snap = browser.recv(Duration::from_secs(5)).await;
    assert!(matches!(
        snap.payload,
        Some(envelope::Payload::RuntimeSnapshot(_))
    ));
    assert_eq!(
        snap.sequence,
        ev.sequence + 1,
        "snapshot follows the buffered event with a contiguous sequence"
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

    // 受控慢 sink:接管浏览器 socket,持续但低速(每帧 5ms)读取,连接
    // 保持可写。背压确定性作用在应用 Outbox(帧上限 2048)上,不再依赖
    // "洪泛恰好耗尽 OS/网络缓冲"这种环境相关假设;收到的帧转发给断言方
    // 记录期望与实际接收帧类型。
    const SLOW_SINK_TICK: Duration = Duration::from_millis(5);
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = tokio::spawn(async move {
        loop {
            let next = futures::StreamExt::next(&mut browser.ws);
            match tokio::time::timeout(Duration::from_secs(90), next).await {
                Ok(Some(Ok(msg))) => {
                    let is_close = matches!(msg, Message::Close(_));
                    if frames_tx.send(msg).is_err() {
                        break; // 断言方已结束。
                    }
                    if is_close {
                        break;
                    }
                    tokio::time::sleep(SLOW_SINK_TICK).await;
                }
                _ => break, // EOF / 错误 / 空闲超时:连接已结束。
            }
        }
    });

    // Bridge 洪泛不可合并(item 逐帧变化)的输出增量。总量(约 26MB)
    // 显著大于 Outbox 帧上限(2048)与内核收发缓冲最坏可吸收量(Linux
    // 自动调优合计约 4-10MB);慢 sink 读取速率远低于洪泛速率,应用
    // Outbox 确定性积压超限并触发慢 consumer 处理。
    for i in 0..6000u64 {
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

    // 期望帧序:…洪泛业务帧… → ResyncRequired → Close(稳定 reason)。
    // 连接可写,ResyncRequired 必须实际送达;跳过业务帧并计数。
    let mut saw_resync = false;
    let mut business_frames = 0usize;
    let close = loop {
        let msg = tokio::time::timeout(Duration::from_secs(60), frames_rx.recv())
            .await
            .expect("timeout waiting for browser frames after flood")
            .expect("slow sink ended before delivering a Close frame");
        match msg {
            Message::Binary(bytes) => match decode_envelope(&bytes) {
                Ok(e) if matches!(e.payload, Some(envelope::Payload::ResyncRequired(_))) => {
                    saw_resync = true;
                }
                Ok(_) => business_frames += 1,
                Err(err) => panic!("relay sent an undecodable frame: {err}"),
            },
            Message::Close(frame) => {
                let f = frame.expect("close frame");
                break (f.code, f.reason.to_string());
            }
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected frame from relay: {other:?}"),
        }
    };
    eprintln!(
        "SLOW SINK: business_frames={business_frames} saw_resync={saw_resync} close=({}, {})",
        close.0, close.1
    );
    assert!(
        business_frames > 0,
        "slow sink must receive forwarded stream frames before close"
    );
    assert!(saw_resync, "ResyncRequired must precede disconnect");
    assert_eq!(close.1, "RESYNC_REQUIRED", "stable close reason");
    let _ = sink.await;

    // 上游不阻塞:Bridge 仍可心跳并收到 HeartbeatAck(§17.6)。
    assert_bridge_alive(&mut bridge).await;
}

/// 完全不可写的慢 consumer:洪泛期间浏览器完全不读取。连接不可写时
/// ResyncRequired/Close 属尽力送达,不能断言对端一定收到 Close 帧;但
/// Relay 必须有限时间收尾:连接在预算内结束,且 Bridge 上游不被阻塞。
#[tokio::test(flavor = "multi_thread")]
async fn fully_blocked_consumer_terminates_in_finite_time_upstream_unblocked() {
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
        .send(&subscribe_session(&device_id.to_string(), "native-blocked"))
        .await;
    let sub = bridge.recv(Duration::from_secs(5)).await;
    let upstream = sub.stream_id.clone();
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::runtime_snapshot_env(
            &device_id.to_string(),
            "native-blocked",
            &upstream,
            0,
        ))
        .await;
    // 消化 Subscribed + snapshot。
    let _ = browser.recv(Duration::from_secs(5)).await;
    let _ = browser.recv(Duration::from_secs(5)).await;

    // 洪泛期间完全不读取:总量(约 26MB)远大于内核缓冲可吸收量,
    // 写循环受阻后应用 Outbox(帧上限 2048)确定性超限。
    for i in 0..6000u64 {
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

    // 有限时间收尾:开始读取后排空积压,连接必须在预算内结束
    // (Close / 错误 / EOF 任一;不断言具体收到哪类帧)。
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut drained = 0usize;
    loop {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        let msg = tokio::time::timeout(wait, futures::StreamExt::next(&mut browser.ws))
            .await
            .unwrap_or_else(|_| {
                panic!("connection must terminate in finite time; drained={drained}")
            });
        match msg {
            Some(Ok(_)) => drained += 1,
            Some(Err(_)) | None => break,
        }
    }
    eprintln!("BLOCKED SINK: connection terminated after draining {drained} frames");

    // 上游不阻塞:Bridge 仍可心跳并收到 HeartbeatAck(§17.6)。
    assert_bridge_alive(&mut bridge).await;
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
    let subd = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        subd.payload,
        Some(envelope::Payload::Subscribed(_))
    ));
    let snap = recv_env_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
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
    let subd2 = recv_env_skip_presence(&mut browser2.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        subd2.payload,
        Some(envelope::Payload::Subscribed(_))
    ));
    let snap2 = recv_env_skip_presence(&mut browser2.ws, Duration::from_secs(5)).await;
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

/// R2-AC01 最小坏序列:快照 → 实时事件(已送达,仍在 buffer)→ 同设备新快照
/// → 新事件。旧实现把 buffer 旧事件重编号到新快照之后并推进全局序号,活跃
/// 订阅者既收不到重编号帧又期待被推进的序号,形成假缺帧。新实现下快照帧
/// 恰为活跃订阅者的下一期待序号,覆盖删除不产生重放,新事件序号紧邻。
#[tokio::test(flavor = "multi_thread")]
async fn fresh_snapshot_does_not_renumber_or_replay_covered_events_to_active_subscriber() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser, mut bridge, _upstream, _downstream, _base, mut tracker) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;

    // 快照1 → 实时 delta(批101,已送达) → 同设备新快照。
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "任务一改",
            101,
        ))
        .await;
    let delta1 = tracker.recv(&mut browser.ws).await;
    assert_delta(&delta1, "任务一改");

    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_id.to_string(),
            "native-1",
            "刷新快照",
        ))
        .await;
    let snap2 = tracker.recv(&mut browser.ws).await;
    match &snap2.payload {
        Some(envelope::Payload::SessionSummaryBatch(b)) => {
            assert!(b.snapshot);
            assert_eq!(b.summaries[0].title, "刷新快照");
        }
        other => panic!("expected refreshed snapshot, got {other:?}"),
    }
    assert_eq!(
        snap2.sequence,
        delta1.sequence + 1,
        "snapshot frame is exactly the next expected sequence (no fabricated gap)"
    );

    // 覆盖删除后不得重放旧 delta:短窗口内无任何流帧。
    tracker
        .expect_quiet(&mut browser.ws, Duration::from_millis(400))
        .await;

    // 新 delta(批102):序号紧邻新快照,内容是快照之后的新事件(不缺帧、
    // 不被旧事件冒充)。
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "任务一改2",
            102,
        ))
        .await;
    let delta2 = tracker.recv(&mut browser.ws).await;
    assert_delta(&delta2, "任务一改2");
    assert_eq!(
        delta2.sequence,
        snap2.sequence + 1,
        "next live event continues contiguously after the fresh snapshot"
    );
}

/// R2-AC01 两设备快照交错:设备 A 的新快照不覆盖、不重编号设备 B 的事件;
/// 列表聚合按设备维护各自最新快照,第二个浏览器订阅时收到各设备快照、
/// 序号连续,不依赖重编号的他人事件。
#[tokio::test(flavor = "multi_thread")]
async fn list_snapshot_for_device_a_does_not_cover_or_renumber_device_b_events() {
    let (env, _d) = basic_env().await;
    let device_a = uuid::Uuid::new_v4();
    let device_b = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_a, &digest).await;
    let (credential_b, digest_b) = test_credential();
    insert_device(&env.pool, device_b, &digest_b).await;
    let mut bridge_a = FakeBridge::connect(&env.relay, &device_a.to_string(), &credential)
        .await
        .expect("bridge a");
    let mut bridge_b = FakeBridge::connect(&env.relay, &device_b.to_string(), &credential_b)
        .await
        .expect("bridge b");
    // 有界就绪轮询:列表订阅按 owner 当前在线设备建立上游绑定,必须等两台
    // 设备的 Bridge 都在 Relay 注册生效。/agent-console/api/devices 的
    // connection 字段直接来自 Relay 注册表,轮询它即可确认注册可见,不污染
    // WebSocket 消息流。间隔 25ms,总预算 5s,超时带当前注册状态 panic。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = http_get(&env.relay, "/agent-console/api/devices", &identity_headers()).await;
        assert_eq!(resp.status(), 200, "devices endpoint must be reachable");
        let body: serde_json::Value = resp.json().await.expect("devices json");
        let registered = |id: uuid::Uuid| {
            body["devices"].as_array().is_some_and(|items| {
                items.iter().any(|d| {
                    d["id"].as_str() == Some(&id.to_string())
                        && d["connection"] == "CONNECTION_ONLINE"
                })
            })
        };
        if registered(device_a) && registered(device_b) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge registration not visible within 5s: a={device_a} b={device_b}, registry={body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser1 = connect_browser(&env.relay, &ticket).await.expect("browser1");

    browser1.send(&subscribe_list()).await;
    // 跳过可能先到的 Relay 心跳帧(与 subscribe_list_flow 相同的过滤)。
    let mut upstream_a = String::new();
    let mut _upstream_b = String::new();
    for bridge in [&mut bridge_a, &mut bridge_b] {
        let sub = loop {
            let frame = bridge.recv(Duration::from_secs(10)).await;
            if matches!(frame.payload, Some(envelope::Payload::Heartbeat(_))) {
                continue;
            }
            break frame;
        };
        let is_list_subscribe = matches!(
            sub.payload.as_ref(),
            Some(envelope::Payload::Subscribe(t))
                if matches!(t.target, Some(agent_console_protocol::v1::subscribe::Target::List(_)))
        );
        assert!(is_list_subscribe);
        if sub.device_id == device_a.to_string() {
            upstream_a = sub.stream_id.clone();
        } else {
            _upstream_b = sub.stream_id.clone();
        }
    }
    assert!(!upstream_a.is_empty() && !_upstream_b.is_empty());
    // A:Subscribed(7,100) + 快照A;B:delta(批201)。
    bridge_a.send_subscribed(&upstream_a, 7, 100).await;
    bridge_a
        .send(&FakeBridge::list_snapshot_env(
            &device_a.to_string(),
            "native-a",
            "任务A",
        ))
        .await;
    let subd1 = recv_env(&mut browser1.ws, Duration::from_secs(5)).await;
    assert!(matches!(subd1.payload, Some(envelope::Payload::Subscribed(_))));
    let mut tracker1 = SeqTracker::from_base(
        match subd1.payload {
            Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
            _ => unreachable!(),
        },
    );

    // 依次推进业务帧;presence 帧由 tracker 按连续序号消化(不跳过校验)。
    async fn next_summary(tracker: &mut SeqTracker, ws: &mut Ws, title: &str, snapshot: bool) -> Envelope {
        loop {
            let e = tracker.recv(ws).await;
            // 快照帧 = SessionSummaryBatch(snapshot=true);增量 = EventBatch
            // 内的 SessionSummaryChanged。presence 等 EventBatch 一并消化
            //(序号已由 tracker 校验连续)。
            let hit = match e.payload.as_ref() {
                Some(envelope::Payload::SessionSummaryBatch(b)) if snapshot => {
                    b.summaries[0].title == title
                }
                Some(envelope::Payload::EventBatch(b)) if !snapshot => b.events.iter().any(
                    |ev| matches!(
                        ev.event.as_ref(),
                        Some(domain_event::Event::SessionSummaryChanged(s)) if s.title == title
                    ),
                ),
                _ => false,
            };
            if hit {
                return e;
            }
        }
    }

    let _snap_a1 = next_summary(&mut tracker1, &mut browser1.ws, "任务A", true).await;
    // B:Subscribed(8,200) + delta(批201 → 下一全局序号)。
    bridge_b.send_subscribed(&_upstream_b, 8, 200).await;
    bridge_b
        .send(&FakeBridge::list_delta_env(
            &device_b.to_string(),
            "native-b",
            "任务B",
            201,
        ))
        .await;
    let delta_b1 = next_summary(&mut tracker1, &mut browser1.ws, "任务B", false).await;

    // 设备 A 的新快照:不覆盖 B 的事件、不重编号,活跃订阅者序号连续。
    bridge_a
        .send(&FakeBridge::list_snapshot_env(
            &device_a.to_string(),
            "native-a",
            "任务A2",
        ))
        .await;
    let snap_a2 = next_summary(&mut tracker1, &mut browser1.ws, "任务A2", true).await;
    assert!(
        snap_a2.sequence > delta_b1.sequence,
        "A snapshot must come after B delta in the single sequence space"
    );

    // 设备 B 的快照:按设备维护的最新快照,列表聚合完整。
    bridge_b
        .send(&FakeBridge::list_snapshot_env(
            &device_b.to_string(),
            "native-b",
            "任务B快照",
        ))
        .await;
    let snap_b = next_summary(&mut tracker1, &mut browser1.ws, "任务B快照", true).await;

    // 第二个浏览器订阅同一列表流:按设备快照恢复,序号连续;B 的旧 delta
    // (已送达 browser1)不得以任何序号重放。
    let ticket2 = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser2 = connect_browser(&env.relay, &ticket2).await.expect("browser2");
    browser2.send(&subscribe_list()).await;
    let subd2 = recv_env(&mut browser2.ws, Duration::from_secs(5)).await;
    let base2 = match subd2.payload {
        Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
        other => panic!("expected Subscribed for browser2, got {other:?}"),
    };
    let mut tracker2 = SeqTracker::from_base(base2);
    let replay_a = next_summary(&mut tracker2, &mut browser2.ws, "任务A2", true).await;
    let replay_b = next_summary(&mut tracker2, &mut browser2.ws, "任务B快照", true).await;
    assert_eq!(
        replay_b.sequence,
        replay_a.sequence + 1,
        "per-device snapshots replay contiguously"
    );
    tracker2
        .expect_quiet(&mut browser2.ws, Duration::from_millis(400))
        .await;

    // 两个浏览器的实时坐标继续一致连续。
    bridge_b
        .send(&FakeBridge::list_delta_env(
            &device_b.to_string(),
            "native-b",
            "任务B2",
            202,
        ))
        .await;
    let d1 = next_summary(&mut tracker1, &mut browser1.ws, "任务B2", false).await;
    let d2 = next_summary(&mut tracker2, &mut browser2.ws, "任务B2", false).await;
    assert_eq!(d1.sequence, d2.sequence, "one sequence space for all");
    assert_eq!(d1.sequence, snap_b.sequence + 1);
}

/// R2-AC01 双浏览器:一个订阅者恢复(重放)不改变另一个订阅者的实时坐标;
/// 两端序号一致,恢复后新事件连续。
#[tokio::test(flavor = "multi_thread")]
async fn second_browser_resync_does_not_disturb_first_browser_coordinates() {
    let (env, _d) = basic_env().await;
    let device_id = uuid::Uuid::new_v4();
    let (mut browser1, mut bridge, _upstream, downstream, _base, mut tracker1) =
        subscribe_list_flow(&env, &device_id.to_string(), "native-1").await;

    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-1",
            101,
        ))
        .await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-2",
            102,
        ))
        .await;
    let d1 = tracker1.recv(&mut browser1.ws).await;
    assert_delta(&d1, "delta-1");
    let d2 = tracker1.recv(&mut browser1.ws).await;
    assert_delta(&d2, "delta-2");

    // 第二个浏览器订阅既有流:完整存活窗口重放(快照 + 连续缓冲事件)。
    let ticket2 = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser2 = connect_browser(&env.relay, &ticket2).await.expect("browser2");
    browser2.send(&subscribe_list()).await;
    let subd2 = recv_env(&mut browser2.ws, Duration::from_secs(5)).await;
    let base2 = match subd2.payload {
        Some(envelope::Payload::Subscribed(s)) => s.base_sequence,
        other => panic!("expected Subscribed for browser2, got {other:?}"),
    };
    let mut tracker2 = SeqTracker::from_base(base2);
    let is_delta = |e: &Envelope, title: &str| {
        matches!(
            e.payload.as_ref(),
            Some(envelope::Payload::EventBatch(b))
                if b.events.len() == 1
                    && matches!(
                        b.events[0].event.as_ref(),
                        Some(domain_event::Event::SessionSummaryChanged(s)) if s.title == title
                    )
        )
    };
    // 重放序列 = 存活帧(可能夹 presence),序号连续;等快照 → delta-1 → delta-2。
    let mut stage = 0;
    let p2 = loop {
        let e = tracker2.recv(&mut browser2.ws).await;
        if is_delta(&e, "delta-1") && stage <= 1 {
            stage = 2;
            continue;
        }
        if is_delta(&e, "delta-2") && stage == 2 {
            break e;
        }
        match e.payload {
            Some(envelope::Payload::SessionSummaryBatch(ref b)) if b.snapshot && stage == 0 => {
                stage = 1;
            }
            Some(envelope::Payload::EventBatch(_)) => {} // presence:序号已校验
            other => panic!("unexpected replay frame at stage {stage}: {other:?}"),
        }
    };
    assert!(
        stage >= 2,
        "replay must cover snapshot then buffered deltas"
    );
    assert_eq!(p2.sequence, d2.sequence, "both browsers share one sequence");

    // browser1 发起恢复:按全局序号原样重放。
    browser1
        .send(&base_env(envelope::Payload::ResyncRequest(
            agent_console_protocol::v1::ResyncRequest {
                stream_id: downstream.clone(),
            },
        )))
        .await;
    let subd1 = tracker1.recv(&mut browser1.ws).await;
    match subd1.payload {
        Some(envelope::Payload::Subscribed(s)) => {
            tracker1 = SeqTracker::from_base(s.base_sequence);
        }
        other => panic!("expected Subscribed on resync, got {other:?}"),
    }
    // 重放序列 = 存活帧(可能夹 presence),序号连续;等快照 → delta-1 → delta-2。
    let mut rstage = 0;
    let q2 = loop {
        let e = tracker1.recv(&mut browser1.ws).await;
        if is_delta(&e, "delta-1") && rstage <= 1 {
            rstage = 2;
            continue;
        }
        if is_delta(&e, "delta-2") && rstage == 2 {
            break e;
        }
        match e.payload {
            Some(envelope::Payload::SessionSummaryBatch(ref b)) if b.snapshot && rstage == 0 => {
                rstage = 1; // 首帧序号连续性已由 tracker 断言(base 起,快照占据 base)。
            }
            Some(envelope::Payload::EventBatch(_)) => {} // presence:序号已校验
            other => {
                panic!(
                    "unexpected resync frame at stage {rstage} seq={}: {other:?}",
                    e.sequence
                );
            }
        }
    };
    assert_eq!(q2.sequence, d2.sequence, "resync replays global sequences");

    // 恢复不得影响 browser2 的实时坐标:短窗口内无任何流帧。
    tracker2
        .expect_quiet(&mut browser2.ws, Duration::from_millis(400))
        .await;

    // 恢复后新事件:两端同序号连续到达。
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_id.to_string(),
            "native-1",
            "delta-3",
            103,
        ))
        .await;
    let n1 = tracker1.recv(&mut browser1.ws).await;
    assert_delta(&n1, "delta-3");
    assert_eq!(n1.sequence, q2.sequence + 1);
    let n2 = tracker2.recv(&mut browser2.ws).await;
    assert_delta(&n2, "delta-3");
    assert_eq!(n2.sequence, n1.sequence, "one sequence space for all");
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

async fn basic_env() -> (Env, ()) {
    let env = setup(&[]).await;
    (env, ())
}

/// 客户端序号追踪器:模拟前端对同 stream+epoch 内 sequence 连续性的校验
/// (§17.5)。presence 等流帧同样占序号,一律参与连续断言;控制帧
/// (Subscribed/ResyncRequired,sequence=0)原样返回由断言方匹配。
struct SeqTracker {
    /// 下一帧期待序号,自 Subscribed.base 起算。首帧允许两种合法窗口形态:
    /// 快照占据 base(纯快照窗口,§17.4 步骤 5)或首帧未覆盖事件在
    /// base+1(快照前有缓冲 presence 等,base=首帧-1);此后严格 +1。
    expect: u64,
    first_frame: bool,
}

impl SeqTracker {
    fn from_base(base: u64) -> Self {
        Self {
            expect: base,
            first_frame: true,
        }
    }

    /// 收下一帧并断言序号连续;跳过 Relay 周期心跳帧。
    async fn recv(&mut self, ws: &mut Ws) -> Envelope {
        loop {
            let e = recv_env(ws, Duration::from_secs(5)).await;
            if matches!(e.payload, Some(envelope::Payload::Heartbeat(_))) {
                continue;
            }
            if e.sequence == 0 {
                return e;
            }
            if self.first_frame && e.sequence == self.expect + 1 {
                // 窗口以未覆盖事件帧开头:base = 首帧-1。
                self.first_frame = false;
                self.expect = e.sequence + 1;
                return e;
            }
            self.first_frame = false;
            assert_eq!(
                e.sequence, self.expect,
                "downstream sequence must be contiguous (no fabricated gap)"
            );
            self.expect += 1;
            return e;
        }
    }

    /// 短窗口内不应再有流帧(重放/覆盖窗口排空);控制帧忽略。
    async fn expect_quiet(&mut self, ws: &mut Ws, window: Duration) {
        let deadline = std::time::Instant::now() + window;
        loop {
            let wait = deadline.saturating_duration_since(std::time::Instant::now());
            if wait.is_zero() {
                return;
            }
            match tokio::time::timeout(wait, futures::StreamExt::next(ws)).await {
                Ok(Some(Ok(Message::Binary(bytes)))) => {
                    if let Ok(e) = decode_envelope(&bytes) {
                        if e.sequence == 0
                            || matches!(e.payload, Some(envelope::Payload::Heartbeat(_)))
                        {
                            continue;
                        }
                        panic!(
                            "unexpected stream frame seq={} (expected quiet window)",
                            e.sequence
                        );
                    }
                }
                Ok(Some(Ok(_))) => {}
                _ => return,
            }
        }
    }
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

/// 上游不阻塞(§17.6):Bridge 仍可心跳并收到 HeartbeatAck。
/// 跳过可能排队的心跳/其他帧,等待 HeartbeatAck。
async fn assert_bridge_alive(bridge: &mut FakeBridge) {
    bridge
        .send(&base_env(envelope::Payload::Heartbeat(
            agent_console_protocol::v1::Heartbeat {},
        )))
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
