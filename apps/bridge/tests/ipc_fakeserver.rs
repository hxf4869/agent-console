//! Codex IPC 客户端集成测试:进程内 fake server(按 docs/CODEX-IPC-PROTOCOL.md 回放)。
//!
//! 不启动真实 Codex Desktop;socket 建在临时目录。不写任何会话内容:
//! 测试载荷全部是合成 ID 与占位字符串。

use std::path::PathBuf;
use std::time::Duration;

use bridge::adapter::codex::ipc::client::{
    connect, IpcClient, IpcClientConfig, IpcError, IpcEvent, StreamDecision, StreamSync,
};
use bridge::adapter::codex::ipc::frame::{encode_frame, FrameDecoder};
use bridge::adapter::codex::ipc::messages::{
    FollowingChangedParams, InputBlock, InterruptMode, InterruptTurnParams, StartTurnParams,
    StreamChange, TurnStart, TurnStartRequest,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;

fn temp_socket_path(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ac-ipc-test-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("ipc.sock")
}

struct FakeServer {
    path: PathBuf,
    /// fake server 收到的客户端帧(信封原样 JSON)。
    server_rx: mpsc::Receiver<Value>,
    /// 向客户端回发帧的通道。
    out_tx: mpsc::Sender<Value>,
}

/// 伪路由器:接受一条连接;客户端→`server_rx`,`out_tx`→客户端。
async fn spawn_fake_router(tag: &str) -> FakeServer {
    let path = temp_socket_path(tag);
    let listener = UnixListener::bind(&path).expect("bind fake ipc socket");
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(64);
    let (in_tx, in_rx) = mpsc::channel::<Value>(64);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (mut read_half, mut write_half) = stream.into_split();
        let reader_in_tx = in_tx.clone();
        tokio::spawn(async move {
            let mut decoder = FrameDecoder::new(64 * 1024 * 1024);
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                match read_half.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if decoder.push(&chunk[..n]).is_err() {
                            break;
                        }
                        while let Ok(Some(v)) = decoder.next_frame() {
                            if reader_in_tx.send(v).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });
        while let Some(v) = out_rx.recv().await {
            let raw = encode_frame(&v, 64 * 1024 * 1024).expect("encode server frame");
            if write_half.write_all(&raw).await.is_err() {
                break;
            }
        }
    });
    FakeServer {
        path,
        server_rx: in_rx,
        out_tx,
    }
}

impl FakeServer {
    async fn next_client_frame(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), self.server_rx.recv())
            .await
            .expect("timed out waiting for client frame")
            .expect("server channel closed")
    }

    async fn send_to_client(&self, v: Value) {
        self.out_tx.send(v).await.expect("send to client");
    }
}

fn client_config(path: PathBuf) -> IpcClientConfig {
    IpcClientConfig::new(path, "agent-console-bridge-test")
}

fn initialize_response(request_id: &str, client_id: &str) -> Value {
    json!({
        "type": "response",
        "requestId": request_id,
        "resultType": "success",
        "method": "initialize",
        "handledByClientId": client_id,
        "result": { "clientId": client_id }
    })
}

fn success_response(request_id: &Value, method: &str, result: Value) -> Value {
    json!({
        "type": "response",
        "requestId": request_id,
        "resultType": "success",
        "method": method,
        "handledByClientId": "owner-1",
        "result": result
    })
}

/// 连接 + 握手作为一个整体驱动(fake server 在 select 中应答 initialize)。
async fn connect_with_handshake(
    mut fake: FakeServer,
    server_client_id: &str,
) -> (FakeServer, IpcClient, mpsc::Receiver<IpcEvent>) {
    let mut cf = Box::pin(connect(client_config(fake.path.clone())));
    let init = tokio::select! {
        r = &mut cf => panic!("connect finished before handshake: {r:?}"),
        f = fake.next_client_frame() => f,
    };
    assert_eq!(init["type"], "request");
    assert_eq!(init["method"], "initialize");
    assert_eq!(init["params"]["clientType"], "agent-console-bridge-test");
    assert_eq!(init["version"], 0);
    fake.send_to_client(initialize_response(
        init["requestId"].as_str().unwrap(),
        server_client_id,
    ))
    .await;
    let (client, events) = cf.await.expect("connect");
    assert_eq!(client.client_id(), server_client_id);
    (fake, client, events)
}

async fn recv_event(events: &mut mpsc::Receiver<IpcEvent>) -> IpcEvent {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("timed out waiting for event")
        .expect("event channel closed")
}

#[tokio::test]
async fn handshake_and_request_correlation() {
    let fake = spawn_fake_router("correlation").await;
    let (mut fake, client, _events) = connect_with_handshake(fake, "self-1").await;

    // 服务端编舞与客户端请求并发驱动,验证 requestId 关联不错位。
    let server_side = async {
        let req_a = fake.next_client_frame().await;
        fake.send_to_client(success_response(
            &req_a["requestId"],
            "thread-owner-discovery",
            json!({}),
        ))
        .await;
        let req_b = fake.next_client_frame().await;
        fake.send_to_client(success_response(
            &req_b["requestId"],
            "ide-context",
            json!({}),
        ))
        .await;
        (req_a, req_b)
    };
    let ((req_a, req_b), (r1, r2)) = tokio::join!(server_side, async {
        tokio::join!(
            client.request(
                "thread-owner-discovery",
                json!({"hostId": "local", "conversationId": "01A"}),
                Default::default()
            ),
            client.request(
                "ide-context",
                json!({"workspaceRoot": "/tmp/ac-e2e"}),
                Default::default()
            )
        )
    });

    assert_eq!(req_a["method"], "thread-owner-discovery");
    assert_eq!(req_b["method"], "ide-context");
    assert_eq!(req_a["sourceClientId"], "self-1");
    // 版本规则:hostId 缺省时 = Ev 表值(非 follower 方法不受 hostId 影响)。
    assert_eq!(req_a["version"], 1);
    assert_eq!(req_b["version"], 0);
    assert!(r1.is_ok(), "owner discovery should succeed: {r1:?}");
    assert!(r2.is_ok(), "ide-context should succeed: {r2:?}");
    // handledByClientId 透出 owner clientId。
    assert_eq!(r1.unwrap().handled_by_client_id.as_deref(), Some("owner-1"));
}

#[tokio::test]
async fn peer_error_maps_to_ipc_error() {
    let fake = spawn_fake_router("peer-error").await;
    let (mut fake, client, _events) = connect_with_handshake(fake, "self-1").await;

    let server_side = async {
        let req = fake.next_client_frame().await;
        fake.send_to_client(json!({
            "type": "response",
            "requestId": req["requestId"],
            "resultType": "error",
            "error": "no-client-found"
        }))
        .await;
    };
    let ((), result) = tokio::join!(
        server_side,
        client.request(
            "thread-follower-start-turn",
            json!({"conversationId": "01A"}),
            Default::default()
        )
    );
    match result {
        Err(IpcError::Peer(msg)) => assert_eq!(msg, "no-client-found"),
        other => panic!("expected Peer error, got {other:?}"),
    }
}

#[tokio::test]
async fn owner_discovery_none_on_no_client_found() {
    let fake = spawn_fake_router("owner-none").await;
    let (mut fake, client, _events) = connect_with_handshake(fake, "self-1").await;

    let server_side = async {
        let req = fake.next_client_frame().await;
        assert_eq!(req["method"], "thread-owner-discovery");
        fake.send_to_client(json!({
            "type": "response",
            "requestId": req["requestId"],
            "resultType": "error",
            "error": "no-client-found"
        }))
        .await;
    };
    let ((), owner) = tokio::join!(server_side, client.discover_owner("local", "01A"));
    let owner = owner.expect("discovery should not fail");
    assert_eq!(owner, None);
}

#[tokio::test]
async fn follower_snapshot_then_patches_and_resync_policy() {
    let fake = spawn_fake_router("stream").await;
    let (mut fake, client, mut events) = connect_with_handshake(fake, "self-1").await;

    // 订阅(broadcast version = Ev = 1)。
    let server_side = async {
        let b = fake.next_client_frame().await;
        assert_eq!(b["type"], "broadcast");
        assert_eq!(b["method"], "thread-stream-following-changed");
        assert_eq!(b["version"], 1);
        assert_eq!(b["params"]["following"], true);
        assert_eq!(b["params"]["conversationId"], "01A");

        // owner 回发定向快照(revision 1)。
        fake.send_to_client(json!({
            "type": "broadcast",
            "method": "thread-stream-state-changed",
            "sourceClientId": "owner-1",
            "targetClientIds": ["self-1"],
            "version": 11,
            "params": {
                "conversationId": "01A",
                "hostId": "local",
                "change": {
                    "type": "snapshot",
                    "revision": 1,
                    "conversationState": {"id": "01A", "threadRuntimeStatus": {"type": "idle"}}
                }
            }
        }))
        .await;

        // 随后推送正序 patch。
        fake.send_to_client(json!({
            "type": "broadcast",
            "method": "thread-stream-state-changed",
            "sourceClientId": "owner-1",
            "version": 11,
            "params": {
                "conversationId": "01A",
                "hostId": "local",
                "change": {"type": "patches", "baseRevision": 2, "revision": 3, "patches": [
                    {"op": "replace", "path": ["title"], "value": "t"}
                ]}
            }
        }))
        .await;
    };
    let ((), follow_result) = tokio::join!(
        server_side,
        client.set_following(
            FollowingChangedParams {
                conversation_id: "01A".to_string(),
                host_id: "local".to_string(),
                following: true,
            },
            None
        )
    );
    follow_result.expect("follow should send");

    // 第一个事件:快照。
    let event = recv_event(&mut events).await;
    let IpcEvent::StreamChanged(params) = event else {
        panic!("expected StreamChanged, got {event:?}");
    };
    let mut sync = StreamSync::new();
    assert_eq!(sync.observe(&params.change), StreamDecision::TakeSnapshot);
    assert_eq!(sync.revision(), Some(1));

    // 错序 patch(base=0):策略裁决为丢弃并 Resync。
    let stale: StreamChange = serde_json::from_value(json!(
        {"type": "patches", "baseRevision": 0, "revision": 2, "patches": []}
    ))
    .unwrap();
    assert_eq!(sync.observe(&stale), StreamDecision::Resync);

    // 正序 patch(base=1):应用。
    let good: StreamChange = serde_json::from_value(json!({
        "type": "patches", "baseRevision": 1, "revision": 2,
        "patches": [{"op": "replace", "path": ["title"], "value": "t"}]
    }))
    .unwrap();
    assert_eq!(
        sync.observe(&good),
        StreamDecision::ApplyPatch {
            base_revision: 1,
            revision: 2
        }
    );

    // 第二个事件:来自 owner 的 patch 广播原样透出。
    let event = recv_event(&mut events).await;
    let IpcEvent::StreamChanged(params) = event else {
        panic!("expected StreamChanged, got {event:?}");
    };
    match params.change {
        StreamChange::Patches {
            base_revision,
            revision,
            patches,
        } => {
            assert_eq!((base_revision, revision, patches.len()), (2, 3, 1));
        }
        _ => panic!("expected patches"),
    }
}

#[tokio::test]
async fn unknown_broadcast_does_not_panic() {
    let fake = spawn_fake_router("unknown-broadcast").await;
    let (fake, _client, mut events) = connect_with_handshake(fake, "self-1").await;

    fake.send_to_client(json!({
        "type": "broadcast",
        "method": "future-mega-broadcast",
        "sourceClientId": "owner-1",
        "version": 99,
        "params": {"anything": [1, 2, 3]}
    }))
    .await;
    fake.send_to_client(json!({"type": "mystery-frame", "x": 1}))
        .await;

    let e1 = recv_event(&mut events).await;
    assert!(
        matches!(
            e1,
            IpcEvent::UnknownBroadcast { .. } | IpcEvent::Broadcast { .. }
        ),
        "未知广播透出 UnknownBroadcast 或 Broadcast,实际 {e1:?}"
    );
}

#[tokio::test]
async fn discovery_request_auto_answered_cannot_handle() {
    let fake = spawn_fake_router("discovery-auto").await;
    let (mut fake, _client, _events) = connect_with_handshake(fake, "self-1").await;

    // 模拟路由器询问本端是否可处理 ide-context。
    fake.send_to_client(json!({
        "type": "client-discovery-request",
        "requestId": "disc-1",
        "request": {"type": "request", "requestId": "inner-1", "method": "ide-context", "params": {}}
    }))
    .await;

    let answer = fake.next_client_frame().await;
    assert_eq!(answer["type"], "client-discovery-response");
    assert_eq!(answer["requestId"], "disc-1");
    assert_eq!(answer["response"]["canHandle"], false);
}

#[tokio::test]
async fn targeted_request_answered_no_handler() {
    let fake = spawn_fake_router("no-handler").await;
    let (mut fake, _client, _events) = connect_with_handshake(fake, "self-1").await;

    // 路由器把 request 定向给本端:必须显式拒绝。
    fake.send_to_client(json!({
        "type": "request",
        "requestId": "r-1",
        "sourceClientId": "router",
        "method": "thread-owner-discovery",
        "version": 1,
        "params": {"hostId": "local", "conversationId": "01A"}
    }))
    .await;

    let answer = fake.next_client_frame().await;
    assert_eq!(answer["type"], "response");
    assert_eq!(answer["resultType"], "error");
    assert_eq!(answer["error"], "no-handler-for-request");
}

#[tokio::test]
async fn write_methods_are_targeted_at_owner_with_version_rule() {
    let fake = spawn_fake_router("write-targeted").await;
    let (mut fake, client, _events) = connect_with_handshake(fake, "self-1").await;

    let server_side = async {
        let req = fake.next_client_frame().await;
        assert_eq!(req["method"], "thread-follower-start-turn");
        // Ev=2,hostId=local → 发送 3;且定向到 owner。
        assert_eq!(req["version"], 3);
        assert_eq!(req["targetClientId"], "owner-1");
        assert_eq!(req["hostId"], "local");
        assert_eq!(req["params"]["turnStart"]["request"]["threadId"], "01A");
        assert_eq!(
            req["params"]["turnStart"]["request"]["input"][0]["type"],
            "text"
        );
        fake.send_to_client(json!({
            "type": "response",
            "requestId": req["requestId"],
            "resultType": "success",
            "method": "thread-follower-start-turn",
            "handledByClientId": "owner-1",
            "result": {"result": {"turnId": "01T", "status": "inProgress"}}
        }))
        .await;

        let req2 = fake.next_client_frame().await;
        assert_eq!(req2["method"], "thread-follower-interrupt-turn");
        // interrupt:带 expectedTurnId,hostId=local → Ev(4)+1 = 5。
        assert_eq!(req2["version"], 5);
        fake.send_to_client(json!({
            "type": "response",
            "requestId": req2["requestId"],
            "resultType": "success",
            "method": "thread-follower-interrupt-turn",
            "handledByClientId": "owner-1",
            "result": {"interruptedTurnId": "01T", "ok": true}
        }))
        .await;
    };
    let ((), (start, interrupt)) = tokio::join!(server_side, async {
        let start = client.start_turn(
            "owner-1",
            StartTurnParams {
                conversation_id: "01A".into(),
                turn_start: TurnStart {
                    request: TurnStartRequest {
                        thread_id: "01A".into(),
                        input: vec![InputBlock::text("回复 ok 即可")],
                        extra: Default::default(),
                    },
                    context: None,
                },
            },
        );
        let interrupt = client.interrupt_turn(
            "owner-1",
            InterruptTurnParams {
                conversation_id: "01A".into(),
                mode: InterruptMode::UserStop,
                expected_turn_id: Some("01T".into()),
            },
        );
        (start.await, interrupt.await)
    });
    let turn = start
        .expect("start turn should succeed")
        .result
        .expect("turn payload");
    assert_eq!(turn["turnId"], "01T");
    let interrupted = interrupt.expect("interrupt should succeed");
    assert_eq!(interrupted.interrupted_turn_id, "01T");
    assert!(interrupted.ok);
}

#[tokio::test]
async fn load_complete_history_sends_host_version_plus_one() {
    let fake = spawn_fake_router("history").await;
    let (mut fake, client, _events) = connect_with_handshake(fake, "self-1").await;

    let server_side = async {
        let req = fake.next_client_frame().await;
        assert_eq!(req["method"], "thread-follower-load-complete-history");
        // hostId != null 且 follower 方法 → Ev(1)+1 = 2(与真实 Desktop 探针一致)。
        assert_eq!(req["version"], 2);
        assert_eq!(req["hostId"], "local");
        fake.send_to_client(success_response(
            &req["requestId"],
            "thread-follower-load-complete-history",
            json!({"revision": 8}),
        ))
        .await;
    };
    let ((), revision) = tokio::join!(server_side, client.load_complete_history("local", "01A"));
    assert_eq!(revision.expect("history should succeed"), 8);
}

#[tokio::test]
async fn server_disconnect_fails_pending_and_reports_state() {
    let fake = spawn_fake_router("disconnect").await;
    let (fake, client, _events) = connect_with_handshake(fake, "self-1").await;
    let mut state = client.connection_state();
    // 丢弃 fake server 侧:socket 关闭。
    drop(fake);

    let err = client
        .request("ide-context", json!({}), Default::default())
        .await
        .expect_err("requests must fail after disconnect");
    assert!(matches!(
        err,
        IpcError::Peer(_) | IpcError::ConnectionLost(_) | IpcError::NotConnected
    ));

    // 状态最终进入 Disconnected(带原因)。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let bridge::adapter::codex::ipc::client::ConnectionState::Disconnected { reason } =
            state.borrow().clone()
        {
            assert!(!reason.is_empty());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "state should become Disconnected"
        );
        if state.changed().await.is_err() {
            // sender 已 drop:客户端任务结束;再读一次终值。
            let final_state = state.borrow().clone();
            assert!(matches!(
                final_state,
                bridge::adapter::codex::ipc::client::ConnectionState::Disconnected { .. }
            ));
            break;
        }
    }
}
