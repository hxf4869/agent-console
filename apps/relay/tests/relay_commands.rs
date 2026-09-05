//! Relay 命令通道与回执集成测试(§15/§29.2):
//! - CommandRequest → Bridge → CommandAccepted(ACCEPTED_BY_BRIDGE 才回执)→ CommandResult
//! - request_receipts 状态流转(RECEIVED → ACCEPTED_BY_BRIDGE → COMPLETED)
//! - 相同 request_id 重试返回已有回执,不重发
//! - 设备离线 → 拒绝写命令 DEVICE_OFFLINE
//! - introspection 撤销后 Browser WS 关闭;dev-toolbox 不可达宽限策略(§20.5)

mod support;

use std::time::Duration;

use agent_console_protocol::codec::decode_envelope;
use agent_console_protocol::v1::{envelope, Envelope};
use tokio_tungstenite::tungstenite::Message;

use support::*;

fn command_request_env(device_id: &str, native: &str, request_id: &str) -> Envelope {
    base_env(envelope::Payload::CommandRequest(
        agent_console_protocol::v1::CommandRequest {
            request_id: request_id.to_string(),
            operation: agent_console_protocol::v1::Operation::StartTurn as i32,
            session_key: Some(agent_console_protocol::v1::SessionKey {
                device_id: device_id.to_string(),
                agent_kind: 1,
                native_session_id: native.to_string(),
                relay_session_uuid: String::new(),
            }),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: "deadbeef".to_string(),
            payload: Some(
                agent_console_protocol::v1::command_request::Payload::StartTurn(
                    agent_console_protocol::v1::StartTurnPayload {
                        prompt: "hi".to_string(),
                    },
                ),
            ),
        },
    ))
}

/// 建立 bridge + browser 并让会话摘要入库(命令路由需要会话存在)。
async fn ready_flow(env: &Env, native: &str) -> (FakeBridge, FakeBrowser, String) {
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let device_str = device_id.to_string();
    let mut bridge = FakeBridge::connect(&env.relay, &device_str, &credential)
        .await
        .expect("bridge");
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");

    // 通过列表订阅让 Bridge 上报摘要(建立 session_summaries 行)。
    browser.send(&subscribe_list()).await;
    let sub = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
    let upstream = sub.stream_id.clone();
    bridge.send_subscribed(&upstream, 1, 0).await;
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_str,
            native,
            "命令测试会话",
        ))
        .await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    (bridge, browser, device_str)
}

#[tokio::test(flavor = "multi_thread")]
async fn command_receipt_flow_and_retry_dedup() {
    let env = setup(&[]).await;
    let (mut bridge, mut browser, device_str) = ready_flow(&env, "native-cmd").await;

    // 1. Browser 发送命令。
    let request_id = uuid::Uuid::new_v4().to_string();
    browser
        .send(&command_request_env(&device_str, "native-cmd", &request_id))
        .await;

    // 2. Relay 转发 Bridge(先落 RECEIVED 回执);跳过 Relay 周期心跳。
    let forwarded = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
    match forwarded.payload {
        Some(envelope::Payload::CommandRequest(cr)) => {
            assert_eq!(cr.request_id, request_id);
            assert_eq!(
                cr.operation,
                agent_console_protocol::v1::Operation::StartTurn as i32
            );
        }
        other => panic!("expected forwarded CommandRequest, got {other:?}"),
    }
    assert_eq!(forwarded.correlation_id, request_id);

    // 3. Bridge 回 CommandAccepted(ACCEPTED_BY_BRIDGE)→ 浏览器收到(§15.2)。
    let accepted = base_env(envelope::Payload::CommandAccepted(
        agent_console_protocol::v1::CommandAccepted {
            request_id: request_id.clone(),
            status: agent_console_protocol::v1::CommandReceiptStatus::ReceiptAcceptedByBridge
                as i32,
            accepted_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        },
    ));
    bridge.send(&accepted).await;
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match got.payload {
        Some(envelope::Payload::CommandAccepted(a)) => {
            assert_eq!(
                a.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptAcceptedByBridge as i32
            );
        }
        other => panic!("expected CommandAccepted, got {other:?}"),
    }

    // 4. Bridge 先回中间态 DISPATCHED_TO_CODEX，再回终态 COMPLETED；
    // Relay 必须把两条都送到浏览器，且只能在终态移除命令跟踪。
    let dispatched = base_env(envelope::Payload::CommandResult(
        agent_console_protocol::v1::CommandResult {
            request_id: request_id.clone(),
            status: agent_console_protocol::v1::CommandReceiptStatus::ReceiptDispatchedToCodex
                as i32,
            error_code: 0,
            details: Default::default(),
            duration_ms: Some(6),
        },
    ));
    bridge.send(&dispatched).await;
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match got.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptDispatchedToCodex as i32
            );
            assert_eq!(r.error_code, 0);
        }
        other => panic!("expected dispatched CommandResult, got {other:?}"),
    }

    let result = base_env(envelope::Payload::CommandResult(
        agent_console_protocol::v1::CommandResult {
            request_id: request_id.clone(),
            status: agent_console_protocol::v1::CommandReceiptStatus::ReceiptCompleted as i32,
            error_code: 0,
            details: Default::default(),
            duration_ms: Some(12),
        },
    ));
    bridge.send(&result).await;
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match got.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptCompleted as i32
            );
        }
        other => panic!("expected CommandResult, got {other:?}"),
    }

    // 回执状态查询 API。
    let resp = http_get(
        &env.relay,
        &format!("/agent-console/api/requests/{request_id}"),
        &identity_headers(),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let mut status = String::new();
    let mut error_code = String::new();
    for _ in 0..30 {
        let body: serde_json::Value = http_get(
            &env.relay,
            &format!("/agent-console/api/requests/{request_id}"),
            &identity_headers(),
        )
        .await
        .json()
        .await
        .unwrap();
        status = body["status"].as_str().unwrap_or("").to_string();
        error_code = body["errorCode"].as_str().unwrap_or("").to_string();
        if status == "COMPLETED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(status, "COMPLETED", "receipt must reach COMPLETED");
    assert_eq!(error_code, "", "successful receipt must not carry an error");

    // 5. 相同 request_id 重试:返回已有回执,不再转发 Bridge(§15.2)。
    browser
        .send(&command_request_env(&device_str, "native-cmd", &request_id))
        .await;
    let retry_reply = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match retry_reply.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptCompleted as i32
            );
            assert_eq!(r.error_code, 0);
        }
        other => panic!("expected stored receipt, got {other:?}"),
    }

    // 6. 同一 request_id 但 payload 不同：必须在 Relay 侧拒绝为
    // DUPLICATE_REQUEST_MISMATCH，不能把旧回执当成这次请求的结果。
    let mut mismatch = command_request_env(&device_str, "native-cmd", &request_id);
    if let Some(envelope::Payload::CommandRequest(request)) = mismatch.payload.as_mut() {
        request.payload = Some(
            agent_console_protocol::v1::command_request::Payload::StartTurn(
                agent_console_protocol::v1::StartTurnPayload {
                    prompt: "different payload".to_string(),
                },
            ),
        );
        // 自报摘要同样不可信；服务端必须从 Protobuf payload 自行计算。
        request.payload_digest = "deadbeef".to_string();
    }
    browser.send(&mismatch).await;
    let mismatch_reply = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match mismatch_reply.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.error_code,
                agent_console_protocol::v1::StableErrorCode::DuplicateRequestMismatch as i32
            );
        }
        other => panic!("expected DUPLICATE_REQUEST_MISMATCH, got {other:?}"),
    }

    // Bridge 未收到第二条命令(允许中间心跳帧)。
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    loop {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            break; // 无重发 ✓
        }
        let msg = tokio::time::timeout(wait, futures::StreamExt::next(&mut bridge.ws)).await;
        let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes)))) = msg else {
            break;
        };
        if let Ok(e) = decode_envelope(&bytes) {
            assert!(
                !matches!(e.payload, Some(envelope::Payload::CommandRequest(_))),
                "retry must not be re-forwarded to bridge"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn device_offline_command_rejected() {
    let env = setup(&[]).await;
    // 无 Bridge 在线:浏览器订阅也会被拒;直接用命令通道验证 DEVICE_OFFLINE。
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");

    // 未入库的设备/会话:归属校验先拒绝(SESSION_NOT_FOUND)。
    let request_id = uuid::Uuid::new_v4().to_string();
    browser
        .send(&command_request_env(
            &uuid::Uuid::new_v4().to_string(),
            "native-x",
            &request_id,
        ))
        .await;
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match got.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptRejected as i32
            );
            assert_eq!(
                r.error_code,
                agent_console_protocol::v1::StableErrorCode::SessionNotFound as i32
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }

    // 设备存在但离线:DEVICE_OFFLINE。
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let request_id2 = uuid::Uuid::new_v4().to_string();
    browser
        .send(&command_request_env(
            &device_id.to_string(),
            "native-x",
            &request_id2,
        ))
        .await;
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    match got.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptRejected as i32
            );
            assert_eq!(
                r.error_code,
                agent_console_protocol::v1::StableErrorCode::DeviceOffline as i32
            );
        }
        other => panic!("expected DEVICE_OFFLINE, got {other:?}"),
    }
    let _ = credential;
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_disconnect_marks_pending_outcome_unknown() {
    let env = setup(&[]).await;
    let (mut bridge, mut browser, device_str) = ready_flow(&env, "native-out").await;

    let request_id = uuid::Uuid::new_v4().to_string();
    browser
        .send(&command_request_env(&device_str, "native-out", &request_id))
        .await;
    let forwarded = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        forwarded.payload,
        Some(envelope::Payload::CommandRequest(_))
    ));

    // Bridge 接受后连接中断:未拿到最终结果 → OUTCOME_UNKNOWN(§15.2/§26.4)。
    let accepted = base_env(envelope::Payload::CommandAccepted(
        agent_console_protocol::v1::CommandAccepted {
            request_id: request_id.clone(),
            status: agent_console_protocol::v1::CommandReceiptStatus::ReceiptAcceptedByBridge
                as i32,
            accepted_at: None,
        },
    ));
    bridge.send(&accepted).await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;

    drop(bridge);
    // 浏览器收到 OUTCOME_UNKNOWN 回执。
    let got = recv_skip_presence(&mut browser.ws, Duration::from_secs(10)).await;
    match got.payload {
        Some(envelope::Payload::CommandResult(r)) => {
            assert_eq!(
                r.status,
                agent_console_protocol::v1::CommandReceiptStatus::ReceiptOutcomeUnknown as i32
            );
            assert_eq!(
                r.error_code,
                agent_console_protocol::v1::StableErrorCode::OutcomeUnknown as i32
            );
        }
        other => panic!("expected OUTCOME_UNKNOWN, got {other:?}"),
    }

    // 数据库状态同步为 OUTCOME_UNKNOWN(轮询直至落库)。
    let mut db_status = String::new();
    for _ in 0..30 {
        let row: (String,) =
            sqlx::query_as("SELECT status FROM request_receipts WHERE request_id = $1")
                .bind(&request_id)
                .fetch_one(&env.pool)
                .await
                .unwrap();
        db_status = row.0;
        if db_status == "OUTCOME_UNKNOWN" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(db_status, "OUTCOME_UNKNOWN");
}

#[tokio::test(flavor = "multi_thread")]
async fn introspection_revocation_closes_browser_ws() {
    // introspect 周期压到 1s(环境变量覆盖,默认 60s;§20.5 至少每 60 秒)。
    let env = setup(&[("RELAY_INTROSPECT_SECS", "1")]).await;
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");
    // 心跳确认连接活跃。
    browser
        .send(&base_env(envelope::Payload::Heartbeat(
            agent_console_protocol::v1::Heartbeat {},
        )))
        .await;
    let ack = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    assert!(matches!(
        ack.payload,
        Some(envelope::Payload::HeartbeatAck(_))
    ));

    // 撤销 auth session → 连接在下一个 introspection 周期内以稳定 close reason 关闭。
    env.toolbox.revoke_session(AUTH_SESSION);
    let close = recv_close(&mut browser.ws, Duration::from_secs(10)).await;
    assert_eq!(
        close.1, "AUTH_EXPIRED",
        "stable close reason for revoked session"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_backend_unavailable_grace_blocks_writes_then_closes() {
    let env = setup(&[
        ("RELAY_INTROSPECT_SECS", "1"),
        ("RELAY_AUTH_GRACE_SECS", "2"),
    ])
    .await;
    let device_str;
    let mut browser;
    {
        let (bridge, b, ds) = ready_flow(&env, "native-grace").await;
        let bridge = bridge;
        let _ = bridge;
        device_str = ds;
        browser = b;
    }
    let _ = device_str;

    // dev-toolbox 不可达 → 宽限期内写命令被拒(INTERNAL_ERROR + reason)。
    env.toolbox.set_unavailable(true);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    // 注意:设备此刻在线状态无关紧要;写命令在降级检查即被拒。
    browser
        .send(&command_request_env(
            &device_id.to_string(),
            "native-grace",
            &uuid::Uuid::new_v4().to_string(),
        ))
        .await;
    // 宽限期内:读帧直到拿到降级拒绝或连接被宽限结束关闭(两者皆为合法时序)。
    let mut saw_rejection = false;
    let mut close_reason: Option<(u16, String)> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while close_reason.is_none() && !saw_rejection {
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            panic!("neither rejection nor close within deadline");
        }
        let msg = tokio::time::timeout(wait, futures::StreamExt::next(&mut browser.ws)).await;
        match msg {
            Err(_) => panic!("timeout waiting for degraded rejection or close"),
            Ok(Some(Err(e))) => panic!("ws error: {e}"),
            Ok(None) => panic!("ws closed without frame"),
            Ok(Some(Ok(Message::Close(frame)))) => {
                let f = frame.expect("close frame");
                close_reason = Some((u16::from(f.code), f.reason.to_string()));
            }
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                if let Ok(e) = decode_envelope(&bytes) {
                    if let Some(envelope::Payload::CommandResult(r)) = e.payload.as_ref() {
                        if r.error_code
                            == agent_console_protocol::v1::StableErrorCode::InternalError as i32
                            && r.details.get("reason").map(|s| s.as_str())
                                == Some("AUTH_BACKEND_UNAVAILABLE")
                        {
                            saw_rejection = true;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_rejection
            || close_reason
                .as_ref()
                .map(|(_, r)| r == "AUTH_EXPIRED")
                .unwrap_or(false),
        "degraded rejection or grace-exhausted close expected"
    );
    if saw_rejection && close_reason.is_none() {
        // 拒绝后连接仍在,直到宽限结束被关闭(§20.5);容忍带/不带 Close 帧的关闭。
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let wait = deadline.saturating_duration_since(std::time::Instant::now());
            if wait.is_zero() {
                panic!("connection not closed after grace");
            }
            match tokio::time::timeout(wait, futures::StreamExt::next(&mut browser.ws)).await {
                Err(_) => panic!("connection not closed after grace (timeout)"),
                Ok(None) => break, // socket 关闭 ✓
                Ok(Some(Err(_))) => break,
                Ok(Some(Ok(m))) => {
                    if matches!(m, Message::Close(_)) {
                        break;
                    }
                }
            }
        }
    }
    let _ = credential;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_list_pagination_and_prefs() {
    let env = setup(&[]).await;
    let (mut bridge, mut browser, device_str) = ready_flow(&env, "native-page").await;

    // 多个会话摘要。
    let upstream = {
        // ready_flow 已完成一次订阅;再推送两个不同会话的 snapshot(delta 不足以新增行,用 snapshot=true)。
        let summaries: Vec<agent_console_protocol::v1::SessionSummary> = ["a", "b", "c"]
            .iter()
            .map(|n| session_summary(&device_str, &format!("native-{n}"), &format!("任务{n}")))
            .collect();
        let mut msg = base_env(envelope::Payload::SessionSummaryBatch(
            agent_console_protocol::v1::SessionSummaryBatch {
                summaries,
                snapshot: true,
            },
        ));
        msg.stream_id = format!("u-{device_str}-list");
        msg.device_id = device_str.clone();
        bridge.send(&msg).await;
        // 快照会触发 flush → Subscribed + snapshot;丢弃到达帧即可。
        tokio::time::sleep(Duration::from_millis(300)).await;
        while try_recv_env(&mut browser.ws, Duration::from_millis(200))
            .await
            .is_some()
        {}
        format!("u-{device_str}-list")
    };
    let _ = upstream;

    // 列表 API:3 条。
    let resp = http_get(
        &env.relay,
        "/agent-console/api/sessions?limit=2",
        &identity_headers(),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["sessions"].as_array().unwrap().len(), 2, "limit=2");
    let cursor = body["nextCursor"]
        .as_str()
        .expect("cursor present")
        .to_string();

    // 第二页。
    let resp = http_get(
        &env.relay,
        &format!("/agent-console/api/sessions?limit=2&cursor={cursor}"),
        &identity_headers(),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let second_page = body["sessions"].as_array().unwrap().len();
    assert_eq!(second_page, 2, "remaining page (4 sessions total)");

    // pin 偏好(Relay 自身偏好,§12)。
    let sessions: serde_json::Value = http_get(
        &env.relay,
        "/agent-console/api/sessions",
        &identity_headers(),
    )
    .await
    .json()
    .await
    .unwrap();
    let sid = sessions["sessions"][0]["id"].as_str().unwrap().to_string();
    let native0 = sessions["sessions"][0]["nativeSessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = reqwest::Client::new()
        .patch(format!(
            "{}/agent-console/api/sessions/{sid}",
            env.relay.base
        ))
        .header("X-Agent-Console-Session-Id", AUTH_SESSION.to_string())
        .header("X-Agent-Console-Owner-Id", OWNER.to_string())
        .header(
            "X-Agent-Console-Session-Expires",
            (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        )
        .json(&serde_json::json!({"pinned": true, "archived": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // archived 过滤。
    let resp = http_get(
        &env.relay,
        "/agent-console/api/sessions?archived=true",
        &identity_headers(),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body["sessions"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["nativeSessionId"].as_str(), Some(native0.as_str()));
    assert_eq!(arr[0]["pinned"], serde_json::json!(true));
}
