//! Relay 认证与设备面集成测试(§29.2):
//! - 迁移从空库成功
//! - Bridge 握手与协议版本协商
//! - ticket 无效/过期/已消费拒绝;可信代理头信任与剥离
//! - 凭据撤销后 WSS 立即断开、重连拒绝
//! - pairing 短码过期/重放/尝试上限/并发批准单赢家
//! - 日志捕获:敏感头/ticket/凭据不出现在日志输出

mod support;

use std::time::Duration;

use agent_console_protocol::v1::envelope;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn migrations_apply_from_empty_db() {
    let env = setup(&[]).await;
    for table in [
        "devices",
        "pairing_challenges",
        "session_summaries",
        "request_receipts",
        "push_subscriptions",
        "notification_settings",
        "audit_events",
    ] {
        let n: (i64,) = sqlx::query_as(&format!(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"
        ))
        .fetch_one(&env.pool)
        .await
        .expect("query");
        assert_eq!(n.0, 1, "table {table} must exist");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_handshake_and_version_negotiation() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;

    // 正常握手:ServerHello。
    let bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("handshake ok");
    drop(bridge);

    // 错误凭据:401。
    let status = FakeBridge::connect(&env.relay, &device_id.to_string(), "wrong-credential").await;
    assert!(
        matches!(status, Err(401)),
        "bad credential must be rejected"
    );

    // 版本不匹配:ProtocolError + 关闭(1002)。
    let mut request = format!("ws://{}/agent-console/bridge/ws", env.relay.addr)
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let (mut ws, _) = connect_async(request).await.expect("upgrade ok");
    // 发送不匹配版本的 ClientHello。
    let mut env_msg = base_env(envelope::Payload::ClientHello(
        agent_console_protocol::v1::ClientHello {
            protocol_version: 99,
            client_kind: BRIDGE,
            device_id: device_id.to_string(),
            auth_subject: String::new(),
            capabilities: vec![],
        },
    ));
    env_msg.device_id = device_id.to_string();
    send_env(&mut ws, &env_msg).await;
    let reply = recv_env(&mut ws, Duration::from_secs(5)).await;
    match reply.payload {
        Some(envelope::Payload::ProtocolError(e)) => {
            assert_eq!(
                e.error_code,
                agent_console_protocol::v1::StableErrorCode::InternalError as i32
            );
        }
        other => panic!("expected ProtocolError, got {other:?}"),
    }
    let close = recv_close(&mut ws, Duration::from_secs(5)).await;
    assert_eq!(close.0, 1002, "version mismatch closes with 1002");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_ws_ticket_validation() {
    let log_path = std::env::temp_dir().join(format!(
        "relay-it-ticket-{}.txt",
        uuid::Uuid::new_v4().simple()
    ));
    let env = setup_with_log_file(&[], &log_path).await;

    // 有效 ticket:握手成功。
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(30));
    let browser = match connect_browser(&env.relay, &ticket).await {
        Ok(b) => b,
        Err(status) => {
            let logs = std::fs::read_to_string(&log_path).unwrap_or_default();
            panic!("valid ticket: {status}; relay logs: {logs}");
        }
    };
    drop(browser);

    // 同一 ticket 重放:WS_TICKET_CONSUMED(401)。
    let again = connect_browser(&env.relay, &ticket).await;
    assert!(matches!(again, Err(401)), "consumed ticket rejected");

    // 未知 ticket:401。
    let unknown = connect_browser(&env.relay, "tk-unknown").await;
    assert!(matches!(unknown, Err(401)));

    // 过期 ticket:401。
    let expired = env.toolbox.issue_ticket(chrono::Duration::seconds(-1));
    let got = connect_browser(&env.relay, &expired).await;
    assert!(matches!(got, Err(401)));
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_proxy_header_trust_and_strip() {
    let env = setup(&[]).await;
    let headers = identity_headers();

    // 可信来源(loopback 默认可信)+ 身份头 → 200。
    let resp = http_get(&env.relay, "/agent-console/api/devices", &headers).await;
    assert_eq!(resp.status(), 200, "trusted identity headers accepted");

    // 缺失身份头 → 401 AUTH_REQUIRED。
    let resp = http_get(&env.relay, "/agent-console/api/devices", &[]).await;
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "AUTH_REQUIRED");

    // 过期身份头 → 401 AUTH_EXPIRED。
    let expired_headers = vec![
        ("X-Agent-Console-Session-Id", AUTH_SESSION.to_string()),
        ("X-Agent-Console-Owner-Id", OWNER.to_string()),
        (
            "X-Agent-Console-Session-Expires",
            (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339(),
        ),
    ];
    let resp = http_get(&env.relay, "/agent-console/api/devices", &expired_headers).await;
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "AUTH_EXPIRED");

    // 不可信来源(TRUSTED_PROXY_CIDRS 清空后 loopback 也不可信)→ 头剥离,视为未认证。
    let env2 = setup(&[("TRUSTED_PROXY_CIDRS", "")]).await;
    let resp = http_get(
        &env2.relay,
        "/agent-console/api/devices",
        &identity_headers(),
    )
    .await;
    assert_eq!(
        resp.status(),
        401,
        "untrusted source headers must be stripped"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "AUTH_REQUIRED");
}

#[tokio::test(flavor = "multi_thread")]
async fn credential_revocation_disconnects_and_blocks_reconnect() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("connected");

    // 撤销设备(§21.9:立即关闭其 socket)。
    let resp = http_get(
        &env.relay,
        "/agent-console/api/devices",
        &identity_headers(),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["devices"].as_array().unwrap().len(), 1);

    let resp = reqwest::Client::new()
        .delete(format!(
            "{}/agent-console/api/devices/{}",
            env.relay.base, device_id
        ))
        .header("X-Agent-Console-Session-Id", AUTH_SESSION.to_string())
        .header("X-Agent-Console-Owner-Id", OWNER.to_string())
        .header(
            "X-Agent-Console-Session-Expires",
            (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 已连接 socket 被立即关闭。
    let close = recv_close(&mut bridge.ws, Duration::from_secs(5)).await;
    assert_eq!(close.0, 1008, "revoked device closed with policy code");

    // 重连被拒绝:DEVICE_REVOKED(401)。
    let status = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential).await;
    assert!(matches!(status, Err(401)));
}

// ---------------------------------------------------------------------------
// pairing v2(Bridge-first,§21)
// ---------------------------------------------------------------------------

/// 43 字符 base64url challenge(与 Bridge 侧生成一致)。
fn new_challenge() -> String {
    relay::state::high_entropy_token()
}

/// Bridge register:发起挑战。
async fn bridge_register(
    client: &reqwest::Client,
    base: &str,
    challenge: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/agent-console/bridge/pairing/register"))
        .json(&serde_json::json!({
            "challenge": challenge,
            "deviceName": "Mac Studio",
            "platform": "darwin",
            "arch": "arm64",
            "bridgeVersion": "0.1.0",
        }))
        .send()
        .await
        .unwrap()
}

/// Bridge claim:轮询领取凭据。
async fn bridge_claim(
    client: &reqwest::Client,
    base: &str,
    challenge_id: &str,
    challenge: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/agent-console/bridge/pairing/claim"))
        .json(&serde_json::json!({
            "challengeId": challenge_id,
            "challenge": challenge,
        }))
        .send()
        .await
        .unwrap()
}

/// 浏览器 lookup:短码查设备信息。
async fn browser_lookup(
    client: &reqwest::Client,
    base: &str,
    auth: &[(&'static str, String)],
    short_code: &str,
) -> reqwest::Response {
    let mut req = client.post(format!("{base}/agent-console/api/pairing/lookup"));
    for (k, v) in auth {
        req = req.header(*k, v);
    }
    req.json(&serde_json::json!({ "shortCode": short_code }))
        .send()
        .await
        .unwrap()
}

/// 浏览器 approve:短码批准(可带 challengeId 归因错误计数)。
async fn browser_approve(
    client: &reqwest::Client,
    base: &str,
    auth: &[(&'static str, String)],
    short_code: &str,
    challenge_id: Option<&str>,
) -> reqwest::Response {
    let mut body = serde_json::json!({ "shortCode": short_code });
    if let Some(id) = challenge_id {
        body["challengeId"] = serde_json::json!(id);
    }
    let mut req = client.post(format!("{base}/agent-console/api/pairing/approve"));
    for (k, v) in auth {
        req = req.header(*k, v);
    }
    req.json(&body).send().await.unwrap()
}

/// register → (lookup → approve) → 返回 (challenge 明文, 短码, challengeId)。
async fn register_and_lookup(
    env: &Env,
    client: &reqwest::Client,
    auth: &[(&'static str, String)],
) -> (String, String, String) {
    let challenge = new_challenge();
    let resp = bridge_register(&client, &env.relay.base, &challenge).await;
    assert_eq!(resp.status(), 200, "register must succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    let challenge_id = body["challengeId"].as_str().unwrap().to_string();
    let short_code = body["shortCode"].as_str().unwrap().to_string();
    assert_eq!(short_code.len(), 6, "short code is 6 digits");

    let resp = browser_lookup(client, &env.relay.base, auth, &short_code).await;
    assert_eq!(resp.status(), 200, "lookup with correct code must succeed");
    let found: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(found["challengeId"].as_str().unwrap(), challenge_id);
    assert_eq!(found["deviceName"].as_str().unwrap(), "Mac Studio");
    (challenge, short_code, challenge_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_v2_bridge_first_full_flow() {
    let log_path = std::env::temp_dir().join(format!(
        "relay-it-pairing-{}.txt",
        uuid::Uuid::new_v4().simple()
    ));
    let mut env = setup_with_log_file(&[], &log_path).await;
    let client = reqwest::Client::new();
    let auth = identity_headers();

    // 旧路径不再注册(单一路由合同,不留双份)。
    for (path, method) in [
        ("/internal/pairing/register", "post"),
        ("/internal/pairing/complete", "post"),
        ("/internal/bridge/ws", "get"),
    ] {
        let resp = match method {
            "post" => {
                client
                    .post(format!("{}{path}", env.relay.base))
                    .send()
                    .await
            }
            _ => client.get(format!("{}{path}", env.relay.base)).send().await,
        }
        .unwrap();
        assert_eq!(resp.status(), 404, "{path} must be gone");
    }

    // 1. Bridge register → 短码明文返回(§21.2 CLI 输出通道)。
    let (challenge, short_code, challenge_id) = register_and_lookup(&env, &client, &auth).await;

    // 2. 批准前 claim → pending(轮询语义)。
    let resp = bridge_claim(&client, &env.relay.base, &challenge_id, &challenge).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "pending", "unapproved challenge is pending");

    // 3. approve(单赢家;owner 批准时回填)。
    let resp = browser_approve(
        &client,
        &env.relay.base,
        &auth,
        &short_code,
        Some(&challenge_id),
    )
    .await;
    assert_eq!(resp.status(), 200, "approve must succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["challengeId"].as_str().unwrap(), challenge_id);
    assert!(body["approvedAt"].as_str().is_some());

    // owner 待处理列表:已批准待领取。
    let resp = http_get(&env.relay, "/agent-console/api/pairing/challenges", &auth).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["challenges"].as_array().unwrap().len(), 1);

    // 4. claim → ready:单次交付 deviceId + deviceCredential(§21.6)。
    let resp = bridge_claim(&client, &env.relay.base, &challenge_id, &challenge).await;
    assert_eq!(
        resp.status(),
        200,
        "claim after approve delivers credential"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");
    let device_id = body["deviceId"].as_str().unwrap().to_string();
    let credential = body["deviceCredential"].as_str().unwrap().to_string();
    assert_eq!(credential.len(), 43, "credential is 32B base64url");

    // 5. 已发放再次 claim → 409 CONSUMED。
    let resp = bridge_claim(&client, &env.relay.base, &challenge_id, &challenge).await;
    assert_eq!(resp.status(), 409, "replay claim rejected");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["details"]["reason"], "CONSUMED");

    // 6. 库内无明文:pairing 行与设备行只含摘要(§21.8/§25)。
    let challenge_uuid = uuid::Uuid::parse_str(&challenge_id).unwrap();
    let pairing_json: (serde_json::Value,) =
        sqlx::query_as("SELECT to_jsonb(t) FROM pairing_challenges t WHERE id = $1")
            .bind(challenge_uuid)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    let pairing_str = pairing_json.0.to_string();
    assert!(
        !pairing_str.contains(&challenge),
        "challenge plaintext must not be stored"
    );
    assert!(
        !pairing_str.contains(&short_code),
        "short code plaintext must not be stored"
    );
    let device_uuid = uuid::Uuid::parse_str(&device_id).unwrap();
    let device_json: (serde_json::Value,) =
        sqlx::query_as("SELECT to_jsonb(t) FROM devices t WHERE id = $1")
            .bind(device_uuid)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    let device_str = device_json.0.to_string();
    assert!(
        !device_str.contains(&credential),
        "credential plaintext must not be stored"
    );
    let expected_digest = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(credential.as_bytes()))
    };
    assert!(device_str.contains(&expected_digest), "digest stored only");

    // 7. 新凭据可连 Bridge WS;撤销后连接被关闭(DEVICE_REVOKED)且重连 401。
    let mut bridge = FakeBridge::connect(&env.relay, &device_id, &credential)
        .await
        .expect("paired credential works");
    let mut revoke = client.delete(format!(
        "{}/agent-console/api/devices/{}",
        env.relay.base, device_id
    ));
    for (k, v) in &auth {
        revoke = revoke.header(*k, v);
    }
    let resp = revoke.send().await.unwrap();
    assert_eq!(resp.status(), 204, "revoke paired device");
    let (code, reason) = recv_close(&mut bridge.ws, Duration::from_secs(5)).await;
    assert_eq!(code, 1008);
    assert_eq!(reason, "DEVICE_REVOKED", "stable close reason");
    drop(bridge);
    let status = FakeBridge::connect(&env.relay, &device_id, &credential).await;
    assert!(matches!(status, Err(401)), "revoked credential rejected");

    // 8. 日志无明文(§25.3):challenge/短码/凭据不出现。
    tokio::time::sleep(Duration::from_millis(800)).await;
    env.relay.kill();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let logs = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(!logs.is_empty(), "expected some log output");
    assert!(!logs.contains(&challenge), "challenge must not be logged");
    assert!(!logs.contains(&short_code), "short code must not be logged");
    assert!(!logs.contains(&credential), "credential must not be logged");
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_v2_lock_expiry_and_rate_limits() {
    let env = setup(&[]).await;
    let client = reqwest::Client::new();
    let auth = identity_headers();

    // 1. 短码错误 5 次 → 锁定(§21.5)。
    let (_, short_code, challenge_id) = register_and_lookup(&env, &client, &auth).await;
    for _ in 0..5 {
        let resp = browser_approve(
            &client,
            &env.relay.base,
            &auth,
            "000000",
            Some(&challenge_id),
        )
        .await;
        assert_eq!(resp.status(), 401, "wrong code mismatch counted");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["details"]["reason"], "CODE_MISMATCH");
    }
    // 第 6 次:即使短码正确也被锁定拒绝。
    let resp = browser_approve(
        &client,
        &env.relay.base,
        &auth,
        &short_code,
        Some(&challenge_id),
    )
    .await;
    assert_eq!(resp.status(), 429, "locked after 5 wrong attempts");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "RATE_LIMITED");
    assert_eq!(body["error"]["details"]["reason"], "LOCKED");
    // 锁定后 lookup 同样 429。
    let resp = browser_lookup(&client, &env.relay.base, &auth, &short_code).await;
    assert_eq!(resp.status(), 429, "locked challenge lookup rejected");

    // 2. TTL 过期:lookup → 404 EXPIRED;claim → 410(§21:5 分钟有效期)。
    let (challenge, short_code, challenge_id) = register_and_lookup(&env, &client, &auth).await;
    sqlx::query(
        "UPDATE pairing_challenges SET expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(&challenge_id).unwrap())
    .execute(&env.pool)
    .await
    .unwrap();
    let resp = browser_lookup(&client, &env.relay.base, &auth, &short_code).await;
    assert_eq!(resp.status(), 404, "expired challenge lookup is 404");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["details"]["reason"], "EXPIRED");
    let resp = bridge_claim(&client, &env.relay.base, &challenge_id, &challenge).await;
    assert_eq!(resp.status(), 410, "expired challenge claim is 410");

    // 3. 短码错误 lookup → 404 NOT_FOUND。
    let resp = browser_lookup(&client, &env.relay.base, &auth, "999999").await;
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["details"]["reason"], "NOT_FOUND");

    // 4. register 来源限速:窗口内第 11 次 → 429(§21:10 次/5 分钟)。
    //    本测试此前已 register 2 次,再发 8 次成功后第 11 次超限。
    for _ in 0..8 {
        let resp = bridge_register(&client, &env.relay.base, &new_challenge()).await;
        assert_eq!(resp.status(), 200);
    }
    let resp = bridge_register(&client, &env.relay.base, &new_challenge()).await;
    assert_eq!(resp.status(), 429, "register rate limited");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "RATE_LIMITED");

    // 5. claim 来源限速(防暴力):601 次无效 claim 后 → 429。
    let mut last = None;
    for i in 0..601 {
        let resp = bridge_claim(
            &client,
            &env.relay.base,
            &uuid::Uuid::new_v4().to_string(),
            &new_challenge(),
        )
        .await;
        last = Some(resp.status());
        if resp.status() == 429 {
            let _ = i;
            break;
        }
    }
    assert_eq!(last, Some(reqwest::StatusCode::TOO_MANY_REQUESTS));

    // 6. lookup 来源限速:31 次错误短码 lookup 后 → 429。
    //    (register 已超限不影响独立 lookup 计数器。)
    let mut last = None;
    for _ in 0..31 {
        let resp = browser_lookup(&client, &env.relay.base, &auth, "888888").await;
        last = Some(resp.status());
        if resp.status() == 429 {
            break;
        }
    }
    assert_eq!(last, Some(reqwest::StatusCode::TOO_MANY_REQUESTS));
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_v2_concurrent_approve_single_winner() {
    let env = setup(&[]).await;
    let client = reqwest::Client::new();
    let auth = identity_headers();

    let (challenge, short_code, challenge_id) = register_and_lookup(&env, &client, &auth).await;

    // 并发 N 个 approve:单赢家(原子 UPDATE ... RETURNING,§21)。
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let c = client.clone();
        let base = env.relay.base.clone();
        let auth = identity_headers();
        let code = short_code.clone();
        let cid = challenge_id.clone();
        tasks.push(tokio::spawn(async move {
            browser_approve(&c, &base, &auth, &code, Some(&cid))
                .await
                .status()
        }));
    }
    let mut ok = 0;
    let mut conflict = 0;
    for t in tasks {
        match t.await.unwrap() {
            reqwest::StatusCode::OK => ok += 1,
            reqwest::StatusCode::CONFLICT => conflict += 1,
            other => panic!("unexpected approve status {other}"),
        }
    }
    assert_eq!(ok, 1, "exactly one approve wins");
    assert_eq!(conflict, 3, "losers get ALREADY_APPROVED conflict");

    // 唯一赢家批准后 Bridge 可正常领取。
    let resp = bridge_claim(&client, &env.relay.base, &challenge_id, &challenge).await;
    assert_eq!(resp.status(), 200, "winner approval allows claim");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_v2_browser_endpoints_require_identity() {
    let env = setup(&[]).await;
    let client = reqwest::Client::new();

    // 浏览器配对端点:无身份头 → 401 AUTH_REQUIRED(网关 forward-auth 语义)。
    let cases: Vec<(reqwest::Method, String, Option<serde_json::Value>)> = vec![
        (
            reqwest::Method::POST,
            "/agent-console/api/pairing/lookup".into(),
            Some(serde_json::json!({"shortCode": "123456"})),
        ),
        (
            reqwest::Method::POST,
            "/agent-console/api/pairing/approve".into(),
            Some(serde_json::json!({"shortCode": "123456"})),
        ),
        (
            reqwest::Method::GET,
            "/agent-console/api/pairing/challenges".into(),
            None,
        ),
        (
            reqwest::Method::DELETE,
            format!(
                "/agent-console/api/pairing/challenges/{}",
                uuid::Uuid::new_v4()
            ),
            None,
        ),
    ];
    for (method, path, body) in cases {
        let mut req = client
            .request(method.clone(), format!("{}{path}", env.relay.base))
            .header(
                "X-Agent-Console-Session-Id",
                uuid::Uuid::new_v4().to_string(),
            );
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), 401, "{method} {path} requires identity");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "AUTH_REQUIRED");
    }

    // Bridge 面不需要浏览器身份(无 Cookie):register 正常返回 200。
    let resp = bridge_register(&client, &env.relay.base, &new_challenge()).await;
    assert_eq!(
        resp.status(),
        200,
        "bridge register needs no browser identity"
    );

    // 旧浏览器创建路由已移除(Bridge-first 后浏览器不创建挑战;
    // 该路径仅保留 GET 列表,POST → 405)。
    let mut create = client.post(format!(
        "{}/agent-console/api/pairing/challenges",
        env.relay.base
    ));
    for (k, v) in &identity_headers() {
        create = create.header(*k, v);
    }
    let resp = create.send().await.unwrap();
    assert_eq!(resp.status(), 405, "browser create route removed");
}

#[tokio::test(flavor = "multi_thread")]
async fn sensitive_values_never_appear_in_logs() {
    let log_path = std::env::temp_dir().join(format!(
        "relay-it-log-{}.txt",
        uuid::Uuid::new_v4().simple()
    ));
    let mut env = setup_with_log_file(&[], &log_path).await;

    let device_id = uuid::Uuid::new_v4();
    let secret_credential = format!("cred-SECRET-CRED-MARKER-{}", uuid::Uuid::new_v4().simple());
    let secret_digest = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(secret_credential.as_bytes()))
    };
    insert_device(&env.pool, device_id, &secret_digest).await;
    let bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &secret_credential)
        .await
        .expect("bridge");
    drop(bridge);

    let secret_ticket = format!("TICKETSECRET-{}", uuid::Uuid::new_v4().simple());
    env.toolbox
        .seed_session(AUTH_SESSION, chrono::Duration::hours(1));
    // 直接注入带标记的 ticket(走内部 map 的等价入口)。
    env.toolbox
        .issue_ticket_with_value(&secret_ticket, chrono::Duration::seconds(30));
    let browser = connect_browser(&env.relay, &secret_ticket)
        .await
        .expect("browser");
    drop(browser);

    // 等日志落盘。
    tokio::time::sleep(Duration::from_millis(800)).await;
    env.relay.kill();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let logs = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(!logs.is_empty(), "expected some log output");
    assert!(
        !logs.contains(&secret_ticket),
        "ticket must not appear in logs"
    );
    assert!(
        !logs.contains(&secret_credential),
        "credential must not appear in logs"
    );
    assert!(
        !logs.contains("SECRET-CRED-MARKER"),
        "credential marker must not appear in logs"
    );
    assert!(
        !logs.contains("TICKETSECRET"),
        "ticket marker must not appear in logs"
    );
    assert!(
        !logs.contains("Sec-WebSocket-Protocol"),
        "subprotocol header must not be logged"
    );
    assert!(
        !logs.contains("Authorization"),
        "authorization header must not be logged"
    );
    let _ = std::fs::remove_file(&log_path);
}
