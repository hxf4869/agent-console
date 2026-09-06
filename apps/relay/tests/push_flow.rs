//! Web Push 后端集成测试(§24/§29.2):
//! - 订阅 CRUD 与设置读写(默认:全部事件开、不显示 title)
//! - 触发矩阵:turn completed/failed/interrupted、等待问题、等待审批
//! - session mute 覆盖事件开关;事件开关关闭不推送
//! - 默认通用文案不含 title;showTitle 开启后含 title
//! - 相同 (session, kind) 去重窗口内不重复推送
//! - 410/404 → 订阅失效清理
//! - 未配置 push(fake sink 缺失)时禁用发送但订阅可存储

mod support;

use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use agent_console_protocol::v1::{envelope, Envelope, SessionSummaryBatch};
use support::*;

/// 轮询等待异步条件成立。
async fn until_async<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<()>>,
{
    for _ in 0..100 {
        if f().await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not met in time");
}

/// 本地 fake push sink:记录通知,可切换返回 200/410。
struct FakeSink {
    seen: Arc<Mutex<Vec<serde_json::Value>>>,
    mode: Arc<AtomicU8>, // 0=200, 1=410
}

async fn start_fake_sink() -> (String, FakeSink) {
    let seen: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let mode = Arc::new(AtomicU8::new(0));
    let state = (seen.clone(), mode.clone());
    let app = axum::Router::new().route(
        "/push/notify",
        axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
            let (seen, mode) = state.clone();
            async move {
                seen.lock().unwrap().push(body);
                if mode.load(Ordering::SeqCst) == 1 {
                    axum::http::StatusCode::GONE
                } else {
                    axum::http::StatusCode::OK
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    (format!("http://{addr}"), FakeSink { seen, mode })
}

fn header_map(headers: &[(&str, String)]) -> reqwest::header::HeaderMap {
    let mut map = reqwest::header::HeaderMap::new();
    for (k, v) in headers {
        map.insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes()).expect("header name"),
            v.parse().expect("header value"),
        );
    }
    map
}

async fn post_json(relay: &Relay, path: &str, body: serde_json::Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{path}", relay.base))
        .headers(header_map(&identity_headers()))
        .json(&body)
        .send()
        .await
        .expect("post")
}

async fn get(relay: &Relay, path: &str) -> reqwest::Response {
    http_get(relay, path, &identity_headers()).await
}

async fn put_json(relay: &Relay, path: &str, body: serde_json::Value) -> reqwest::Response {
    reqwest::Client::new()
        .put(format!("{}{path}", relay.base))
        .headers(header_map(&identity_headers()))
        .json(&body)
        .send()
        .await
        .expect("put")
}

/// 列表 delta 摘要事件(自定义终态与待处理关注);sequence 自增以通过上游去重。
fn delta_env(
    device_str: &str,
    native: &str,
    outcome: i32,
    attention_count: u32,
    attention_kinds: Vec<i32>,
) -> Envelope {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(100);
    let mut summary = session_summary(device_str, native, "推送测试");
    summary.last_turn_outcome = outcome;
    summary.pending_attention_count = attention_count;
    summary.pending_attention_kinds = attention_kinds;
    let mut env = base_env(envelope::Payload::SessionSummaryBatch(
        SessionSummaryBatch {
            summaries: vec![summary],
            snapshot: false,
        },
    ));
    env.stream_id = format!("u-{device_str}-list");
    env.device_id = device_str.to_string();
    env.sequence = SEQ.fetch_add(1, Ordering::SeqCst);
    env
}

/// 等待 fake sink 收到指定数量的通知。
async fn wait_seen(sink: &FakeSink, n: usize) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let seen = sink.seen.lock().unwrap();
        if seen.len() >= n {
            return seen.clone();
        }
        drop(seen);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("fake sink did not receive {n} notifications in time");
}

use base64::Engine;

/// 解码通知 payload(fake sink 收到的是 base64 编码的 JSON 通知体)。
fn decoded(seen: &[serde_json::Value], i: usize) -> serde_json::Value {
    let payload_b64 = seen[i]["payload"].as_str().expect("payload");
    let raw = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .expect("payload b64");
    serde_json::from_slice(&raw).expect("payload json")
}

/// 确认 400ms 内没有新通知。
async fn assert_no_new(sink: &FakeSink, expected: usize) {
    tokio::time::sleep(Duration::from_millis(400)).await;
    let seen = sink.seen.lock().unwrap();
    assert_eq!(
        seen.len(),
        expected,
        "unexpected notifications: {:?}",
        seen.iter().map(|n| &n["body"]).collect::<Vec<_>>()
    );
}

const ATTENTION_APPROVAL: i32 = 2; // ATTENTION_RISK_APPROVAL

#[tokio::test(flavor = "multi_thread")]
async fn push_trigger_matrix_mute_switches_and_cleanup() {
    let (sink_base, sink) = start_fake_sink().await;
    let env = setup(&[("RELAY_PUSH_FAKE_SINK", sink_base.as_str())]).await;

    // 注册订阅。
    let created = post_json(
        &env.relay,
        "/agent-console/api/push/subscriptions",
        serde_json::json!({
            "endpoint": "https://push.example/endpoint-1",
            "keys": {"p256dh": "key-1", "auth": "auth-1"},
        }),
    )
    .await;
    assert_eq!(created.status(), 201);

    // 设置默认值:全部开、不显示 title(§24)。
    let settings: serde_json::Value = get(&env.relay, "/agent-console/api/push/settings")
        .await
        .json()
        .await
        .expect("settings");
    assert_eq!(settings["showTitle"], false);
    assert_eq!(settings["events"]["turnCompleted"], true);
    assert_eq!(settings["events"]["waitingApproval"], true);

    // 建立设备/bridge/浏览器订阅(上游绑定)+ 会话行。
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let device_str = device_id.to_string();
    let mut bridge = FakeBridge::connect(&env.relay, &device_str, &credential)
        .await
        .expect("bridge");
    // 等 bridge 注册生效(订阅转发需要设备已在线)。
    tokio::time::sleep(Duration::from_millis(300)).await;
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(60));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");
    browser.send(&subscribe_list()).await;
    let sub = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
    bridge.send_subscribed(&sub.stream_id, 1, 0).await;
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_str,
            "native-push",
            "推送测试",
        ))
        .await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    until_async(|| async {
        sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT id FROM session_summaries WHERE device_id = $1 AND native_session_id = 'native-push'",
        )
        .bind(device_id)
        .fetch_optional(&env.pool)
        .await
        .ok()
        .flatten()
        .map(|_| ())
    })
    .await;
    let (session_id,): (uuid::Uuid,) = sqlx::query_as(
        "SELECT id FROM session_summaries WHERE device_id = $1 AND native_session_id = 'native-push'",
    )
    .bind(device_id)
    .fetch_one(&env.pool)
    .await
    .expect("session row");

    // 1. turn completed → 推送;默认通用文案,不含 title。
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeCompleted as i32,
            0,
            vec![],
        ))
        .await;
    let seen = wait_seen(&sink, 1).await;
    assert_eq!(decoded(&seen, 0)["body"], "任务已完成");
    assert_eq!(decoded(&seen, 0)["title"], serde_json::Value::Null);
    assert_eq!(decoded(&seen, 0)["kind"], "turnCompleted");
    assert_eq!(
        decoded(&seen, 0)["sessionId"],
        serde_json::json!(session_id)
    );
    assert_eq!(
        decoded(&seen, 0)["deepLink"],
        serde_json::json!(format!("/agent-console/s/{session_id}"))
    );
    assert_eq!(seen[0]["endpoint"], "https://push.example/endpoint-1");

    // 2. 相同 (session, kind) 去重窗口内不重复推送(§24 只推状态跃迁)。
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeCompleted as i32,
            0,
            vec![],
        ))
        .await;
    assert_no_new(&sink, 1).await;

    // 3. turn failed → 新 kind,推送。
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeFailed as i32,
            0,
            vec![],
        ))
        .await;
    let seen = wait_seen(&sink, 2).await;
    assert_eq!(decoded(&seen, 1)["body"], "任务未完成,已失败");
    assert_eq!(decoded(&seen, 1)["kind"], "turnFailed");

    // 4. session mute 覆盖事件开关(§24):静音后 interrupted 不推送。
    let patch = reqwest::Client::new()
        .patch(format!(
            "{}/agent-console/api/sessions/{session_id}",
            env.relay.base
        ))
        .headers(header_map(&identity_headers()))
        .json(&serde_json::json!({"muted": true}))
        .send()
        .await
        .expect("mute");
    assert_eq!(patch.status(), 204);
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeInterrupted as i32,
            0,
            vec![],
        ))
        .await;
    assert_no_new(&sink, 2).await;

    // 5. 取消静音 → interrupted 推送。
    let patch = reqwest::Client::new()
        .patch(format!(
            "{}/agent-console/api/sessions/{session_id}",
            env.relay.base
        ))
        .headers(header_map(&identity_headers()))
        .json(&serde_json::json!({"muted": false}))
        .send()
        .await
        .expect("unmute");
    assert_eq!(patch.status(), 204);
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeInterrupted as i32,
            0,
            vec![],
        ))
        .await;
    let seen = wait_seen(&sink, 3).await;
    assert_eq!(decoded(&seen, 2)["body"], "任务已中断");
    assert_eq!(decoded(&seen, 2)["kind"], "turnInterrupted");

    // 6. 事件开关关闭:waitingApproval=false → 审批不推送(此前该 kind 从未推送,
    //    不受去重影响,可区分开关与去重)。
    let put = put_json(
        &env.relay,
        "/agent-console/api/push/settings",
        serde_json::json!({"events": {"waitingApproval": false}}),
    )
    .await;
    assert_eq!(put.status(), 200);
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            0,
            1,
            vec![ATTENTION_APPROVAL],
        ))
        .await;
    assert_no_new(&sink, 3).await;

    // 7. 重新开启 + showTitle=true → 审批推送且包含 title。
    let put = put_json(
        &env.relay,
        "/agent-console/api/push/settings",
        serde_json::json!({"showTitle": true, "events": {"waitingApproval": true}}),
    )
    .await;
    assert_eq!(put.status(), 200);
    bridge
        .send(&delta_env(
            &device_str,
            "native-push",
            0,
            1,
            vec![ATTENTION_APPROVAL],
        ))
        .await;
    let seen = wait_seen(&sink, 4).await;
    assert_eq!(decoded(&seen, 3)["body"], "任务在等待风险审批");
    assert_eq!(decoded(&seen, 3)["kind"], "waitingApproval");
    assert_eq!(decoded(&seen, 3)["title"], "推送测试");

    // 8. 410 → 订阅删除(§24 失效清理)。新会话(独立去重键)+ COMPLETED 触发推送,
    //    fake sink 返回 410,Relay 删除订阅。
    sink.mode.store(1, Ordering::SeqCst);
    bridge
        .send(&delta_env(
            &device_str,
            "native-push-2",
            agent_console_protocol::v1::LastTurnOutcome::TurnOutcomeCompleted as i32,
            0,
            vec![],
        ))
        .await;
    let seen = wait_seen(&sink, 5).await;
    assert_eq!(decoded(&seen, 4)["body"], "任务已完成");
    until_async(|| async {
        let n: Option<i64> = sqlx::query_scalar("SELECT count(*) FROM push_subscriptions")
            .fetch_one(&env.pool)
            .await
            .ok();
        n.filter(|c| *c == 0).map(|_| ())
    })
    .await;
    let list: serde_json::Value = get(&env.relay, "/agent-console/api/push/subscriptions")
        .await
        .json()
        .await
        .expect("list");
    assert_eq!(list["subscriptions"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn push_subscription_crud_and_disabled_without_config() {
    // 不注入 RELAY_PUSH_FAKE_SINK:push 发送禁用,订阅仍可存储(§24)。
    let env = setup(&[]).await;

    // CRUD:创建 → 列表 → 更新 → 删除。
    let created = post_json(
        &env.relay,
        "/agent-console/api/push/subscriptions",
        serde_json::json!({
            "endpoint": "https://push.example/ep",
            "keys": {"p256dh": "k", "auth": "a"},
        }),
    )
    .await;
    assert_eq!(created.status(), 201);
    let id: serde_json::Value = created.json().await.expect("json");
    let id = id["id"].as_str().unwrap().to_string();

    let list: serde_json::Value = get(&env.relay, "/agent-console/api/push/subscriptions")
        .await
        .json()
        .await
        .expect("list");
    assert_eq!(list["subscriptions"].as_array().unwrap().len(), 1);

    let updated = put_json(
        &env.relay,
        &format!("/agent-console/api/push/subscriptions/{id}"),
        serde_json::json!({
            "endpoint": "https://push.example/ep2",
            "keys": {"p256dh": "k2", "auth": "a2"},
        }),
    )
    .await;
    assert_eq!(updated.status(), 204);

    let deleted = reqwest::Client::new()
        .delete(format!(
            "{}/agent-console/api/push/subscriptions/{id}",
            env.relay.base
        ))
        .headers(header_map(&identity_headers()))
        .send()
        .await
        .expect("delete");
    assert_eq!(deleted.status(), 204);

    // 事件到来也无副作用(发送禁用)。
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let device_str = device_id.to_string();
    let mut bridge = FakeBridge::connect(&env.relay, &device_str, &credential)
        .await
        .expect("bridge");
    // 等 bridge 注册生效(订阅转发需要设备已在线)。
    tokio::time::sleep(Duration::from_millis(300)).await;
    let ticket = env.toolbox.issue_ticket(chrono::Duration::seconds(60));
    let mut browser = connect_browser(&env.relay, &ticket).await.expect("browser");
    browser.send(&subscribe_list()).await;
    let sub = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
    bridge.send_subscribed(&sub.stream_id, 1, 0).await;
    // 先 snapshot(浏览器 Subscribed 依赖 snapshot),再 delta 触发 push 路径。
    bridge
        .send(&FakeBridge::list_snapshot_env(
            &device_str,
            "native-disabled",
            "禁用推送",
        ))
        .await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    let _ = recv_skip_presence(&mut browser.ws, Duration::from_secs(5)).await;
    bridge
        .send(&FakeBridge::list_delta_env(
            &device_str,
            "native-disabled",
            "禁用推送",
            1,
        ))
        .await;
    // 摘要事件正常扇出、发送禁用路径无异常即视为通过。
    tokio::time::sleep(Duration::from_millis(400)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn push_vapid_public_key_endpoint() {
    // 未配置 VAPID:enabled=false,publicKey=null(前端据此隐藏订阅入口)。
    let env = setup(&[]).await;
    let config: serde_json::Value =
        get(&env.relay, "/agent-console/api/push/vapid-public-key")
            .await
            .json()
            .await
            .expect("config");
    assert_eq!(config["enabled"], false);
    assert_eq!(config["publicKey"], serde_json::Value::Null);

    // fake sink + VAPID_PUBLIC_KEY:enabled=true 且公钥原样返回。
    let (sink_base, _sink) = start_fake_sink().await;
    let env = setup(&[
        ("RELAY_PUSH_FAKE_SINK", sink_base.as_str()),
        ("VAPID_PUBLIC_KEY", "test-vapid-public-key"),
    ])
    .await;
    let config: serde_json::Value =
        get(&env.relay, "/agent-console/api/push/vapid-public-key")
            .await
            .json()
            .await
            .expect("config");
    assert_eq!(config["enabled"], true);
    assert_eq!(config["publicKey"], "test-vapid-public-key");
}
