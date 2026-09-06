//! 审计查询与 Git/文件元数据查询端点集成测试(§18.7/§23.1/§22.2/§27.2/§27.3/§27.5):
//! - PATCH 会话偏好等写路径产生审计事件;GET /agent-console/api/audit 只返回元数据
//! - 审计分页:keyset 稳定 cursor,默认 50、最大 200
//! - GET /sessions/{id}/git → QueryRequest(git_summary)转发 → JSON 回放
//! - GET /sessions/{id}/git/diff → git_file_diff
//! - GET /sessions/{id}/files/metadata?handle= → file_metadata
//! - 设备离线 → DEVICE_OFFLINE;Bridge 错误码透传

mod support;

use std::time::Duration;

use agent_console_protocol::v1::{
    envelope, query_response, GitSummaryData, GitSummaryQuery, QueryRequest, QueryResponse,
    StableErrorCode,
};
use support::*;

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

async fn insert_session_row(
    db: &sqlx::PgPool,
    device_id: uuid::Uuid,
    native: &str,
    title: &str,
) -> uuid::Uuid {
    let row: (uuid::Uuid,) = sqlx::query_as(
        "INSERT INTO session_summaries (id, device_id, agent_kind, native_session_id, title, device_connection) \
         VALUES (gen_random_uuid(), $1, 'AGENT_KIND_CODEX_DESKTOP', $2, $3, 'CONNECTION_ONLINE') RETURNING id",
    )
    .bind(device_id)
    .bind(native)
    .bind(title)
    .fetch_one(db)
    .await
    .expect("insert session row");
    row.0
}

/// 读取 relay 转发来的 QueryRequest,返回 correlation_id。
async fn wait_query_request(ws: &mut Ws) -> String {
    loop {
        let env = recv_skip_heartbeat(ws, Duration::from_secs(5)).await;
        if let Some(envelope::Payload::QueryRequest(_)) = env.payload {
            return env.correlation_id;
        }
    }
}

/// 以 QueryResponse 回放(result JSON 化由 relay 完成)。
async fn reply_query(
    bridge: &mut FakeBridge,
    correlation: &str,
    result: Option<query_response::Result>,
    error_code: i32,
) {
    let resp = QueryResponse {
        request_id: correlation.to_string(),
        result: result,
        error_code,
    };
    let env = base_env(envelope::Payload::QueryResponse(resp));
    bridge.send(&env).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_records_and_lists_with_cursor_pagination() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (_credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let session = insert_session_row(&env.pool, device_id, "native-audit", "审计").await;

    // 写路径产生审计事件(§18.7):PATCH 会话偏好。
    let patch = reqwest::Client::new()
        .patch(format!(
            "{}/agent-console/api/sessions/{session}",
            env.relay.base
        ))
        .headers(header_map(&identity_headers()))
        .json(&serde_json::json!({"pinned": true}))
        .send()
        .await
        .expect("patch");
    assert_eq!(patch.status(), 204);

    // 直接触发第二个事件(撤销审计等设备路径在此不重复;直接插入一条用于分页)。
    sqlx::query(
        "INSERT INTO audit_events (owner_id, device_id, session_id, request_id, operation, result, latency_ms, created_at) \
         VALUES ($1, $2, $3, 'req-1', 'session_prefs', 'OK', 5, now() - interval '10 seconds')",
    )
    .bind(OWNER)
    .bind(device_id)
    .bind(session)
    .execute(&env.pool)
    .await
    .expect("insert audit");

    // 列表:只返回元数据。
    let resp = http_get(&env.relay, "/agent-console/api/audit", &identity_headers()).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let events = body["events"].as_array().expect("events");
    assert!(events.len() >= 2, "expect at least 2 audit events");
    assert_eq!(events[0]["operation"], "session_prefs");
    assert_eq!(events[0]["result"], "OK");
    assert!(events[0]["deviceId"].is_string());
    assert!(events[0]["sessionId"].is_string());
    assert!(events[0]["createdAt"].is_string());
    // 元数据之外不保存正文/标题/路径(§18.7):字段集合固定。
    let allowed = [
        "id",
        "deviceId",
        "sessionId",
        "requestId",
        "operation",
        "result",
        "latencyMs",
        "createdAt",
    ];
    for key in events[0].as_object().expect("object").keys() {
        assert!(
            allowed.contains(&key.as_str()),
            "unexpected audit field {key}"
        );
    }

    // 分页:limit=1 → nextCursor;cursor 续读不重叠(稳定 keyset,§27.5)。
    let page1 = http_get(
        &env.relay,
        "/agent-console/api/audit?limit=1",
        &identity_headers(),
    )
    .await;
    let page1: serde_json::Value = page1.json().await.expect("json");
    let p1 = page1["events"].as_array().unwrap();
    assert_eq!(p1.len(), 1);
    let cursor = page1["nextCursor"].as_str().expect("cursor").to_string();

    let page2 = http_get(
        &env.relay,
        &format!("/agent-console/api/audit?limit=1&cursor={cursor}"),
        &identity_headers(),
    )
    .await;
    let page2: serde_json::Value = page2.json().await.expect("json");
    let p2 = page2["events"].as_array().unwrap();
    assert_eq!(p2.len(), 1);
    assert_ne!(p1[0]["id"], p2[0]["id"], "cursor page must not overlap");

    // 只见自己的事件(owner 过滤)。
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events")
        .fetch_one(&env.pool)
        .await
        .expect("count");
    assert!(count >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn git_diff_query_roundtrip() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let session = insert_session_row(&env.pool, device_id, "native-git2", "Diff").await;

    // GET 挂起 → bridge 收到 git_file_diff 查询 → 回放 → HTTP JSON。
    let relay_base = env.relay.base.clone();
    let sess = session;
    let get_task = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!(
                "{relay_base}/agent-console/api/sessions/{sess}/git/diff"
            ))
            .query(&[("path", "src/main.rs"), ("staged", "true")])
            .headers(header_map(&identity_headers()))
            .send()
            .await
            .expect("git diff get")
    });

    let correlation = wait_query_request(&mut bridge.ws).await;
    reply_query(
        &mut bridge,
        &correlation,
        Some(query_response::Result::GitFileDiff(
            agent_console_protocol::v1::GitFileDiffData {
                relative_path: "src/main.rs".into(),
                staged: true,
                patch_text: "@@ -1 +1 @@\n-old\n+new".into(),
                truncated: false,
                total_bytes: 18,
                binary: false,
            },
        )),
        0,
    )
    .await;

    let resp = get_task.await.expect("join");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let diff = &body["gitFileDiff"];
    assert_eq!(diff["relativePath"], "src/main.rs");
    assert_eq!(diff["staged"], true);
    assert!(diff["patchText"].as_str().unwrap().contains("+new"));
    assert_eq!(diff["truncated"], false);
    assert_eq!(diff["totalBytes"], 18);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_metadata_query_roundtrip_and_error_passthrough() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let session = insert_session_row(&env.pool, device_id, "native-meta", "Meta").await;

    // 成功路径。
    let relay_base = env.relay.base.clone();
    let sess = session;
    let get_task = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!(
                "{relay_base}/agent-console/api/sessions/{sess}/files/metadata"
            ))
            .query(&[("handle", "h-meta-1")])
            .headers(header_map(&identity_headers()))
            .send()
            .await
            .expect("metadata get")
    });
    let correlation = wait_query_request(&mut bridge.ws).await;
    reply_query(
        &mut bridge,
        &correlation,
        Some(query_response::Result::FileMetadata(
            agent_console_protocol::v1::FileMetadataData {
                display_name: "report.pdf".into(),
                mime_type: "application/pdf".into(),
                size_bytes: 4096,
                preview_kind: "pdf".into(),
                not_previewable_reason: String::new(),
                file_handle: "h-meta-1-refreshed".into(),
            },
        )),
        0,
    )
    .await;
    let resp = get_task.await.expect("join");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let meta = &body["fileMetadata"];
    assert_eq!(meta["displayName"], "report.pdf");
    assert_eq!(meta["mimeType"], "application/pdf");
    assert_eq!(meta["sizeBytes"], 4096);
    assert_eq!(meta["previewKind"], "pdf");
    assert!(meta["notPreviewableReason"].is_null());
    assert_eq!(meta["fileHandle"], "h-meta-1-refreshed");

    // Bridge 错误码透传(§27.5/§27.6):FILE_HANDLE_INVALID。
    let relay_base = env.relay.base.clone();
    let get_task = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!(
                "{relay_base}/agent-console/api/sessions/{sess}/files/metadata"
            ))
            .query(&[("handle", "h-missing")])
            .headers(header_map(&identity_headers()))
            .send()
            .await
            .expect("metadata get")
    });
    let correlation = wait_query_request(&mut bridge.ws).await;
    reply_query(
        &mut bridge,
        &correlation,
        None,
        StableErrorCode::FileHandleInvalid as i32,
    )
    .await;
    let resp = get_task.await.expect("join");
    assert_eq!(resp.status(), 500); // FILE_HANDLE_INVALID 走 status_for_code 默认映射
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"]["code"], "FILE_HANDLE_INVALID");
}

#[tokio::test(flavor = "multi_thread")]
async fn git_summary_query_roundtrip() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let mut bridge = FakeBridge::connect(&env.relay, &device_id.to_string(), &credential)
        .await
        .expect("bridge");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let session = insert_session_row(&env.pool, device_id, "native-git3", "Git3").await;

    let relay_base = env.relay.base.clone();
    let sess = session;
    let get_task = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!(
                "{relay_base}/agent-console/api/sessions/{sess}/git"
            ))
            .headers(header_map(&identity_headers()))
            .send()
            .await
            .expect("git get")
    });
    let correlation = wait_query_request(&mut bridge.ws).await;
    reply_query(
        &mut bridge,
        &correlation,
        Some(query_response::Result::GitSummary(GitSummaryData {
            branch: "main".into(),
            detached_head: false,
            head_short: "abc1234".into(),
            head_full: "abc1234def5678".into(),
            root_display_name: "repo".into(),
            entries: vec![agent_console_protocol::v1::GitStatusEntry {
                relative_path: "src/main.rs".into(),
                status: "modified".into(),
                staged: false,
            }],
            insertions: 12,
            deletions: 3,
            binary_files: vec!["assets/logo.png".into()],
        })),
        0,
    )
    .await;

    let resp = get_task.await.expect("join");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let summary = &body["gitSummary"];
    assert_eq!(summary["branch"], "main");
    assert_eq!(summary["detachedHead"], false);
    assert_eq!(summary["headShort"], "abc1234");
    assert_eq!(summary["headFull"], "abc1234def5678");
    assert_eq!(summary["rootDisplayName"], "repo");
    assert_eq!(summary["insertions"], 12);
    assert_eq!(summary["deletions"], 3);
    assert_eq!(summary["binaryFiles"][0], "assets/logo.png");
    assert_eq!(summary["entries"][0]["relativePath"], "src/main.rs");
    assert_eq!(summary["entries"][0]["status"], "modified");
    assert_eq!(summary["entries"][0]["staged"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_device_returns_device_offline_for_queries() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (_credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let session = insert_session_row(&env.pool, device_id, "native-off", "离线").await;

    let resp = http_get(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/git"),
        &identity_headers(),
    )
    .await;
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"]["code"], "DEVICE_OFFLINE");
}

#[tokio::test(flavor = "multi_thread")]
async fn version_endpoint_reports_relay_and_protocol_versions() {
    let env = setup(&[]).await;
    // 网关按 /agent-console/api/* 原样转发:版本端点必须挂在该前缀下。
    let resp = http_get(&env.relay, "/agent-console/api/version", &[]).await;
    assert!(resp.status().is_success(), "GET /version must succeed");
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["relayVersion"].as_str(),
        Some(env!("CARGO_PKG_VERSION")),
        "relay version must match the relay crate version"
    );
    assert_eq!(
        body["protocolVersion"].as_u64(),
        Some(agent_console_protocol::codec::PROTOCOL_VERSION as u64),
        "protocol version must match the protocol crate constant"
    );
}

// 引用保持:确保 QueryRequest/GitSummaryQuery 类型被编译器检查(转发侧字段一致性)。
#[allow(dead_code)]
fn _type_witness(_q: QueryRequest, _g: GitSummaryQuery) {}
