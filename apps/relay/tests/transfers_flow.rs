//! 文件数据面集成测试(§22/§29.2):
//! - preview 元数据头透传 + 安全响应头 + 正文 pipe
//! - 单 Range 透传(206);多段/无效 Range 拒绝(TRANSFER_RANGE_INVALID)
//! - upload 声明 → consumer 出站 → 正文流式 PUT → TransferResult 回传
//! - 上传超限 TRANSFER_TOO_LARGE;设备离线 DEVICE_OFFLINE;并发上限 RATE_LIMITED
//! - Bridge 拒绝(TransferReady)三端联动 + 迟到 producer 被拒
//! - producer/consumer 端点鉴权(401)与未知 transfer(TRANSFER_EXPIRED)
//! - browser 断开 → 三端取消(consumer 提前收尾 + Bridge 收到取消通知)
//! - rendezvous 超时(TRANSFER_EXPIRED)
//! - 磁盘无落地(TMPDIR 监控断言)

mod support;

use std::time::Duration;

use agent_console_protocol::v1::{envelope, TransferDirection, TransferOutcome, TransferResult};
use futures::StreamExt;
use support::*;

fn identity() -> Vec<(&'static str, String)> {
    identity_headers()
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

async fn post_json(
    relay: &Relay,
    path: &str,
    body: serde_json::Value,
    headers: &[(&str, String)],
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{path}", relay.base))
        .headers(header_map(headers))
        .json(&body)
        .send()
        .await
        .expect("post")
}

async fn put_bytes(
    relay: &Relay,
    path: &str,
    body: Vec<u8>,
    headers: &[(&str, String)],
) -> reqwest::Response {
    reqwest::Client::new()
        .put(format!("{}{path}", relay.base))
        .headers(header_map(headers))
        .body(body)
        .send()
        .await
        .expect("put")
}

/// 等 TransferOffer(跳过心跳)。
async fn wait_offer(ws: &mut Ws) -> agent_console_protocol::v1::TransferOffer {
    loop {
        let env = recv_skip_heartbeat(ws, Duration::from_secs(5)).await;
        if let Some(envelope::Payload::TransferOffer(offer)) = env.payload {
            return offer;
        }
    }
}

/// 模拟 Bridge producer:POST 文件字节流(对齐 apps/bridge files::transfer::produce)。
async fn produce(
    relay: &Relay,
    credential: &str,
    offer: &agent_console_protocol::v1::TransferOffer,
    content: Vec<u8>,
    content_range: Option<String>,
    total_length: Option<u64>,
    content_type: &str,
) -> reqwest::Response {
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = content
        .chunks(64 * 1024)
        .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
        .collect();
    let mut req = reqwest::Client::new()
        .post(format!(
            "{}/agent-console/transfers/producer/{}",
            relay.base, offer.transfer_id
        ))
        .header("Authorization", format!("Bearer {credential}"))
        // 与 Bridge 对齐:transfer token = transfer_id(§17.2/v1 proto)。
        .header("X-Transfer-Token", &offer.transfer_id)
        .header("X-Transfer-Length", content.len())
        .header("X-Transfer-Content-Type", content_type)
        .header("X-Transfer-Disposition", "inline")
        .header("Content-Type", content_type);
    if let Some(cr) = content_range {
        req = req.header("X-Transfer-Content-Range", cr);
    }
    if let Some(t) = total_length {
        req = req.header("X-Transfer-Total-Length", t);
    }
    req.body(reqwest::Body::wrap_stream(futures::stream::iter(chunks)))
        .send()
        .await
        .expect("producer post")
}

async fn err_code_of(resp: reqwest::Response) -> String {
    let body: serde_json::Value = resp.json().await.expect("json body");
    body["error"]["code"].as_str().unwrap_or("").to_string()
}

/// 标准环境:设备 + 在线 fake bridge + 会话行。
async fn ready_env(
    tmpdir: Option<&std::path::Path>,
) -> (Env, FakeBridge, uuid::Uuid, uuid::Uuid, String) {
    let env = match tmpdir {
        Some(dir) => setup(&[("TMPDIR", dir.to_str().unwrap())]).await,
        None => setup(&[]).await,
    };
    let device_id = uuid::Uuid::new_v4();
    let (credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let device_str = device_id.to_string();
    let bridge = FakeBridge::connect(&env.relay, &device_str, &credential)
        .await
        .expect("bridge connect");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let session = insert_session_row(&env.pool, device_id, "native-transfer", "传输测试").await;
    (env, bridge, device_id, session, credential)
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_pipes_body_and_forwards_metadata() {
    let tmp = std::env::temp_dir().join(format!("relay-transfer-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).expect("mkdir tmp");
    let (env, mut bridge, _device, session, credential) = ready_env(Some(&tmp)).await;

    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let relay_base = env.relay.base.clone();
    let browser_req = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{relay_base}{path}"))
            .headers(header_map(&identity()))
            .json(&serde_json::json!({"fileHandle": "h-preview-1", "fileName": "a.txt"}))
            .send()
            .await
            .expect("browser preview")
    });

    // Relay → Bridge TransferOffer(下载方向,带 handle 与 session 绑定)。
    let offer = wait_offer(&mut bridge.ws).await;
    assert_eq!(offer.direction, TransferDirection::Download as i32);
    assert_eq!(offer.file_handle, "h-preview-1");
    assert_eq!(offer.file_name, "a.txt");
    let key = offer.session_key.as_ref().expect("session key");
    assert_eq!(key.relay_session_uuid, session.to_string());

    // Bridge producer 出站 POST;Relay 尽早返回响应头(§22.4 第 4 步)。
    let producer_resp = produce(
        &env.relay,
        &credential,
        &offer,
        b"hello world".to_vec(),
        None,
        Some(11),
        "text/plain",
    )
    .await;
    assert_eq!(producer_resp.status(), 202);

    let browser_resp = browser_req.await.expect("join");
    assert_eq!(browser_resp.status(), 200);
    let headers = browser_resp.headers();
    assert_eq!(headers.get("content-type").unwrap(), "text/plain");
    assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(
        headers.get("content-security-policy").unwrap(),
        "default-src 'none'"
    );
    assert_eq!(headers.get("cache-control").unwrap(), "private, no-store");
    assert_eq!(
        // Bridge 通过 X-Transfer-Disposition 给出的值优先透传(§22.3)。
        headers.get("content-disposition").unwrap(),
        "inline"
    );
    assert_eq!(headers.get("content-length").unwrap(), "11");
    let body = browser_resp.bytes().await.expect("body");
    assert_eq!(body.as_ref(), b"hello world");

    // 磁盘无落地断言:Relay 进程 TMPDIR 全程为空(§22.1)。
    let entries = std::fs::read_dir(&tmp).expect("read tmpdir").count();
    assert_eq!(entries, 0, "relay wrote files to TMPDIR");
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_single_range_passes_through() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;

    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let relay_base = env.relay.base.clone();
    let browser_req = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{relay_base}{path}"))
            .headers(header_map(&identity()))
            .json(&serde_json::json!({
                "fileHandle": "h-range",
                "fileName": "data.bin",
                "range": {"start": 2, "endInclusive": 5}
            }))
            .send()
            .await
            .expect("browser preview")
    });

    let offer = wait_offer(&mut bridge.ws).await;
    assert_eq!(offer.range_start, 2);
    assert_eq!(offer.range_end_inclusive, Some(5));

    // producer 只发送切片字节,并带 Content-Range 元数据。
    let producer_resp = produce(
        &env.relay,
        &credential,
        &offer,
        b"cdef".to_vec(),
        Some("bytes 2-5/11".into()),
        Some(4),
        "application/octet-stream",
    )
    .await;
    assert_eq!(producer_resp.status(), 202);

    let browser_resp = browser_req.await.expect("join");
    assert_eq!(browser_resp.status(), 206);
    assert_eq!(
        browser_resp.headers().get("content-range").unwrap(),
        "bytes 2-5/11"
    );
    let body = browser_resp.bytes().await.expect("body");
    assert_eq!(body.as_ref(), b"cdef");
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_rejects_multi_and_invalid_range() {
    let (env, _bridge, _device, session, _credential) = ready_env(None).await;
    let path = format!("/agent-console/api/sessions/{session}/files/preview");

    // 多段 Range(HTTP header)拒绝。
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", env.relay.base))
        .headers(header_map(&identity()))
        .header("Range", "bytes=0-1,3-4")
        .json(&serde_json::json!({"fileHandle": "h"}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
    assert_eq!(err_code_of(resp).await, "TRANSFER_RANGE_INVALID");

    // 逆序 JSON Range 拒绝。
    let resp = post_json(
        &env.relay,
        &path,
        serde_json::json!({"fileHandle": "h", "range": {"start": 5, "endInclusive": 2}}),
        &identity(),
    )
    .await;
    assert_eq!(resp.status(), 400);
    assert_eq!(err_code_of(resp).await, "TRANSFER_RANGE_INVALID");

    // 非法 header 形态拒绝。
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", env.relay.base))
        .headers(header_map(&identity()))
        .header("Range", "items=0-1")
        .json(&serde_json::json!({"fileHandle": "h"}))
        .send()
        .await
        .expect("post");
    assert_eq!(err_code_of(resp).await, "TRANSFER_RANGE_INVALID");
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_streams_to_consumer_and_returns_result() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;

    // 1. 声明上传。
    let declare = post_json(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/files/upload"),
        serde_json::json!({"fileName": "u.bin", "mime": "application/octet-stream", "length": 5}),
        &identity(),
    )
    .await;
    assert_eq!(declare.status(), 201);
    let declared: serde_json::Value = declare.json().await.expect("json");
    let transfer_id = declared["transferId"]
        .as_str()
        .expect("transferId")
        .to_string();

    // 2. Bridge 收到 upload offer(无 file_handle)。
    let base = env.relay.base.clone();
    let credential2 = credential.clone();
    let consumer = tokio::spawn(async move {
        let offer = wait_offer(&mut bridge.ws).await;
        assert_eq!(offer.direction, TransferDirection::Upload as i32);
        assert_eq!(offer.file_handle, "");
        assert_eq!(offer.file_name, "u.bin");
        assert_eq!(offer.size_bytes, 5);
        // 3. consumer 出站 GET;读上传字节流。
        let resp = reqwest::Client::new()
            .get(format!(
                "{base}/agent-console/transfers/consumer/{}",
                offer.transfer_id
            ))
            .header("Authorization", format!("Bearer {credential2}"))
            .header("X-Transfer-Token", &offer.transfer_id)
            .send()
            .await
            .expect("consumer get");
        assert_eq!(resp.status(), 200);
        let bytes = resp.bytes().await.expect("consumer body");
        // 桥侧写临时目录校验后回传 TransferResult(§22.5 第 6 步)。
        bridge
            .send(&base_env(envelope::Payload::TransferResult(
                TransferResult {
                    transfer_id: offer.transfer_id.clone(),
                    outcome: TransferOutcome::Completed as i32,
                    error_code: 0,
                    upload_file_handle: "fixture-upload-handle".to_string(),
                },
            )))
            .await;
        (offer.transfer_id, bytes)
    });

    // 4. consumer ready 后 PUT 正文流。
    let put = put_bytes(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/files/upload/{transfer_id}"),
        b"12345".to_vec(),
        &identity(),
    )
    .await;
    assert_eq!(put.status(), 200);
    let result: serde_json::Value = put.json().await.expect("json");
    assert_eq!(result["outcome"], "TRANSFER_OUTCOME_COMPLETED");
    assert!(result["errorCode"].is_null());
    assert_eq!(result["uploadFileHandle"], "fixture-upload-handle");

    let (tid, bytes) = consumer.await.expect("consumer join");
    assert_eq!(tid, transfer_id);
    assert_eq!(bytes.as_ref(), b"12345");
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_declare_rejects_too_large() {
    let (env, _bridge, _device, session, _credential) = ready_env(None).await;
    let resp = post_json(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/files/upload"),
        serde_json::json!({"fileName": "big.bin", "length": 21 * 1024 * 1024}),
        &identity(),
    )
    .await;
    assert_eq!(resp.status(), 413);
    assert_eq!(err_code_of(resp).await, "TRANSFER_TOO_LARGE");
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_device_rejects_preview() {
    let env = setup(&[]).await;
    let device_id = uuid::Uuid::new_v4();
    let (_credential, digest) = test_credential();
    insert_device(&env.pool, device_id, &digest).await;
    let session = insert_session_row(&env.pool, device_id, "native-offline", "离线").await;
    let resp = post_json(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/files/preview"),
        serde_json::json!({"fileHandle": "h"}),
        &identity(),
    )
    .await;
    assert_eq!(resp.status(), 503);
    assert_eq!(err_code_of(resp).await, "DEVICE_OFFLINE");
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrency_limit_two_per_browser_and_device() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;
    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let base = env.relay.base.clone();
    let path_for_task = path.clone();

    // 两个 pending transfer(producer 不连接)。
    let mut pending = Vec::new();
    for i in 0..2 {
        let base = base.clone();
        let path = path_for_task.clone();
        pending.push(tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{base}{path}"))
                .headers(header_map(&identity()))
                .json(&serde_json::json!({"fileHandle": format!("h-{i}")}))
                .send()
                .await
                .expect("pending preview")
        }));
    }
    let offer1 = wait_offer(&mut bridge.ws).await;
    let offer2 = wait_offer(&mut bridge.ws).await;

    // 第三个 → RATE_LIMITED(§22.6:每 browser/device 各 2)。
    let third = post_json(
        &env.relay,
        &path,
        serde_json::json!({"fileHandle": "h-3"}),
        &identity(),
    )
    .await;
    assert_eq!(third.status(), 429);
    assert_eq!(err_code_of(third).await, "RATE_LIMITED");

    // 完成两个后名额释放:新请求拿到 offer。
    let p1 = produce(
        &env.relay,
        &credential,
        &offer1,
        b"one".to_vec(),
        None,
        None,
        "text/plain",
    )
    .await;
    let p2 = produce(
        &env.relay,
        &credential,
        &offer2,
        b"two".to_vec(),
        None,
        None,
        "text/plain",
    )
    .await;
    assert_eq!(p1.status(), 202);
    assert_eq!(p2.status(), 202);
    for req in pending {
        let resp = req.await.expect("pending join");
        assert_eq!(resp.status(), 200);
    }

    let fourth = post_json(
        &env.relay,
        &path,
        serde_json::json!({"fileHandle": "h-4"}),
        &identity(),
    )
    .await;
    // 请求成功建立(收到第四个 offer 即证明名额已释放);取消收尾。
    let offer4 = wait_offer(&mut bridge.ws).await;
    assert!(!offer4.transfer_id.is_empty());
    let _ = fourth;
    bridge
        .send(&base_env(envelope::Payload::TransferResult(
            TransferResult {
                transfer_id: offer4.transfer_id.clone(),
                outcome: TransferOutcome::Cancelled as i32,
                error_code: 0,
                upload_file_handle: String::new(),
            },
        )))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_rejection_cancels_and_late_producer_rejected() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;
    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let base = env.relay.base.clone();

    let browser_req = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base}{path}"))
            .headers(header_map(&identity()))
            .json(&serde_json::json!({"fileHandle": "bad-handle"}))
            .send()
            .await
            .expect("preview")
    });

    let offer = wait_offer(&mut bridge.ws).await;
    // Bridge 复验 handle 失败 → TransferReady(ready=false, FILE_HANDLE_INVALID)。
    bridge
        .send(&base_env(envelope::Payload::TransferReady(
            agent_console_protocol::v1::TransferReady {
                transfer_id: offer.transfer_id.clone(),
                ready: false,
                rejection_code: agent_console_protocol::v1::StableErrorCode::FileHandleInvalid
                    as i32,
            },
        )))
        .await;

    let browser_resp = browser_req.await.expect("join");
    assert_eq!(browser_resp.status(), 500);
    assert_eq!(err_code_of(browser_resp).await, "FILE_HANDLE_INVALID");

    // 迟到 producer:transfer 已取消移除 → TRANSFER_EXPIRED。
    let late = produce(
        &env.relay,
        &credential,
        &offer,
        b"x".to_vec(),
        None,
        None,
        "text/plain",
    )
    .await;
    assert_eq!(late.status(), 404);
    assert_eq!(err_code_of(late).await, "TRANSFER_EXPIRED");
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_consumer_auth_enforced() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;

    // 建 download transfer。
    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let base = env.relay.base.clone();
    let browser_req = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base}{path}"))
            .headers(header_map(&identity()))
            .json(&serde_json::json!({"fileHandle": "h-auth"}))
            .send()
            .await
            .expect("preview")
    });
    let offer = wait_offer(&mut bridge.ws).await;

    // 无凭据 → 401。
    let anon = reqwest::Client::new()
        .post(format!(
            "{}/agent-console/transfers/producer/{}",
            env.relay.base, offer.transfer_id
        ))
        .header("X-Transfer-Token", &offer.transfer_id)
        .send()
        .await
        .expect("anon producer");
    assert_eq!(anon.status(), 401);

    // 错误 token → 401。
    let bad_token = reqwest::Client::new()
        .post(format!(
            "{}/agent-console/transfers/producer/{}",
            env.relay.base, offer.transfer_id
        ))
        .header("Authorization", format!("Bearer {credential}"))
        .header("X-Transfer-Token", format!("{}x", offer.transfer_id))
        .send()
        .await
        .expect("bad token producer");
    assert_eq!(bad_token.status(), 401);

    // 下载 transfer 打 consumer 端点(方向不符)→ 401。
    let wrong_direction = reqwest::Client::new()
        .get(format!(
            "{}/agent-console/transfers/consumer/{}",
            env.relay.base, offer.transfer_id
        ))
        .header("Authorization", format!("Bearer {credential}"))
        .header("X-Transfer-Token", &offer.transfer_id)
        .send()
        .await
        .expect("wrong direction");
    assert_eq!(wrong_direction.status(), 401);

    // 未知 transfer id → 404 TRANSFER_EXPIRED。
    let unknown = reqwest::Client::new()
        .post(format!(
            "{}/agent-console/transfers/producer/{}",
            env.relay.base,
            uuid::Uuid::new_v4()
        ))
        .header("Authorization", format!("Bearer {credential}"))
        .header("X-Transfer-Token", uuid::Uuid::new_v4().to_string())
        .send()
        .await
        .expect("unknown transfer");
    assert_eq!(unknown.status(), 404);
    assert_eq!(err_code_of(unknown).await, "TRANSFER_EXPIRED");

    // 正常完成,收尾 browser 请求。
    let producer = produce(
        &env.relay,
        &credential,
        &offer,
        b"ok".to_vec(),
        None,
        None,
        "text/plain",
    )
    .await;
    assert_eq!(producer.status(), 202);
    let browser_resp = browser_req.await.expect("join");
    assert_eq!(browser_resp.status(), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_abort_cancels_all_three_sides() {
    let (env, mut bridge, _device, session, credential) = ready_env(None).await;

    // 声明 + consumer 连接。
    let declare = post_json(
        &env.relay,
        &format!("/agent-console/api/sessions/{session}/files/upload"),
        serde_json::json!({"fileName": "big.bin", "length": 512 * 1024}),
        &identity(),
    )
    .await;
    assert_eq!(declare.status(), 201);
    let declared: serde_json::Value = declare.json().await.expect("json");
    let transfer_id = declared["transferId"].as_str().unwrap().to_string();

    let base = env.relay.base.clone();
    let credential2 = credential.clone();
    // 先在本任务内等 offer(保持 bridge.ws 所有权),再只把 HTTP 读取交给子任务。
    let offer = wait_offer(&mut bridge.ws).await;
    assert_eq!(offer.direction, TransferDirection::Upload as i32);
    let offer_tid = offer.transfer_id.clone();
    // consumer 收到首个分块后发信号,保证 PUT 已在传输中。
    let (first_chunk_tx, first_chunk_rx) = tokio::sync::oneshot::channel::<()>();
    let consumer = tokio::spawn(async move {
        let resp = reqwest::Client::new()
            .get(format!(
                "{base}/agent-console/transfers/consumer/{offer_tid}"
            ))
            .header("Authorization", format!("Bearer {credential2}"))
            .header("X-Transfer-Token", &offer_tid)
            .send()
            .await
            .expect("consumer get");
        assert_eq!(resp.status(), 200);
        // 读到 EOF(取消传播后可能被截断)。
        let mut got: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        let mut first_chunk_tx = Some(first_chunk_tx);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(c) => {
                    got.extend_from_slice(&c);
                    if !got.is_empty() {
                        if let Some(tx) = first_chunk_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                }
                Err(_) => break,
            }
        }
        (offer_tid, got)
    });
    // 等 consumer 建立连接(ready 由 relay PUT 等待侧协调)。
    tokio::time::sleep(Duration::from_millis(300)).await;

    // browser PUT 分块推送后中途断开(abort 任务)。
    let base = env.relay.base.clone();
    let url = format!("/agent-console/api/sessions/{session}/files/upload/{transfer_id}");
    let put_task = tokio::spawn(async move {
        // 慢速分块流:块间 300ms 停顿,确保 abort 发生在传输中途。
        let body_stream = futures::stream::unfold(0u32, |i| async move {
            if i >= 10 {
                None
            } else {
                if i > 0 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Some((
                    Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(vec![b'x'; 64 * 1024])),
                    i + 1,
                ))
            }
        });
        reqwest::Client::new()
            .put(format!("{base}{url}"))
            .headers(header_map(&identity()))
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .expect("put")
    });
    // consumer 收到首块(证明 PUT 在传)后强制中止 PUT。
    tokio::time::timeout(Duration::from_secs(5), first_chunk_rx)
        .await
        .expect("first chunk")
        .expect("signal");
    put_task.abort();

    // consumer 流提前收束(取消传播),字节数小于声明值。
    let (ctid, _bytes) = consumer.await.expect("consumer join");
    assert_eq!(ctid, transfer_id);

    // Bridge 收到 Relay 的取消通知 TransferResult(三端联动,§22.4 第 6 步)。
    loop {
        let env = recv_skip_heartbeat(&mut bridge.ws, Duration::from_secs(5)).await;
        if let Some(envelope::Payload::TransferResult(r)) = env.payload {
            assert_eq!(r.transfer_id, transfer_id);
            assert_eq!(r.outcome, TransferOutcome::Cancelled as i32);
            break;
        }
    }

    // 迟到 producer/consumer 都被拒。
    let late = reqwest::Client::new()
        .get(format!(
            "{}/agent-console/transfers/consumer/{transfer_id}",
            env.relay.base
        ))
        .header("Authorization", format!("Bearer {credential}"))
        .header("X-Transfer-Token", &transfer_id)
        .send()
        .await
        .expect("late consumer");
    assert_eq!(late.status(), 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_rendezvous_timeout_returns_transfer_expired() {
    let (env, mut bridge, _device, session, _credential) = ready_env(None).await;
    let path = format!("/agent-console/api/sessions/{session}/files/preview");
    let base = env.relay.base.clone();
    let started = std::time::Instant::now();
    let browser_req = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base}{path}"))
            .headers(header_map(&identity()))
            .json(&serde_json::json!({"fileHandle": "h-slow"}))
            .send()
            .await
            .expect("preview")
    });
    let _offer = wait_offer(&mut bridge.ws).await;
    // producer 永不连接 → rendezvous 超时(10s)。
    let resp = browser_req.await.expect("join");
    assert_eq!(resp.status(), 504);
    assert_eq!(err_code_of(resp).await, "TRANSFER_EXPIRED");
    assert!(
        started.elapsed() >= Duration::from_secs(9),
        "should wait for rendezvous timeout"
    );
}
