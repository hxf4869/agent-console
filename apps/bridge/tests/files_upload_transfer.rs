//! 上传落盘与 producer/consumer 出站传输测试(§22.5、§22.4、§29.1:
//! 上传校验+句柄、下载/Range 流、取消两端停止、超限拒绝、临时目录清理)。
//! Relay 用本地 axum 测试服务器模拟;全部合成数据。

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::Path as AxPath;
use axum::http::{HeaderMap, StatusCode};
use bridge::config::RelayUrls;
use bridge::files::{
    consume, open_slice, produce, remove_upload_dir, sweep_expired_uploads, FileGrantManager,
    GrantAction, GrantActions, GrantSource, TransferConfig, TransferError, UploadSink,
    CONSUMER_PATH_PREFIX, DEFAULT_UPLOAD_TTL, MAX_UPLOAD_BYTES, PRODUCER_PATH_PREFIX,
    TRANSFER_TOKEN_HEADER,
};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

const DEV: &str = "device-1";
const SESS: &str = "session-1";
const CRED: &str = "test-device-credential";

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// 端点完整 URL 由 config::RelayUrls 统一派生(与 runtime 同一入口)。
fn producer_url_at(base: &str, id: &str) -> String {
    RelayUrls::parse(base).unwrap().transfer_producer_url(id)
}

fn consumer_url_at(base: &str, id: &str) -> String {
    RelayUrls::parse(base).unwrap().transfer_consumer_url(id)
}

async fn spawn_app(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn config() -> TransferConfig {
    TransferConfig {
        rendezvous: Duration::from_secs(5),
        first_byte: Duration::from_secs(5),
        idle_read: Duration::from_secs(5),
    }
}

fn synthetic(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

// ---------------------------------------------------------------------------
// UploadSink(§22.5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upload_writes_validates_and_issues_handle() {
    let tmp = tempfile::tempdir().unwrap();
    let upload_root = tmp.path().join("uploads");
    let mgr = FileGrantManager::new();

    let mut sink = UploadSink::create(&upload_root, MAX_UPLOAD_BYTES)
        .await
        .unwrap();
    // PNG 头 + 数据:MIME 校验基于实际内容
    let png = [b"\x89PNG\r\n\x1a\n".as_slice(), b"pixels"].concat();
    sink.write_chunk(&png).await.unwrap();
    assert_eq!(sink.size(), png.len() as u64);
    assert_eq!(sink.sniffed_mime().as_deref(), Some("image/png"));

    let outcome = sink
        .finish(&mgr, DEV, SESS, DEFAULT_UPLOAD_TTL)
        .await
        .unwrap();
    assert_eq!(outcome.size, png.len() as u64);
    assert_eq!(
        outcome.handle.source_kind,
        bridge::files::GrantSourceKind::Upload
    );
    // handle 只属于当前 session,且可复验读取
    let mut vf = mgr
        .resolve_for_read(DEV, SESS, &outcome.handle.token, GrantAction::Download)
        .unwrap();
    use std::io::Read;
    let mut got = Vec::new();
    vf.file.read_to_end(&mut got).unwrap();
    assert_eq!(got, png);
    // 其他 session 不可读
    assert_eq!(
        mgr.resolve_for_read(DEV, "other", &outcome.handle.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "FILE_HANDLE_INVALID"
    );

    // 生命周期清理记录:只删 Bridge 自己的临时目录
    let dir = outcome.cleanup.dir.clone();
    assert!(dir.starts_with(&upload_root));
    remove_upload_dir(&outcome.cleanup).await.unwrap();
    assert!(!dir.exists());
}

#[tokio::test]
async fn upload_over_limit_rejected_and_partial_cleaned() {
    let tmp = tempfile::tempdir().unwrap();
    let upload_root = tmp.path().join("uploads");
    let mut sink = UploadSink::create(&upload_root, 8).await.unwrap();

    sink.write_chunk(b"12345678").await.unwrap();
    let err = sink.write_chunk(b"9").await.unwrap_err();
    assert_eq!(err.stable_code(), "TRANSFER_TOO_LARGE");

    // Drop 兜底清理半成品
    let sink_path = sink.path().to_owned();
    drop(sink);
    assert!(!sink_path.exists());

    // 显式 abort 亦清理
    let mut sink2 = UploadSink::create(&upload_root, 8).await.unwrap();
    sink2.write_chunk(b"abc").await.unwrap();
    let dir2 = sink2.path().to_owned();
    sink2.abort().await;
    assert!(!dir2.exists());
}

#[tokio::test]
async fn upload_sweep_by_ttl_removes_only_temp_dirs() {
    let tmp = tempfile::tempdir().unwrap();
    let upload_root = tmp.path().join("uploads");
    std::fs::create_dir_all(&upload_root).unwrap();
    // 模拟进程崩溃留下的孤儿上传目录(Drop/abort 路径不会产生它):
    // sweep 只看目录 mtime,按 TTL 清理。
    let orphan = upload_root.join("u-orphan");
    std::fs::create_dir(&orphan).unwrap();
    std::fs::write(orphan.join("payload"), b"partial").unwrap();

    let future = std::time::SystemTime::now() + Duration::from_secs(3600);
    let removed = sweep_expired_uploads(&upload_root, future).await.unwrap();
    assert_eq!(removed, 1);
    assert!(!orphan.exists());
    assert!(upload_root.exists(), "上传根目录本身不受影响");

    // 未到期的不清:cutoff 取过去时间,fresh 的 mtime(≈现在)晚于 cutoff。
    let fresh = upload_root.join("u-fresh");
    std::fs::create_dir(&fresh).unwrap();
    let past = std::time::SystemTime::now() - Duration::from_secs(3600);
    let removed = sweep_expired_uploads(&upload_root, past).await.unwrap();
    assert_eq!(removed, 0);
    assert!(fresh.exists());
}

// ---------------------------------------------------------------------------
// producer(§22.4 第 4-5 步)
// ---------------------------------------------------------------------------

async fn grant_slice(
    mgr: &FileGrantManager,
    root: &std::path::Path,
    rel: &str,
    range: Option<&str>,
) -> bridge::files::FileSlice {
    let h = mgr
        .issue(
            DEV,
            SESS,
            root,
            std::path::Path::new(rel),
            GrantActions::all(),
            GrantSource::SessionReference {
                evidence: "tool:diff test".into(),
            },
            Duration::from_secs(60),
        )
        .unwrap();
    let vf = mgr
        .resolve_for_read(DEV, SESS, &h.token, GrantAction::Download)
        .unwrap();
    open_slice(vf, bridge::files::TransferMode::Download, range)
        .await
        .unwrap()
}

#[tokio::test]
async fn producer_streams_file_with_auth_headers() {
    let tmp = tempfile::tempdir().unwrap();
    let content = synthetic(200 * 1024); // 跨多个 64KiB 块
    std::fs::write(tmp.path().join("data.txt"), &content).unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, HeaderMap, Bytes)>(1);
    let app = axum::Router::new().route(
        &format!("{PRODUCER_PATH_PREFIX}{{id}}"),
        axum::routing::post(
            |AxPath(id): AxPath<String>, headers: HeaderMap, body: Bytes| async move {
                tx.send((id, headers, body)).await.unwrap();
                StatusCode::OK
            },
        ),
    );
    let base = spawn_app(app).await;

    let mgr = FileGrantManager::new();
    let slice = grant_slice(&mgr, tmp.path(), "data.txt", None).await;
    let out = produce(
        &client(),
        &producer_url_at(&base, "transfer-1"),
        CRED,
        "tok-1",
        slice,
        config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.status, 200);
    assert_eq!(out.bytes_sent, content.len() as u64);

    let (id, headers, body) = rx.recv().await.unwrap();
    assert_eq!(id, "transfer-1");
    assert_eq!(headers.get(TRANSFER_TOKEN_HEADER).unwrap(), "tok-1");
    assert_eq!(
        headers.get(axum::http::header::AUTHORIZATION).unwrap(),
        &format!("Bearer {CRED}")
    );
    assert_eq!(body, content);
}

#[tokio::test]
async fn producer_sends_ranged_slice_only() {
    let tmp = tempfile::tempdir().unwrap();
    let content: Vec<u8> = (0..100u8).collect();
    std::fs::write(tmp.path().join("r.txt"), &content).unwrap();

    let mgr = FileGrantManager::new();
    let slice = grant_slice(&mgr, tmp.path(), "r.txt", Some("bytes=10-19")).await;
    assert_eq!(slice.body_len, 10);
    assert_eq!(
        slice.content_range_value().as_deref(),
        Some("bytes 10-19/100")
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let app = axum::Router::new().route(
        &format!("{PRODUCER_PATH_PREFIX}{{id}}"),
        axum::routing::post(move |_h: AxPath<String>, body: Bytes| async move {
            tx.send(body).await.unwrap();
            StatusCode::OK
        }),
    );
    let base = spawn_app(app).await;

    let out = produce(
        &client(),
        &producer_url_at(&base, "t"),
        CRED,
        "tok",
        slice,
        config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.bytes_sent, 10);
    assert_eq!(rx.recv().await.unwrap(), &content[10..20]);
}

#[tokio::test]
async fn producer_rejection_maps_to_error() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("f.txt"), b"hello").unwrap();

    let app = axum::Router::new().route(
        &format!("{PRODUCER_PATH_PREFIX}{{id}}"),
        axum::routing::post(|| async { StatusCode::FORBIDDEN }),
    );
    let base = spawn_app(app).await;

    let mgr = FileGrantManager::new();
    let slice = grant_slice(&mgr, tmp.path(), "f.txt", None).await;
    let err = produce(
        &client(),
        &producer_url_at(&base, "t"),
        CRED,
        "tok",
        slice,
        config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Rejected(403)), "got {err:?}");
}

#[tokio::test]
async fn producer_cancel_stops_streaming() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("f.txt"), synthetic(300 * 1024)).unwrap();

    // 服务端逐块慢读:produce 应被取消,不再继续发送。
    let app = axum::Router::new().route(
        &format!("{PRODUCER_PATH_PREFIX}{{id}}"),
        axum::routing::post(|request: axum::extract::Request| async move {
            let mut stream = request.into_body().into_data_stream();
            while let Some(Ok(_)) = stream.next().await {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            StatusCode::OK
        }),
    );
    let base = spawn_app(app).await;

    let mgr = FileGrantManager::new();
    let slice = grant_slice(&mgr, tmp.path(), "f.txt", None).await;
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        c2.cancel();
    });
    let started = std::time::Instant::now();
    let err = produce(
        &client(),
        &producer_url_at(&base, "t"),
        CRED,
        "tok",
        slice,
        config(),
        &cancel,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Cancelled), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(2), "取消应立即生效");
}

// ---------------------------------------------------------------------------
// consumer(§22.5 第 3-5 步)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_streams_into_sink_and_finishes() {
    let tmp = tempfile::tempdir().unwrap();
    let upload_root = tmp.path().join("uploads");
    let content: Vec<u8> = (0..150u32).map(|i| (i % 253) as u8).collect();
    let chunks: Vec<Vec<u8>> = content.chunks(64).map(<[u8]>::to_vec).collect();

    let app = axum::Router::new().route(
        &format!("{CONSUMER_PATH_PREFIX}{{id}}"),
        axum::routing::get(
            move |AxPath(id): AxPath<String>, headers: HeaderMap| async move {
                assert_eq!(id, "up-1");
                assert_eq!(headers.get(TRANSFER_TOKEN_HEADER).unwrap(), "tok-up");
                let stream = futures::stream::iter(
                    chunks
                        .into_iter()
                        .map(|c| Ok::<Bytes, std::convert::Infallible>(Bytes::from(c))),
                );
                axum::body::Body::from_stream(stream)
            },
        ),
    );
    let base = spawn_app(app).await;

    let mgr = FileGrantManager::new();
    let mut sink = UploadSink::create(&upload_root, MAX_UPLOAD_BYTES)
        .await
        .unwrap();
    let outcome = consume(
        &client(),
        &consumer_url_at(&base, "up-1"),
        CRED,
        "tok-up",
        &mut sink,
        config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(outcome.bytes_read, content.len() as u64);

    let uploaded = sink
        .finish(&mgr, DEV, SESS, DEFAULT_UPLOAD_TTL)
        .await
        .unwrap();
    assert_eq!(uploaded.size, content.len() as u64);
    let mut vf = mgr
        .resolve_for_read(DEV, SESS, &uploaded.handle.token, GrantAction::Download)
        .unwrap();
    use std::io::Read;
    let mut got = Vec::new();
    vf.file.read_to_end(&mut got).unwrap();
    assert_eq!(got, content);
    remove_upload_dir(&uploaded.cleanup).await.unwrap();
}

#[tokio::test]
async fn consumer_over_upload_limit_maps_too_large() {
    let tmp = tempfile::tempdir().unwrap();
    let upload_root = tmp.path().join("uploads");

    let app = axum::Router::new().route(
        &format!("{CONSUMER_PATH_PREFIX}{{id}}"),
        axum::routing::get(|| async {
            let big = vec![7u8; 128];
            let stream = futures::stream::once(async move {
                Ok::<Bytes, std::convert::Infallible>(Bytes::from(big))
            });
            axum::body::Body::from_stream(stream)
        }),
    );
    let base = spawn_app(app).await;

    let mut sink = UploadSink::create(&upload_root, 64).await.unwrap();
    let err = consume(
        &client(),
        &consumer_url_at(&base, "t"),
        CRED,
        "tok",
        &mut sink,
        config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Files(_)), "got {err:?}");
    assert_eq!(err.stable_code(), "TRANSFER_TOO_LARGE");
    sink.abort().await;
}

#[tokio::test]
async fn consumer_timeouts_are_classified() {
    let tmp = tempfile::tempdir().unwrap();

    // rendezvous:服务端迟迟不响应 → TRANSFER_EXPIRED
    let hang_app = axum::Router::new().route(
        &format!("{CONSUMER_PATH_PREFIX}{{id}}"),
        axum::routing::get(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            StatusCode::OK
        }),
    );
    let base = spawn_app(hang_app).await;
    let mut sink = UploadSink::create(&tmp.path().join("u"), MAX_UPLOAD_BYTES)
        .await
        .unwrap();
    let cfg = TransferConfig {
        rendezvous: Duration::from_millis(100),
        first_byte: Duration::from_secs(5),
        idle_read: Duration::from_secs(5),
    };
    let err = consume(
        &client(),
        &consumer_url_at(&base, "t"),
        CRED,
        "tok",
        &mut sink,
        cfg,
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Timeout(_)), "got {err:?}");
    assert_eq!(err.stable_code(), "TRANSFER_EXPIRED");
    sink.abort().await;

    // 首字节:响应头立即返回,但 body 不给数据
    let app2 = axum::Router::new().route(
        &format!("{CONSUMER_PATH_PREFIX}{{id}}"),
        axum::routing::get(|| async {
            axum::body::Body::from_stream(futures::stream::pending::<
                Result<Bytes, std::convert::Infallible>,
            >())
        }),
    );
    let base2 = spawn_app(app2).await;
    let mut sink2 = UploadSink::create(&tmp.path().join("u2"), MAX_UPLOAD_BYTES)
        .await
        .unwrap();
    let cfg2 = TransferConfig {
        rendezvous: Duration::from_secs(5),
        first_byte: Duration::from_millis(100),
        idle_read: Duration::from_secs(5),
    };
    let err2 = consume(
        &client(),
        &consumer_url_at(&base2, "t"),
        CRED,
        "tok",
        &mut sink2,
        cfg2,
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err2, TransferError::Timeout(_)), "got {err2:?}");
    sink2.abort().await;
}

#[tokio::test]
async fn consumer_cancel_stops_reading() {
    let chunks = Arc::new((0..100).map(|i| vec![i as u8; 1024]).collect::<Vec<_>>());
    let app = axum::Router::new().route(
        &format!("{CONSUMER_PATH_PREFIX}{{id}}"),
        axum::routing::get(move || async move {
            let chunks = Arc::clone(&chunks);
            let stream = futures::stream::iter((*chunks).clone()).then(|c| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<Bytes, std::convert::Infallible>(Bytes::from(c))
            });
            axum::body::Body::from_stream(stream)
        }),
    );
    let base = spawn_app(app).await;
    let tmp = tempfile::tempdir().unwrap();
    let mut sink = UploadSink::create(&tmp.path().join("u"), MAX_UPLOAD_BYTES)
        .await
        .unwrap();

    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        c2.cancel();
    });
    let started = std::time::Instant::now();
    let err = consume(
        &client(),
        &consumer_url_at(&base, "t"),
        CRED,
        "tok",
        &mut sink,
        config(),
        &cancel,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Cancelled), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    sink.abort().await;
}

// ---------------------------------------------------------------------------
// URL 约定
// ---------------------------------------------------------------------------

#[test]
fn url_conventions() {
    assert_eq!(
        producer_url_at("https://relay.example/", "t1"),
        format!("https://relay.example{PRODUCER_PATH_PREFIX}t1")
    );
    assert_eq!(
        consumer_url_at("https://relay.example", "t1"),
        format!("https://relay.example{CONSUMER_PATH_PREFIX}t1")
    );
}
