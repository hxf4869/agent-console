//! 文件数据面(§22):transfer 协调与正文流式转发。
//!
//! 固定原则(§22.1/§22.6):WebSocket 只协调 transfer,不传正文;Relay 全程
//! 内存有界 channel pipe,不落任何磁盘/数据库/日志;任一端断开或超时立即
//! 取消另外两端,不继续读取。
//!
//! 端点:
//! - Browser(挂 /agent-console/api,经网关身份头认证):
//!   `POST /sessions/{id}/files/preview|download`、`POST /sessions/{id}/files/upload`(声明)、
//!   `PUT /sessions/{id}/files/upload/{transfer_id}`(正文流)。
//! - Bridge(公网经网关可达,`/agent-console/transfers/*`,不要求浏览器 cookie):
//!   `POST .../producer/{transfer_id}`(请求体=文件字节流)、
//!   `GET .../consumer/{transfer_id}`(响应体=上传字节流)。
//!   鉴权 = Bearer 设备凭据 + X-Transfer-Token + 方向/目标设备校验(§25.4)。
//!
//! 超时常量与 bridge 侧 `files::transfer` 对齐(rendezvous 10s、首字节 30s、
//! 空闲读 30s);并发上限每 browser / 每 device 各 2(§22.6)。

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Request, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;

use agent_console_protocol::{
    codec::{new_message_id, PROTOCOL_VERSION},
    v1::{
        envelope, Envelope, SessionKey, StableErrorCode, TransferDirection, TransferOffer,
        TransferOutcome, TransferReady, TransferResult,
    },
};

use crate::{
    auth::require_identity,
    state::{
        api_error, ct_eq_hex, high_entropy_token, new_request_id, sha256_hex, AppState,
        StableErrorCode as Code,
    },
};

// ---------------------------------------------------------------------------
// 集中常量(与 apps/bridge/src/files/transfer.rs 对齐;§22.3/§22.5/§22.6)
// ---------------------------------------------------------------------------

/// producer 端点前缀:`POST /agent-console/transfers/producer/{transfer_id}`。
pub const PRODUCER_PATH_PREFIX: &str = "/agent-console/transfers/producer/";
/// consumer 端点前缀:`GET /agent-console/transfers/consumer/{transfer_id}`。
pub const CONSUMER_PATH_PREFIX: &str = "/agent-console/transfers/consumer/";
/// transfer token header(Bridge → Relay 出站请求携带)。
pub const TRANSFER_TOKEN_HEADER: &str = "X-Transfer-Token";
/// rendezvous(等待对端连接)超时。
pub const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(10);
/// 首字节超时。
pub const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(30);
/// 空闲读取超时。
pub const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// 单条 transfer 生存期(协调层 TTL;正文不在 Relay 停留)。
pub const TRANSFER_TTL: Duration = Duration::from_secs(10 * 60);
/// pipe 块大小(64 KiB,§22.6)。
pub const CHUNK_BYTES: usize = 64 * 1024;
/// 有界 channel 缓冲块数(固定小缓冲,不读取完整文件进内存)。
pub const CHANNEL_BUFFER_CHUNKS: usize = 4;
/// 每 browser 并发 transfer 上限(§22.6)。
pub const MAX_CONCURRENT_PER_BROWSER: usize = 2;
/// 每 device 并发 transfer 上限(§22.6)。
pub const MAX_CONCURRENT_PER_DEVICE: usize = 2;
/// 单文件上传上限(§22.5:20 MiB;调整必须以真实样本重新确认)。
pub const MAX_UPLOAD_BYTES: u64 = 20 * 1024 * 1024;
/// upload TransferResult 等待窗口(bridge 写临时目录 + 签发 handle)。
pub const RESULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Registry(内存,有界,TTL 过期清理)
// ---------------------------------------------------------------------------

/// HTTP 单 Range 语义(§22.4:仅单 Range;越界由 Bridge 复验)。
#[derive(Debug, Clone, Copy)]
pub struct RangeSpec {
    pub start: u64,
    pub end_inclusive: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TransferInfo {
    pub direction: TransferDirection,
    pub owner: uuid::Uuid,
    pub device: uuid::Uuid,
    pub session: uuid::Uuid,
    /// 安全显示名(sanitized;不来自原始路径)。
    pub file_name: String,
    pub mime: String,
    /// 声明长度(上传必填;下载由 producer 元数据补齐)。
    pub length: Option<u64>,
    pub range: Option<RangeSpec>,
    /// "inline" | "attachment"。
    pub disposition: &'static str,
    /// Bridge 签发的高熵 file handle(仅下载方向;Relay 不解引用,§22.2)。
    pub file_handle: String,
}

/// producer 连接时交出的元数据与正文接收端。
pub struct ProducerHandoff {
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_range: Option<String>,
    pub total_length: Option<u64>,
    pub body: mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>,
}

struct Shared {
    producer_tx: Option<oneshot::Sender<ProducerHandoff>>,
    producer_rx: Option<oneshot::Receiver<ProducerHandoff>>,
    consumer_tx: Option<oneshot::Sender<mpsc::Sender<bytes::Bytes>>>,
    consumer_rx: Option<oneshot::Receiver<mpsc::Sender<bytes::Bytes>>>,
    /// producer 响应体:终态时 drop 使其结束(bridge produce 调用随之返回)。
    producer_ack: Option<mpsc::Sender<bytes::Bytes>>,
    result_tx: Option<oneshot::Sender<TransferResult>>,
    result_rx: Option<oneshot::Receiver<TransferResult>>,
    cancel_tx: Option<watch::Sender<bool>>,
    terminal: bool,
    /// 取消/失败时的稳定码(等待端取消分支透传,如 Bridge 拒绝码)。
    terminal_code: Option<StableErrorCode>,
}

pub struct Transfer {
    pub id: String,
    /// 高熵随机;只在 TransferOffer(设备 WSS)与内部匹配使用,不进 URL query/日志。
    pub token: String,
    pub info: TransferInfo,
    created_at: Instant,
    expires_at: Instant,
    shared: Mutex<Shared>,
}

impl Transfer {
    fn cancel_rx(&self) -> CancellationToken {
        let rx = self
            .shared
            .lock()
            .unwrap()
            .cancel_tx
            .as_ref()
            .map(|tx| tx.subscribe());
        CancellationToken(rx)
    }

    fn expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    fn elapsed_ms(&self) -> u64 {
        self.created_at.elapsed().as_millis() as u64
    }

    /// producer 就绪;transfer 已终态时返回 false。
    fn set_producer(&self, handoff: ProducerHandoff) -> bool {
        let mut g = self.shared.lock().unwrap();
        if g.terminal {
            return false;
        }
        if let Some(wait) = g.producer_tx.take() {
            let _ = wait.send(handoff);
            true
        } else {
            false
        }
    }

    /// consumer 就绪;交出正文写入端。
    fn set_consumer(&self, tx: mpsc::Sender<bytes::Bytes>) -> bool {
        let mut g = self.shared.lock().unwrap();
        if g.terminal {
            return false;
        }
        if let Some(wait) = g.consumer_tx.take() {
            let _ = wait.send(tx);
            true
        } else {
            false
        }
    }

    fn take_result_rx(&self) -> Option<oneshot::Receiver<TransferResult>> {
        self.shared.lock().unwrap().result_rx.take()
    }

    /// bridge/取消侧推送终态结果用。
    fn take_result_tx(&self) -> Option<oneshot::Sender<TransferResult>> {
        self.shared.lock().unwrap().result_tx.take()
    }

    /// 终态(成功):结束 producer ack 响应。
    fn complete(&self) {
        let mut g = self.shared.lock().unwrap();
        g.terminal = true;
        g.producer_ack.take();
    }

    /// 取消/失败(幂等):广播取消信号、通知全部等待者并停止 pump。
    fn cancel(&self, code: StableErrorCode) {
        let (result, wait) = {
            let mut g = self.shared.lock().unwrap();
            if g.terminal {
                return;
            }
            g.terminal = true;
            g.terminal_code = Some(code);
            g.producer_ack.take();
            // 唤醒所有 CancellationToken(等待端立即取消,§22.4 第 6 步)。
            if let Some(cancel_tx) = g.cancel_tx.as_ref() {
                let _ = cancel_tx.send(true);
            }
            let result = TransferResult {
                transfer_id: self.id.clone(),
                outcome: TransferOutcome::Cancelled as i32,
                error_code: code as i32,
                upload_file_handle: String::new(),
            };
            (result, g.result_tx.take())
        };
        if let Some(wait) = wait {
            let _ = wait.send(result);
        }
    }

    /// 取消/失败时的稳定码(未取消返回 None)。
    fn terminal_code(&self) -> Option<StableErrorCode> {
        self.shared.lock().unwrap().terminal_code
    }

    fn is_terminal(&self) -> bool {
        self.shared.lock().unwrap().terminal
    }
}

/// 取消信号;entry 被移除(发送端 drop)同样视为取消(§22.6:重启全部失败)。
pub struct CancellationToken(Option<watch::Receiver<bool>>);

impl CancellationToken {
    pub async fn cancelled(&mut self) {
        match self.0.as_mut() {
            None => std::future::pending::<()>().await,
            Some(rx) => loop {
                if *rx.borrow_and_update() {
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            },
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.as_ref().map(|rx| *rx.borrow()).unwrap_or(true)
    }
}

#[derive(Default)]
pub struct TransferRegistry {
    inner: Mutex<HashMap<String, Arc<Transfer>>>,
    /// 取消时向 Bridge 发送 TransferResult(三端联动,§22.4 第 6 步)。
    hub: std::sync::OnceLock<Arc<crate::realtime::Hub>>,
}

/// create 的失败原因(稳定码)。
pub type CreateError = StableErrorCode;

impl TransferRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注入 Hub(main.rs 装配;取消通知用)。
    pub fn with_hub(hub: Arc<crate::realtime::Hub>) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            hub: std::sync::OnceLock::from(hub),
        }
    }

    fn notify_cancelled(&self, transfer: &Transfer, code: StableErrorCode) {
        if let Some(hub) = self.hub.get() {
            let result = TransferResult {
                transfer_id: transfer.id.clone(),
                outcome: TransferOutcome::Cancelled as i32,
                error_code: code as i32,
                upload_file_handle: String::new(),
            };
            let env = Envelope {
                protocol_version: PROTOCOL_VERSION,
                message_id: new_message_id(),
                correlation_id: new_message_id(),
                sent_at: Some(prost_types::Timestamp::from(SystemTime::now())),
                device_id: transfer.info.device.to_string(),
                agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
                stream_id: String::new(),
                stream_epoch: 0,
                sequence: 0,
                payload: Some(envelope::Payload::TransferResult(result)),
                provider_extension: None,
            };
            hub.send_to_bridge(transfer.info.device, env);
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Transfer>> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    pub fn remove(&self, id: &str) {
        self.inner.lock().unwrap().remove(id);
    }

    /// 取消并移除(处理端统一入口):转移成功时向 Bridge 发送 TransferResult
    /// 取消通知(三端联动,§22.4 第 6 步)。
    pub fn cancel_and_remove(&self, id: &str, code: StableErrorCode) {
        let Some(t) = self.get(id) else {
            return;
        };
        let already = t.is_terminal();
        t.cancel(code);
        if !already {
            self.notify_cancelled(&t, code);
        }
        self.remove(id);
    }

    /// 创建 transfer(高熵 id/token;每 browser / 每 device 并发上限,§22.6)。
    pub fn create(&self, info: TransferInfo) -> Result<Arc<Transfer>, CreateError> {
        let mut g = self.inner.lock().unwrap();
        let per_browser = g
            .values()
            .filter(|t| !t.shared.lock().unwrap().terminal && t.info.owner == info.owner)
            .count();
        if per_browser >= MAX_CONCURRENT_PER_BROWSER {
            return Err(StableErrorCode::RateLimited);
        }
        let per_device = g
            .values()
            .filter(|t| !t.shared.lock().unwrap().terminal && t.info.device == info.device)
            .count();
        if per_device >= MAX_CONCURRENT_PER_DEVICE {
            return Err(StableErrorCode::RateLimited);
        }
        let id = high_entropy_token();
        // 协议 v1 TransferOffer 无独立 token 字段;与 Bridge 对齐的契约是
        // transfer token = transfer_id(高熵随机短期凭据;不经 Envelope 传
        // 第二凭据 §17.2)。见 apps/bridge/src/runtime/transfers.rs。
        let token = id.clone();
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        let (producer_tx, producer_rx) = oneshot::channel::<ProducerHandoff>();
        let (consumer_tx, consumer_rx) = oneshot::channel::<mpsc::Sender<bytes::Bytes>>();
        let (result_tx, result_rx) = oneshot::channel::<TransferResult>();
        let transfer = Arc::new(Transfer {
            id: id.clone(),
            token,
            info,
            created_at: Instant::now(),
            expires_at: Instant::now() + TRANSFER_TTL,
            shared: Mutex::new(Shared {
                producer_tx: Some(producer_tx),
                producer_rx: Some(producer_rx),
                consumer_tx: Some(consumer_tx),
                consumer_rx: Some(consumer_rx),
                producer_ack: None,
                result_tx: Some(result_tx),
                result_rx: Some(result_rx),
                cancel_tx: Some(cancel_tx),
                terminal: false,
                terminal_code: None,
            }),
        });
        drop(_cancel_rx);
        g.insert(id, transfer.clone());
        Ok(transfer)
    }

    /// 过期清理(maintenance ticker 调用):过期条目先取消再移除。
    pub fn sweep(&self) {
        let expired: Vec<(String, Arc<Transfer>)> = {
            let mut g = self.inner.lock().unwrap();
            let ids: Vec<String> = g
                .iter()
                .filter(|(_, t)| t.expired())
                .map(|(k, _)| k.clone())
                .collect();
            ids.iter()
                .filter_map(|k| g.remove(k).map(|t| (k.clone(), t)))
                .collect()
        };
        for (_id, t) in expired {
            let already = t.is_terminal();
            t.cancel(StableErrorCode::TransferExpired);
            if !already {
                self.notify_cancelled(&t, StableErrorCode::TransferExpired);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 路由
// ---------------------------------------------------------------------------

/// Browser 文件端点(挂 /agent-console/api 下)。
pub fn browser_routes() -> Router<AppState> {
    Router::new()
        .route("/sessions/{id}/files/preview", post(preview))
        .route("/sessions/{id}/files/download", post(download))
        .route("/sessions/{id}/files/upload", post(upload_declare))
        .route(
            "/sessions/{id}/files/upload/{transfer_id}",
            put(upload_stream),
        )
}

/// Bridge producer/consumer 端点(挂根;/agent-console/transfers/*)。
pub fn device_routes() -> Router<AppState> {
    Router::new()
        .route(
            &format!("{PRODUCER_PATH_PREFIX}{{transfer_id}}"),
            post(producer),
        )
        .route(
            &format!("{CONSUMER_PATH_PREFIX}{{transfer_id}}"),
            get(consumer),
        )
}

fn err(status: axum::http::StatusCode, code: Code, message: &str) -> Response {
    api_error(
        status,
        code,
        message,
        &new_request_id(),
        serde_json::json!({}),
    )
}

/// transfer 失败/取消的统一响应:超时 504,其余按稳定码默认映射。
fn transfer_error(code: Code, message: &str) -> Response {
    let status = if code == Code::TransferExpired {
        axum::http::StatusCode::GATEWAY_TIMEOUT
    } else {
        crate::state::status_for_code(&code)
    };
    err(status, code, message)
}

// ---------------------------------------------------------------------------
// Browser:preview / download
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileRequest {
    file_handle: String,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    range: Option<RangeBody>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RangeBody {
    start: u64,
    end_inclusive: Option<u64>,
}

async fn preview(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    request: Request,
) -> Response {
    start_download(app, addr, headers, id, request, "inline").await
}

async fn download(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    request: Request,
) -> Response {
    start_download(app, addr, headers, id, request, "attachment").await
}

/// Range 输入:JSON body 的 range 或 HTTP Range header(单段)。
/// 多段/无效/逆序 → TRANSFER_RANGE_INVALID(§22.4)。
fn parse_range(
    body: Option<RangeBody>,
    range_header: Option<&str>,
) -> Result<Option<RangeSpec>, Code> {
    if let Some(r) = body {
        if let Some(e) = r.end_inclusive {
            if r.start > e {
                return Err(Code::TransferRangeInvalid);
            }
        }
        return Ok(Some(RangeSpec {
            start: r.start,
            end_inclusive: r.end_inclusive,
        }));
    }
    match range_header {
        None => Ok(None),
        Some(raw) => {
            let value = raw.trim();
            let Some(spec) = value.strip_prefix("bytes=") else {
                return Err(Code::TransferRangeInvalid);
            };
            if spec.contains(',') {
                return Err(Code::TransferRangeInvalid); // 多段拒绝
            }
            let Some((start, end)) = spec.split_once('-') else {
                return Err(Code::TransferRangeInvalid);
            };
            let Ok(start) = start.trim().parse::<u64>() else {
                return Err(Code::TransferRangeInvalid);
            };
            let end_inclusive = if end.trim().is_empty() {
                None
            } else {
                match end.trim().parse::<u64>() {
                    Ok(e) if e >= start => Some(e),
                    _ => return Err(Code::TransferRangeInvalid),
                }
            };
            Ok(Some(RangeSpec {
                start,
                end_inclusive,
            }))
        }
    }
}

async fn start_download(
    app: AppState,
    addr: SocketAddr,
    headers: HeaderMap,
    session_id: uuid::Uuid,
    request: Request,
    disposition: &'static str,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, session_id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if !app.hub.is_online(row.device_id) {
        return err(
            crate::state::status_for_code(&Code::DeviceOffline),
            Code::DeviceOffline,
            "设备离线",
        );
    }
    // body:JSON(可能带 Range);Range header 作为替代输入。
    let range_header = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let (parsed, range_header) = {
        let bytes = match axum::body::to_bytes(request.into_body(), 64 * 1024).await {
            Ok(b) => b,
            Err(_) => {
                return err(
                    axum::http::StatusCode::BAD_REQUEST,
                    Code::InternalError,
                    "请求体无效",
                )
            }
        };
        let parsed: FileRequest = match serde_json::from_slice(&bytes) {
            Ok(p) => p,
            Err(_) => {
                return err(
                    axum::http::StatusCode::BAD_REQUEST,
                    Code::InternalError,
                    "请求体无效",
                )
            }
        };
        (parsed, range_header)
    };
    if parsed.file_handle.is_empty() {
        return err(
            axum::http::StatusCode::BAD_REQUEST,
            Code::FileHandleInvalid,
            "缺少 file handle",
        );
    }
    let range = match parse_range(parsed.range, range_header.as_deref()) {
        Ok(r) => r,
        Err(code) => return err(axum::http::StatusCode::BAD_REQUEST, code, "Range 无效"),
    };

    let info = TransferInfo {
        direction: TransferDirection::Download,
        owner: ident.owner_id,
        device: row.device_id,
        session: row.id,
        file_name: sanitize_name(parsed.file_name.as_deref().unwrap_or("file")),
        mime: String::new(),
        length: None,
        range,
        disposition,
        file_handle: parsed.file_handle,
    };
    let transfer = match app.transfers.create(info) {
        Ok(t) => t,
        Err(code) => {
            return err(
                crate::state::status_for_code(&code),
                code,
                "并发 transfer 超限",
            )
        }
    };
    if !send_offer(&app, &row, &transfer).await {
        app.transfers
            .cancel_and_remove(&transfer.id, Code::DeviceOffline);
        return err(
            crate::state::status_for_code(&Code::DeviceOffline),
            Code::DeviceOffline,
            "设备离线",
        );
    }

    // 等 producer rendezvous(§22.4 第 4 步)。
    // 注意:std Mutex 不可重入;cancel_rx() 会再次加锁,须在释放 shared 锁后调用。
    let handoff_rx = {
        let mut g = transfer.shared.lock().unwrap();
        g.producer_rx.take().expect("producer wait taken twice")
    };
    let mut cancel_rx = transfer.cancel_rx();
    let handoff = tokio::select! {
        _ = cancel_rx.cancelled() => {
            let code = transfer.terminal_code().unwrap_or(Code::TransferExpired);
            app.transfers.remove(&transfer.id);
            return transfer_error(code, "传输已取消或被拒绝");
        }
        r = handoff_rx => match r {
            Ok(h) => h,
            Err(_) => {
                app.transfers.remove(&transfer.id);
                return transfer_error(Code::TransferExpired, "传输已取消");
            }
        },
        _ = tokio::time::sleep(RENDEZVOUS_TIMEOUT) => {
            app.transfers.cancel_and_remove(&transfer.id, Code::TransferExpired);
            return transfer_error(Code::TransferExpired, "等待设备响应超时");
        }
    };

    // 透传 producer 元数据 + 安全响应头(§22.3)。
    let status = if handoff.content_range.is_some() {
        axum::http::StatusCode::PARTIAL_CONTENT
    } else {
        axum::http::StatusCode::OK
    };
    let mut builder = axum::http::Response::builder()
        .status(status)
        .header("X-Content-Type-Options", "nosniff")
        .header("Content-Security-Policy", "default-src 'none'")
        .header("Cache-Control", "private, no-store");
    let headers_mut = builder.headers_mut().expect("response headers");
    let content_type = handoff
        .content_type
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "application/octet-stream".into());
    if let Ok(v) = axum::http::HeaderValue::from_str(&content_type) {
        headers_mut.insert(axum::http::header::CONTENT_TYPE, v);
    }
    let disposition_value = handoff
        .content_disposition
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            format!(
                "{}; filename=\"{}\"",
                transfer.info.disposition, transfer.info.file_name
            )
        });
    if let Ok(v) = axum::http::HeaderValue::from_str(&disposition_value) {
        headers_mut.insert(axum::http::header::CONTENT_DISPOSITION, v);
    }
    if let Some(cr) = handoff.content_range.as_ref() {
        if let Ok(v) = axum::http::HeaderValue::from_str(cr) {
            headers_mut.insert(axum::http::header::CONTENT_RANGE, v);
        }
    } else if let Some(len) = handoff.total_length {
        headers_mut.insert(
            axum::http::header::CONTENT_LENGTH,
            axum::http::HeaderValue::from(len),
        );
    }
    let body = Body::from_stream(ReceiverStream::new(handoff.body));
    tracing::info!(
        target: "relay::transfers",
        operation = "files.download",
        bytes_total = handoff.total_length.unwrap_or(0),
        "transfer started"
    );
    builder.body(body).unwrap_or_else(|_| {
        err(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Code::InternalError,
            "响应构造失败",
        )
    })
}

// ---------------------------------------------------------------------------
// Browser:upload(声明 + 正文流)
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadDeclare {
    file_name: String,
    #[serde(default)]
    mime: Option<String>,
    length: u64,
}

async fn upload_declare(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<UploadDeclare>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if !app.hub.is_online(row.device_id) {
        return err(
            crate::state::status_for_code(&Code::DeviceOffline),
            Code::DeviceOffline,
            "设备离线",
        );
    }
    if body.length > MAX_UPLOAD_BYTES {
        return err(
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Code::TransferTooLarge,
            "文件超过上传上限",
        );
    }
    let info = TransferInfo {
        direction: TransferDirection::Upload,
        owner: ident.owner_id,
        device: row.device_id,
        session: row.id,
        file_name: sanitize_name(&body.file_name),
        mime: body
            .mime
            .unwrap_or_else(|| "application/octet-stream".into()),
        length: Some(body.length),
        range: None,
        disposition: "attachment",
        file_handle: String::new(),
    };
    let transfer = match app.transfers.create(info) {
        Ok(t) => t,
        Err(code) => {
            return err(
                crate::state::status_for_code(&code),
                code,
                "并发 transfer 超限",
            )
        }
    };
    if !send_offer(&app, &row, &transfer).await {
        app.transfers
            .cancel_and_remove(&transfer.id, Code::DeviceOffline);
        return err(
            crate::state::status_for_code(&Code::DeviceOffline),
            Code::DeviceOffline,
            "设备离线",
        );
    }
    (
        axum::http::StatusCode::CREATED,
        Json(serde_json::json!({
            "transferId": transfer.id,
            "uploadUrl": format!("/agent-console/api/sessions/{id}/files/upload/{}", transfer.id),
        })),
    )
        .into_response()
}

async fn upload_stream(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, transfer_id)): Path<(uuid::Uuid, String)>,
    request: Request,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(transfer) = app.transfers.get(&transfer_id) else {
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    };
    if transfer.info.owner != ident.owner_id
        || transfer.info.session != row.id
        || transfer.info.direction != TransferDirection::Upload
    {
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }
    if transfer.expired() {
        app.transfers
            .cancel_and_remove(&transfer.id, Code::TransferExpired);
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }

    // consumer ready 后才开始读取 Browser body(§22.5 第 4 步)。
    // 注意:std Mutex 不可重入;cancel_rx() 会再次加锁,须在释放 shared 锁后调用。
    let consumer_rx = {
        let mut g = transfer.shared.lock().unwrap();
        g.consumer_rx.take().expect("consumer wait taken twice")
    };
    let mut cancel_rx = transfer.cancel_rx();
    let tx = tokio::select! {
        _ = cancel_rx.cancelled() => {
            let code = transfer.terminal_code().unwrap_or(Code::TransferExpired);
            app.transfers.remove(&transfer.id);
            return transfer_error(code, "传输已取消或被拒绝");
        }
        r = consumer_rx => match r {
            Ok(tx) => tx,
            Err(_) => {
                app.transfers.remove(&transfer.id);
                return transfer_error(Code::TransferExpired, "传输已取消");
            }
        },
        _ = tokio::time::sleep(RENDEZVOUS_TIMEOUT) => {
            app.transfers.cancel_and_remove(&transfer.id, Code::TransferExpired);
            return transfer_error(Code::TransferExpired, "等待设备响应超时");
        }
    };

    // 流式 pump browser body → consumer(有界 channel;失败即取消)。
    let mut cancel_rx2 = transfer.cancel_rx();
    let mut body = request.into_body().into_data_stream();
    let mut first = true;
    let mut transferred: u64 = 0;
    loop {
        let budget = if first {
            FIRST_BYTE_TIMEOUT
        } else {
            IDLE_READ_TIMEOUT
        };
        let chunk = tokio::select! {
            _ = cancel_rx2.cancelled() => {
                let code = transfer.terminal_code().unwrap_or(Code::TransferExpired);
                app.transfers.remove(&transfer.id);
                return transfer_error(code, "传输已取消或被拒绝");
            }
            n = tokio::time::timeout(budget, body.next()) => match n {
                Err(_) => {
                    app.transfers.cancel_and_remove(&transfer.id, Code::TransferExpired);
                    return transfer_error(Code::TransferExpired, "读取请求体超时");
                }
                Ok(None) => break,
                Ok(Some(Err(_))) => {
                    app.transfers.cancel_and_remove(&transfer.id, Code::InternalError);
                    return err(axum::http::StatusCode::BAD_REQUEST, Code::InternalError, "请求体读取失败");
                }
                Ok(Some(Ok(c))) => c,
            }
        };
        first = false;
        transferred += chunk.len() as u64;
        if transferred > MAX_UPLOAD_BYTES {
            app.transfers
                .cancel_and_remove(&transfer.id, Code::TransferTooLarge);
            return err(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                Code::TransferTooLarge,
                "文件超过上传上限",
            );
        }
        if tx.send(chunk).await.is_err() {
            // consumer 断开:三端取消(§22.4 第 6 步)。
            app.transfers
                .cancel_and_remove(&transfer.id, Code::InternalError);
            return transfer_error(Code::TransferExpired, "设备中断传输");
        }
    }
    drop(tx); // consumer 响应体结束

    // 等 bridge TransferResult(写入临时目录校验 + upload handle)。
    let result_rx = transfer.take_result_rx();
    let mut cancel_rx3 = transfer.cancel_rx();
    let result = match result_rx {
        Some(rx) => tokio::select! {
            _ = cancel_rx3.cancelled() => None,
            r = rx => r.ok(),
            _ = tokio::time::sleep(RESULT_WAIT_TIMEOUT) => None,
        },
        None => None,
    };
    let latency = transfer.elapsed_ms();
    app.transfers.remove(&transfer.id);
    match result {
        Some(result) => {
            tracing::info!(
                target: "relay::transfers",
                operation = "files.upload",
                bytes_total = transferred,
                latency_ms = latency,
                "transfer completed"
            );
            let outcome = TransferOutcome::try_from(result.outcome)
                .map(|o| o.as_str_name().to_string())
                .unwrap_or_else(|_| "TRANSFER_OUTCOME_UNSPECIFIED".into());
            let error_code = if result.error_code == 0 {
                serde_json::Value::Null
            } else {
                serde_json::json!(crate::state::stable_code_name(result.error_code))
            };
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({
                    "transferId": transfer.id,
                    "outcome": outcome,
                    "errorCode": error_code,
                    "uploadFileHandle": if result.upload_file_handle.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(result.upload_file_handle)
                    },
                })),
            )
                .into_response()
        }
        None => err(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Code::TransferExpired,
            "设备未回传结果",
        ),
    }
}

// ---------------------------------------------------------------------------
// Bridge:producer / consumer
// ---------------------------------------------------------------------------

struct DeviceAuth {
    device: uuid::Uuid,
}

/// Bearer 设备凭据认证(对齐 bridge_ws;§25.4:不要求浏览器 cookie)。
async fn authenticate_device(app: &AppState, headers: &HeaderMap) -> Result<DeviceAuth, Response> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("");
    if bearer.is_empty() {
        return Err(err(
            axum::http::StatusCode::UNAUTHORIZED,
            Code::AuthRequired,
            "缺少设备凭据",
        ));
    }
    let digest = sha256_hex(bearer.as_bytes());
    let row = sqlx::query_as::<_, (uuid::Uuid, bool)>(
        "SELECT id, revoked_at IS NOT NULL FROM devices WHERE credential_digest = $1",
    )
    .bind(&digest)
    .fetch_optional(&app.db)
    .await;
    match row {
        Ok(Some((device, revoked))) if !revoked => Ok(DeviceAuth { device }),
        Ok(Some(_)) => Err(err(
            axum::http::StatusCode::UNAUTHORIZED,
            Code::DeviceRevoked,
            "设备已撤销",
        )),
        _ => Err(err(
            axum::http::StatusCode::UNAUTHORIZED,
            Code::AuthRequired,
            "设备凭据无效",
        )),
    }
}

/// transfer 存在性 + token + 方向 + 目标设备校验。
fn authorize_transfer(
    transfer: Option<Arc<Transfer>>,
    headers: &HeaderMap,
    device: uuid::Uuid,
    direction: TransferDirection,
) -> Result<Arc<Transfer>, Response> {
    let provided = headers
        .get(TRANSFER_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some(transfer) = transfer else {
        return Err(err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        ));
    };
    // token 恒定时间比较(摘要后 ct_eq,对齐既有凭据比较模式)。
    let ok = !provided.is_empty()
        && ct_eq_hex(
            &sha256_hex(provided.as_bytes()),
            &sha256_hex(transfer.token.as_bytes()),
        );
    if !ok || transfer.info.device != device || transfer.info.direction != direction {
        return Err(err(
            axum::http::StatusCode::UNAUTHORIZED,
            Code::AuthRequired,
            "transfer 鉴权失败",
        ));
    }
    Ok(transfer)
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// Bridge producer:请求体 = 文件字节流;响应头尽早返回(§22.4 第 4-5 步)。
async fn producer(
    State(app): State<AppState>,
    Path(transfer_id): Path<String>,
    request: Request,
) -> Response {
    let headers = request.headers().clone();
    let device = match authenticate_device(&app, &headers).await {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let transfer = match authorize_transfer(
        app.transfers.get(&transfer_id),
        &headers,
        device.device,
        TransferDirection::Download,
    ) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if transfer.expired() {
        app.transfers
            .cancel_and_remove(&transfer.id, Code::TransferExpired);
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }

    // 元数据:X-Transfer-* 优先,标准 Content-Type/Content-Range 兜底。
    let (ack_tx, ack_rx) = mpsc::channel::<bytes::Bytes>(1);
    let (body_tx, body_rx) =
        mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(CHANNEL_BUFFER_CHUNKS);
    let handoff = ProducerHandoff {
        content_type: header_str(&headers, "X-Transfer-Content-Type")
            .or_else(|| header_str(&headers, "content-type")),
        content_disposition: header_str(&headers, "X-Transfer-Disposition"),
        content_range: header_str(&headers, "X-Transfer-Content-Range")
            .or_else(|| header_str(&headers, "content-range")),
        total_length: header_str(&headers, "X-Transfer-Total-Length")
            .or_else(|| header_str(&headers, "X-Transfer-Length"))
            .and_then(|s| s.parse::<u64>().ok()),
        body: body_rx,
    };
    if !transfer.set_producer(handoff) {
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }
    {
        let mut g = transfer.shared.lock().unwrap();
        g.producer_ack = Some(ack_tx);
    }

    // pump:producer 请求体 → 有界 channel → browser;带首字节/空闲超时。
    let registry = app.transfers.clone();
    let t = transfer.clone();
    let mut cancel_rx = transfer.cancel_rx();
    let mut body = request.into_body().into_data_stream();
    tokio::spawn(async move {
        let exit = pump_body(&mut cancel_rx, &mut body, &body_tx).await;
        match exit {
            PumpExit::Eof => {
                t.complete();
                registry.remove(&t.id);
            }
            PumpExit::Timeout => {
                // 超时:三端取消并通知 Bridge(§22.4 第 6 步)。
                registry.cancel_and_remove(&t.id, Code::TransferExpired);
            }
            PumpExit::StreamError | PumpExit::SinkClosed => {
                registry.cancel_and_remove(&t.id, Code::InternalError);
            }
            // 外部取消:notify 已由发起方发出;幂等收尾。
            PumpExit::Cancelled => {
                registry.cancel_and_remove(&t.id, Code::TransferExpired);
            }
        }
    });

    // 响应头尽早返回;正文在终态时结束(bridge produce 调用随之收尾)。
    axum::http::Response::builder()
        .status(axum::http::StatusCode::ACCEPTED)
        .header("Cache-Control", "private, no-store")
        .body(Body::from_stream(
            ReceiverStream::new(ack_rx).map(|b| Ok::<_, std::io::Error>(b)),
        ))
        .unwrap_or_else(|_| {
            err(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "响应构造失败",
            )
        })
}

/// Bridge consumer:响应体 = 上传字节流(§22.5 第 3-4 步)。
async fn consumer(
    State(app): State<AppState>,
    Path(transfer_id): Path<String>,
    request: Request,
) -> Response {
    let headers = request.headers().clone();
    let device = match authenticate_device(&app, &headers).await {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let transfer = match authorize_transfer(
        app.transfers.get(&transfer_id),
        &headers,
        device.device,
        TransferDirection::Upload,
    ) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if transfer.expired() {
        app.transfers
            .cancel_and_remove(&transfer.id, Code::TransferExpired);
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }
    let (tx, rx) = mpsc::channel::<bytes::Bytes>(CHANNEL_BUFFER_CHUNKS);
    if !transfer.set_consumer(tx) {
        return err(
            axum::http::StatusCode::NOT_FOUND,
            Code::TransferExpired,
            "transfer 不存在或已过期",
        );
    }
    axum::http::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("Cache-Control", "private, no-store")
        .body(Body::from_stream(
            ReceiverStream::new(rx).map(|b| Ok::<_, std::io::Error>(b)),
        ))
        .unwrap_or_else(|_| {
            err(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "响应构造失败",
            )
        })
}

/// 通用 pump:src → 有界 channel;首字节/空闲超时;取消立即停止(§22.6)。
/// pump 退出原因(决定 transfer 终态与通知方向)。
enum PumpExit {
    /// 上游正常 EOF。
    Eof,
    /// 首字节/空闲超时(已向 channel 写入错误块)。
    Timeout,
    /// 上游流错误(已向 channel 写入错误块)。
    StreamError,
    /// 外部取消(已标记 terminal)。
    Cancelled,
    /// 下游关闭(浏览器断开)。
    SinkClosed,
}

async fn pump_body(
    cancel_rx: &mut CancellationToken,
    body: &mut (impl futures::Stream<Item = Result<bytes::Bytes, axum::Error>> + Unpin),
    tx: &mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
) -> PumpExit {
    let mut first = true;
    loop {
        let budget = if first {
            FIRST_BYTE_TIMEOUT
        } else {
            IDLE_READ_TIMEOUT
        };
        let chunk = tokio::select! {
            _ = cancel_rx.cancelled() => {
                let _ = tx.try_send(Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "transfer cancelled",
                )));
                return PumpExit::Cancelled;
            }
            n = tokio::time::timeout(budget, body.next()) => match n {
                Err(_) => {
                    let _ = tx.try_send(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "idle read timeout",
                    )));
                    return PumpExit::Timeout;
                }
                Ok(None) => return PumpExit::Eof,
                Ok(Some(Err(_))) => {
                    let _ = tx.try_send(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "stream error",
                    )));
                    return PumpExit::StreamError;
                }
                Ok(Some(Ok(c))) => c,
            }
        };
        first = false;
        if tx.send(Ok(chunk)).await.is_err() {
            return PumpExit::SinkClosed;
        }
    }
}

// ---------------------------------------------------------------------------
// Bridge → Relay 协调消息(经 realtime::handle_bridge_envelope)
// ---------------------------------------------------------------------------

/// TransferReady(拒绝):取消 transfer 并把拒绝码传给等待端。
pub async fn on_bridge_ready(app: &AppState, device: uuid::Uuid, ready: &TransferReady) {
    let Some(transfer) = app.transfers.get(&ready.transfer_id) else {
        return;
    };
    if transfer.info.device != device {
        return;
    }
    if !ready.ready {
        let code = StableErrorCode::try_from(ready.rejection_code)
            .unwrap_or(StableErrorCode::InternalError);
        transfer.cancel(code);
    }
}

/// TransferResult:upload 终态路由给等待的 browser PUT。
pub async fn on_bridge_result(app: &AppState, device: uuid::Uuid, result: &TransferResult) {
    let Some(transfer) = app.transfers.get(&result.transfer_id) else {
        return;
    };
    if transfer.info.device != device {
        return;
    }
    if let Some(tx) = transfer.take_result_tx() {
        let _ = tx.send(result.clone());
    }
    transfer.complete();
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn sanitize_name(name: &str) -> String {
    let trimmed = name.trim();
    let safe: String = trimmed
        .chars()
        .filter(|c| c.is_ascii_graphic() && *c != '"' && *c != '\\')
        .take(128)
        .collect();
    if safe.is_empty() {
        "file".into()
    } else {
        safe
    }
}

/// 校验会话归属(owner 的未撤销设备)。
async fn owned_session(
    app: &AppState,
    owner: uuid::Uuid,
    id: uuid::Uuid,
) -> Result<crate::sessions::store::SessionRow, Response> {
    let row = crate::sessions::store::get_session(&app.db, id)
        .await
        .map_err(|_| {
            err(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "查询会话失败",
            )
        })?;
    let Some(row) = row else {
        return Err(err(
            axum::http::StatusCode::NOT_FOUND,
            Code::SessionNotFound,
            "会话不存在",
        ));
    };
    let owned = sqlx::query_as::<_, (uuid::Uuid,)>(
        "SELECT owner_id FROM devices WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(row.device_id)
    .fetch_optional(&app.db)
    .await
    .ok()
    .flatten();
    match owned {
        Some((o,)) if o == owner => Ok(row),
        _ => Err(err(
            axum::http::StatusCode::NOT_FOUND,
            Code::SessionNotFound,
            "会话不存在",
        )),
    }
}

/// 经 Hub 向目标设备发送 TransferOffer(WSS 只协调,不传正文,§22.1)。
async fn send_offer(
    app: &AppState,
    row: &crate::sessions::store::SessionRow,
    transfer: &Transfer,
) -> bool {
    let offer = TransferOffer {
        transfer_id: transfer.id.clone(),
        direction: transfer.info.direction as i32,
        session_key: Some(SessionKey {
            device_id: row.device_id.to_string(),
            agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
            native_session_id: row.native_session_id.clone(),
            relay_session_uuid: row.id.to_string(),
        }),
        file_name: transfer.info.file_name.clone(),
        mime_type: transfer.info.mime.clone(),
        size_bytes: transfer.info.length.unwrap_or(0),
        file_handle: offer_file_handle(transfer),
        range_start: transfer.info.range.map(|r| r.start).unwrap_or(0),
        range_end_inclusive: transfer.info.range.and_then(|r| r.end_inclusive),
        expires_at: Some(prost_types::Timestamp::from(
            SystemTime::now() + TRANSFER_TTL,
        )),
    };
    let mut env = Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: new_message_id(),
        sent_at: Some(prost_types::Timestamp::from(SystemTime::now())),
        device_id: row.device_id.to_string(),
        agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(envelope::Payload::TransferOffer(offer)),
        provider_extension: None,
    };
    env.correlation_id = env.message_id.clone();
    app.hub.send_to_bridge(row.device_id, env)
}

/// 仅下载方向携带 file_handle(§22.2:Relay 不需要知道本机路径)。
fn offer_file_handle(transfer: &Transfer) -> String {
    if transfer.info.direction == TransferDirection::Download {
        transfer.info.file_handle.clone()
    } else {
        String::new()
    }
}
