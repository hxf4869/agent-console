//! producer/consumer 出站 HTTPS 连接(权威规格 §22.4、§22.5 Bridge 侧)。
//!
//! - Mac 只主动建立出站连接;对 Relay 的 producer/consumer HTTPS endpoint
//!   发起请求,认证为 Bearer device credential + 短期 transfer token。
//! - 端点完整 URL 由 [`crate::config::RelayUrls`] 统一派生
//!   (`transfer_producer_url`/`transfer_consumer_url`);路径常量集中定义于
//!   `crate::config`,本模块 re-export 供 Relay 侧对齐引用:
//!   [`PRODUCER_PATH_PREFIX`]、[`CONSUMER_PATH_PREFIX`],以及
//!   [`TRANSFER_TOKEN_HEADER`] 等本模块自己的 header 约定。
//! - 文件正文经有界缓冲流式收发(64 KiB 块),不整文件进内存(§22.6);
//! - rendezvous / 首字节 / 空闲读取超时集中定义([`TransferConfig`]),
//!   供上层与 Relay 对齐;任一端取消立即停止读写(§22.4 第 6 步)。
//!
//! 错误分类见 [`TransferError`];`stable_code()` 映射 §27.6
//! (超时 → `TRANSFER_EXPIRED`,大小 → `TRANSFER_TOO_LARGE`,取消视为正常中止)。

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::Stream;
use futures::task::{Context, Poll};
use futures::StreamExt;
use tokio::time::Sleep;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

use super::policy::FileSlice;
use super::upload::UploadSink;
use super::FilesError;

// ---------------------------------------------------------------------------
// 与 Relay 对齐的约定(路径常量见 crate::config,此处 re-export;header 集中)
// ---------------------------------------------------------------------------

pub use crate::config::{CONSUMER_PATH_PREFIX, PRODUCER_PATH_PREFIX};
/// 短期 transfer token 的自定义 header。
pub const TRANSFER_TOKEN_HEADER: &str = "X-Transfer-Token";
/// device credential 的 Bearer scheme(Authorization: Bearer <credential>)。
pub const DEVICE_CREDENTIAL_SCHEME: &str = "Bearer";
/// 信息性 header:本次响应体/请求体总长度(Relay 可用于转发元数据)。
pub const TRANSFER_LENGTH_HEADER: &str = "X-Transfer-Length";

/// rendezvous(建立连接到响应头)默认超时。
pub const DEFAULT_RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(10);
/// 首字节默认超时。
pub const DEFAULT_FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(30);
/// 空闲读取默认超时。
pub const DEFAULT_IDLE_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// 超时配置(默认值即上述常量;测试可注入更短值)。
#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    pub rendezvous: Duration,
    pub first_byte: Duration,
    pub idle_read: Duration,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            rendezvous: DEFAULT_RENDEZVOUS_TIMEOUT,
            first_byte: DEFAULT_FIRST_BYTE_TIMEOUT,
            idle_read: DEFAULT_IDLE_READ_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutPhase {
    Rendezvous,
    FirstByte,
    IdleRead,
}

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// 任一端取消;属正常中止路径,上层不应作为错误呈现给用户。
    #[error("transfer cancelled")]
    Cancelled,
    #[error("relay rejected with status {0}")]
    Rejected(u16),
    #[error("transfer timed out at {0:?}")]
    Timeout(TimeoutPhase),
    #[error("http client error")]
    Http(#[from] reqwest::Error),
    #[error("io error")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Files(#[from] FilesError),
}

impl TransferError {
    /// 映射 §27.6 稳定码;Cancelled 属中止而非错误,仍给出 INTERNAL_ERROR
    /// 供日志记录,上层应优先检查 `matches!(e, TransferError::Cancelled)`。
    pub fn stable_code(&self) -> &'static str {
        match self {
            TransferError::Cancelled => "INTERNAL_ERROR",
            TransferError::Timeout(_) => "TRANSFER_EXPIRED",
            TransferError::Rejected(_) => "INTERNAL_ERROR",
            TransferError::Http(_) | TransferError::Io(_) => "INTERNAL_ERROR",
            TransferError::Files(e) => e.stable_code(),
        }
    }
}

#[derive(Debug)]
pub struct ProducerOutcome {
    pub status: u16,
    pub bytes_sent: u64,
}

#[derive(Debug)]
pub struct ConsumerOutcome {
    pub status: u16,
    pub bytes_read: u64,
    pub mime: Option<String>,
}

// ---------------------------------------------------------------------------
// producer:Bridge → Relay 流式上传文件正文(§22.4 第 4-5 步)
// ---------------------------------------------------------------------------

/// 把 [`FileSlice`] 流式推送到 Relay producer 端点。
///
/// `url` 为 [`crate::config::RelayUrls::transfer_producer_url`] 派生的完整
/// 端点地址(路径不在本模块拼装)。
///
/// 注意(Relay 侧契约):producer 端点应在 transfer 管道建立后尽早返回
/// 响应头(正文经有界 channel pipe 给 Browser),而不是收完整个正文才响应;
/// 否则 rendezvous 超时 ([`DEFAULT_RENDEZVOUS_TIMEOUT`]) 会截断大文件传输。
pub async fn produce(
    http: &reqwest::Client,
    url: &str,
    device_credential: &str,
    transfer_token: &str,
    slice: FileSlice,
    config: TransferConfig,
    cancel: &CancellationToken,
) -> Result<ProducerOutcome, TransferError> {
    let sent = Arc::new(AtomicU64::new(0));
    let body_len = slice.body_len;

    let stream = ReaderStream::with_capacity(slice.body, super::policy::TRANSFER_CHUNK_BYTES);
    let body_stream = IdleTimeoutStream::new(stream, config.idle_read, Arc::clone(&sent));

    let mut request = http
        .post(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("{} {}", DEVICE_CREDENTIAL_SCHEME, device_credential),
        )
        .header(TRANSFER_TOKEN_HEADER, transfer_token)
        .header(TRANSFER_LENGTH_HEADER, body_len.to_string())
        .header(reqwest::header::CONTENT_TYPE, slice.content_type)
        .header(reqwest::header::CACHE_CONTROL, super::policy::CACHE_CONTROL);
    if let Some(range) = &slice.content_range {
        request = request.header(
            reqwest::header::CONTENT_RANGE,
            range.content_range_value(slice.total_len),
        );
    }
    let request = request.body(reqwest::Body::wrap_stream(body_stream));

    // rendezvous:等待响应头;取消立即返回,不再读取(§22.4 第 6 步)。
    let send_fut = request.send();
    tokio::pin!(send_fut);
    let resp = tokio::select! {
        _ = cancel.cancelled() => return Err(TransferError::Cancelled),
        r = tokio::time::timeout(config.rendezvous, &mut send_fut) => match r {
            Ok(resp) => resp?,
            Err(_) => return Err(TransferError::Timeout(TimeoutPhase::Rendezvous)),
        },
    };
    if !resp.status().is_success() {
        tracing::debug!(
            code = "INTERNAL_ERROR",
            operation = "files.transfer.produce",
            "relay rejected"
        );
        return Err(TransferError::Rejected(resp.status().as_u16()));
    }
    let status = resp.status().as_u16();
    // 主动读完响应(有界,忽略错误)以便连接复用;之后统计已发送字节。
    let _ = resp.bytes().await;

    Ok(ProducerOutcome {
        status,
        bytes_sent: sent.load(Ordering::SeqCst),
    })
}

// ---------------------------------------------------------------------------
// consumer:Bridge ← Relay 流式接收 Browser 上传(§22.5 第 3-5 步)
// ---------------------------------------------------------------------------

/// 从 Relay consumer 端点流式接收正文并写入 [`UploadSink`]。
/// `url` 为 [`crate::config::RelayUrls::transfer_consumer_url`] 派生的完整
/// 端点地址。成功返回后由上层调用 `sink.finish(...)` 签发 upload handle;
/// 错误路径由上层调用 `sink.abort()`(Drop 亦有兜底清理)。
pub async fn consume(
    http: &reqwest::Client,
    url: &str,
    device_credential: &str,
    transfer_token: &str,
    sink: &mut UploadSink,
    config: TransferConfig,
    cancel: &CancellationToken,
) -> Result<ConsumerOutcome, TransferError> {
    let request = http
        .get(url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("{} {}", DEVICE_CREDENTIAL_SCHEME, device_credential),
        )
        .header(TRANSFER_TOKEN_HEADER, transfer_token)
        .header(reqwest::header::CACHE_CONTROL, super::policy::CACHE_CONTROL);

    let resp = tokio::select! {
        _ = cancel.cancelled() => return Err(TransferError::Cancelled),
        r = tokio::time::timeout(config.rendezvous, request.send()) => match r {
            Ok(resp) => resp?,
            Err(_) => return Err(TransferError::Timeout(TimeoutPhase::Rendezvous)),
        },
    };
    if !resp.status().is_success() {
        tracing::debug!(
            code = "INTERNAL_ERROR",
            operation = "files.transfer.consume",
            "relay rejected"
        );
        return Err(TransferError::Rejected(resp.status().as_u16()));
    }
    let status = resp.status().as_u16();

    let mut stream = resp.bytes_stream();
    let mut first = true;
    loop {
        let budget = if first {
            config.first_byte
        } else {
            config.idle_read
        };
        let chunk = tokio::select! {
            _ = cancel.cancelled() => return Err(TransferError::Cancelled),
            c = tokio::time::timeout(budget, stream.next()) => match c {
                Err(_) => return Err(TransferError::Timeout(
                    if first { TimeoutPhase::FirstByte } else { TimeoutPhase::IdleRead })),
                Ok(None) => break,
                Ok(Some(chunk)) => chunk?,
            },
        };
        first = false;
        sink.write_chunk(&chunk).await?;
    }

    Ok(ConsumerOutcome {
        status,
        bytes_read: sink.size(),
        mime: sink.sniffed_mime(),
    })
}

// ---------------------------------------------------------------------------
// 带 per-chunk 空闲超时与字节计数的流适配器
// ---------------------------------------------------------------------------

/// 包装 `Stream<Item = io::Result<Bytes>>`:每块之间执行空闲超时;
/// 超时以 `TimedOut` io 错误结束流。同时累计已产出字节数。
struct IdleTimeoutStream<S> {
    inner: S,
    idle: Duration,
    timer: Option<std::pin::Pin<Box<Sleep>>>,
    sent: Arc<AtomicU64>,
}

impl<S> IdleTimeoutStream<S> {
    fn new(inner: S, idle: Duration, sent: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            idle,
            timer: Some(Box::pin(tokio::time::sleep(idle))),
            sent,
        }
    }
}

impl<S> Stream for IdleTimeoutStream<S>
where
    S: Stream<Item = io::Result<bytes::Bytes>> + Unpin,
{
    type Item = io::Result<bytes::Bytes>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.sent.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                this.timer = Some(Box::pin(tokio::time::sleep(this.idle)));
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => match this.timer.as_mut() {
                Some(timer) => match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "idle read timeout",
                    )))),
                    Poll::Pending => Poll::Pending,
                },
                None => Poll::Pending,
            },
        }
    }
}
