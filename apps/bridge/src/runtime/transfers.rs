//! TransferOffer 处理:文件数据面 Bridge 侧接线(权威规格 §22.4/§22.5)。
//!
//! - 下载(preview/download):acquire_transfer_slot → resolve_for_read →
//!   open_slice(单 Range 透传,Bridge 复验)→ `files::transfer::produce`
//!   主动出站连接 Relay producer 端点 → TransferResult。请求头带
//!   X-Transfer-Content-Type / X-Transfer-Disposition / X-Transfer-Content-Range
//!   / X-Transfer-Total-Length(§22.3 头语义;由 per-transfer HTTP 客户端的
//!   default headers 携带,produce 本身补 Content-Type/Content-Range/
//!   X-Transfer-Length)。
//! - 上传:UploadSink::create → consume → finish 签发 upload handle →
//!   TransferResult(§22.5;v1 TransferResult 无句柄字段,句柄留在 Bridge
//!   的登记表,经上层 CommandRequest 附件路径交给 Codex)。
//! - 取消/超时:CancellationToken 三端联动(§22.4 第 6 步);入站
//!   TransferResult(取消)由 runtime 转发到令牌。
//! - transfer token:Relay 创建 transfer 时分配的短期凭据即 transfer_id
//!   (Relay 只接受存活 transfer 的 id;不经 Envelope 传第二凭据,§17.2)。

use std::sync::Arc;

use agent_console_protocol::agent_console::v1 as pb;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio_util::sync::CancellationToken;

use crate::config::RelayUrls;
use crate::domain as dm;
use crate::files::{
    self, GrantAction, TransferError, TransferMode, UploadSink, DEFAULT_GRANT_TTL, MAX_UPLOAD_BYTES,
};

use super::proto_mapping as pm;
use super::BridgeRuntime;

/// 从 runtime 配置的 Relay 公网基地址派生 [`RelayUrls`](AGENT_CONSOLE_RELAY_URL
/// 合同;解析失败视为配置错误,由调用方按传输失败处理)。
fn relay_urls(runtime: &BridgeRuntime) -> Option<RelayUrls> {
    RelayUrls::parse(runtime.relay_url().as_deref()?).ok()
}

/// TransferOffer 入口(§22.4 下载 / §22.5 上传)。
pub(crate) async fn handle_transfer_offer(
    runtime: &Arc<BridgeRuntime>,
    offer: pb::TransferOffer,
    correlation: String,
) {
    match pb::TransferDirection::try_from(offer.direction) {
        Ok(pb::TransferDirection::Download) => {
            handle_download(runtime, offer, correlation).await;
        }
        Ok(pb::TransferDirection::Upload) => {
            handle_upload(runtime, offer, correlation).await;
        }
        _ => {
            runtime.send_transfer_ready(
                &offer.transfer_id,
                false,
                Some(dm::StableErrorCode::InternalError),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 下载(preview/download;§22.4)
// ---------------------------------------------------------------------------

async fn handle_download(
    runtime: &Arc<BridgeRuntime>,
    offer: pb::TransferOffer,
    correlation: String,
) {
    let device = runtime.device_id().to_owned();
    let Some(session_key) = offer.session_key.as_ref().map(pm::session_key_from_proto) else {
        runtime.send_transfer_ready(
            &offer.transfer_id,
            false,
            Some(dm::StableErrorCode::FileHandleInvalid),
        );
        return;
    };
    let session = session_key.native_session_id.clone();

    // ① 并发名额(§22.6:每 device 默认 2;guard 随任务 Drop 释放)。
    let slot = match runtime.grants().acquire_transfer_slot(&device) {
        Ok(slot) => slot,
        Err(err) => {
            runtime.send_transfer_ready(
                &offer.transfer_id,
                false,
                Some(stable_code(err.stable_code())),
            );
            return;
        }
    };

    // ② 复验 handle(先按 Preview,再按 Download;§22.2)。
    let verified = match runtime.grants().resolve_for_read(
        &device,
        &session,
        &offer.file_handle,
        GrantAction::Preview,
    ) {
        Ok(verified) => (verified, TransferMode::Preview),
        Err(_) => match runtime.grants().resolve_for_read(
            &device,
            &session,
            &offer.file_handle,
            GrantAction::Download,
        ) {
            Ok(verified) => (verified, TransferMode::Download),
            Err(err) => {
                runtime.send_transfer_ready(
                    &offer.transfer_id,
                    false,
                    Some(stable_code(err.stable_code())),
                );
                return;
            }
        },
    };

    // ③ 打开切片(嗅探/大小限制/单 Range 复验,§22.3/§22.4)。
    let range_header = range_header_of(&offer, verified.0.size);
    let slice = match files::open_slice(verified.0, verified.1, range_header.as_deref()).await {
        Ok(slice) => slice,
        Err(err) => {
            runtime.send_transfer_ready(
                &offer.transfer_id,
                false,
                Some(stable_code(err.stable_code())),
            );
            return;
        }
    };

    // ④ Ready → 出站 producer 流(§22.4 第 4-5 步)。
    runtime.send_transfer_ready(&offer.transfer_id, true, None);

    let Some(urls) = relay_urls(runtime) else {
        runtime.send_transfer_result(
            &offer.transfer_id,
            pb::TransferOutcome::Failed,
            Some(dm::StableErrorCode::CodexUnavailable),
        );
        return;
    };

    let token = CancellationToken::new();
    runtime.register_transfer(offer.transfer_id.clone(), token.clone());

    // §22.3 信息性头:per-transfer 客户端 default headers。
    let headers = download_meta_headers(&slice);
    let client = match runtime.http_client_with_headers(&headers) {
        Ok(client) => client,
        Err(_) => {
            runtime.send_transfer_result(
                &offer.transfer_id,
                pb::TransferOutcome::Failed,
                Some(dm::StableErrorCode::InternalError),
            );
            runtime.remove_transfer(&offer.transfer_id);
            return;
        }
    };
    let credential = runtime.credential_token();
    let transfer_id = offer.transfer_id.clone();
    let runtime_for_task = runtime.clone();
    let correlation_for_task = correlation;
    tokio::spawn(async move {
        let _slot = slot; // 名额 guard 随任务存续(§22.6)。
        let url = urls.transfer_producer_url(&transfer_id);
        let outcome = files::transfer::produce(
            &client,
            &url,
            &credential,
            // transfer token = Relay 分配的 transfer_id(短期凭据;§22.4)。
            &transfer_id,
            slice,
            runtime_for_task.transfer_config(),
            &token,
        )
        .await;
        match outcome {
            Ok(outcome) => {
                tracing::debug!(bytes = outcome.bytes_sent, "transfer produce completed");
                runtime_for_task.send_transfer_result(
                    &transfer_id,
                    pb::TransferOutcome::Completed,
                    None,
                );
            }
            Err(TransferError::Cancelled) => {
                runtime_for_task.send_transfer_result(
                    &transfer_id,
                    pb::TransferOutcome::Cancelled,
                    None,
                );
            }
            Err(err) => {
                tracing::debug!(error = %err, "transfer produce failed");
                runtime_for_task.send_transfer_result(
                    &transfer_id,
                    pb::TransferOutcome::Failed,
                    Some(stable_code(err.stable_code())),
                );
            }
        }
        runtime_for_task.remove_transfer(&transfer_id);
        drop(correlation_for_task);
    });
}

/// §22.4 Range 协调字段 → HTTP 单 Range 头(仅描述;复验由 open_slice 执行)。
fn range_header_of(offer: &pb::TransferOffer, total: u64) -> Option<String> {
    let start = offer.range_start;
    let end = offer.range_end_inclusive;
    if start == 0 && end.is_none() {
        return None;
    }
    if let Some(end) = end {
        // 全文件 Range 等价无 Range。
        if start == 0 && end + 1 >= total {
            return None;
        }
        return Some(format!("bytes={start}-{end}"));
    }
    Some(format!("bytes={start}-"))
}

/// 下载信息性头(§22.3:类型/内联语义/Range/总长;正文头由 produce 补)。
fn download_meta_headers(slice: &files::FileSlice) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let mut insert = |name: &'static str, value: String| {
        if let Ok(value) = HeaderValue::from_str(&value) {
            headers.insert(HeaderName::from_static(name), value);
        }
    };
    insert("x-transfer-content-type", slice.content_type.to_owned());
    insert(
        "x-transfer-disposition",
        if slice.inline { "inline" } else { "attachment" }.to_owned(),
    );
    insert("x-transfer-total-length", slice.total_len.to_string());
    if let Some(range) = &slice.content_range {
        insert(
            "x-transfer-content-range",
            range.content_range_value(slice.total_len),
        );
    }
    headers
}

// ---------------------------------------------------------------------------
// 上传(§22.5)
// ---------------------------------------------------------------------------

async fn handle_upload(
    runtime: &Arc<BridgeRuntime>,
    offer: pb::TransferOffer,
    correlation: String,
) {
    let device = runtime.device_id().to_owned();
    let Some(session_key) = offer.session_key.as_ref().map(pm::session_key_from_proto) else {
        runtime.send_transfer_ready(
            &offer.transfer_id,
            false,
            Some(dm::StableErrorCode::FileHandleInvalid),
        );
        return;
    };
    let session = session_key.native_session_id.clone();

    let slot = match runtime.grants().acquire_transfer_slot(&device) {
        Ok(slot) => slot,
        Err(err) => {
            runtime.send_transfer_ready(
                &offer.transfer_id,
                false,
                Some(stable_code(err.stable_code())),
            );
            return;
        }
    };

    // ① 私有临时目录 sink(实际大小/MIME 校验在 sink 内,§22.5 第 5 步)。
    let mut sink = match UploadSink::create(&runtime.upload_root(), MAX_UPLOAD_BYTES).await {
        Ok(sink) => sink,
        Err(err) => {
            runtime.send_transfer_ready(
                &offer.transfer_id,
                false,
                Some(stable_code(err.stable_code())),
            );
            return;
        }
    };

    runtime.send_transfer_ready(&offer.transfer_id, true, None);

    let Some(urls) = relay_urls(runtime) else {
        sink.abort().await;
        runtime.send_transfer_result(
            &offer.transfer_id,
            pb::TransferOutcome::Failed,
            Some(dm::StableErrorCode::CodexUnavailable),
        );
        return;
    };

    let token = CancellationToken::new();
    runtime.register_transfer(offer.transfer_id.clone(), token.clone());
    let client = runtime.http().clone();
    let credential = runtime.credential_token();
    let transfer_id = offer.transfer_id.clone();
    let runtime_for_task = runtime.clone();
    let _ = correlation;
    tokio::spawn(async move {
        let _slot = slot;
        let url = urls.transfer_consumer_url(&transfer_id);
        let result = files::transfer::consume(
            &client,
            &url,
            &credential,
            &transfer_id,
            &mut sink,
            runtime_for_task.transfer_config(),
            &token,
        )
        .await;
        match result {
            Ok(_) => match sink
                .finish(
                    runtime_for_task.grants(),
                    &device,
                    &session,
                    DEFAULT_GRANT_TTL,
                )
                .await
            {
                Ok(outcome) => {
                    // §22.5 第 6 步:句柄只属于当前 session;经 TransferResult
                    // 回传给 Browser(用于把上传文件提交给会话消息),
                    // 同时登记在 Bridge 供命令附件路径复用。
                    runtime_for_task
                        .store_upload_handle(&transfer_id, outcome.handle.token.clone());
                    runtime_for_task.send_transfer_result_with_handle(
                        &transfer_id,
                        pb::TransferOutcome::Completed,
                        None,
                        Some(&outcome.handle.token),
                    );
                }
                Err(err) => {
                    runtime_for_task.send_transfer_result(
                        &transfer_id,
                        pb::TransferOutcome::Failed,
                        Some(stable_code(err.stable_code())),
                    );
                }
            },
            Err(TransferError::Cancelled) => {
                sink.abort().await;
                runtime_for_task.send_transfer_result(
                    &transfer_id,
                    pb::TransferOutcome::Cancelled,
                    None,
                );
            }
            Err(err) => {
                sink.abort().await;
                tracing::debug!(error = %err, "transfer consume failed");
                runtime_for_task.send_transfer_result(
                    &transfer_id,
                    pb::TransferOutcome::Failed,
                    Some(stable_code(err.stable_code())),
                );
            }
        }
        runtime_for_task.remove_transfer(&transfer_id);
    });
}

fn stable_code(code: &str) -> dm::StableErrorCode {
    use dm::StableErrorCode as E;
    match code {
        "FILE_HANDLE_INVALID" => E::FileHandleInvalid,
        "FILE_OUTSIDE_SCOPE" => E::FileOutsideScope,
        "FILE_CHANGED" => E::FileChanged,
        "FILE_TYPE_NOT_PREVIEWABLE" => E::FileTypeNotPreviewable,
        "TRANSFER_EXPIRED" => E::TransferExpired,
        "TRANSFER_TOO_LARGE" => E::TransferTooLarge,
        "TRANSFER_RANGE_INVALID" => E::TransferRangeInvalid,
        "RATE_LIMITED" => E::RateLimited,
        _ => E::InternalError,
    }
}
