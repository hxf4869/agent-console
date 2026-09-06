//! 浏览器 WSS 端点 `/agent-console/ws`(§17/§20)。
//!
//! ticket 经 `Sec-WebSocket-Protocol` 携带(固定协议名 + `agent-console.ticket-<b64url>`);
//! Relay 只回显固定协议名,不记录完整 header。升级前原子消费 ticket;
//! 握手 ClientHello/ServerHello 协商 protocol_version,不匹配发 ProtocolError 并关闭。
//!
//! 连接收尾(§17.6/§26.4):
//! - WS 消息/帧上限对齐 codec 单帧限制,分片聚合超限直接终止。
//! - 每次 sink 写入有可取消时间预算;超时丢弃整个连接(计数 +1)。
//! - 读写循环共享最小取消机制:任一端结束都收尾另一端并摘除 Hub 注册。
//! - 认证撤销/停机的 Close 经 Outbox.close 丢弃可恢复帧后优先送达。

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{State, WebSocketUpgrade},
    http::HeaderMap,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;

use agent_console_protocol::{
    codec::{decode_envelope, encode_envelope, new_message_id, MAX_FRAME_BYTES, PROTOCOL_VERSION},
    v1::{envelope, ClientHello, ClientKind, ProtocolError, ServerHello, StableErrorCode},
};

use crate::{
    auth::{parse_ws_subprotocols, ticket_reject_response},
    realtime::buffer::{
        OutItem, Outbox, BROWSER_QUEUE_MAX_BYTES, WS_WRITE_TIMEOUT, WS_WRITE_TIMEOUTS,
    },
    state::{limits, AppState},
};

type WsMessage = axum::extract::ws::Message;

/// writer 收尾预算:读循环结束后丢弃待发数据并限时发送 Close。
const WRITER_CLOSE_BUDGET: Duration = Duration::from_millis(500);

pub async fn browser_ws_handler(
    State(app): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // 1. 解析 subprotocol(§20.2):固定协议名 + Base64URL ticket;失败 → 401。
    let ticket = match parse_ws_subprotocols(&headers) {
        Ok(t) => crate::auth::decode_ticket(&t).unwrap_or_else(|| t.clone()),
        Err(resp) => {
            tracing::info!(target: "relay::auth", code = "WS_TICKET_INVALID", "ws subprotocol missing");
            return resp;
        }
    };
    // 2. 原子消费 ticket(§20.2)。
    let ident = match app.toolbox.consume_ticket(&ticket).await {
        crate::auth::ConsumeOutcome::Valid(ok) => crate::realtime::BrowserIdentity {
            auth_session_id: ok.auth_session_id,
            owner_id: ok.owner_id,
            expires_at: ok.expires_at,
        },
        crate::auth::ConsumeOutcome::Rejected(reason) => {
            tracing::info!(target: "relay::auth", code = crate::state::stable_code_ref_name(&reason.stable_code()), "ws auth rejected");
            return ticket_reject_response(reason);
        }
        crate::auth::ConsumeOutcome::Unavailable => {
            tracing::info!(target: "relay::auth", code = "INTERNAL_ERROR", "ws auth backend unreachable");
            return crate::state::api_error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                StableErrorCode::InternalError,
                "认证服务暂不可达",
                &crate::state::new_request_id(),
                serde_json::json!({}),
            );
        }
    };
    // 3. 只回显固定协议名(§20.2);WS 消息/帧上限对齐 codec 单帧限制:
    // 分片聚合超限由 transport 层终止,不无限聚合(T15)。
    ws.protocols([limits::WS_SUBPROTOCOL])
        .max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move { browser_connection(app, socket, ident).await })
}

fn server_hello_bytes() -> Vec<u8> {
    let env = agent_console_protocol::v1::Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        device_id: String::new(),
        agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(envelope::Payload::ServerHello(ServerHello {
            accepted_protocol_version: PROTOCOL_VERSION,
            server_time: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            capabilities: vec![
                "list".to_string(),
                "session".to_string(),
                "query".to_string(),
                "command".to_string(),
            ],
        })),
        provider_extension: None,
    };
    encode_envelope(&env).unwrap_or_default()
}

fn protocol_error_bytes(code: StableErrorCode, message: &str) -> Vec<u8> {
    let env = agent_console_protocol::v1::Envelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: new_message_id(),
        correlation_id: String::new(),
        sent_at: None,
        device_id: String::new(),
        agent_kind: 0,
        stream_id: String::new(),
        stream_epoch: 0,
        sequence: 0,
        payload: Some(envelope::Payload::ProtocolError(ProtocolError {
            error_code: code as i32,
            message: message.to_string(),
            request_id: String::new(),
            stream_id: String::new(),
            details: Default::default(),
        })),
        provider_extension: None,
    };
    encode_envelope(&env).unwrap_or_default()
}

async fn send_raw(
    sink: &mut futures::stream::SplitSink<axum::extract::ws::WebSocket, WsMessage>,
    bytes: Vec<u8>,
) -> bool {
    sink.send(WsMessage::Binary(bytes.into())).await.is_ok()
}

async fn send_close(
    sink: &mut futures::stream::SplitSink<axum::extract::ws::WebSocket, WsMessage>,
    code: u16,
    reason: &'static str,
) {
    let _ = sink
        .send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}

async fn browser_connection(
    app: AppState,
    socket: axum::extract::ws::WebSocket,
    ident: crate::realtime::BrowserIdentity,
) {
    let (mut sink, mut stream) = socket.split();
    // 4. Hello 握手(§17.2):首条必须是 ClientHello(BROWSER),版本不匹配 → ProtocolError + 关闭。
    let first = match timeout(limits::HELLO_TIMEOUT, stream.next()).await {
        Ok(Some(Ok(msg))) => Some(msg),
        _ => None,
    };
    let hello_ok = match first {
        Some(WsMessage::Binary(bytes)) => match decode_envelope(&bytes) {
            Ok(env) => match env.payload {
                Some(envelope::Payload::ClientHello(ClientHello {
                    protocol_version,
                    client_kind,
                    ..
                })) => {
                    protocol_version == PROTOCOL_VERSION
                        && client_kind == ClientKind::ClientBrowser as i32
                }
                _ => false,
            },
            Err(_) => false,
        },
        _ => false,
    };
    if !hello_ok {
        let _ = send_raw(
            &mut sink,
            protocol_error_bytes(StableErrorCode::InternalError, "协议版本或握手不匹配"),
        )
        .await;
        send_close(&mut sink, 1002, "PROTOCOL_VERSION_MISMATCH").await;
        return;
    }
    if !send_raw(&mut sink, server_hello_bytes()).await {
        return;
    }

    // 5. 注册连接,启动写循环(§17.4 步骤 2:有界发送队列 + 控制帧通道)。
    let outbox = Arc::new(Outbox::new(
        limits::BROWSER_QUEUE_MAX_FRAMES,
        BROWSER_QUEUE_MAX_BYTES,
    ));
    let conn_id = app.hub.register_browser(ident, outbox.clone());
    tracing::info!(target: "relay::auth", "browser connected");

    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel::<WsMessage>(16);
    // 读写循环共享的最小取消机制:任一端结束都收尾另一端。
    let (writer_cancel, mut writer_cancel_rx) = tokio::sync::watch::channel(false);
    let writer_outbox = outbox.clone();
    let mut writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                item = writer_outbox.recv() => {
                    match item {
                        Some(OutItem::Frame(frame)) => {
                            let Some(bytes) = frame.encoded() else {
                                // 编码失败绝不发送空帧:终止对应流,走重连恢复。
                                tracing::warn!(target: "relay::auth", "browser frame encode failed; closing");
                                return;
                            };
                            if write_bounded(&mut sink, WsMessage::Binary(bytes)).await.is_err() {
                                tracing::debug!(target: "relay::auth", "browser writer send error");
                                return;
                            }
                        }
                        Some(OutItem::Direct(bytes)) => {
                            if write_bounded(&mut sink, WsMessage::Binary(bytes))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Some(OutItem::Close(reason)) => {
                            // 稳定 close reason(§20.5);auth 类关闭用 1008。
                            let _ = write_bounded(
                                &mut sink,
                                WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                                    code: 1008,
                                    reason: reason.into(),
                                })),
                            )
                            .await;
                            return;
                        }
                        None => return,
                    }
                }
                ctrl = control_rx.recv() => {
                    // Ping/Pong 等控制帧;写预算与数据帧一致。
                    let Some(msg) = ctrl else { continue };
                    if write_bounded(&mut sink, msg).await.is_err() {
                        return;
                    }
                }
                _ = writer_cancel_rx.changed() => {
                    // 读循环已结束:丢弃剩余待发数据,限时发送 Close 后释放。
                    let _ = timeout(
                        WRITER_CLOSE_BUDGET,
                        sink.send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                            code: 1001,
                            reason: "SERVER_CLOSED".into(),
                        }))),
                    )
                    .await;
                    return;
                }
            }
        }
    });

    // 6. 读循环:writer 提前结束(对端断开/写超时/编码失败)时同步收尾。
    let mut writer_done = false;
    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(bytes))) => match decode_envelope(&bytes) {
                        Ok(env) => {
                            app.hub.handle_browser_envelope(&app, conn_id, env).await;
                        }
                        Err(_) => {
                            if let Some(outbox) = app.hub.browser_outbox(conn_id) {
                                outbox.push_direct(
                                    protocol_error_bytes(
                                        StableErrorCode::InternalError,
                                        "无法解码消息",
                                    )
                                    .into(),
                                );
                            }
                        }
                    },
                    Some(Ok(WsMessage::Ping(payload))) => {
                        // Ping 回复不等待失去写入能力的小通道:满即丢弃。
                        let _ = control_tx.try_send(WsMessage::Pong(payload));
                    }
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            result = &mut writer => {
                if let Err(e) = result {
                    tracing::debug!(target: "relay::auth", error = %e, "browser writer joined");
                }
                writer_done = true;
                break;
            }
        }
    }
    // 收尾:通知 writer 限时退出,再摘除 Hub 注册(幂等)。
    // writer 先结束时其 JoinHandle 已在上面的 select 中完成过一次 poll;
    // JoinHandle 完成后再次 poll 会 panic 并跳过 remove_browser(连接滞留
    // Hub 泄漏),因此只有读循环先结束(writer 仍在运行)才限时等待收尾。
    let _ = writer_cancel.send(true);
    if !writer_done {
        let _ = timeout(WRITER_CLOSE_BUDGET * 3, &mut writer).await;
    }
    app.hub.remove_browser(conn_id);
    tracing::info!(target: "relay::auth", "browser disconnected");
}

/// 带可取消时间预算的 sink 写入;超时/失败都返回 Err,调用方丢弃整个连接,
/// 绝不在原 sink 上继续写半帧。
async fn write_bounded(
    sink: &mut futures::stream::SplitSink<axum::extract::ws::WebSocket, WsMessage>,
    msg: WsMessage,
) -> Result<(), ()> {
    match timeout(WS_WRITE_TIMEOUT, sink.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(()),
        Err(_) => {
            WS_WRITE_TIMEOUTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(())
        }
    }
}
