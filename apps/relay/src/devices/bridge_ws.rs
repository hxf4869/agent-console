//! Bridge WSS 端点 `/internal/bridge/ws`(§21/§26.2)。
//!
//! 认证:`Authorization: Bearer <device_credential>`,SHA-256 摘要对库内摘要
//! constant-time 比较;失败拒绝;撤销设备立即关闭其 socket(§21.9)。
//! 握手:ClientHello(BRIDGE)/ServerHello 协议版本协商;不匹配发 ProtocolError 并关闭。

use std::time::Duration;

use axum::{
    extract::{State, WebSocketUpgrade},
    http::HeaderMap,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;

use agent_console_protocol::{
    codec::{decode_envelope, encode_envelope, new_message_id, PROTOCOL_VERSION},
    v1::{envelope, ClientHello, ClientKind, ProtocolError, ServerHello, StableErrorCode},
};

use crate::state::{ct_eq_hex, limits, new_request_id, sha256_hex, AppState};

type WsMessage = axum::extract::ws::Message;

pub async fn bridge_ws_handler(
    State(app): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // 1. 凭据认证:Bearer <device_credential> → SHA-256 摘要查库 + 常量时间比较(§21)。
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("");
    let credential_digest = sha256_hex(bearer.as_bytes());
    let device = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, bool, String)>(
        "SELECT id, owner_id, revoked_at IS NOT NULL, credential_digest FROM devices WHERE credential_digest = $1",
    )
    .bind(&credential_digest)
    .fetch_optional(&app.db)
    .await;
    let (device_id, owner_id, revoked, stored_digest) = match device {
        Ok(Some(row)) => row,
        _ => {
            return crate::state::api_error(
                axum::http::StatusCode::UNAUTHORIZED,
                StableErrorCode::AuthRequired,
                "设备凭据无效",
                &new_request_id(),
                serde_json::json!({}),
            );
        }
    };
    // constant-time 摘要比较(纵深防御;查询命中后再校验一次)。
    if !ct_eq_hex(&credential_digest, &stored_digest) {
        return crate::state::api_error(
            axum::http::StatusCode::UNAUTHORIZED,
            StableErrorCode::AuthRequired,
            "设备凭据无效",
            &new_request_id(),
            serde_json::json!({}),
        );
    }
    if revoked {
        return crate::state::api_error(
            axum::http::StatusCode::UNAUTHORIZED,
            StableErrorCode::DeviceRevoked,
            "设备已撤销",
            &new_request_id(),
            serde_json::json!({}),
        );
    }
    // 2. 升级;日志不记录 Authorization(§25.3)。
    ws.on_upgrade(
        move |socket| async move { bridge_connection(app, socket, device_id, owner_id).await },
    )
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
            capabilities: vec![],
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

/// 每设备单连接:新连接挤掉旧连接(旧 socket 由 close 信号关闭)。
///
/// 握手在升级回调执行体内完成;其后的注册、心跳、读循环与离线清理作为
/// 独立 tokio 任务运行——升级回调的执行体由 hyper 连接任务驱动,
/// 不能在其中做与 socket 无关的长时 await(否则任务会停摆,§26.4)。
async fn bridge_connection(
    app: AppState,
    socket: axum::extract::ws::WebSocket,
    device_id: uuid::Uuid,
    owner_id: uuid::Uuid,
) {
    let (mut sink, mut stream) = socket.split();
    // Hello 握手:ClientHello(BRIDGE)且 device_id 与认证设备一致;
    // 版本不匹配 → ProtocolError + 关闭(§17.2)。
    let first = match timeout(limits::HELLO_TIMEOUT, stream.next()).await {
        Ok(Some(Ok(msg))) => Some(msg),
        _ => None,
    };
    let handshake = match first {
        Some(WsMessage::Binary(bytes)) => match decode_envelope(&bytes) {
            Ok(env) => match env.payload {
                Some(envelope::Payload::ClientHello(ClientHello {
                    protocol_version,
                    client_kind,
                    device_id: hello_device,
                    ..
                })) if protocol_version == PROTOCOL_VERSION
                    && client_kind == ClientKind::ClientBridge as i32 =>
                {
                    if hello_device == device_id.to_string() {
                        Ok(())
                    } else {
                        Err("DEVICE_MISMATCH")
                    }
                }
                _ => Err("PROTOCOL_VERSION_MISMATCH"),
            },
            Err(_) => Err("PROTOCOL_VERSION_MISMATCH"),
        },
        _ => Err("PROTOCOL_VERSION_MISMATCH"),
    };
    if let Err(reason) = handshake {
        let _ = sink
            .send(WsMessage::Binary(
                protocol_error_bytes(StableErrorCode::InternalError, "协议版本或握手不匹配").into(),
            ))
            .await;
        let _ = sink
            .send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                code: 1002,
                reason: reason.into(),
            })))
            .await;
        return;
    }
    let _ = sink
        .send(WsMessage::Binary(server_hello_bytes().into()))
        .await;

    // 出站队列 + 控制通道;写任务持有 sink 独立运行。
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<crate::realtime::WireBytes>(limits::BRIDGE_QUEUE_MAX_FRAMES);
    let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::channel::<WsMessage>(16);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                bytes = rx.recv() => match bytes {
                    Some(b) => {
                        if sink.send(WsMessage::Binary(b)).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
                ctrl = ctrl_rx.recv() => match ctrl {
                    Some(m) => {
                        if sink.send(m).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                },
            }
        }
    });
    let (close_tx, close_rx) = tokio::sync::watch::channel(false);
    let session = tokio::spawn(bridge_session(
        app, stream, ctrl_tx, close_rx, tx, close_tx, device_id, owner_id,
    ));
    // 会话任务退出即结束(writer 随 out 队列关闭/abort 停止)。
    let _ = session.await;
}

/// Bridge 会话:注册、上线广播、心跳监测、读循环与离线清理。
async fn bridge_session(
    app: AppState,
    mut stream: futures::stream::SplitStream<axum::extract::ws::WebSocket>,
    ctrl_tx: tokio::sync::mpsc::Sender<WsMessage>,
    mut close_rx: tokio::sync::watch::Receiver<bool>,
    tx: tokio::sync::mpsc::Sender<crate::realtime::WireBytes>,
    close_tx: tokio::sync::watch::Sender<bool>,
    device_id: uuid::Uuid,
    owner_id: uuid::Uuid,
) {
    // 注册连接(单连接:关闭旧连接)。
    app.hub.force_close_bridge(device_id);
    app.hub.register_bridge(device_id, owner_id, tx, close_tx);
    tracing::info!(target: "relay::devices", "bridge connected");

    // 上线:更新 last_seen、摘要连接状态,并广播 presence(§10.1/§26.2)。
    let _ = sqlx::query("UPDATE devices SET last_seen_at = now() WHERE id = $1")
        .bind(device_id)
        .execute(&app.db)
        .await;
    let _ = crate::sessions::store::set_device_connection(&app.db, device_id, "CONNECTION_ONLINE")
        .await;
    app.hub.fan_out_presence(device_id, owner_id, true);

    // 读循环:任何入站帧刷新 last_rx;OFFLINE_AFTER 无帧视为 offline(§26.2)。
    let mut heartbeat_tick = tokio::time::interval(app.config.heartbeat_interval);
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut check = tokio::time::interval(Duration::from_secs(5));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_rx = tokio::time::Instant::now();
    let mut offline = false;
    let mut revoked = false;

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(bytes))) => {
                        last_rx = tokio::time::Instant::now();
                        match decode_envelope(&bytes) {
                            Ok(env) => app.hub.handle_bridge_envelope(&app, device_id, env).await,
                            Err(_) => {
                                tracing::debug!(target: "relay::devices", "undecodable bridge frame");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {
                        last_rx = tokio::time::Instant::now();
                        let _ = ctrl_tx.send(WsMessage::Pong(payload)).await;
                    }
                    Some(Ok(_)) => { last_rx = tokio::time::Instant::now(); }
                    Some(Err(_)) | None => break,
                }
            }
            _ = heartbeat_tick.tick() => {
                // Relay 主动心跳(§26.2:15s);发送失败即断开。
                let hb = agent_console_protocol::v1::Envelope {
                    protocol_version: PROTOCOL_VERSION,
                    message_id: new_message_id(),
                    correlation_id: String::new(),
                    sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    device_id: device_id.to_string(),
                    agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
                    stream_id: String::new(),
                    stream_epoch: 0,
                    sequence: 0,
                    payload: Some(envelope::Payload::Heartbeat(
                        agent_console_protocol::v1::Heartbeat {},
                    )),
                    provider_extension: None,
                };
                if !app.hub.send_to_bridge(device_id, hb) {
                    break;
                }
            }
            _ = check.tick() => {
                if last_rx.elapsed() > app.config.offline_after {
                    offline = true;
                    break;
                }
            }
            _ = close_rx.changed() => {
                // 撤销设备立即关闭其 socket(§21.9)。
                if *close_rx.borrow() {
                    revoked = true;
                    break;
                }
            }
        }
    }

    // 断开前发送 WS Close(稳定 reason,§25.3 白名单)。
    let reason = if revoked {
        "DEVICE_REVOKED"
    } else if offline {
        "DEVICE_OFFLINE"
    } else {
        "INTERNAL_ERROR"
    };
    let _ = ctrl_tx
        .send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
            code: 1008,
            reason: reason.into(),
        })))
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ctrl_tx);

    // 离线清理:presence + 摘要状态 + 未完成命令 OUTCOME_UNKNOWN(§15.2/§26.4)。
    let pendings = app.hub.remove_bridge(device_id);
    let _ = sqlx::query("UPDATE devices SET last_seen_at = now() WHERE id = $1")
        .bind(device_id)
        .execute(&app.db)
        .await;
    let _ = crate::sessions::store::set_device_connection(&app.db, device_id, "CONNECTION_OFFLINE")
        .await;
    app.hub.fan_out_presence(device_id, owner_id, false);
    for (request_id, _operation) in pendings {
        let _ = crate::sessions::store::update_receipt(
            &app.db,
            &request_id,
            "OUTCOME_UNKNOWN",
            "OUTCOME_UNKNOWN",
        )
        .await;
    }
    if offline {
        tracing::debug!(target: "relay::devices", "bridge offline after heartbeat timeout");
    }
    tracing::info!(target: "relay::devices", "bridge disconnected");
}
