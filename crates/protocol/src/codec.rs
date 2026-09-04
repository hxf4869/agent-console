//! 窄编解码助手(§17.6):Envelope 二进制编/解码、message_id/correlation_id
//! 生成、单帧大小上限与 payload oneof 提取。
//!
//! 只做传输层职责,不包含业务逻辑;结构化 Protobuf 单帧默认不得超过 1 MiB,
//! 超限在编码与解码入口统一拒绝。输出分块上限(64 KiB)仅作为协议常量导出,
//! 由产生输出事件的调用方自行遵守。

use crate::agent_console::v1::envelope;
use crate::agent_console::v1::Envelope;
use prost::Message;

/// 当前协议主版本,与 `Envelope.protocol_version` 一致。
pub const PROTOCOL_VERSION: u32 = 1;

/// 结构化 Protobuf 单帧上限:1 MiB(§17.6)。
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// 输出流单块默认上限:64 KiB;所有分块必须在 UTF-8 边界切分(§13.2)。
pub const MAX_OUTPUT_CHUNK_BYTES: usize = 64 * 1024;

/// 编解码错误。帧超限与 protobuf 编解码失败分开表达,
/// 调用方据此决定是本地拒绝还是发送 `ProtocolError`。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// 帧超过 `MAX_FRAME_BYTES`。
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: usize, max: usize },
    /// protobuf 编码失败。
    #[error("encode failed: {0}")]
    Encode(#[from] prost::EncodeError),
    /// protobuf 解码失败(字节损坏或非本协议数据)。
    #[error("decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// 编码 Envelope 为二进制帧;超过 `MAX_FRAME_BYTES` 时返回
/// [`CodecError::FrameTooLarge`],不产出截断数据。
pub fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>, CodecError> {
    let size = envelope.encoded_len();
    if size > MAX_FRAME_BYTES {
        return Err(CodecError::FrameTooLarge {
            size,
            max: MAX_FRAME_BYTES,
        });
    }
    let mut buf = Vec::with_capacity(size);
    envelope.encode(&mut buf)?;
    debug_assert_eq!(buf.len(), size);
    Ok(buf)
}

/// 解码二进制帧为 Envelope;先检查上限再解码,超过 `MAX_FRAME_BYTES`
/// 时返回 [`CodecError::FrameTooLarge`]。
pub fn decode_envelope(bytes: &[u8]) -> Result<Envelope, CodecError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(CodecError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    Ok(Envelope::decode(bytes)?)
}

/// 生成新的 message_id(UUID v4)。
pub fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 生成新的 correlation_id(UUID v4);请求方也可直接复用 request_id。
pub fn new_correlation_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 提取 payload oneof 引用;payload 缺省(空帧)时返回 `None`。
pub fn payload(envelope: &Envelope) -> Option<&envelope::Payload> {
    envelope.payload.as_ref()
}

/// payload 变体的稳定名称(如 `"event_batch"`),供日志与调试使用;
/// 只暴露变体名,不暴露任何消息内容。
pub fn payload_kind(payload: &envelope::Payload) -> &'static str {
    match payload {
        envelope::Payload::ClientHello(_) => "client_hello",
        envelope::Payload::ServerHello(_) => "server_hello",
        envelope::Payload::CapabilitySnapshot(_) => "capability_snapshot",
        envelope::Payload::DevicePresence(_) => "device_presence",
        envelope::Payload::Subscribe(_) => "subscribe",
        envelope::Payload::Subscribed(_) => "subscribed",
        envelope::Payload::Unsubscribe(_) => "unsubscribe",
        envelope::Payload::Ack(_) => "ack",
        envelope::Payload::SessionSummaryBatch(_) => "session_summary_batch",
        envelope::Payload::RuntimeSnapshot(_) => "runtime_snapshot",
        envelope::Payload::EventBatch(_) => "event_batch",
        envelope::Payload::ResyncRequired(_) => "resync_required",
        envelope::Payload::ResyncRequest(_) => "resync_request",
        envelope::Payload::QueryRequest(_) => "query_request",
        envelope::Payload::QueryResponse(_) => "query_response",
        envelope::Payload::CommandRequest(_) => "command_request",
        envelope::Payload::CommandAccepted(_) => "command_accepted",
        envelope::Payload::CommandResult(_) => "command_result",
        envelope::Payload::TransferOffer(_) => "transfer_offer",
        envelope::Payload::TransferReady(_) => "transfer_ready",
        envelope::Payload::TransferResult(_) => "transfer_result",
        envelope::Payload::Heartbeat(_) => "heartbeat",
        envelope::Payload::HeartbeatAck(_) => "heartbeat_ack",
        envelope::Payload::ProtocolError(_) => "protocol_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_console::v1::command_request;
    use crate::agent_console::v1::domain_event;
    use crate::agent_console::v1::query_request;
    use crate::agent_console::v1::query_response;
    use crate::agent_console::v1::subscribe;
    use crate::agent_console::v1::{
        Ack, ActiveTurnPhase, CapabilitySnapshot, ClientHello, CommandAccepted,
        CommandReceiptStatus, CommandRequest, CommandResult, CurrentTurn, DeviceConnection,
        DevicePresence, DomainEvent, EventBatch, Heartbeat, HeartbeatAck, ItemId, Operation,
        OutputAppend, ProtocolError, QueryRequest, QueryResponse, ResyncRequest, ResyncRequired,
        RuntimeSnapshot, RuntimeSnapshotQuery, ServerHello, SessionKey, SessionList,
        SessionSummary, SessionSummaryBatch, StableErrorCode, StartTurnPayload, Subscribe,
        Subscribed, TransferDirection, TransferOffer, TransferOutcome, TransferReady,
        TransferResult, TurnId, Unsubscribe,
    };
    use prost_types::Timestamp;

    fn base_envelope(payload: envelope::Payload) -> Envelope {
        Envelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: new_message_id(),
            correlation_id: "corr-1".to_string(),
            sent_at: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            device_id: "device-1".to_string(),
            agent_kind: 1, // CODEX_DESKTOP
            stream_id: "stream-1".to_string(),
            stream_epoch: 7,
            sequence: 42,
            payload: Some(payload),
            provider_extension: None,
        }
    }

    /// §17.3 全部 23 个 payload 各取一种代表样本。
    fn sample_payloads() -> Vec<(&'static str, envelope::Payload)> {
        let session_key = SessionKey {
            device_id: "device-1".to_string(),
            agent_kind: 1,
            native_session_id: "native-1".to_string(),
            relay_session_uuid: "uuid-1".to_string(),
        };
        let turn = Some(TurnId {
            id: "turn-1".to_string(),
            synthetic: false,
        });
        vec![
            (
                "client_hello",
                envelope::Payload::ClientHello(ClientHello {
                    protocol_version: PROTOCOL_VERSION,
                    client_kind: 1,
                    device_id: "device-1".to_string(),
                    auth_subject: "user-1".to_string(),
                    capabilities: vec!["list".into(), "detail".into()],
                }),
            ),
            (
                "server_hello",
                envelope::Payload::ServerHello(ServerHello {
                    accepted_protocol_version: PROTOCOL_VERSION,
                    server_time: Some(Timestamp::default()),
                    capabilities: vec!["list".into()],
                }),
            ),
            (
                "capability_snapshot",
                envelope::Payload::CapabilitySnapshot(CapabilitySnapshot {
                    control_mode: 1,
                    compatibility_state: 1,
                    codex_version: "0.153.0-alpha.5".to_string(),
                    supported_operations: vec![Operation::StartTurn as i32],
                    settings: vec![],
                    transfer_limits: None,
                }),
            ),
            (
                "device_presence",
                envelope::Payload::DevicePresence(DevicePresence {
                    device_id: "device-1".to_string(),
                    connection: DeviceConnection::ConnectionOnline as i32,
                    last_seen_at: Some(Timestamp::default()),
                    degraded_reason: String::new(),
                }),
            ),
            (
                "subscribe",
                envelope::Payload::Subscribe(Subscribe {
                    target: Some(subscribe::Target::List(SessionList {})),
                }),
            ),
            (
                "subscribed",
                envelope::Payload::Subscribed(Subscribed {
                    stream_id: "stream-1".to_string(),
                    stream_epoch: 1,
                    base_sequence: 0,
                }),
            ),
            (
                "unsubscribe",
                envelope::Payload::Unsubscribe(Unsubscribe {
                    stream_id: "stream-1".to_string(),
                }),
            ),
            (
                "ack",
                envelope::Payload::Ack(Ack {
                    stream_id: "stream-1".to_string(),
                    sequence: 9,
                }),
            ),
            (
                "session_summary_batch",
                envelope::Payload::SessionSummaryBatch(SessionSummaryBatch {
                    summaries: vec![SessionSummary {
                        session_key: Some(session_key.clone()),
                        title: "task".to_string(),
                        ..Default::default()
                    }],
                    snapshot: true,
                }),
            ),
            (
                "runtime_snapshot",
                envelope::Payload::RuntimeSnapshot(RuntimeSnapshot {
                    runtime_revision: 3,
                    current_turn: Some(CurrentTurn {
                        turn,
                        phase: ActiveTurnPhase::TurnPhaseRunning as i32,
                        started_at: Some(Timestamp::default()),
                    }),
                    ..Default::default()
                }),
            ),
            (
                "event_batch",
                envelope::Payload::EventBatch(EventBatch {
                    stream_id: "stream-1".to_string(),
                    events: vec![DomainEvent {
                        emitted_at: Some(Timestamp::default()),
                        event: Some(domain_event::Event::OutputAppend(OutputAppend {
                            item_id: Some(ItemId {
                                id: "item-1".to_string(),
                                synthetic: false,
                            }),
                            expected_offset: 0,
                            bytes: b"hello".to_vec(),
                            channel: 3,
                        })),
                    }],
                }),
            ),
            (
                "resync_required",
                envelope::Payload::ResyncRequired(ResyncRequired {
                    stream_id: "stream-1".to_string(),
                    reason_code: StableErrorCode::ResyncRequired as i32,
                }),
            ),
            (
                "resync_request",
                envelope::Payload::ResyncRequest(ResyncRequest {
                    stream_id: "stream-1".to_string(),
                }),
            ),
            (
                "query_request",
                envelope::Payload::QueryRequest(QueryRequest {
                    session_key: Some(session_key.clone()),
                    query: Some(query_request::Query::RuntimeSnapshot(
                        RuntimeSnapshotQuery {},
                    )),
                }),
            ),
            (
                "query_response",
                envelope::Payload::QueryResponse(QueryResponse {
                    request_id: "req-1".to_string(),
                    result: Some(query_response::Result::RuntimeSnapshot(RuntimeSnapshot {
                        runtime_revision: 5,
                        ..Default::default()
                    })),
                    error_code: 0,
                }),
            ),
            (
                "command_request",
                envelope::Payload::CommandRequest(CommandRequest {
                    request_id: "req-1".to_string(),
                    operation: Operation::StartTurn as i32,
                    session_key: Some(session_key.clone()),
                    expected_turn_id: None,
                    expected_runtime_revision: Some(3),
                    payload_digest: "deadbeef".to_string(),
                    payload: Some(command_request::Payload::StartTurn(StartTurnPayload {
                        prompt: "hi".to_string(),
                    })),
                }),
            ),
            (
                "command_accepted",
                envelope::Payload::CommandAccepted(CommandAccepted {
                    request_id: "req-1".to_string(),
                    status: CommandReceiptStatus::ReceiptAcceptedByBridge as i32,
                    accepted_at: Some(Timestamp::default()),
                }),
            ),
            (
                "command_result",
                envelope::Payload::CommandResult(CommandResult {
                    request_id: "req-1".to_string(),
                    status: CommandReceiptStatus::ReceiptRejected as i32,
                    error_code: StableErrorCode::StaleTurn as i32,
                    details: Default::default(),
                    duration_ms: Some(12),
                }),
            ),
            (
                "transfer_offer",
                envelope::Payload::TransferOffer(TransferOffer {
                    transfer_id: "t-1".to_string(),
                    direction: TransferDirection::Download as i32,
                    session_key: Some(session_key.clone()),
                    file_name: "report.pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    size_bytes: 1024,
                    file_handle: "handle-1".to_string(),
                    range_start: 0,
                    range_end_inclusive: None,
                    expires_at: Some(Timestamp::default()),
                }),
            ),
            (
                "transfer_ready",
                envelope::Payload::TransferReady(TransferReady {
                    transfer_id: "t-1".to_string(),
                    ready: false,
                    rejection_code: StableErrorCode::TransferTooLarge as i32,
                }),
            ),
            (
                "transfer_result",
                envelope::Payload::TransferResult(TransferResult {
                    transfer_id: "t-1".to_string(),
                    outcome: TransferOutcome::Completed as i32,
                    error_code: 0,
                    upload_file_handle: "uh-1".to_string(),
                }),
            ),
            ("heartbeat", envelope::Payload::Heartbeat(Heartbeat {})),
            (
                "heartbeat_ack",
                envelope::Payload::HeartbeatAck(HeartbeatAck {}),
            ),
            (
                "protocol_error",
                envelope::Payload::ProtocolError(ProtocolError {
                    error_code: StableErrorCode::DeviceOffline as i32,
                    message: "设备离线".to_string(),
                    request_id: "req-1".to_string(),
                    stream_id: String::new(),
                    details: Default::default(),
                }),
            ),
        ]
    }

    #[test]
    fn roundtrip_covers_every_payload_kind() {
        let samples = sample_payloads();
        assert_eq!(samples.len(), 24, "必须覆盖 §17.3 全部 payload 变体");
        for (kind, sample) in samples {
            let envelope = base_envelope(sample);
            let frame =
                encode_envelope(&envelope).unwrap_or_else(|e| panic!("encode {kind} failed: {e}"));
            assert!(frame.len() <= MAX_FRAME_BYTES);
            let decoded =
                decode_envelope(&frame).unwrap_or_else(|e| panic!("decode {kind} failed: {e}"));
            assert_eq!(decoded, envelope, "roundtrip mismatch for {kind}");
            let extracted = payload(&decoded).expect("payload must survive roundtrip");
            assert_eq!(payload_kind(extracted), kind);
        }
    }

    #[test]
    fn rejects_oversized_frame_on_encode_and_decode() {
        // 构造一个超过 1 MiB 的帧(payload 内 1.5 MiB 字节)。
        let big = vec![0u8; MAX_FRAME_BYTES + 512 * 1024];
        let envelope = base_envelope(envelope::Payload::EventBatch(EventBatch {
            stream_id: "stream-1".to_string(),
            events: vec![DomainEvent {
                emitted_at: None,
                event: Some(domain_event::Event::OutputAppend(OutputAppend {
                    item_id: Some(ItemId {
                        id: "item-1".to_string(),
                        synthetic: false,
                    }),
                    expected_offset: 0,
                    bytes: big,
                    channel: 1,
                })),
            }],
        }));

        match encode_envelope(&envelope) {
            Err(CodecError::FrameTooLarge { size, max }) => {
                assert!(size > MAX_FRAME_BYTES);
                assert_eq!(max, MAX_FRAME_BYTES);
            }
            other => panic!("encode must reject oversized frame, got {other:?}"),
        }

        // 解码入口同样拒绝:手工编码一个合法但超限的 protobuf 消息。
        let mut raw = Vec::new();
        envelope.encode(&mut raw).unwrap();
        assert!(raw.len() > MAX_FRAME_BYTES);
        assert_eq!(
            decode_envelope(&raw),
            Err(CodecError::FrameTooLarge {
                size: raw.len(),
                max: MAX_FRAME_BYTES
            })
        );
    }

    #[test]
    fn tolerates_unknown_agent_kind_and_unknown_fields() {
        // 未知 agent_kind 数值:proto3 未知枚举值必须保留,不得解码失败(§17.2 注)。
        let envelope = base_envelope(envelope::Payload::Heartbeat(Heartbeat {}));
        let mut with_unknown = envelope.clone();
        with_unknown.agent_kind = 99;
        let frame = {
            let mut buf = Vec::new();
            with_unknown.encode(&mut buf).unwrap();
            buf
        };
        let decoded = decode_envelope(&frame).expect("unknown enum value must not fail decode");
        assert_eq!(decoded.agent_kind, 99);
        assert_eq!(
            crate::agent_console::v1::AgentKind::try_from(decoded.agent_kind).is_err(),
            true
        );
        // 其余字段不受影响。
        assert_eq!(decoded.sequence, envelope.sequence);
        assert!(matches!(
            decoded.payload,
            Some(envelope::Payload::Heartbeat(_))
        ));
    }

    #[test]
    fn missing_fields_default_when_absent() {
        // 空帧:全部字段取 proto3 默认值,不报错。
        let decoded = decode_envelope(&[]).expect("empty frame is a default Envelope");
        assert_eq!(decoded.protocol_version, 0);
        assert_eq!(decoded.sequence, 0);
        assert_eq!(decoded.stream_epoch, 0);
        assert_eq!(decoded.sent_at, None);
        assert_eq!(decoded.payload, None);
        assert!(payload(&decoded).is_none());

        // 只写 message_id:其余字段保持默认,已写字段往返保真。
        let partial = Envelope {
            message_id: "only-id".to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        partial.encode(&mut buf).unwrap();
        let decoded = decode_envelope(&buf).unwrap();
        assert_eq!(decoded.message_id, "only-id");
        assert_eq!(decoded.sequence, 0);
        assert_eq!(decoded.payload, None);
        // proto3 标量默认值不上 wire:帧内只应有 message_id 一个字段
        // (1 字节 tag + 1 字节 varint 长度 + 7 字节内容)。
        assert_eq!(buf.len(), 2 + "only-id".len());

        // optional uint64 缺省与 0 语义可区分(§15.1 expected_runtime_revision)。
        let zero_rev = CommandRequest {
            expected_runtime_revision: Some(0),
            ..Default::default()
        };
        let absent_rev = CommandRequest::default();
        assert_ne!(
            zero_rev.encoded_len(),
            absent_rev.encoded_len(),
            "Some(0) 与 None 必须编码为不同字节"
        );
    }

    #[test]
    fn id_helpers_produce_valid_uuids() {
        let id = new_message_id();
        let corr = new_correlation_id();
        assert_ne!(id, corr);
        uuid::Uuid::parse_str(&id).expect("message_id must be a UUID");
        uuid::Uuid::parse_str(&corr).expect("correlation_id must be a UUID");
    }
}
