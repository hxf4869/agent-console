//! Codex Desktop IPC 帧编解码。
//!
//! 线上格式:4 字节小端 u32 长度前缀 + UTF-8 JSON payload。
//! 参见 `docs/CODEX-IPC-PROTOCOL.md` §1。
//!
//! 设计约束(执行规格 §12):
//! - 读侧流式解析:半包、粘包、跨 chunk 的 UTF-8 字符都必须安全。
//! - 单帧尺寸有上限;超限不 panic,返回错误并把连接交由上层处置。
//! - 未知帧内容不在此层解释,原样交给消息层。

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// 协议线上硬上限(Desktop 校验 1..=256 MiB)。
pub const PROTOCOL_MAX_FRAME_BYTES: u32 = 256 * 1024 * 1024;

/// Bridge 默认单帧上限。刻意远小于协议上限(§12:一次连接的帧尺寸有上限)。
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame length {announced} bytes exceeds limit {limit} bytes")]
    TooLarge { announced: u32, limit: u32 },
    #[error("frame length 0 is invalid")]
    ZeroLength,
    #[error("frame payload is not valid UTF-8")]
    InvalidUtf8,
    #[error("frame payload is not a JSON object")]
    NotAnObject,
    #[error("failed to encode frame: {0}")]
    Encode(String),
    #[error("io error while reading frame: {0}")]
    Io(#[from] std::io::Error),
}

/// 增量帧解码器:把任意切分的字节流切成完整 JSON 对象帧。
#[derive(Debug)]
pub struct FrameDecoder {
    max_frame_bytes: u32,
    buf: BytesMut,
    /// 当前帧已宣告的长度;None 表示尚未读满长度前缀。
    current_len: Option<u32>,
}

impl FrameDecoder {
    pub fn new(max_frame_bytes: u32) -> Self {
        assert!(max_frame_bytes >= 8, "max_frame_bytes too small");
        Self {
            max_frame_bytes,
            buf: BytesMut::with_capacity(64 * 1024),
            current_len: None,
        }
    }

    pub fn max_frame_bytes(&self) -> u32 {
        self.max_frame_bytes
    }

    /// 追加一段原始字节。超限帧立即报错;半包/粘包安全。
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), FrameError> {
        self.buf.extend_from_slice(chunk);
        // 若存在已宣告但超限的帧,push 时即拒绝,避免无界缓冲。
        if let Some(len) = self.current_len {
            self.check_limit(len)?;
        } else if self.buf.len() >= 4 {
            let announced =
                u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
            self.check_limit(announced)?;
        }
        Ok(())
    }

    fn check_limit(&self, announced: u32) -> Result<(), FrameError> {
        if announced == 0 {
            return Err(FrameError::ZeroLength);
        }
        if announced > self.max_frame_bytes {
            return Err(FrameError::TooLarge {
                announced,
                limit: self.max_frame_bytes,
            });
        }
        Ok(())
    }

    /// 取出一条完整帧并解析为 JSON 对象。
    ///
    /// 返回:
    /// - `Ok(Some(value))`:取到一条完整帧。
    /// - `Ok(None)`:缓冲中暂无完整帧。
    /// - `Err(...)`:帧格式错误(UTF-8 / JSON / 尺寸)。错误后解码器状态不可恢复,
    ///   上层应断开连接并走重连(错误即协议破坏,继续读取无意义)。
    pub fn next_frame(&mut self) -> Result<Option<serde_json::Value>, FrameError> {
        loop {
            let len = match self.current_len {
                Some(len) => len,
                None => {
                    if self.buf.len() < 4 {
                        return Ok(None);
                    }
                    let announced =
                        u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                    self.check_limit(announced)?;
                    self.buf.advance(4);
                    self.current_len = Some(announced);
                    announced
                }
            };
            let need = len as usize;
            if self.buf.len() < need {
                return Ok(None);
            }
            let payload = self.buf.split_to(need);
            self.current_len = None;
            let text = std::str::from_utf8(&payload).map_err(|_| FrameError::InvalidUtf8)?;
            let value: serde_json::Value =
                serde_json::from_str(text).map_err(|_| FrameError::NotAnObject)?;
            if !value.is_object() {
                return Err(FrameError::NotAnObject);
            }
            return Ok(Some(value));
        }
    }

    /// 缓冲中尚未凑齐的字节数(诊断用)。
    pub fn pending_bytes(&self) -> usize {
        self.buf.len()
    }
}

/// 把一条消息编码为待发送的完整帧(长度前缀 + JSON)。
///
/// 序列化失败或超出 `max_frame_bytes` 时返回错误,不产生半截帧。
pub fn encode_frame(
    value: &serde_json::Value,
    max_frame_bytes: u32,
) -> Result<BytesMut, FrameEncodeError> {
    let text = serde_json::to_string(value).map_err(FrameEncodeError::Serialize)?;
    let payload = text.as_bytes();
    let total = 4 + payload.len();
    if payload.is_empty() || payload.len() > max_frame_bytes as usize {
        return Err(FrameEncodeError::TooLarge {
            announced: payload.len() as u32,
            limit: max_frame_bytes,
        });
    }
    let mut out = BytesMut::with_capacity(total);
    out.put_u32_le(payload.len() as u32);
    out.put_slice(payload);
    Ok(out)
}

#[derive(Debug, Error)]
pub enum FrameEncodeError {
    #[error("frame payload {announced} bytes exceeds limit {limit} bytes")]
    TooLarge { announced: u32, limit: u32 },
    #[error("failed to serialize frame: {0}")]
    Serialize(#[from] serde_json::Error),
}

impl From<FrameEncodeError> for FrameError {
    fn from(e: FrameEncodeError) -> Self {
        match e {
            FrameEncodeError::TooLarge { announced, limit } => {
                FrameError::TooLarge { announced, limit }
            }
            FrameEncodeError::Serialize(msg) => FrameError::Encode(msg.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame_json(v: serde_json::Value) -> Vec<u8> {
        let text = serde_json::to_string(&v).unwrap();
        let mut out = Vec::with_capacity(4 + text.len());
        out.extend_from_slice(&(text.len() as u32).to_le_bytes());
        out.extend_from_slice(text.as_bytes());
        out
    }

    #[test]
    fn decodes_single_frame() {
        let mut dec = FrameDecoder::new(DEFAULT_MAX_FRAME_BYTES);
        let raw = frame_json(json!({"type": "broadcast", "method": "x"}));
        dec.push(&raw).unwrap();
        let v = dec.next_frame().unwrap().unwrap();
        assert_eq!(v["method"], "x");
        assert!(dec.next_frame().unwrap().is_none());
    }

    #[test]
    fn decodes_split_frames_across_utf8_boundary() {
        // 多字节 UTF-8 字符被 chunk 切开也必须安全。
        let mut dec = FrameDecoder::new(DEFAULT_MAX_FRAME_BYTES);
        let raw = frame_json(json!({"text": "中文内容🎯字幕"}));
        for byte in &raw {
            dec.push(std::slice::from_ref(byte)).unwrap();
        }
        let v = dec.next_frame().unwrap().unwrap();
        assert_eq!(v["text"], "中文内容🎯字幕");
    }

    #[test]
    fn decodes_coalesced_frames() {
        let mut dec = FrameDecoder::new(DEFAULT_MAX_FRAME_BYTES);
        let mut raw = frame_json(json!({"seq": 1}));
        raw.extend_from_slice(&frame_json(json!({"seq": 2})));
        dec.push(&raw).unwrap();
        assert_eq!(dec.next_frame().unwrap().unwrap()["seq"], 1);
        assert_eq!(dec.next_frame().unwrap().unwrap()["seq"], 2);
        assert!(dec.next_frame().unwrap().is_none());
    }

    #[test]
    fn rejects_zero_length_frame() {
        let mut dec = FrameDecoder::new(DEFAULT_MAX_FRAME_BYTES);
        let err = dec.push(&0u32.to_le_bytes()).unwrap_err();
        assert!(matches!(err, FrameError::ZeroLength));
    }

    #[test]
    fn rejects_oversized_frame_at_push() {
        let mut dec = FrameDecoder::new(1024);
        let err = dec.push(&(5000u32).to_le_bytes()).unwrap_err();
        assert!(matches!(
            err,
            FrameError::TooLarge {
                announced: 5000,
                limit: 1024
            }
        ));
    }

    #[test]
    fn rejects_oversized_frame_at_prefix_completion() {
        let mut dec = FrameDecoder::new(8);
        // 长度前缀单次 push 不足 4 字节时,延迟到凑齐前缀再判定。
        dec.push(&[0x10, 0x00]).unwrap();
        let err = dec.push(&[0x00, 0x00]).unwrap_err();
        assert!(matches!(
            err,
            FrameError::TooLarge {
                announced: 16,
                limit: 8
            }
        ));
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let big = json!({"blob": "x".repeat(2048)});
        let err = encode_frame(&big, 1024).unwrap_err();
        assert!(matches!(err, FrameEncodeError::TooLarge { .. }));
    }

    #[test]
    fn encode_roundtrip() {
        let v = json!({"type": "request", "n": 42});
        let frame = encode_frame(&v, DEFAULT_MAX_FRAME_BYTES).unwrap();
        let mut dec = FrameDecoder::new(DEFAULT_MAX_FRAME_BYTES);
        dec.push(&frame).unwrap();
        assert_eq!(dec.next_frame().unwrap().unwrap(), v);
    }
}
