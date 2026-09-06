//! transport 错误类型。文本为稳定原因描述,不含凭据与 URL 凭据参数。

use agent_console_protocol::codec::CodecError;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// 连接/握手失败(文本为底层错误的安全摘要)。
    #[error("connect failed: {0}")]
    Connect(String),
    /// 出站队列满:调用方应丢弃或稍后重试,不得阻塞(§17.6)。
    #[error("outbound queue full")]
    QueueFull,
    /// 客户端已停止或句柄全部丢弃。
    #[error("client stopped")]
    Stopped,
    /// 出站帧写出失败(底层 WS/TCP 错误的安全摘要)。
    #[error("sink write failed: {0}")]
    Send(String),
    /// 出站帧在写预算内未完成:判链路失活,断开走退避重连(§26.2/§26.4)。
    #[error("sink write stalled beyond budget")]
    SendStalled,
    /// Envelope 编解码失败(含单帧超 1 MiB,§17.6)。
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
}
