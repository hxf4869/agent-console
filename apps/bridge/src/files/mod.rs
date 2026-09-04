//! Bridge 文件数据面(权威规格 §22)。
//!
//! 组成:
//! - [`grant`]:file_handle 签发/复验/撤销(§22.2)。高熵随机 token,
//!   绑定 device/session/授权根/相对路径/动作/文件系统 identity/size/mtime/TTL;
//!   每次读取前重新解析 + fd identity 复核防 TOCTOU;Unix 阻断 symlink escape。
//! - [`policy`]:预览/下载策略与单 Range(§22.3、§22.4)。MIME 嗅探 +
//!   允许列表;限制集中定义并经 [`policy::preview_limits`] 暴露给能力响应。
//! - [`upload`]:上传落盘私有临时目录(0700),实际大小与 MIME 校验,
//!   成功签发 upload handle,带 TTL/生命周期清理(§22.5)。
//! - [`transfer`]:producer/consumer 出站 HTTPS 连接(§22.4、§22.5),
//!   Bearer device credential + transfer token,流式 64KiB 块,可取消、有超时。
//!
//! 错误统一映射 §27.6 稳定码(见 [`FilesError::stable_code`]),由上层转
//! StableErrorCode。日志只含 operation/稳定码/字节数(§25.3),不记录
//! 文件名、路径与内容;各结构的 Debug 实现对路径与 token 脱敏。

pub mod grant;
pub mod policy;
pub mod transfer;
pub mod upload;

pub use grant::{
    FileGrantManager, FileHandle, FileIdentity, GrantAction, GrantActions, GrantSource,
    GrantSourceKind, TransferSlot, VerifiedFile, DEFAULT_GRANT_TTL,
    MAX_ACTIVE_TRANSFERS_PER_DEVICE, MAX_GRANT_TTL,
};
pub use policy::{
    check_preview, classify, open_slice, parse_single_range, preview_limits, BoundedFileReader,
    Classification, FileSlice, HttpRange, TransferMode, CACHE_CONTROL, MAX_DOWNLOAD_BYTES,
    MAX_IMAGE_PREVIEW_BYTES, MAX_PDF_PREVIEW_BYTES, MAX_TEXT_PREVIEW_BYTES, MAX_UPLOAD_BYTES,
    NOSNIFF, SNIFF_PREFIX_BYTES, TRANSFER_CHUNK_BYTES,
};
pub use transfer::{
    consume, produce, ConsumerOutcome, ProducerOutcome, TimeoutPhase, TransferConfig,
    TransferError, CONSUMER_PATH_PREFIX, DEFAULT_FIRST_BYTE_TIMEOUT, DEFAULT_IDLE_READ_TIMEOUT,
    DEFAULT_RENDEZVOUS_TIMEOUT, DEVICE_CREDENTIAL_SCHEME, PRODUCER_PATH_PREFIX,
    TRANSFER_LENGTH_HEADER, TRANSFER_TOKEN_HEADER,
};
pub use upload::{
    remove_upload_dir, sweep_expired_uploads, UploadCleanup, UploadOutcome, UploadSink,
    DEFAULT_UPLOAD_TTL,
};

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

/// 超出预览限制的具体原因(§22.3:带大小原因 + 允许下载)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotPreviewableReason {
    /// MIME 嗅探不在预览允许列表内。
    #[error("type unsupported for inline preview")]
    TypeUnsupported,
    /// 类型可预览但超过该类型的大小上限。
    #[error("size {size} exceeds preview limit {limit}")]
    SizeLimit { size: u64, limit: u64 },
}

/// 文件数据面统一错误。消息不含路径与内容。
#[derive(Debug, thiserror::Error)]
pub enum FilesError {
    /// handle 不存在、被撤销、绑定不匹配、动作不允许、目标缺失等。
    #[error("file handle invalid: {0}")]
    HandleInvalid(&'static str),
    /// 解析后的路径落在授权根之外(symlink escape 等)。
    #[error("path outside authorized scope")]
    OutsideScope,
    /// 文件与签发时 identity/size/mtime 不一致,要求刷新 handle。
    #[error("file changed since handle was issued")]
    Changed,
    #[error("file type not previewable: {0}")]
    NotPreviewable(NotPreviewableReason),
    /// handle/transfer 已过 TTL。
    #[error("transfer expired")]
    TransferExpired,
    /// 超过下载/上传大小上限。
    #[error("transfer too large")]
    TooLarge,
    /// Range 无效、多段或越界(不回退整文件,§22.4)。
    #[error("transfer range invalid")]
    RangeInvalid,
    #[error("io error")]
    Io(#[source] std::io::Error),
    /// 超过每设备并发 transfer 上限。
    #[error("too many active transfers")]
    TooManyTransfers,
    #[error("internal error: {0}")]
    Internal(&'static str),
}

impl FilesError {
    /// 映射到 §27.6 稳定错误码,由上层转 StableErrorCode。
    pub fn stable_code(&self) -> &'static str {
        match self {
            FilesError::HandleInvalid(_) => "FILE_HANDLE_INVALID",
            FilesError::OutsideScope => "FILE_OUTSIDE_SCOPE",
            FilesError::Changed => "FILE_CHANGED",
            FilesError::NotPreviewable(_) => "FILE_TYPE_NOT_PREVIEWABLE",
            FilesError::TransferExpired => "TRANSFER_EXPIRED",
            FilesError::TooLarge => "TRANSFER_TOO_LARGE",
            FilesError::RangeInvalid => "TRANSFER_RANGE_INVALID",
            FilesError::TooManyTransfers => "RATE_LIMITED",
            FilesError::Io(_) | FilesError::Internal(_) => "INTERNAL_ERROR",
        }
    }
}

impl From<std::io::Error> for FilesError {
    fn from(e: std::io::Error) -> Self {
        FilesError::Io(e)
    }
}
