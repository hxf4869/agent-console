//! 预览/下载策略与单 Range(权威规格 §22.3、§22.4)。
//!
//! - 类型判定以 MIME 嗅探(infer,读文件头)为准,不只信扩展名;
//!   允许列表:UTF-8 可识别文本/源码/Markdown/JSON/日志、PNG/JPEG/WebP/GIF、PDF。
//! - HTML/SVG 无扩展名依赖地落入文本分支,一律按 `text/plain; charset=utf-8`
//!   inline 返回源码,不作为活动页面执行(`nosniff` 由上层一并落头)。
//! - 其余类型按 attachment 下载;不引入 DOCX/XLSX/PPTX 服务端转换。
//! - 限制集中定义,并经 [`preview_limits`] 作为能力数据暴露(§22.3);
//!   超预览限未超下载限 → [`FilesError::NotPreviewable`] + 允许下载;
//!   超下载限 → [`FilesError::TooLarge`]。
//! - Range 仅支持单个 `bytes=a-b / a- / -N`;无效、多段或越界返回
//!   [`FilesError::RangeInvalid`],绝不整文件回退(§22.4)。
//!
//! 输出结构 [`FileSlice`] 只携带数据与元信息,HTTP 头由上层落:
//! `nosniff`、受限 CSP、Content-Disposition、`Cache-Control: private, no-store`。

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf, Take};

use super::grant::VerifiedFile;
use super::{FilesError, NotPreviewableReason};

// ---------------------------------------------------------------------------
// 集中限制(§22.3、§22.5;能力响应经 preview_limits() 暴露)
// ---------------------------------------------------------------------------

/// 文本/源码 inline 预览上限:2 MiB。
pub const MAX_TEXT_PREVIEW_BYTES: u64 = 2 * 1024 * 1024;
/// 图片 inline 预览上限:25 MiB。
pub const MAX_IMAGE_PREVIEW_BYTES: u64 = 25 * 1024 * 1024;
/// PDF Range 预览的文件大小上限:100 MiB。
pub const MAX_PDF_PREVIEW_BYTES: u64 = 100 * 1024 * 1024;
/// 普通下载上限:512 MiB。
pub const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;
/// 单文件上传上限:20 MiB(§22.5;不得自动提高)。
pub const MAX_UPLOAD_BYTES: u64 = 20 * 1024 * 1024;
/// MIME 嗅探读取的文件头字节数。
pub const SNIFF_PREFIX_BYTES: usize = 8 * 1024;
/// 文件流分块大小(有界缓冲,不整文件进内存,§22.6)。
pub const TRANSFER_CHUNK_BYTES: usize = 64 * 1024;
/// `nosniff` 固定开启。
pub const NOSNIFF: bool = true;
/// 缓存策略固定为私有不缓存。
pub const CACHE_CONTROL: &str = "private, no-store";

/// 预览/下载能力数据(能力响应直接序列化本结构)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PreviewLimits {
    pub text_preview_bytes: u64,
    pub image_preview_bytes: u64,
    pub pdf_preview_bytes: u64,
    pub download_bytes: u64,
    pub upload_bytes: u64,
}

pub fn preview_limits() -> PreviewLimits {
    PreviewLimits {
        text_preview_bytes: MAX_TEXT_PREVIEW_BYTES,
        image_preview_bytes: MAX_IMAGE_PREVIEW_BYTES,
        pdf_preview_bytes: MAX_PDF_PREVIEW_BYTES,
        download_bytes: MAX_DOWNLOAD_BYTES,
        upload_bytes: MAX_UPLOAD_BYTES,
    }
}

// ---------------------------------------------------------------------------
// 分类
// ---------------------------------------------------------------------------

/// 预览分类结果;`content_type` 为响应 Content-Type 值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classification {
    /// 文本/源码/Markdown/JSON/日志/HTML/SVG:一律 text/plain 源码 inline。
    InlineText,
    InlineImage(&'static str),
    InlinePdf,
    /// 非 inline:attachment 下载,携带嗅探到的(或默认)Content-Type。
    Attachment(&'static str),
}

impl Classification {
    pub fn content_type(&self) -> &'static str {
        match self {
            Classification::InlineText => "text/plain; charset=utf-8",
            Classification::InlineImage(m) | Classification::Attachment(m) => m,
            Classification::InlinePdf => "application/pdf",
        }
    }

    pub fn is_inline(&self) -> bool {
        !matches!(self, Classification::Attachment(_))
    }

    fn preview_limit(&self) -> Option<u64> {
        match self {
            Classification::InlineText => Some(MAX_TEXT_PREVIEW_BYTES),
            Classification::InlineImage(_) => Some(MAX_IMAGE_PREVIEW_BYTES),
            Classification::InlinePdf => Some(MAX_PDF_PREVIEW_BYTES),
            Classification::Attachment(_) => None,
        }
    }
}

/// 从文件头前缀嗅探分类(infer 优先;无匹配时按 UTF-8 校验判文本)。
pub fn classify(prefix: &[u8]) -> Classification {
    if let Some(t) = infer::get(prefix) {
        // infer::Type::mime_type 返回 &'static str(matcher 常量表)。
        let mime: &'static str = t.mime_type();
        // §22.3:文本类(含 HTML/SVG/XML/脚本等源码)一律按源码文本
        // inline 返回,不作为活动页面执行;SVG 也不是可执行图片。
        if mime.starts_with("text/") || mime == "image/svg+xml" {
            return Classification::InlineText;
        }
        return match mime {
            "image/png" | "image/jpeg" | "image/webp" | "image/gif" => {
                Classification::InlineImage(mime)
            }
            "application/pdf" => Classification::InlinePdf,
            _ => Classification::Attachment(mime),
        };
    }
    match std::str::from_utf8(prefix) {
        // 文本(含 HTML/SVG 源码)一律按源码文本 inline,不执行。
        Ok(_) => Classification::InlineText,
        Err(_) => Classification::Attachment("application/octet-stream"),
    }
}

/// 纯策略:预览模式的大小检查。超限返回原因(§22.3:带大小原因 + 允许下载)。
pub fn check_preview(
    classification: &Classification,
    size: u64,
) -> Result<(), NotPreviewableReason> {
    match classification.preview_limit() {
        None => Err(NotPreviewableReason::TypeUnsupported),
        Some(limit) if size > limit => Err(NotPreviewableReason::SizeLimit { size, limit }),
        Some(_) => Ok(()),
    }
}

/// 纯策略:下载大小检查(§22.3:超下载限 → TRANSFER_TOO_LARGE)。
pub fn check_download(size: u64) -> Result<(), FilesError> {
    if size > MAX_DOWNLOAD_BYTES {
        Err(FilesError::TooLarge)
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Range(§22.4:仅单 Range,不整文件回退)
// ---------------------------------------------------------------------------

/// 已解析的单 Range(start 起、`end_inclusive` 止,基于整文件坐标)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRange {
    pub start: u64,
    pub end_inclusive: u64,
}

impl HttpRange {
    pub fn len(&self) -> u64 {
        self.end_inclusive - self.start + 1
    }

    /// `start-end/total` 形式。
    pub fn content_range_value(&self, total: u64) -> String {
        format!("bytes {}-{}/{}", self.start, self.end_inclusive, total)
    }
}

/// 解析 `Range` 头(如 `bytes=0-1023`、`bytes=100-`、`bytes=-500`)。
/// 规则(§22.4):
/// - 仅支持一个范围;含 `,`(多段)、非 bytes 单位或语法错误 → RangeInvalid;
/// - `start >= total`(越界)→ RangeInvalid;
/// - `end` 超过文件尾按 HTTP 语义截到 `total-1`;
/// - 后缀 `-N`:`N == 0` 非法;`N >= total` 覆盖整文件(允许);
/// - 空文件上的任何 Range 均不可满足 → RangeInvalid。
pub fn parse_single_range(header: &str, total: u64) -> Result<HttpRange, FilesError> {
    let spec = header
        .trim()
        .strip_prefix("bytes=")
        .ok_or(FilesError::RangeInvalid)?;
    if spec.contains(',') {
        // 多段 Range:稳定错误,不回退整文件。
        return Err(FilesError::RangeInvalid);
    }
    let spec = spec.trim();
    let (start_s, end_s) = spec.split_once('-').ok_or(FilesError::RangeInvalid)?;
    let range = if start_s.is_empty() {
        // 后缀形式:-N(取最后 N 字节)
        let n: u64 = end_s.trim().parse().map_err(|_| FilesError::RangeInvalid)?;
        if n == 0 || total == 0 {
            return Err(FilesError::RangeInvalid);
        }
        let start = total.saturating_sub(n);
        HttpRange {
            start,
            end_inclusive: total - 1,
        }
    } else {
        let start: u64 = start_s
            .trim()
            .parse()
            .map_err(|_| FilesError::RangeInvalid)?;
        if start >= total {
            // 越界:不可满足,稳定错误。
            return Err(FilesError::RangeInvalid);
        }
        let end = if end_s.is_empty() {
            total - 1
        } else {
            let e: u64 = end_s.trim().parse().map_err(|_| FilesError::RangeInvalid)?;
            if e < start {
                return Err(FilesError::RangeInvalid);
            }
            e.min(total - 1)
        };
        HttpRange {
            start,
            end_inclusive: end,
        }
    };
    Ok(range)
}

// ---------------------------------------------------------------------------
// FileSlice
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Preview,
    Download,
}

/// 有界文件读取器:最多读 `len` 字节(§22.6:不读取完整文件进内存)。
pub struct BoundedFileReader {
    inner: Take<tokio::fs::File>,
}

impl BoundedFileReader {
    pub fn limit(&self) -> u64 {
        self.inner.limit()
    }

    pub fn into_inner(self) -> Take<tokio::fs::File> {
        self.inner
    }
}

impl AsyncRead for BoundedFileReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

/// 一次文件切片读取的输出。HTTP 头(nosniff/CSP/Disposition/Cache-Control)
/// 由上层按这里的元数据落(§22.3)。Debug 对正文脱敏。
pub struct FileSlice {
    pub content_type: &'static str,
    /// true → `Content-Disposition: inline`;false → attachment。
    pub inline: bool,
    /// 正文流(位置已就绪,长度受界)。
    pub body: BoundedFileReader,
    /// 本次响应体字节数(整文件或 Range 长度)。
    pub body_len: u64,
    /// Some → 应答 206 并携带 `Content-Range`。
    pub content_range: Option<HttpRange>,
    /// 整文件大小(Range 的 `/total`)。
    pub total_len: u64,
}

impl std::fmt::Debug for FileSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSlice")
            .field("content_type", &self.content_type)
            .field("inline", &self.inline)
            .field("body", &"<stream>")
            .field("body_len", &self.body_len)
            .field("content_range", &self.content_range)
            .field("total_len", &self.total_len)
            .finish()
    }
}

impl FileSlice {
    pub fn content_range_value(&self) -> Option<String> {
        self.content_range
            .map(|r| r.content_range_value(self.total_len))
    }
}

/// 复验后的文件 → 预览/下载切片(§22.3 + §22.4)。
///
/// 流程:嗅探(从 fd 读取前缀后回卷,不二次打开文件)→ 下载限 →
/// 预览限(Preview 模式)→ 解析单 Range → 定位并返回有界读取器。
pub async fn open_slice(
    verified: VerifiedFile,
    mode: TransferMode,
    range_header: Option<&str>,
) -> Result<FileSlice, FilesError> {
    let total = verified.size;
    check_download(total)?;

    let mut file = tokio::fs::File::from(verified.file);
    // 嗅探:读前 N 字节后回卷;fd 与 identity 复核是同一个文件,无 TOCTOU。
    let want = (SNIFF_PREFIX_BYTES as u64).min(total) as usize;
    let mut prefix = vec![0u8; want];
    file.read_exact(&mut prefix).await?;
    file.seek(std::io::SeekFrom::Start(0)).await?;
    let classification = classify(&prefix);

    if mode == TransferMode::Preview {
        // 超预览限:FILE_TYPE_NOT_PREVIEWABLE(带大小原因),上层允许下载。
        check_preview(&classification, total).map_err(FilesError::NotPreviewable)?;
    }

    let content_range = match range_header {
        Some(h) => Some(parse_single_range(h, total)?),
        None => None,
    };
    let (offset, body_len) = match content_range {
        Some(r) => (r.start, r.len()),
        None => (0, total),
    };
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset)).await?;
    }

    Ok(FileSlice {
        content_type: classification.content_type(),
        inline: classification.is_inline(),
        body: BoundedFileReader {
            inner: file.take(body_len),
        },
        body_len,
        content_range,
        total_len: total,
    })
}
