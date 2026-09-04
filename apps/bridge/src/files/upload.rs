//! Browser 上传落盘(权威规格 §22.5)。
//!
//! - 写入调用方传入的 data_dir 下的私有临时目录:根目录与每个上传目录
//!   均为 0700,文件 0600(unix);流式写入,不整文件进内存。
//! - 执行实际大小校验(默认 [`MAX_UPLOAD_BYTES`] = 20 MiB,可注入更小值)
//!   与 MIME 嗅探(读文件头前缀)。
//! - 成功后经 [`FileGrantManager`] 签发只属于当前 session 的 upload handle
//!   (动作全集),并返回 [`UploadCleanup`] 记录供上层按 turn 生命周期/TTL
//!   清理;清理只删 Bridge 自己的临时目录,绝不触及用户原始文件。
//! - 不持久化、不记日志任何文件内容与原始文件名。

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use base64::Engine;
use tokio::io::AsyncWriteExt;

use super::grant::{FileGrantManager, GrantActions, GrantSource};
use super::policy::{MAX_UPLOAD_BYTES, SNIFF_PREFIX_BYTES};
use super::FilesError;

/// 上传根目录名(相对调用方传入的 data_dir)。
pub const UPLOAD_DIR_NAME: &str = "uploads";
/// 上传文件默认 TTL:1 小时;turn 生命周期结束即清,两者取先。
pub const DEFAULT_UPLOAD_TTL: Duration = Duration::from_secs(3600);

/// 上传落盘 sink。`Drop` 时若未 `finish`,尽力同步删除半成品文件,
/// 保证不留下孤儿数据。
pub struct UploadSink {
    dir: PathBuf,
    path: PathBuf,
    file: Option<tokio::fs::File>,
    prefix: Vec<u8>,
    size: u64,
    max_bytes: u64,
    finished: bool,
}

impl std::fmt::Debug for UploadSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadSink")
            .field("size", &self.size)
            .field("max_bytes", &self.max_bytes)
            .field("finished", &self.finished)
            .finish()
    }
}

/// 上传成功结果。
pub struct UploadOutcome {
    /// 绑定当前 device/session 的 upload handle(动作全集)。
    pub handle: super::grant::FileHandle,
    pub size: u64,
    /// 基于文件头嗅探的 MIME;无法识别时为 None(交上层按扩展名/默认值处理)。
    pub mime: Option<String>,
    /// 生命周期清理记录:只含 Bridge 临时目录路径,不含内容与原始文件名。
    pub cleanup: UploadCleanup,
}

/// 一次上传的生命周期清理记录。
#[derive(Debug, Clone)]
pub struct UploadCleanup {
    pub dir: PathBuf,
    pub expires_at: SystemTime,
}

impl UploadSink {
    /// 在 `upload_root` 下创建私有上传目录(0700)与目标文件(0600)。
    /// `max_bytes` 传 [`MAX_UPLOAD_BYTES`] 或更小值(不得更大)。
    pub async fn create(upload_root: &Path, max_bytes: u64) -> Result<Self, FilesError> {
        if max_bytes > MAX_UPLOAD_BYTES {
            return Err(FilesError::Internal("upload limit above first-version cap"));
        }
        tokio::fs::create_dir_all(upload_root).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(upload_root, std::fs::Permissions::from_mode(0o700)).await?;
        }

        let dir = upload_root.join(new_temp_name("u"));
        tokio::fs::create_dir(&dir).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).await?;
        }
        let path = dir.join("payload");
        let file = tokio::fs::File::create(&path).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .await?;
        }

        Ok(Self {
            dir,
            path,
            file: Some(file),
            prefix: Vec::new(),
            size: 0,
            max_bytes,
            finished: false,
        })
    }

    /// 已写入字节数。
    pub fn size(&self) -> u64 {
        self.size
    }

    /// 基于已写入前缀的 MIME 嗅探。
    pub fn sniffed_mime(&self) -> Option<String> {
        infer::get(&self.prefix).map(|t| t.mime_type().to_owned())
    }

    /// 落盘目标(Bridge 私有临时目录内),供上层交接 Codex 前参考。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 流式写入一块(§22.5:实际大小校验)。
    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<(), FilesError> {
        if self.finished {
            return Err(FilesError::Internal("upload sink already finished"));
        }
        if self.size + data.len() as u64 > self.max_bytes {
            tracing::debug!(
                code = "TRANSFER_TOO_LARGE",
                operation = "files.upload.chunk",
                "rejected"
            );
            return Err(FilesError::TooLarge);
        }
        // 保留前缀用于 MIME 嗅探;整体不缓冲。
        if self.prefix.len() < SNIFF_PREFIX_BYTES {
            let take = (SNIFF_PREFIX_BYTES - self.prefix.len()).min(data.len());
            self.prefix.extend_from_slice(&data[..take]);
        }
        let file = self
            .file
            .as_mut()
            .ok_or(FilesError::Internal("sink closed"))?;
        file.write_all(data).await?;
        self.size += data.len() as u64;
        Ok(())
    }

    /// 完成:刷盘、按 §22.5 第 6 步签发 upload handle 并返回清理记录。
    pub async fn finish(
        mut self,
        grants: &FileGrantManager,
        device_id: &str,
        session: &str,
        ttl: Duration,
    ) -> Result<UploadOutcome, FilesError> {
        let file = self
            .file
            .take()
            .ok_or(FilesError::Internal("sink closed"))?;
        file.sync_all().await?;
        self.finished = true;

        let handle = grants.issue(
            device_id,
            session,
            &self.dir,
            Path::new("payload"),
            GrantActions::all(),
            GrantSource::SessionUpload,
            ttl,
        )?;
        Ok(UploadOutcome {
            handle,
            size: self.size,
            mime: self.sniffed_mime(),
            cleanup: UploadCleanup {
                dir: self.dir.clone(),
                expires_at: SystemTime::now() + ttl,
            },
        })
    }

    /// 中止并删除半成品(调用方在错误路径上调用;Drop 亦兜底)。
    pub async fn abort(mut self) {
        self.file = None;
        self.finished = true;
        let _ = tokio::fs::remove_dir_all(&self.dir).await;
    }
}

impl Drop for UploadSink {
    fn drop(&mut self) {
        if !self.finished {
            // 未 finish 的半成品:同步尽力删除(单文件 + 空目录,不扫盘)。
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_dir(&self.dir);
        }
    }
}

/// 删除一次上传的临时目录(turn 结束、TTL 到期或用户取消后由上层调用)。
pub async fn remove_upload_dir(cleanup: &UploadCleanup) -> std::io::Result<()> {
    tokio::fs::remove_dir_all(&cleanup.dir).await
}

/// 清理上传根目录下修改时间早于 `older_than` 的上传目录,
/// 返回清理数量。只看目录 mtime,不读取内容;不触碰根外的任何路径。
pub async fn sweep_expired_uploads(
    upload_root: &Path,
    older_than: SystemTime,
) -> std::io::Result<usize> {
    let mut reader = tokio::fs::read_dir(upload_root).await?;
    let mut removed = 0usize;
    while let Some(entry) = reader.next_entry().await? {
        let md = entry.metadata().await?;
        if !md.is_dir() {
            continue;
        }
        if md.modified()? < older_than {
            tokio::fs::remove_dir_all(entry.path()).await?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn new_temp_name(tag: &str) -> String {
    use rand::RngCore;
    let mut buf = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut buf);
    format!(
        "{tag}-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
    )
}
