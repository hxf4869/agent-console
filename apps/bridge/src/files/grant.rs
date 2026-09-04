//! file_handle 签发与复验(权威规格 §22.2)。
//!
//! 来源限制(仅三类,由上层调用 [`FileGrantManager::issue`] 时声明):
//! - 本会话上传或生成的文件([`GrantSource::SessionUpload`]);
//! - 当前会话工具、Diff 或文件变化明确引用的文件
//!   ([`GrantSource::SessionReference`],须携带非空引用证据字符串);
//! - 用户在已授权工作区内从会话上下文明确打开的文件([`GrantSource::UserOpened`])。
//!
//! handle 绑定 device、session、授权根(canonical)、相对路径、允许动作、
//! 文件系统 identity(dev+ino)、size、mtime、TTL。token 为 32 字节高熵随机值
//! 的 base64url 表示;Relay 与日志均不接触本机路径。
//!
//! 每次读取前复验(§22.2 逐条):
//! 1. token 有效期(device/session/动作校验);
//! 2. 从授权根重新 canonical 解析,全链 symlink 解析后仍须以根为前缀,
//!    否则 [`FilesError::OutsideScope`] —— 覆盖目标自身与父目录中的 symlink;
//! 3. 打开后用 fd 再次核对 dev/ino/size/mtime,不一致返回
//!    [`FilesError::Changed`](防 TOCTOU,不静默读取新目标);
//! 4. 不做内容哈希、不扫描整个文件。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use base64::Engine;

use super::FilesError;

/// 默认 handle TTL:10 分钟(短期,§22.2)。
pub const DEFAULT_GRANT_TTL: Duration = Duration::from_secs(600);
/// TTL 上限:1 小时。
pub const MAX_GRANT_TTL: Duration = Duration::from_secs(3600);
/// 每设备同时活跃 transfer 上限(§22.6,首版默认 2)。
pub const MAX_ACTIVE_TRANSFERS_PER_DEVICE: usize = 2;

/// token 随机字节数(高熵引用,§22.2)。
const TOKEN_BYTES: usize = 32;

// ---------------------------------------------------------------------------
// 类型
// ---------------------------------------------------------------------------

/// 允许动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantAction {
    Preview,
    Download,
    /// 该 handle 可作为上传引用交给 Codex(§22.5 第 6 步)。
    Upload,
}

/// 动作位集。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GrantActions(u8);

impl GrantActions {
    pub const PREVIEW: Self = Self(1);
    pub const DOWNLOAD: Self = Self(1 << 1);
    pub const UPLOAD: Self = Self(1 << 2);

    pub const fn all() -> Self {
        Self(0b111)
    }

    pub const fn allows(self, a: GrantAction) -> bool {
        let bit = match a {
            GrantAction::Preview => Self::PREVIEW.0,
            GrantAction::Download => Self::DOWNLOAD.0,
            GrantAction::Upload => Self::UPLOAD.0,
        };
        self.0 & bit != 0
    }
}

impl std::ops::BitOr for GrantActions {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for GrantActions {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// 签发来源声明(§22.2 三类)。
#[derive(Debug, Clone)]
pub enum GrantSource {
    SessionUpload,
    /// 引用证据字符串由上层提供;本模块只校验非空,不存储、不记日志。
    SessionReference {
        evidence: String,
    },
    UserOpened,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantSourceKind {
    Upload,
    SessionReference,
    UserOpened,
}

/// 文件系统 identity(防 TOCTOU 的锚点)。非 unix 目标 identity 恒为 0,
/// 退化为 size+mtime 校验(首版不做 Windows)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
}

/// 已签发的 handle(对外可见快照)。
/// Debug 实现对 token、根与相对路径脱敏(§25.3 禁止文件名/绝对路径入日志)。
#[derive(Clone)]
pub struct FileHandle {
    pub token: String,
    pub device_id: String,
    pub session: String,
    /// 授权根(canonical)。仅本机内部使用。
    pub root: PathBuf,
    /// 相对根的规范化相对路径('/' 分隔)。
    pub relative_path: String,
    pub actions: GrantActions,
    pub source_kind: GrantSourceKind,
    pub identity: FileIdentity,
    pub size: u64,
    /// 签发时的 mtime(纳秒,unix epoch);非 unix 为 0。
    pub mtime_ns: i128,
    pub expires_at: SystemTime,
}

impl std::fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHandle")
            .field("token", &"<redacted>")
            .field("device_id", &self.device_id)
            .field("session", &self.session)
            .field("root", &"<redacted>")
            .field("relative_path", &"<redacted>")
            .field("actions", &self.actions)
            .field("source_kind", &self.source_kind)
            .field("identity", &self.identity)
            .field("size", &self.size)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// 复验通过后的已打开文件:fd 的 identity 已与签发时核对一致。
/// `file` 位置为 0,读前可 seek;调用方负责消费后关闭(随 Drop)。
pub struct VerifiedFile {
    pub file: std::fs::File,
    pub size: u64,
    pub mtime_ns: i128,
    pub identity: FileIdentity,
    pub handle: FileHandle,
}

impl std::fmt::Debug for VerifiedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedFile")
            .field("file", &"<fd>")
            .field("size", &self.size)
            .field("identity", &self.identity)
            .field("handle", &"<redacted>")
            .finish()
    }
}

struct GrantEntry {
    handle: FileHandle,
    deadline: Instant,
}

struct Inner {
    grants: parking_lot::Mutex<HashMap<String, GrantEntry>>,
    transfers: parking_lot::Mutex<HashMap<String, usize>>,
}

/// file_handle 管理器。`Clone` 廉价共享。
#[derive(Clone)]
pub struct FileGrantManager {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for FileGrantManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileGrantManager").finish()
    }
}

impl Default for FileGrantManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 实现
// ---------------------------------------------------------------------------

impl FileGrantManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                grants: parking_lot::Mutex::new(HashMap::new()),
                transfers: parking_lot::Mutex::new(HashMap::new()),
            }),
        }
    }

    /// 签发 handle。上层声明来源与动作;本模块校验参数完整性、
    /// 根内路径与存在性,并在 canonical 解析后记录 identity(§22.2)。
    ///
    /// `root` 会被 canonical 化;`relative_path` 必须是根内相对路径
    /// (词法校验:仅普通组件,拒绝绝对路径与 `..`)。
    pub fn issue(
        &self,
        device_id: &str,
        session: &str,
        root: &Path,
        relative_path: &Path,
        actions: GrantActions,
        source: GrantSource,
        ttl: Duration,
    ) -> Result<FileHandle, FilesError> {
        if device_id.is_empty() || session.is_empty() {
            return Err(FilesError::Internal("device/session required"));
        }
        if actions.0 == 0 {
            return Err(FilesError::Internal("at least one action required"));
        }
        let source_kind = match &source {
            GrantSource::SessionUpload => GrantSourceKind::Upload,
            GrantSource::SessionReference { evidence } => {
                if evidence.trim().is_empty() {
                    return Err(FilesError::Internal("session reference requires evidence"));
                }
                GrantSourceKind::SessionReference
            }
            GrantSource::UserOpened => GrantSourceKind::UserOpened,
        };
        let ttl = ttl.min(MAX_GRANT_TTL);

        let root_c =
            std::fs::canonicalize(root).map_err(|_| FilesError::HandleInvalid("root missing"))?;
        if !root_c.is_dir() {
            return Err(FilesError::HandleInvalid("root not a directory"));
        }
        let rel = normalize_relative(relative_path)?;
        let target = root_c.join(&rel);
        // 全链 canonical 解析:目标自身与父目录中的 symlink 一并解析,
        // 结果仍须以根为前缀,否则按 symlink escape 拒绝。
        let target_c = std::fs::canonicalize(&target)
            .map_err(|_| FilesError::HandleInvalid("target missing"))?;
        if !target_c.starts_with(&root_c) {
            tracing::debug!(
                code = "FILE_OUTSIDE_SCOPE",
                operation = "files.grant.issue",
                "rejected"
            );
            return Err(FilesError::OutsideScope);
        }
        let meta = std::fs::metadata(&target_c)?;
        if !meta.is_file() {
            return Err(FilesError::HandleInvalid("target is not a regular file"));
        }

        let token = new_token();
        let handle = FileHandle {
            token,
            device_id: device_id.to_owned(),
            session: session.to_owned(),
            root: root_c,
            relative_path: rel,
            actions,
            source_kind,
            identity: identity_of(&meta),
            size: meta.len(),
            mtime_ns: mtime_ns_of(&meta),
            expires_at: SystemTime::now() + ttl,
        };
        self.inner.grants.lock().insert(
            handle.token.clone(),
            GrantEntry {
                handle: handle.clone(),
                deadline: Instant::now() + ttl,
            },
        );
        tracing::debug!(
            operation = "files.grant.issue",
            source = ?source_kind,
            size = handle.size,
            "handle issued"
        );
        Ok(handle)
    }

    /// 读取前复验并打开文件(§22.2 逐条)。返回的 fd 已核对 identity。
    pub fn resolve_for_read(
        &self,
        device_id: &str,
        session: &str,
        token: &str,
        action: GrantAction,
    ) -> Result<VerifiedFile, FilesError> {
        let handle = self.lookup_valid(device_id, session, token, action)?;

        // 重新从授权根解析(canonical 全链,防父目录 symlink escape)。
        let root_c = std::fs::canonicalize(&handle.root)
            .map_err(|_| FilesError::HandleInvalid("root missing"))?;
        let target_c = std::fs::canonicalize(root_c.join(&handle.relative_path))
            .map_err(|_| FilesError::HandleInvalid("target missing"))?;
        if !target_c.starts_with(&root_c) {
            tracing::debug!(
                code = "FILE_OUTSIDE_SCOPE",
                operation = "files.grant.resolve",
                "rejected"
            );
            return Err(FilesError::OutsideScope);
        }

        // 打开后用 fd 复核 identity(§22.2:防检查与使用之间替换)。
        let file = std::fs::File::open(&target_c)?;
        let fd_meta = file.metadata()?;
        if identity_of(&fd_meta) != handle.identity
            || fd_meta.len() != handle.size
            || mtime_ns_of(&fd_meta) != handle.mtime_ns
        {
            tracing::debug!(
                code = "FILE_CHANGED",
                operation = "files.grant.resolve",
                "rejected"
            );
            return Err(FilesError::Changed);
        }

        Ok(VerifiedFile {
            file,
            size: handle.size,
            mtime_ns: handle.mtime_ns,
            identity: handle.identity,
            handle,
        })
    }

    /// 显式撤销;返回是否确有该 handle。
    pub fn revoke(&self, token: &str) -> bool {
        self.inner.grants.lock().remove(token).is_some()
    }

    /// 清理已过期 handle,返回清理数量(上层可周期调用)。
    pub fn sweep_expired(&self) -> usize {
        let now = Instant::now();
        let mut grants = self.inner.grants.lock();
        let before = grants.len();
        grants.retain(|_, e| e.deadline > now);
        before - grants.len()
    }

    /// 占用一个并发 transfer 名额(§22.6:每设备默认 2)。
    /// 返回的 guard Drop 时自动释放。
    pub fn acquire_transfer_slot(&self, device_id: &str) -> Result<TransferSlot, FilesError> {
        let mut transfers = self.inner.transfers.lock();
        let count = transfers.entry(device_id.to_owned()).or_insert(0);
        if *count >= MAX_ACTIVE_TRANSFERS_PER_DEVICE {
            tracing::debug!(
                code = "RATE_LIMITED",
                operation = "files.transfer.slot",
                "rejected"
            );
            return Err(FilesError::TooManyTransfers);
        }
        *count += 1;
        Ok(TransferSlot {
            inner: Arc::clone(&self.inner),
            device_id: device_id.to_owned(),
        })
    }

    fn lookup_valid(
        &self,
        device_id: &str,
        session: &str,
        token: &str,
        action: GrantAction,
    ) -> Result<FileHandle, FilesError> {
        let mut grants = self.inner.grants.lock();
        let entry = match grants.get(token) {
            Some(e) => e,
            None => return Err(FilesError::HandleInvalid("unknown token")),
        };
        if entry.deadline <= Instant::now() {
            grants.remove(token);
            return Err(FilesError::TransferExpired);
        }
        if entry.handle.device_id != device_id || entry.handle.session != session {
            return Err(FilesError::HandleInvalid("binding mismatch"));
        }
        if !entry.handle.actions.allows(action) {
            return Err(FilesError::HandleInvalid("action not allowed"));
        }
        Ok(entry.handle.clone())
    }
}

/// 并发 transfer 名额 guard,Drop 自动释放。
pub struct TransferSlot {
    inner: Arc<Inner>,
    device_id: String,
}

impl std::fmt::Debug for TransferSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferSlot").finish()
    }
}

impl Drop for TransferSlot {
    fn drop(&mut self) {
        let mut transfers = self.inner.transfers.lock();
        if let Some(count) = transfers.get_mut(&self.device_id) {
            *count -= 1;
            if *count == 0 {
                transfers.remove(&self.device_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn new_token() -> String {
    use rand::RngCore;
    let mut buf = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// 词法校验并规范化相对路径:仅普通组件,'/' 连接。
fn normalize_relative(rel: &Path) -> Result<String, FilesError> {
    if rel.as_os_str().is_empty() || rel.is_absolute() {
        return Err(FilesError::HandleInvalid("relative path required"));
    }
    let mut parts = Vec::new();
    for comp in rel.components() {
        match comp {
            std::path::Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
            _ => return Err(FilesError::HandleInvalid("path traversal rejected")),
        }
    }
    if parts.is_empty() {
        return Err(FilesError::HandleInvalid("relative path required"));
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn identity_of(md: &std::fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity {
        dev: md.dev(),
        ino: md.ino(),
    }
}

#[cfg(not(unix))]
fn identity_of(_md: &std::fs::Metadata) -> FileIdentity {
    FileIdentity { dev: 0, ino: 0 }
}

#[cfg(unix)]
fn mtime_ns_of(md: &std::fs::Metadata) -> i128 {
    use std::os::unix::fs::MetadataExt;
    md.mtime() as i128 * 1_000_000_000 + md.mtime_nsec() as i128
}

#[cfg(not(unix))]
fn mtime_ns_of(_md: &std::fs::Metadata) -> i128 {
    0
}
