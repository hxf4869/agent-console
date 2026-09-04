//! Git 只读能力(权威规格 §23.1)。
//!
//! 安全模型:
//! - 仅对"传入 cwd 所属且落在授权根列表内"的 Git worktree 提供服务;
//!   cwd 与 worktree 根都必须位于同一个授权根之内,否则 [`GitError::OutsideScope`]。
//! - 固定 executable(从 PATH 解析一次缓存,或调用方注入)+ 固定 argv 数组,
//!   不经 shell、不拼接 pathspec 字符串;所有参数以独立 argv 直传。
//! - 统一加 `--no-optional-locks`(绝不触碰 index.lock,真只读)、`--no-pager`、
//!   `-c core.pager=cat`;diff 另加 `--no-ext-diff`、`--no-color`、`--no-textconv`。
//! - 每条命令有超时(默认 [`DEFAULT_GIT_TIMEOUT`],可注入)与输出上限
//!   (默认 [`MAX_DIFF_OUTPUT_BYTES`] = 2 MiB,§27.5)。
//! - 错误映射稳定码(§27.6):非 worktree/未授权 → `FILE_OUTSIDE_SCOPE`;
//!   输出超限 → `DIFF_TOO_LARGE`;其余(spawn/超时/git 失败)→ `INTERNAL_ERROR`,
//!   消息只带 stderr 摘要,不含文件内容。
//!
//! 日志只允许 operation、字节数、耗时与稳定码(§25.3),不记录 cwd、路径与输出。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

/// 默认单条 git 命令超时(§23.1:限制时间)。
pub const DEFAULT_GIT_TIMEOUT: Duration = Duration::from_secs(5);
/// 单响应输出上限:2 MiB(§27.5:单个 Diff 响应最大 2 MiB)。
pub const MAX_DIFF_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
/// stderr 摘要截断长度(仅诊断用,不含文件内容)。
const MAX_STDERR_BYTES: usize = 4 * 1024;

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    /// cwd、worktree 根或目标路径不在授权范围内(含"不是 Git worktree")。
    #[error("cwd or worktree outside authorized roots")]
    OutsideScope,
    /// 相对路径非法(绝对路径、`..`、pathspec magic 等)。
    #[error("invalid relative path")]
    InvalidPath,
    /// git 可执行文件不存在或不可执行。
    #[error("git executable not found (searched PATH)")]
    GitNotFound,
    /// spawn 失败(注入的 executable 无效等)。
    #[error("spawn git failed: {0}")]
    Spawn(#[source] std::io::Error),
    /// 命令超时(进程已被 kill)。
    #[error("git operation '{operation}' timed out after {timeout_secs}s")]
    Timeout {
        operation: &'static str,
        timeout_secs: u64,
    },
    /// git 以非零状态退出。
    #[error("git operation '{operation}' failed (exit {code:?})")]
    GitFailed {
        operation: &'static str,
        code: Option<i32>,
        /// stderr 首行摘要(截断;git 读命令的 stderr 不含文件内容)。
        stderr_brief: String,
    },
    /// 输出超过上限。
    #[error("git output exceeded {limit} bytes for '{operation}'")]
    OutputTooLarge {
        operation: &'static str,
        limit: usize,
    },
}

impl GitError {
    /// 映射到 §27.6 稳定错误码,由上层转 StableErrorCode。
    pub fn stable_code(&self) -> &'static str {
        match self {
            GitError::OutsideScope | GitError::InvalidPath => "FILE_OUTSIDE_SCOPE",
            GitError::OutputTooLarge { .. } => "DIFF_TOO_LARGE",
            GitError::GitNotFound
            | GitError::Spawn(_)
            | GitError::Timeout { .. }
            | GitError::GitFailed { .. } => "INTERNAL_ERROR",
        }
    }
}

// ---------------------------------------------------------------------------
// 数据结构
// ---------------------------------------------------------------------------

/// 单个状态条目:路径相对 worktree 根;rename 时 `orig_path` 为旧路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub orig_path: Option<String>,
    /// porcelain v1 的 index(X)状态字符。
    pub index_status: char,
    /// porcelain v1 的 worktree(Y)状态字符。
    pub worktree_status: char,
}

/// Git 概要(§23.1)。`root` 仅用于本机内部识别与相对路径计算,
/// 不得下发 Browser;Browser 只见安全显示名与相对路径。
#[derive(Debug, Clone, Default)]
pub struct GitSummary {
    /// detached HEAD 明确标志(此时 `branch` 为 None)。
    pub detached_head: bool,
    /// 当前分支;detached HEAD 时为 None。
    pub branch: Option<String>,
    /// HEAD 完整 ID;unborn 分支(空仓库)为 None。
    pub head_full: Option<String>,
    /// HEAD 短 ID。
    pub head_short: Option<String>,
    /// worktree 根(canonical),内部识别用,禁止下发。
    pub root: PathBuf,
    pub staged: Vec<FileEntry>,
    pub unstaged: Vec<FileEntry>,
    pub untracked: Vec<FileEntry>,
    pub added: Vec<FileEntry>,
    pub modified: Vec<FileEntry>,
    pub deleted: Vec<FileEntry>,
    pub renamed: Vec<FileEntry>,
    /// 相对 HEAD 的总新增行(`git diff --numstat HEAD` 汇总,不含 untracked)。
    pub total_added_lines: u64,
    /// 相对 HEAD 的总删除行。
    pub total_deleted_lines: u64,
    /// numstat 标记为 binary 的文件(相对路径),不计入增删行。
    pub binary_files: Vec<String>,
    /// false 表示 unborn HEAD,无 numstat 数据。
    pub numstat_available: bool,
}

impl GitSummary {
    /// 防泄漏自查:所有状态条目路径必须是相对路径(§23.1:Browser 只见相对路径)。
    pub fn entries_all_relative(&self) -> bool {
        let check = |list: &Vec<FileEntry>| {
            list.iter().all(|e| {
                !Path::new(&e.path).is_absolute()
                    && e.orig_path
                        .as_ref()
                        .is_none_or(|p| !Path::new(p).is_absolute())
            })
        };
        [
            (&self.staged),
            (&self.unstaged),
            (&self.untracked),
            (&self.added),
            (&self.modified),
            (&self.deleted),
            (&self.renamed),
        ]
        .into_iter()
        .all(check)
    }
}

/// 单文件 staged/unstaged diff(§23.1:按需加载、限制单响应大小)。
#[derive(Debug, Clone)]
pub struct GitFileDiff {
    pub relative_path: String,
    pub staged: bool,
    /// true 表示输出在 [`MAX_DIFF_OUTPUT_BYTES`](或注入上限)处截断;
    /// 上层应提供"下载完整 diff"选择,不得把截断内容当作完整结果。
    pub truncated: bool,
    /// diff 正文(≤ 上限字节数;可能非 UTF-8,由上层决定编码处理)。
    pub content: Vec<u8>,
}

// ---------------------------------------------------------------------------
// GitService
// ---------------------------------------------------------------------------

/// Git 只读服务。`Clone` 廉价共享;无可变状态,天然并发安全。
pub struct GitService {
    git_exe: PathBuf,
    timeout: Duration,
    max_output: usize,
}

impl std::fmt::Debug for GitService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitService")
            .field("git_exe", &"<resolved>")
            .field("timeout", &self.timeout)
            .field("max_output", &self.max_output)
            .finish()
    }
}

/// 从 PATH 解析一次 git 绝对路径(unix 校验可执行位)。解析结果由调用方缓存。
pub fn resolve_git_from_path() -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(if cfg!(windows) { "git.exe" } else { "git" });
        if let Ok(md) = std::fs::metadata(&candidate) {
            if md.is_file() {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if md.permissions().mode() & 0o111 != 0 {
                        return Some(candidate);
                    }
                }
                #[cfg(not(unix))]
                return Some(candidate);
            }
        }
    }
    None
}

/// 一次命令执行的原始结果。非零退出不在此处报错,由调用方解释。
struct CmdOutput {
    success: bool,
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    /// stdout 在上限处被截断。
    stdout_truncated: bool,
}

impl CmdOutput {
    fn stderr_brief(&self) -> String {
        let line = self
            .stderr
            .split(|&b| b == b'\n')
            .find(|l| !l.is_empty())
            .unwrap_or(&[]);
        let mut s = String::from_utf8_lossy(line).into_owned();
        if s.len() > 200 {
            s.truncate(200);
        }
        s
    }

    fn failed(&self, operation: &'static str) -> GitError {
        GitError::GitFailed {
            operation,
            code: self.code,
            stderr_brief: self.stderr_brief(),
        }
    }
}

impl GitService {
    /// 系统默认:从 PATH 解析 git,5s 超时,2 MiB 输出上限。
    pub fn new() -> Result<Self, GitError> {
        let exe = resolve_git_from_path().ok_or(GitError::GitNotFound)?;
        Ok(Self::with_limits(
            exe,
            DEFAULT_GIT_TIMEOUT,
            MAX_DIFF_OUTPUT_BYTES,
        ))
    }

    /// 注入 git executable(测试用 fake / 部署环境固定路径)。
    pub fn with_git_exe(exe: PathBuf) -> Self {
        Self::with_limits(exe, DEFAULT_GIT_TIMEOUT, MAX_DIFF_OUTPUT_BYTES)
    }

    /// 全量注入:executable、超时、输出上限。
    pub fn with_limits(git_exe: PathBuf, timeout: Duration, max_output: usize) -> Self {
        Self {
            git_exe,
            timeout,
            max_output,
        }
    }

    /// 读取 Git 概要(§23.1)。
    pub async fn read_summary(
        &self,
        authorized_roots: &[PathBuf],
        cwd: &Path,
    ) -> Result<GitSummary, GitError> {
        let (root, top) = self.authorize_worktree(authorized_roots, cwd).await?;

        // branch / detached:`symbolic-ref --short -q HEAD` 在 detached 时
        // 以非零退出且无输出;这不算失败。
        let sym = self
            .execute(&top, "symbolic-ref", |args| {
                args.arg("symbolic-ref")
                    .arg("--short")
                    .arg("-q")
                    .arg("HEAD")
            })
            .await?;
        let branch_name = String::from_utf8_lossy(&sym.stdout).trim().to_owned();
        let detached = !sym.success || branch_name.is_empty();

        // HEAD ids:unborn 分支时 rev-parse 失败,容忍非零退出。
        let head = self
            .execute(&top, "rev-parse", |args| args.arg("rev-parse").arg("HEAD"))
            .await?;
        let head_full = if head.success {
            Some(String::from_utf8_lossy(&head.stdout).trim().to_owned())
        } else {
            None
        };
        let head_short = if head.success {
            let short = self
                .execute(&top, "rev-parse", |args| {
                    args.arg("rev-parse").arg("--short").arg("HEAD")
                })
                .await?;
            Some(String::from_utf8_lossy(&short.stdout).trim().to_owned())
        } else {
            None
        };

        // status:porcelain v1 + NUL 分隔(路径原样,无 quoting)。
        let status = self
            .execute(&top, "status", |args| {
                args.arg("status")
                    .arg("--porcelain=v1")
                    .arg("-z")
                    .arg("--untracked-files=normal")
            })
            .await?;
        if !status.success {
            return Err(status.failed("status"));
        }
        if status.stdout_truncated {
            return Err(GitError::OutputTooLarge {
                operation: "status",
                limit: self.max_output,
            });
        }
        let entries = parse_status_z(&status.stdout)?;

        // numstat:相对 HEAD 的总增删行;unborn HEAD 时跳过。
        let mut summary = GitSummary {
            detached_head: detached,
            branch: if detached { None } else { Some(branch_name) },
            head_short,
            head_full,
            root,
            ..GitSummary::default()
        };
        for e in entries {
            let (x, y) = (e.index_status, e.worktree_status);
            if x == '?' {
                summary.untracked.push(e.clone());
            }
            if x != ' ' && x != '?' {
                summary.staged.push(e.clone());
            }
            if y != ' ' && y != '?' {
                summary.unstaged.push(e.clone());
            }
            if x == 'A' {
                summary.added.push(e.clone());
            }
            if x == 'M' || y == 'M' {
                summary.modified.push(e.clone());
            }
            if x == 'D' || y == 'D' {
                summary.deleted.push(e.clone());
            }
            if x == 'R' || x == 'C' {
                summary.renamed.push(e.clone());
            }
        }

        if summary.head_full.is_some() {
            let numstat = self
                .execute(&top, "diff", |args| {
                    args.arg("diff")
                        .arg("--numstat")
                        .arg("--no-ext-diff")
                        .arg("--no-color")
                        .arg("--no-textconv")
                        .arg("HEAD")
                })
                .await?;
            if !numstat.success {
                return Err(numstat.failed("diff"));
            }
            if numstat.stdout_truncated {
                return Err(GitError::OutputTooLarge {
                    operation: "diff",
                    limit: self.max_output,
                });
            }
            parse_numstat(
                &numstat.stdout,
                &mut summary.total_added_lines,
                &mut summary.total_deleted_lines,
                &mut summary.binary_files,
            );
            summary.numstat_available = true;
        }
        Ok(summary)
    }

    /// 读取单文件 staged/unstaged diff(§23.1)。超限返回截断标记,
    /// 上层应转而提供完整 diff 下载。
    pub async fn read_file_diff(
        &self,
        authorized_roots: &[PathBuf],
        cwd: &Path,
        relative_path: &str,
        staged: bool,
    ) -> Result<GitFileDiff, GitError> {
        let (_, top) = self.authorize_worktree(authorized_roots, cwd).await?;
        validate_relative_path(relative_path)?;

        // diff 输出上限:读 max_output+1 以判定截断。
        let op: &'static str = if staged { "diff-cached" } else { "diff" };
        let out = self
            .execute_capped(&top, op, self.max_output + 1, |args| {
                let a = args.arg("diff");
                let a = if staged { a.arg("--cached") } else { a };
                a.arg("--no-ext-diff")
                    .arg("--no-color")
                    .arg("--no-textconv")
                    .arg("--")
                    .arg(relative_path)
            })
            .await?;
        if !out.success && !out.stdout_truncated {
            return Err(out.failed(op));
        }
        let mut content = out.stdout;
        let truncated = out.stdout_truncated || content.len() > self.max_output;
        content.truncate(self.max_output);
        Ok(GitFileDiff {
            relative_path: relative_path.to_owned(),
            staged,
            truncated,
            content,
        })
    }

    // -----------------------------------------------------------------
    // 内部
    // -----------------------------------------------------------------

    /// 校验 cwd 在授权根内,且其所属 worktree 根也在同一授权根内。
    /// 返回(授权根, worktree 根);worktree 根后续作为命令的 `-C` 目录,
    /// 保证相对路径计算与根一致。
    async fn authorize_worktree(
        &self,
        authorized_roots: &[PathBuf],
        cwd: &Path,
    ) -> Result<(PathBuf, PathBuf), GitError> {
        let cwd_c = std::fs::canonicalize(cwd).map_err(|_| GitError::OutsideScope)?;
        let mut root: Option<PathBuf> = None;
        for r in authorized_roots {
            if let Ok(rc) = std::fs::canonicalize(r) {
                if cwd_c.starts_with(&rc) {
                    root = Some(rc);
                    break;
                }
            }
        }
        let root = root.ok_or(GitError::OutsideScope)?;

        let probe = self
            .execute(&cwd_c, "rev-parse", |args| {
                args.arg("rev-parse")
                    .arg("--is-inside-work-tree")
                    .arg("--show-toplevel")
            })
            .await?;
        if !probe.success {
            // 不是 git 仓库 / 是 bare 仓库:按越权处理(§23.1 只服务 worktree)。
            return Err(GitError::OutsideScope);
        }
        let text = String::from_utf8_lossy(&probe.stdout);
        let mut lines = text.lines();
        let inside = lines.next().unwrap_or("").trim();
        let top_raw = lines.next().unwrap_or("").trim();
        if inside != "true" || top_raw.is_empty() {
            return Err(GitError::OutsideScope);
        }
        let top = std::fs::canonicalize(top_raw).map_err(|_| GitError::OutsideScope)?;
        if !top.starts_with(&root) {
            // worktree 根逃出授权根:整个仓库未授权。
            return Err(GitError::OutsideScope);
        }
        Ok((root, top))
    }

    /// 基础命令构造:固定 executable + `-C <dir>` + 只读/无 pager 全局参数。
    fn base_command(&self, dir: &Path) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.git_exe);
        cmd.arg("-C")
            .arg(dir)
            .arg("--no-optional-locks")
            .arg("--no-pager")
            .arg("-c")
            .arg("core.pager=cat")
            .stdin(Stdio::null());
        cmd
    }

    /// 执行一条 git 命令(默认输出上限)。子命令由 `build` 追加。
    async fn execute(
        &self,
        dir: &Path,
        operation: &'static str,
        build: impl FnOnce(&mut tokio::process::Command) -> &mut tokio::process::Command,
    ) -> Result<CmdOutput, GitError> {
        self.execute_capped(dir, operation, self.max_output, build)
            .await
    }

    /// 执行 git 命令,stdout 流式读取并在 `cap` 处截断(带截断标记),
    /// 超时则 kill 进程。日志只含 operation/字节数/耗时(§25.3)。
    async fn execute_capped(
        &self,
        dir: &Path,
        operation: &'static str,
        cap: usize,
        build: impl FnOnce(&mut tokio::process::Command) -> &mut tokio::process::Command,
    ) -> Result<CmdOutput, GitError> {
        let mut cmd = self.base_command(dir);
        build(&mut cmd);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let started = Instant::now();

        let mut child = cmd.spawn().map_err(GitError::Spawn)?;
        let out_task = spawn_pipe_reader(child.stdout.take(), cap);
        let err_task = spawn_pipe_reader(child.stderr.take(), MAX_STDERR_BYTES);

        let wait = child.wait();
        let status = match tokio::time::timeout(self.timeout, wait).await {
            Ok(s) => s.map_err(GitError::Spawn)?,
            Err(_) => {
                // 超时:kill 并回收,防止僵尸进程(测试断言进程已终止)。
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(GitError::Timeout {
                    operation,
                    timeout_secs: self.timeout.as_secs(),
                });
            }
        };
        let (stdout, stdout_truncated) = out_task
            .await
            .map_err(|_| GitError::Spawn(std::io::Error::other("git stdout reader panicked")))??;
        let (stderr, _) = err_task
            .await
            .map_err(|_| GitError::Spawn(std::io::Error::other("git stderr reader panicked")))??;

        tracing::debug!(
            operation,
            bytes = stdout.len(),
            duration_ms = started.elapsed().as_millis() as u64,
            "git command finished"
        );

        Ok(CmdOutput {
            success: status.success(),
            code: status.code(),
            stdout,
            stderr,
            stdout_truncated,
        })
    }
}

/// 流式读取一条管道到上限;返回(内容, 是否截断)。超限即停读(管道被
/// drop 后 git 收 SIGPIPE 退出),避免无界内存。
fn spawn_pipe_reader(
    pipe: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>,
    cap: usize,
) -> tokio::task::JoinHandle<Result<(Vec<u8>, bool), GitError>> {
    tokio::spawn(async move {
        let mut pipe =
            pipe.ok_or_else(|| GitError::Spawn(std::io::Error::other("git pipe not captured")))?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut pipe, &mut chunk)
                .await
                .map_err(GitError::Spawn)?;
            if n == 0 {
                return Ok((buf, false));
            }
            if buf.len() + n > cap {
                buf.extend_from_slice(&chunk[..cap.saturating_sub(buf.len())]);
                return Ok((buf, true));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    })
}

// ---------------------------------------------------------------------------
// 解析
// ---------------------------------------------------------------------------

/// 解析 `git status --porcelain=v1 -z`:
/// 每条目为 `XY <newpath>\0`,rename/copy 追加 `<oldpath>\0`(实测 git 2.x:
/// 第一个 NUL 字段是新路径,第二个是旧路径)。
fn parse_status_z(bytes: &[u8]) -> Result<Vec<FileEntry>, GitError> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 3 <= bytes.len() {
        let x = bytes[pos] as char;
        let y = bytes[pos + 1] as char;
        if bytes[pos + 2] != b' ' {
            return Err(GitError::GitFailed {
                operation: "status",
                code: None,
                stderr_brief: "malformed porcelain status entry".into(),
            });
        }
        pos += 3;
        let (path, next) = read_nul_field(bytes, pos)?;
        pos = next;
        let orig_path = if x == 'R' || x == 'C' {
            let (old, next) = read_nul_field(bytes, pos)?;
            pos = next;
            Some(old)
        } else {
            None
        };
        entries.push(FileEntry {
            path,
            orig_path,
            index_status: x,
            worktree_status: y,
        });
    }
    Ok(entries)
}

fn read_nul_field(bytes: &[u8], start: usize) -> Result<(String, usize), GitError> {
    let rel = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| GitError::GitFailed {
            operation: "status",
            code: None,
            stderr_brief: "unterminated porcelain path".into(),
        })?;
    let field = String::from_utf8_lossy(&bytes[start..start + rel]).into_owned();
    Ok((field, start + rel + 1))
}

/// 解析 `git diff --numstat HEAD`(非 -z,默认 quoting 保证按行切分安全):
/// `added\tdeleted\tpath`,binary 行为 `-\t-\tpath`。只取前两列汇总。
fn parse_numstat(bytes: &[u8], added: &mut u64, deleted: &mut u64, binary: &mut Vec<String>) {
    for line in String::from_utf8_lossy(bytes).lines() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(3, '\t');
        let (a, d, path) = match (parts.next(), parts.next(), parts.next()) {
            (Some(a), Some(d), Some(p)) => (a, d, p),
            _ => continue,
        };
        if a == "-" || d == "-" {
            binary.push(path.to_owned());
            continue;
        }
        if let Ok(n) = a.parse::<u64>() {
            *added += n;
        }
        if let Ok(n) = d.parse::<u64>() {
            *deleted += n;
        }
    }
}

/// 词法校验相对路径:仅允许普通组件;拒绝绝对路径、`..`、`.`
/// 与 pathspec magic(如 `:/`、`:(top)`)。diff 的 pathspec 以独立 argv
/// 直传,该校验保证不会指向 worktree 之外。
fn validate_relative_path(rel: &str) -> Result<(), GitError> {
    let p = Path::new(rel);
    if rel.is_empty() || p.is_absolute() || rel.starts_with(':') {
        return Err(GitError::InvalidPath);
    }
    for comp in p.components() {
        if !matches!(comp, std::path::Component::Normal(_)) {
            return Err(GitError::InvalidPath);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_path_validation() {
        assert!(validate_relative_path("a/b.txt").is_ok());
        assert!(validate_relative_path("a.txt").is_ok());
        assert!(validate_relative_path("").is_err());
        assert!(validate_relative_path("/etc/passwd").is_err());
        assert!(validate_relative_path("../outside").is_err());
        assert!(validate_relative_path("a/../../b").is_err());
        assert!(validate_relative_path("./a").is_err());
        assert!(validate_relative_path(":/magic").is_err());
        assert!(validate_relative_path(":(top)a").is_err());
    }

    #[test]
    fn parse_status_z_rename_order() {
        // 实测格式:R <new>\0<old>\0
        let raw = b"R  b.txt\0a.txt\0 M c.txt\0?? d.txt\0";
        let entries = parse_status_z(raw).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, "b.txt");
        assert_eq!(entries[0].orig_path.as_deref(), Some("a.txt"));
        assert_eq!(entries[0].index_status, 'R');
        assert_eq!(entries[1].path, "c.txt");
        assert_eq!(entries[1].worktree_status, 'M');
        assert_eq!(entries[2].index_status, '?');
    }

    #[test]
    fn parse_numstat_binary_and_totals() {
        let raw = b"12\t3\ta.txt\n-\t-\timg.bin\n0\t7\told => {old => new}.txt\n";
        let (mut a, mut d) = (0u64, 0u64);
        let mut bin = Vec::new();
        parse_numstat(raw, &mut a, &mut d, &mut bin);
        assert_eq!((a, d), (12, 10));
        assert_eq!(bin, vec!["img.bin"]);
    }

    #[test]
    fn stable_codes_match_spec() {
        assert_eq!(GitError::OutsideScope.stable_code(), "FILE_OUTSIDE_SCOPE");
        assert_eq!(GitError::InvalidPath.stable_code(), "FILE_OUTSIDE_SCOPE");
        assert_eq!(
            GitError::OutputTooLarge {
                operation: "diff",
                limit: 1
            }
            .stable_code(),
            "DIFF_TOO_LARGE"
        );
        assert_eq!(
            GitError::Timeout {
                operation: "status",
                timeout_secs: 1
            }
            .stable_code(),
            "INTERNAL_ERROR"
        );
    }
}
