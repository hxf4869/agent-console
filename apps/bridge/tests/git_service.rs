//! Git 只读能力集成测试(§23.1、§29.1:固定 argv、超时、detached HEAD、
//! binary、大 Diff、授权根)。测试仓库全部使用无害合成数据。

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use bridge::git::{GitError, GitService};

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.invalid")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.invalid")
        .output()
        .expect("git available");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// 建一个最小仓库(空提交,便于统一测试)。
fn init_repo(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    git(path, &["init", "-q", "-b", "main"]);
}

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn head(path: &Path) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

async fn wait_until(pred: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while !pred() {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// 生成 fake git:先记录 argv,再 exec 真 git(用于固定 argv 断言)。
fn recording_git(dir: &Path, argv_out: &Path) -> PathBuf {
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nexec /usr/bin/git \"$@\"\n",
        argv_out.display()
    );
    let path = dir.join("fake-git");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// 生成只会挂住的 fake git(超时测试),并写出自身 pid。
/// 首参不是 `-C` 时立即退出(供测试预热 freshly-written 可执行文件,
/// 避免并行负载下首次启动慢于超时);`-C` 开头则写 pid 后挂起。
fn sleeping_git(dir: &Path, pid_out: &Path) -> PathBuf {
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" != \"-C\" ]; then exit 0; fi\necho $$ > '{}'\nexec sleep 60\n",
        pid_out.display()
    );
    let path = dir.join("sleepy-git");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// summary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn summary_reports_branch_counts_and_binary() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("base.txt"), "l1\nl2\nl3\n");
    write(&repo.join("old.txt"), "same\n");
    write(&repo.join("gone.txt"), "d1\nd2\n");
    write(&repo.join("bin.dat"), "raw\x00\x01base\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    // staged:新文件 added.txt(+3 行)、base.txt 加一行;再补一次未暂存修改。
    write(&repo.join("added.txt"), "a1\na2\na3\n");
    write(&repo.join("base.txt"), "l1\nl2\nl3\nl4\n");
    git(&repo, &["add", "added.txt", "base.txt"]);
    write(&repo.join("base.txt"), "l1\nl2\nl3\nl4\nl5\n");
    // 未暂存删除
    std::fs::remove_file(repo.join("gone.txt")).unwrap();
    // staged rename
    git(&repo, &["mv", "old.txt", "new.txt"]);
    // staged binary 修改
    std::fs::write(repo.join("bin.dat"), b"raw\x00\x02changed\x00\n").unwrap();
    // untracked
    write(&repo.join("untracked.txt"), "u\n");

    let svc = GitService::new().unwrap();
    let s = svc.read_summary(&[tmp.path().into()], &repo).await.unwrap();

    assert!(!s.detached_head);
    assert_eq!(s.branch.as_deref(), Some("main"));
    assert_eq!(s.head_full.as_deref(), Some(head(&repo).as_str()));
    assert!(s
        .head_short
        .as_deref()
        .is_some_and(|h| head(&repo).starts_with(h)));

    // staged:A added.txt、M base.txt、R old->new、M bin.dat
    let staged_paths: Vec<&str> = s.staged.iter().map(|e| e.path.as_str()).collect();
    assert!(staged_paths.contains(&"added.txt"));
    assert!(staged_paths.contains(&"base.txt"));
    assert!(staged_paths.contains(&"new.txt"));
    // unstaged:M base.txt(工作区再改)、D gone.txt
    let unstaged_paths: Vec<&str> = s.unstaged.iter().map(|e| e.path.as_str()).collect();
    assert!(unstaged_paths.contains(&"base.txt"));
    assert!(unstaged_paths.contains(&"gone.txt"));
    // untracked
    assert_eq!(s.untracked.len(), 1);
    assert_eq!(s.untracked[0].path, "untracked.txt");
    // added / modified / deleted / renamed
    assert!(s.added.iter().any(|e| e.path == "added.txt"));
    assert!(s.modified.iter().any(|e| e.path == "base.txt"));
    assert!(s.deleted.iter().any(|e| e.path == "gone.txt"));
    let renamed = &s.renamed;
    assert_eq!(renamed.len(), 1);
    assert_eq!(renamed[0].path, "new.txt");
    assert_eq!(renamed[0].orig_path.as_deref(), Some("old.txt"));

    // numstat:base.txt +2(相对 HEAD)、added.txt +3、gone.txt -2;binary 不计行
    assert_eq!(s.total_added_lines, 5);
    assert_eq!(s.total_deleted_lines, 2);
    assert!(s.binary_files.iter().any(|p| p == "bin.dat"));
    assert!(s.numstat_available);
    // Browser 只应拿到相对路径;root 仅内部识别用。
    assert!(s.entries_all_relative());
}

#[tokio::test]
async fn detached_head_is_flagged() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("f.txt"), "1\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    git(&repo, &["checkout", "-q", "--detach", "HEAD"]);

    let svc = GitService::new().unwrap();
    let s = svc.read_summary(&[tmp.path().into()], &repo).await.unwrap();
    assert!(s.detached_head);
    assert!(s.branch.is_none());
    assert_eq!(s.head_full.as_deref(), Some(head(&repo).as_str()));
}

#[tokio::test]
async fn unborn_repo_has_no_head_and_no_numstat() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("fresh");
    init_repo(&repo);

    let svc = GitService::new().unwrap();
    let s = svc.read_summary(&[tmp.path().into()], &repo).await.unwrap();
    assert!(!s.detached_head);
    assert_eq!(s.branch.as_deref(), Some("main"));
    assert!(s.head_full.is_none() && s.head_short.is_none());
    assert!(!s.numstat_available);
    assert_eq!((s.total_added_lines, s.total_deleted_lines), (0, 0));
}

// ---------------------------------------------------------------------------
// 单文件 diff
// ---------------------------------------------------------------------------

#[tokio::test]
async fn file_diff_staged_and_unstaged() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("a.txt"), "one\ntwo\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    // staged 修改
    write(&repo.join("a.txt"), "one\nTWO staged\n");
    git(&repo, &["add", "a.txt"]);
    // unstaged 修改
    write(&repo.join("a.txt"), "one\nTWO staged\nthree unstaged\n");

    let svc = GitService::new().unwrap();
    let staged = svc
        .read_file_diff(&[tmp.path().into()], &repo, "a.txt", true)
        .await
        .unwrap();
    assert!(!staged.truncated);
    let text = String::from_utf8(staged.content).unwrap();
    assert!(text.contains("+TWO staged"));
    assert!(!text.contains("three unstaged"));

    let unstaged = svc
        .read_file_diff(&[tmp.path().into()], &repo, "a.txt", false)
        .await
        .unwrap();
    let text = String::from_utf8(unstaged.content).unwrap();
    assert!(text.contains("+three unstaged"));
    assert!(!text.contains("+TWO staged"));
}

#[tokio::test]
async fn file_diff_truncated_at_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    let old: String = (0..2000).map(|i| format!("old line {i}\n")).collect();
    let new: String = (0..2000).map(|i| format!("new line {i}\n")).collect();
    write(&repo.join("big.txt"), &old);
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    write(&repo.join("big.txt"), &new);

    let exe = bridge::git::resolve_git_from_path().unwrap();
    let svc = GitService::with_limits(exe, Duration::from_secs(5), 500);
    let d = svc
        .read_file_diff(&[tmp.path().into()], &repo, "big.txt", false)
        .await
        .unwrap();
    assert!(d.truncated);
    assert_eq!(d.content.len(), 500);
}

#[tokio::test]
async fn file_diff_rejects_traversal_and_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("a.txt"), "x\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    let svc = GitService::new().unwrap();
    for bad in ["../escape.txt", "/etc/passwd", "a/../../b", ":/magic"] {
        let err = svc
            .read_file_diff(&[tmp.path().into()], &repo, bad, false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::InvalidPath),
            "want InvalidPath for {bad}, got {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 固定 argv / 超时 / spawn 失败
// ---------------------------------------------------------------------------

#[tokio::test]
async fn argv_is_fixed_without_shell() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("a.txt"), "x\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    let fake = recording_git(tmp.path(), &tmp.path().join("argv.txt"));
    let svc = GitService::with_git_exe(fake);
    svc.read_file_diff(&[tmp.path().into()], &repo, "a.txt", true)
        .await
        .unwrap();

    let argv = std::fs::read_to_string(tmp.path().join("argv.txt")).unwrap();
    let lines: Vec<&str> = argv.lines().collect();
    // 最后一次调用是 diff;前缀是全局只读参数,-C 指向 worktree 根。
    let idx = lines
        .iter()
        .position(|l| *l == "diff")
        .expect("diff in argv");
    assert!(lines[..idx].contains(&"--no-optional-locks"));
    assert!(lines[..idx].contains(&"--no-pager"));
    assert_eq!(lines[0], "-C");
    assert_eq!(lines[idx + 1], "--cached");
    assert!(lines[idx + 2..].contains(&"--no-ext-diff"));
    // pathspec 以独立 argv 直传(在 `--` 之后)。
    let dd = lines.iter().position(|l| *l == "--").unwrap();
    assert_eq!(lines[dd + 1], "a.txt");
}

#[tokio::test]
async fn timeout_kills_git_process() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    write(&repo.join("a.txt"), "x\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    let pid_file = tmp.path().join("pid.txt");
    let fake = sleeping_git(tmp.path(), &pid_file);
    // 预热:同步空跑一次,完成可执行文件的首次加载(`-C` 开头才挂起)。
    Command::new(&fake).arg("--version").output().unwrap();
    // 500ms:远短于脚本里 sleep 60,又给 /bin/sh 充足启动时间。
    let svc = GitService::with_limits(fake, Duration::from_millis(500), 1024);

    let err = svc
        .read_summary(&[tmp.path().into()], &repo)
        .await
        .unwrap_err();
    assert!(
        matches!(err, GitError::Timeout { .. }),
        "want Timeout, got {err:?}"
    );
    assert_eq!(err.stable_code(), "INTERNAL_ERROR");

    // fake git 启动后第一步就是写 pid 文件;若迟迟不出现,说明 kill 抢在了
    // 子进程首条指令之前(环境过载),此时无法断言 pid。
    assert!(
        wait_until(|| pid_file.exists(), Duration::from_secs(1)).await,
        "fake git never started; dir = {:?}",
        std::fs::read_dir(tmp.path()).map(|rd| rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect::<Vec<_>>())
    );
    let pid: u32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        wait_until(|| !pid_alive(pid), Duration::from_secs(3)).await,
        "git process {pid} should be killed after timeout"
    );
}

#[tokio::test]
async fn missing_git_exe_maps_to_internal_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    init_repo(&repo);
    let svc = GitService::with_git_exe(PathBuf::from("/nonexistent/git-binary"));
    let err = svc
        .read_summary(&[tmp.path().into()], &repo)
        .await
        .unwrap_err();
    assert!(matches!(err, GitError::Spawn(_)), "want Spawn, got {err:?}");
    assert_eq!(err.stable_code(), "INTERNAL_ERROR");
}

// ---------------------------------------------------------------------------
// 授权根
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cwd_outside_authorized_roots_rejected() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let repo = b.path().join("other");
    init_repo(&repo);
    write(&repo.join("a.txt"), "x\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);

    let svc = GitService::new().unwrap();
    let err = svc
        .read_summary(&[a.path().into()], &repo)
        .await
        .unwrap_err();
    assert!(matches!(err, GitError::OutsideScope));
    assert_eq!(err.stable_code(), "FILE_OUTSIDE_SCOPE");
}

#[tokio::test]
async fn non_git_dir_inside_root_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();

    let svc = GitService::new().unwrap();
    let err = svc
        .read_summary(&[tmp.path().into()], &plain)
        .await
        .unwrap_err();
    assert!(matches!(err, GitError::OutsideScope));
}

#[tokio::test]
async fn worktree_root_outside_authorized_root_rejected() {
    // repo 根在授权根之上:即使 cwd 在授权根内,worktree 整体未授权。
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    init_repo(&repo);
    write(&repo.join("a.txt"), "x\n");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    let subdir = repo.join("sub");
    std::fs::create_dir_all(&subdir).unwrap();

    let svc = GitService::new().unwrap();
    let err = svc
        .read_summary(&[subdir.clone()], &subdir)
        .await
        .unwrap_err();
    assert!(matches!(err, GitError::OutsideScope), "got {err:?}");
}
