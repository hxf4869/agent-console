//! file_handle 签发与复验安全测试(§22.2、§29.1:symlink escape、TOCTOU、
//! 过期/撤销、并发上限、日志白名单)。全部使用 tempfile + 合成数据,
//! 绝对路径为临时目录,规避真实用户路径。

use std::fs;
use std::os::unix::fs::symlink;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bridge::files::{
    FileGrantManager, GrantAction, GrantActions, GrantSource, DEFAULT_GRANT_TTL,
    MAX_ACTIVE_TRANSFERS_PER_DEVICE,
};

fn write(path: &std::path::Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

fn issue_default(
    mgr: &FileGrantManager,
    root: &std::path::Path,
    rel: &str,
) -> bridge::files::FileHandle {
    mgr.issue(
        "device-1",
        "session-1",
        root,
        std::path::Path::new(rel),
        GrantActions::PREVIEW | GrantActions::DOWNLOAD,
        GrantSource::UserOpened,
        DEFAULT_GRANT_TTL,
    )
    .unwrap()
}

fn resolve(mgr: &FileGrantManager, h: &bridge::files::FileHandle) -> bridge::files::VerifiedFile {
    mgr.resolve_for_read("device-1", "session-1", &h.token, GrantAction::Download)
        .unwrap()
}

fn resolve_err(mgr: &FileGrantManager, h: &bridge::files::FileHandle) -> String {
    mgr.resolve_for_read("device-1", "session-1", &h.token, GrantAction::Download)
        .unwrap_err()
        .stable_code()
        .to_owned()
}

// ---------------------------------------------------------------------------
// 基本签发/复验
// ---------------------------------------------------------------------------

#[tokio::test]
async fn issue_and_resolve_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("docs/report.txt"), "REPORT-CONTENT");

    let mgr = FileGrantManager::new();
    let h = issue_default(&mgr, tmp.path(), "docs/report.txt");

    assert_eq!(h.size, "REPORT-CONTENT".len() as u64);
    assert!(
        h.token.len() >= 32,
        "token should be high-entropy base64url"
    );

    let mut vf = resolve(&mgr, &h);
    let mut buf = Vec::new();
    use std::io::Read;
    vf.file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"REPORT-CONTENT");
}

#[tokio::test]
async fn binding_and_action_must_match() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("f.txt"), "x");
    let mgr = FileGrantManager::new();
    let h = issue_default(&mgr, tmp.path(), "f.txt");

    // 错误 device / 错误 session
    assert_eq!(
        mgr.resolve_for_read("device-2", "session-1", &h.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "FILE_HANDLE_INVALID"
    );
    assert_eq!(
        mgr.resolve_for_read("device-1", "session-9", &h.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "FILE_HANDLE_INVALID"
    );
    // 未授权动作:该 handle 只签发了 download
    let dl_only = mgr
        .issue(
            "device-1",
            "session-1",
            tmp.path(),
            std::path::Path::new("f.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap();
    assert_eq!(
        mgr.resolve_for_read(
            "device-1",
            "session-1",
            &dl_only.token,
            GrantAction::Preview
        )
        .unwrap_err()
        .stable_code(),
        "FILE_HANDLE_INVALID"
    );
    // 未知 token
    assert_eq!(
        mgr.resolve_for_read(
            "device-1",
            "session-1",
            "does-not-exist",
            GrantAction::Download
        )
        .unwrap_err()
        .stable_code(),
        "FILE_HANDLE_INVALID"
    );
}

#[tokio::test]
async fn revoke_and_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("f.txt"), "x");
    let mgr = FileGrantManager::new();

    let h = issue_default(&mgr, tmp.path(), "f.txt");
    assert!(mgr.revoke(&h.token));
    assert_eq!(
        mgr.resolve_for_read("device-1", "session-1", &h.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "FILE_HANDLE_INVALID"
    );

    // TTL 过期 → TRANSFER_EXPIRED,且 sweep 会移除
    let h2 = mgr
        .issue(
            "device-1",
            "session-1",
            tmp.path(),
            std::path::Path::new("f.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::SessionReference {
                evidence: "tool:diff a.txt".into(),
            },
            Duration::from_millis(50),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(
        mgr.resolve_for_read("device-1", "session-1", &h2.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "TRANSFER_EXPIRED"
    );
    // 未被访问过的过期 handle 由 sweep 收集
    let h3 = mgr
        .issue(
            "device-1",
            "session-1",
            tmp.path(),
            std::path::Path::new("f.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::UserOpened,
            Duration::from_millis(50),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(mgr.sweep_expired(), 1);
    assert_eq!(
        mgr.resolve_for_read("device-1", "session-1", &h3.token, GrantAction::Download)
            .unwrap_err()
            .stable_code(),
        "FILE_HANDLE_INVALID"
    );
    // SessionReference 必须携带非空证据
    assert!(mgr
        .issue(
            "d",
            "s",
            tmp.path(),
            std::path::Path::new("f.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::SessionReference {
                evidence: "  ".into()
            },
            DEFAULT_GRANT_TTL
        )
        .is_err());
}

// ---------------------------------------------------------------------------
// symlink escape(§29.1 硬性要求)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn symlink_escape_rejected_at_issue() {
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(&outside.path().join("secret.txt"), "OUTSIDE");

    // 根内 symlink 指向根外
    symlink(
        outside.path().join("secret.txt"),
        tmp.path().join("link.txt"),
    )
    .unwrap();

    let mgr = FileGrantManager::new();
    let err = mgr
        .issue(
            "d",
            "s",
            tmp.path(),
            std::path::Path::new("link.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap_err();
    assert_eq!(err.stable_code(), "FILE_OUTSIDE_SCOPE");
}

#[tokio::test]
async fn parent_symlink_escape_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir_all(outside.path().join("dir")).unwrap();
    write(&outside.path().join("dir/f.txt"), "OUTSIDE");
    // 根内目录本身是 symlink → 全链 canonical 后逃出根
    symlink(outside.path().join("dir"), tmp.path().join("dir")).unwrap();

    let mgr = FileGrantManager::new();
    let err = mgr
        .issue(
            "d",
            "s",
            tmp.path(),
            std::path::Path::new("dir/f.txt"),
            GrantActions::DOWNLOAD,
            GrantSource::UserOpened,
            DEFAULT_GRANT_TTL,
        )
        .unwrap_err();
    assert_eq!(err.stable_code(), "FILE_OUTSIDE_SCOPE");
}

#[tokio::test]
async fn symlink_swap_after_issue_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    write(&outside.path().join("secret.txt"), "OUTSIDE");
    write(&tmp.path().join("f.txt"), "INSIDE");

    let mgr = FileGrantManager::new();
    let h = issue_default(&mgr, tmp.path(), "f.txt");

    // 签发后把文件替换成指向根外的 symlink
    fs::remove_file(tmp.path().join("f.txt")).unwrap();
    symlink(outside.path().join("secret.txt"), tmp.path().join("f.txt")).unwrap();

    assert_eq!(resolve_err(&mgr, &h), "FILE_OUTSIDE_SCOPE");
}

#[tokio::test]
async fn symlink_within_root_is_allowed() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("real.txt"), "REAL");
    symlink("real.txt", tmp.path().join("alias.txt")).unwrap();

    let mgr = FileGrantManager::new();
    // 通过 symlink 签发:canonical 后仍在根内,允许。
    let h = issue_default(&mgr, tmp.path(), "alias.txt");
    let mut vf = resolve(&mgr, &h);
    let mut buf = String::new();
    use std::io::Read;
    vf.file.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "REAL");
}

// ---------------------------------------------------------------------------
// TOCTOU(§29.1 硬性要求)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_path_new_inode_detected_as_changed() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("f.txt"), "VERSION-1");

    let mgr = FileGrantManager::new();
    let h = issue_default(&mgr, tmp.path(), "f.txt");

    // 同路径替换文件(新 inode + 新 size/mtime)
    fs::remove_file(tmp.path().join("f.txt")).unwrap();
    write(&tmp.path().join("f.txt"), "VERSION-2-longer");

    assert_eq!(resolve_err(&mgr, &h), "FILE_CHANGED");

    // 同 inode 内容变化(size/mtime 变)同样拒绝
    let h2 = issue_default(&mgr, tmp.path(), "f.txt");
    let meta_before = fs::metadata(tmp.path().join("f.txt")).unwrap().len();
    write(&tmp.path().join("f.txt"), "VERSION-3-appended-content");
    let _ = meta_before;
    assert_eq!(resolve_err(&mgr, &h2), "FILE_CHANGED");
}

#[tokio::test]
async fn deleted_target_is_invalid() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("f.txt"), "x");
    let mgr = FileGrantManager::new();
    let h = issue_default(&mgr, tmp.path(), "f.txt");
    fs::remove_file(tmp.path().join("f.txt")).unwrap();
    assert_eq!(resolve_err(&mgr, &h), "FILE_HANDLE_INVALID");
}

// ---------------------------------------------------------------------------
// 并发 transfer 上限(§22.6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transfer_slot_limit_per_device() {
    let mgr = FileGrantManager::new();
    let s1 = mgr.acquire_transfer_slot("device-1").unwrap();
    let s2 = mgr.acquire_transfer_slot("device-1").unwrap();
    let err = mgr.acquire_transfer_slot("device-1").unwrap_err();
    assert_eq!(err.stable_code(), "RATE_LIMITED");
    // 其他设备不受影响
    let _other = mgr.acquire_transfer_slot("device-2").unwrap();

    drop(s1);
    let s3 = mgr.acquire_transfer_slot("device-1").unwrap();
    drop(s2);
    drop(s3);
    let _s4 = mgr.acquire_transfer_slot("device-1").unwrap();
    let _s5 = mgr.acquire_transfer_slot("device-1").unwrap();
    assert_eq!(MAX_ACTIVE_TRANSFERS_PER_DEVICE, 2);
}

// ---------------------------------------------------------------------------
// 日志白名单(§25.3):不落文件内容与绝对路径
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn logs_do_not_contain_paths_or_content() {
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("f.txt"), "SECRET-CONTENT-MARKER");

    let sink = SharedBuf::default();
    let buf = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();

    let mgr = FileGrantManager::new();
    tracing::subscriber::with_default(subscriber, || {
        let h = issue_default(&mgr, tmp.path(), "f.txt");
        // 拒绝路径(含 debug 日志)与成功路径都要在捕获范围内。
        let _ = mgr.resolve_for_read("wrong-device", "session-1", &h.token, GrantAction::Download);
        let _ = resolve(&mgr, &h);
    });

    let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        !logs.contains("SECRET-CONTENT-MARKER"),
        "content leaked into logs: {logs}"
    );
    assert!(
        !logs.contains(&tmp.path().to_string_lossy().to_string()),
        "abs path leaked: {logs}"
    );
    assert!(!logs.contains("f.txt"), "file name leaked: {logs}");
}
