//! power 集成测试:argv 固定 + 生命周期(注入 fake 可执行文件,不真跑 caffeinate)。

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use bridge::power::{caffeinate_args, should_hold, PowerSource, WakePolicy};

/// 生成 fake "caffeinate":把收到的 argv 逐行写入文件;stay=true 时驻留
/// (exec sleep 60,并先写自身 pid),否则立即退出。
fn write_fake_caffeinate(
    dir: &std::path::Path,
    argv_out: &std::path::Path,
    pid_out: Option<&std::path::Path>,
    stay: bool,
) -> PathBuf {
    let mut script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
        argv_out.display()
    );
    if let Some(pid_out) = pid_out {
        script.push_str(&format!("echo $$ > '{}'\n", pid_out.display()));
    }
    if stay {
        script.push_str("exec sleep 60\n");
    } else {
        script.push_str("exit 0\n");
    }
    // 先完整写入并关闭临时路径，再原子发布为可执行文件。Linux CI 的
    // overlay 文件系统可能在“直接写最终路径后立刻 exec”时返回 ETXTBSY。
    let path = dir.join("fake-caffeinate");
    let pending = dir.join("fake-caffeinate.pending");
    std::fs::write(&pending, script).unwrap();
    std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&pending, &path).unwrap();
    path
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

#[test]
fn argv_is_fixed_per_power_source() {
    // man caffeinate:-s 仅 AC 有效;-i 防系统空闲睡眠;都不影响显示器。
    assert_eq!(caffeinate_args(PowerSource::Ac), &["-i", "-s"]);
    assert_eq!(caffeinate_args(PowerSource::Battery), &["-i"]);
}

#[test]
fn hold_decision_follows_spec() {
    // §19:接通恒持有;电池仅 turn 活跃或 pending attention 时持有。
    assert!(should_hold(PowerSource::Ac, false, false));
    assert!(should_hold(PowerSource::Ac, true, true));
    assert!(!should_hold(PowerSource::Battery, false, false));
    assert!(should_hold(PowerSource::Battery, true, false));
    assert!(should_hold(PowerSource::Battery, false, true));
}

#[tokio::test]
async fn lifecycle_acquires_releases_and_rewrites_argv_on_mode_change() {
    let dir = tempfile::tempdir().unwrap();
    let argv_out = dir.path().join("argv.txt");
    let fake = write_fake_caffeinate(dir.path(), &argv_out, None, true);
    let policy = WakePolicy::with_program(fake);

    assert!(!policy.assertion_held());

    // 接通电源 → 持有,argv = -i -s。
    policy.update(PowerSource::Ac, false, false).await.unwrap();
    assert!(policy.assertion_held());
    assert!(wait_until(|| argv_out.exists(), Duration::from_secs(10)).await);
    let argv = std::fs::read_to_string(&argv_out).unwrap();
    assert_eq!(argv.lines().collect::<Vec<_>>(), vec!["-i", "-s"]);

    // 电池 + 无活动 → 释放。
    policy
        .update(PowerSource::Battery, false, false)
        .await
        .unwrap();
    assert!(!policy.assertion_held());
    assert!(wait_until(|| !policy.assertion_held(), Duration::from_secs(10)).await);

    // 电池 + turn 活跃 → 重新持有,argv 因 -s 仅 AC 有效而变为 -i。
    policy
        .update(PowerSource::Battery, true, false)
        .await
        .unwrap();
    assert!(policy.assertion_held());
    assert!(
        wait_until(
            || std::fs::read_to_string(&argv_out).is_ok_and(|s| s.lines().eq(["-i"].into_iter())),
            Duration::from_secs(10)
        )
        .await
    );

    // release:进程被 kill + reap。
    policy.release().await;
    assert!(!policy.assertion_held());
}

#[tokio::test]
async fn spontaneous_exit_clears_handle() {
    let dir = tempfile::tempdir().unwrap();
    let argv_out = dir.path().join("argv.txt");
    let fake = write_fake_caffeinate(dir.path(), &argv_out, None, false); // 立即退出
    let policy = WakePolicy::with_program(fake);

    policy.update(PowerSource::Ac, false, false).await.unwrap();
    assert!(policy.assertion_held(), "spawn 后应短暂视为持有");

    // 进程自发退出后,supervisor 清理句柄 → assertion_held 与真实状态一致。
    assert!(
        wait_until(|| !policy.assertion_held(), Duration::from_secs(3)).await,
        "进程退出后必须清理句柄"
    );
}

/// 进程存活检查:/bin/kill -0 pid,0 = 存活。
fn process_alive(pid: &str) -> bool {
    std::process::Command::new("/bin/kill")
        .args(["-0", pid])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn drop_kills_child_process() {
    let dir = tempfile::tempdir().unwrap();
    let argv_out = dir.path().join("argv.txt");
    let pid_out = dir.path().join("pid.txt");
    let fake = write_fake_caffeinate(dir.path(), &argv_out, Some(&pid_out), true);
    let policy = WakePolicy::with_program(fake);
    policy.update(PowerSource::Ac, false, false).await.unwrap();
    assert!(policy.assertion_held());
    assert!(wait_until(|| pid_out.exists(), Duration::from_secs(10)).await);
    let pid = std::fs::read_to_string(&pid_out).unwrap().trim().to_owned();
    assert!(process_alive(&pid), "fake caffeinate 应已驻留");

    // Drop 触发 supervisor kill + reap:驻留进程必须消失(§19:句柄可靠释放)。
    drop(policy);
    assert!(
        wait_until(|| !process_alive(&pid), Duration::from_secs(3)).await,
        "Drop 后子进程未被清理"
    );
}
