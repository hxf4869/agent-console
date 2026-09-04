//! 电源唤醒断言策略(权威规格 §19 首版电源策略)。
//!
//! - 接通电源:持有"阻止系统睡眠、允许显示器休眠"断言。
//! - 电池供电:仅在 Codex turn 活跃或存在 pending attention 时持有;
//!   turn 结束且无 attention 后释放。
//! - 用 `/usr/bin/caffeinate` 固定 argv 直接 spawn,**不经 shell**;
//!   断言句柄随状态变化、进程退出与 Drop 可靠释放。
//!
//! argv 决策(man caffeinate,Darwin):
//! - `-i`  阻止系统**空闲**睡眠;不影响显示器(不传 `-d`,显示器可休眠)。
//! - `-s`  阻止系统睡眠,**仅在 AC 供电时有效** → 只在 [`PowerSource::Ac`]
//!   模式附加;电池下传 `-s` 无效,故电池模式只用 `-i`。
//! 组合:`Ac => ["-i", "-s"]`,`Battery => ["-i"]`。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

/// 系统 caffeinate 路径(fixed argv,不经 shell)。
pub const CAFFEINATE_PATH: &str = "/usr/bin/caffeinate";

// ---------------------------------------------------------------------------
// 纯策略(可单测,无副作用)
// ---------------------------------------------------------------------------

/// 电源来源。由上层运行时检测后传入(首版不做自动检测的集成点)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerSource {
    /// 接通电源。
    Ac,
    /// 电池供电。
    Battery,
}

/// 是否应持有系统唤醒断言(§19):
/// 接通电源恒持有;电池下仅 turn 活跃或存在 pending attention 时持有。
pub fn should_hold(source: PowerSource, turn_active: bool, attention_pending: bool) -> bool {
    match source {
        PowerSource::Ac => true,
        PowerSource::Battery => turn_active || attention_pending,
    }
}

/// caffeinate 固定 argv;语义依据见模块注释(man caffeinate)。
pub fn caffeinate_args(source: PowerSource) -> &'static [&'static str] {
    match source {
        // -s 仅 AC 有效:两者并用,系统在线 + 显示器可睡。
        PowerSource::Ac => &["-i", "-s"],
        // 电池:-s 无效,仅 -i 防空闲睡眠。
        PowerSource::Battery => &["-i"],
    }
}

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    #[error("spawn {0:?} failed: {1}")]
    Spawn(PathBuf, std::io::Error),
}

// ---------------------------------------------------------------------------
// WakePolicy
// ---------------------------------------------------------------------------

struct AssertionHandle {
    mode: PowerSource,
    /// 发送 oneshot ack 触发 supervisor 杀掉并回收 caffeinate 进程。
    kill: mpsc::Sender<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Default)]
struct Inner {
    /// 当前断言(None = 未持有)。
    assertion: Option<AssertionHandle>,
    /// 断言代次:supervisor 退出时据此判断是否仍归属当前断言。
    generation: u64,
}

/// 可测试的唤醒断言策略。生产用 [`WakePolicy::system`];测试注入替代可执行
/// 文件路径([`WakePolicy::with_program`]),断言固定 argv 与生命周期,
/// 不真跑 caffeinate。
pub struct WakePolicy {
    program: PathBuf,
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for WakePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakePolicy")
            .field("program", &self.program)
            .finish()
    }
}

impl WakePolicy {
    /// 系统默认策略:/usr/bin/caffeinate。
    pub fn system() -> Self {
        Self::with_program(PathBuf::from(CAFFEINATE_PATH))
    }

    /// 注入可执行文件(测试 fake)。argv 仍由 [`caffeinate_args`] 固定生成。
    pub fn with_program(program: PathBuf) -> Self {
        Self {
            program,
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// 当前是否持有断言(有存活 caffeinate 进程)。
    pub fn assertion_held(&self) -> bool {
        self.inner.lock().assertion.is_some()
    }

    /// 当前断言的电源模式(未持有时 None)。
    pub fn held_mode(&self) -> Option<PowerSource> {
        self.inner.lock().assertion.as_ref().map(|a| a.mode)
    }

    /// 按当前电源与 Codex 活动状态推进断言:
    /// - 需要且未持有 → spawn;
    /// - 持有中电源模式变化 → 重启进程以切换 argv(`-s` 仅 AC 有效);
    /// - 不需要且持有 → 杀进程释放。
    pub async fn update(
        &self,
        source: PowerSource,
        turn_active: bool,
        attention_pending: bool,
    ) -> Result<(), WakeError> {
        let desired = should_hold(source, turn_active, attention_pending);
        let held = self.held_mode();

        match (desired, held) {
            (true, Some(mode)) if mode == source => Ok(()), // 已是目标状态
            (true, _) => {
                self.release().await; // 模式变化:先释放旧进程
                self.spawn(source)?;
                Ok(())
            }
            (false, Some(_)) => {
                self.release().await;
                Ok(())
            }
            (false, None) => Ok(()),
        }
    }

    /// 释放断言(状态变化、进程退出、shutdown 前调用)。未持有时为 no-op。
    pub async fn release(&self) {
        let handle = self.inner.lock().assertion.take();
        if let Some(handle) = handle {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            // supervisor 已退出(进程自发结束)时 send 失败,视为已释放。
            if handle.kill.send(ack_tx).await.is_ok() {
                let _ = ack_rx.await; // supervisor 已 kill + reap
            }
        }
    }

    fn spawn(&self, mode: PowerSource) -> Result<(), WakeError> {
        let mut cmd = tokio::process::Command::new(&self.program);
        cmd.args(caffeinate_args(mode))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let generation = {
            let mut inner = self.inner.lock();
            inner.generation += 1;
            inner.generation
        };

        let child = cmd
            .spawn()
            .map_err(|e| WakeError::Spawn(self.program.clone(), e))?;

        let (kill_tx, mut kill_rx) = mpsc::channel::<tokio::sync::oneshot::Sender<()>>(1);
        let supervisor_inner = Arc::clone(&self.inner);

        // supervisor:唯一持有 &mut Child。进程自发退出时清理句柄;
        // 收到 kill 请求时 kill + reap 后清理。generation 防止误清新断言。
        tokio::spawn(async move {
            let mut child = child;
            tokio::select! {
                status = child.wait() => {
                    tracing::debug!(status = ?status.ok().map(|s| s.to_string()), "caffeinate exited");
                }
                ack = kill_rx.recv() => {
                    if let Some(ack) = ack {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        let _ = ack.send(());
                    }
                }
            }
            let mut guard = supervisor_inner.lock();
            if guard.generation == generation {
                guard.assertion = None;
            }
        });

        self.inner.lock().assertion = Some(AssertionHandle {
            mode,
            kill: kill_tx,
        });
        Ok(())
    }
}

impl Drop for WakePolicy {
    fn drop(&mut self) {
        // 不能 await:try_send 触发 supervisor 异步 kill;即使本对象已析构,
        // supervisor 任务仍会完成 kill + reap(§19:句柄随进程退出可靠释放)。
        let mut inner = self.inner.lock();
        if let Some(handle) = inner.assertion.take() {
            let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
            let _ = handle.kill.try_send(ack_tx);
        }
    }
}

/// 测试辅助:确认给定程序存在(生产路径健康检查用)。
pub fn caffeinate_available() -> bool {
    Path::new(CAFFEINATE_PATH).exists()
}
