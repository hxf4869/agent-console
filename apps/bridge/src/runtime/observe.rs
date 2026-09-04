//! 事件优先与自适应观察(权威规格 §14 逐字实现)。
//!
//! Bridge 优先消费 Desktop 原生事件(adapter watcher 推送),snapshot 只用于
//! 补偿、校正和低频确认。初始观察策略(§14, M1 初始值,非产品 SLA):
//!
//! - 有详情订阅且 turn RUNNING:约 500ms 轻量观察起点。
//! - RUNNING 无详情订阅:约 2s 轻量状态检查(投影快照,不读完整历史)。
//! - idle 且有列表订阅:摘要级刷新,起点约 30s;原生事件到达立即更新
//!   (事件路径由 adapter watcher 直接驱动,不经本模块)。
//! - 无订阅:不轮询 idle 历史;仅对已知活跃 turn 保留约 2s 完成检测。
//! - 断线/sequence 缺口/owner 切换/未知 patch → 立即取一次完整
//!   RuntimeSnapshot:断线由 [`BridgeRuntime::resync`] 处理;未知 patch 由
//!   adapter(ResyncNeeded → load-complete-history)处理;本模块的重同步
//!   观察随 resync 触发。
//! - 电池供电、系统压力或活跃任务数增加时降频;等待 attention 不高频读
//!   完整输出(轻量检查本就只看投影状态,不读输出流)。
//!
//! 差分只比较稳定 ID、revision、长度与状态字段(由 adapter mapper 保证);
//! 本模块只保留每会话的 `runtime_revision`/phase/attention 计数。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// 有详情订阅 + turn RUNNING:轻量观察起点(§14 约 500ms)。
pub const DETAIL_RUNNING_OBSERVE_INTERVAL: Duration = Duration::from_millis(500);
/// RUNNING 无详情订阅 / 无订阅但 turn 活跃:轻量状态检查间隔(§14 约 2s)。
pub const LIGHT_STATE_CHECK_INTERVAL: Duration = Duration::from_secs(2);
/// idle 且有列表订阅:摘要级刷新起点(§14 约 30s)。
pub const IDLE_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);
/// 无订阅且 idle:不轮询(None,§14:保留心跳与活跃 turn 完成检测即可)。
pub const NO_SUBSCRIPTION_INTERVAL: Option<Duration> = None;
/// 电池供电降频倍数(§14:电池供电时降低频率)。
pub const BATTERY_SLOWDOWN: u32 = 2;
/// 活跃任务压力阈值:同 device 活跃 turn 数达到该值后再降频(§14)。
pub const ACTIVE_TASK_PRESSURE_THRESHOLD: usize = 3;
/// 压力降频倍数。
pub const PRESSURE_SLOWDOWN: u32 = 2;
/// 降频倍数上限(避免长 turn 完全失去完成检测)。
pub const MAX_SLOWDOWN: u32 = 4;
/// 观察循环基础 tick:调度粒度,不等于观察频率。
pub const OBSERVE_TICK: Duration = Duration::from_millis(200);

/// 单会话观察输入(全部来自缓存,不做 IO)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionObserveState {
    /// 是否存在详情订阅流。
    pub has_detail_subscription: bool,
    /// 是否存在列表订阅。
    pub has_list_subscription: bool,
    /// 当前 turn 是否活跃(RUNNING/FINISHING)。
    pub turn_active: bool,
}

/// 电源与压力输入。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservePressure {
    pub battery_powered: bool,
    /// 同 device 活跃 turn 数。
    pub active_tasks: usize,
}

/// 纯策略:单会话下一次观察间隔。`None` = 不轮询(§14 无订阅 idle)。
pub fn next_interval(state: SessionObserveState, pressure: ObservePressure) -> Option<Duration> {
    let base = if state.turn_active {
        if state.has_detail_subscription {
            DETAIL_RUNNING_OBSERVE_INTERVAL
        } else {
            LIGHT_STATE_CHECK_INTERVAL
        }
    } else if state.has_list_subscription {
        IDLE_SUMMARY_INTERVAL
    } else {
        return NO_SUBSCRIPTION_INTERVAL;
    };
    let mut factor = 1u32;
    if pressure.battery_powered {
        factor = factor.saturating_mul(BATTERY_SLOWDOWN);
    }
    if pressure.active_tasks >= ACTIVE_TASK_PRESSURE_THRESHOLD {
        factor = factor.saturating_mul(PRESSURE_SLOWDOWN);
    }
    Some(base * factor.min(MAX_SLOWDOWN))
}

/// 观察循环:每 tick 评估到期的会话并执行轻量检查。
/// 会话缓存只记 ID/revision/phase,不保留会话内容(§25.3)。
pub(crate) struct ObserveScheduler {
    /// `None` = 策略关闭(无订阅且 idle):停摆,不删除登记。
    due: HashMap<String, Option<std::time::Instant>>,
}

impl Default for ObserveScheduler {
    fn default() -> Self {
        Self {
            due: HashMap::new(),
        }
    }
}

impl ObserveScheduler {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 登记会话(立即到期一次,建立基线)。
    pub(crate) fn register(&mut self, native_session_id: &str) {
        self.due.insert(
            native_session_id.to_string(),
            Some(std::time::Instant::now()),
        );
    }

    pub(crate) fn known(&self, native_session_id: &str) -> bool {
        self.due.contains_key(native_session_id)
    }

    /// 取出到期会话并按策略预约下一次。
    pub(crate) fn take_due(
        &mut self,
        state_of: impl Fn(&str) -> Option<SessionObserveState>,
        pressure: ObservePressure,
    ) -> Vec<String> {
        let now = std::time::Instant::now();
        let mut due = Vec::new();
        for (session, deadline) in self.due.iter_mut() {
            match *deadline {
                Some(at) if at <= now => {
                    due.push(session.clone());
                    // 策略关闭(无订阅且 idle)→ 停摆;否则按间隔重排。
                    *deadline = state_of(session)
                        .and_then(|s| next_interval(s, pressure))
                        .map(|interval| now + interval);
                }
                _ => {}
            }
        }
        due
    }
}

/// 观察任务主循环。失败不退出;adapter 快照不可得时静默跳过(下一 tick 重试)。
pub(crate) fn spawn_observer(
    runtime: Arc<crate::runtime::BridgeRuntime>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut scheduler = ObserveScheduler::new();
        // 已知会话建立基线(事件到达时由 runtime 再登记)。
        for session in runtime.known_session_ids() {
            scheduler.register(&session);
        }
        let mut tick = tokio::time::interval(OBSERVE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = runtime.observe_stopped() => break,
            }
            // 新出现/新订阅的会话每 tick 补登记(登记即到期一次,建立基线)。
            for session in runtime.known_session_ids() {
                if !scheduler.known(&session) {
                    scheduler.register(&session);
                }
            }
            if scheduler.due.is_empty() {
                continue;
            }
            let pressure = runtime.observe_pressure();
            let state_of = |session: &str| runtime.observe_state_of(session);
            for session in scheduler.take_due(state_of, pressure) {
                runtime.observe_session(&session).await;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(detail: bool, list: bool, active: bool) -> SessionObserveState {
        SessionObserveState {
            has_detail_subscription: detail,
            has_list_subscription: list,
            turn_active: active,
        }
    }

    fn pressure(battery: bool, tasks: usize) -> ObservePressure {
        ObservePressure {
            battery_powered: battery,
            active_tasks: tasks,
        }
    }

    /// §14 初始策略逐条锁定。
    #[test]
    fn policy_matches_spec_initial_values() {
        // 详情订阅 + RUNNING:约 500ms 轻量观察起点。
        assert_eq!(
            next_interval(state(true, true, true), pressure(false, 0)),
            Some(DETAIL_RUNNING_OBSERVE_INTERVAL)
        );
        // RUNNING 无详情订阅:约 2s 轻量状态检查。
        assert_eq!(
            next_interval(state(false, true, true), pressure(false, 0)),
            Some(LIGHT_STATE_CHECK_INTERVAL)
        );
        // idle 且有列表订阅:摘要级,起点约 30s。
        assert_eq!(
            next_interval(state(false, true, false), pressure(false, 0)),
            Some(IDLE_SUMMARY_INTERVAL)
        );
        // 无订阅:不轮询 idle 历史。
        assert_eq!(
            next_interval(state(false, false, false), pressure(false, 0)),
            None
        );
        // 无订阅但 turn 活跃:保留活跃 turn 完成检测(2s)。
        assert_eq!(
            next_interval(state(false, false, true), pressure(false, 0)),
            Some(LIGHT_STATE_CHECK_INTERVAL)
        );
    }

    /// 电池/活跃任务压力降频;上限封顶。
    #[test]
    fn pressure_slows_down_polling_with_cap() {
        let base = next_interval(state(true, true, true), pressure(false, 0)).unwrap();
        let battery = next_interval(state(true, true, true), pressure(true, 0)).unwrap();
        assert_eq!(battery, base * BATTERY_SLOWDOWN);

        let loaded = next_interval(
            state(true, true, true),
            pressure(false, ACTIVE_TASK_PRESSURE_THRESHOLD),
        )
        .unwrap();
        assert_eq!(loaded, base * PRESSURE_SLOWDOWN);

        let both = next_interval(
            state(true, true, true),
            pressure(true, ACTIVE_TASK_PRESSURE_THRESHOLD + 5),
        )
        .unwrap();
        assert_eq!(both, base * MAX_SLOWDOWN, "降频封顶 4x,保留完成检测");

        // idle 会话不受压力影响(已经是最慢档)。
        let idle = next_interval(state(false, true, false), pressure(true, 10)).unwrap();
        assert_eq!(idle, IDLE_SUMMARY_INTERVAL * MAX_SLOWDOWN);
    }

    /// 调度器:登记即到期;take_due 后按策略重排;策略关闭则停摆。
    #[test]
    fn scheduler_reschedules_by_policy() {
        let mut scheduler = ObserveScheduler::new();
        scheduler.register("s1");
        scheduler.register("s2");
        let due = scheduler.take_due(
            |s| {
                if s == "s1" {
                    Some(state(true, true, true))
                } else {
                    Some(state(false, false, false))
                }
            },
            pressure(false, 0),
        );
        assert_eq!(due.len(), 2);
        // s1 仍被调度(500ms 后),s2 已停摆(无订阅 idle)。
        assert!(scheduler.known("s1"));
        assert!(scheduler.known("s2"), "停摆会话保留登记但不再触发");
        let due_again =
            scheduler.take_due(|_| Some(state(false, false, false)), pressure(false, 0));
        assert!(
            !due_again.contains(&"s1".to_string()),
            "500ms 未到不应再次到期"
        );
    }
}
