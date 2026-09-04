//! 电源断言接线点(权威规格 §19)。
//!
//! [`PowerCoordinator`] 注入 [`WakePolicy`],把 Codex 活动状态映射为断言:
//! turn active 或存在 pending attention → hold;turn 结束且无 attention →
//! release。不真正检测电源来源(集成代理接线),电源状态由外部传入。
//! 事件驱动入口 `watch_session` 订阅 adapter 事件并驱动状态;也可由上层
//! 直接调用 `set_turn_active`/`set_attention_pending`/`set_power_source`。

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::adapter::codex::CodexAdapter;
use crate::domain::{DomainEvent, SessionKey};
use crate::power::{PowerSource, WakeError, WakePolicy};

#[derive(Debug, Clone, Copy)]
struct ActivityState {
    source: PowerSource,
    turn_active: bool,
    attention_pending: bool,
}

/// 电源协调器:活动状态 → WakePolicy 断言推进。克隆廉价。
#[derive(Clone)]
pub struct PowerCoordinator {
    policy: Arc<WakePolicy>,
    state: Arc<Mutex<ActivityState>>,
}

impl std::fmt::Debug for PowerCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowerCoordinator")
            .field("state", &*self.state.lock())
            .finish_non_exhaustive()
    }
}

impl PowerCoordinator {
    pub fn new(policy: Arc<WakePolicy>, source: PowerSource) -> Self {
        Self {
            policy,
            state: Arc::new(Mutex::new(ActivityState {
                source,
                turn_active: false,
                attention_pending: false,
            })),
        }
    }

    /// 注入电源来源(外部检测;§19 集成点)。
    pub async fn set_power_source(&self, source: PowerSource) -> Result<(), WakeError> {
        let desired = {
            let mut state = self.state.lock();
            state.source = source;
            *state
        };
        self.sync(desired).await
    }

    /// turn 活动状态(RUNNING/FINISHING;idle 为 false)。
    pub async fn set_turn_active(&self, active: bool) -> Result<(), WakeError> {
        let desired = {
            let mut state = self.state.lock();
            state.turn_active = active;
            *state
        };
        self.sync(desired).await
    }

    /// 存在 pending attention(问题或审批)。
    pub async fn set_attention_pending(&self, pending: bool) -> Result<(), WakeError> {
        let desired = {
            let mut state = self.state.lock();
            state.attention_pending = pending;
            *state
        };
        self.sync(desired).await
    }

    /// 是否持有断言。
    pub fn assertion_held(&self) -> bool {
        self.policy.assertion_held()
    }

    /// 释放断言(shutdown 前调用)。
    pub async fn release(&self) {
        self.policy.release().await;
    }

    /// 订阅会话事件并驱动断言:TurnLifecycle / PendingAttention 事件 →
    /// 以运行快照重估 turn 活动与 attention → WakePolicy 推进。
    pub async fn watch_session(
        &self,
        adapter: &CodexAdapter,
        key: &SessionKey,
    ) -> Result<(), crate::domain::BridgeError> {
        let (tx, mut rx) = mpsc::channel::<DomainEvent>(256);
        adapter
            .subscribe(key, tx)
            .await
            .map_err(|err| crate::domain::BridgeError::new(err.code(), err.to_string()))?;
        let coordinator = self.clone();
        let adapter_for_task = adapter.clone();
        let watch_key = key.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let relevant = matches!(
                    event,
                    DomainEvent::TurnLifecycle { .. }
                        | DomainEvent::PendingAttentionAdded { .. }
                        | DomainEvent::PendingAttentionRemoved { .. }
                );
                if !relevant {
                    continue;
                }
                if let Ok(snapshot) = adapter_for_task.runtime_snapshot(&watch_key).await {
                    let turn_active = snapshot
                        .current_turn
                        .as_ref()
                        .map(|t| t.phase != crate::domain::ActiveTurnPhase::Idle)
                        .unwrap_or(false);
                    let attention_pending = !snapshot.pending_questions.is_empty()
                        || !snapshot.pending_approvals.is_empty();
                    // 快照噪声变化不推进断言:仅在值变化时调用 update。
                    coordinator
                        .apply_if_changed(turn_active, attention_pending)
                        .await;
                }
            }
        });
        Ok(())
    }

    async fn apply_if_changed(&self, turn_active: bool, attention_pending: bool) {
        let desired = {
            let mut state = self.state.lock();
            if state.turn_active == turn_active && state.attention_pending == attention_pending {
                return;
            }
            state.turn_active = turn_active;
            state.attention_pending = attention_pending;
            *state
        };
        self.sync(desired).await.ok();
    }

    async fn sync(&self, desired: ActivityState) -> Result<(), WakeError> {
        self.policy
            .update(
                desired.source,
                desired.turn_active,
                desired.attention_pending,
            )
            .await
    }
}
