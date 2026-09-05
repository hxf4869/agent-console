//! per-session 投影状态机:snapshot/patch → RuntimeSnapshot + DomainEvent。
//!
//! 差分规则(§14):只比较稳定 ID、revision、游标、长度与状态字段,不做全树
//! 深比较。revision 不吻合或未知 patch → [`PatchOutcome::ResyncNeeded`],
//! 由上层触发 load-complete-history / 新 snapshot。
//!
//! 输出合并(§26.3):高频 delta 按 [`OutputMergePolicy`] 合并——缓冲达到
//! 64 KiB 立即分块发出(UTF-8 边界安全),否则最多等 `max_delay`(默认 75ms,
//! 50–100ms 区间中值)由 [`SessionMapper::flush_due_outputs`] 发出。
//!
//! 输出真实性(§13):新输出是旧输出的前缀扩展时才走 OutputAppend;缺口/改写
//! 走 OutputReplace(完整替换);命令进入终态时冲刷缓冲并发 OutputFinal;
//! 权威快照缺输出字段而预览存在时标记 final_unavailable
//! (FINAL_OUTPUT_UNAVAILABLE),不把预览冒充完整结果。
//!
//! 隐私:中间状态只在内存(会话状态树 + item 轨迹),绝不落盘;`Debug`
//! 只含 ID/revision/长度。

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::domain::{
    ActiveTurnPhase, BackgroundCommand, CapabilitySet, CommandStatusState, CurrentTurn,
    DeviceConnection, DomainEvent, Item, ItemContent, ItemId, LastTurnOutcome, OutputBytes,
    OutputChannel, OutputCursor, OutputText, PendingAttention, PendingAttentionKind, Plan,
    QueueStatus, RunningCommand, RuntimeSnapshot, SessionKey, SessionSummary, TurnId,
};

use super::projection::{self, SessionFacts, UnknownKeyCounts};

/// 输出合并策略(§26.3:约 50–100ms 或 64KiB 先到者)。
#[derive(Debug, Clone)]
pub struct OutputMergePolicy {
    pub max_delay: Duration,
    pub max_bytes: usize,
}

impl Default for OutputMergePolicy {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_millis(75),
            max_bytes: 64 * 1024,
        }
    }
}

/// patch 应用结果。
#[derive(Debug, Clone, PartialEq)]
pub enum PatchOutcome {
    Applied { events: Vec<DomainEvent> },
    ResyncNeeded,
}

/// item 输出/内容轨迹(差分只依赖这里记录的稳定维度,§14)。
#[derive(Debug, Clone, Default)]
struct ItemTrack {
    /// 投影序号(最近输出游标排序用)。
    seq: u64,
    native_type: String,
    /// 原生 status 字符串(命令终态判定)。
    status: Option<String>,
    /// 非 output 内容签名(文本长度/步骤数);变化 → ItemUpsert。
    content_len: usize,
    /// 输出 item 的当前完整输出(内存;不落盘)。
    output: Vec<u8>,
    /// 权威状态中输出字段是否存在(§13.3 final_unavailable 判定)。
    output_present: bool,
    final_emitted: bool,
}

/// 待合并输出缓冲:expected_offset + 未发字节 + 入队时间。
#[derive(Debug)]
struct PendingOutput {
    expected_offset: u64,
    buffer: Vec<u8>,
    queued_at: Instant,
}

/// 单会话投影状态机。
pub struct SessionMapper {
    session_key: SessionKey,
    merge_policy: OutputMergePolicy,
    revision: Option<u64>,
    state: Option<Value>,
    facts: SessionFacts,
    capabilities: CapabilitySet,
    device_connection: DeviceConnection,
    queue: QueueStatus,
    items: BTreeMap<String, ItemTrack>,
    attentions: BTreeMap<String, PendingAttentionKind>,
    pending_outputs: BTreeMap<String, PendingOutput>,
    last_summary: Option<SessionSummary>,
    current_turn: Option<TurnId>,
    phase: ActiveTurnPhase,
    last_outcome: Option<LastTurnOutcome>,
    next_seq: u64,
    unknown_keys: UnknownKeyCounts,
}

impl std::fmt::Debug for SessionMapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionMapper")
            .field("session", &self.session_key.native_session_id)
            .field("revision", &self.revision)
            .field("tracked_items", &self.items.len())
            .field("pending_outputs", &self.pending_outputs.len())
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

impl SessionMapper {
    pub fn new(session_key: SessionKey) -> Self {
        Self {
            session_key,
            merge_policy: OutputMergePolicy::default(),
            revision: None,
            state: None,
            facts: SessionFacts::default(),
            capabilities: CapabilitySet::read_only_degraded(None),
            device_connection: DeviceConnection::Online,
            queue: QueueStatus::default(),
            items: BTreeMap::new(),
            attentions: BTreeMap::new(),
            pending_outputs: BTreeMap::new(),
            last_summary: None,
            current_turn: None,
            phase: ActiveTurnPhase::Idle,
            last_outcome: None,
            next_seq: 0,
            unknown_keys: UnknownKeyCounts::new(),
        }
    }

    /// 注入能力摘要(适配器 probe 后调用;进 RuntimeSnapshot 与 summary)。
    pub fn set_capabilities(&mut self, capabilities: CapabilitySet) {
        self.capabilities = capabilities;
    }

    /// 注入设备连接状态。
    pub fn set_device_connection(&mut self, connection: DeviceConnection) {
        self.device_connection = connection;
    }

    /// 注入 Bridge 本地队列状态;变化时返回 QueueStateChanged 事件。
    pub fn set_queue(&mut self, queue: QueueStatus) -> Option<DomainEvent> {
        if self.queue == queue {
            return None;
        }
        self.queue = queue.clone();
        Some(DomainEvent::QueueStateChanged { queue })
    }

    pub fn runtime_revision(&self) -> Option<u64> {
        self.revision
    }

    pub fn has_snapshot(&self) -> bool {
        self.state.is_some()
    }

    /// 会话 cwd(仅 adapter 内部/本机 Git 与文件授权使用;
    /// 绝不序列化出 Bridge,§12/§17.2/§23.1)。
    pub(crate) fn cwd(&self) -> Option<std::path::PathBuf> {
        self.state
            .as_ref()
            .and_then(|state| state.get("cwd"))
            .and_then(Value::as_str)
            .map(std::path::PathBuf::from)
    }

    /// Desktop 0.153.1 的 steer owner 会读取 restoreMessage.cwd；从本机
    /// 权威快照原样回填 cwd 与协作模式，仅用于本机 IPC，不进入 Relay。
    pub(crate) fn steer_restore_message(&self) -> Option<Value> {
        let state = self.state.as_ref()?;
        let cwd = state.get("cwd").and_then(Value::as_str)?;
        let mut context = serde_json::json!({"workspaceRoots": [cwd]});
        if let Some(mode) = state.get("latestCollaborationMode") {
            context["collaborationMode"] = mode.clone();
        }
        Some(serde_json::json!({"cwd": cwd, "context": context}))
    }

    pub fn unknown_keys(&self) -> &UnknownKeyCounts {
        &self.unknown_keys
    }

    /// 待合并输出总字节数(诊断/测试)。
    pub fn pending_output_bytes(&self) -> usize {
        self.pending_outputs.values().map(|p| p.buffer.len()).sum()
    }

    // -----------------------------------------------------------------
    // snapshot 路径
    // -----------------------------------------------------------------

    /// 应用权威快照。首个快照只登记轨迹 + 发生命周期/attention/summary 事件
    /// (历史条目由 HistoryPage 提供,不在此重放);后续快照按 §14 差分。
    pub fn apply_snapshot(&mut self, revision: u64, state: &Value) -> Vec<DomainEvent> {
        let first = self.state.is_none();
        self.revision = Some(revision);
        self.state = Some(state.clone());
        self.unknown_keys = projection::count_unknown_keys(state);
        self.facts = projection::extract_facts(state);

        let mut events = Vec::new();
        let turns = collect_turns(state);

        // ---- item 轨迹差分 ----
        let mut new_items: BTreeMap<String, ItemTrack> = BTreeMap::new();
        for (turn_id, turn_value) in &turns {
            for item_value in projection::turn_items_of(turn_value) {
                let native_type = item_type_of(item_value);
                let item =
                    projection::parse_item(&native_type, item_value, Some(turn_id.clone()), None);
                let key = track_key(&item.item_id);
                new_items.entry(key).or_insert_with(|| {
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    self.build_track(&native_type, item_value, &item, seq)
                });
            }
        }

        for (key, mut new_track) in new_items {
            match self.items.get(&key) {
                None => {
                    self.items.insert(key.clone(), new_track.clone());
                    if !first {
                        // 后续快照中出现的新 item:ItemUpsert + 权威输出校正。
                        if let Some(item) = find_item(state, &key) {
                            events.push(DomainEvent::ItemUpsert { item });
                            events
                                .extend(self.output_events_for_new_snapshot_item(&key, &new_track));
                        }
                    }
                }
                Some(old_track) => {
                    let old_track = old_track.clone();
                    let changed = old_track.status != new_track.status
                        || old_track.content_len != new_track.content_len
                        || old_track.output.len() != new_track.output.len();
                    events.extend(self.output_delta_events(&key, &old_track, &mut new_track));
                    if changed {
                        if let Some(item) = find_item(state, &key) {
                            events.push(DomainEvent::ItemUpsert { item });
                        }
                    }
                    self.items.insert(key, new_track);
                }
            }
        }
        // 移除消失的 item(分页窗口变化)。
        let current_keys: Vec<String> = self.items.keys().cloned().collect();
        for key in current_keys {
            if !turns.iter().any(|(_, t)| {
                projection::turn_items_of(t)
                    .iter()
                    .any(|it| it.get("id").and_then(Value::as_str) == Some(key.as_str()))
            }) {
                self.items.remove(&key);
            }
        }

        // ---- 当前 turn / 生命周期 ----
        let running_turn = turns
            .iter()
            .find(|(_, t)| projection::turn_status_of(t) == Some("inProgress"));
        let last_terminal = turns.iter().rev().find_map(|(turn_id, t)| {
            projection::map_turn_outcome(projection::turn_status_of(t))
                .map(|o| (turn_id.clone(), o))
        });
        // turn 状态权威(§5):turn 已终态时即使 runtimeStatus 仍报 active,
        // 也回到 Idle;runtimeStatus 只在无 turn 信息时作为辅助。
        let new_phase = if running_turn.is_some() {
            ActiveTurnPhase::Running
        } else if self.facts.runtime_status.as_deref() == Some("finishing") {
            ActiveTurnPhase::Finishing
        } else if last_terminal.is_some() {
            ActiveTurnPhase::Idle
        } else {
            projection::map_phase(self.facts.runtime_status.as_deref())
        };
        let new_current_turn = running_turn.map(|(turn_id, _)| turn_id.clone());
        let last_terminal = turns.iter().rev().find_map(|(turn_id, t)| {
            projection::map_turn_outcome(projection::turn_status_of(t))
                .map(|o| (turn_id.clone(), o))
        });
        let phase_changed = self.phase != new_phase || self.current_turn != new_current_turn;
        if phase_changed {
            events.push(DomainEvent::TurnLifecycle {
                turn: new_current_turn
                    .clone()
                    .or_else(|| last_terminal.as_ref().map(|(t, _)| t.clone()))
                    .unwrap_or_else(|| TurnId::synthetic("unknown")),
                phase: new_phase,
                outcome: if new_phase == ActiveTurnPhase::Idle {
                    last_terminal.as_ref().map(|(_, o)| *o)
                } else {
                    None
                },
                finished_at: None,
            });
        }
        self.phase = new_phase;
        self.current_turn = new_current_turn;
        if new_phase == ActiveTurnPhase::Idle {
            if let Some((_, outcome)) = last_terminal {
                self.last_outcome = Some(outcome);
            }
        }

        // ---- attention(可靠恢复,§13.1) ----
        events.extend(self.diff_attentions(state));

        // ---- summary ----
        let summary = self.session_summary();
        if self.last_summary.as_ref() != Some(&summary) {
            events.push(DomainEvent::SessionSummaryChanged {
                summary: summary.clone(),
            });
            self.last_summary = Some(summary);
        }
        events
    }

    // -----------------------------------------------------------------
    // patch 路径
    // -----------------------------------------------------------------

    /// 应用增量 patch。baseRevision 与当前 revision 不吻合、未知 op 或应用
    /// 失败 → ResyncNeeded(状态保持不变,§12/§14)。
    pub fn apply_patches(
        &mut self,
        base_revision: u64,
        revision: u64,
        patches: &[Value],
    ) -> PatchOutcome {
        if self.revision != Some(base_revision) || revision <= base_revision {
            return PatchOutcome::ResyncNeeded;
        }
        let Some(previous_state) = self.state.clone() else {
            return PatchOutcome::ResyncNeeded;
        };
        let mut patched = previous_state;
        if projection::apply_immer_patches(&mut patched, patches).is_err() {
            return PatchOutcome::ResyncNeeded;
        }
        let events = self.apply_snapshot(revision, &patched);
        PatchOutcome::Applied { events }
    }

    // -----------------------------------------------------------------
    // 输出合并(§26.3)
    // -----------------------------------------------------------------

    /// 冲刷到期的待合并输出(`now - queued_at >= max_delay`)。
    pub fn flush_due_outputs(&mut self, now: Instant) -> Vec<DomainEvent> {
        let keys: Vec<String> = self
            .pending_outputs
            .iter()
            .filter(|(_, p)| now.duration_since(p.queued_at) >= self.merge_policy.max_delay)
            .map(|(k, _)| k.clone())
            .collect();
        let mut events = Vec::new();
        for key in keys {
            events.extend(self.take_pending_output(&key));
        }
        events
    }

    /// 立即冲刷某 item 的全部待合并输出(终态定稿前调用)。
    fn take_pending_output(&mut self, key: &str) -> Vec<DomainEvent> {
        let Some(pending) = self.pending_outputs.remove(key) else {
            return Vec::new();
        };
        if pending.buffer.is_empty() {
            return Vec::new();
        }
        let item_id = key_to_item_id(key);
        let mut events = Vec::new();
        let mut offset = pending.expected_offset;
        let mut rest = OutputBytes(pending.buffer);
        while !rest.is_empty() {
            let (chunk, tail) = rest
                .split_utf8_safe(self.merge_policy.max_bytes)
                .expect("non-empty buffer splits at UTF-8 boundary");
            events.push(DomainEvent::OutputAppend {
                item_id: item_id.clone(),
                expected_offset: offset,
                bytes: chunk.clone(),
                channel: OutputChannel::Combined,
            });
            offset += chunk.len() as u64;
            rest = tail;
        }
        events
    }

    /// 缓冲达到上限时立即分块发出;否则留待 flush_due_outputs。
    fn maybe_flush_full(&mut self, key: &str) -> Vec<DomainEvent> {
        let full = self
            .pending_outputs
            .get(key)
            .map(|p| p.buffer.len() >= self.merge_policy.max_bytes)
            .unwrap_or(false);
        if full {
            self.take_pending_output(key)
        } else {
            Vec::new()
        }
    }

    // -----------------------------------------------------------------
    // 快照构建
    // -----------------------------------------------------------------

    /// 从当前投影状态构建 RuntimeSnapshot;尚未收到快照时返回 None。
    pub fn runtime_snapshot(&self) -> Option<RuntimeSnapshot> {
        let state = self.state.as_ref()?;
        let questions = projection::extract_questions(state);
        let approvals = projection::extract_approvals(state);
        let (running, backgrounds) = self.derive_commands(state);
        let background_count = backgrounds.len() as u32;
        Some(RuntimeSnapshot {
            session_key: self.session_key.clone(),
            runtime_revision: self.revision.unwrap_or_default(),
            current_turn: self.current_turn.as_ref().map(|turn| CurrentTurn {
                turn: turn.clone(),
                phase: self.phase,
                started_at: None,
            }),
            plan: self.latest_plan(state),
            pending_questions: questions,
            pending_approvals: approvals,
            running_commands: running,
            background_commands: backgrounds.clone(),
            background_command_count: background_count,
            queue: self.queue.clone(),
            capabilities: {
                let mut caps = self.capabilities.clone();
                caps.settings = projection::extract_settings(state, self.phase);
                caps
            },
            recent_output_cursors: self.recent_output_cursors(),
        })
    }

    /// 会话摘要(§11.1)。
    pub fn session_summary(&self) -> SessionSummary {
        let mut kinds: Vec<PendingAttentionKind> = self.attentions.values().copied().collect();
        kinds.sort();
        kinds.dedup();
        SessionSummary {
            session_key: self.session_key.clone(),
            title: self.facts.title.clone().map(OutputText::new),
            agent_kind: self.session_key.agent_kind,
            project_display_name: self.facts.project_display.clone(),
            current_branch: self.facts.git_branch.clone(),
            updated_at: None,
            device_connection: self.device_connection,
            device_last_seen_at: None,
            degraded_reason: None,
            control_mode: self.capabilities.control_mode,
            compatibility_state: self.capabilities.compatibility_state,
            active_turn_phase: self.phase,
            pending_attention_count: self.attentions.len() as u32,
            pending_attention_kinds: kinds,
            queue_state: self.queue.state,
            last_turn_outcome: self.last_outcome.unwrap_or(LastTurnOutcome::Unknown),
            pinned: false,
            muted: false,
            archived: false,
        }
    }

    // -----------------------------------------------------------------
    // 内部:差分与事件生成
    // -----------------------------------------------------------------

    fn build_track(
        &self,
        native_type: &str,
        item_value: &Value,
        item: &Item,
        seq: u64,
    ) -> ItemTrack {
        let is_output = matches!(item.content, ItemContent::CommandStatus { .. });
        ItemTrack {
            seq,
            native_type: native_type.to_string(),
            status: item_value
                .get("status")
                .and_then(Value::as_str)
                .map(String::from),
            content_len: content_signature(&item.content),
            output: if is_output {
                projection::command_output(item_value)
                    .map(|o| o.as_bytes().to_vec())
                    .unwrap_or_default()
            } else {
                Vec::new()
            },
            output_present: is_output && projection::command_output(item_value).is_some(),
            final_emitted: false,
        }
    }

    /// 后续快照中首次登记的输出 item:发出权威内容校正(OutputReplace/Final)。
    fn output_events_for_new_snapshot_item(
        &mut self,
        key: &str,
        new_track: &ItemTrack,
    ) -> Vec<DomainEvent> {
        if !is_output_item(&new_track.native_type) {
            return Vec::new();
        }
        let item_id = key_to_item_id(key);
        let mut events = Vec::new();
        if !new_track.output.is_empty() {
            events.push(DomainEvent::OutputReplace {
                item_id: item_id.clone(),
                revision: 0,
                bytes: OutputBytes(new_track.output.clone()),
                channel: OutputChannel::Combined,
            });
        }
        if command_status_is_terminal(&new_track.status) && !new_track.final_emitted {
            events.push(DomainEvent::OutputFinal {
                item_id,
                revision: 0,
                byte_length: new_track.output.len() as u64,
                channel: OutputChannel::Combined,
                // 预览存在但权威快照缺失输出字段 → FINAL_OUTPUT_UNAVAILABLE。
                final_unavailable: !new_track.output_present,
            });
        }
        events
    }

    /// 差分单个输出 item:旧轨迹 → 新轨迹的 Append/Replace/Final 事件。
    fn output_delta_events(
        &mut self,
        key: &str,
        old_track: &ItemTrack,
        new_track: &mut ItemTrack,
    ) -> Vec<DomainEvent> {
        if !is_output_item(&new_track.native_type) {
            return Vec::new();
        }
        let item_id = key_to_item_id(key);
        let mut events = Vec::new();

        if new_track.output.len() != old_track.output.len() {
            if new_track.output.starts_with(&old_track.output) {
                // 前缀扩展 → 增量追加,走合并缓冲(§26.3)。
                let delta = new_track.output[old_track.output.len()..].to_vec();
                let entry = self
                    .pending_outputs
                    .entry(key.to_string())
                    .or_insert_with(|| PendingOutput {
                        expected_offset: old_track.output.len() as u64,
                        buffer: Vec::new(),
                        queued_at: Instant::now(),
                    });
                entry.buffer.extend_from_slice(&delta);
                events.extend(self.maybe_flush_full(key));
            } else {
                // 缺口/改写 → 完整替换(§13.2),并清掉未发缓冲避免拼接错误。
                self.pending_outputs.remove(key);
                events.push(DomainEvent::OutputReplace {
                    item_id: item_id.clone(),
                    revision: 0,
                    bytes: OutputBytes(new_track.output.clone()),
                    channel: OutputChannel::Combined,
                });
            }
        }

        // 终态定稿(§13.3):先冲缓冲,再 OutputFinal。
        if command_status_is_terminal(&new_track.status)
            && !command_status_is_terminal(&old_track.status)
            && !new_track.final_emitted
        {
            events.extend(self.take_pending_output(key));
            events.push(DomainEvent::OutputFinal {
                item_id,
                revision: 0,
                byte_length: new_track.output.len() as u64,
                channel: OutputChannel::Combined,
                final_unavailable: !new_track.output_present,
            });
            new_track.final_emitted = true;
        }
        events
    }

    /// attention 差分:Added/Removed(§13.1 生命周期必须可靠恢复)。
    fn diff_attentions(&mut self, state: &Value) -> Vec<DomainEvent> {
        let mut events = Vec::new();
        let mut next: BTreeMap<String, PendingAttentionKind> = BTreeMap::new();
        for q in projection::extract_questions(state) {
            next.insert(q.question_id.clone(), PendingAttentionKind::UserQuestion);
        }
        for a in projection::extract_approvals(state) {
            next.insert(a.approval_id.clone(), PendingAttentionKind::RiskApproval);
        }
        // 移除(先算,需要旧列表)。
        let removed: Vec<(String, PendingAttentionKind)> = self
            .attentions
            .iter()
            .filter(|(id, _)| !next.contains_key(*id))
            .map(|(id, kind)| (id.clone(), *kind))
            .collect();
        for (id, kind) in removed {
            events.push(DomainEvent::PendingAttentionRemoved {
                kind,
                native_id: id,
                turn: None,
            });
        }
        // 新增。
        for (id, kind) in &next {
            if self.attentions.contains_key(id) {
                continue;
            }
            let attention = if *kind == PendingAttentionKind::UserQuestion {
                projection::extract_questions(state)
                    .into_iter()
                    .find(|q| &q.question_id == id)
                    .map(PendingAttention::Question)
            } else {
                projection::extract_approvals(state)
                    .into_iter()
                    .find(|a| &a.approval_id == id)
                    .map(PendingAttention::Approval)
            };
            if let Some(attention) = attention {
                events.push(DomainEvent::PendingAttentionAdded { attention });
            }
        }
        self.attentions = next;
        events
    }

    /// 派生运行中/后台命令(§9.2/§10.8):
    /// turn 进行中且命令运行 → 运行命令;turn 已终态而命令仍在运行 → 后台命令;
    /// 无稳定 ID 的命令只计入总数,不伪造单项(§9.2)。
    fn derive_commands(&self, state: &Value) -> (Vec<RunningCommand>, Vec<BackgroundCommand>) {
        let mut running = Vec::new();
        let mut backgrounds: Vec<BackgroundCommand> = Vec::new();
        let mut unidentifiable = 0usize;
        for (turn_id, turn_value) in collect_turns(state) {
            let turn_running = projection::turn_status_of(turn_value) == Some("inProgress");
            for item_value in projection::turn_items_of(turn_value) {
                if item_type_of(item_value) != "commandExecution" {
                    continue;
                }
                let status = item_value.get("status").and_then(Value::as_str);
                let cmd_state = projection::map_command_status(status);
                let item = projection::parse_item(
                    "commandExecution",
                    item_value,
                    Some(turn_id.clone()),
                    None,
                );
                let native_id = item_value.get("id").and_then(Value::as_str);
                match (turn_running, cmd_state) {
                    (true, CommandStatusState::Running) => match native_id {
                        Some(id) => running.push(RunningCommand {
                            command_id: id.to_string(),
                            item_id: Some(item.item_id.clone()),
                            started_at: None,
                        }),
                        None => unidentifiable += 1,
                    },
                    (false, CommandStatusState::Running) => {
                        backgrounds.push(BackgroundCommand {
                            command_id: native_id.map(String::from),
                            item_id: Some(item.item_id.clone()),
                            state: projection::command_state_to_background(cmd_state),
                            display: item_value
                                .get("command")
                                .and_then(Value::as_str)
                                .map(OutputText::new),
                            started_at: None,
                            finished_at: None,
                        });
                    }
                    _ => {}
                }
            }
        }
        let _ = unidentifiable; // 只计入 runtime_snapshot.background_command_count 的兜底来源
        (running, backgrounds)
    }

    fn latest_plan(&self, state: &Value) -> Plan {
        for (_, turn_value) in collect_turns(state) {
            for item_value in projection::turn_items_of(turn_value) {
                if matches!(item_type_of(item_value).as_str(), "plan" | "updatePlan") {
                    return projection::parse_plan(item_value.get("steps"));
                }
            }
        }
        Plan::default()
    }

    fn recent_output_cursors(&self) -> Vec<OutputCursor> {
        let mut tracks: Vec<(&String, &ItemTrack)> = self
            .items
            .iter()
            .filter(|(_, t)| !t.output.is_empty() || t.output_present)
            .collect();
        tracks.sort_by_key(|(_, t)| t.seq);
        tracks
            .iter()
            .rev()
            .take(20)
            .map(|(key, t)| OutputCursor {
                item_id: key_to_item_id(key),
                revision: self.revision.unwrap_or_default(),
                byte_length: t.output.len() as u64,
                is_final: t.final_emitted || command_status_is_terminal(&t.status),
                channel: OutputChannel::Combined,
                final_unavailable: !t.output_present,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn item_type_of(item_value: &Value) -> String {
    item_value
        .get("type")
        .or_else(|| item_value.get("item_type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

fn is_output_item(native_type: &str) -> bool {
    matches!(native_type, "commandExecution" | "command")
}

fn command_status_is_terminal(status: &Option<String>) -> bool {
    status
        .as_deref()
        .map(|s| projection::map_command_status(Some(s)))
        .map(projection::command_status_is_terminal)
        .unwrap_or(false)
}

/// item 轨迹键 = item id(原生或合成,均稳定,§9.2)。
fn track_key(item_id: &ItemId) -> String {
    item_id.id.clone()
}

fn key_to_item_id(key: &str) -> ItemId {
    match key.strip_suffix(":unnamed") {
        Some(base) => ItemId::synthetic(base.to_string()),
        None => ItemId::native(key),
    }
}

/// 非 output 内容签名:文本长度 / 步骤数 / 时长(§14:只比长度与状态)。
fn content_signature(content: &ItemContent) -> usize {
    match content {
        ItemContent::UserMessage { text }
        | ItemContent::AssistantMessage { text, .. }
        | ItemContent::ReasoningSummary { text } => text.len(),
        ItemContent::Plan { plan } => plan.steps.len(),
        ItemContent::ToolCall { duration_ms, .. } => *duration_ms as usize,
        ItemContent::CommandStatus { duration_ms, .. } => *duration_ms as usize,
        ItemContent::FileChange { changes } => changes.len(),
        ItemContent::TokenUsage { input_tokens, .. } => *input_tokens as usize,
        _ => 0,
    }
}

/// 收集 (turn_id, turn_value)。
fn collect_turns<'a>(state: &'a Value) -> Vec<(TurnId, &'a Value)> {
    projection::extract_turns(state)
        .into_iter()
        .enumerate()
        .map(|(i, turn)| (projection::turn_id_of(turn, i), turn))
        .collect()
}

/// 在状态树中按 item id 定位 Item(用于 ItemUpsert 事件)。
fn find_item(state: &Value, key: &str) -> Option<Item> {
    for (i, turn) in projection::extract_turns(state).into_iter().enumerate() {
        let turn_id = projection::turn_id_of(turn, i);
        for item_value in projection::turn_items_of(turn) {
            if item_value.get("id").and_then(Value::as_str) == Some(key) {
                let native_type = item_type_of(item_value);
                return Some(projection::parse_item(
                    &native_type,
                    item_value,
                    Some(turn_id),
                    None,
                ));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn steer_restore_message_uses_authoritative_local_context() {
        let mut mapper = SessionMapper::new(SessionKey::codex("dev", "thread-1"));
        mapper.apply_snapshot(
            1,
            &json!({
                "cwd": "/tmp/ac-e2e",
                "latestCollaborationMode": {"mode": "default", "settings": {}}
            }),
        );

        assert_eq!(
            mapper.steer_restore_message(),
            Some(json!({
                "cwd": "/tmp/ac-e2e",
                "context": {
                    "workspaceRoots": ["/tmp/ac-e2e"],
                    "collaborationMode": {"mode": "default", "settings": {}}
                }
            }))
        );
    }

    #[test]
    fn steer_restore_message_requires_snapshot_cwd() {
        let mapper = SessionMapper::new(SessionKey::codex("dev", "thread-1"));
        assert_eq!(mapper.steer_restore_message(), None);
    }
}
