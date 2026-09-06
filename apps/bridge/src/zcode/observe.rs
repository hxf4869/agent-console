//! 最小状态观察(04 §8.7):把官方 Hook 事件映射为 Bridge 侧最小元数据。
//!
//! 只采集:会话发现、开始一轮、工具等待审批、工具完成/失败、本轮停止。
//! **不读取 transcript、不复制对话**;Stop 不等同于 session 永久关闭。
//! 没有 token stream:能力是「运行中 + 最后更新时间」,不伪造聊天输出。
//! 本轮只做数据结构与 fixture 测试;web 展示归后续任务。

use std::collections::HashMap;
use parking_lot::Mutex;

use super::contract::HookInvoke;

/// 单会话最小元数据(内存内;不落盘、不进日志、不含 cwd 等路径)。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZcodeSessionMeta {
    /// 已发现会话(SessionStart)。
    pub discovered: bool,
    /// 已开始一轮(UserPromptSubmit;Stop 只结束本轮)。
    pub turn_started: bool,
    /// 当前轮是否仍在运行(Stop 置 false;PostToolUse 不改变)。
    pub turn_running: bool,
    /// 工具等待审批中(PermissionRequest 期间)。
    pub awaiting_approval: bool,
    /// 最后一次工具完成(true)/失败(false);None = 无记录。
    pub last_tool_ok: Option<bool>,
    /// 本轮已停止(Stop;≠ session 关闭)。
    pub turn_stopped: bool,
}

impl ZcodeSessionMeta {
    /// 面向展示的一句话能力描述(无 token stream 时的诚实表述)。
    pub fn activity_summary(&self) -> &'static str {
        match (self.turn_running, self.awaiting_approval) {
            (_, true) => "等待工具审批",
            (true, false) => "运行中(最后状态更新于 Hook 事件)",
            (false, false) => "空闲",
        }
    }
}

/// Hook 状态事件(官方事件名 → 内部枚举;未知事件忽略)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookStatusEvent {
    /// SessionStart(source 字段保留原文,不进日志)。
    SessionStart,
    UserPromptSubmit,
    /// PreToolUse/PostToolUse/PostToolUseFailure 携带工具名与可选 call id。
    ToolEvent {
        tool_name: Option<String>,
        tool_use_id: Option<String>,
        failed: bool,
    },
    /// PermissionRequest(工具等待审批)。
    PermissionRequested {
        tool_name: Option<String>,
        tool_use_id: Option<String>,
    },
    Stop,
}

/// 由 status invoke 推导事件(纯函数;fixture 测试入口)。
pub fn map_status_event(invoke: &HookInvoke) -> Option<HookStatusEvent> {
    let event = invoke.status_event.as_deref()?;
    match event {
        "SessionStart" => Some(HookStatusEvent::SessionStart),
        "UserPromptSubmit" => Some(HookStatusEvent::UserPromptSubmit),
        "PermissionRequest" => Some(HookStatusEvent::PermissionRequested {
            tool_name: invoke.tool_name.clone(),
            tool_use_id: invoke.tool_use_id.clone(),
        }),
        "PreToolUse" => Some(HookStatusEvent::ToolEvent {
            tool_name: invoke.tool_name.clone(),
            tool_use_id: invoke.tool_use_id.clone(),
            failed: false,
        }),
        "PostToolUse" => Some(HookStatusEvent::ToolEvent {
            tool_name: invoke.tool_name.clone(),
            tool_use_id: invoke.tool_use_id.clone(),
            failed: false,
        }),
        "PostToolUseFailure" => Some(HookStatusEvent::ToolEvent {
            tool_name: invoke.tool_name.clone(),
            tool_use_id: invoke.tool_use_id.clone(),
            failed: true,
        }),
        "Stop" => Some(HookStatusEvent::Stop),
        // Notification/SubagentStop 等官方不支持的事件:忽略。
        _ => None,
    }
}

/// 会话元数据存储(native_session_id → meta)。
#[derive(Default)]
pub struct ObservationStore {
    sessions: Mutex<HashMap<String, ZcodeSessionMeta>>,
}

impl ObservationStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 应用一条 status invoke;返回受影响会话(无法定位会话时 None)。
    pub fn apply(&self, invoke: &HookInvoke) -> Option<String> {
        let event = map_status_event(invoke)?;
        let session = invoke.native_session_id.clone()?;
        let mut sessions = self.sessions.lock();
        let meta = sessions.entry(session.clone()).or_default();
        match event {
            HookStatusEvent::SessionStart => {
                meta.discovered = true;
                meta.turn_stopped = false;
            }
            HookStatusEvent::UserPromptSubmit => {
                meta.turn_started = true;
                meta.turn_running = true;
                meta.turn_stopped = false;
            }
            HookStatusEvent::PermissionRequested { .. } => {
                meta.awaiting_approval = true;
            }
            HookStatusEvent::ToolEvent { failed, .. } => {
                meta.awaiting_approval = false;
                meta.last_tool_ok = Some(!failed);
            }
            HookStatusEvent::Stop => {
                // Stop ≠ session 关闭:只结束当前轮。
                meta.turn_running = false;
                meta.turn_stopped = true;
                meta.awaiting_approval = false;
            }
        }
        Some(session)
    }

    /// 审批通路直接更新等待标记(PermissionRequest 事件不经 apply)。
    pub fn set_awaiting_approval(&self, native_session_id: &str, awaiting: bool) {
        let mut sessions = self.sessions.lock();
        if let Some(meta) = sessions.get_mut(native_session_id) {
            meta.awaiting_approval = awaiting;
        }
    }

    /// 会话已在实际流程中出现(L1:如首次 MCP 问答登记),以默认元数据
    /// 登记进镜像使其进入列表;幂等,且此后一直保留(会话保留行为)。
    pub fn ensure_session(&self, native_session_id: &str) {
        self.sessions
            .lock()
            .entry(native_session_id.to_string())
            .or_default();
    }

    /// 读取会话元数据。
    pub fn get(&self, native_session_id: &str) -> Option<ZcodeSessionMeta> {
        self.sessions.lock().get(native_session_id).cloned()
    }

    /// 已发现会话列表(稳定维度)。
    pub fn known_sessions(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.sessions.lock().keys().cloned().collect();
        keys.sort();
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zcode::server::status_invoke_from_line;

    /// 官方事件 → fixture 输入 → 元数据迁移(04 §8.7 最小集)。
    #[test]
    fn official_event_fixtures_map_to_minimal_metadata() {
        let store = ObservationStore::new();

        // 会话发现。
        let session_start = status_invoke_from_line(
            r#"{"hook_event_name":"SessionStart","session_id":"s1","source":"startup","cwd":"/tmp/ws"}"#,
        )
        .unwrap();
        assert_eq!(
            store.apply(&session_start).as_deref(),
            Some("s1")
        );
        assert!(store.get("s1").unwrap().discovered);

        // 开始一轮。
        let prompt = status_invoke_from_line(
            r#"{"hook_event_name":"UserPromptSubmit","session_id":"s1","prompt":"..."}"#,
        )
        .unwrap();
        store.apply(&prompt);
        let meta = store.get("s1").unwrap();
        assert!(meta.turn_started && meta.turn_running);
        assert_eq!(meta.activity_summary(), "运行中(最后状态更新于 Hook 事件)");

        // 工具等待审批。
        let permission = status_invoke_from_line(
            r#"{"hook_event_name":"PermissionRequest","session_id":"s1","tool_name":"Bash","tool_input":{"command":"ls"}}"#,
        )
        .unwrap();
        store.apply(&permission);
        let meta = store.get("s1").unwrap();
        assert!(meta.awaiting_approval);
        assert_eq!(meta.activity_summary(), "等待工具审批");

        // 工具完成。
        let done = status_invoke_from_line(
            r#"{"hook_event_name":"PostToolUse","session_id":"s1","tool_name":"Bash","tool_use_id":"t1"}"#,
        )
        .unwrap();
        store.apply(&done);
        let meta = store.get("s1").unwrap();
        assert!(!meta.awaiting_approval);
        assert_eq!(meta.last_tool_ok, Some(true));

        // 工具失败。
        let failed = status_invoke_from_line(
            r#"{"hook_event_name":"PostToolUseFailure","session_id":"s1","tool_name":"Bash","error":"boom"}"#,
        )
        .unwrap();
        store.apply(&failed);
        assert_eq!(store.get("s1").unwrap().last_tool_ok, Some(false));

        // 本轮停止(≠ session 关闭)。
        let stop = status_invoke_from_line(
            r#"{"hook_event_name":"Stop","session_id":"s1","stop_hook_active":false}"#,
        )
        .unwrap();
        store.apply(&stop);
        let meta = store.get("s1").unwrap();
        assert!(!meta.turn_running);
        assert!(meta.turn_stopped);
        assert!(meta.discovered, "Stop 不清除会话发现");
        assert_eq!(meta.activity_summary(), "空闲");
    }

    /// 未知/不支持事件与缺 session 输入被忽略,不产生元数据。
    #[test]
    fn unknown_events_and_missing_session_ignored() {
        let store = ObservationStore::new();
        let notification = status_invoke_from_line(
            r#"{"hook_event_name":"Notification","session_id":"s2","message":"..."}"#,
        )
        .unwrap();
        assert_eq!(map_status_event(&notification), None);
        assert_eq!(store.apply(&notification), None);
        let no_session = status_invoke_from_line(
            r#"{"hook_event_name":"SessionStart","source":"startup"}"#,
        )
        .unwrap();
        assert_eq!(map_status_event(&no_session), Some(HookStatusEvent::SessionStart));
        assert_eq!(store.apply(&no_session), None, "无 session_id 不产生元数据");
        assert!(store.known_sessions().is_empty());
    }

    /// 多会话相互独立。
    #[test]
    fn sessions_are_independent() {
        let store = ObservationStore::new();
        let a = status_invoke_from_line(
            r#"{"hook_event_name":"UserPromptSubmit","session_id":"a"}"#,
        )
        .unwrap();
        let b = status_invoke_from_line(
            r#"{"hook_event_name":"Stop","session_id":"b"}"#,
        )
        .unwrap();
        store.apply(&a);
        store.apply(&b);
        assert!(store.get("a").unwrap().turn_running);
        assert!(!store.get("b").unwrap().turn_running);
        assert_eq!(store.known_sessions(), vec!["a".to_string(), "b".to_string()]);
    }
}
