//! conversationState / catalog item 的受控投影:原生形态 → 领域模型。
//!
//! 这里是"Desktop 原生 JSON 形态"知识的唯一归属点(§12 Adapter 完全封装
//! 变化面)。所有映射表集中在本文件;未知类型/键一律显式落入 Opaque 或
//! 未知计数,不猜测语义。
//!
//! 已验证 schema 事实来源(2026-09-04 只读探针,只记录类型/键名):
//! - `thread_history_1.sqlite`:item_type ∈ {userMessage, reasoning,
//!   agentMessage, commandExecution, fileChange, contextCompaction,
//!   webSearch, subAgentActivity, collabAgentToolCall, mcpToolCall,
//!   dynamicToolCall, imageView, imageGeneration, functionCallOutput, sleep};
//!   turn status ∈ {completed, interrupted, inProgress, failed}。
//! - item_json 顶层键(节选):commandExecution → {id, command, status,
//!   aggregatedOutput, exitCode, durationMs, cwd(绝不外传)};
//!   agentMessage → {id, text, phase ∈ {commentary, final_answer}};
//!   userMessage → {id, content};reasoning → {id, summary}(content 不读);
//!   fileChange → {id, changes, status};mcpToolCall → {id, server, tool,
//!   status, durationMs}。
//! - conversationState 顶层键清单见 docs/CODEX-IPC-PROTOCOL.md §6.3 与
//!   §10.3(0.153.1 增补)。未列入 KNOWN_STATE_KEYS 的顶层键忽略并计数
//!   (供 doctor/兼容文档)。
//!
//! 待真实验证项(记录于最终报告):conversationState 中问题/审批、后台命令
//! 的确切原生键;当前以宽容提取实现,fake owner 按本文件注释的形态回放。

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

use crate::domain::{
    ActiveTurnPhase, ApprovalDecision, BackgroundCommandState, CommandStatusState, FileChange,
    FileChangeKind, Item, ItemContent, ItemId, LastTurnOutcome, OutputText, PendingApproval,
    PendingQuestion, Plan, PlanStep, PlanStepStatus, QuestionOption, SettingKind, SettingOption,
    SettingValue, TurnId,
};

/// conversationState 已知顶层键(docs/CODEX-IPC-PROTOCOL.md §6.3 实测清单)。
pub const KNOWN_STATE_KEYS: &[&str] = &[
    "id",
    "title",
    "cwd",
    "hostId",
    "resumeState",
    "threadRuntimeStatus",
    "threadSource",
    "source",
    "ephemeral",
    "hasUnreadTurn",
    "latestModel",
    "latestReasoningEffort",
    "latestCollaborationMode",
    "latestTokenUsageInfo",
    "gitInfo",
    "currentPermissions",
    "turns",
    "turnHistory",
    "turnsPagination",
    "createdAt",
    "updatedAt",
    "recencyAt",
    // 0.153.1 复核新增顶层键(docs/CODEX-IPC-PROTOCOL.md §10.3,实测 39 键)。
    "agentNickname",
    "forkedFromId",
    "generatedTitle",
    "historyMode",
    "latestThreadSettings",
    "mode",
    "modelProvider",
    "previousTurnModel",
    "projectlessOutputDirectory",
    "requests",
    "rolloutPath",
    "sessionId",
    "shellEnvironmentPolicy",
    "sideConversation",
    "threadStartKind",
    "workspaceBrowserRoot",
    "workspaceKind",
    // Bridge 投影约定的扩展读取点(宽容:缺失即不投影)。
    "pendingQuestions",
    "pendingApprovals",
    "queuedFollowUps",
];

/// 未识别键计数:unknown 顶层键 → 出现次数(供 doctor/兼容文档)。
pub type UnknownKeyCounts = BTreeMap<String, u64>;

/// 统计 conversationState 中未知顶层键(§13.4 容忍未知字段)。
pub fn count_unknown_keys(state: &Value) -> UnknownKeyCounts {
    let mut counts = UnknownKeyCounts::new();
    if let Some(map) = state.as_object() {
        for key in map.keys() {
            if !KNOWN_STATE_KEYS.contains(&key.as_str()) {
                *counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }
    counts
}

// ---------------------------------------------------------------------------
// 原生 → 领域映射表
// ---------------------------------------------------------------------------

/// 原生 turn/threadRuntimeStatus 状态 → phase。
/// 已验证:turn status ∈ {completed, interrupted, inProgress, failed};
/// threadRuntimeStatus.type 为 6 字符枚举,实测含 "idle"/"active"。
pub fn map_phase(native_status: Option<&str>) -> ActiveTurnPhase {
    match native_status {
        Some("idle") | None => ActiveTurnPhase::Idle,
        Some("active") | Some("inProgress") => ActiveTurnPhase::Running,
        Some("finishing") => ActiveTurnPhase::Finishing,
        Some("completed") | Some("failed") | Some("interrupted") => ActiveTurnPhase::Idle,
        // 未知状态:无法证明运行中时保持 IDLE,并把该值交由调用方计数。
        Some(_) => ActiveTurnPhase::Idle,
    }
}

/// 原生 turn 终态 → LastTurnOutcome(§10.7)。
pub fn map_turn_outcome(native_status: Option<&str>) -> Option<LastTurnOutcome> {
    match native_status {
        Some("completed") => Some(LastTurnOutcome::Completed),
        Some("failed") => Some(LastTurnOutcome::Failed),
        Some("interrupted") => Some(LastTurnOutcome::Interrupted),
        _ => None,
    }
}

/// 命令 item 的原生 status → 条目状态。
pub fn map_command_status(native_status: Option<&str>) -> CommandStatusState {
    match native_status {
        Some("inProgress") | Some("running") => CommandStatusState::Running,
        Some("completed") => CommandStatusState::Completed,
        Some("failed") => CommandStatusState::Failed,
        Some("interrupted") | Some("userStopped") | Some("stopped") => {
            CommandStatusState::Interrupted
        }
        _ => CommandStatusState::Unknown,
    }
}

/// 命令条目状态 → 后台命令状态(§10.8)。
pub fn command_state_to_background(state: CommandStatusState) -> BackgroundCommandState {
    match state {
        CommandStatusState::Running => BackgroundCommandState::Running,
        CommandStatusState::Completed => BackgroundCommandState::Completed,
        CommandStatusState::Failed => BackgroundCommandState::Failed,
        CommandStatusState::Interrupted => BackgroundCommandState::Stopped,
        CommandStatusState::Unknown => BackgroundCommandState::Unknown,
    }
}

/// 命令条目状态是否终态(§13.3 结束校正的触发条件)。
pub fn command_status_is_terminal(state: CommandStatusState) -> bool {
    !matches!(
        state,
        CommandStatusState::Running | CommandStatusState::Unknown
    )
}

// ---------------------------------------------------------------------------
// item 解析
// ---------------------------------------------------------------------------

/// 从原生 item JSON 构造领域 Item(§9.3 的 11 类;未知类型 → Opaque)。
///
/// `native_type` 是原生 `type`/`item_type` 字符串;`turn` 为所属 turn;
/// `created_ms` 为毫秒时间戳(目录库提供,运行时可缺省)。
pub fn parse_item(
    native_type: &str,
    json: &Value,
    turn: Option<TurnId>,
    created_ms: Option<i64>,
) -> Item {
    let item_id = native_item_id(json, native_type);
    Item {
        item_id,
        turn,
        revision: json
            .get("revision")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        created_at: created_ms.and_then(ms_to_datetime),
        content: parse_item_content(native_type, json),
    }
}

/// 原生 item ID;缺失时生成 Adapter scoped 稳定 ID 并置 synthetic(§9.2)。
fn native_item_id(json: &Value, native_type: &str) -> ItemId {
    match json.get("id").and_then(Value::as_str) {
        Some(id) if !id.is_empty() => ItemId::native(id),
        // synthetic ID 用类型名 + 原生对象指针式命名空间;同批内稳定
        // (由调用方保证同一对象的 type 串稳定)。
        _ => ItemId::synthetic(format!("{native_type}:unnamed")),
    }
}

/// 内容解析:映射表见模块注释。
pub fn parse_item_content(native_type: &str, json: &Value) -> ItemContent {
    match native_type {
        "userMessage" => ItemContent::UserMessage {
            text: text_from(json.get("content")),
        },
        "agentMessage" => ItemContent::AssistantMessage {
            text: text_from(json.get("text")),
            // 已验证 phase ∈ {commentary, final_answer};缺失时按 final 处理
            // (目录库历史 agentMessage 主体为最终回复)。
            final_message: json
                .get("phase")
                .and_then(Value::as_str)
                .map(|p| p == "final_answer")
                .unwrap_or(true),
        },
        // §9.3:只读可见 reasoning summary;绝不读取 `content`(隐藏 CoT)。
        "reasoning" => ItemContent::ReasoningSummary {
            text: text_from(json.get("summary")),
        },
        "commandExecution" | "command" | "localShellCall" => ItemContent::CommandStatus {
            command_id: json
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            state: map_command_status(json.get("status").and_then(Value::as_str)),
            // 命令显示;cwd 等本机绝对路径字段一律不取(§12)。
            display: json
                .get("command")
                .and_then(Value::as_str)
                .map(OutputText::new),
            exit_code: json.get("exitCode").and_then(Value::as_i64),
            duration_ms: json.get("durationMs").and_then(Value::as_u64).unwrap_or(0),
        },
        "fileChange" => ItemContent::FileChange {
            changes: parse_file_changes(json.get("changes")),
        },
        "subAgentActivity" => {
            let status = json.get("status").and_then(Value::as_str);
            ItemContent::SubAgentStatus {
                subagent_turn: json
                    .get("agentThreadId")
                    .and_then(Value::as_str)
                    .map(TurnId::native),
                label: json
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("subagent")
                    .to_string(),
                phase: map_phase(status),
                outcome: map_turn_outcome(status),
            }
        }
        "mcpToolCall" | "collabAgentToolCall" | "dynamicToolCall" | "webSearch" => {
            let name = match native_type {
                "mcpToolCall" => format!(
                    "{}/{}",
                    json.get("server").and_then(Value::as_str).unwrap_or("?"),
                    json.get("tool").and_then(Value::as_str).unwrap_or("?")
                ),
                _ => native_type.to_string(),
            };
            ItemContent::ToolCall {
                tool_call_id: json
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                name,
                status: tool_status(json.get("status").and_then(Value::as_str)),
                // 原始 arguments 可能携带敏感参数原文,不进摘要;
                // 只保留用户可见事实(名称/状态/时长)。
                summary: None,
                duration_ms: json.get("durationMs").and_then(Value::as_u64).unwrap_or(0),
            }
        }
        "plan" | "updatePlan" => ItemContent::Plan {
            plan: parse_plan(json.get("steps")),
        },
        "question" => ItemContent::Question {
            question_id: json
                .get("id")
                .or_else(|| json.get("questionId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            title: text_from(json.get("title")),
        },
        "approval" => ItemContent::Approval {
            approval_id: json
                .get("id")
                .or_else(|| json.get("approvalId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            requested_action: text_from(json.get("requestedAction").or_else(|| json.get("action"))),
        },
        "tokenUsage" | "tokenCount" => ItemContent::TokenUsage {
            input_tokens: json.get("inputTokens").and_then(Value::as_u64).unwrap_or(0),
            output_tokens: json
                .get("outputTokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            context_used_tokens: json.get("contextUsedTokens").and_then(Value::as_u64),
            context_window_tokens: json.get("modelContextWindow").and_then(Value::as_u64),
        },
        // 其余已验证类型(contextCompaction/imageView/imageGeneration/
        // functionCallOutput/sleep)没有对应的 §9.3 领域类别:保留类型名,
        // 不崩溃、不猜测(§12)。未知的全新类型同样落入此处。
        other => ItemContent::Opaque {
            native_type: other.to_string(),
        },
    }
}

fn tool_status(native: Option<&str>) -> PlanStepStatus {
    match native {
        Some("inProgress") | Some("running") => PlanStepStatus::InProgress,
        Some("completed") => PlanStepStatus::Completed,
        _ => PlanStepStatus::Pending,
    }
}

/// 文件变化解析:只取 path/kind,不取 Diff 正文(正文走 OnDemandDetail)。
fn parse_file_changes(value: Option<&Value>) -> Vec<FileChange> {
    let Some(list) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|entry| {
            let path = entry
                .get("path")
                .or_else(|| entry.get("file"))
                .and_then(Value::as_str)?;
            let kind = match entry
                .get("kind")
                .or_else(|| entry.get("changeType"))
                .or_else(|| entry.get("type"))
                .and_then(Value::as_str)
            {
                Some("added") | Some("add") | Some("created") => FileChangeKind::Added,
                Some("modified") | Some("edited") | Some("updated") => FileChangeKind::Modified,
                Some("deleted") | Some("removed") => FileChangeKind::Deleted,
                Some("renamed") | Some("moved") => FileChangeKind::Renamed,
                _ => FileChangeKind::Unknown,
            };
            Some(FileChange {
                path: path.to_string(),
                change: kind,
            })
        })
        .collect()
}

/// Plan 步骤解析(宽容:字段名可能为 id/stepId、title/step、status)。
pub fn parse_plan(value: Option<&Value>) -> Plan {
    let Some(list) = value.and_then(Value::as_array) else {
        return Plan::default();
    };
    Plan {
        steps: list
            .iter()
            .map(|step| PlanStep {
                step_id: step
                    .get("id")
                    .or_else(|| step.get("stepId"))
                    .and_then(Value::as_str)
                    .map(String::from)
                    .unwrap_or_else(|| {
                        // synthetic 步骤 ID:按下标不可当稳定 ID(§9.2),
                        // 因此用标题内容派生;同标题稳定。
                        let title = step
                            .get("title")
                            .or_else(|| step.get("step"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        format!("plan-step:{}", title)
                    }),
                title: OutputText::new(
                    step.get("title")
                        .or_else(|| step.get("step"))
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                ),
                status: match step.get("status").and_then(Value::as_str) {
                    Some("inProgress") | Some("in-progress") | Some("running") => {
                        PlanStepStatus::InProgress
                    }
                    Some("completed") | Some("done") => PlanStepStatus::Completed,
                    _ => PlanStepStatus::Pending,
                },
            })
            .collect(),
    }
}

/// 文本块提取:字符串、{text} 对象或 [{type:"text",text} 数组]。
pub fn text_from(value: Option<&Value>) -> OutputText {
    match value {
        Some(Value::String(s)) => OutputText::new(s.clone()),
        Some(Value::Object(map)) => map
            .get("text")
            .and_then(Value::as_str)
            .map(OutputText::new)
            .unwrap_or_default(),
        Some(Value::Array(parts)) => {
            let mut joined = String::new();
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    joined.push_str(text);
                }
            }
            OutputText::new(joined)
        }
        _ => OutputText::default(),
    }
}

/// 命令输出的聚合文本字段(已验证键:`aggregatedOutput`)。
/// stdout/stderr 原生不可区分 → 通道一律 COMBINED,不猜测(§13.2)。
pub fn command_output(json: &Value) -> Option<&str> {
    json.get("aggregatedOutput").and_then(Value::as_str)
}

// ---------------------------------------------------------------------------
// conversationState 投影
// ---------------------------------------------------------------------------

/// conversationState 的轻量事实提取(快照/差分共用)。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionFacts {
    pub native_id: Option<String>,
    /// 标题原文(mapper 内部差分用;Debug 脱敏由领域类型保证)。
    pub title: Option<String>,
    /// 项目显示名 = cwd 尾段(绝对路径绝不出 adapter,§12)。
    pub project_display: Option<String>,
    pub git_branch: Option<String>,
    pub runtime_status: Option<String>,
    pub latest_model: Option<String>,
    pub latest_reasoning_effort: Option<String>,
    pub collaboration_mode: Option<String>,
    pub approval_policy: Option<String>,
    pub service_tier: Option<String>,
}

/// 从 conversationState 提取会话事实。
pub fn extract_facts(state: &Value) -> SessionFacts {
    let cwd = state.get("cwd").and_then(Value::as_str);
    SessionFacts {
        native_id: state.get("id").and_then(Value::as_str).map(String::from),
        title: state.get("title").and_then(Value::as_str).map(String::from),
        project_display: cwd.and_then(project_display_from_cwd),
        git_branch: state
            .get("gitInfo")
            .and_then(|g| g.get("branch"))
            .and_then(Value::as_str)
            .map(String::from),
        runtime_status: state
            .get("threadRuntimeStatus")
            .and_then(|r| r.get("type"))
            .and_then(Value::as_str)
            .map(String::from),
        latest_model: state
            .get("latestModel")
            .and_then(Value::as_str)
            .map(String::from),
        latest_reasoning_effort: state
            .get("latestReasoningEffort")
            .and_then(Value::as_str)
            .map(String::from),
        collaboration_mode: state
            .get("latestCollaborationMode")
            .and_then(|m| m.get("mode"))
            .and_then(Value::as_str)
            .map(String::from),
        approval_policy: state
            .get("currentPermissions")
            .and_then(|p| p.get("approvalPolicy"))
            .and_then(Value::as_str)
            .map(String::from),
        service_tier: state
            .get("latestCollaborationMode")
            .and_then(|m| m.get("settings"))
            .and_then(|s| s.get("serviceTier"))
            .and_then(Value::as_str)
            .map(String::from),
    }
}

/// cwd → 项目显示名(尾段目录名);绝不把绝对路径带出 adapter(§12)。
pub fn project_display_from_cwd(cwd: &str) -> Option<String> {
    let trimmed = cwd.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed.rsplit('/').next().map(String::from)
}

/// 当前(或最近)turn 数组:`turns[]`。
pub fn extract_turns(state: &Value) -> Vec<&Value> {
    state
        .get("turns")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .collect()
}

/// turn 的原生 turnId(§9.2:缺失时由 mapper 生成 synthetic)。
pub fn turn_id_of(turn: &Value, index: usize) -> TurnId {
    match turn.get("turnId").and_then(Value::as_str) {
        Some(id) if !id.is_empty() => TurnId::native(id),
        _ => TurnId::synthetic(format!("turn:{index}")),
    }
}

/// turn 的原生 status 字符串。
pub fn turn_status_of(turn: &Value) -> Option<&str> {
    turn.get("status").and_then(Value::as_str)
}

/// turn 的 items 数组。
pub fn turn_items_of(turn: &Value) -> Vec<&Value> {
    turn.get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .collect()
}

/// 问题提取(§16.3):`pendingQuestions[]`。
/// 每项形态(投影约定,fake owner 按此回放;真实验证待记录):
/// `{ id, title, description, options: [{id, label}], allowMultiple, allowFreeText, turnId? }`。
pub fn extract_questions(state: &Value) -> Vec<PendingQuestion> {
    let Some(list) = state.get("pendingQuestions").and_then(Value::as_array) else {
        return Vec::new();
    };
    list.iter()
        .map(|q| PendingQuestion {
            question_id: q
                .get("id")
                .or_else(|| q.get("questionId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            title: text_from(q.get("title")),
            description: text_from(q.get("description")),
            options: q
                .get("options")
                .and_then(Value::as_array)
                .map(|opts| {
                    opts.iter()
                        .filter_map(|o| {
                            Some(QuestionOption {
                                option_id: o.get("id").and_then(Value::as_str)?.to_string(),
                                label: text_from(o.get("label")),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
            allow_multiple: q
                .get("allowMultiple")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            allow_free_text: q
                .get("allowFreeText")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            turn: q.get("turnId").and_then(Value::as_str).map(TurnId::native),
            created_at: q
                .get("createdAtMs")
                .and_then(Value::as_i64)
                .and_then(ms_to_datetime),
            valid: q.get("valid").and_then(Value::as_bool).unwrap_or(true),
        })
        .collect()
}

/// 审批提取(§16.4):`pendingApprovals[]`。
/// 每项形态(投影约定,fake owner 按此回放;真实验证待记录):
/// `{ id, riskDescription, requestedAction, decisions: [{id, label}], scope, turnId? }`。
pub fn extract_approvals(state: &Value) -> Vec<PendingApproval> {
    let Some(list) = state.get("pendingApprovals").and_then(Value::as_array) else {
        return Vec::new();
    };
    list.iter()
        .map(|a| PendingApproval {
            approval_id: a
                .get("id")
                .or_else(|| a.get("approvalId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            risk_description: text_from(a.get("riskDescription")),
            requested_action: text_from(a.get("requestedAction").or_else(|| a.get("action"))),
            decisions: a
                .get("decisions")
                .and_then(Value::as_array)
                .map(|ds| {
                    ds.iter()
                        .filter_map(|d| {
                            Some(ApprovalDecision {
                                decision_id: d.get("id").and_then(Value::as_str)?.to_string(),
                                label: text_from(d.get("label")),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
            scope: a.get("scope").and_then(Value::as_str).map(OutputText::new),
            turn: a.get("turnId").and_then(Value::as_str).map(TurnId::native),
            created_at: a
                .get("createdAtMs")
                .and_then(Value::as_i64)
                .and_then(ms_to_datetime),
            valid: a.get("valid").and_then(Value::as_bool).unwrap_or(true),
        })
        .collect()
}

/// 设置动态提取(§16.1):模型/思考深度/服务等级/权限/协作模式。
/// 只投影 snapshot 实际携带的键;可用值列表原生未提供时为空(不硬编码)。
///
/// 写能力收敛(0.153.1 真机核实:snapshot 不携带任何动态可选值列表):
/// - 产品仅对 ReasoningEffort 做过逐项真机验证,其余四类恒不可写
///   (`mutable=false`);仅 effort 的可写性随 idle/运行相位移。
/// - effort 的可选值数据驱动读取(见 [`effort_available_values`]):真机
///   缺省为空 → 命令层据此拒绝写入(设置写整体关闭),绝不硬编码档位。
pub fn extract_settings(state: &Value, phase: ActiveTurnPhase) -> Vec<SettingOption> {
    let facts = extract_facts(state);
    let mut out = Vec::new();
    let mut push = |option_id: &'static str,
                    kind: SettingKind,
                    current: Option<String>,
                    available: Vec<SettingValue>,
                    mutable: bool| {
        if let Some(current) = current {
            out.push(SettingOption {
                option_id: option_id.to_string(),
                kind,
                label: OutputText::new(option_id),
                current_value: Some(current),
                available_values: available,
                mutable,
            });
        }
    };
    let model = facts
        .latest_model
        .clone()
        .or_else(|| collaboration_setting(state, "model"));
    // 产品仅 ReasoningEffort 逐项真机验证;其余类别不可写(恒 mutable=false)。
    push("model", SettingKind::Model, model, Vec::new(), false);
    push(
        "effort",
        SettingKind::ReasoningEffort,
        facts.latest_reasoning_effort.clone(),
        effort_available_values(state),
        phase == ActiveTurnPhase::Idle,
    );
    push(
        "serviceTier",
        SettingKind::ServiceTier,
        facts.service_tier.clone(),
        Vec::new(),
        false,
    );
    push(
        "approvalPolicy",
        SettingKind::PermissionMode,
        // 默认权限 = Codex 原生"帮我批准"(approvalPolicy 原生值透传,§16.2)。
        facts.approval_policy.clone(),
        Vec::new(),
        false,
    );
    push(
        "collaborationMode",
        SettingKind::CollaborationMode,
        facts.collaboration_mode.clone(),
        Vec::new(),
        false,
    );
    out
}

/// effort 的动态可选值(完全可选、数据驱动):读
/// `latestCollaborationMode.settings.effortAvailableValues`(字符串数组)。
///
/// 0.153.1 原生 conversationState 不提供该字段 → 恒返回空 Vec,命令层据此
/// 拒绝写入;Desktop 未来提供时无需改码即数据驱动生效。字段缺失、类型不符
/// 或元素含非字符串一律视为解析失败 → 空 Vec(不猜测、不硬编码档位)。
fn effort_available_values(state: &Value) -> Vec<SettingValue> {
    let Some(list) = state
        .get("latestCollaborationMode")
        .and_then(|m| m.get("settings"))
        .and_then(|s| s.get("effortAvailableValues"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(list.len());
    for entry in list {
        let Some(text) = entry.as_str() else {
            return Vec::new();
        };
        out.push(SettingValue {
            value: text.to_string(),
            label: OutputText::new(text),
        });
    }
    out
}

fn collaboration_setting(state: &Value, key: &str) -> Option<String> {
    state
        .get("latestCollaborationMode")
        .and_then(|m| m.get("settings"))
        .and_then(|s| s.get(key))
        .and_then(Value::as_str)
        .map(String::from)
}

// ---------------------------------------------------------------------------
// Immer patches
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PatchError {
    #[error("patch is not an object")]
    NotAnObject,
    #[error("unsupported patch op: {0}")]
    UnsupportedOp(String),
    #[error("patch path segment is invalid at {0}")]
    InvalidPath(usize),
}

/// 应用一批 Immer patch(`{op, path[], value}`;path 是数组而非 JSON Pointer,
/// docs/CODEX-IPC-PROTOCOL.md §6.3)到状态树。
///
/// 只实现已验证的 add/replace/remove;其他 op 返回错误 → 上层触发
/// snapshot 补偿(§12:未识别的关键 patch 触发 snapshot)。
pub fn apply_immer_patches(state: &mut Value, patches: &[Value]) -> Result<(), PatchError> {
    for patch in patches {
        apply_one(state, patch)?;
    }
    Ok(())
}

fn apply_one(state: &mut Value, patch: &Value) -> Result<(), PatchError> {
    let obj = patch.as_object().ok_or(PatchError::NotAnObject)?;
    let op = obj
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| PatchError::UnsupportedOp("<missing>".to_string()))?;
    let path = obj
        .get("path")
        .and_then(Value::as_array)
        .ok_or(PatchError::InvalidPath(0))?;
    if path.is_empty() {
        return Err(PatchError::InvalidPath(0));
    }
    // 定位父节点;路径上的中间节点缺失视为无效 patch(交由上层补偿)。
    let mut cursor = state;
    for (i, segment) in path.iter().enumerate().take(path.len() - 1) {
        cursor = descend_mut(cursor, segment).ok_or(PatchError::InvalidPath(i))?;
    }
    let last = path.last().expect("path non-empty checked");
    match op {
        "add" | "replace" => {
            let value = obj.get("value").cloned().unwrap_or(Value::Null);
            match descend_mut(cursor, last) {
                Some(slot) => *slot = value,
                None => {
                    // add 到缺失位置:追加到父容器(数组 push / 对象插入)。
                    if let Some(map) = cursor.as_object_mut() {
                        if let Some(key) = last.as_str() {
                            map.insert(key.to_string(), value);
                            return Ok(());
                        }
                    }
                    if let Some(list) = cursor.as_array_mut() {
                        let idx = last
                            .as_u64()
                            .or_else(|| last.as_str().and_then(|s| s.parse::<u64>().ok()));
                        if let Some(idx) = idx {
                            let at = (idx as usize).min(list.len());
                            list.insert(at, value);
                            return Ok(());
                        }
                    }
                    return Err(PatchError::InvalidPath(path.len() - 1));
                }
            }
        }
        "remove" => {
            if let Some(map) = cursor.as_object_mut() {
                if let Some(key) = last.as_str() {
                    map.remove(key);
                    return Ok(());
                }
            }
            if let Some(list) = cursor.as_array_mut() {
                if let Some(idx) = last.as_u64() {
                    if (idx as usize) < list.len() {
                        list.remove(idx as usize);
                        return Ok(());
                    }
                }
            }
            // remove 不存在的目标:幂等成功。
        }
        other => return Err(PatchError::UnsupportedOp(other.to_string())),
    }
    Ok(())
}

fn descend_mut<'a>(node: &'a mut Value, segment: &Value) -> Option<&'a mut Value> {
    if let Some(key) = segment.as_str() {
        // 先按对象键;未命中再按数字字符串下标解析(Immer 数组形态)。
        if node.is_object() {
            return node.get_mut(key);
        }
        if let Ok(idx) = key.parse::<usize>() {
            return node.get_mut(idx);
        }
        return None;
    }
    if let Some(idx) = segment.as_u64() {
        return node.get_mut(idx as usize);
    }
    None
}

// ---------------------------------------------------------------------------
// 时间与工具
// ---------------------------------------------------------------------------

/// 毫秒时间戳 → DateTime(容错:秒级时间戳 >10^12 视为 ms)。
pub fn ms_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    if ms <= 0 {
        return None;
    }
    Utc.timestamp_millis_opt(ms).single()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_top_level_keys_counted() {
        let state = json!({
            "id": "01A",
            "title": "t",
            "brandNewKey": {"x": 1},
            "anotherUnknown": true
        });
        let counts = count_unknown_keys(&state);
        assert_eq!(counts.get("brandNewKey"), Some(&1));
        assert_eq!(counts.get("anotherUnknown"), Some(&1));
        assert!(!counts.contains_key("id"));
    }

    #[test]
    fn project_display_is_tail_only() {
        assert_eq!(
            project_display_from_cwd("/Users/x/projects/demo-app"),
            Some("demo-app".to_string())
        );
        assert_eq!(project_display_from_cwd("/"), None);
    }

    #[test]
    fn immer_patches_apply_add_replace_remove() {
        let mut state = json!({"turns": [{"turnId": "t1", "status": "inProgress"}], "title": "a"});
        apply_immer_patches(
            &mut state,
            &[
                json!({"op": "replace", "path": ["title"], "value": "b"}),
                json!({"op": "replace", "path": ["turns", 0, "status"], "value": "completed"}),
                json!({"op": "add", "path": ["pendingQuestions"], "value": []}),
            ],
        )
        .unwrap();
        assert_eq!(state["title"], "b");
        assert_eq!(state["turns"][0]["status"], "completed");
        assert_eq!(state["pendingQuestions"], json!([]));
        apply_immer_patches(&mut state, &[json!({"op": "remove", "path": ["title"]})]).unwrap();
        assert!(state.get("title").is_none());
    }

    #[test]
    fn immer_unknown_op_errors() {
        let mut state = json!({});
        let err = apply_immer_patches(
            &mut state,
            &[json!({"op": "copy", "path": ["a"], "from": ["b"]})],
        )
        .unwrap_err();
        assert!(matches!(err, PatchError::UnsupportedOp(_)));
    }

    #[test]
    fn item_parsing_maps_verified_types() {
        let cmd = json!({
            "id": "c1", "command": "cargo test", "status": "completed",
            "aggregatedOutput": "ok", "exitCode": 0, "durationMs": 12
        });
        let item = parse_item("commandExecution", &cmd, Some(TurnId::native("t1")), None);
        match &item.content {
            ItemContent::CommandStatus {
                command_id,
                state,
                display,
                exit_code,
                duration_ms,
            } => {
                assert_eq!(command_id, "c1");
                assert_eq!(*state, CommandStatusState::Completed);
                assert_eq!(display.as_ref().map(|d| d.as_str()), Some("cargo test"));
                assert_eq!(*exit_code, Some(0));
                assert_eq!(*duration_ms, 12);
            }
            other => panic!("unexpected content: {other:?}"),
        }

        // reasoning:只取 summary,不读 content(§9.3)。
        let reasoning = json!({"id": "r1", "summary": "visible", "content": "hidden-cot"});
        let item = parse_item("reasoning", &reasoning, None, None);
        match &item.content {
            ItemContent::ReasoningSummary { text } => {
                assert_eq!(text.as_str(), "visible");
            }
            other => panic!("unexpected content: {other:?}"),
        }
        let debug = format!("{item:?}");
        assert!(!debug.contains("hidden-cot"), "Debug 不得泄露正文: {debug}");

        // 未知类型 → Opaque 保留类型名。
        let item = parse_item("brandNewThing", &json!({"id": "x"}), None, None);
        assert_eq!(
            item.content,
            ItemContent::Opaque {
                native_type: "brandNewThing".to_string()
            }
        );
    }

    #[test]
    fn item_without_id_becomes_synthetic() {
        let item = parse_item("userMessage", &json!({"content": "hi"}), None, None);
        assert!(item.item_id.synthetic);
        assert_eq!(
            item.content,
            ItemContent::UserMessage {
                text: OutputText::new("hi")
            }
        );
    }

    #[test]
    fn effort_writability_follows_phase_and_values_are_data_driven() {
        let state = json!({
            "latestModel": "m-fixture",
            "latestReasoningEffort": "high",
            "latestCollaborationMode": {
                "mode": "fixture",
                "settings": {
                    "serviceTier": "default",
                    "effortAvailableValues": ["high", "max"]
                }
            },
            "currentPermissions": {"approvalPolicy": "untrusted"}
        });
        let idle = extract_settings(&state, ActiveTurnPhase::Idle);
        let effort = idle
            .iter()
            .find(|o| o.kind == SettingKind::ReasoningEffort)
            .expect("effort option");
        assert!(effort.mutable);
        assert_eq!(
            effort
                .available_values
                .iter()
                .map(|v| v.value.as_str())
                .collect::<Vec<_>>(),
            vec!["high", "max"]
        );
        // 其余类别投影存在但恒不可写(产品仅 effort 逐项真机验证)。
        for kind in [
            SettingKind::Model,
            SettingKind::ServiceTier,
            SettingKind::PermissionMode,
            SettingKind::CollaborationMode,
        ] {
            let option = idle
                .iter()
                .find(|o| o.kind == kind)
                .unwrap_or_else(|| panic!("{kind:?} option"));
            assert!(!option.mutable, "{kind:?} 恒不可写");
            assert!(option.available_values.is_empty());
        }
        // 运行中:effort 同步锁定。
        let running = extract_settings(&state, ActiveTurnPhase::Running);
        let effort = running
            .iter()
            .find(|o| o.kind == SettingKind::ReasoningEffort)
            .expect("effort option");
        assert!(!effort.mutable);
    }

    #[test]
    fn effort_available_values_absent_or_malformed_yield_empty() {
        // 真机 0.153.1 形状:无该字段 → 空(不硬编码)。
        let idle = extract_settings(
            &json!({"latestReasoningEffort": "high"}),
            ActiveTurnPhase::Idle,
        );
        let effort = idle
            .iter()
            .find(|o| o.kind == SettingKind::ReasoningEffort)
            .expect("effort option");
        assert!(effort.available_values.is_empty());

        // 元素含非字符串 → 解析失败,整体为空。
        let idle = extract_settings(
            &json!({
                "latestReasoningEffort": "high",
                "latestCollaborationMode": {
                    "settings": {"effortAvailableValues": ["high", 3]}
                }
            }),
            ActiveTurnPhase::Idle,
        );
        let effort = idle
            .iter()
            .find(|o| o.kind == SettingKind::ReasoningEffort)
            .expect("effort option");
        assert!(effort.available_values.is_empty());
    }
}
