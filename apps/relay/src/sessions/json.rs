//! proto → JSON 转换(HTTP API §27.3/§27.5;camelCase 与 Protobuf JSON 映射一致)。
//! 只做结构转换;不落盘、不记日志(§25.2/§25.3)。

use serde_json::{json, Value};

use agent_console_protocol::v1::item::Content;
use agent_console_protocol::v1::{
    ActiveTurnPhase, BackgroundCommandState, CompatibilityState, ControlMode, DeviceConnection,
    HistoryPage, LastTurnOutcome, OutputChannel, PendingAttentionKind, PlanStepStatus, QueueState,
    RuntimeSnapshot, SettingKind,
};

fn ts_value(ts: &Option<prost_types::Timestamp>) -> Value {
    ts.as_ref()
        .and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32))
        .map(|t| Value::String(t.with_timezone(&chrono::Utc).to_rfc3339()))
        .unwrap_or(Value::Null)
}

fn enum_name<T>(v: i32, f: impl Fn(T) -> &'static str) -> String
where
    T: TryFrom<i32>,
{
    T::try_from(v).map(f).unwrap_or("UNSPECIFIED").to_string()
}

fn turn_id(v: &Option<agent_console_protocol::v1::TurnId>) -> Value {
    match v {
        Some(t) => json!({ "id": t.id, "synthetic": t.synthetic }),
        None => Value::Null,
    }
}

fn item_id(v: &Option<agent_console_protocol::v1::ItemId>) -> Value {
    match v {
        Some(t) => json!({ "id": t.id, "synthetic": t.synthetic }),
        None => Value::Null,
    }
}

fn plan_json(plan: &Option<agent_console_protocol::v1::Plan>) -> Value {
    match plan {
        Some(p) => json!({
            "steps": p.steps.iter().map(|s| json!({
                "stepId": s.step_id,
                "title": s.title,
                "status": enum_name(s.status, |e: PlanStepStatus| match e {
                    PlanStepStatus::PlanStepPending => "PLAN_STEP_PENDING",
                    PlanStepStatus::PlanStepInProgress => "PLAN_STEP_IN_PROGRESS",
                    PlanStepStatus::PlanStepCompleted => "PLAN_STEP_COMPLETED",
                    _ => "PLAN_STEP_STATUS_UNSPECIFIED",
                }),
            })).collect::<Vec<_>>(),
        }),
        None => Value::Null,
    }
}

fn question_json(q: &agent_console_protocol::v1::PendingAttentionQuestion) -> Value {
    json!({
        "questionId": q.question_id,
        "title": q.title,
        "description": q.description,
        "options": q.options.iter().map(|o| json!({"optionId": o.option_id, "label": o.label})).collect::<Vec<_>>(),
        "allowMultiple": q.allow_multiple,
        "allowFreeText": q.allow_free_text,
        "turn": turn_id(&q.turn),
        "createdAt": ts_value(&q.created_at),
        "valid": q.valid,
    })
}

fn approval_json(a: &agent_console_protocol::v1::PendingAttentionApproval) -> Value {
    json!({
        "approvalId": a.approval_id,
        "riskDescription": a.risk_description,
        "requestedAction": a.requested_action,
        "decisions": a.decisions.iter().map(|d| json!({"decisionId": d.decision_id, "label": d.label})).collect::<Vec<_>>(),
        "scope": a.scope,
        "turn": turn_id(&a.turn),
        "createdAt": ts_value(&a.created_at),
        "valid": a.valid,
    })
}

fn capabilities_json(cap: &Option<agent_console_protocol::v1::CapabilitySnapshot>) -> Value {
    match cap {
        Some(c) => json!({
            "controlMode": enum_name(c.control_mode, |e: ControlMode| match e {
                ControlMode::FullControl => "CONTROL_MODE_FULL_CONTROL",
                ControlMode::LimitedControl => "CONTROL_MODE_LIMITED_CONTROL",
                ControlMode::ReadOnly => "CONTROL_MODE_READ_ONLY",
                ControlMode::Unavailable => "CONTROL_MODE_UNAVAILABLE",
                _ => "CONTROL_MODE_UNSPECIFIED",
            }),
            "compatibilityState": enum_name(c.compatibility_state, |e: CompatibilityState| match e {
                CompatibilityState::CompatibilityVerified => "COMPATIBILITY_VERIFIED",
                CompatibilityState::CompatibilityDegraded => "COMPATIBILITY_DEGRADED",
                CompatibilityState::CompatibilityUnsupported => "COMPATIBILITY_UNSUPPORTED",
                _ => "COMPATIBILITY_STATE_UNSPECIFIED",
            }),
            "codexVersion": c.codex_version,
            "supportedOperations": c.supported_operations.iter().map(|op| {
                agent_console_protocol::v1::Operation::try_from(*op)
                    .map(|o| o.as_str_name().to_string())
                    .unwrap_or_else(|_| format!("OPERATION_{op}"))
            }).collect::<Vec<_>>(),
            "settings": c.settings.iter().map(|s| json!({
                "optionId": s.option_id,
                "kind": enum_name(s.kind, |e: SettingKind| e.as_str_name()),
                "label": s.label,
                "currentValue": s.current_value,
                "availableValues": s.available_values.iter().map(|v| json!({"value": v.value, "label": v.label})).collect::<Vec<_>>(),
                "mutable": s.mutable,
            })).collect::<Vec<_>>(),
            "transferLimits": c.transfer_limits.as_ref().map(|t| json!({
                "textInlineMaxBytes": t.text_inline_max_bytes,
                "imageInlineMaxBytes": t.image_inline_max_bytes,
                "pdfRangeMaxBytes": t.pdf_range_max_bytes,
                "downloadMaxBytes": t.download_max_bytes,
                "uploadMaxBytes": t.upload_max_bytes,
                "maxConcurrentPerBrowser": t.max_concurrent_per_browser,
                "maxConcurrentPerDevice": t.max_concurrent_per_device,
                "previewableMimePrefixes": t.previewable_mime_prefixes,
            })),
        }),
        None => Value::Null,
    }
}

fn item_content_json(content: &Content) -> Value {
    match content {
        Content::UserMessage(m) => json!({ "userMessage": { "text": m.text } }),
        Content::AssistantMessage(m) => {
            json!({ "assistantMessage": { "text": m.text, "final": m.r#final } })
        }
        Content::ReasoningSummary(m) => json!({ "reasoningSummary": { "text": m.text } }),
        Content::Plan(p) => json!({ "plan": plan_json(&Some(p.clone())) }),
        Content::ToolCall(t) => json!({
            "toolCall": {
                "toolCallId": t.tool_call_id,
                "name": t.name,
                "status": enum_name(t.status, |e: PlanStepStatus| e.as_str_name()),
                "summary": t.summary,
                "durationMs": t.duration_ms,
            }
        }),
        Content::CommandStatus(c) => json!({
            "commandStatus": {
                "commandId": c.command_id,
                "state": enum_name(c.state, |e: BackgroundCommandState| e.as_str_name()),
                "startedAt": ts_value(&c.started_at),
                "finishedAt": ts_value(&c.finished_at),
                "durationMs": c.duration_ms,
            }
        }),
        Content::FileChange(f) => json!({
            "fileChange": {
                "files": f.files.iter().map(|fc| {
                    let kind = agent_console_protocol::v1::FileChangeKind::try_from(fc.kind)
                        .map(|k| k.as_str_name().to_string())
                        .unwrap_or_else(|_| format!("FILE_CHANGE_{}", fc.kind));
                    json!({
                        "path": fc.path,
                        "kind": kind,
                        "newPath": fc.new_path,
                        "inlineDiff": base64_bytes(&fc.inline_diff),
                        "diffTruncated": fc.diff_truncated,
                    })
                }).collect::<Vec<_>>(),
            }
        }),
        Content::SubagentStatus(s) => json!({
            "subagentStatus": {
                "subagentTurn": turn_id(&s.subagent_turn),
                "parentTurn": turn_id(&s.parent_turn),
                "label": s.label,
                "phase": enum_name(s.phase, |e: ActiveTurnPhase| e.as_str_name()),
                "outcome": enum_name(s.outcome, |e: LastTurnOutcome| e.as_str_name()),
            }
        }),
        Content::Question(q) => json!({ "question": question_json(q) }),
        Content::Approval(a) => json!({ "approval": approval_json(a) }),
        Content::TokenUsage(t) => json!({
            "tokenUsage": {
                "inputTokens": t.input_tokens,
                "outputTokens": t.output_tokens,
                "contextUsedTokens": t.context_used_tokens,
                "contextWindowTokens": t.context_window_tokens,
            }
        }),
    }
}

fn base64_bytes(b: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// RuntimeSnapshot → JSON(§11.1 数据接口 2/4)。
pub fn runtime_snapshot_json(s: &RuntimeSnapshot) -> Value {
    json!({
        "runtimeRevision": s.runtime_revision,
        "currentTurn": s.current_turn.as_ref().map(|t| json!({
            "turn": turn_id(&t.turn),
            "phase": enum_name(t.phase, |e: ActiveTurnPhase| e.as_str_name()),
            "startedAt": ts_value(&t.started_at),
        })),
        "plan": plan_json(&s.plan),
        "pendingQuestions": s.pending_questions.iter().map(question_json).collect::<Vec<_>>(),
        "pendingApprovals": s.pending_approvals.iter().map(approval_json).collect::<Vec<_>>(),
        "runningCommands": s.running_commands.iter().map(|c| json!({
            "commandId": c.command_id,
            "itemId": item_id(&c.item_id),
            "startedAt": ts_value(&c.started_at),
        })).collect::<Vec<_>>(),
        "backgroundCommands": s.background_commands.iter().map(|c| json!({
            "commandId": c.command_id,
            "itemId": item_id(&c.item_id),
            "state": enum_name(c.state, |e: BackgroundCommandState| e.as_str_name()),
            "startedAt": ts_value(&c.started_at),
            "finishedAt": ts_value(&c.finished_at),
        })).collect::<Vec<_>>(),
        "backgroundCommandCount": s.background_command_count,
        "queue": s.queue.as_ref().map(|q| json!({
            "state": enum_name(q.state, |e: QueueState| e.as_str_name()),
            "afterTurnId": turn_id(&q.after_turn_id),
            "acceptedRuntimeRevision": q.accepted_runtime_revision,
        })),
        "capabilities": capabilities_json(&s.capabilities),
        "recentOutputCursors": s.recent_output_cursors.iter().map(|c| json!({
            "itemId": item_id(&c.item_id),
            "revision": c.revision,
            "byteLength": c.byte_length,
            "isFinal": c.is_final,
            "channel": enum_name(c.channel, |e: OutputChannel| e.as_str_name()),
        })).collect::<Vec<_>>(),
    })
}

/// HistoryPage → JSON(§11.1 数据接口 3/4)。
pub fn history_page_json(h: &HistoryPage) -> Value {
    json!({
        "entries": h.entries.iter().map(|e| {
            let item = e.item.as_ref();
            json!({
                "turn": turn_id(&e.turn),
                "item": item.map(|i| {
                    let mut base = json!({
                        "itemId": item_id(&i.item_id),
                        "turn": turn_id(&i.turn),
                        "revision": i.revision,
                        "createdAt": ts_value(&i.created_at),
                    });
                    if let Some(content) = i.content.as_ref() {
                        base["content"] = item_content_json(content);
                    }
                    base
                }),
            })
        }).collect::<Vec<_>>(),
        "nextCursor": if h.next_cursor.is_empty() { Value::Null } else { Value::String(h.next_cursor.clone()) },
        "hasMore": h.has_more,
    })
}

/// 设备连接状态名(store 行 → 列表 JSON 共用)。
pub fn device_connection_name(v: i32) -> String {
    enum_name(v, |e: DeviceConnection| e.as_str_name())
}

pub fn pending_attention_kind_name(v: i32) -> String {
    enum_name(v, |e: PendingAttentionKind| e.as_str_name())
}
