//! mapper/projection 集成测试(§13.4 必测场景,合成 conversationState):
//! 连续输出编号无缺号、COMBINED 通道、无换行小片段、突发大输出 ≤64KiB 且
//! UTF-8 边界、命令失败/停止/interrupt 终态、revision 跳跃 → ResyncNeeded、
//! final snapshot 完整替换、final 不可读标记。

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use bridge::adapter::codex::mapper::{PatchOutcome, SessionMapper};
use bridge::adapter::codex::projection::{apply_immer_patches, count_unknown_keys};
use bridge::domain::{
    ActiveTurnPhase, DomainEvent, ItemContent, LastTurnOutcome, OutputChannel, SessionKey,
};

const CONV: &str = "conv-mapper-1";
const OUTPUT_PATH: &[&str] = &["turns", "0", "items", "0", "aggregatedOutput"];

/// 构造最小 conversationState:1 个 inProgress turn + 1 条命令 item。
fn initial_state() -> Value {
    json!({
        "id": CONV,
        "title": "fixture-mapper",
        "cwd": "/tmp/fixture-mapper",
        "hostId": "local",
        "threadRuntimeStatus": {"type": "active", "activeFlags": []},
        "latestModel": "gpt-5.3-fixture",
        "latestReasoningEffort": "medium",
        "gitInfo": {"branch": "fixture-branch"},
        "currentPermissions": {"approvalPolicy": "untrusted"},
        "turns": [{
            "turnId": "turn-m1",
            "status": "inProgress",
            "items": [{
                "id": "item-cmd-1",
                "type": "commandExecution",
                "command": "fixture-cmd",
                "status": "inProgress",
                "aggregatedOutput": "",
            }],
        }],
        "pendingQuestions": [],
        "pendingApprovals": [],
    })
}

fn output_patch(value: Value) -> Vec<Value> {
    let mut path: Vec<Value> = OUTPUT_PATH.iter().map(|s| json!(s)).collect();
    // 数字段需要数值形式。
    path[1] = json!(0);
    path[3] = json!(0);
    vec![json!({"op": "replace", "path": path, "value": value})]
}

fn flush_all(mapper: &mut SessionMapper) -> Vec<DomainEvent> {
    mapper.flush_due_outputs(Instant::now() + Duration::from_secs(1))
}

fn append_events(events: &[DomainEvent]) -> Vec<(u64, &str)> {
    events
        .iter()
        .filter_map(|e| match e {
            DomainEvent::OutputAppend {
                expected_offset,
                bytes,
                ..
            } => Some((
                *expected_offset,
                std::str::from_utf8(bytes.as_bytes()).unwrap(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn numbered_output_1_to_n_has_no_gaps() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    let events = mapper.apply_snapshot(1, &initial_state());
    // 首个快照:生命周期 + summary;不重放历史。
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::TurnLifecycle {
            phase: ActiveTurnPhase::Running,
            ..
        }
    )));

    let mut all_events = Vec::new();
    let mut revision = 1u64;
    for k in 1..=10u32 {
        let content = (1..=k).map(|i| format!("{i}\n")).collect::<String>();
        let base = revision;
        revision += 1;
        all_events.extend(
            mapper
                .apply_patches(base, revision, &output_patch(json!(content)))
                .apply_events(),
        );
    }
    all_events.extend(flush_all(&mut mapper));

    let appends = append_events(&all_events);
    // 拼接结果必须严格等于 1..10,无缺号(§13.4)。
    let mut concatenated = String::new();
    let mut expected_offset = 0u64;
    for (offset, text) in &appends {
        assert_eq!(*offset, expected_offset, "expected_offset 必须连续");
        concatenated.push_str(text);
        expected_offset += text.len() as u64;
    }
    let expected: String = (1..=10).map(|i| format!("{i}\n")).collect();
    assert_eq!(concatenated, expected);
}

/// apply_patches 结果的辅助展开。
trait PatchOutcomeExt {
    fn apply_events(self) -> Vec<DomainEvent>;
}
impl PatchOutcomeExt for PatchOutcome {
    fn apply_events(self) -> Vec<DomainEvent> {
        match self {
            PatchOutcome::Applied { events } => events,
            PatchOutcome::ResyncNeeded => panic!("unexpected resync"),
        }
    }
}

#[test]
fn interleaved_output_is_combined_channel() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    // stdout/stderr 原生不可区分:所有增量必须 COMBINED,不猜通道(§13.2)。
    let outcome = mapper.apply_patches(
        1,
        2,
        &output_patch(json!("stdout-line\nstderr-line\nstdout-2\n")),
    );
    let mut events = outcome.apply_events();
    events.extend(flush_all(&mut mapper));
    for event in &events {
        match event {
            DomainEvent::OutputAppend { channel, .. }
            | DomainEvent::OutputReplace { channel, .. }
            | DomainEvent::OutputFinal { channel, .. } => {
                assert_eq!(*channel, OutputChannel::Combined);
            }
            _ => {}
        }
    }
    let appends = append_events(&events);
    let joined: String = appends.iter().map(|(_, t)| *t).collect();
    assert_eq!(joined, "stdout-line\nstderr-line\nstdout-2\n");
}

#[test]
fn small_fragments_without_newline_are_merged_in_order() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    let mut all = Vec::new();
    let mut revision = 1u64;
    for fragment in ["a", "b", "c", "d"] {
        let mut current = String::new();
        // 每个 fragment 是累计值的一步;不换行。
        current.push_str(&fragments_upto(&["a", "b", "c", "d"], fragment));
        revision += 1;
        all.extend(
            mapper
                .apply_patches(revision - 1, revision, &output_patch(json!(current)))
                .apply_events(),
        );
    }
    all.extend(flush_all(&mut mapper));
    let joined: String = append_events(&all).iter().map(|(_, t)| *t).collect();
    assert_eq!(joined, "abcd");
}

fn fragments_upto(all: &[&str], last: &str) -> String {
    let mut out = String::new();
    for item in all {
        out.push_str(item);
        if *item == last {
            break;
        }
    }
    out
}

#[test]
fn burst_output_chunks_at_64kib_utf8_boundary() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    // 突发大输出:~200KiB 的多字节内容。
    let big = "中文输出内容-".repeat(20_000);
    assert!(big.len() > 3 * 64 * 1024);
    let mut all = mapper
        .apply_patches(1, 2, &output_patch(json!(big.clone())))
        .apply_events();
    all.extend(flush_all(&mut mapper));

    let mut rebuilt = Vec::new();
    let mut expected_offset = 0u64;
    for event in &all {
        if let DomainEvent::OutputAppend {
            expected_offset: offset,
            bytes,
            ..
        } = event
        {
            assert_eq!(*offset, expected_offset, "分块必须连续");
            assert!(bytes.len() <= 64 * 1024, "单块不得超过 64KiB(§26.3)");
            rebuilt.extend_from_slice(bytes.as_bytes());
            expected_offset += bytes.len() as u64;
        }
    }
    assert_eq!(rebuilt.len(), big.len());
    assert_eq!(
        std::str::from_utf8(&rebuilt).unwrap(),
        big,
        "分块必须在 UTF-8 边界切分且内容无损"
    );
}

#[test]
fn command_failure_terminal_state_is_reported() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    // 输出两行后命令失败。
    let _ = mapper
        .apply_patches(1, 2, &output_patch(json!("step-1\nstep-2\n")))
        .apply_events();
    let mut all = mapper
        .apply_patches(
            2,
            3,
            &[
                json!({"op": "replace", "path": ["turns", 0, "items", 0, "status"], "value": "failed"}),
                json!({"op": "replace", "path": ["turns", 0, "items", 0, "exitCode"], "value": 1}),
                json!({"op": "replace", "path": ["turns", 0, "status"], "value": "failed"}),
            ],
        )
        .apply_events();
    all.extend(flush_all(&mut mapper));

    // 输出定稿(§13.3)。
    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::OutputFinal {
            byte_length: 14,
            final_unavailable: false,
            ..
        }
    )));
    // turn 生命周期:Idle + Failed(§10.7:只描述上一轮)。
    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::TurnLifecycle {
            phase: ActiveTurnPhase::Idle,
            outcome: Some(LastTurnOutcome::Failed),
            ..
        }
    )));
    // 摘要携带 last_turn_outcome。
    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::SessionSummaryChanged { summary } if summary.last_turn_outcome == LastTurnOutcome::Failed
    )));
}

#[test]
fn turn_interrupt_yields_interrupted_outcome() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    let mut all = mapper
        .apply_patches(
            1,
            2,
            &[json!({"op": "replace", "path": ["turns", 0, "status"], "value": "interrupted"}),
              json!({"op": "replace", "path": ["turns", 0, "items", 0, "status"], "value": "interrupted"})],
        )
        .apply_events();
    all.extend(flush_all(&mut mapper));
    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::TurnLifecycle {
            phase: ActiveTurnPhase::Idle,
            outcome: Some(LastTurnOutcome::Interrupted),
            ..
        }
    )));
}

#[test]
fn revision_jump_returns_resync_needed() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(5, &initial_state());
    // baseRevision 与当前 revision 不吻合 → ResyncNeeded(§14)。
    let outcome = mapper.apply_patches(2, 6, &output_patch(json!("x\n")));
    assert_eq!(outcome, PatchOutcome::ResyncNeeded);
    // 未知 op → ResyncNeeded,状态不被破坏。
    let outcome =
        mapper.apply_patches(5, 6, &[json!({"op": "copy", "path": ["a"], "from": ["b"]})]);
    assert_eq!(outcome, PatchOutcome::ResyncNeeded);
    assert_eq!(mapper.runtime_revision(), Some(5));
}

#[test]
fn final_snapshot_with_rewritten_output_replaces_preview() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    let _ = mapper
        .apply_patches(1, 2, &output_patch(json!("1\n2\n3\n")))
        .apply_events();
    let _ = flush_all(&mut mapper);

    // 权威最终快照:输出被改写(非前缀扩展)→ 完整替换 + 定稿(§13.3)。
    let mut state = initial_state();
    state["turns"][0]["status"] = json!("completed");
    state["turns"][0]["items"][0]["status"] = json!("completed");
    state["turns"][0]["items"][0]["aggregatedOutput"] = json!("rewritten-authoritative\n");
    let all = mapper.apply_snapshot(3, &state);

    let replaced = all.iter().any(|e| {
        matches!(
            e,
            DomainEvent::OutputReplace { bytes, .. }
                if std::str::from_utf8(bytes.as_bytes()).unwrap() == "rewritten-authoritative\n"
        )
    });
    assert!(replaced, "缺口必须走 OutputReplace: {all:?}");
    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::OutputFinal {
            byte_length: 24,
            final_unavailable: false,
            ..
        }
    )));
}

#[test]
fn missing_authoritative_output_marks_final_unavailable() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    let _ = mapper
        .apply_patches(1, 2, &output_patch(json!("preview-only\n")))
        .apply_events();
    let _ = flush_all(&mut mapper);

    // 权威快照中 aggregatedOutput 字段缺失 → FINAL_OUTPUT_UNAVAILABLE 标记
    // (§13.3:不能把预览冒充完整结果)。
    let mut state = initial_state();
    state["turns"][0]["status"] = json!("completed");
    state["turns"][0]["items"][0]["status"] = json!("completed");
    state["turns"][0]["items"][0]
        .as_object_mut()
        .unwrap()
        .remove("aggregatedOutput");
    let all = mapper.apply_snapshot(3, &state);

    assert!(all.iter().any(|e| matches!(
        e,
        DomainEvent::OutputFinal {
            final_unavailable: true,
            ..
        }
    )));
}

#[test]
fn pending_attention_lifecycle_and_unknown_key_counting() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());

    // 注入问题(§16.3 全字段)。
    let mut state = initial_state();
    state["pendingQuestions"] = json!([{
        "id": "q-1",
        "title": "fixture question",
        "description": "fixture description",
        "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
        "allowMultiple": false,
        "allowFreeText": false,
        "turnId": "turn-m1",
    }]);
    let added = mapper.apply_snapshot(2, &state);
    assert!(added.iter().any(|e| matches!(
        e,
        DomainEvent::PendingAttentionAdded { attention }
            if attention.native_id() == "q-1"
    )));
    let snapshot = mapper.runtime_snapshot().unwrap();
    assert_eq!(snapshot.pending_questions.len(), 1);
    assert_eq!(snapshot.pending_questions[0].options.len(), 2);
    assert_eq!(
        mapper.session_summary().pending_attention_count,
        1,
        "摘要角标(§11.1)"
    );

    // 回答后移除。
    state["pendingQuestions"] = json!([]);
    let removed = mapper.apply_snapshot(3, &state);
    assert!(removed.iter().any(|e| matches!(
        e,
        DomainEvent::PendingAttentionRemoved { native_id, .. } if native_id == "q-1"
    )));

    // 未知顶层键被计数(容忍未知,不猜语义)。
    assert!(mapper.unknown_keys().is_empty());
    let extra = json!({"id": CONV, "brandNew": 1, "anotherUnknownKey": true});
    let counts = count_unknown_keys(&extra);
    assert_eq!(counts.len(), 2);
}

#[test]
fn settings_are_projected_dynamically() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    mapper.apply_snapshot(1, &initial_state());
    let snapshot = mapper.runtime_snapshot().unwrap();
    // 动态读取:只投影 snapshot 实际携带的设置(§16.1),无硬编码列表。
    let kinds: Vec<_> = snapshot
        .capabilities
        .settings
        .iter()
        .map(|s| s.kind)
        .collect();
    assert!(kinds.contains(&bridge::domain::SettingKind::Model));
    assert!(kinds.contains(&bridge::domain::SettingKind::ReasoningEffort));
    assert!(kinds.contains(&bridge::domain::SettingKind::PermissionMode));
    let model = snapshot
        .capabilities
        .settings
        .iter()
        .find(|s| s.kind == bridge::domain::SettingKind::Model)
        .unwrap();
    assert_eq!(model.current_value.as_deref(), Some("gpt-5.3-fixture"));
    // 运行中锁定(RUNNING → 不可变更;设置作用于下一次 turn,§16.2)。
    assert!(!model.mutable);
}

#[test]
fn paginated_history_projects_active_turn_and_running_command() {
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    let state = json!({
        "id": CONV,
        "cwd": "/tmp/fixture-mapper",
        "historyMode": "paginated",
        "threadRuntimeStatus": {"type": "active", "activeFlags": []},
        "turns": [],
        "turnHistory": {"history": {"entitiesByKey": {
            "turn-old": {
                "turnId": "turn-old",
                "turnStartedAtMs": 10,
                "status": "completed",
                "items": []
            },
            "turn-live": {
                "turnId": "turn-live",
                "turnStartedAtMs": 20,
                "status": "inProgress",
                "items": [{
                    "id": "command-live",
                    "type": "commandExecution",
                    "status": "inProgress",
                    "command": "fixture-read-only"
                }]
            }
        }}},
        "pendingQuestions": [],
        "pendingApprovals": []
    });

    let events = mapper.apply_snapshot(7, &state);
    assert!(events.iter().any(|event| matches!(
        event,
        DomainEvent::TurnLifecycle {
            phase: ActiveTurnPhase::Running,
            ..
        }
    )));
    let snapshot = mapper.runtime_snapshot().expect("runtime snapshot");
    let current = snapshot.current_turn.expect("current paginated turn");
    assert_eq!(current.turn.id, "turn-live");
    assert_eq!(current.phase, ActiveTurnPhase::Running);
    assert_eq!(snapshot.running_commands.len(), 1);
    assert_eq!(snapshot.running_commands[0].command_id, "command-live");
}

#[test]
fn immer_patch_application_and_opaque_items() {
    let mut state = json!({"a": {"b": [1, 2]}, "keep": true});
    apply_immer_patches(
        &mut state,
        &[
            json!({"op": "add", "path": ["a", "b", "2"], "value": 3}),
            json!({"op": "replace", "path": ["a", "b"], "value": [9]}),
            json!({"op": "remove", "path": ["keep"]}),
        ],
    )
    .unwrap();
    assert_eq!(state["a"]["b"], json!([9]));
    assert!(state.get("keep").is_none());

    // 未知 item 类型 → Opaque,不崩溃(§12)。
    let mut mapper = SessionMapper::new(SessionKey::codex("dev", CONV));
    let mut state = initial_state();
    state["turns"][0]["items"][0] = json!({
        "id": "item-weird",
        "type": "brandNewItemType",
        "payload": "opaque",
    });
    mapper.apply_snapshot(1, &state);
    let snapshot = mapper.runtime_snapshot().unwrap();
    let _ = snapshot;
    // 不 panic 即通过;条目形态由 parse_item 单元测试覆盖。
    let _ = ItemContent::Opaque {
        native_type: "brandNewItemType".to_string(),
    };
}
