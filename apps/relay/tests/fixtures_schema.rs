//! fixtures 生产类型校验(§28/§29 验证要求第 8 项;阻塞交付门)。
//!
//! `contracts/fixtures/*.json` 中与生产 HTTP 响应相同 schema 的 fixture,
//! 必须能反序列化为 `apps/relay/src/sessions/json.rs` 与各 handler 实际
//! 产出的响应结构(此处为等价 serde 结构,字段全集必填 + deny_unknown_fields,
//! 即键名、类型、可选性逐一对齐生产 JSON 形状);错误类 fixture 校验统一
//! 错误对象(`state.rs api_error`)字段。fixture 与生产 schema 不符时本测试失败,
//! 应修 fixture(合成数据),不得放宽生产结构。
//!
//! 运行:`cargo test -p relay --test fixtures_schema`
//! (逐文件 PASS 行需 `-- --nocapture` 查看)。
//!
//! `output-gap.json` 是 WS 详情流事件的 JSON 视图(contracts/fixtures/README.md
//! 注明的唯一例外),按其文件内声明的形状校验,不冒充生产 HTTP 响应。

#![allow(non_snake_case)]
// 字段名与生产 camelCase JSON 键逐字对齐
// 这些结构只为经 serde 验证 fixture 的键名/类型/可选性与生产 JSON 形状一致
// (全集必填 + deny_unknown_fields),字段本身不逐个读取。
#![allow(dead_code)]

use serde::Deserialize;
use serde_json::Map;

const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../contracts/fixtures");

fn load(name: &str) -> serde_json::Value {
    let path = format!("{FIXTURES_DIR}/{name}");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读取 fixture 失败 {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("fixture JSON 解析失败 {name}: {e}"))
}

fn de<T: for<'de> Deserialize<'de>>(name: &str, v: serde_json::Value) -> T {
    serde_json::from_value(v).unwrap_or_else(|e| panic!("fixture 与生产 schema 不符 {name}: {e}"))
}

// ---------------------------------------------------------------------------
// 生产响应等价结构(sessions/json.rs + 各 handler 的 camelCase 形状)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSnapshotResponse {
    runtimeSnapshot: RuntimeSnapshot,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdRef {
    id: String,
    synthetic: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentTurn {
    turn: Option<IdRef>,
    phase: String,
    startedAt: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    steps: Vec<PlanStep>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanStep {
    stepId: String,
    title: String,
    status: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuestionOption {
    optionId: String,
    label: String,
}

/// 审批 decisions 用原生 decisionId(与问题 options 的 optionId 区分,json.rs)。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalDecision {
    decisionId: String,
    label: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingQuestion {
    questionId: String,
    title: String,
    description: String,
    options: Vec<QuestionOption>,
    allowMultiple: bool,
    allowFreeText: bool,
    turn: Option<IdRef>,
    createdAt: Option<String>,
    valid: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingApproval {
    approvalId: String,
    riskDescription: String,
    requestedAction: String,
    decisions: Vec<ApprovalDecision>,
    scope: String,
    turn: Option<IdRef>,
    createdAt: Option<String>,
    valid: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunningCommand {
    commandId: String,
    itemId: Option<IdRef>,
    startedAt: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackgroundCommand {
    commandId: String,
    itemId: Option<IdRef>,
    state: String,
    startedAt: Option<String>,
    finishedAt: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueView {
    state: String,
    afterTurnId: Option<IdRef>,
    acceptedRuntimeRevision: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingValue {
    value: String,
    label: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Setting {
    optionId: String,
    kind: String,
    label: String,
    currentValue: String,
    availableValues: Vec<SettingValue>,
    mutable: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferLimits {
    textInlineMaxBytes: i64,
    imageInlineMaxBytes: i64,
    pdfRangeMaxBytes: i64,
    downloadMaxBytes: i64,
    uploadMaxBytes: i64,
    maxConcurrentPerBrowser: i64,
    maxConcurrentPerDevice: i64,
    previewableMimePrefixes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capabilities {
    controlMode: String,
    compatibilityState: String,
    codexVersion: String,
    supportedOperations: Vec<String>,
    settings: Vec<Setting>,
    transferLimits: Option<TransferLimits>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecentOutputCursor {
    itemId: Option<IdRef>,
    revision: i64,
    byteLength: i64,
    isFinal: bool,
    channel: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSnapshot {
    runtimeRevision: i64,
    currentTurn: Option<CurrentTurn>,
    plan: Option<Plan>,
    pendingQuestions: Vec<PendingQuestion>,
    pendingApprovals: Vec<PendingApproval>,
    runningCommands: Vec<RunningCommand>,
    backgroundCommands: Vec<BackgroundCommand>,
    backgroundCommandCount: i64,
    queue: Option<QueueView>,
    capabilities: Option<Capabilities>,
    recentOutputCursors: Vec<RecentOutputCursor>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionListResponse {
    sessions: Vec<SessionSummary>,
    nextCursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionSummary {
    id: String,
    deviceId: String,
    agentKind: String,
    nativeSessionId: String,
    title: String,
    projectDisplayName: String,
    currentBranch: String,
    deviceConnection: String,
    deviceLastSeenAt: Option<String>,
    degradedReason: String,
    controlMode: String,
    compatibilityState: String,
    activeTurnPhase: String,
    pendingAttentionCount: i64,
    pendingAttentionKinds: Vec<String>,
    queueState: String,
    lastTurnOutcome: String,
    lastUpdatedAt: String,
    pinned: bool,
    muted: bool,
    archived: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DevicesResponse {
    devices: Vec<Device>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Device {
    id: String,
    displayName: String,
    platform: String,
    arch: String,
    bridgeVersion: String,
    connection: String,
    lastSeenAt: Option<String>,
    pairedAt: String,
    revoked: bool,
    compatibilityState: String,
    controlMode: String,
    privacyHideTitles: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GitSummaryResponse {
    gitSummary: GitSummary,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GitSummary {
    branch: String,
    detachedHead: bool,
    headShort: String,
    headFull: String,
    rootDisplayName: String,
    entries: Vec<GitEntry>,
    insertions: i64,
    deletions: i64,
    binaryFiles: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GitEntry {
    relativePath: String,
    status: String,
    staged: bool,
}

/// 统一错误对象(state.rs api_error)。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorBody {
    code: String,
    message: String,
    requestId: String,
    details: Map<String, serde_json::Value>,
}

/// output-gap.json:WS 详情流事件 JSON 视图(README 注明的例外形状)。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputGapView {
    streamId: String,
    streamEpoch: i64,
    events: Vec<OutputGapEvent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputGapEvent {
    emittedAt: String,
    outputAppend: Option<OutputAppend>,
    resyncHint: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputAppend {
    itemId: Option<IdRef>,
    expectedOffset: i64,
    bytesBase64: String,
    channel: String,
}

// ---------------------------------------------------------------------------
// 逐文件校验
// ---------------------------------------------------------------------------

#[test]
fn fixtures_match_production_response_schemas() {
    let runtime_fixtures = [
        "runtime-running.json",
        "runtime-waiting-question.json",
        "runtime-waiting-approval.json",
        "runtime-completed-background-command.json",
        "runtime-read-only-unverified.json",
        "queue-queued.json",
        "queue-paused.json",
    ];
    for name in runtime_fixtures {
        let v = load(name);
        let resp: RuntimeSnapshotResponse = de(name, v);
        assert!(
            resp.runtimeSnapshot.runtimeRevision >= 0,
            "{name}: runtimeRevision 非法"
        );
        println!("PASS {name} (GET /agent-console/api/sessions/{{id}}/runtime 同形)");
    }

    let name = "session-list.json";
    let list: SessionListResponse = de(name, load(name));
    assert!(!list.sessions.is_empty(), "{name}: sessions 不应为空");
    println!("PASS {name} (GET /agent-console/api/sessions 同形)");

    let name = "git-dirty-summary.json";
    let git: GitSummaryResponse = de(name, load(name));
    assert!(
        !git.gitSummary.entries.is_empty(),
        "{name}: entries 不应为空"
    );
    println!("PASS {name} (GET /agent-console/api/sessions/{{id}}/git 同形)");

    for name in ["device-online.json", "device-offline.json"] {
        let devices: DevicesResponse = de(name, load(name));
        assert!(!devices.devices.is_empty(), "{name}: devices 不应为空");
        println!("PASS {name} (GET /agent-console/api/devices 同形)");
    }

    // 错误类 fixture:统一错误对象四字段齐全,稳定码与登记一致。
    let name = "file-changed-error.json";
    let err: ErrorResponse = de(name, load(name));
    assert_eq!(err.error.code, "FILE_CHANGED", "{name}: 稳定错误码不符");
    assert!(!err.error.requestId.is_empty(), "{name}: requestId 缺失");
    println!("PASS {name} (统一错误对象形状)");

    // WS 事件 JSON 视图(README 声明的例外形状)。
    let name = "output-gap.json";
    let gap: OutputGapView = de(name, load(name));
    assert!(!gap.events.is_empty(), "{name}: events 不应为空");
    println!("PASS {name} (WS 详情流事件 JSON 视图)");
}
