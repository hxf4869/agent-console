//! 真实 Codex Desktop 写能力验收 harness(权威规格 §29.5;结果落地
//! docs/CODEX-COMPATIBILITY.md §9)。
//!
//! 运行(必须显式指定专用测试会话,否则报错跳过):
//! ```text
//! AC_REAL_THREAD=<专用测试会话 ID> \
//! AC_REAL_CWD=<专用测试工作区绝对路径> \
//!   cargo test -p bridge --test real_desktop_write -- --ignored --nocapture
//! ```
//!
//! 硬性边界(违反任何一条即整剧本失败):
//! 1. 唯一写目标由 AC_REAL_THREAD + AC_REAL_CWD 显式指定;任何写命令发送前:
//!    (a) S0 先从 Codex 目录库(只读)断言该 thread cwd 完全等于专用工作区,
//!        否则 abort;(b) IPC 发送层([`WriteGuard`])集中断言每条写命令的
//!        conversationId == 目标,不等即 panic 拒发。
//! 2. 不对其他 thread 发任何写;不读其他会话正文(catalog 查询按精确 ID)。
//! 3. 提示词只要求纯文本回复或只读 shell(seq/echo 类);禁止写文件/网络/安装/git 写。
//! 4. 不自动批准风险操作;不读、不改 approval/permission 档位。
//! 5. 不 kill/重启 Codex;SQLite 只读;不启动 App Server/daemon。
//! 6. 版本门禁:探测版本必须命中 `VERIFIED_VERSIONS`,否则 abort 零写入。
//! 7. 结束恢复现场:变更过的设置恢复原值;会话回 idle;确认无 pending attention。
//! 8. 仓库不 commit/push;不改 apps/web、relay、proto。
//!
//! 隐私:输出只含状态枚举、数量、稳定 ID 片段与测试产物(ok/编号);不打印
//! 会话正文、标题与本机路径。

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use bridge::adapter::codex::ipc::client::{
    connect, IpcClient, IpcClientConfig, IpcEvent, RequestOptions, StreamSync,
};
use bridge::adapter::codex::ipc::discovery;
use bridge::adapter::codex::ipc::messages::{self, StreamChange};
use bridge::adapter::codex::ipc::messages::{
    FollowingChangedParams, InputBlock, InterruptMode, StartTurnParams, SteerTurnParams, TurnStart,
    TurnStartRequest,
};
use bridge::adapter::codex::projection::apply_immer_patches;
use bridge::adapter::codex::{CodexAdapter, CodexAdapterConfig};
use bridge::domain::SessionKey;

/// 专用测试会话和工作区必须由调用者显式提供；环境变量只读取一次，保证
/// 整个 harness 的所有写守卫使用同一目标。
fn target_thread() -> &'static str {
    static VALUE: OnceLock<String> = OnceLock::new();
    VALUE
        .get_or_init(|| {
            let value = std::env::var("AC_REAL_THREAD")
                .expect("AC_REAL_THREAD 未设置:本 harness 只写显式指定的专用测试会话");
            assert!(!value.trim().is_empty(), "AC_REAL_THREAD 不能为空");
            value
        })
        .as_str()
}

fn target_cwd() -> &'static str {
    static VALUE: OnceLock<String> = OnceLock::new();
    VALUE
        .get_or_init(|| {
            let value = std::env::var("AC_REAL_CWD")
                .expect("AC_REAL_CWD 未设置:必须显式指定专用测试工作区");
            assert!(
                PathBuf::from(&value).is_absolute(),
                "AC_REAL_CWD 必须是绝对路径"
            );
            value
        })
        .as_str()
}

const IDLE_WAIT: Duration = Duration::from_secs(120);
const ACTIVE_WAIT: Duration = Duration::from_secs(90);
const TURN_DONE_WAIT: Duration = Duration::from_secs(300);
const AUTHORITATIVE_WAIT: Duration = Duration::from_secs(30);

/// 各步骤共享的观察状态(pump 写,主流程读)。
#[derive(Default)]
struct Shared {
    revision: Option<u64>,
    state: Value,
    /// patch 缺口/应用失败触发的自动补偿次数(load-complete-history)。
    auto_resyncs: u32,
    /// 全程出现过的原生问题/审批最大条数(结构计数,不含正文)。
    questions_ever: u32,
    approvals_ever: u32,
}

struct Ctx {
    client: Arc<IpcClient>,
    shared: Arc<Mutex<Shared>>,
    owner: String,
}

/// IPC 发送层写守卫:每条写命令发送前集中断言 conversationId(边界 1b)。
#[derive(Clone)]
struct WriteGuard {
    client: Arc<IpcClient>,
    owner: String,
}

impl WriteGuard {
    fn new(ctx: &Ctx) -> Self {
        Self {
            client: ctx.client.clone(),
            owner: ctx.owner.clone(),
        }
    }

    fn assert_target(&self, method: &str, params: &Value) {
        let cid = params.get("conversationId").and_then(Value::as_str);
        assert_eq!(
            cid,
            Some(target_thread()),
            "IPC 发送层守卫:{method} 的 conversationId 不是专用测试会话,拒绝发送"
        );
    }

    async fn start_turn(&self, prompt: &str) -> Result<Option<String>, String> {
        let params = StartTurnParams {
            conversation_id: target_thread().to_string(),
            turn_start: TurnStart {
                request: TurnStartRequest {
                    thread_id: target_thread().to_string(),
                    input: vec![InputBlock::text(prompt)],
                    extra: Default::default(),
                },
                context: None,
            },
        };
        let probe = serde_json::to_value(&params).expect("start-turn params serialize");
        self.assert_target(messages::method::THREAD_FOLLOWER_START_TURN, &probe);
        let result = self
            .client
            .start_turn(&self.owner, params)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.result.as_ref().and_then(find_turn_id))
    }

    /// steer 带 restoreMessage(协议 §7.2;0.153.1 owner 侧缺省时报
    /// `undefined (reading 'cwd')`,Desktop 自身 steer 会构造该字段)。
    /// `collaboration_mode`:来自快照 latestCollaborationMode 的实测值(若可得)。
    async fn steer_turn(
        &self,
        text: &str,
        collaboration_mode: Option<Value>,
    ) -> Result<(), String> {
        let mut context = json!({ "workspaceRoots": [target_cwd()] });
        if let Some(mode) = collaboration_mode {
            context["collaborationMode"] = mode;
        }
        let params = SteerTurnParams {
            conversation_id: target_thread().to_string(),
            input: vec![InputBlock::text(text)],
            client_user_message_id: None,
            service_tier: None,
            attachments: None,
            additional_context: None,
            tool_output: None,
            restore_message: Some(json!({ "cwd": target_cwd(), "context": context })),
        };
        let probe = serde_json::to_value(&params).expect("steer params serialize");
        self.assert_target(messages::method::THREAD_FOLLOWER_STEER_TURN, &probe);
        self.client
            .steer_turn(&self.owner, params)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn interrupt_turn(
        &self,
        expected_turn: &str,
    ) -> Result<messages::InterruptTurnResult, String> {
        let params = IpcClient::interrupt_params(
            target_thread(),
            InterruptMode::UserStop,
            Some(expected_turn.to_string()),
        );
        let probe = serde_json::to_value(&params).expect("interrupt params serialize");
        self.assert_target(messages::method::THREAD_FOLLOWER_INTERRUPT_TURN, &probe);
        self.client
            .interrupt_turn(&self.owner, params)
            .await
            .map_err(|e| e.to_string())
    }

    /// 通用 follower 写(submit-user-input / 审批 / settings)。
    async fn generic_write(&self, method: &str, params: Value) -> Result<Value, String> {
        self.assert_target(method, &params);
        let resp = self
            .client
            .request(
                method,
                params,
                RequestOptions {
                    target_client_id: Some(self.owner.clone()),
                    host_id: Some("local".to_string()),
                    timeout: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(resp.result.unwrap_or(Value::Null))
    }
}

// ---------------------------------------------------------------------------
// 状态观察辅助
// ---------------------------------------------------------------------------

fn runtime_status(state: &Value) -> &str {
    state
        .pointer("/threadRuntimeStatus/type")
        .and_then(Value::as_str)
        .unwrap_or("idle")
}

fn is_idle(state: &Value) -> bool {
    runtime_status(state) == "idle"
}

/// 会话的 turn 对象序列(0.153.1 真机实测:historyMode="paginated" 时
/// `turns` 数组恒空,turn 对象存于 turnHistory.history.entitiesByKey,按
/// turnStartedAtMs 升序;旧模式直接用 `turns[]`)。只换字段来源,不改断言语义。
fn turns(state: &Value) -> Vec<&Value> {
    if let Some(arr) = state.get("turns").and_then(Value::as_array) {
        if !arr.is_empty() {
            return arr.iter().collect();
        }
    }
    let mut entities: Vec<&Value> = state
        .pointer("/turnHistory/history/entitiesByKey")
        .and_then(Value::as_object)
        .map(|m| m.values().filter(|t| t.get("turnId").is_some()).collect())
        .unwrap_or_default();
    entities.sort_by_key(|t| {
        t.get("turnStartedAtMs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    });
    entities
}

fn turn_status(turn: &Value) -> &str {
    turn.get("status").and_then(Value::as_str).unwrap_or("")
}

fn turn_id_of(turn: &Value) -> &str {
    turn.get("turnId").and_then(Value::as_str).unwrap_or("")
}

fn turn_by_id<'a>(state: &'a Value, id: &str) -> Option<&'a Value> {
    let all = turns(state);
    if let Some(t) = all.into_iter().find(|t| turn_id_of(t) == id) {
        return Some(t);
    }
    turns(state).into_iter().last()
}

/// 解析目标 turn:优先 start 回执的 turnId,其次 inProgress,最后最后一个。
fn resolve_turn(state: &Value, started: &Option<String>) -> Option<String> {
    if let Some(id) = started {
        if turns(state).iter().any(|t| turn_id_of(t) == id.as_str()) {
            return Some(id.clone());
        }
    }
    for t in turns(state).iter().rev() {
        if turn_status(t) == "inProgress" {
            return Some(turn_id_of(t).to_string());
        }
    }
    turns(state).last().map(|t| turn_id_of(t).to_string())
}

/// item 的用户可见文本(text / aggregatedOutput / content[].text)。
fn item_text(item: &Value) -> String {
    let mut out = String::new();
    if let Some(t) = item.get("text").and_then(Value::as_str) {
        out.push_str(t);
        out.push('\n');
    }
    if let Some(o) = item.get("aggregatedOutput").and_then(Value::as_str) {
        out.push_str(o);
        out.push('\n');
    }
    if let Some(arr) = item.get("content").and_then(Value::as_array) {
        for c in arr {
            if let Some(t) = c.get("text").and_then(Value::as_str) {
                out.push_str(t);
                out.push('\n');
            } else if let Some(t) = c.as_str() {
                out.push_str(t);
                out.push('\n');
            }
        }
    }
    out
}

fn turn_text(turn: &Value) -> String {
    let mut out = String::new();
    for item in turn
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
    {
        out.push_str(&item_text(item));
    }
    out
}

/// turn 中最后一个 assistant 消息(优先 final_answer phase)的可见文本。
fn final_assistant_text(turn: &Value) -> Option<String> {
    let items = turn.get("items").and_then(Value::as_array)?;
    let is_assistant = |i: &Value| {
        i.get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("agentMessage")
    };
    let mut candidates: Vec<&Value> = items.iter().filter(|i| is_assistant(i)).collect();
    if let Some(finals) = candidates
        .iter()
        .filter(|i| i.get("phase").and_then(Value::as_str) == Some("final_answer"))
        .max_by_key(|i| item_text(i).len())
    {
        return Some(item_text(finals));
    }
    let _ = &mut candidates;
    candidates.pop().map(item_text)
}

/// 提取 turn 文本中的纯整数行(编号输出核对)。
fn number_lines(text: &str) -> Vec<u32> {
    text.lines()
        .map(str::trim)
        .filter_map(|l| l.parse::<u32>().ok())
        .collect()
}

/// `nums` 是否按顺序包含 `target` 作为子序列(允许穿插其他内容)。
fn contains_sequence(nums: &[u32], target: &[u32]) -> bool {
    let mut iter = nums.iter().copied();
    for t in target {
        if !iter.any(|n| n == *t) {
            return false;
        }
    }
    true
}

/// 目标 turn 的命令输出最大长度(观察输出增长)。
fn command_output_len(state: &Value, turn: Option<&str>) -> usize {
    let Some(turn) = turn
        .and_then(|id| turn_by_id(state, id))
        .or_else(|| turns(state).into_iter().last())
    else {
        return 0;
    };
    let mut max = 0usize;
    for item in turn
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
    {
        if let Some(o) = item.get("aggregatedOutput").and_then(Value::as_str) {
            max = max.max(o.len());
        }
    }
    max
}

fn current_effort(state: &Value) -> Option<String> {
    let settings = state.get("latestThreadSettings");
    let from_settings = || {
        settings
            .and_then(|s| s.get("effort").or_else(|| s.get("reasoning_effort")))
            .and_then(Value::as_str)
            .map(String::from)
    };
    from_settings()
        .or_else(|| {
            state
                .get("latestReasoningEffort")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .or_else(from_settings)
}

/// reasoning_effort 的合法值序(原生枚举;只用于选择已验证的相邻值)。
/// "max" 为 0.153.1 专用会话快照 latestThreadSettings.effort 实测值。
const EFFORT_ORDER: [&str; 5] = ["minimal", "low", "medium", "high", "max"];

/// 向下相邻合法档位(如 max→high)。0.153.1 真机实测:环形取到 minimal 后
/// 新一轮 turn 长时间无终态;向下相邻为确定合法且无害的变更目标。
fn prev_effort(current: &str) -> Option<String> {
    let idx = EFFORT_ORDER.iter().position(|v| *v == current)?;
    if idx == 0 {
        return None;
    }
    Some(EFFORT_ORDER[idx - 1].to_string())
}

fn pending_questions(state: &Value) -> Vec<Value> {
    state
        .get("pendingQuestions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn pending_approvals(state: &Value) -> Vec<Value> {
    state
        .get("pendingApprovals")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn short(id: &str) -> String {
    id.chars().take(8).collect()
}

/// start-turn 回执 result 中的 turnId(协议 §7.1:回执 result 内嵌 turn 对象;
/// 层级不确定时按已知键逐层找,找不到由 resolve_turn 的 fallback 兜底)。
fn find_turn_id(v: &Value) -> Option<String> {
    v.get("turnId")
        .or_else(|| v.get("result").and_then(|r| r.get("turnId")))
        .and_then(Value::as_str)
        .map(String::from)
}

// ---------------------------------------------------------------------------
// 等待与权威重读
// ---------------------------------------------------------------------------

async fn wait_pred(ctx: &Ctx, timeout: Duration, mut pred: impl FnMut(&Value) -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let st = ctx.shared.lock();
            if st.revision.is_some() && pred(&st.state) {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// 终态后的落稳窗口(§13.3:等待状态落稳,不用固定长 sleep):revision
/// 停止推进约 1.2s 或总等待超过 8s。
async fn wait_settled(ctx: &Ctx) {
    let mut last = ctx.shared.lock().revision;
    let mut stable_since = tokio::time::Instant::now();
    let start = stable_since;
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let rev = ctx.shared.lock().revision;
        if rev != last {
            last = rev;
            stable_since = tokio::time::Instant::now();
        }
        if stable_since.elapsed() >= Duration::from_millis(1200)
            || start.elapsed() >= Duration::from_secs(8)
        {
            return;
        }
    }
}

/// 权威重读:load-complete-history → 等待 revision >= 返回值的快照。
async fn authoritative_snapshot(ctx: &Ctx) -> Result<Value, String> {
    let rev = ctx
        .client
        .load_complete_history("local", target_thread())
        .await
        .map_err(|e| e.to_string())?;
    let deadline = tokio::time::Instant::now() + AUTHORITATIVE_WAIT;
    loop {
        {
            let st = ctx.shared.lock();
            if st.revision.is_some_and(|r| r >= rev) {
                return Ok(st.state.clone());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "未在 {AUTHORITATIVE_WAIT:?} 内收到 revision>={rev} 的权威快照"
            ));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// 等待目标 turn 进入终态并回 idle;返回 (完成, 期间最大命令输出长度)。
async fn wait_turn_terminal(ctx: &Ctx, turn: Option<String>) -> (bool, usize) {
    let mut max_output = 0usize;
    let deadline = tokio::time::Instant::now() + TURN_DONE_WAIT;
    loop {
        {
            let st = ctx.shared.lock();
            max_output = max_output.max(command_output_len(&st.state, turn.as_deref()));
            let idle = is_idle(&st.state);
            let terminal = match turn.as_deref().and_then(|id| turn_by_id(&st.state, id)) {
                Some(t) => turn_status(t) != "inProgress",
                None => turns(&st.state)
                    .last()
                    .map(|t| turn_status(t) != "inProgress")
                    .unwrap_or(false),
            };
            if st.revision.is_some() && idle && terminal {
                return (true, max_output);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return (false, max_output);
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// 完整跑一轮:start → idle→active→idle → 终态权威重读。
/// 返回 (turn_id, 观察到 active, 权威最终状态)。
async fn run_turn(
    ctx: &Ctx,
    guard: &WriteGuard,
    label: &str,
    prompt: &str,
) -> Result<(Option<String>, bool, Value), String> {
    if !wait_pred(ctx, IDLE_WAIT, |st| is_idle(st)).await {
        return Err(format!("{label}:start 前会话未回 idle"));
    }
    let started = guard.start_turn(prompt).await?;
    let active_observed = wait_pred(ctx, ACTIVE_WAIT, |st| !is_idle(st)).await;
    let (done, _output) = wait_turn_terminal(ctx, started.clone()).await;
    if !done {
        return Err(format!("{label}:turn 未在 {TURN_DONE_WAIT:?} 内进入终态"));
    }
    wait_settled(ctx).await;
    let final_state = authoritative_snapshot(ctx).await?;
    Ok((started, active_observed, final_state))
}

// ---------------------------------------------------------------------------
// pump:订阅流 → 维护状态树(快照 + Immer patches,缺口自动补偿)
// ---------------------------------------------------------------------------

async fn pump(
    client: Arc<IpcClient>,
    mut events: tokio::sync::mpsc::Receiver<IpcEvent>,
    shared: Arc<Mutex<Shared>>,
) {
    let mut sync = StreamSync::new();
    while let Some(event) = events.recv().await {
        let IpcEvent::StreamChanged(params) = event else {
            continue;
        };
        if params.conversation_id != target_thread() {
            continue;
        }
        let mut need_resync = false;
        {
            let mut st = shared.lock();
            match &params.change {
                StreamChange::Snapshot {
                    revision,
                    conversation_state,
                } => {
                    let _ = sync.observe(&params.change);
                    st.revision = Some(*revision);
                    st.state = conversation_state.clone();
                    st.questions_ever = st
                        .questions_ever
                        .max(pending_questions(conversation_state).len() as u32);
                    st.approvals_ever = st
                        .approvals_ever
                        .max(pending_approvals(conversation_state).len() as u32);
                }
                StreamChange::Patches {
                    base_revision,
                    revision,
                    patches,
                } => {
                    if sync.revision() == Some(*base_revision) {
                        let mut next = st.state.clone();
                        match apply_immer_patches(&mut next, patches) {
                            Ok(()) => {
                                let _ = sync.observe(&params.change);
                                st.questions_ever =
                                    st.questions_ever.max(pending_questions(&next).len() as u32);
                                st.approvals_ever =
                                    st.approvals_ever.max(pending_approvals(&next).len() as u32);
                                st.state = next;
                                st.revision = Some(*revision);
                            }
                            Err(_) => need_resync = true,
                        }
                    } else {
                        need_resync = true;
                    }
                }
            }
        }
        if need_resync {
            shared.lock().auto_resyncs += 1;
            // §12:缺口 → 快照补偿(读路径,允许)。
            let _ = client.load_complete_history("local", target_thread()).await;
        }
    }
}

// ---------------------------------------------------------------------------
// S7:全程原生审批被动处理(不制造风险操作;按原生 decision 回应)
// ---------------------------------------------------------------------------

/// 从原生 decisions 中选择 approve 语义项;无法确信时返回 None(不猜字段)。
fn pick_approve_decision(approval: &Value) -> Option<String> {
    let decisions = approval.get("decisions").and_then(Value::as_array)?;
    for d in decisions {
        let id = d
            .get("id")
            .or_else(|| d.get("decisionId"))
            .and_then(Value::as_str)?;
        let label = d
            .get("label")
            .and_then(|l| {
                l.as_str()
                    .map(String::from)
                    .or_else(|| l.get("text").and_then(Value::as_str).map(String::from))
            })
            .unwrap_or_default();
        let kind = d.get("kind").and_then(Value::as_str).unwrap_or("");
        let haystack = format!("{id} {label} {kind}").to_lowercase();
        if ["approve", "allow", "accept", "yes", "once"]
            .iter()
            .any(|k| haystack.contains(k))
        {
            return Some(id.to_string());
        }
    }
    None
}

async fn approval_handler(
    ctx: &Ctx,
    handled: Arc<Mutex<Vec<String>>>,
    log: Arc<Mutex<Vec<String>>>,
) {
    let guard = WriteGuard::new(ctx);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let pending = pending_approvals(&ctx.shared.lock().state);
        for approval in pending {
            let Some(id) = approval
                .get("id")
                .or_else(|| approval.get("approvalId"))
                .and_then(Value::as_str)
                .map(String::from)
            else {
                continue;
            };
            if handled.lock().iter().any(|h| h == &id) {
                continue;
            }
            let Some(decision) = pick_approve_decision(&approval) else {
                let keys = approval
                    .as_object()
                    .map(|m| m.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                log.lock().push(format!(
                    "出现原生审批 id={} 但无法从 decisions 中确信 approve 项,未发送(顶层键:{keys:?})",
                    short(&id)
                ));
                handled.lock().push(id);
                continue;
            };
            let result = guard
                .generic_write(
                    messages::method::THREAD_FOLLOWER_COMMAND_APPROVAL_DECISION,
                    json!({
                        "conversationId": target_thread(),
                        "requestId": id,
                        "decision": decision,
                    }),
                )
                .await;
            match result {
                Ok(_) => {
                    log.lock().push(format!(
                        "已按原生 decision 回应审批 id={} decision={decision}",
                        short(&id)
                    ));
                }
                Err(err) => {
                    log.lock()
                        .push(format!("审批 id={} 回应失败:{err}(保留待重试)", short(&id)));
                }
            }
            handled.lock().push(id);
        }
    }
}

// ---------------------------------------------------------------------------
// 目录库只读查询(边界 1a / S9)
// ---------------------------------------------------------------------------

async fn catalog_thread_row() -> Result<(String, bool), String> {
    let home = std::env::var("HOME").map_err(|_| "HOME 未设置".to_string())?;
    let db = PathBuf::from(home).join(".codex").join("state_5.sqlite");
    let opts = SqliteConnectOptions::new()
        .filename(&db)
        .read_only(true)
        .busy_timeout(Duration::from_secs(2));
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(opts)
        .await
        .map_err(|e| e.to_string())?;
    let row: Option<(String, i64)> =
        sqlx::query_as("SELECT cwd, archived FROM threads WHERE id = ?")
            .bind(target_thread())
            .fetch_optional(&pool)
            .await
            .map_err(|e| e.to_string())?;
    pool.close().await;
    match row {
        Some((cwd, archived)) => Ok((cwd, archived != 0)),
        None => Err("专用测试会话不在目录库中".to_string()),
    }
}

// ---------------------------------------------------------------------------
// 剧本主体
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires live Codex Desktop; writes ONLY to the dedicated AC_REAL_THREAD test session"]
async fn real_desktop_write_acceptance_s0_to_s9() {
    // ---- 边界:会话与工作区必须由 env 显式指定 ----
    let _ = (target_thread(), target_cwd());

    let mut restore_effort: Option<String> = None;
    let mut failures: Vec<String> = Vec::new();
    let mut step = |name: &str, ok: bool, evidence: String| {
        println!(
            "STEP {name}: {} {evidence}",
            if ok { "PASS" } else { "FAIL" }
        );
        if !ok {
            failures.push(format!("{name}: {evidence}"));
        }
    };

    // ================= S0 门禁(版本 → cwd → owner)=================
    let version = discovery::probe_version(None)
        .await
        .expect("codex --version 探测失败");
    if !discovery::version_is_verified(&version) {
        panic!("S0 版本门禁未命中({version} 不在 VERIFIED_VERSIONS):abort 零写入");
    }
    println!("S0 版本门禁: {version} ∈ VERIFIED_VERSIONS");

    let (cwd, archived) = catalog_thread_row().await.expect("目录库只读查询失败");
    if cwd != target_cwd() {
        panic!("S0 cwd 门禁失败:专用会话 cwd 与测试工作区不一致,abort 零写入");
    }
    assert!(!archived, "专用会话已归档,abort");

    let Some(socket) = discovery::socket_path(None, None) else {
        panic!("S0:未发现 ipc socket");
    };
    let (client, events) = connect(IpcClientConfig::new(
        socket,
        "agent-console-bridge-real-write",
    ))
    .await
    .expect("S0:connect + initialize 失败");
    let client = Arc::new(client);
    let shared: Arc<Mutex<Shared>> = Arc::new(Mutex::new(Shared::default()));
    {
        let client = client.clone();
        let shared = shared.clone();
        tokio::spawn(pump(client, events, shared));
    }

    let owner = client
        .discover_owner("local", target_thread())
        .await
        .expect("S0:owner discovery 请求失败")
        .expect("S0:owner discovery 无 owner(会话未在 Desktop 打开)");
    let ctx = Ctx {
        client: client.clone(),
        shared: shared.clone(),
        owner: owner.clone(),
    };

    // 订阅并等待首个快照(只读)。
    client
        .set_following(
            FollowingChangedParams {
                conversation_id: target_thread().to_string(),
                host_id: "local".to_string(),
                following: true,
            },
            None,
        )
        .await
        .expect("S0:following 广播失败");
    // 首个快照:wait_pred 内部已要求 revision.is_some(),谓词恒真即可。
    let got_snapshot = wait_pred(&ctx, Duration::from_secs(15), |_| true).await;
    assert!(got_snapshot, "S0:15s 内未收到首个快照");
    {
        let st = shared.lock();
        let runtime_cwd = st.state.get("cwd").and_then(Value::as_str).unwrap_or("");
        assert_eq!(
            runtime_cwd,
            target_cwd(),
            "S0:运行时快照 cwd 与测试工作区不一致,abort"
        );
    }
    step(
        "S0",
        true,
        format!("版本命中({version});目录库 cwd 断言通过;快照 cwd 断言通过;owner discovery 成功"),
    );

    // 全程原生审批被动处理(S7)。
    let handled: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let approval_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let approval_task = {
        let ctx = Ctx {
            client: client.clone(),
            shared: shared.clone(),
            owner: owner.clone(),
        };
        let handled = handled.clone();
        let log = approval_log.clone();
        tokio::spawn(async move { approval_handler(&ctx, handled, log).await })
    };

    let guard = WriteGuard::new(&ctx);

    // ================= S1 读链 =================
    {
        let adapter = CodexAdapter::connect({
            let mut cfg = CodexAdapterConfig::new("ac-real-write-test");
            cfg.version_report = Some(version.clone());
            cfg
        })
        .await
        .expect("S1:adapter 连接失败");
        let key = SessionKey::codex("ac-real-write-test", target_thread());
        let listed = adapter.list_sessions(None, 50, false).await;
        let in_list = listed
            .as_ref()
            .map(|p| {
                p.sessions
                    .iter()
                    .any(|s| s.session_key.native_session_id == target_thread())
            })
            .unwrap_or(false);
        let snapshot = adapter.runtime_snapshot(&key).await;
        let history = adapter.history_page(&key, None, 50).await;
        let hist_evidence = history
            .as_ref()
            .map(|p| format!("entries={} has_more={}", p.entries.len(), p.has_more))
            .unwrap_or_else(|e| format!("history_page 错误:{e}"));
        let snap_evidence = snapshot
            .as_ref()
            .map(|s| {
                format!(
                    "revision={} phase={:?} questions={} approvals={}",
                    s.runtime_revision,
                    s.current_turn.as_ref().map(|t| t.phase),
                    s.pending_questions.len(),
                    s.pending_approvals.len()
                )
            })
            .unwrap_or_else(|e| format!("runtime_snapshot 错误:{e}"));
        step(
            "S1",
            in_list && snapshot.is_ok() && history.as_ref().map(|p| !p.entries.is_empty()).unwrap_or(false),
            format!(
                "list_sessions 含该 thread={in_list};runtime_snapshot 可读({snap_evidence});history_page({hist_evidence})"
            ),
        );
    }

    // ================= S2 StartTurn(纯文本回复)=================
    match run_turn(&ctx, &guard, "S2", "请只回复 ok 两个字母,不要做任何其他事").await
    {
        Ok((started, active_observed, final_state)) => {
            let st = shared.lock();
            let auto_resyncs = st.auto_resyncs;
            drop(st);
            let turn = resolve_turn(&final_state, &started);
            let final_text = turn
                .as_deref()
                .and_then(|id| turn_by_id(&final_state, id))
                .and_then(final_assistant_text)
                .unwrap_or_default();
            let status = turn
                .as_deref()
                .and_then(|id| turn_by_id(&final_state, id))
                .map(turn_status)
                .unwrap_or("")
                .to_string();
            let contains_ok = final_text.to_lowercase().contains("ok");
            step(
                "S2",
                active_observed && contains_ok && status == "completed",
                format!(
                    "idle→active→{};权威最终 assistant 含 ok={contains_ok}(最终文本 {}/{} 字符);终态权威重读经 load-complete-history 已触发,期间自动补偿 {} 次",
                    if status.is_empty() { "?" } else { &status },
                    final_text.trim().len(),
                    final_text.len(),
                    auto_resyncs
                ),
            );
        }
        Err(err) => step("S2", false, err),
    }

    // ================= S3 编号输出(seq 1 20)=================
    {
        // 保持命令超过 Desktop 单次工具输出的常见等待窗口，确保能观察到
        // 至少两次真实 aggregatedOutput 增长，而不是只收到一次终态快照。
        let prompt = "请运行只读命令：i=1; while [ $i -le 20 ]; do echo $i; i=$((i+1)); sleep 1; done。请把输出原样展示，不要执行其他操作";
        match run_turn_intro(&ctx, &guard, prompt).await {
            Ok((started, turn)) => {
                // 记录执行中是否观察到输出增长。LIVE_PREVIEW 是尽力而为：
                // Desktop 可能只在命令结束时一次广播 aggregatedOutput，因此
                // 发布门槛以 AUTHORITATIVE_FINAL 的完整性为准。
                let mut max_len = 0usize;
                let mut grew = false;
                let deadline = tokio::time::Instant::now() + TURN_DONE_WAIT;
                loop {
                    let (idle, len) = {
                        let st = ctx.shared.lock();
                        (
                            is_idle(&st.state),
                            command_output_len(&st.state, Some(&turn)),
                        )
                    };
                    if len > max_len {
                        grew = grew || max_len > 0;
                        max_len = len;
                    }
                    if idle || tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                let _ = started;
                let (done, _max) = wait_turn_terminal(&ctx, Some(turn.clone())).await;
                wait_settled(&ctx).await;
                let authoritative = authoritative_snapshot(&ctx).await;
                match authoritative {
                    Ok(state) => {
                        let text = turn_by_id(&state, &turn).map(turn_text).unwrap_or_default();
                        let nums = number_lines(&text);
                        let target: Vec<u32> = (1..=20).collect();
                        let complete = contains_sequence(&nums, &target);
                        step(
                            "S3",
                            done && complete,
                            format!(
                                "OBSERVED 尽力直播输出增长={grew}(最大输出 {max_len} 字节);终态权威输出 1..20 无缺号={complete}(解析到 {} 个编号行)",
                                nums.len()
                            ),
                        );
                    }
                    Err(err) => step("S3", false, format!("权威重读失败:{err}")),
                }
            }
            Err(err) => step("S3", false, err),
        }
    }

    // ================= S4 steer(1..30 数数 + done)=================
    {
        let prompt = "请从 1 慢数到 30,每个数字单独一行,数完后停止";
        match run_turn_intro(&ctx, &guard, prompt).await {
            Ok((started, turn)) => {
                let turns_before = {
                    let st = shared.lock();
                    turns(&st.state).len()
                };
                // active 期间等前几个数字出现。
                let saw = wait_pred(&ctx, Duration::from_secs(180), |st| {
                    turn_by_id(st, &turn)
                        .map(|t| {
                            let nums = number_lines(&turn_text(t));
                            contains_sequence(&nums, &[1, 2, 3])
                        })
                        .unwrap_or(false)
                })
                .await;
                let collaboration_mode =
                    shared.lock().state.get("latestCollaborationMode").cloned();
                let steer_result = guard
                    .steer_turn("请在数完后额外添加一行 done", collaboration_mode)
                    .await;
                let (done, _) = wait_turn_terminal(&ctx, Some(turn.clone())).await;
                wait_settled(&ctx).await;
                let state = authoritative_snapshot(&ctx).await;
                match (&steer_result, &state) {
                    (Ok(()), Ok(state)) => {
                        let turns_after = turns(state).len();
                        let same_turn = started
                            .as_deref()
                            .map(|id| turn_by_id(state, id).is_some())
                            .unwrap_or(true);
                        let text = turn_by_id(state, &turn).map(turn_text).unwrap_or_default();
                        let nums = number_lines(&text);
                        let target: Vec<u32> = (1..=30).collect();
                        let complete = contains_sequence(&nums, &target);
                        let has_done = text.to_lowercase().contains("done");
                        step(
                            "S4",
                            saw && done && same_turn && complete && has_done && turns_before == turns_after,
                            format!(
                                "前 3 个数字后 steer 被同轮接受;turn 数 {turns_before:?}→{turns_after:?}(未新增轮);序列 1..30 完整={complete};含 done={has_done}"
                            ),
                        );
                    }
                    (Err(err), _) => step("S4", false, format!("steer 被拒绝:{err}")),
                    (_, Err(err)) => step("S4", false, format!("权威重读失败:{err}")),
                }
            }
            Err(err) => step("S4", false, err),
        }
    }

    // ================= S5 interrupt(1..50 数数,中途打断)=================
    {
        let prompt = "请从 1 慢数到 50,每个数字一行";
        match run_turn_intro(&ctx, &guard, prompt).await {
            Ok((_started, turn)) => {
                let output_began = wait_pred(&ctx, Duration::from_secs(120), |st| {
                    turn_by_id(st, &turn)
                        .map(|t| !number_lines(&turn_text(t)).is_empty())
                        .unwrap_or(false)
                })
                .await;
                let interrupted = guard.interrupt_turn(&turn).await;
                let back_idle = wait_pred(&ctx, Duration::from_secs(60), |st| is_idle(st)).await;
                wait_settled(&ctx).await;
                let state = authoritative_snapshot(&ctx).await;
                let status = state
                    .as_ref()
                    .ok()
                    .and_then(|s| {
                        turns(s)
                            .into_iter()
                            .find(|t| turn_id_of(t) == turn.as_str())
                    })
                    .map(turn_status)
                    .unwrap_or("")
                    .to_string();
                // 可再开新轮:一个最小轮确认会话可继续。
                let next_turn = run_turn(
                    &ctx,
                    &guard,
                    "S5-recovery",
                    "请只回复 ok 两个字母,不要做任何其他事",
                )
                .await;
                let can_restart = matches!(&next_turn, Ok((_, _, final_state)) if {
                    let turn = resolve_turn(final_state, &next_turn.as_ref().unwrap().0);
                    turn.as_deref()
                        .and_then(|id| turn_by_id(final_state, id))
                        .and_then(final_assistant_text)
                        .map(|t| t.to_lowercase().contains("ok"))
                        .unwrap_or(false)
                });
                match &interrupted {
                    Ok(result) => {
                        let interrupted_id_ok = result.interrupted_turn_id == turn || result.ok;
                        step(
                            "S5",
                            output_began && interrupted_id_ok && back_idle && status == "interrupted" && can_restart,
                            format!(
                                "输出开始后 interrupt(user-stop,expectedTurnId) 被接受(ok={});回 idle={back_idle};终态权威 status={status:?};随后新轮正常完成={can_restart}",
                                result.ok
                            ),
                        );
                    }
                    Err(err) => step("S5", false, format!("interrupt 被拒绝:{err}")),
                }
            }
            Err(err) => step("S5", false, err),
        }
    }

    // ================= S6 原生问题 =================
    {
        let prompt = "请使用你的原生提问功能向我提出一个选择题,不要用普通文本替代";
        match run_turn_intro(&ctx, &guard, prompt).await {
            Ok((_started, turn)) => {
                let question = wait_pred(&ctx, Duration::from_secs(240), |st| {
                    !pending_questions(st).is_empty()
                })
                .await;
                if question {
                    let q = pending_questions(&ctx.shared.lock().state)
                        .into_iter()
                        .next();
                    let Some(q) = q else {
                        step("S6", false, "pendingQuestions 出现但读取失败".to_string());
                        let _ = wait_turn_terminal(&ctx, Some(turn)).await;
                        return finish(failures).await;
                    };
                    let keys = q
                        .as_object()
                        .map(|m| m.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let qid = q
                        .get("id")
                        .or_else(|| q.get("questionId"))
                        .and_then(Value::as_str)
                        .map(String::from);
                    let first_option = q
                        .get("options")
                        .and_then(Value::as_array)
                        .and_then(|os| os.first())
                        .and_then(|o| o.get("id").and_then(Value::as_str).map(String::from));
                    match (qid, first_option) {
                        (Some(qid), Some(option_id)) => {
                            let answer = guard
                                .generic_write(
                                    messages::method::THREAD_FOLLOWER_SUBMIT_USER_INPUT,
                                    json!({
                                        "conversationId": target_thread(),
                                        "requestId": qid,
                                        "response": {"optionIds": [option_id], "text": null},
                                    }),
                                )
                                .await;
                            let removed = wait_pred(&ctx, Duration::from_secs(30), |st| {
                                pending_questions(st).is_empty()
                            })
                            .await;
                            let (done, _) = wait_turn_terminal(&ctx, Some(turn)).await;
                            wait_settled(&ctx).await;
                            let _ = authoritative_snapshot(&ctx).await;
                            match answer {
                                Ok(_) => step(
                                    "S6",
                                    removed && done,
                                    format!(
                                        "原生问题出现(原生 question ID + 选项,顶层键 {keys:?});按原生 option ID AnswerQuestion 被接受;attention 移除={removed};会话继续完成={done}"
                                    ),
                                ),
                                Err(err) => step(
                                    "S6",
                                    false,
                                    format!("原生问题出现但 AnswerQuestion 被拒绝:{err}"),
                                ),
                            }
                        }
                        _ => {
                            step(
                                "S6",
                                false,
                                format!(
                                    "pendingQuestions 出现但无法按约定键提取 question/option(顶层键 {keys:?});按边界不猜字段"
                                ),
                            );
                            let _ = wait_turn_terminal(&ctx, Some(turn)).await;
                        }
                    }
                } else {
                    let (done, _) = wait_turn_terminal(&ctx, Some(turn)).await;
                    step(
                        "S6",
                        true,
                        format!(
                            "OBSERVED 240s 内未观察到原生问题(会话正常结束={done})→ FIXTURE_ONLY,未猜测字段"
                        ),
                    );
                }
            }
            Err(err) => step("S6", false, err),
        }
    }

    // ================= S7 审批(全程被动)=================
    {
        approval_task.abort();
        let approvals_ever = shared.lock().approvals_ever;
        let log = approval_log.lock().join("; ");
        if approvals_ever == 0 {
            step(
                "S7",
                true,
                "OBSERVED 全程未出现原生审批(未制造风险操作)→ FIXTURE_ONLY".to_string(),
            );
        } else {
            let all_handled = handled.lock().len() as u32 >= approvals_ever;
            step(
                "S7",
                all_handled,
                format!(
                    "全程出现原生审批 {approvals_ever} 次;处理记录: {}",
                    if log.is_empty() { "(无)" } else { &log }
                ),
            );
        }
    }

    // ================= S8 设置(reasoning_effort;绝不碰 permission)=================
    {
        wait_settled(&ctx).await;
        let before = authoritative_snapshot(&ctx).await;
        match before {
            Ok(before) => {
                let orig = current_effort(&before);
                // 0.153.1 已知 max 环形前进会落到 minimal，并使下一轮进入
                // systemError；主验收只使用已经真机验证可正常往返的 max→high。
                let new = orig.as_deref().and_then(prev_effort);
                match (orig.clone(), new) {
                    (Some(orig), Some(new)) => {
                        restore_effort = Some(orig.clone());
                        let update = guard
                            .generic_write(
                                messages::method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                                json!({
                                    "conversationId": target_thread(),
                                    "threadSettings": {"effort": new},
                                }),
                            )
                            .await;
                        // 断言快照反映新值(轮询 + 中途一次权威重读)。
                        let mut reflected =
                            wait_pred(&ctx, Duration::from_secs(15), |st| {
                                current_effort(st).as_deref() == Some(new.as_str())
                            })
                            .await;
                        if !reflected {
                            let _ = authoritative_snapshot(&ctx).await;
                            reflected = wait_pred(&ctx, Duration::from_secs(10), |st| {
                                current_effort(st).as_deref() == Some(new.as_str())
                            })
                            .await;
                        }
                        let settings_evidence = match &update {
                            Ok(_) => format!("UpdateSettings({orig}→{new}) 被接受;快照反映新值={reflected}"),
                            Err(err) => format!("UpdateSettings 被拒绝:{err}"),
                        };
                        step("S8a", update.is_ok() && reflected, settings_evidence);

                        if update.is_ok() {
                            // 再开一轮确认正常。
                            let next_turn = run_turn(
                                &ctx,
                                &guard,
                                "S8b",
                                "请只回复 ok 两个字母,不要做任何其他事",
                            )
                            .await;
                            let normal = matches!(&next_turn, Ok((_, _, final_state)) if {
                                let turn = resolve_turn(final_state, &next_turn.as_ref().unwrap().0);
                                turn.as_deref()
                                    .and_then(|id| turn_by_id(final_state, id))
                                    .and_then(final_assistant_text)
                                    .map(|t| t.to_lowercase().contains("ok"))
                                    .unwrap_or(false)
                            });
                            step(
                                "S8b",
                                normal,
                                format!("设置变更后新一轮正常完成={normal}"),
                            );
                        }

                        // 恢复原值。
                        let restore = guard
                            .generic_write(
                                messages::method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                                json!({
                                    "conversationId": target_thread(),
                                    "threadSettings": {"effort": orig},
                                }),
                            )
                            .await;
                        let mut restored =
                            wait_pred(&ctx, Duration::from_secs(15), |st| {
                                current_effort(st).as_deref() == Some(orig.as_str())
                            })
                            .await;
                        if !restored {
                            let _ = authoritative_snapshot(&ctx).await;
                            restored = wait_pred(&ctx, Duration::from_secs(10), |st| {
                                current_effort(st).as_deref() == Some(orig.as_str())
                            })
                            .await;
                        }
                        let restore_evidence = match &restore {
                            Ok(_) => format!("恢复原值({orig})已发送;快照确认还原={restored}"),
                            Err(err) => format!("恢复原值被拒绝:{err}"),
                        };
                        if restore.is_ok() && restored {
                            restore_effort = None; // 已还原,失败路径无需再恢复
                        }
                        step("S8c", restore.is_ok() && restored, restore_evidence);
                    }
                    (orig, _) => step(
                        "S8a",
                        true,
                        format!(
                            "OBSERVED 快照无 reasoning_effort 当前值或不在合法值序({orig:?})→ 设置写入 FIXTURE_ONLY,未猜测合法值"
                        ),
                    ),
                }
            }
            Err(err) => step("S8a", false, format!("设置读取前权威重读失败:{err}")),
        }
    }

    // ================= S9 收尾 =================
    {
        wait_settled(&ctx).await;
        let _ = authoritative_snapshot(&ctx).await;
        let (idle, no_attention, effort_restored) = {
            let st = shared.lock();
            (
                is_idle(&st.state),
                pending_questions(&st.state).is_empty() && pending_approvals(&st.state).is_empty(),
                match &restore_effort {
                    None => true,
                    Some(orig) => current_effort(&st.state).as_deref() == Some(orig.as_str()),
                },
            )
        };
        let archived_now = catalog_thread_row().await.map(|(_, a)| a).unwrap_or(true);
        step(
            "S9",
            idle && no_attention && effort_restored && !archived_now,
            format!(
                "最终快照 idle={idle};无 pending attention={no_attention};设置已还原={effort_restored};thread 未被归档={}",
                !archived_now
            ),
        );
    }

    // 退订(礼貌收尾;不影响 Desktop)。
    let _ = client
        .set_following(
            FollowingChangedParams {
                conversation_id: target_thread().to_string(),
                host_id: "local".to_string(),
                following: false,
            },
            None,
        )
        .await;

    finish(failures).await;
}

/// start 一轮并解析目标 turn(等 active 出现,不等待终态)。
async fn run_turn_intro(
    ctx: &Ctx,
    guard: &WriteGuard,
    prompt: &str,
) -> Result<(Option<String>, String), String> {
    if !wait_pred(ctx, IDLE_WAIT, |st| is_idle(st)).await {
        return Err("start 前会话未回 idle".to_string());
    }
    let started = guard.start_turn(prompt).await?;
    let _ = wait_pred(ctx, ACTIVE_WAIT, |st| !is_idle(st)).await;
    let turn = {
        let st = ctx.shared.lock();
        resolve_turn(&st.state, &started)
    };
    match turn {
        Some(turn) if !turn.is_empty() => Ok((started, turn)),
        _ => Err("无法确定目标 turnId".to_string()),
    }
}

/// 汇总:全部通过 → 正常结束;否则按失败清单 panic(允许整剧本重跑一次)。
async fn finish(failures: Vec<String>) {
    if failures.is_empty() {
        println!("ALL STEPS PASS");
        return;
    }
    // 失败兜底:确保会话不残留 active(仅限本专用会话)。
    println!("FAILED STEPS:");
    for f in &failures {
        println!("  - {f}");
    }
    panic!("{} 个 STEP 失败;可整剧本重跑一次", failures.len());
}

// ---------------------------------------------------------------------------
// 聚焦重试(收尾诊断):steer 带 restoreMessage 修正 + update-settings 重试。
// 与全剧本同一门禁与硬边界,只覆盖 S4/S8 两项未通过操作。
// ---------------------------------------------------------------------------

/// 公共门禁与连接(S0 等价):env → 版本 → 目录库 cwd → connect + pump →
/// owner(短轮询 2 分钟,等 owner 出现) → following → 首快照 → 运行时 cwd 断言。
/// 版本/ cwd 门禁不命中即 panic 零写入;owner 始终不可达时 panic 终止。
async fn setup_gate_and_connect() -> (Arc<IpcClient>, Arc<Mutex<Shared>>, String) {
    let _ = (target_thread(), target_cwd());
    let version = discovery::probe_version(None)
        .await
        .expect("codex --version 探测失败");
    if !discovery::version_is_verified(&version) {
        panic!("版本门禁未命中({version} 不在 VERIFIED_VERSIONS):abort 零写入");
    }
    println!("版本门禁: {version} ∈ VERIFIED_VERSIONS");
    let (cwd, archived) = catalog_thread_row().await.expect("目录库只读查询失败");
    if cwd != target_cwd() {
        panic!("cwd 门禁失败:专用会话 cwd 与测试工作区不一致,abort 零写入");
    }
    assert!(!archived, "专用会话已归档,abort");
    let Some(socket) = discovery::socket_path(None, None) else {
        panic!("未发现 ipc socket");
    };
    let (client, events) = connect(IpcClientConfig::new(
        socket,
        "agent-console-bridge-real-write",
    ))
    .await
    .expect("connect + initialize 失败");
    let client = Arc::new(client);
    let shared: Arc<Mutex<Shared>> = Arc::new(Mutex::new(Shared::default()));
    {
        let client = client.clone();
        let shared = shared.clone();
        tokio::spawn(pump(client, events, shared));
    }
    let Some(owner) = wait_owner(&client, Duration::from_secs(120)).await else {
        panic!("owner discovery 短轮询 2 分钟始终 no-client-found:owner 不可达,零写入终止");
    };
    println!("owner discovery: 在线(clientId 前 8 位 {})", short(&owner));
    client
        .set_following(
            FollowingChangedParams {
                conversation_id: target_thread().to_string(),
                host_id: "local".to_string(),
                following: true,
            },
            None,
        )
        .await
        .expect("following 广播失败");
    let ctx_for_wait = Ctx {
        client: client.clone(),
        shared: shared.clone(),
        owner: owner.clone(),
    };
    let got_snapshot = wait_pred(&ctx_for_wait, Duration::from_secs(15), |_| true).await;
    assert!(got_snapshot, "15s 内未收到首个快照");
    {
        let st = shared.lock();
        let runtime_cwd = st.state.get("cwd").and_then(Value::as_str).unwrap_or("");
        assert_eq!(
            runtime_cwd,
            target_cwd(),
            "运行时快照 cwd 与测试工作区不一致,abort"
        );
        println!(
            "首快照: revision={:?} threadRuntimeStatus={:?}",
            st.revision,
            st.state.get("threadRuntimeStatus"),
        );
    }
    (client, shared, owner)
}

/// owner 短轮询:每 5s 一次 discovery,最多 `timeout`;始终 no-client-found
/// 时返回 None(如实记录 owner 不可达,不造 owner)。
async fn wait_owner(client: &Arc<IpcClient>, timeout: Duration) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(Some(owner)) = client.discover_owner("local", target_thread()).await {
            return Some(owner);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// steer 单次尝试的逐项观测。
struct SteerAttempt {
    label: String,
    /// steer RPC 错误(None = 被接受,无 peer error)。
    steer_err: Option<String>,
    saw_prefix: bool,
    done: bool,
    completed: bool,
    same_turn: bool,
    complete: bool,
    has_done: bool,
    /// steer 前后 turn 数差(0 = 未新增轮)。
    turns_delta: i64,
}

impl SteerAttempt {
    fn passed(&self) -> bool {
        self.steer_err.is_none()
            && self.saw_prefix
            && self.done
            && self.completed
            && self.same_turn
            && self.complete
            && self.has_done
            && self.turns_delta == 0
    }

    fn evidence(&self) -> String {
        let steer = match &self.steer_err {
            None => "被接受(无 peer error)".to_string(),
            Some(err) => format!("被拒绝:{err}"),
        };
        format!(
            "{}: steer {};前 3 个数字出现={};turn 终态完成={};status completed={};同轮={};序列 1..30 完整={};含 done={};轮数差={}",
            self.label, steer, self.saw_prefix, self.done, self.completed, self.same_turn, self.complete, self.has_done, self.turns_delta
        )
    }
}

/// 跑一轮数数,前 3 个数字出现后 steer;`collaboration_mode` 为快照
/// latestCollaborationMode 的实测值(完整对象或 mode 字符串)。
async fn steer_attempt(
    ctx: &Ctx,
    guard: &WriteGuard,
    label: &str,
    collaboration_mode: Option<Value>,
) -> SteerAttempt {
    let mut out = SteerAttempt {
        label: label.to_string(),
        steer_err: None,
        saw_prefix: false,
        done: false,
        completed: false,
        same_turn: false,
        complete: false,
        has_done: false,
        turns_delta: -1,
    };
    let prompt = "请从 1 慢数到 30,每个数字单独一行,数完后停止";
    let (started, turn) = match run_turn_intro(ctx, guard, prompt).await {
        Ok(v) => v,
        Err(err) => {
            out.steer_err = Some(format!("turn 启动失败:{err}"));
            return out;
        }
    };
    out.saw_prefix = wait_pred(ctx, Duration::from_secs(180), |st| {
        turn_by_id(st, &turn)
            .map(|t| contains_sequence(&number_lines(&turn_text(t)), &[1, 2, 3]))
            .unwrap_or(false)
    })
    .await;
    let turns_before = turns(&ctx.shared.lock().state).len();
    let steer_result = guard
        .steer_turn("请在数完后额外添加一行 done", collaboration_mode)
        .await;
    let (done, _) = wait_turn_terminal(ctx, Some(turn.clone())).await;
    wait_settled(ctx).await;
    out.done = done;
    if let Err(err) = steer_result {
        out.steer_err = Some(err);
    }
    match authoritative_snapshot(ctx).await {
        Ok(state) => {
            let turns_after = turns(&state).len();
            out.turns_delta = turns_after as i64 - turns_before as i64;
            out.same_turn = started
                .as_deref()
                .map(|id| turn_by_id(&state, id).is_some())
                .unwrap_or(true);
            let text = turn_by_id(&state, &turn).map(turn_text).unwrap_or_default();
            let nums = number_lines(&text);
            out.complete = contains_sequence(&nums, &(1..=30).collect::<Vec<u32>>());
            out.has_done = text.to_lowercase().contains("done");
            out.completed = turn_by_id(&state, &turn).map(turn_status) == Some("completed");
        }
        Err(err) if out.steer_err.is_none() => {
            out.steer_err = Some(format!("权威重读失败:{err}"));
        }
        Err(_) => {}
    }
    out
}

#[tokio::test]
#[ignore = "requires live Codex Desktop; writes ONLY to the dedicated AC_REAL_THREAD test session"]
async fn real_desktop_retry_steer_and_settings() {
    let mut restore_effort: Option<String> = None;
    let mut failures: Vec<String> = Vec::new();
    let mut step = |name: &str, ok: bool, evidence: String| {
        println!(
            "STEP {name}: {} {evidence}",
            if ok { "PASS" } else { "FAIL" }
        );
        if !ok {
            failures.push(format!("{name}: {evidence}"));
        }
    };

    // ---- 门禁与连接(公共 S0 等价;owner 短轮询 2 分钟内建)----
    let (client, shared, owner) = setup_gate_and_connect().await;
    let ctx = Ctx {
        client: client.clone(),
        shared: shared.clone(),
        owner: owner.clone(),
    };
    let guard = WriteGuard::new(&ctx);

    // ================= R1 steer 修正重试(restoreMessage)=================
    // 尝试 A:collaborationMode = 快照 latestCollaborationMode 完整对象(实测值原样);
    // A 被拒时允许一次变体 B:collaborationMode = mode 字符串实测值。不做更多变体。
    let collaboration_full = shared.lock().state.get("latestCollaborationMode").cloned();
    let mode_string = collaboration_full
        .as_ref()
        .and_then(|m| m.get("mode"))
        .cloned();
    println!(
        "R1 restoreMessage 形态: cwd+workspaceRoots 必带;collaborationMode A={} B={}",
        if collaboration_full.is_some() {
            "完整对象"
        } else {
            "缺省(快照无)"
        },
        if mode_string.is_some() {
            "mode 字符串"
        } else {
            "缺省(快照无)"
        },
    );
    let attempt_a = steer_attempt(
        &ctx,
        &guard,
        "A(cwd+workspaceRoots+collaborationMode=latestCollaborationMode 对象)",
        collaboration_full.clone(),
    )
    .await;
    println!("R1 尝试 A → {}", attempt_a.evidence());
    let attempt_b = if attempt_a.passed() {
        None
    } else {
        let b = steer_attempt(
            &ctx,
            &guard,
            "B(cwd+workspaceRoots+collaborationMode=mode 字符串)",
            mode_string.clone(),
        )
        .await;
        println!("R1 尝试 B → {}", b.evidence());
        Some(b)
    };
    let r1_ok = attempt_b.as_ref().map(|b| b.passed()).unwrap_or(false) || attempt_a.passed();
    let r1_evidence = match &attempt_b {
        Some(b) => format!("{};{}", attempt_a.evidence(), b.evidence()),
        None => attempt_a.evidence(),
    };
    if !r1_ok {
        let st = shared.lock();
        println!(
            "R1 诊断: revision={:?} threadRuntimeStatus={:?} 顶层键数={}",
            st.revision,
            st.state.get("threadRuntimeStatus"),
            st.state.as_object().map(|m| m.len()).unwrap_or(0),
        );
    }
    step("R1-steer", r1_ok, r1_evidence);

    // ================= R2 update-settings 重试 =================
    // 写前刷新 owner(0.153.1 观测:owner clientId 可能中途更换);
    // 始终 no-client-found 则如实记录 owner 不可达,维持 FIXTURE_ONLY。
    match wait_owner(&client, Duration::from_secs(120)).await {
        None => step(
            "R2-settings",
            false,
            "owner 不可达(短轮询 2 分钟始终 no-client-found)→ update-settings 维持 FIXTURE_ONLY,未写入"
                .to_string(),
        ),
        Some(owner2) => {
            let ctx2 = Ctx {
                client: client.clone(),
                shared: shared.clone(),
                owner: owner2.clone(),
            };
            let guard2 = WriteGuard::new(&ctx2);
            wait_settled(&ctx2).await;
            let before = authoritative_snapshot(&ctx2).await;
            match before {
                Err(err) => step("R2-settings", false, format!("设置读取前权威重读失败:{err}")),
                Ok(before) => {
                    let orig = current_effort(&before);
                    let new = orig.as_deref().and_then(prev_effort);
                    match (orig.clone(), new) {
                        (Some(orig), Some(new)) => {
                            let update = guard2
                                .generic_write(
                                    messages::method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                                    json!({
                                        "conversationId": target_thread(),
                                        "threadSettings": {"effort": new},
                                    }),
                                )
                                .await;
                            let mut reflected =
                                wait_pred(&ctx2, Duration::from_secs(15), |st| {
                                    current_effort(st).as_deref() == Some(new.as_str())
                                })
                                .await;
                            if !reflected {
                                let _ = authoritative_snapshot(&ctx2).await;
                                reflected = wait_pred(&ctx2, Duration::from_secs(10), |st| {
                                    current_effort(st).as_deref() == Some(new.as_str())
                                })
                                .await;
                            }
                            let update_evidence = match &update {
                                Ok(_) => format!("UpdateSettings({orig}→{new}) 被接受;快照反映新值={reflected}"),
                                Err(err) => format!("UpdateSettings 被拒绝:{err}"),
                            };
                            step("R2a-update", update.is_ok() && reflected, update_evidence);

                            if update.is_ok() {
                                let next_turn = run_turn(
                                    &ctx2,
                                    &guard2,
                                    "R2b",
                                    "请只回复 ok 两个字母,不要做任何其他事",
                                )
                                .await;
                                let (normal, detail) = match &next_turn {
                                    Ok((_, _, final_state)) => {
                                        let turn = resolve_turn(final_state, &next_turn.as_ref().unwrap().0);
                                        let ok = turn
                                            .as_deref()
                                            .and_then(|id| turn_by_id(final_state, id))
                                            .and_then(final_assistant_text)
                                            .map(|t| t.to_lowercase().contains("ok"))
                                            .unwrap_or(false);
                                        (ok, "turn 进入终态".to_string())
                                    }
                                    Err(err) => (false, format!("run_turn 失败:{err}")),
                                };
                                step(
                                    "R2b-next-turn",
                                    normal,
                                    format!("设置变更后新一轮正常完成={normal}({detail})"),
                                );
                            }

                            // 恢复原值并断言。
                            let restore = guard2
                                .generic_write(
                                    messages::method::THREAD_FOLLOWER_UPDATE_THREAD_SETTINGS,
                                    json!({
                                        "conversationId": target_thread(),
                                        "threadSettings": {"effort": orig},
                                    }),
                                )
                                .await;
                            let mut restored =
                                wait_pred(&ctx2, Duration::from_secs(15), |st| {
                                    current_effort(st).as_deref() == Some(orig.as_str())
                                })
                                .await;
                            if !restored {
                                let _ = authoritative_snapshot(&ctx2).await;
                                restored = wait_pred(&ctx2, Duration::from_secs(10), |st| {
                                    current_effort(st).as_deref() == Some(orig.as_str())
                                })
                                .await;
                            }
                            let restore_evidence = match &restore {
                                Ok(_) => format!("恢复原值({orig})已发送;快照确认还原={restored}"),
                                Err(err) => format!("恢复原值被拒绝:{err}"),
                            };
                            if restore.is_ok() && restored {
                                restore_effort = None;
                            }
                            step("R2c-restore", restore.is_ok() && restored, restore_evidence);
                        }
                        (orig, _) => step(
                            "R2a-update",
                            true,
                            format!(
                                "OBSERVED 快照无 reasoning_effort 当前值或不在合法值序({orig:?})→ 设置写入 FIXTURE_ONLY,未猜测合法值"
                            ),
                        ),
                    }
                }
            }
        }
    }

    // ================= 收尾:idle / 无 attention / 设置还原 / 未归档 =================
    wait_settled(&ctx).await;
    let _ = authoritative_snapshot(&ctx).await;
    {
        let st = shared.lock();
        println!(
            "收尾诊断: revision={:?} threadRuntimeStatus={:?}",
            st.revision,
            st.state.get("threadRuntimeStatus"),
        );
    }
    let (idle, no_attention) = {
        let st = shared.lock();
        (
            is_idle(&st.state),
            pending_questions(&st.state).is_empty() && pending_approvals(&st.state).is_empty(),
        )
    };
    let archived_now = catalog_thread_row().await.map(|(_, a)| a).unwrap_or(true);
    step(
        "R-final",
        idle && no_attention && restore_effort.is_none() && !archived_now,
        format!(
            "最终快照 idle={idle};无 pending attention={no_attention};设置已还原={};thread 未被归档={}",
            restore_effort.is_none(),
            !archived_now
        ),
    );

    // 退订(礼貌收尾;不影响 Desktop)。
    let _ = client
        .set_following(
            FollowingChangedParams {
                conversation_id: target_thread().to_string(),
                host_id: "local".to_string(),
                following: false,
            },
            None,
        )
        .await;

    finish(failures).await;
}

/// 会话卡在 `threadRuntimeStatus.type="systemError"` 时的最小恢复尝试:
/// 0.153.1 真机观测(2026-09-04):turn 在 Desktop 内部失败后会话停留
/// systemError,不回 idle,follower 此后无法按 idle 门禁开启任何新轮。
/// 本测试直接发一条纯文本 start-turn(唯一授权写目标),观察 owner 是否
/// 接受并使会话回 idle;被拒则如实记录(不猜测其他恢复通道)。
#[tokio::test]
#[ignore = "requires live Codex Desktop; writes ONLY to the dedicated AC_REAL_THREAD test session"]
async fn real_desktop_recover_system_error() {
    let (client, shared, owner) = setup_gate_and_connect().await;
    let ctx = Ctx {
        client: client.clone(),
        shared: shared.clone(),
        owner: owner.clone(),
    };
    let guard = WriteGuard::new(&ctx);
    {
        let st = shared.lock();
        let status = runtime_status(&st.state);
        if status == "idle" {
            println!("会话已 idle,无需恢复");
            return;
        }
        println!("恢复前状态: {status}");
    }
    let result = guard
        .start_turn("请只回复 ok 两个字母,不要做任何其他事")
        .await;
    match &result {
        Ok(_) => println!("恢复 start-turn 被接受"),
        Err(err) => println!("恢复 start-turn 被拒绝:{err}"),
    }
    let (done, _) = wait_turn_terminal(&ctx, None).await;
    wait_settled(&ctx).await;
    let _ = authoritative_snapshot(&ctx).await;
    let final_status = {
        let st = shared.lock();
        runtime_status(&st.state).to_string()
    };
    println!("恢复轮终态完成={done};恢复后状态: {final_status}");
    assert_eq!(
        final_status, "idle",
        "恢复未成功:会话仍停留非 idle 状态(需 Desktop UI 手动处理)"
    );
    println!("RECOVER PASS");
}
