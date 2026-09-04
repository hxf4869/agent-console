//! fake-codex-owner:独立进程的假 Codex Desktop(路由器 + owner),供
//! CodexAdapter 集成测试与无 UI e2e 使用(执行规格 §13.4/§29.4)。
//!
//! 行为契约(与 docs/CODEX-IPC-PROTOCOL.md 一致,全部为合成数据):
//! - 接受多客户端连接;`initialize` → 分配 clientId。
//! - `thread-owner-discovery` → 已知会话由本进程虚拟 owner("fake-owner")
//!   应答 `supportsUntrustedAppInput`;未知会话回 `no-client-found`。
//! - broadcast `thread-stream-following-changed` → 登记 follower 并回发全量
//!   快照(定向 targetClientIds)。
//! - `thread-follower-start-turn` → 建立 inProgress turn,按脚本逐行输出
//!   (Immer patch);`finalOutput` 与预览不同即可驱动 OutputReplace 校正;
//!   `questionAfterLine` + `question`/`approval` 可脚本触发 attention 并暂停
//!   直到 `thread-follower-submit-user-input` / 审批决策回答。
//! - `thread-follower-steer-turn` → 注入固定行 "steer-accepted" 继续输出;
//!   idle 时回 `SteerTurnInactiveError`。
//! - `thread-follower-interrupt-turn` → 终止输出并广播 interrupted 终态快照;
//!   idle 时回 `NoActiveTurn`。
//! - `thread-follower-load-complete-history` → 回 `{revision}` 并重发快照。
//!
//! 脚本格式(命令行参数 2:JSON 文件):
//! ```json
//! {
//!   "sessions": [{
//!     "conversationId": "11111111-1111-4111-8111-111111111111",
//!     "title": "fixture-alpha",
//!     "cwd": "/tmp/fixture-alpha",
//!     "branch": "fixture-branch",
//!     "model": "gpt-5.3-fixture",
//!     "reasoningEffort": "medium",
//!     "latestCollaborationMode": {"mode": "fixture", "settings": {}},
//!     "approvalPolicy": "untrusted",
//!     "pendingQuestions": [],
//!     "pendingApprovals": [],
//!     "backgroundCommands": [
//!       {"id": "bg-e2e-1", "display": "fixture-bg-server", "state": "running"}
//!     ],
//!     "turn": {
//!       "outputLines": ["1", "2", "3"],
//!       "lineDelayMs": 30,
//!       "finalAnswer": "fixture-final",
//!       "finalOutput": null,
//!       "fail": false,
//!       "questionAfterLine": null,
//!       "question": null,
//!       "approval": null
//!     }
//!   }]
//! }
//! ```
//!
//! `latestCollaborationMode`(可选)按投影契约整体透传进 conversationState,
//! 其 `settings` 可携带 `effortAvailableValues` 等动态字段;缺省为
//! `{"mode": "fixture", "settings": {}}`。
//!
//! `backgroundCommands`(§10.8/§23.2 脚本扩展,无 UI e2e 场景 9 使用):
//! turn 开始时作为 `commandExecution` item 与主命令并行注入(带 `background: true`
//! 内部标记),`emit_output` 与终态校正跳过这些 item;turn 结束后仍保持
//! `status: "inProgress"` 与自身聚合输出 —— 投影层由此派生“turn 完成后仍在
//! 运行的后台命令”(turn 非运行 + item 运行 → BackgroundCommand,见
//! `mapper::derive_commands`)。`state` 支持 `running`/`completed`/`failed`,
//! 分别映射 inProgress/completed/failed。
//!
//! 日志走 stderr,只含方法名/会话 ID/turn 状态,不回显任何正文(§25.3)。
//! 协议帧实现复用 `bridge::adapter::codex::ipc::frame`。
//!
//! 入口:`run_server(socket_path, script)` —— library 函数,供 headless bin
//! (`fake-codex-owner`)与无 UI e2e(进程内 spawn)复用。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::adapter::codex::ipc::frame::{encode_frame, FrameDecoder, DEFAULT_MAX_FRAME_BYTES};

/// 虚拟 owner 的 clientId。
const OWNER_CLIENT_ID: &str = "fake-owner";
const MAX_FRAME: u32 = DEFAULT_MAX_FRAME_BYTES;
/// 状态轮询间隔(fake 内部;不影响协议语义)。
const POLL: Duration = Duration::from_millis(20);
/// attention 回答等待上限。
const ATTENTION_TIMEOUT: Duration = Duration::from_secs(30);

/// 启动 fake IPC owner:绑定 `socket_path`(长度前缀 JSON frame 协议),
/// 按 `script` 提供虚拟 owner 会话;直到 listener 出错才返回。
pub async fn run_server(socket_path: PathBuf, script: Value) -> std::io::Result<()> {
    // socket 文件归 fake 进程所有;启动前清掉残留(绝不触碰真实 Desktop socket)。
    let _ = std::fs::remove_file(&socket_path);
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let listener = UnixListener::bind(&socket_path)?;
    eprintln!("[fake-owner] listening on {}", socket_path.display());

    let mut sessions = HashMap::new();
    for node in script
        .get("sessions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let Some(conversation) = node.get("conversationId").and_then(Value::as_str) else {
            continue;
        };
        sessions.insert(
            conversation.to_string(),
            SessionState {
                script: node.clone(),
                state: initial_state(node),
                revision: 0,
                active: false,
                turn_counter: 0,
                interrupt: false,
                waiting_attention: false,
                injected_lines: Vec::new(),
            },
        );
    }

    let server = Arc::new(Server {
        sessions: Arc::new(Mutex::new(sessions)),
        conns: Arc::new(Mutex::new(HashMap::new())),
        followers: Arc::new(Mutex::new(HashMap::new())),
        conn_counter: AtomicU64::new(0),
    });

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let server = server.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(server, stream).await {
                eprintln!("[fake-owner] connection error: {err}");
            }
        });
    }
    Ok(())
}

/// 单个脚本会话的运行时状态。
struct SessionState {
    script: Value,
    /// conversationState(投影契约见 bridge::adapter::codex::projection)。
    state: Value,
    revision: u64,
    active: bool,
    turn_counter: u64,
    /// interrupt_turn 置位,run_turn 轮询。
    interrupt: bool,
    /// attention 已注入、等待回答。
    waiting_attention: bool,
    /// steer 注入的追加输出行。
    injected_lines: Vec<String>,
}

struct Server {
    sessions: Arc<Mutex<HashMap<String, SessionState>>>,
    /// clientId → 出站通道。
    conns: Arc<Mutex<HashMap<String, mpsc::Sender<Value>>>>,
    /// clientId → 关注的会话集合(一个 client 可同时 follow 多会话)。
    followers: Arc<Mutex<HashMap<String, HashSet<String>>>>,
    conn_counter: AtomicU64,
}

/// 由脚本节点构造初始 conversationState(投影契约键)。
fn initial_state(node: &Value) -> Value {
    let conversation = node
        .get("conversationId")
        .and_then(Value::as_str)
        .unwrap_or("");
    json!({
        "id": conversation,
        "title": node.get("title").and_then(Value::as_str).unwrap_or("fixture"),
        "cwd": node.get("cwd").and_then(Value::as_str).unwrap_or("/tmp/fixture"),
        "hostId": "local",
        "threadRuntimeStatus": {"type": "idle", "activeFlags": []},
        "latestModel": node.get("model").and_then(Value::as_str),
        "latestReasoningEffort": node.get("reasoningEffort").and_then(Value::as_str),
        // 投影契约透传:脚本可提供 latestCollaborationMode(settings 内可携带
        // effortAvailableValues 等动态字段);缺省保持 fixture 默认形态。
        "latestCollaborationMode": node
            .get("latestCollaborationMode")
            .cloned()
            .unwrap_or_else(|| json!({"mode": "fixture", "settings": {}})),
        "gitInfo": {"branch": node.get("branch").and_then(Value::as_str), "sha": null},
        "currentPermissions": {
            "approvalPolicy": node.get("approvalPolicy").and_then(Value::as_str).unwrap_or("untrusted")
        },
        "turns": [],
        "pendingQuestions": node.get("pendingQuestions").cloned().unwrap_or(json!([])),
        "pendingApprovals": node.get("pendingApprovals").cloned().unwrap_or(json!([])),
        "createdAt": 1_000i64,
        "updatedAt": 1_000i64,
        "recencyAt": 1_000i64,
    })
}

async fn handle_connection(server: Arc<Server>, stream: UnixStream) -> Result<(), String> {
    let client_id = format!(
        "client-{}",
        server.conn_counter.fetch_add(1, Ordering::SeqCst)
    );
    let (mut read_half, mut write_half) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(256);
    server
        .conns
        .lock()
        .insert(client_id.clone(), out_tx.clone());

    // 写任务:出站队列 → 长度前缀帧。
    let writer = tokio::spawn(async move {
        while let Some(value) = out_rx.recv().await {
            let raw = encode_frame(&value, MAX_FRAME).map_err(|e| e.to_string())?;
            write_half
                .write_all(&raw)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok::<(), String>(())
    });

    // 读任务:帧 → 消息分发。
    let mut decoder = FrameDecoder::new(MAX_FRAME);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut read_result = Ok(());
    loop {
        match read_half.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                if let Err(err) = decoder.push(&chunk[..n]) {
                    read_result = Err(err.to_string());
                    break;
                }
                loop {
                    match decoder.next_frame() {
                        Ok(Some(value)) => {
                            if let Err(err) =
                                dispatch(server.clone(), &client_id, value, &out_tx).await
                            {
                                read_result = Err(err);
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            read_result = Err(err.to_string());
                            break;
                        }
                    }
                }
                if read_result.is_err() {
                    break;
                }
            }
            Err(err) => {
                read_result = Err(err.to_string());
                break;
            }
        }
    }
    server.conns.lock().remove(&client_id);
    server.followers.lock().remove(&client_id);
    let _ = out_tx.send(Value::Null).await; // 结束写任务
    let _ = writer.await;
    read_result
}

/// 推进 revision 并广播;`mutate` 返回 Some(patches) 走 patch 通道,
/// None 走全量 snapshot。锁内完成状态修改与 revision 推进。
async fn push_change<F>(server: &Arc<Server>, conversation: &str, mutate: F)
where
    F: FnOnce(&mut SessionState) -> Option<Vec<Value>>,
{
    let Some(change) = ({
        let mut sessions = server.sessions.lock();
        let Some(session) = sessions.get_mut(conversation) else {
            return;
        };
        let patches = mutate(session);
        session.revision += 1;
        let revision = session.revision;
        match patches {
            Some(ops) => Some(json!({
                "type": "patches",
                "baseRevision": revision - 1,
                "revision": revision,
                "patches": ops,
            })),
            None => Some(json!({
                "type": "snapshot",
                "revision": revision,
                "conversationState": session.state,
            })),
        }
    }) else {
        return;
    };
    broadcast_change(server, conversation, change, None).await;
}

async fn dispatch(
    server: Arc<Server>,
    client_id: &str,
    value: Value,
    out_tx: &mpsc::Sender<Value>,
) -> Result<(), String> {
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "request" => {
            let method = value.get("method").and_then(Value::as_str).unwrap_or("");
            let request_id = value
                .get("requestId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let params = value.get("params").cloned().unwrap_or(json!({}));
            let conversation = conversation_of(&params);
            eprintln!("[fake-owner] request {method} conv={conversation}");
            match method {
                "initialize" => {
                    respond(
                        out_tx,
                        &request_id,
                        method,
                        true,
                        json!({"clientId": client_id}),
                        None,
                    );
                }
                "thread-owner-discovery" => {
                    let known = server.sessions.lock().contains_key(&conversation);
                    if known {
                        respond(
                            out_tx,
                            &request_id,
                            method,
                            true,
                            json!({"supportsUntrustedAppInput": true}),
                            None,
                        );
                    } else {
                        respond(
                            out_tx,
                            &request_id,
                            method,
                            false,
                            json!({}),
                            Some("no-client-found".to_string()),
                        );
                    }
                }
                "thread-follower-start-turn" => {
                    start_turn(&server, out_tx, &request_id, &conversation).await;
                }
                "thread-follower-steer-turn" => {
                    steer_turn(&server, out_tx, &request_id, &conversation);
                }
                "thread-follower-interrupt-turn" => {
                    interrupt_turn(&server, out_tx, &request_id, &conversation);
                }
                "thread-follower-load-complete-history" => {
                    let revision = {
                        let mut sessions = server.sessions.lock();
                        let Some(session) = sessions.get_mut(&conversation) else {
                            respond(
                                out_tx,
                                &request_id,
                                method,
                                false,
                                json!({}),
                                Some("no-client-found".to_string()),
                            );
                            return Ok(());
                        };
                        session.revision += 1;
                        session.revision
                    };
                    respond(
                        out_tx,
                        &request_id,
                        method,
                        true,
                        json!({"revision": revision}),
                        None,
                    );
                    let snapshot = {
                        let sessions = server.sessions.lock();
                        sessions.get(&conversation).map(|s| {
                            json!({
                                "type": "snapshot",
                                "revision": s.revision,
                                "conversationState": s.state,
                            })
                        })
                    };
                    if let Some(change) = snapshot {
                        broadcast_change(&server, &conversation, change, Some(client_id)).await;
                    }
                }
                // 回答问题 / 审批决策:移除对应 pending 项并广播终态。
                "thread-follower-submit-user-input"
                | "thread-follower-command-approval-decision"
                | "thread-follower-file-approval-decision"
                | "thread-follower-permissions-request-approval-response" => {
                    let native_id = params
                        .get("requestId")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let removed = {
                        let mut sessions = server.sessions.lock();
                        let mut removed = false;
                        if let Some(session) = sessions.get_mut(&conversation) {
                            for key in ["pendingQuestions", "pendingApprovals"] {
                                if let Some(list) =
                                    session.state.get_mut(key).and_then(Value::as_array_mut)
                                {
                                    let before = list.len();
                                    list.retain(|item| {
                                        item.get("id").and_then(Value::as_str) != Some(&native_id)
                                    });
                                    removed = removed || list.len() != before;
                                }
                            }
                            if removed {
                                session.waiting_attention = false;
                            }
                        }
                        removed
                    };
                    respond(out_tx, &request_id, method, true, json!({"ok": true}), None);
                    if removed {
                        push_change(&server, &conversation, |_| None).await;
                        eprintln!("[fake-owner] attention {native_id} answered");
                    }
                }
                "thread-follower-update-thread-settings" => {
                    apply_settings(&server, &conversation, params.get("threadSettings"));
                    respond(out_tx, &request_id, method, true, json!({"ok": true}), None);
                    push_change(&server, &conversation, |_| None).await;
                }
                _ => {
                    respond(
                        out_tx,
                        &request_id,
                        method,
                        false,
                        json!({}),
                        Some("no-handler-for-request".to_string()),
                    );
                }
            }
            Ok(())
        }
        "broadcast" => {
            let method = value.get("method").and_then(Value::as_str).unwrap_or("");
            let params = value.get("params").cloned().unwrap_or(json!({}));
            if method == "thread-stream-following-changed" {
                let conversation = conversation_of(&params);
                let following = params.get("following").and_then(Value::as_bool) == Some(true);
                if following {
                    server
                        .followers
                        .lock()
                        .entry(client_id.to_string())
                        .or_default()
                        .insert(conversation.clone());
                    let snapshot = {
                        let sessions = server.sessions.lock();
                        sessions.get(&conversation).map(|s| {
                            json!({
                                "type": "snapshot",
                                "revision": s.revision,
                                "conversationState": s.state,
                            })
                        })
                    };
                    if let Some(change) = snapshot {
                        broadcast_change(&server, &conversation, change, Some(client_id)).await;
                    }
                    eprintln!("[fake-owner] follower {client_id} -> {conversation}");
                } else {
                    let mut g = server.followers.lock();
                    if let Some(convs) = g.get_mut(client_id) {
                        convs.remove(&conversation);
                        if convs.is_empty() {
                            g.remove(client_id);
                        }
                    }
                }
            }
            Ok(())
        }
        "client-discovery-request" => {
            let request_id = value.get("requestId").and_then(Value::as_str).unwrap_or("");
            out_tx
                .send(json!({
                    "type": "client-discovery-response",
                    "requestId": request_id,
                    "response": {"canHandle": false},
                }))
                .await
                .map_err(|_| "outbound closed".to_string())
        }
        _ => Ok(()),
    }
}

fn conversation_of(params: &Value) -> String {
    params
        .get("conversationId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn respond(
    out_tx: &mpsc::Sender<Value>,
    request_id: &str,
    method: &str,
    success: bool,
    result: Value,
    error: Option<String>,
) {
    let mut frame = json!({
        "type": "response",
        "requestId": request_id,
        "resultType": if success { "success" } else { "error" },
        "method": method,
        "handledByClientId": OWNER_CLIENT_ID,
    });
    if success {
        frame["result"] = result;
    } else {
        frame["error"] = json!(error.unwrap_or_else(|| "error".to_string()));
    }
    let _ = out_tx.try_send(frame);
}

/// 广播 snapshot/patches 给关注该会话的 follower(可限定单 client)。
async fn broadcast_change(
    server: &Arc<Server>,
    conversation: &str,
    change: Value,
    only: Option<&str>,
) {
    let frame = json!({
        "type": "broadcast",
        "method": "thread-stream-state-changed",
        "sourceClientId": OWNER_CLIENT_ID,
        "version": 11,
        "params": {
            "conversationId": conversation,
            "hostId": "local",
            "change": change,
        },
    });
    let targets: Vec<mpsc::Sender<Value>> = {
        let followers = server.followers.lock();
        let conns = server.conns.lock();
        followers
            .iter()
            .filter(|(client, convs)| {
                convs.contains(&conversation.to_string())
                    && only.map(|o| o == client.as_str()).unwrap_or(true)
            })
            .filter_map(|(client, _)| conns.get(client).cloned())
            .collect()
    };
    for target in targets {
        let _ = target.send(frame.clone()).await;
    }
}

async fn start_turn(
    server: &Arc<Server>,
    out_tx: &mpsc::Sender<Value>,
    request_id: &str,
    conversation: &str,
) {
    let conversation_owned = conversation.to_string();
    let turn_id;
    {
        let mut sessions = server.sessions.lock();
        let Some(session) = sessions.get_mut(conversation) else {
            respond(
                out_tx,
                request_id,
                "thread-follower-start-turn",
                false,
                json!({}),
                Some("no-client-found".to_string()),
            );
            return;
        };
        if session.active {
            respond(
                out_tx,
                request_id,
                "thread-follower-start-turn",
                false,
                json!({}),
                Some("turn-already-active".to_string()),
            );
            return;
        }
        session.turn_counter += 1;
        turn_id = format!("turn-{}", session.turn_counter);
        let command_item = json!({
            "id": format!("item-cmd-{turn_id}"),
            "type": "commandExecution",
            "command": "fixture-cmd",
            "status": "inProgress",
            "aggregatedOutput": "",
        });
        let turns = session
            .state
            .get_mut("turns")
            .and_then(Value::as_array_mut)
            .expect("state.turns array");
        let mut items = vec![command_item];
        // 脚本扩展 backgroundCommands(§10.8/§23.2):与主命令并行注入,
        // 带 background 标记;输出与终态校正跳过它们(见 emit_output/run_turn)。
        for bg in session
            .script
            .get("backgroundCommands")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let Some(id) = bg.get("id").and_then(Value::as_str) else {
                continue;
            };
            let display = bg
                .get("display")
                .and_then(Value::as_str)
                .unwrap_or("fixture-bg-command");
            let status = match bg.get("state").and_then(Value::as_str) {
                Some("completed") => "completed",
                Some("failed") => "failed",
                _ => "inProgress",
            };
            items.push(json!({
                "id": id,
                "type": "commandExecution",
                "command": display,
                "status": status,
                "aggregatedOutput": format!("{display} output\n"),
                "background": true,
            }));
        }
        turns.push(json!({
            "turnId": turn_id,
            "status": "inProgress",
            "items": items,
            "durationMs": Value::Null,
            "error": Value::Null,
        }));
        session.active = true;
        session.interrupt = false;
        session.injected_lines.clear();
    }
    respond(
        out_tx,
        request_id,
        "thread-follower-start-turn",
        true,
        json!({"result": {"turnId": turn_id, "status": "inProgress"}}),
        None,
    );
    // 初始快照(turn 建立;mapper 侧产生 TurnLifecycle + ItemUpsert)。
    push_change(server, &conversation_owned, |_| None).await;
    let server2 = server.clone();
    let conversation2 = conversation_owned.clone();
    let turn_id2 = turn_id.clone();
    tokio::spawn(async move {
        run_turn(server2, conversation2, turn_id2).await;
    });
}

/// 脚本驱动的 turn 状态机:active → 逐行 patch → 终态 + 权威快照。
async fn run_turn(server: Arc<Server>, conversation: String, turn_id: String) {
    let (lines, line_delay, fail, question_after, question, approval, final_answer, final_output) = {
        let sessions = server.sessions.lock();
        let Some(session) = sessions.get(&conversation) else {
            return;
        };
        let turn = session.script.get("turn").cloned().unwrap_or(json!({}));
        (
            turn.get("outputLines")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            turn.get("lineDelayMs")
                .and_then(Value::as_u64)
                .unwrap_or(30),
            turn.get("fail").and_then(Value::as_bool).unwrap_or(false),
            turn.get("questionAfterLine")
                .and_then(Value::as_u64)
                .map(|n| n as usize),
            turn.get("question").cloned().filter(|v| !v.is_null()),
            turn.get("approval").cloned().filter(|v| !v.is_null()),
            turn.get("finalAnswer")
                .and_then(Value::as_str)
                .unwrap_or("fixture-final")
                .to_string(),
            turn.get("finalOutput")
                .and_then(Value::as_str)
                .map(String::from),
        )
    };

    let mut streamed = String::new();
    let mut interrupted = false;

    let set_active_status = |server: &Arc<Server>, conversation: &str, active: &str| {
        let mut sessions = server.sessions.lock();
        if let Some(session) = sessions.get_mut(conversation) {
            if let Some(runtime) = session.state.get_mut("threadRuntimeStatus") {
                *runtime = json!({"type": active, "activeFlags": []});
            }
        }
    };
    set_active_status(&server, &conversation, "active");

    let mut line_index = 0usize;
    'outer: loop {
        // 先输出 steer 注入行。
        loop {
            let injected = {
                let mut sessions = server.sessions.lock();
                sessions.get_mut(&conversation).and_then(|s| {
                    if s.injected_lines.is_empty() {
                        None
                    } else {
                        Some(s.injected_lines.remove(0))
                    }
                })
            };
            match injected {
                Some(extra) => {
                    streamed.push_str(&extra);
                    streamed.push('\n');
                    emit_output(&server, &conversation, &turn_id, &streamed).await;
                }
                None => break,
            }
        }
        // 中断检查。
        if server
            .sessions
            .lock()
            .get(&conversation)
            .map(|s| s.interrupt)
            .unwrap_or(true)
        {
            interrupted = true;
            break 'outer;
        }
        // 常规行。
        let Some(line) = lines.get(line_index) else {
            break 'outer;
        };
        line_index += 1;
        streamed.push_str(line);
        streamed.push('\n');
        emit_output(&server, &conversation, &turn_id, &streamed).await;
        eprintln!(
            "[fake-owner] conv={conversation} turn={turn_id} line={} bytes={}",
            line_index,
            streamed.len()
        );

        // 脚本触发问题/审批并等待回答。
        if Some(line_index) == question_after {
            if let Some(question) = &question {
                inject_pending(&server, &conversation, "pendingQuestions", question).await;
            }
            if let Some(approval) = &approval {
                inject_pending(&server, &conversation, "pendingApprovals", approval).await;
            }
            let deadline = tokio::time::Instant::now() + ATTENTION_TIMEOUT;
            loop {
                let (waiting, interrupt) = {
                    let sessions = server.sessions.lock();
                    let Some(session) = sessions.get(&conversation) else {
                        break 'outer;
                    };
                    (session.waiting_attention, session.interrupt)
                };
                if interrupt {
                    interrupted = true;
                    break 'outer;
                }
                if !waiting {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    eprintln!("[fake-owner] attention wait timed out conv={conversation}");
                    break;
                }
                tokio::time::sleep(POLL).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(line_delay)).await;
    }

    // 终态:权威快照(finalOutput 与预览不同 → mapper 走 OutputReplace 校正)。
    let authoritative = final_output.unwrap_or(streamed);
    let item_status = if interrupted {
        "interrupted"
    } else if fail {
        "failed"
    } else {
        "completed"
    };
    let exit_code = if interrupted || fail { 130 } else { 0 };
    let turn_status = if interrupted {
        "interrupted"
    } else if fail {
        "failed"
    } else {
        "completed"
    };
    let final_answer_item = if !interrupted && !fail {
        Some(json!({
            "id": format!("item-final-{turn_id}"),
            "type": "agentMessage",
            "text": final_answer,
            "phase": "final_answer",
        }))
    } else {
        None
    };
    push_change(&server, &conversation, |session| {
        session.active = false;
        session.interrupt = false;
        session.waiting_attention = false;
        if let Some(runtime) = session.state.get_mut("threadRuntimeStatus") {
            *runtime = json!({"type": "idle", "activeFlags": []});
        }
        if let Some(turns) = session.state.get_mut("turns").and_then(Value::as_array_mut) {
            if let Some(turn) = turns
                .iter_mut()
                .find(|t| t.get("turnId").and_then(Value::as_str) == Some(turn_id.as_str()))
            {
                turn["status"] = json!(turn_status);
                turn["durationMs"] = json!(1_000);
                if let Some(items) = turn.get_mut("items").and_then(Value::as_array_mut) {
                    for item in items.iter_mut() {
                        // backgroundCommands item:turn 终态后仍保持运行状态
                        // 与自身聚合输出(§10.8:主 turn 完成后后台命令继续)。
                        if item.get("background").and_then(Value::as_bool) == Some(true) {
                            continue;
                        }
                        if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
                            item["status"] = json!(item_status);
                            item["aggregatedOutput"] = json!(authoritative);
                            item["exitCode"] = json!(exit_code);
                        }
                    }
                    if let Some(final_item) = final_answer_item {
                        items.push(final_item);
                    }
                }
            }
        }
        None // 全量快照(§13.3 权威来源)
    })
    .await;
    eprintln!("[fake-owner] conv={conversation} turn={turn_id} finished status={turn_status}");
}

/// 输出一行:replace patch 更新 turns 数组中的 aggregatedOutput(累积值)。
async fn emit_output(server: &Arc<Server>, conversation: &str, turn_id: &str, streamed: &str) {
    let turns_value = {
        let sessions = server.sessions.lock();
        sessions
            .get(conversation)
            .map(|s| s.state.get("turns").cloned().unwrap_or(json!([])))
            .unwrap_or(json!([]))
    };
    let mut turns = turns_value;
    if let Some(list) = turns.as_array_mut() {
        for turn in list.iter_mut() {
            if turn.get("turnId").and_then(Value::as_str) == Some(turn_id) {
                if let Some(items) = turn.get_mut("items").and_then(Value::as_array_mut) {
                    for item in items.iter_mut() {
                        // backgroundCommands item:保持自身状态与聚合输出,
                        // 不跟随主命令输出流(§10.8)。
                        if item.get("background").and_then(Value::as_bool) == Some(true) {
                            continue;
                        }
                        if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
                            item["aggregatedOutput"] = json!(streamed);
                        }
                    }
                }
            }
        }
    }
    // push_change 的 patches 参数就是 op 数组,不要再包一层。
    let ops = vec![json!({"op": "replace", "path": ["turns"], "value": turns})];
    push_change(server, conversation, |_| Some(ops)).await;
}

fn steer_turn(
    server: &Arc<Server>,
    out_tx: &mpsc::Sender<Value>,
    request_id: &str,
    conversation: &str,
) {
    let mut sessions = server.sessions.lock();
    let Some(session) = sessions.get_mut(conversation) else {
        respond(
            out_tx,
            request_id,
            "thread-follower-steer-turn",
            false,
            json!({}),
            Some("no-client-found".to_string()),
        );
        return;
    };
    if session.active {
        session.injected_lines.push("steer-accepted".to_string());
        respond(
            out_tx,
            request_id,
            "thread-follower-steer-turn",
            true,
            json!({"result": {"accepted": true}}),
            None,
        );
        eprintln!("[fake-owner] conv={conversation} steered");
    } else {
        respond(
            out_tx,
            request_id,
            "thread-follower-steer-turn",
            false,
            json!({}),
            Some("SteerTurnInactiveError".to_string()),
        );
    }
}

fn interrupt_turn(
    server: &Arc<Server>,
    out_tx: &mpsc::Sender<Value>,
    request_id: &str,
    conversation: &str,
) {
    let mut sessions = server.sessions.lock();
    let Some(session) = sessions.get_mut(conversation) else {
        respond(
            out_tx,
            request_id,
            "thread-follower-interrupt-turn",
            false,
            json!({}),
            Some("no-client-found".to_string()),
        );
        return;
    };
    if !session.active {
        respond(
            out_tx,
            request_id,
            "thread-follower-interrupt-turn",
            false,
            json!({}),
            Some("NoActiveTurn".to_string()),
        );
        return;
    }
    session.interrupt = true;
    let last_turn = session
        .state
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|t| t.last())
        .and_then(|t| t.get("turnId"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    respond(
        out_tx,
        request_id,
        "thread-follower-interrupt-turn",
        true,
        json!({"interruptedTurnId": last_turn, "ok": true}),
        None,
    );
    eprintln!("[fake-owner] conv={conversation} interrupt requested");
    // 中断的终态快照由 run_turn 在中断分支广播。
}

/// 注入 pending attention(脚本触发)并广播快照。
async fn inject_pending(server: &Arc<Server>, conversation: &str, key: &str, item: &Value) {
    push_change(server, conversation, |session| {
        if let Some(list) = session.state.get_mut(key).and_then(Value::as_array_mut) {
            list.push(item.clone());
        }
        session.waiting_attention = true;
        None
    })
    .await;
    eprintln!("[fake-owner] conv={conversation} attention injected ({key})");
}

fn apply_settings(server: &Arc<Server>, conversation: &str, settings: Option<&Value>) {
    let Some(settings) = settings.and_then(Value::as_object) else {
        return;
    };
    let mut sessions = server.sessions.lock();
    if let Some(session) = sessions.get_mut(conversation) {
        let state = &mut session.state;
        if let Some(model) = settings.get("model").and_then(Value::as_str) {
            state["latestModel"] = json!(model);
        }
        if let Some(effort) = settings.get("effort").and_then(Value::as_str) {
            state["latestReasoningEffort"] = json!(effort);
        }
        if let Some(policy) = settings.get("approvalPolicy").and_then(Value::as_str) {
            if let Some(perms) = state.get_mut("currentPermissions") {
                perms["approvalPolicy"] = json!(policy);
            }
        }
        if let Some(tier) = settings.get("serviceTier").and_then(Value::as_str) {
            if let Some(mode) = state.get_mut("latestCollaborationMode") {
                mode["settings"]["serviceTier"] = json!(tier);
            }
        }
    }
}
