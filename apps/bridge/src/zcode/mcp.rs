//! MCP stdio server(`bridge mcp-stdio` 子命令;04 §8.8)。
//!
//! - 手写最小 JSON-RPC 2.0 / MCP 协议层(newline-delimited JSON;不引入
//!   MCP SDK 依赖)。
//! - 工具 `agent_console.ask_user`:question + 有限选项 + 是否允许补充
//!   文本;返回 answered/cancelled/expired,内部复用 ZC-01 同一 pending
//!   通路(socket → Bridge 注册表 → 远程卡片)。
//! - 宿主取消:`notifications/cancelled` 在 tools/call 等待期间仍被读取
//!   (调用在独立任务等待),经 cancel 事件撤销 pending;被取消的请求不回
//!   JSON-RPC 响应。
//! - session 绑定:stdio helper 进程即会话上下文(本机日志证据:用户配置
//!   的 stdio server 以 `mcpIsolation: "session"` 每会话独立连接);调用
//!   路由以 invokeId 为准,不依赖进程绑定也正确(04 §8.8 判定见报告)。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use super::contract::{
    self, AskRequest, HookInvoke, EVENT_ASK_USER, MAX_FRAME_BYTES,
};
use super::helper::HelperConfig;

/// MCP 协议版本(客户端提供时回显客户端值)。
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// 工具名(04 §8.8 固定)。
pub const ASK_USER_TOOL: &str = "agent_console.ask_user";

/// 单帧上限(与 helper 合同一致)。
const MCP_MAX_LINE: usize = MAX_FRAME_BYTES;

/// 在给定 IO 上运行 MCP server(泛型以便内存测试)。
pub async fn serve<R, W>(config: HelperConfig, input: R, output: W) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(input);
    let writer = Arc::new(tokio::sync::Mutex::new(output));
    // rpc id 关联键(统一 JSON 序列化形态,数字/字符串 id 一致处理):
    // `Value::to_string()`。在途表:关联键 → invoke_id;取消传播用。
    let mut in_flight: HashMap<String, String> = HashMap::new();
    let mut cancelled: std::collections::HashSet<String> = Default::default();
    // (原始 rpc id Value, 工具结果):外层包装需要原样回传 id(保留类型)。
    let (done_tx, mut done_rx) = mpsc::channel::<(serde_json::Value, serde_json::Value)>(16);
    let mut line = String::new();
    loop {
        tokio::select! {
            read = reader.read_line(&mut line) => {
                if read? == 0 {
                    return Ok(()); // EOF:正常退出(在途调用随进程结束)。
                }
                if line.len() > MCP_MAX_LINE {
                    line.clear();
                    continue;
                }
                let trimmed = line.trim().to_string();
                line.clear();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(message) = serde_json::from_str::<serde_json::Value>(&trimmed) else {
                    write_output(
                        &writer,
                        &rpc_error(serde_json::Value::Null, -32700, "parse error"),
                    )
                    .await?;
                    continue;
                };
                let method = message.get("method").and_then(|m| m.as_str()).unwrap_or("");
                let id = message.get("id").cloned().unwrap_or(serde_json::Value::Null);
                let has_id = !id.is_null();
                match (method, has_id) {
                    ("initialize", true) => {
                        let client_version = message
                            .pointer("/params/protocolVersion")
                            .and_then(|v| v.as_str())
                            .unwrap_or(MCP_PROTOCOL_VERSION)
                            .to_string();
                        let result = serde_json::json!({
                            "protocolVersion": client_version,
                            "capabilities": { "tools": { "listChanged": false } },
                            "serverInfo": {
                                "name": "agent-console-bridge",
                                "version": env!("CARGO_PKG_VERSION"),
                            },
                        });
                        write_output(&writer, &rpc_result(id, result)).await?;
                    }
                    ("ping", true) => {
                        write_output(&writer, &rpc_result(id, serde_json::json!({}))).await?;
                    }
                    ("tools/list", true) => {
                        write_output(&writer, &rpc_result(id, tools_listing())).await?;
                    }
                    ("tools/call", true) => {
                        match build_ask_invoke(&config, &message) {
                            Ok(invoke) => {
                                in_flight.insert(rpc_id_key(&id), invoke.invoke_id.clone());
                                let done_tx = done_tx.clone();
                                let writer = writer.clone();
                                let config = config.clone();
                                tokio::spawn(async move {
                                    let result = await_ask(&config, &invoke).await;
                                    let _ = done_tx.send((id, result)).await;
                                    let _ = writer; // 保持 writer 存活到任务结束。
                                });
                            }
                            Err(response) => {
                                write_output(&writer, &response).await?;
                            }
                        }
                    }
                    ("notifications/cancelled", false) => {
                        // 宿主取消:撤销在途 pending;响应将被抑制。
                        // requestId 与登记统一以 Value::to_string() 为关联键
                        // (数字/字符串 id 一致;P2-8)。
                        let request_id = message.pointer("/params/requestId").cloned();
                        if let Some(request_id) = request_id.filter(|v| !v.is_null()) {
                            let key = rpc_id_key(&request_id);
                            if let Some(invoke_id) = in_flight.get(&key) {
                                super::helper::cancel_invoke(&config.socket_path, invoke_id)
                                    .await;
                            }
                            cancelled.insert(key);
                        }
                    }
                    (_, true) => {
                        write_output(
                            &writer,
                            &rpc_error(id, -32601, &format!("method not found: {method}")),
                        )
                        .await?;
                    }
                    (_, false) => {}
                }
            }
            Some((rpc_id, response)) = done_rx.recv() => {
                in_flight.remove(&rpc_id_key(&rpc_id));
                // 已取消的调用:按 JSON-RPC 取消语义不回响应。
                if cancelled.remove(&rpc_id_key(&rpc_id)) {
                    continue;
                }
                // 完整 JSON-RPC 外层:宿主按原样 id 关联结果(jsonrpc/id/result)。
                write_output(&writer, &rpc_result(rpc_id, response)).await?;
            }
        }
    }
}

/// rpc id 关联键:数字与字符串 id 用同一形态(`Value::to_string()` 的 JSON
/// 序列化),登记与取消取值方式一致。
fn rpc_id_key(id: &serde_json::Value) -> String {
    id.to_string()
}

async fn write_output<W: tokio::io::AsyncWrite + Unpin>(
    output: &Arc<tokio::sync::Mutex<W>>,
    message: &serde_json::Value,
) -> std::io::Result<()> {
    let mut output = output.lock().await;
    let mut line = serde_json::to_string(message).expect("rpc message serializes");
    line.push('\n');
    output.write_all(line.as_bytes()).await?;
    output.flush().await
}

fn rpc_result(id: serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: serde_json::Value, code: i64, message: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

fn tools_listing() -> serde_json::Value {
    serde_json::json!({
        "tools": [{
            "name": ASK_USER_TOOL,
            "description": "Ask the Agent Console user a question with limited options; \
                            returns answered/cancelled/expired.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "maxLength": AskRequest::MAX_QUESTION_CHARS,
                    },
                    "options": {
                        "type": "array",
                        "maxItems": AskRequest::MAX_OPTIONS,
                        "items": {
                            "type": "string",
                            "maxLength": AskRequest::MAX_OPTION_CHARS,
                        },
                    },
                    "allow_free_text": { "type": "boolean", "default": true },
                },
                "required": ["question"],
            },
        }],
    })
}

/// tools/call → 校验并构造 ask invoke(不等待;等待在独立任务)。
fn build_ask_invoke(
    config: &HelperConfig,
    message: &serde_json::Value,
) -> Result<HookInvoke, serde_json::Value> {
    let id = message.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let Some(arguments) = message.pointer("/params/arguments").cloned() else {
        return Err(rpc_error(
            id,
            -32602,
            "tools/call requires params.arguments",
        ));
    };
    let Ok(ask) = serde_json::from_value::<AskPayload>(arguments) else {
        return Err(rpc_error(
            id,
            -32602,
            "invalid arguments for agent_console.ask_user",
        ));
    };
    let ask = AskRequest {
        question: ask.question,
        options: ask.options.unwrap_or_default(),
        allow_free_text: ask.allow_free_text.unwrap_or(true),
        call_id: Some(id.to_string()),
    };
    if let Err(reason) = ask.validate() {
        return Err(rpc_error(id, -32602, &format!("invalid ask: {reason}")));
    }
    Ok(HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: uuid::Uuid::new_v4().to_string(),
        event: EVENT_ASK_USER.to_string(),
        native_session_id: None,
        tool_name: None,
        tool_use_id: None,
        requested_wait_ms: config.wait_ms,
        tool_input: None,
        ask: Some(ask),
        status_event: None,
        status_input: None,
    })
}

/// 等待 Bridge 决定并生成 MCP 工具结果。
async fn await_ask(config: &HelperConfig, invoke: &HookInvoke) -> serde_json::Value {
    match super::helper::invoke_once(
        &config.socket_path,
        invoke,
        super::helper::helper_wait(config),
    )
    .await
    {
        Ok(reply) => match reply.status.as_str() {
            contract::STATUS_ANSWERED => tool_text(
                &serde_json::json!({
                    "status": "answered",
                    "option": reply.option,
                    "text": reply.text,
                })
                .to_string(),
            ),
            contract::STATUS_CANCELLED => {
                tool_text(&serde_json::json!({ "status": "cancelled" }).to_string())
            }
            contract::STATUS_EXPIRED => {
                tool_text(&serde_json::json!({ "status": "expired" }).to_string())
            }
            other => tool_error(&format!("ask failed: {other}")),
        },
        Err(err) => tool_error(&err), // Bridge 不可达:明确失败,不伪装已回答。
    }
}

/// MCP 工具结果(text content;isError 标记失败)。
fn tool_text(text: &str) -> serde_json::Value {
    serde_json::json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false,
    })
}

fn tool_error(message: &str) -> serde_json::Value {
    serde_json::json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true,
    })
}

/// tools/call 参数结构(仅 04 §8.8 允许的字段)。
#[derive(Debug, serde::Deserialize)]
struct AskPayload {
    question: String,
    #[serde(default)]
    options: Option<Vec<String>>,
    #[serde(default)]
    allow_free_text: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// tools/call 结果必须带完整 JSON-RPC 外层(jsonrpc/id/result)且保留原始
    /// id 类型(Codex 复现:裸 content/isError,宿主无法按 id 关联结果)。
    /// 经实际 serve 主路径驱动:成功(数字 id / 字符串 id)与工具错误(isError)。
    #[tokio::test]
    async fn tools_call_result_wrapped_with_original_id() {
        use std::sync::Arc;
        // macOS SUN_LEN:unix socket 路径必须短(同 tests/zcode_hooks.rs)。
        let work_dir = std::path::PathBuf::from(format!(
            "/tmp/ac-mcp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&work_dir).unwrap();
        let socket = work_dir.join("hook.sock");
        let hooks = Arc::new(crate::zcode::ZcodeHooks::new("device-mcp", socket.clone()));
        let hook_server = crate::zcode::server::serve(
            crate::zcode::server::HookServerConfig {
                socket_path: socket.clone(),
            },
            hooks.clone(),
        )
        .await
        .unwrap();
        let config = HelperConfig {
            socket_path: socket.clone(),
            wait_ms: 10_000,
        };
        let (client, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, server_write) = tokio::io::split(server_side);
        let task = tokio::spawn(serve(config, server_read, server_write));
        let mut client = BufReader::new(client);

        async fn wait_card(hooks: &Arc<crate::zcode::ZcodeHooks>) -> String {
            for _ in 0..200 {
                if let Some(card) = hooks.registry().waiting_cards().first() {
                    return card.invoke_id.clone();
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("ask pending not registered in time");
        }

        async fn send(client: &mut BufReader<tokio::io::DuplexStream>, line: &str) {
            let mut framed = line.to_string();
            framed.push('\n');
            client.write_all(framed.as_bytes()).await.unwrap();
            client.flush().await.unwrap();
        }

        async fn read_response(
            client: &mut BufReader<tokio::io::DuplexStream>,
        ) -> serde_json::Value {
            let mut out = String::new();
            tokio::time::timeout(std::time::Duration::from_secs(3), client.read_line(&mut out))
                .await
                .expect("mcp response in time")
                .unwrap();
            serde_json::from_str(out.trim()).unwrap()
        }

        // ① 数字 id:ask 经 Bridge 注册表真实往返 → answered(决定在等待中给出)。
        send(
            &mut client,
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"agent_console.ask_user","arguments":{"question":"继续吗?","options":["继续","取消"]}}}"#,
        )
        .await;
        let invoke_id = wait_card(&hooks).await;
        hooks
            .registry()
            .resolve(
                &invoke_id,
                crate::zcode::contract::HookReply::answered(Some("继续".into()), None),
            )
            .unwrap();
        let response = read_response(&mut client).await;
        assert_eq!(response["jsonrpc"], "2.0", "缺 JSON-RPC 外层: {response}");
        assert!(response["id"].is_number(), "数字 id 不得变形: {response}");
        assert_eq!(response["id"], 7);
        let result = response
            .get("result")
            .unwrap_or_else(|| panic!("缺 result 外层: {response}"));
        assert_eq!(result["isError"], false);
        assert_eq!(result["content"][0]["type"], "text");
        assert!(
            result["content"][0]["text"].to_string().contains("answered"),
            "text 应为 answered 结果: {result}"
        );

        // ② 字符串 id:保持字符串类型,不 to_string 变形。
        send(
            &mut client,
            r#"{"jsonrpc":"2.0","id":"call-abc","method":"tools/call","params":{"name":"agent_console.ask_user","arguments":{"question":"继续吗?","options":["继续","取消"]}}}"#,
        )
        .await;
        let invoke_id = wait_card(&hooks).await;
        hooks
            .registry()
            .resolve(
                &invoke_id,
                crate::zcode::contract::HookReply::answered(Some("取消".into()), None),
            )
            .unwrap();
        let response = read_response(&mut client).await;
        assert_eq!(response["jsonrpc"], "2.0", "缺 JSON-RPC 外层: {response}");
        assert!(response["id"].is_string(), "字符串 id 不得变形: {response}");
        assert_eq!(response["id"], "call-abc");
        assert_eq!(response["result"]["isError"], false);
        assert!(
            response["result"]["content"][0]["text"]
                .to_string()
                .contains("取消"),
            "answered 选项原文应进入 content: {response}"
        );

        // ③ 工具错误(isError):Bridge 不可达 → 完整外层 + isError=true。
        hook_server.abort();
        crate::zcode::server::remove_socket(&socket);
        send(
            &mut client,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"agent_console.ask_user","arguments":{"question":"继续吗?"}}}"#,
        )
        .await;
        let response = read_response(&mut client).await;
        assert_eq!(response["jsonrpc"], "2.0", "错误结果也必须有外层: {response}");
        assert_eq!(response["id"], 3);
        assert_eq!(response["result"]["isError"], true);

        task.abort();
        let _ = std::fs::remove_dir_all(&work_dir);
    }

    /// notifications/cancelled 必须能撤销在途 ask:数字 id 与字符串 id 都要
    /// 命中(Codex 复现:登记用 id.to_string()、取消读 as_str(),两种 id 的
    /// 取消都到不了 Bridge;等待中的 tools/call 不得回迟到答案)。
    #[tokio::test]
    async fn notifications_cancelled_revokes_pending_for_numeric_and_string_ids() {
        use std::sync::Arc;
        // macOS SUN_LEN:unix socket 路径必须短(同 tests/zcode_hooks.rs)。
        let work_dir = std::path::PathBuf::from(format!(
            "/tmp/ac-mcp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&work_dir).unwrap();
        let socket = work_dir.join("hook.sock");
        let hooks = Arc::new(crate::zcode::ZcodeHooks::new("device-mcp-cancel", socket.clone()));
        let hook_server = crate::zcode::server::serve(
            crate::zcode::server::HookServerConfig {
                socket_path: socket.clone(),
            },
            hooks.clone(),
        )
        .await
        .unwrap();
        let config = HelperConfig {
            socket_path: socket.clone(),
            wait_ms: 10_000,
        };
        let (client, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, server_write) = tokio::io::split(server_side);
        let task = tokio::spawn(serve(config, server_read, server_write));
        let mut client = BufReader::new(client);

        async fn wait_card(hooks: &Arc<crate::zcode::ZcodeHooks>) -> String {
            for _ in 0..200 {
                if let Some(card) = hooks.registry().waiting_cards().first() {
                    return card.invoke_id.clone();
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("ask pending not registered in time");
        }

        async fn send(client: &mut BufReader<tokio::io::DuplexStream>, line: &str) {
            let mut framed = line.to_string();
            framed.push('\n');
            client.write_all(framed.as_bytes()).await.unwrap();
            client.flush().await.unwrap();
        }

        async fn read_response(
            client: &mut BufReader<tokio::io::DuplexStream>,
        ) -> serde_json::Value {
            let mut out = String::new();
            tokio::time::timeout(std::time::Duration::from_secs(3), client.read_line(&mut out))
                .await
                .expect("mcp response in time")
                .unwrap();
            serde_json::from_str(out.trim()).unwrap()
        }

        // 数字 id 与字符串 id 两个用例,各自走完整取消链路。
        for (call_id, request_id) in [("9", "9"), (r#""req-str""#, r#""req-str""#)] {
            send(
                &mut client,
                &format!(
                    r#"{{"jsonrpc":"2.0","id":{call_id},"method":"tools/call","params":{{"name":"agent_console.ask_user","arguments":{{"question":"继续吗?","options":["继续","取消"]}}}}}}"#
                ),
            )
            .await;
            let invoke_id = wait_card(&hooks).await;

            // 宿主取消:requestId 同 call id。
            send(
                &mut client,
                &format!(
                    r#"{{"jsonrpc":"2.0","method":"notifications/cancelled","params":{{"requestId":{request_id}}}}}"#
                ),
            )
            .await;

            // 取消必须传播到 Bridge:pending 撤销(HandledLocally),决定不可达。
            // (ping 响应保证 cancel_invoke 已被 serve 同步处理完毕。)
            send(
                &mut client,
                r#"{"jsonrpc":"2.0","id":100,"method":"ping"}"#,
            )
            .await;
            let ping = read_response(&mut client).await;
            assert_eq!(
                ping["id"], 100,
                "被取消的 tools/call 不得先回响应/迟到答案: {ping}"
            );
            let snapshot = hooks
                .registry()
                .get(&invoke_id)
                .unwrap_or_else(|| panic!("[{call_id}] pending 必须仍可查询"));
            assert_eq!(
                snapshot.state,
                crate::zcode::PendingState::HandledLocally,
                "[{call_id}] 取消必须撤销 pending(数字与字符串 id 都要生效)"
            );
            assert!(
                hooks.registry().resolve(&invoke_id, crate::zcode::contract::HookReply::allowed()).is_err(),
                "[{call_id}] 被取消请求不得再被回填决定"
            );
        }

        hook_server.abort();
        task.abort();
        let _ = std::fs::remove_dir_all(&work_dir);
    }

    /// 协议层单元行为:initialize / tools/list / 未知方法 / 参数校验。
    #[tokio::test]
    async fn protocol_layer_initialize_tools_list_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = HelperConfig {
            socket_path: dir.path().join("missing.sock"),
            wait_ms: 500,
        };
        let (client, server_side) = tokio::io::duplex(64 * 1024);
        let (server_read, server_write) = tokio::io::split(server_side);
        let task = tokio::spawn(serve(config, server_read, server_write));
        let mut client = BufReader::new(client);
        // initialize:回显客户端协议版本。
        client
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2024-11-05\"}}\n",
            )
            .await
            .unwrap();
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(response["result"]["serverInfo"]["name"], "agent-console-bridge");
        // tools/list:仅 agent_console.ask_user。
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n")
            .await
            .unwrap();
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["result"]["tools"][0]["name"], ASK_USER_TOOL);
        // 未知方法 → -32601。
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"bogus\"}\n")
            .await
            .unwrap();
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["error"]["code"], -32601);
        // tools/call 缺参数 → -32602。
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\"}\n")
            .await
            .unwrap();
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["error"]["code"], -32602);
        task.abort();
    }
}
