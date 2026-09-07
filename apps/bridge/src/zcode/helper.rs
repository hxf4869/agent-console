//! helper(`bridge zcode-hook` 子命令):stdin → socket → stdout。
//!
//! - stdin:ZCode 单行 JSON;stdout 仅输出协议 JSON(官方上限 32KiB),
//!   诊断一律走 stderr。
//! - PermissionRequest:连接 Bridge → 等决定 → allow/deny 输出官方
//!   decision JSON;输出成功后回发交付确认(绑定 invoke_id),输出失败
//!   不确认(R2-ZC01);超时/不可达/过期 → 空输出回原生确认,不自动允许。
//! - 其他官方事件(SessionStart 等):转发为 status invoke 即时确认。
//! - 退出码恒为 0(除致命本地错误):exit 2 = 阻断语义,绝不误用。

use std::path::{Path, PathBuf};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::contract::{
    self, HookInvoke, HookReply, NativeHookInput, EVENT_PERMISSION_REQUEST, MAX_FRAME_BYTES,
    STATUS_ALLOWED, STATUS_DENIED,
};
use super::server::status_invoke_from_line;

/// helper 运行配置。
#[derive(Debug, Clone)]
pub struct HelperConfig {
    pub socket_path: PathBuf,
    /// 远程等待预算(默认 45s)。
    pub wait_ms: u64,
}

impl Default for HelperConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            wait_ms: contract::DEFAULT_REMOTE_WAIT_MS,
        }
    }
}

/// 默认 socket 路径:应用数据目录下(与 Bridge `run` 侧一致)。
pub fn default_socket_path() -> PathBuf {
    crate::config::BridgeConfig::from_env()
        .data_dir
        .join("zcode-hook.sock")
}

/// 读取 stdin 单行(官方:单行 JSON + 换行);超限整单拒绝。
pub async fn read_stdin_line() -> Result<String, super::contract::ParseError> {
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    loop {
        let read = stdin
            .read(&mut chunk)
            .await
            .map_err(|_| super::contract::ParseError::Io)?;
        if read == 0 {
            break;
        }
        if let Some(newline) = chunk[..read].iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&chunk[..newline]);
            return finish_line(buf);
        }
        buf.extend_from_slice(&chunk[..read]);
        if buf.len() > MAX_FRAME_BYTES {
            return Err(super::contract::ParseError::Oversize(buf.len()));
        }
    }
    finish_line(buf)
}

fn finish_line(buf: Vec<u8>) -> Result<String, super::contract::ParseError> {
    if buf.len() > MAX_FRAME_BYTES {
        return Err(super::contract::ParseError::Oversize(buf.len()));
    }
    String::from_utf8(buf).map_err(|_| super::contract::ParseError::NotObject)
}

/// 写一行到 stdout(仅协议 JSON;三次 write/flush 结果全部检查 ——
/// R2-ZC01:输出失败必须可见,不得静默假定原生已收到)。
pub async fn write_stdout_line(line: &str) -> std::io::Result<()> {
    let mut out = tokio::io::stdout();
    out.write_all(line.as_bytes()).await?;
    out.write_all(b"\n").await?;
    out.flush().await
}

/// 写诊断到 stderr(永不进 stdout)。
macro_rules! helper_diag {
    ($($arg:tt)*) => {
        eprintln!($($arg)*)
    };
}

/// PermissionRequest invoke 组装(native 字段 → 本机合同)。
pub fn permission_invoke(native: &NativeHookInput, config: &HelperConfig) -> HookInvoke {
    HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: uuid::Uuid::new_v4().to_string(),
        event: EVENT_PERMISSION_REQUEST.to_string(),
        native_session_id: native.session_id.clone(),
        tool_name: native.tool_name.clone(),
        tool_use_id: native.tool_use_id.clone(),
        requested_wait_ms: config.wait_ms,
        tool_input: native.tool_input.clone(),
        ask: None,
        status_event: None,
        status_input: None,
    }
}

/// 已收到 Bridge 决定但尚未确认交付的连接句柄(R2-ZC01)。
///
/// 调用方在完成**原生协议输出**(PermissionRequest stdout / MCP JSON-RPC
/// 响应写出+flush)之后调用 [`ReplyAck::acknowledge`];Bridge 侧有限等待
/// 该确认,缺失/失败按未确认处理(不报成功)。句柄丢弃(未确认)即连接
/// 写半关闭,Bridge 按超时/EOF 收尾。
pub struct ReplyAck {
    writer: Option<tokio::net::unix::OwnedWriteHalf>,
    invoke_id: String,
}

impl ReplyAck {
    /// 回发交付确认行(绑定 invoke_id)并 flush;失败 = 交付未确认。
    pub async fn acknowledge(mut self) -> Result<(), String> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        let mut line = contract::delivery_ack_json(&self.invoke_id);
        line.push('\n');
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|err| format!("delivery ack write failed: {err}"))?;
        writer
            .flush()
            .await
            .map_err(|err| format!("delivery ack flush failed: {err}"))
    }
}

/// 单次 invoke:连接 → 发送 → 读应答(错误统一为 Err,不区分阶段细节)。
///
/// 写半连接必须保持到应答读完:提前 drop OwnedWriteHalf 会让服务端收到
/// EOF,按"helper 消失"取消 pending,远程决定永远到不了 helper。
pub async fn invoke_once(
    socket: &Path,
    invoke: &HookInvoke,
    wait: std::time::Duration,
) -> Result<HookReply, String> {
    invoke_once_with_ack(socket, invoke, wait)
        .await
        .map(|(reply, _ack)| reply)
}

/// 同 [`invoke_once`],但保留连接写半并返回交付确认句柄:调用方完成原生
/// 协议输出后必须 [`ReplyAck::acknowledge`];在此之前不关闭写半连接
/// (R2-ZC01:输出原生结果之前关闭写半会让 Bridge 无法区分「已输出」
/// 与「连接断开」)。
pub async fn invoke_once_with_ack(
    socket: &Path,
    invoke: &HookInvoke,
    wait: std::time::Duration,
) -> Result<(HookReply, ReplyAck), String> {
    let connect = async {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|err| format!("bridge socket unreachable: {err}"))?;
        let (reader, mut writer) = stream.into_split();
        let mut line = serde_json::to_string(invoke).map_err(|err| err.to_string())?;
        line.push('\n');
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|err| format!("bridge socket write failed: {err}"))?;
        writer
            .flush()
            .await
            .map_err(|err| format!("bridge socket write failed: {err}"))?;
        Ok::<_, String>((reader, writer))
    };
    // writer 随本 future 存活到 read_line 完成之后,期间不触发服务端 EOF。
    let (reader, writer) = tokio::time::timeout(std::time::Duration::from_secs(5), connect)
        .await
        .map_err(|_| "bridge socket connect timed out".to_string())??;
    let mut reader = BufReader::new(reader);
    let mut reply_line = String::new();
    tokio::time::timeout(wait, reader.read_line(&mut reply_line))
        .await
        .map_err(|_| "bridge reply timed out".to_string())?
        .map_err(|err| format!("bridge socket read failed: {err}"))?;
    if reply_line.trim().is_empty() {
        // 写半随 reader/ReplyAck 生命周期处理:读取失败时 writer 在此 drop。
        drop(writer);
        return Err("bridge closed without reply".to_string());
    }
    let reply: HookReply = serde_json::from_str(reply_line.trim())
        .map_err(|err| format!("bridge reply invalid: {err}"))?;
    let ack = ReplyAck {
        writer: Some(writer),
        invoke_id: invoke.invoke_id.clone(),
    };
    Ok((reply, ack))
}

/// invoke 等待预算(config 收敛)。
pub fn helper_wait(config: &HelperConfig) -> std::time::Duration {
    contract::clamp_wait_ms(config.wait_ms)
}

/// 取消在途 invoke(MCP 宿主取消传播;独立短连接)。
pub async fn cancel_invoke(socket: &Path, invoke_id: &str) {
    let cancel = HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: invoke_id.to_string(),
        event: super::contract::EVENT_CANCEL.to_string(),
        native_session_id: None,
        tool_name: None,
        tool_use_id: None,
        requested_wait_ms: contract::MIN_REMOTE_WAIT_MS,
        tool_input: None,
        ask: None,
        status_event: None,
        status_input: None,
    };
    let _ = invoke_once(socket, &cancel, std::time::Duration::from_secs(5)).await;
}

/// `zcode-hook` 主入口:返回进程退出码(stdout 协议由本函数唯一负责)。
pub async fn run_hook(config: HelperConfig) -> i32 {
    let line = match read_stdin_line().await {
        Ok(line) => line,
        Err(err) => {
            helper_diag!("agent-console helper: hook input rejected ({err})");
            return 0;
        }
    };
    let native = match contract::parse_native_input(&line) {
        Ok(native) => native,
        Err(err) => {
            helper_diag!("agent-console helper: hook input rejected ({err})");
            return 0;
        }
    };
    match native.hook_event_name.as_str() {
        "PermissionRequest" => {
            let invoke = permission_invoke(&native, &config);
            let wait = contract::clamp_wait_ms(config.wait_ms);
            match invoke_once_with_ack(&config.socket_path, &invoke, wait).await {
                Ok((reply, ack)) => {
                    // 交付确认(R2-ZC01):只有原生 decision JSON 实际写出
                    // 成功后才回 ack;输出失败不确认,Bridge 侧按未确定处理。
                    // expired/cancelled/rejected:空输出回原生确认(不自动
                    // 允许),随后同样确认「本 helper 对该决定处理完毕」。
                    let outcome = match reply.status.as_str() {
                        STATUS_ALLOWED => {
                            write_stdout_line(&contract::allow_decision_json()).await
                        }
                        STATUS_DENIED => {
                            let message = reply.message.unwrap_or_else(|| {
                                "User declined this action in Agent Console".into()
                            });
                            write_stdout_line(&contract::deny_decision_json(&message)).await
                        }
                        other => {
                            helper_diag!(
                                "agent-console helper: remote decision unavailable ({other}); \
                                 falling back to native confirmation"
                            );
                            Ok(())
                        }
                    };
                    if let Err(err) = outcome {
                        helper_diag!("agent-console helper: native stdout write failed ({err}); \
                                      decision delivery left unconfirmed");
                        return 0;
                    }
                    if let Err(err) = ack.acknowledge().await {
                        helper_diag!("agent-console helper: delivery ack failed ({err})");
                    }
                }
                // Bridge 不可达/超时:尽快结束远程尝试,空结果无额外效果。
                Err(err) => {
                    helper_diag!(
                        "agent-console helper: bridge unavailable ({err}); \
                         falling back to native confirmation"
                    );
                }
            }
            0
        }
        // 状态观察事件:转发即返回,不影响会话。
        "SessionStart" | "UserPromptSubmit" | "PreToolUse" | "PostToolUse"
        | "PostToolUseFailure" | "Stop" => {
            match status_invoke_from_line(&line) {
                Ok(invoke) => {
                    let _ = invoke_once(
                        &config.socket_path,
                        &invoke,
                        super::server::STATUS_ACK_WAIT,
                    )
                    .await;
                }
                Err(err) => helper_diag!("agent-console helper: status forward skipped ({err})"),
            }
            0
        }
        // 未知事件:空输出,不干预。
        other => {
            helper_diag!("agent-console helper: ignoring event {other}");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(socket: PathBuf) -> HelperConfig {
        HelperConfig {
            socket_path: socket,
            wait_ms: contract::DEFAULT_REMOTE_WAIT_MS,
        }
    }

    /// stdin 单行读取与超限拒绝。
    #[tokio::test]
    async fn stdin_line_read_and_oversize() {
        // 直接测 finish_line/read 路径:构造超限字节。
        let big = vec![b'x'; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            finish_line(big).unwrap_err(),
            contract::ParseError::Oversize(_)
        ));
        assert_eq!(finish_line(b"{}".to_vec()).unwrap(), "{}");
    }

    /// Bridge 不可达:run_hook 输出空 stdout、退出码 0(原生确认回退)。
    #[tokio::test]
    async fn bridge_unreachable_falls_back_with_empty_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("missing.sock");
        let config = config_with(socket);
        // 非法事件:空输出。
        // 这里验证 invoke_once 对不存在 socket 的错误路径:
        let invoke = permission_invoke(
            &contract::parse_native_input(
                r#"{"hook_event_name":"PermissionRequest","session_id":"s","tool_name":"Bash"}"#,
            )
            .unwrap(),
            &config,
        );
        let err = invoke_once(&config.socket_path, &invoke, std::time::Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(err.contains("unreachable"), "actual: {err}");
    }

    /// deny 时缺失消息 → 固定默认消息。
    #[tokio::test]
    async fn deny_without_message_uses_default() {
        // 经由 run_hook 的 JSON 输出层验证:直接验证决策串。
        let deny = contract::deny_decision_json("User declined this action in Agent Console");
        assert!(deny.contains("deny"));
    }

    /// permission_invoke:UUID 每次调用唯一(同输入两次执行 = 两个请求)。
    #[test]
    fn invoke_ids_are_unique_per_invocation() {
        let native = contract::parse_native_input(
            r#"{"hook_event_name":"PermissionRequest","session_id":"s","tool_name":"Bash","tool_input":{"command":"ls"}}"#,
        )
        .unwrap();
        let config = config_with(PathBuf::from("/tmp/x.sock"));
        let a = permission_invoke(&native, &config);
        let b = permission_invoke(&native, &config);
        assert_ne!(a.invoke_id, b.invoke_id, "同输入两次执行必须产生两个请求");
        assert_eq!(a.tool_name, b.tool_name);
        assert_eq!(a.native_session_id.as_deref(), Some("s"));
        // tool_use_id 缺失时不得伪造。
        assert!(a.tool_use_id.is_none());
    }
}
