//! 本机 Unix socket 服务(Bridge 侧;04 §8.5/§8.6)。
//!
//! - socket 位于当前用户私有目录:目录 0700、socket 文件 0600;macOS 上
//!   额外以 `getpeereid` 校验对端 euid(不夸大安全边界:这不是防御同一
//!   用户下恶意进程的沙箱,见 04 §8.5;不支持校验的平台跳过并报告)。
//! - 一条连接 = 一个 invoke:读一行请求(超 [`MAX_FRAME_BYTES`] 整单拒绝)→
//!   登记 pending → 发远程卡片事件 → 等决定/超时 → 回一行应答。
//! - 决定写回结果不可忽略(R2-ZC01):写回后有限等待 helper 完成**原生
//!   协议输出**(stdout / MCP JSON-RPC)后的交付确认行(绑定 invoke_id);
//!   写回失败/确认缺失或超时按未确认收尾,不报成功、不静默假定已送达。
//! - 决定前连接断开(原生取消 / helper 消失)或出现多余输入 → 撤销远程
//!   卡片并拒绝迟到回复;决定后的合法交付确认不会被当作 helper 消失。
//! - 状态事件(status)即时确认,不等待;cancel 事件撤销指定 invoke
//!   (MCP 宿主取消传播)。

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

use super::contract::{
    self, HookInvoke, HookReply, ParseError, EVENT_ASK_USER, EVENT_CANCEL,
    EVENT_PERMISSION_REQUEST, EVENT_STATUS, MAX_FRAME_BYTES, STATUS_WAIT_MS,
};
use super::link::ZcodeHooks;
use super::pending::{DeliveryOutcome, InvokeKind, PendingError, PendingRegistry};

/// socket 服务配置。
#[derive(Debug, Clone)]
pub struct HookServerConfig {
    pub socket_path: PathBuf,
}

/// socket 服务错误(稳定维度;不含请求正文)。
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("socket dir unavailable: {0}")]
    SocketDir(String),
    #[error("socket listen failed: {0}")]
    Listen(String),
    #[error("socket permission setup failed: {0}")]
    Permission(String),
}

/// 启动 socket 服务(返回 join handle,由调用方在停机时 abort)。
pub async fn serve(
    config: HookServerConfig,
    hooks: Arc<ZcodeHooks>,
) -> Result<tokio::task::JoinHandle<()>, ServerError> {
    let listener = bind(&config.socket_path)?;
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let hooks = hooks.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, hooks).await;
                    });
                }
                Err(err) => {
                    // accept 瞬时错误不终止服务(§26.4:失败不退出)。
                    tracing::debug!(error = %err, "zcode hook socket accept error");
                }
            }
        }
    });
    Ok(handle)
}

/// 绑定 socket:清理残留 → 父目录 0700 → listen → 文件 0600。
fn bind(socket_path: &Path) -> Result<UnixListener, ServerError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| ServerError::SocketDir(err.to_string()))?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .map_err(|err| ServerError::Permission(err.to_string()))?;
    }
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)
        .map_err(|err| ServerError::Listen(err.to_string()))?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
        .map_err(|err| ServerError::Permission(err.to_string()))?;
    Ok(listener)
}

/// socket 目录与文件权限(测试/诊断)。
pub fn socket_permissions(socket_path: &Path) -> Result<(u32, u32), std::io::Error> {
    let file = std::fs::metadata(socket_path)?;
    let dir = std::fs::metadata(
        socket_path
            .parent()
            .ok_or_else(|| std::io::Error::other("socket has no parent dir"))?,
    )?;
    Ok((
        dir.permissions().mode() & 0o777,
        file.permissions().mode() & 0o777,
    ))
}

/// 删除 socket 文件(停机清理)。
pub fn remove_socket(socket_path: &Path) {
    let _ = std::fs::remove_file(socket_path);
}

// ---------------------------------------------------------------------------
// peer UID 校验(macOS getpeereid;直接声明符号,不新增依赖)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod peer {
    unsafe extern "C" {
        fn getpeereid(fd: i32, euid: *mut u32, egid: *mut u32) -> i32;
        fn geteuid() -> u32;
    }

    /// 对端 euid(借用 fd,不取得所有权);系统调用失败返回 None(拒绝)。
    pub fn peer_euid(raw_fd: i32) -> Option<u32> {
        let mut euid: u32 = 0;
        let mut egid: u32 = 0;
        // SAFETY:fd 合法(连接存活),输出指针为有效可写内存。
        let rc = unsafe { getpeereid(raw_fd, &mut euid, &mut egid) };
        if rc == 0 {
            Some(euid)
        } else {
            None
        }
    }

    pub fn current_euid() -> u32 {
        // SAFETY:无参数、无全局状态。
        unsafe { geteuid() }
    }
}

/// 校验对端为本用户。macOS:比较 euid;其他平台:恒真(跳过,报告说明)。
pub fn peer_is_current_user(raw_fd: i32) -> bool {
    #[cfg(target_os = "macos")]
    {
        match peer::peer_euid(raw_fd) {
            Some(euid) => euid == peer::current_euid(),
            None => false,
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = raw_fd;
        true
    }
}

// ---------------------------------------------------------------------------
// 连接处理
// ---------------------------------------------------------------------------

async fn handle_connection(stream: UnixStream, hooks: Arc<ZcodeHooks>) {
    // peer UID 校验:必须在处理任何请求前(借用 fd,不影响后续所有权转移)。
    {
        use std::os::unix::io::AsRawFd;
        if !peer_is_current_user(stream.as_raw_fd()) {
            tracing::debug!("zcode hook socket: peer uid mismatch, rejected");
            return;
        }
    }
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    match read_limited_line(&mut reader, &mut line).await {
        Ok(()) => {}
        Err(ReadLineError::Oversize) => {
            let reply = HookReply::rejected(format!(
                "invoke exceeds {MAX_FRAME_BYTES} bytes; refused without approval"
            ));
            let _ = write_reply(&mut writer, &reply).await;
            return;
        }
        Err(ReadLineError::Io) => return,
    }
    let Ok(invoke) = serde_json::from_str::<HookInvoke>(line.trim()) else {
        let _ =
            write_reply(&mut writer, &HookReply::rejected("invoke is not valid JSON contract"))
                .await;
        return;
    };
    match invoke.event.as_str() {
        EVENT_PERMISSION_REQUEST => {
            handle_pending_invoke(&mut writer, hooks, invoke, InvokeKind::PermissionRequest, reader)
                .await;
        }
        EVENT_ASK_USER => {
            handle_pending_invoke(&mut writer, hooks, invoke, InvokeKind::AskUser, reader).await;
        }
        EVENT_STATUS => {
            hooks.note_status(&invoke);
            // 状态变化进入实时发布(P2-11):SessionStart/UserPromptSubmit/
            // Stop 等经现有 publish_summary → SessionSummaryChanged 到达
            // 浏览器订阅流(不新协议)。
            hooks.publish_summary_after_status(invoke.native_session_id.as_deref()).await;
            let _ = write_reply(&mut writer, &HookReply::accepted()).await;
        }
        EVENT_CANCEL => {
            // 宿主取消传播:撤销仍等待中的 invoke,拒绝迟到回复。
            let cancelled = hooks.registry().cancel(&invoke.invoke_id).is_ok();
            hooks.publish_removal_for_cancelled(&invoke.invoke_id).await;
            let _ = write_reply(
                &mut writer,
                &if cancelled {
                    HookReply::accepted()
                } else {
                    HookReply::rejected("unknown invoke")
                },
            )
            .await;
        }
        other => {
            let _ = write_reply(
                &mut writer,
                &HookReply::rejected(format!("unknown invoke event {other}")),
            )
            .await;
        }
    }
}

/// 连接监视事件:监视任务恰好产出一次。决定前任何事件 = 连接失效
/// (原生取消 / helper 消失 / 协议违约);决定写回后,该行若为绑定本
/// invoke 的合法交付确认 = 交付完成,其余(EOF/错误内容)按未确认收尾
/// —— 合法确认不会被当成 helper 消失(R2-ZC01)。
enum ConnEvent {
    /// 读到一行(内容原样;超限内容不保留,以空串表达)。
    Line(String),
    /// EOF / 读失败:连接关闭。
    Closed,
}

/// 审批/问答 invoke:登记 → 发卡片 → 等决定/超时/连接消失 → 写回应答 →
/// 有限等待交付确认(R2-ZC01)。
async fn handle_pending_invoke(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    hooks: Arc<ZcodeHooks>,
    invoke: HookInvoke,
    kind: InvokeKind,
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
) {
    let registry: &Arc<PendingRegistry> = hooks.registry();
    if let Err(message) = validate_invoke(&invoke, kind) {
        let _ = write_reply(writer, &HookReply::rejected(message)).await;
        return;
    }
    let wait = contract::clamp_wait_ms(invoke.requested_wait_ms);
    let ask = if kind == InvokeKind::AskUser {
        invoke.ask.as_ref()
    } else {
        None
    };
    // 审批操作摘要在登记时有界化保留(P1-5):快照/重连后与实时卡片一致;
    // 仅本机内存、不进日志、不落盘。
    let action_summary = if kind == InvokeKind::PermissionRequest {
        Some(super::link::summarize_tool_input(invoke.tool_input.as_ref()))
    } else {
        None
    };
    let rx = match registry.register(
        &invoke.invoke_id,
        kind,
        invoke.native_session_id.clone(),
        invoke.tool_name.clone(),
        invoke.tool_use_id.clone(),
        ask,
        action_summary,
        wait,
    ) {
        Ok(rx) => rx,
        Err(PendingError::Duplicate) => {
            // 双注册兜底:同一 Hook 重复注册时,第二个连接被显式拒绝,
            // 不会产生两张都能生效的远程卡片。
            let _ = write_reply(
                writer,
                &HookReply::rejected("duplicate invoke id; possible double hook registration"),
            )
            .await;
            return;
        }
        Err(_) => {
            let _ = write_reply(writer, &HookReply::rejected("invoke rejected")).await;
            return;
        }
    };
    // 发远程卡片(现有事件通道;无订阅者时事件被丢弃,不阻塞本机等待)。
    // MCP 问答首次登记即把 plugin-ask 虚拟会话登记进观察镜像(L1:有过
    // 问答才列入列表,空会话不常驻)。
    if kind == InvokeKind::AskUser {
        hooks.note_ask_session_used();
    }
    hooks.publish_attention_added(&invoke, kind).await;
    if kind == InvokeKind::PermissionRequest {
        hooks.mark_awaiting_approval(invoke.native_session_id.as_deref(), true);
    }

    // 连接监视:读到一行或 EOF 都汇成事件。决定前任何事件 = 连接失效
    // (原生取消 / helper 消失 / 协议违约);决定写回后,该行若为绑定本
    // invoke 的合法交付确认 = 交付完成,其余(EOF/错误内容)按未确认收尾
    // —— 合法确认不会被当成 helper 消失(R2-ZC01)。
    // 决定前后共用的连接事件(监视任务恰好产出一次)。
    let (event_tx, mut event_rx) = oneshot::channel::<ConnEvent>();
    tokio::spawn(async move {
        let mut reader = reader;
        let mut extra = String::new();
        // 决定前协议上不应再有输入;这里只读取并上报内容,分类由等待方
        // 按所处阶段决定(确认合法性与阶段绑定在一起判断)。
        let event = match read_limited_line(&mut reader, &mut extra).await {
            Ok(()) => ConnEvent::Line(std::mem::take(&mut extra)),
            Err(ReadLineError::Oversize) => ConnEvent::Line(String::new()),
            Err(ReadLineError::Io) => ConnEvent::Closed,
        };
        let _ = event_tx.send(event);
        drop(reader);
    });

    enum Outcome {
        Decided(HookReply),
        /// 决定前连接失效(原生取消 / helper 消失 / 协议违约)。
        Gone,
        /// 等待超时。
        Timeout,
    }
    let outcome = tokio::select! {
        decision = rx => {
            match decision {
                // responder 被 cancel 事件取走/丢弃 → 已按本机处理撤销。
                Err(_) => Outcome::Decided(HookReply::cancelled()),
                Ok(reply) => Outcome::Decided(reply),
            }
        }
        event = &mut event_rx => {
            match event {
                // 决定前出现输入或连接关闭:连接失效(该事件已被消费,
                // Decided 分支不会再等待它)。
                _ => Outcome::Gone,
            }
        }
        _ = tokio::time::sleep(wait) => Outcome::Timeout,
    };
    match outcome {
        Outcome::Decided(reply) => {
            // 交付层级:Bridge 已接受决定(Decided)→ 写回 helper(结果
            // 不可忽略)→ 有限等待 helper 完成原生协议输出后的交付确认。
            let delivered = match write_reply(writer, &reply).await {
                Ok(()) => wait_delivery_ack(&mut event_rx, &invoke.invoke_id).await,
                Err(err) => {
                    tracing::warn!(
                        invoke_id = %invoke.invoke_id,
                        error = %err,
                        "zcode decision socket write failed; delivery unconfirmed"
                    );
                    false
                }
            };
            let outcome_state = registry
                .complete_delivery(
                    &invoke.invoke_id,
                    if delivered {
                        DeliveryOutcome::Delivered
                    } else {
                        DeliveryOutcome::Unconfirmed
                    },
                )
                .ok();
            // returned 事件仅在确认送达且记录真实迁移成功(Decided →
            // ReturnedToRuntime)时发布:决定被 MCP cancel 抢先撤销时,卡片
            // 已由 cancel 路径摘除(对 cancelled 回复的 ack 不改变终态,
            // complete_delivery 返回 Err → outcome_state=None),不重复发布
            // removed + summary。
            if delivered && outcome_state.is_some() {
                hooks.publish_attention_returned(&invoke, kind).await;
            } else if outcome_state.is_some() {
                // 仅在记录仍处于本连接管理的 Decided 阶段时发布;取消竞争
                // 等情形下卡片已由对应路径摘除,不重复发布。
                hooks.publish_attention_removed(&invoke, kind, "delivery-unconfirmed").await;
            }
            if kind == InvokeKind::PermissionRequest {
                hooks.mark_awaiting_approval(invoke.native_session_id.as_deref(), false);
            }
            if !delivered {
                tracing::warn!(
                    invoke_id = %invoke.invoke_id,
                    "zcode decision delivery unconfirmed; outcome stays unknown"
                );
            }
        }
        Outcome::Gone | Outcome::Timeout => {
            // 超时:过期;连接失效:撤销(本机已处理)。两者都拒绝一切
            // 迟到回复;helper 应答统一 `expired`(见 settle_without_decision)。
            settle_without_decision(&hooks, &invoke, kind, matches!(outcome, Outcome::Timeout))
                .await;
            let _ = write_reply(writer, &HookReply::expired()).await;
        }
    }
}

/// 有限等待交付确认(R2-ZC01):读监视事件,仅当该行是绑定本 invoke 的
/// 合法 ack(helper 已完成原生协议输出)才算确认。EOF、错误内容、等待
/// 超时一律 `false`(不静默假定已送达;迟到确认不影响既有终态)。
async fn wait_delivery_ack(
    event_rx: &mut oneshot::Receiver<ConnEvent>,
    invoke_id: &str,
) -> bool {
    let ack_wait = std::time::Duration::from_millis(contract::DELIVERY_ACK_WAIT_MS);
    match tokio::time::timeout(ack_wait, event_rx).await {
        Ok(Ok(ConnEvent::Line(line))) => contract::is_delivery_ack(&line, invoke_id),
        _ => false,
    }
}

/// Timeout/Gone 分支收尾:超时 → 过期;连接失效 → 撤销(本机已处理)。
/// 随后清除审批等待标记并摘除远程卡片。`pub` 仅供测试确定性驱动:
/// 决定与超时/消失同时就绪时 `tokio::select!` 随机选分支,黑盒下无法
/// 稳定复现落选分支(直接调用本函数即模拟"超时/消失分支胜出")。
pub async fn settle_without_decision(
    hooks: &ZcodeHooks,
    invoke: &HookInvoke,
    kind: InvokeKind,
    timed_out: bool,
) {
    let registry = hooks.registry();
    let settled = if timed_out {
        // 超时收尾为单锁原子判定(检查 + 迁移同一把锁内完成):Waiting →
        // 置 Expired;`resolve` 恰在 deadline 之后到达时已把记录置为
        // Expired 并返回 Err(Expired)(R2-ZC01 回归分支,该路径不摘卡)
        // → 同样返回 true,卡片移除与等待标记清理仍必须恰好执行一次。
        // 不得拆回「探针 Expired + expire」两步:两步各自独立加锁,探针
        // 读到 Waiting 后迟到 resolve 把记录置 Expired、expire 返回
        // AlreadyDecided,settled=false 会漏摘卡片与等待标记。
        registry.expire_or_already_expired(&invoke.invoke_id)
    } else {
        // 连接失效收尾:cancel 仅在 Waiting → HandledLocally 时成功(Ok)。
        // 记录已被 MCP cancel 事件置为 HandledLocally 或已被决定原子锁定
        // 时,cancel 返回 AlreadyDecided(不存在 Cancelled 错误面)
        // → settled=false,卡片/标记交给既有路径,行为与旧代码一致。
        registry.cancel(&invoke.invoke_id).is_ok()
    };
    // 竞态兜底:决定与超时/连接消失同时就绪、select 选中本分支时,决定已
    // 被 resolve 原子锁定(Decided),但应答通道 rx 已随落选分支被丢弃,
    // 决定无法再送达 helper —— 保持 expired 回复、helper 回退原生确认是
    // 安全行为(不会自动允许)。此时仍必须终态化注册表记录(否则过期
    // Decided 记录永久滞留)并照常摘除远程卡片(否则浏览器卡片残留)。
    let raced = !settled && registry.finalize_decided_after_race(&invoke.invoke_id);
    if settled || raced {
        if kind == InvokeKind::PermissionRequest {
            hooks.mark_awaiting_approval(invoke.native_session_id.as_deref(), false);
        }
        hooks.publish_attention_removed(invoke, kind, "expired").await;
    }
}

fn validate_invoke(invoke: &HookInvoke, kind: InvokeKind) -> Result<(), String> {
    if invoke.version != contract::CONTRACT_VERSION {
        return Err(format!("unsupported contract version {}", invoke.version));
    }
    match kind {
        InvokeKind::PermissionRequest => {
            if invoke.tool_name.is_none() {
                return Err("permission_request requires tool_name".to_string());
            }
            if let Some(input) = &invoke.tool_input {
                let serialized =
                    serde_json::to_string(input).map_err(|_| "tool_input not serializable")?;
                if serialized.len() > MAX_FRAME_BYTES {
                    return Err("tool_input exceeds limit".to_string());
                }
            }
            Ok(())
        }
        InvokeKind::AskUser => {
            let Some(ask) = &invoke.ask else {
                return Err("ask_user requires ask payload".to_string());
            };
            ask.validate()
        }
    }
}

async fn write_reply(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    reply: &HookReply,
) -> std::io::Result<()> {
    let mut line = serde_json::to_string(reply).expect("reply serializes");
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await
}

enum ReadLineError {
    Oversize,
    Io,
}

/// 读一行,超过上限立即拒绝(不把超限内容留在内存)。
async fn read_limited_line(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    out: &mut String,
) -> Result<(), ReadLineError> {
    out.clear();
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let available = match reader.fill_buf().await {
            Ok(slice) => slice,
            Err(_) => return Err(ReadLineError::Io),
        };
        if available.is_empty() {
            // EOF:有内容但无换行 → 协议违约(要求单行);空 → 连接关闭。
            return if buf.is_empty() && out.is_empty() {
                Err(ReadLineError::Io)
            } else {
                Err(ReadLineError::Oversize)
            };
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(newline) => {
                buf.extend_from_slice(&available[..=newline]);
                reader.consume(newline + 1);
                return match String::from_utf8(buf) {
                    Ok(text) => {
                        *out = text;
                        Ok(())
                    }
                    Err(_) => Err(ReadLineError::Io),
                };
            }
            None => {
                buf.extend_from_slice(available);
                let consumed = available.len();
                reader.consume(consumed);
                if buf.len() > MAX_FRAME_BYTES {
                    return Err(ReadLineError::Oversize);
                }
            }
        }
    }
}

/// 由原生 Hook 单行输入构造 status invoke(helper 侧与测试共用)。
pub fn status_invoke_from_line(line: &str) -> Result<HookInvoke, ParseError> {
    let native = contract::parse_native_input(line)?;
    Ok(HookInvoke {
        version: contract::CONTRACT_VERSION,
        agent_kind: "zcode".to_string(),
        invoke_id: uuid::Uuid::new_v4().to_string(),
        event: EVENT_STATUS.to_string(),
        native_session_id: native.session_id,
        tool_name: native.tool_name,
        tool_use_id: native.tool_use_id,
        requested_wait_ms: STATUS_WAIT_MS,
        tool_input: native.tool_input,
        ask: None,
        status_event: Some(native.hook_event_name),
        status_input: None,
    })
}

/// 状态事件确认等待(helper 侧;远小于决策预算)。
pub const STATUS_ACK_WAIT: Duration = Duration::from_millis(STATUS_WAIT_MS);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zcode::link::ZcodeHooks;
    use super::super::pending::PendingState;
    use std::os::unix::net::UnixStream as StdUnixStream;

    fn test_hooks(dir: &Path) -> Arc<ZcodeHooks> {
        Arc::new(ZcodeHooks::new("device-test", dir.join("zcode-hook.sock")))
    }

    fn socket_path(dir: &Path) -> PathBuf {
        dir.join("zcode-hook.sock")
    }

    async fn start(dir: &Path, hooks: Arc<ZcodeHooks>) -> tokio::task::JoinHandle<()> {
        serve(
            HookServerConfig {
                socket_path: socket_path(dir),
            },
            hooks,
        )
        .await
        .unwrap()
    }

    fn permission_invoke(id: &str, wait_ms: u64) -> HookInvoke {
        HookInvoke {
            version: contract::CONTRACT_VERSION,
            agent_kind: "zcode".to_string(),
            invoke_id: id.to_string(),
            event: EVENT_PERMISSION_REQUEST.to_string(),
            native_session_id: Some("sess-1".to_string()),
            tool_name: Some("Bash".to_string()),
            tool_use_id: None,
            requested_wait_ms: wait_ms,
            tool_input: Some(serde_json::json!({"command": "echo hi"})),
            ask: None,
            status_event: None,
            status_input: None,
        }
    }

    /// 发送一行请求,返回连接(写半保留防 EOF)与读端。
    async fn send_request(
        invoke: &HookInvoke,
        socket: &Path,
    ) -> (tokio::net::unix::OwnedWriteHalf, BufReader<tokio::net::unix::OwnedReadHalf>) {
        let client = UnixStream::connect(socket).await.unwrap();
        let (reader, mut writer) = client.into_split();
        let mut line = serde_json::to_string(invoke).unwrap();
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
        (writer, BufReader::new(reader))
    }

    async fn read_reply(
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> HookReply {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    /// 模拟新 helper 合同:收到决定并完成原生输出后回发交付确认行。
    async fn send_ack(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        invoke_id: &str,
    ) {
        let mut line = contract::delivery_ack_json(invoke_id);
        line.push('\n');
        writer.write_all(line.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
    }

    /// socket 目录 0700、socket 文件 0600(用户私有,04 §8.5)。
    #[tokio::test]
    async fn socket_permissions_are_user_private() {
        let dir = tempfile::tempdir().unwrap();
        let handle = start(dir.path(), test_hooks(dir.path())).await;
        let (dir_mode, file_mode) = socket_permissions(&socket_path(dir.path())).unwrap();
        assert_eq!(dir_mode, 0o700, "socket 目录必须 0700");
        assert_eq!(file_mode, 0o600, "socket 文件必须 0600");
        handle.abort();
        remove_socket(&socket_path(dir.path()));
    }

    /// peer UID 校验:本用户 socketpair 双端通过(跨 UID 分支无法在本进程
    /// 内构造另一 euid 连接,拒绝分支以 FIXTURE 级逻辑覆盖)。
    #[test]
    fn peer_check_accepts_self() {
        use std::os::unix::io::AsRawFd;
        let (a, b) = StdUnixStream::pair().unwrap();
        assert!(peer_is_current_user(a.as_raw_fd()));
        assert!(peer_is_current_user(b.as_raw_fd()));
    }

    /// 等待服务端完成登记(轮询注册表,有界)。
    async fn wait_registered(registry: &PendingRegistry, invoke_id: &str) {
        for _ in 0..200 {
            if registry.get(invoke_id).is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("invoke {invoke_id} not registered in time");
    }

    /// 全链路(FIXTURE):连接 → 登记 → 原子决定 → helper 收到 allowed →
    /// 完成原生输出后回 ack → ReturnedToRuntime(R2-ZC01 交付确认)。
    #[tokio::test]
    async fn round_trip_allow_via_socket() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (mut guard, mut reader) =
            send_request(&permission_invoke("rt-1", 10_000), &socket).await;
        wait_registered(hooks.registry(), "rt-1").await;
        hooks.registry().resolve("rt-1", HookReply::allowed()).unwrap();
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_ALLOWED);
        // 决定写回后状态仍是 Decided(未确认不冒充已返回)。
        assert_eq!(
            hooks.registry().get("rt-1").unwrap().state,
            PendingState::Decided
        );
        send_ack(&mut guard, "rt-1").await;
        // ack 到达后: ReturnedToRuntime(确认 = 原生协议结果已输出)。
        for _ in 0..100 {
            if hooks.registry().get("rt-1").unwrap().state == PendingState::ReturnedToRuntime {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            hooks.registry().get("rt-1").unwrap().state,
            PendingState::ReturnedToRuntime
        );
        handle.abort();
        remove_socket(&socket);
    }

    /// 缺失/迟到确认(R2-ZC01):helper 收到决定但不回 ack(旧 helper 行为)
    /// → 有限等待超时后按未确认收尾:HandledLocally(不冒充 Returned),
    /// 且不自动重新投递(迟到决定被拒)。
    #[tokio::test]
    async fn missing_ack_leaves_delivery_unconfirmed() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (_guard, mut reader) =
            send_request(&permission_invoke("rt-noack", 10_000), &socket).await;
        wait_registered(hooks.registry(), "rt-noack").await;
        hooks
            .registry()
            .resolve("rt-noack", HookReply::allowed())
            .unwrap();
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_ALLOWED);
        // ack 等待超时(合同常量)→ Unconfirmed → HandledLocally。
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(contract::DELIVERY_ACK_WAIT_MS + 1_500);
        loop {
            match hooks.registry().get("rt-noack").map(|s| s.state) {
                Some(PendingState::HandledLocally) => break,
                Some(_) if tokio::time::Instant::now() >= deadline => {
                    panic!("ack 超时后必须按未确认收尾")
                }
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        // 不自动重新投递:迟到决定一律拒绝。
        assert_eq!(
            hooks
                .registry()
                .resolve("rt-noack", HookReply::allowed())
                .unwrap_err(),
            PendingError::Cancelled
        );
        handle.abort();
        remove_socket(&socket);
    }

    /// 错误绑定的 ack 行(R2-ZC01):内容不是本 invoke 的确认 → 未确认收尾。
    #[tokio::test]
    async fn wrong_ack_binding_is_unconfirmed() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (mut guard, mut reader) =
            send_request(&permission_invoke("rt-badack", 10_000), &socket).await;
        wait_registered(hooks.registry(), "rt-badack").await;
        hooks
            .registry()
            .resolve("rt-badack", HookReply::allowed())
            .unwrap();
        let _ = read_reply(&mut reader).await;
        // 回发绑定其他 invoke 的 ack:不得视为确认。
        send_ack(&mut guard, "some-other-invoke").await;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(contract::DELIVERY_ACK_WAIT_MS + 1_500);
        loop {
            match hooks.registry().get("rt-badack").map(|s| s.state) {
                Some(PendingState::HandledLocally) => break,
                Some(_) if tokio::time::Instant::now() >= deadline => {
                    panic!("错误绑定 ack 不得视为确认")
                }
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        handle.abort();
        remove_socket(&socket);
    }

    /// deny:消息原样回传。
    #[tokio::test]
    async fn round_trip_deny_carries_message() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (_guard, mut reader) = send_request(&permission_invoke("rt-2", 10_000), &socket).await;
        wait_registered(hooks.registry(), "rt-2").await;
        hooks
            .registry()
            .resolve("rt-2", HookReply::denied("User declined this action in Agent Console"))
            .unwrap();
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_DENIED);
        assert_eq!(reply.message.as_deref(), Some("User declined this action in Agent Console"));
        handle.abort();
        remove_socket(&socket);
    }

    /// 同一 invoke_id 二次连接 = 疑似 Hook 双注册:第二个连接被显式拒绝。
    #[tokio::test]
    async fn duplicate_invoke_rejected_without_second_card() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (_g1, _r1) = send_request(&permission_invoke("dup-1", 10_000), &socket).await;
        let (_g2, mut r2) = send_request(&permission_invoke("dup-1", 10_000), &socket).await;
        wait_registered(hooks.registry(), "dup-1").await;
        let reply = read_reply(&mut r2).await;
        assert_eq!(reply.status, contract::STATUS_REJECTED);
        // 第一连接仍可正常决定。
        hooks.registry().resolve("dup-1", HookReply::allowed()).unwrap();
        handle.abort();
        remove_socket(&socket);
    }

    /// 超限输入:整单拒绝,不截断后批准未知动作。
    #[tokio::test]
    async fn oversize_frame_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let handle = start(dir.path(), test_hooks(dir.path())).await;
        let socket = socket_path(dir.path());
        let client = UnixStream::connect(&socket).await.unwrap();
        let (reader, mut writer) = client.into_split();
        let oversized = "x".repeat(MAX_FRAME_BYTES + 32);
        writer.write_all(oversized.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        let mut reader = BufReader::new(reader);
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_REJECTED);
        handle.abort();
        remove_socket(&socket);
    }

    /// Bridge 侧超时:helper 收到 expired,迟到决定被拒(04 §8.6)。
    #[tokio::test]
    async fn timeout_returns_expired_and_rejects_late() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let mut invoke = permission_invoke("rt-timeout", contract::MIN_REMOTE_WAIT_MS);
        invoke.tool_input = None;
        let (_guard, mut reader) = send_request(&invoke, &socket).await;
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_EXPIRED);
        assert_eq!(
            hooks
                .registry()
                .resolve("rt-timeout", HookReply::allowed())
                .unwrap_err(),
            PendingError::Expired
        );
        handle.abort();
        remove_socket(&socket);
    }

    /// helper 消失(决定前 EOF):pending 撤销,迟到决定被拒。
    #[tokio::test]
    async fn helper_disappearance_cancels_pending() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (writer, reader) =
            send_request(&permission_invoke("rt-drop", 30_000), &socket).await;
        wait_registered(hooks.registry(), "rt-drop").await;
        drop(writer); // helper 消失
        drop(reader);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            hooks
                .registry()
                .resolve("rt-drop", HookReply::allowed())
                .unwrap_err(),
            PendingError::Cancelled
        );
        handle.abort();
        remove_socket(&socket);
    }

    /// cancel 事件通道(宿主取消传播):撤销等待中的 invoke。
    #[tokio::test]
    async fn cancel_event_revokes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let (_guard, _reader) = send_request(&permission_invoke("rt-cancel", 30_000), &socket).await;
        wait_registered(hooks.registry(), "rt-cancel").await;
        let mut cancel = permission_invoke("rt-cancel", contract::MIN_REMOTE_WAIT_MS);
        cancel.event = EVENT_CANCEL.to_string();
        cancel.tool_name = None;
        cancel.tool_input = None;
        cancel.native_session_id = None;
        let (_cw, mut creader) = send_request(&cancel, &socket).await;
        let reply = read_reply(&mut creader).await;
        assert_eq!(reply.status, contract::STATUS_ACCEPTED);
        assert_eq!(
            hooks
                .registry()
                .resolve("rt-cancel", HookReply::allowed())
                .unwrap_err(),
            PendingError::Cancelled
        );
        handle.abort();
        remove_socket(&socket);
    }

    /// 快照恢复必须携带操作信息(Codex 复现:runtime_snapshot 的
    /// requested_action 为空但仍 valid=true,首次打开详情/重连后用户看不到
    /// 具体动作即可批准)。经 socket 实际登记后读快照,并要求与实时事件
    /// 卡片(publish_attention_added 用的 approval_card)内容一致。
    #[tokio::test]
    async fn snapshot_pending_approval_carries_action_summary() {
        use crate::domain as dm;
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let invoke = permission_invoke("snap-1", 10_000);
        let (_guard, _reader) = send_request(&invoke, &socket).await;
        wait_registered(hooks.registry(), "snap-1").await;
        let key = dm::SessionKey::zcode("device-test", "sess-1");
        let snapshot = hooks.runtime_snapshot(&key);
        assert_eq!(snapshot.pending_approvals.len(), 1);
        let approval = &snapshot.pending_approvals[0];
        assert!(approval.valid);
        assert!(
            approval.requested_action.0.contains("echo hi"),
            "快照必须可看懂实际操作,requested_action={:?}",
            approval.requested_action.0
        );
        // 与实时事件卡片一致(同 invoke 构造的 approval_card)。
        let live_card = crate::zcode::link::approval_card_for_test(&invoke);
        let crate::domain::PendingAttention::Approval(live) = live_card else {
            panic!("卡片类型错误");
        };
        assert_eq!(approval.requested_action.0, live.requested_action.0);
        assert_eq!(approval.risk_description.0, live.risk_description.0);
        handle.abort();
        remove_socket(&socket);
    }

    /// 状态事件:即时确认并登记元数据,不进入决策等待。
    #[tokio::test]
    async fn status_event_acknowledged_without_wait() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = test_hooks(dir.path());
        let socket = socket_path(dir.path());
        let handle = start(dir.path(), hooks.clone()).await;
        let invoke = status_invoke_from_line(
            r#"{"hook_event_name":"SessionStart","session_id":"sess-9","source":"startup","cwd":"/tmp/ws"}"#,
        )
        .unwrap();
        let (_guard, mut reader) = send_request(&invoke, &socket).await;
        let reply = read_reply(&mut reader).await;
        assert_eq!(reply.status, contract::STATUS_ACCEPTED);
        let observed = hooks.observed_session("sess-9").expect("session observed");
        assert!(observed.discovered);
        handle.abort();
        remove_socket(&socket);
    }
}
