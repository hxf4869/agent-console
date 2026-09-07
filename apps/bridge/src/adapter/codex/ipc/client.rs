//! Codex Desktop IPC 客户端(follower 角色)。
//!
//! 职责(执行规格 §12):
//! - Unix socket 连接 + `initialize` 握手,拿到本端 clientId。
//! - request/response 关联(response 按 `requestId` 路由,带超时)。
//! - broadcast 分发给订阅者;对路由器的 client-discovery 一律应答 canHandle=false
//!   (Bridge 首版不作为任何 IPC request 的 handler;来自路由器的定向 request
//!   一律应答 `no-handler-for-request`)。
//! - follower 辅助:owner 发现、following 广播、快照补偿(load-complete-history)。
//!
//! 边界:
//! - 连接失败只返回错误:不删除 socket、不终止/重启 Codex(§12)。
//! - 断线后的重连退避由上层调度:客户端只暴露连接状态与失败原因。
//! - 写方法必须显式传入 owner clientId,帧层做定向转发,不做猜测性广播写。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use super::frame::{encode_frame, FrameDecoder, FrameError, DEFAULT_MAX_FRAME_BYTES};
use super::messages as m;
use super::messages::{
    request_version, BroadcastFrame, ClientDiscoveryResponseFrame, DiscoveryAnswer,
    FollowingChangedParams, IncomingMessage, InitializeParams, InitializeResult, InterruptMode,
    InterruptTurnParams, InterruptTurnResult, LoadCompleteHistoryParams, LoadCompleteHistoryResult,
    OutgoingMessage, OwnerDiscoveryParams, RequestFrame, ResponseFrame, ResultType,
    StartTurnParams, StartTurnResult, SteerTurnParams, SteerTurnResult, StreamChange,
    StreamStateChangedParams,
};

pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Desktop owner 路由器在无匹配客户端时约 10 秒返回 `no-client-found`；
/// discovery 必须等到该稳定结果，不能先以通用请求超时截断。
const OWNER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(12);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// 发送队列上限(§12:一次连接的队列有上限)。满即拒绝,不无界缓冲。
pub const DEFAULT_OUTBOUND_QUEUE: usize = 256;
/// 广播事件通道上限;慢消费者承受背压而非无界累积。
pub const DEFAULT_EVENT_QUEUE: usize = 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("socket connect failed: {0}")]
    Connect(String),
    #[error("not connected")]
    NotConnected,
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("request timed out after {timeout_secs}s (method: {method})")]
    RequestTimeout { method: String, timeout_secs: u64 },
    #[error("peer returned error: {0}")]
    Peer(String),
    #[error("connection lost: {0}")]
    ConnectionLost(String),
    #[error("frame protocol violated: {0}")]
    Frame(#[from] FrameError),
    #[error("outbound queue full (capacity {capacity})")]
    QueueFull { capacity: usize },
}

#[derive(Debug, Clone)]
pub struct IpcClientConfig {
    pub socket_path: PathBuf,
    pub client_type: String,
    pub max_frame_bytes: u32,
    pub outbound_queue: usize,
    pub event_queue: usize,
    pub default_timeout: Duration,
}

impl IpcClientConfig {
    pub fn new(socket_path: PathBuf, client_type: impl Into<String>) -> Self {
        Self {
            socket_path,
            client_type: client_type.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            outbound_queue: DEFAULT_OUTBOUND_QUEUE,
            event_queue: DEFAULT_EVENT_QUEUE,
            default_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

/// 连接状态;`Disconnected` 携带原因,供上层决定退避重连。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Connected { client_id: String },
    Disconnected { reason: String },
}

/// follower 视角的事件流(强类型)。
#[derive(Debug, Clone)]
pub enum IpcEvent {
    /// thread-stream-state-changed(snapshot / patches)。
    StreamChanged(StreamStateChangedParams),
    /// 其他已识别 broadcast(方法名记录于协议文档 §6.5)。
    Broadcast {
        method: String,
        source_client_id: Option<String>,
        params: Value,
    },
    /// 未识别 broadcast:不解释,仅透出方法名;是否触发补偿由上层策略决定。
    UnknownBroadcast { method: Option<String> },
}

/// 单会话流同步策略(§12:未识别/错序的关键 patch 触发 snapshot 补偿)。
///
/// Desktop 的应用规则:patch 仅在 `baseRevision == 当前 revision` 且来源为
/// 当前 owner 时可应用;否则静默丢弃并重新拉全量快照
/// ([`IpcClient::load_complete_history`])。
#[derive(Debug, Default)]
pub struct StreamSync {
    revision: Option<u64>,
}

/// `observe` 的裁决结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDecision {
    /// 快照:直接采纳其 revision。
    TakeSnapshot,
    /// patch 可应用(调用方负责应用后推进 revision)。
    ApplyPatch { base_revision: u64, revision: u64 },
    /// patch 被丢弃:revision 缺口/错序/未知载荷 → 调用方应触发快照补偿。
    Resync,
}

impl StreamSync {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> Option<u64> {
        self.revision
    }

    pub fn observe(&mut self, change: &StreamChange) -> StreamDecision {
        match change {
            StreamChange::Snapshot { revision, .. } => {
                self.revision = Some(*revision);
                StreamDecision::TakeSnapshot
            }
            StreamChange::Patches {
                base_revision,
                revision,
                ..
            } => {
                if self.revision == Some(*base_revision) && revision > base_revision {
                    self.revision = Some(*revision);
                    StreamDecision::ApplyPatch {
                        base_revision: *base_revision,
                        revision: *revision,
                    }
                } else {
                    StreamDecision::Resync
                }
            }
        }
    }

    /// owner 切换或断线重连后:revision 序列不再可信,显式失效。
    pub fn invalidate(&mut self) {
        self.revision = None;
    }
}

#[derive(Debug)]
struct Shared {
    pending: parking_lot::Mutex<HashMap<String, oneshot::Sender<ResponseFrame>>>,
    state_tx: watch::Sender<ConnectionState>,
}

/// 打开连接并完成握手。返回客户端句柄与事件接收端(单消费者)。
pub async fn connect(
    config: IpcClientConfig,
) -> Result<(IpcClient, mpsc::Receiver<IpcEvent>), IpcError> {
    let stream = timeout(
        DEFAULT_CONNECT_TIMEOUT,
        tokio::net::UnixStream::connect(&config.socket_path),
    )
    .await
    .map_err(|_| IpcError::Connect("connect timed out".to_string()))?
    .map_err(|e| IpcError::Connect(e.to_string()))?;

    let (outbound_tx, outbound_rx) = mpsc::channel::<OutgoingMessage>(config.outbound_queue);
    let (event_tx, event_rx) = mpsc::channel::<IpcEvent>(config.event_queue.max(16));
    let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected {
        reason: "handshake".to_string(),
    });
    let shared = Arc::new(Shared {
        pending: parking_lot::Mutex::new(HashMap::new()),
        state_tx,
    });

    // 注册握手请求后再发送,响应到达时 reader 能直接路由。
    let init_request_id = Uuid::new_v4().to_string();
    let (pending_tx, pending_rx) = oneshot::channel();
    shared
        .pending
        .lock()
        .insert(init_request_id.clone(), pending_tx);

    let init_frame = OutgoingMessage::Request {
        frame: RequestFrame {
            request_id: init_request_id.clone(),
            source_client_id: Some("initializing-client".to_string()),
            version: request_version(m::method::INITIALIZE, &Value::Null, None),
            method: m::method::INITIALIZE.to_string(),
            params: serde_json::to_value(InitializeParams {
                client_type: config.client_type.clone(),
            })
            .expect("initialize params serialize"),
            target_client_id: None,
            host_id: None,
            timeout_ms: None,
        },
    };
    let raw = encode_frame(&init_frame.to_value(), config.max_frame_bytes)
        .map_err(|e| IpcError::Frame(e.into()))?;
    let (read_half, mut write_half) = stream.into_split();
    write_half
        .write_all(&raw)
        .await
        .map_err(|e| IpcError::Handshake(e.to_string()))?;

    let reader = tokio::spawn(reader_loop(
        read_half,
        config.clone(),
        shared.clone(),
        outbound_tx.clone(),
        event_tx,
    ));
    let writer = tokio::spawn(writer_loop(
        write_half,
        config.max_frame_bytes,
        outbound_rx,
        shared.clone(),
    ));

    let resp = timeout(DEFAULT_CONNECT_TIMEOUT, pending_rx)
        .await
        .map_err(|_| IpcError::Handshake("handshake timed out".to_string()))?
        .map_err(|_| IpcError::Handshake("connection closed during handshake".to_string()))?;

    if resp.result_type != ResultType::Success
        || resp.method.as_deref() != Some(m::method::INITIALIZE)
    {
        return Err(IpcError::Handshake(
            resp.error.unwrap_or_else(|| "rejected".to_string()),
        ));
    }
    let init_result: InitializeResult = serde_json::from_value(resp.result.unwrap_or(Value::Null))
        .map_err(|e| IpcError::Handshake(format!("bad initialize result: {e}")))?;
    let client_id = init_result.client_id;

    let _ = shared.state_tx.send(ConnectionState::Connected {
        client_id: client_id.clone(),
    });

    Ok((
        IpcClient {
            config,
            client_id,
            outbound_tx,
            state_rx,
            shared,
            reader,
            writer,
        },
        event_rx,
    ))
}

/// 读循环:帧解码 → 消息分发。协议错误即断开并置为 Disconnected。
async fn reader_loop(
    mut read_half: tokio::net::unix::OwnedReadHalf,
    config: IpcClientConfig,
    shared: Arc<Shared>,
    outbound_tx: mpsc::Sender<OutgoingMessage>,
    event_tx: mpsc::Sender<IpcEvent>,
) {
    let mut decoder = FrameDecoder::new(config.max_frame_bytes);
    let mut chunk = vec![0u8; 64 * 1024];
    let failure: Result<(), String> = 'read: loop {
        match read_half.read(&mut chunk).await {
            Ok(0) => break 'read Err("peer closed connection".to_string()),
            Ok(n) => {
                if let Err(e) = decoder.push(&chunk[..n]) {
                    break 'read Err(e.to_string());
                }
                loop {
                    match decoder.next_frame() {
                        Ok(Some(value)) => {
                            if let Err(e) = dispatch(
                                IncomingMessage::parse(value),
                                &shared,
                                &outbound_tx,
                                &event_tx,
                            )
                            .await
                            {
                                break 'read Err(e);
                            }
                        }
                        Ok(None) => break,
                        Err(e) => break 'read Err(e.to_string()),
                    }
                }
            }
            Err(e) => break 'read Err(e.to_string()),
        }
    };
    let reason = match failure {
        Ok(()) => "reader exited".to_string(),
        Err(reason) => reason,
    };
    // 唤醒全部 pending 与状态观察者。
    {
        let mut pending = shared.pending.lock();
        for (_, tx) in pending.drain() {
            let _ = tx.send(ResponseFrame {
                request_id: String::new(),
                result_type: ResultType::Error,
                method: None,
                handled_by_client_id: None,
                result: None,
                error: Some(format!("connection-closed: {reason}")),
            });
        }
    }
    let _ = shared.state_tx.send(ConnectionState::Disconnected {
        reason: reason.clone(),
    });
    let _ = reason;
}

async fn dispatch(
    msg: IncomingMessage,
    shared: &Arc<Shared>,
    outbound_tx: &mpsc::Sender<OutgoingMessage>,
    event_tx: &mpsc::Sender<IpcEvent>,
) -> Result<(), String> {
    match msg {
        IncomingMessage::Response(resp) => {
            let tx = shared.pending.lock().remove(&resp.request_id);
            if let Some(tx) = tx {
                let _ = tx.send(resp);
            }
            Ok(())
        }
        IncomingMessage::Broadcast(b) => {
            let event = if b.method == m::method::THREAD_STREAM_STATE_CHANGED {
                match serde_json::from_value::<StreamStateChangedParams>(b.params.clone()) {
                    Ok(params) => IpcEvent::StreamChanged(params),
                    // 关键广播解析失败:透出 Unknown,由上层触发 snapshot 补偿。
                    Err(_) => IpcEvent::UnknownBroadcast {
                        method: Some(b.method.clone()),
                    },
                }
            } else {
                IpcEvent::Broadcast {
                    method: b.method.clone(),
                    source_client_id: b.source_client_id.clone(),
                    params: b.params.clone(),
                }
            };
            event_tx
                .send(event)
                .await
                .map_err(|_| "event channel closed".to_string())
        }
        IncomingMessage::ClientDiscoveryRequest(disc) => {
            // Bridge 不注册任何 request handler:一律 canHandle=false。
            send_now(
                outbound_tx,
                OutgoingMessage::ClientDiscoveryResponse {
                    frame: ClientDiscoveryResponseFrame {
                        request_id: disc.request_id,
                        response: DiscoveryAnswer { can_handle: false },
                    },
                },
                shared,
            )
            .await
        }
        IncomingMessage::Request(req) => {
            // 定向到本端的 request(本端未注册 handler):显式拒绝。
            send_now(
                outbound_tx,
                OutgoingMessage::Response {
                    frame: ResponseFrame {
                        request_id: req.request_id,
                        result_type: ResultType::Error,
                        method: Some(req.method),
                        handled_by_client_id: None,
                        result: None,
                        error: Some("no-handler-for-request".to_string()),
                    },
                },
                shared,
            )
            .await
        }
        // 路由器不应把 discovery 响应发给我们(我们从不作为中间路由)。
        IncomingMessage::ClientDiscoveryResponse(_) => Ok(()),
        IncomingMessage::Unknown(value) => {
            let _ = event_tx
                .send(IpcEvent::UnknownBroadcast {
                    method: value
                        .get("method")
                        .and_then(Value::as_str)
                        .map(String::from),
                })
                .await;
            Ok(())
        }
    }
}

async fn send_now(
    outbound_tx: &mpsc::Sender<OutgoingMessage>,
    msg: OutgoingMessage,
    _shared: &Arc<Shared>,
) -> Result<(), String> {
    outbound_tx
        .send(msg)
        .await
        .map_err(|_| "outbound channel closed".to_string())
}

async fn writer_loop(
    mut write_half: tokio::net::unix::OwnedWriteHalf,
    max_frame_bytes: u32,
    mut outbound_rx: mpsc::Receiver<OutgoingMessage>,
    shared: Arc<Shared>,
) {
    while let Some(msg) = outbound_rx.recv().await {
        let raw = match encode_frame(&msg.to_value(), max_frame_bytes) {
            Ok(raw) => raw,
            Err(e) => {
                // 单帧超限:丢弃该帧并让等待方收到错误,不断开连接。
                if let OutgoingMessage::Request { frame } = &msg {
                    if let Some(tx) = shared.pending.lock().remove(&frame.request_id) {
                        let _ = tx.send(ResponseFrame {
                            request_id: frame.request_id.clone(),
                            result_type: ResultType::Error,
                            method: Some(frame.method.clone()),
                            handled_by_client_id: None,
                            result: None,
                            error: Some(e.to_string()),
                        });
                    }
                }
                continue;
            }
        };
        if write_half.write_all(&raw).await.is_err() {
            break;
        }
    }
}

#[derive(Debug)]
pub struct IpcClient {
    config: IpcClientConfig,
    client_id: String,
    outbound_tx: mpsc::Sender<OutgoingMessage>,
    state_rx: watch::Receiver<ConnectionState>,
    shared: Arc<Shared>,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// 定向目标(通常为已确认的 owner clientId)。
    pub target_client_id: Option<String>,
    pub host_id: Option<String>,
    pub timeout: Option<Duration>,
}

impl IpcClient {
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn config(&self) -> &IpcClientConfig {
        &self.config
    }

    pub fn connection_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    /// 发送 request 并等待匹配的 response(超时不重试;失败由上层决定动作)。
    pub async fn request(
        &self,
        method: &str,
        params: Value,
        opts: RequestOptions,
    ) -> Result<ResponseFrame, IpcError> {
        let request_id = Uuid::new_v4().to_string();
        let version = request_version(method, &params, opts.host_id.as_deref());
        let (pending_tx, pending_rx) = oneshot::channel();
        {
            let mut pending = self.shared.pending.lock();
            if pending.len() >= self.config.outbound_queue {
                return Err(IpcError::QueueFull {
                    capacity: self.config.outbound_queue,
                });
            }
            pending.insert(request_id.clone(), pending_tx);
        }
        let send_result = self
            .outbound_tx
            .send(OutgoingMessage::Request {
                frame: RequestFrame {
                    request_id: request_id.clone(),
                    source_client_id: Some(self.client_id.clone()),
                    version,
                    method: method.to_string(),
                    params,
                    target_client_id: opts.target_client_id,
                    host_id: opts.host_id,
                    timeout_ms: opts.timeout.map(|d| d.as_millis() as u64),
                },
            })
            .await;
        if send_result.is_err() {
            self.shared.pending.lock().remove(&request_id);
            return Err(IpcError::NotConnected);
        }

        let timeout_duration = opts.timeout.unwrap_or(self.config.default_timeout);
        let resp = match timeout(timeout_duration, pending_rx).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => {
                // pending 被清空:连接已断。
                return Err(IpcError::ConnectionLost("pending dropped".to_string()));
            }
            Err(_) => {
                self.shared.pending.lock().remove(&request_id);
                return Err(IpcError::RequestTimeout {
                    method: method.to_string(),
                    timeout_secs: timeout_duration.as_secs(),
                });
            }
        };
        if resp.result_type == ResultType::Error {
            return Err(IpcError::Peer(
                resp.error.unwrap_or_else(|| "unknown error".to_string()),
            ));
        }
        Ok(resp)
    }

    /// 发送 broadcast(不等待响应)。`target_client_ids` 为 None 时全量广播。
    pub async fn broadcast(
        &self,
        method: &str,
        params: Value,
        target_client_ids: Option<Vec<String>>,
    ) -> Result<(), IpcError> {
        self.outbound_tx
            .send(OutgoingMessage::Broadcast {
                frame: BroadcastFrame {
                    method: method.to_string(),
                    source_client_id: Some(self.client_id.clone()),
                    target_client_ids,
                    version: super::messages::broadcast_version(method),
                    params,
                },
            })
            .await
            .map_err(|_| IpcError::NotConnected)
    }

    // -----------------------------------------------------------------
    // follower 辅助(协议文档 §6)
    // -----------------------------------------------------------------

    /// owner 发现:`Ok(None)` 表示当前没有 owner(`no-client-found`)。
    pub async fn discover_owner(
        &self,
        host_id: &str,
        conversation_id: &str,
    ) -> Result<Option<String>, IpcError> {
        let params = serde_json::to_value(OwnerDiscoveryParams {
            host_id: host_id.to_string(),
            conversation_id: conversation_id.to_string(),
        })
        .expect("owner discovery params serialize");
        match self
            .request(
                m::method::THREAD_OWNER_DISCOVERY,
                params,
                RequestOptions {
                    timeout: Some(OWNER_DISCOVERY_TIMEOUT),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(resp) => Ok(resp.handled_by_client_id),
            Err(IpcError::Peer(e)) if e == "no-client-found" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 订阅/退订某会话的流(owner 会回发全量快照)。
    pub async fn set_following(
        &self,
        params: FollowingChangedParams,
        target_client_ids: Option<Vec<String>>,
    ) -> Result<(), IpcError> {
        let method = m::method::THREAD_STREAM_FOLLOWING_CHANGED;
        self.broadcast(
            method,
            serde_json::to_value(params).expect("following params serialize"),
            target_client_ids,
        )
        .await
    }

    /// 快照补偿:owner 重读完整历史并回发快照,返回最新 revision。
    pub async fn load_complete_history(
        &self,
        host_id: &str,
        conversation_id: &str,
    ) -> Result<u64, IpcError> {
        let params = serde_json::to_value(LoadCompleteHistoryParams {
            conversation_id: conversation_id.to_string(),
        })
        .expect("load history params serialize");
        let resp = self
            .request(
                m::method::THREAD_FOLLOWER_LOAD_COMPLETE_HISTORY,
                params,
                RequestOptions {
                    host_id: Some(host_id.to_string()),
                    timeout: Some(Duration::from_secs(65)),
                    ..Default::default()
                },
            )
            .await?;
        let result: LoadCompleteHistoryResult =
            serde_json::from_value(resp.result.unwrap_or(Value::Null))
                .map_err(|e| IpcError::Peer(format!("bad load-complete-history result: {e}")))?;
        Ok(result.revision)
    }

    // -----------------------------------------------------------------
    // 写方法(协议文档 §7)。全部要求传入已确认的 owner clientId,定向发送。
    // -----------------------------------------------------------------

    pub async fn start_turn(
        &self,
        owner_client_id: &str,
        params: StartTurnParams,
    ) -> Result<StartTurnResult, IpcError> {
        let method = m::method::THREAD_FOLLOWER_START_TURN;
        let resp = self
            .request(
                method,
                serde_json::to_value(&params).expect("start turn params serialize"),
                RequestOptions {
                    target_client_id: Some(owner_client_id.to_string()),
                    host_id: Some("local".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        serde_json::from_value(resp.result.unwrap_or(Value::Null))
            .map_err(|e| IpcError::Peer(format!("bad start-turn result: {e}")))
    }

    pub async fn steer_turn(
        &self,
        owner_client_id: &str,
        params: SteerTurnParams,
    ) -> Result<SteerTurnResult, IpcError> {
        let method = m::method::THREAD_FOLLOWER_STEER_TURN;
        let resp = self
            .request(
                method,
                serde_json::to_value(&params).expect("steer params serialize"),
                RequestOptions {
                    target_client_id: Some(owner_client_id.to_string()),
                    host_id: Some("local".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        serde_json::from_value(resp.result.unwrap_or(Value::Null))
            .map_err(|e| IpcError::Peer(format!("bad steer-turn result: {e}")))
    }

    pub async fn interrupt_turn(
        &self,
        owner_client_id: &str,
        params: InterruptTurnParams,
    ) -> Result<InterruptTurnResult, IpcError> {
        let method = m::method::THREAD_FOLLOWER_INTERRUPT_TURN;
        let resp = self
            .request(
                method,
                serde_json::to_value(&params).expect("interrupt params serialize"),
                RequestOptions {
                    target_client_id: Some(owner_client_id.to_string()),
                    host_id: Some("local".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        serde_json::from_value(resp.result.unwrap_or(Value::Null))
            .map_err(|e| IpcError::Peer(format!("bad interrupt-turn result: {e}")))
    }

    /// 中断便捷构造(mode + 可选期望 turn)。
    pub fn interrupt_params(
        conversation_id: impl Into<String>,
        mode: InterruptMode,
        expected_turn_id: Option<String>,
    ) -> InterruptTurnParams {
        InterruptTurnParams {
            conversation_id: conversation_id.into(),
            mode,
            expected_turn_id,
        }
    }
}

impl Drop for IpcClient {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}
