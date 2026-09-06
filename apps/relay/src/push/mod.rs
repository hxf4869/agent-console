//! Web Push 后端(§24):
//! - 订阅/设置的 HTTP CRUD(挂 /agent-console/api/push 下,浏览器身份认证);
//! - VAPID 公钥只读端点 /push/vapid-public-key(前端 applicationServerKey 来源);
//! - 触发事件:turn completed/failed/interrupted、等待问题、等待风险审批;
//!   普通进度、输出增量、token 更新不推送;session mute 覆盖事件开关;
//! - 默认通知正文只写通用状态(不含标题/prompt/文件名/分支/项目);
//!   用户显式开启 showTitle 后才含 title(仍不含正文);
//! - payload 带内部 session ID、机器可读 kind 与安全相对 deep link
//!   (`/agent-console/s/{uuid}`),不带本机路径,不含 ticket/批准参数(§24);
//! - VAPID 私钥只从 `VAPID_PRIVATE_KEY_FILE` 指向的文件加载,不进仓库与日志;
//!   未配置时 push 发送禁用(订阅仍可存储);
//! - 发送失败不改变任务状态;404/410 → 删除订阅。
//!
//! 自动化测试:`RELAY_PUSH_FAKE_SINK=http://127.0.0.1:port` 时使用转发型
//! fake sender(POST {sink}/push/notify),不调用真实 Push 服务。

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde::Deserialize;

use crate::{
    auth::require_identity,
    state::{api_error, new_request_id, AppState, StableErrorCode as Code},
};

/// 同一 (session, kind) 的推送去重窗口(摘要事件高频,§24 只推状态跃迁)。
const DEDUP_WINDOW: Duration = Duration::from_secs(60);
const DEDUP_MAX_ENTRIES: usize = 4096;

// ---------------------------------------------------------------------------
// 触发矩阵(§24)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushEventKind {
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    WaitingQuestion,
    WaitingApproval,
}

impl PushEventKind {
    fn key(self) -> u8 {
        match self {
            PushEventKind::TurnCompleted => 1,
            PushEventKind::TurnFailed => 2,
            PushEventKind::TurnInterrupted => 3,
            PushEventKind::WaitingQuestion => 4,
            PushEventKind::WaitingApproval => 5,
        }
    }

    /// payload 中的机器可读事件名(与通知设置 JSON 键一致;无凭据语义)。
    pub fn name(self) -> &'static str {
        match self {
            PushEventKind::TurnCompleted => "turnCompleted",
            PushEventKind::TurnFailed => "turnFailed",
            PushEventKind::TurnInterrupted => "turnInterrupted",
            PushEventKind::WaitingQuestion => "waitingQuestion",
            PushEventKind::WaitingApproval => "waitingApproval",
        }
    }

    /// 默认通用文案(§24:不含标题/prompt/文件名/分支/项目)。
    pub fn generic_body(self) -> &'static str {
        match self {
            PushEventKind::TurnCompleted => "任务已完成",
            PushEventKind::TurnFailed => "任务未完成,已失败",
            PushEventKind::TurnInterrupted => "任务已中断",
            PushEventKind::WaitingQuestion => "任务在等待你的回答",
            PushEventKind::WaitingApproval => "任务在等待风险审批",
        }
    }

    fn from_turn_outcome(outcome: i32) -> Option<Self> {
        use agent_console_protocol::v1::LastTurnOutcome;
        let outcome = LastTurnOutcome::try_from(outcome).ok()?;
        match outcome {
            LastTurnOutcome::TurnOutcomeCompleted => Some(PushEventKind::TurnCompleted),
            LastTurnOutcome::TurnOutcomeFailed => Some(PushEventKind::TurnFailed),
            LastTurnOutcome::TurnOutcomeInterrupted => Some(PushEventKind::TurnInterrupted),
            _ => None,
        }
    }
}

/// 一次推送触发(仅元数据;不含正文/标题/路径)。
#[derive(Debug, Clone)]
pub struct PushTrigger {
    pub owner: uuid::Uuid,
    pub device: uuid::Uuid,
    pub agent_kind: String,
    pub native_session_id: String,
    pub kind: PushEventKind,
}

/// 从单个领域事件映射推送触发;不命中返回空(§24 白名单)。
pub fn triggers_from_event(
    owner: uuid::Uuid,
    device: uuid::Uuid,
    agent_kind: &str,
    native_session_id: &str,
    event: &agent_console_protocol::v1::domain_event::Event,
) -> Vec<PushTrigger> {
    use agent_console_protocol::v1::{
        domain_event::Event, PendingAttentionAdded, PendingAttentionKind,
    };
    let mut out = Vec::new();
    let mut push = |kind| {
        out.push(PushTrigger {
            owner,
            device,
            agent_kind: agent_kind.to_string(),
            native_session_id: native_session_id.to_string(),
            kind,
        });
    };
    match event {
        Event::TurnLifecycle(t) => {
            if let Some(kind) = PushEventKind::from_turn_outcome(t.outcome) {
                push(kind);
            }
        }
        Event::PendingAttentionAdded(PendingAttentionAdded { attention }) => {
            match attention.as_ref() {
                Some(agent_console_protocol::v1::pending_attention_added::Attention::Question(
                    _,
                )) => {
                    push(PushEventKind::WaitingQuestion);
                }
                Some(agent_console_protocol::v1::pending_attention_added::Attention::Approval(
                    _,
                )) => {
                    push(PushEventKind::WaitingApproval);
                }
                None => {}
            }
        }
        // 列表流上的摘要变化也携带终态与待处理关注(Browser 未订阅详情流时
        // 唯一来源);重复状态由 notify 侧去重窗口抑制。
        Event::SessionSummaryChanged(s) => {
            if let Some(kind) = PushEventKind::from_turn_outcome(s.last_turn_outcome) {
                push(kind);
            }
            let has_kind = |k: PendingAttentionKind| {
                s.pending_attention_kinds
                    .iter()
                    .any(|v| PendingAttentionKind::try_from(*v) == Ok(k))
            };
            if s.pending_attention_count > 0 {
                if has_kind(PendingAttentionKind::AttentionUserQuestion) {
                    push(PushEventKind::WaitingQuestion);
                }
                if has_kind(PendingAttentionKind::AttentionRiskApproval) {
                    push(PushEventKind::WaitingApproval);
                }
            }
        }
        _ => {}
    }
    out
}

// ---------------------------------------------------------------------------
// Sender(可注入;fake 转发,不调真实服务)
// ---------------------------------------------------------------------------

pub enum PushSendError {
    /// 404/410:订阅失效,应删除(§24)。
    Gone,
    /// 其他失败:不改任务状态,保留订阅。
    Transient,
}

#[async_trait::async_trait]
pub trait PushSender: Send + Sync {
    async fn send(
        &self,
        endpoint: &str,
        p256dh: &str,
        auth: &str,
        payload: &[u8],
    ) -> Result<(), PushSendError>;
}

/// 真实 Web Push(web-push crate + VAPID;client 复用,创建成本较高)。
struct WebPushSender {
    client: web_push::IsahcWebPushClient,
    private_pem: Vec<u8>,
    subject: Option<String>,
}

impl WebPushSender {
    fn new(private_pem: Vec<u8>, subject: Option<String>) -> Result<Self, ()> {
        Ok(Self {
            client: web_push::IsahcWebPushClient::new().map_err(|_| ())?,
            private_pem,
            subject,
        })
    }
}

#[async_trait::async_trait]
impl PushSender for WebPushSender {
    async fn send(
        &self,
        endpoint: &str,
        p256dh: &str,
        auth: &str,
        payload: &[u8],
    ) -> Result<(), PushSendError> {
        use web_push::{
            ContentEncoding, SubscriptionInfo, VapidSignatureBuilder, WebPushClient,
            WebPushMessageBuilder,
        };
        let subscription = SubscriptionInfo::new(endpoint, p256dh, auth);
        let mut sig = VapidSignatureBuilder::from_pem(&self.private_pem[..], &subscription)
            .map_err(|_| PushSendError::Transient)?;
        if let Some(subject) = self.subject.as_deref() {
            sig.add_claim("sub", subject.to_string());
        }
        let signature = sig.build().map_err(|_| PushSendError::Transient)?;
        let mut builder = WebPushMessageBuilder::new(&subscription);
        builder.set_payload(ContentEncoding::Aes128Gcm, payload);
        builder.set_vapid_signature(signature);
        let message = builder.build().map_err(|_| PushSendError::Transient)?;
        match self.client.send(message).await {
            Ok(()) => Ok(()),
            Err(web_push::WebPushError::EndpointNotValid)
            | Err(web_push::WebPushError::EndpointNotFound) => Err(PushSendError::Gone),
            Err(_) => Err(PushSendError::Transient),
        }
    }
}

/// fake sender(测试):把决策后的通知转发到本地 fake 服务,便于断言
/// 触发矩阵/静音/失效清理;不调用真实 Push 服务。
struct ForwardingSender {
    base: String,
    http: reqwest::Client,
}

#[async_trait::async_trait]
impl PushSender for ForwardingSender {
    async fn send(
        &self,
        endpoint: &str,
        p256dh: &str,
        auth: &str,
        payload: &[u8],
    ) -> Result<(), PushSendError> {
        let body = serde_json::json!({
            "endpoint": endpoint,
            "p256dh": p256dh,
            "auth": auth,
            "payload": base64_std(payload),
        });
        match self
            .http
            .post(format!("{}/push/notify", self.base))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => match resp.status().as_u16() {
                200..=299 => Ok(()),
                404 | 410 => Err(PushSendError::Gone),
                _ => Err(PushSendError::Transient),
            },
            Err(_) => Err(PushSendError::Transient),
        }
    }
}

fn base64_std(b: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// Push 全局状态(AppState 持有)。
pub struct PushState {
    sender: Box<dyn PushSender>,
    /// VAPID/fake sink 未配置时禁用发送(订阅仍可存储,§24)。
    pub enabled: bool,
    /// VAPID application server key(公钥;供前端 pushManager.subscribe 使用)。
    pub public_key: Option<String>,
    /// (session, kind) → 上次推送时间(去重窗口)。
    dedup: Mutex<HashMap<(uuid::Uuid, u8), Instant>>,
}

impl PushState {
    /// 环境装配:fake sink(测试)优先,其次 VAPID 文件;均未配置 → 禁用。
    pub fn from_env() -> Arc<PushState> {
        let public_key = std::env::var("VAPID_PUBLIC_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        if let Ok(sink) = std::env::var("RELAY_PUSH_FAKE_SINK") {
            if !sink.is_empty() {
                tracing::info!(target: "relay::push", "push sender: fake forwarding sink");
                return Arc::new(Self {
                    sender: Box::new(ForwardingSender {
                        base: sink,
                        http: reqwest::Client::new(),
                    }),
                    enabled: true,
                    public_key,
                    dedup: Mutex::new(HashMap::new()),
                });
            }
        }
        let subject = std::env::var("VAPID_SUBJECT")
            .ok()
            .filter(|s| !s.is_empty());
        let pem = std::env::var("VAPID_PRIVATE_KEY_FILE")
            .ok()
            .and_then(|path| std::fs::read(path).ok());
        match pem {
            Some(private_pem) => match WebPushSender::new(private_pem.clone(), subject) {
                Ok(sender) => {
                    // public_key(VAPID_PUBLIC_KEY)经 /push/vapid-public-key 提供给前端
                    // applicationServerKey;私钥内容与 endpoint 一样不进日志(§25.3)。
                    tracing::info!(target: "relay::push", "push sender: web-push (VAPID)");
                    Arc::new(Self {
                        sender: Box::new(sender),
                        enabled: true,
                        public_key,
                        dedup: Mutex::new(HashMap::new()),
                    })
                }
                Err(()) => {
                    tracing::warn!(target: "relay::push", code = "INTERNAL_ERROR", "push sender init failed, sending disabled");
                    Arc::new(Self {
                        sender: Box::new(DisabledSender),
                        enabled: false,
                        public_key,
                        dedup: Mutex::new(HashMap::new()),
                    })
                }
            },
            None => {
                tracing::info!(target: "relay::push", "push sending disabled (no VAPID config)");
                Arc::new(Self {
                    sender: Box::new(DisabledSender),
                    enabled: false,
                    public_key,
                    dedup: Mutex::new(HashMap::new()),
                })
            }
        }
    }

    fn dedup_allow(&self, session: uuid::Uuid, kind: PushEventKind) -> bool {
        let mut g = self.dedup.lock().unwrap();
        let key = (session, kind.key());
        let now = Instant::now();
        if g.len() > DEDUP_MAX_ENTRIES {
            g.retain(|_, t| now.duration_since(*t) <= DEDUP_WINDOW);
        }
        match g.get(&key) {
            Some(t) if now.duration_since(*t) <= DEDUP_WINDOW => false,
            _ => {
                g.insert(key, now);
                true
            }
        }
    }
}

struct DisabledSender;

#[async_trait::async_trait]
impl PushSender for DisabledSender {
    async fn send(&self, _: &str, _: &str, _: &str, _: &[u8]) -> Result<(), PushSendError> {
        Err(PushSendError::Transient)
    }
}

// ---------------------------------------------------------------------------
// 触发处理(realtime Hub 事件流接入点)
// ---------------------------------------------------------------------------

/// 处理 push 触发(由 realtime 事件循环在锁外调用;失败不影响任务状态,§24)。
pub async fn notify(app: &AppState, triggers: &[PushTrigger]) {
    if triggers.is_empty() || !app.push.enabled {
        return;
    }
    for trigger in triggers {
        let Some(session) = crate::sessions::store::find_session(
            &app.db,
            trigger.device,
            &trigger.agent_kind,
            &trigger.native_session_id,
        )
        .await
        .ok()
        .flatten() else {
            continue;
        };
        // session mute 覆盖事件开关(§24)。
        if session.muted {
            continue;
        }
        let settings = load_settings(&app.db, trigger.owner).await;
        if !settings.event_enabled(trigger.kind) {
            continue;
        }
        // 同一会话同类状态去重(摘要高频更新不重复推送);
        // 仅在通过 mute/开关判定后记录,避免被拦截的事件占用去重窗口。
        if !app.push.dedup_allow(session.id, trigger.kind) {
            continue;
        }
        let payload = serde_json::json!({
            "sessionId": session.id,
            "kind": trigger.kind.name(),
            "deepLink": format!("/agent-console/s/{}", session.id),
            "title": if settings.show_title { serde_json::json!(session.title) } else { serde_json::Value::Null },
            "body": trigger.kind.generic_body(),
        })
        .to_string();
        let subs = sqlx::query_as::<_, (uuid::Uuid, String, String, String)>(
            "SELECT id, endpoint, p256dh_key, auth_key FROM push_subscriptions \
             WHERE owner_id = $1 AND invalidated_at IS NULL",
        )
        .bind(trigger.owner)
        .fetch_all(&app.db)
        .await
        .unwrap_or_default();
        for (id, endpoint, p256dh, auth) in subs {
            match app
                .push
                .sender
                .send(&endpoint, &p256dh, &auth, payload.as_bytes())
                .await
            {
                Ok(()) => {
                    tracing::debug!(target: "relay::push", operation = "push.sent", "push delivered");
                }
                Err(PushSendError::Gone) => {
                    // 失效 endpoint 标记并清理(§24)。
                    let _ = sqlx::query("DELETE FROM push_subscriptions WHERE id = $1")
                        .bind(id)
                        .execute(&app.db)
                        .await;
                }
                Err(PushSendError::Transient) => {
                    tracing::debug!(target: "relay::push", operation = "push.failed", "push send failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 通知设置(§18.6;默认:全部事件开、不显示 title、通用文案)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub show_title: bool,
    turn_completed: bool,
    turn_failed: bool,
    turn_interrupted: bool,
    waiting_question: bool,
    waiting_approval: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            show_title: false,
            turn_completed: true,
            turn_failed: true,
            turn_interrupted: true,
            waiting_question: true,
            waiting_approval: true,
        }
    }
}

impl Settings {
    fn event_enabled(&self, kind: PushEventKind) -> bool {
        match kind {
            PushEventKind::TurnCompleted => self.turn_completed,
            PushEventKind::TurnFailed => self.turn_failed,
            PushEventKind::TurnInterrupted => self.turn_interrupted,
            PushEventKind::WaitingQuestion => self.waiting_question,
            PushEventKind::WaitingApproval => self.waiting_approval,
        }
    }

    fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "showTitle": self.show_title,
            "events": {
                "turnCompleted": self.turn_completed,
                "turnFailed": self.turn_failed,
                "turnInterrupted": self.turn_interrupted,
                "waitingQuestion": self.waiting_question,
                "waitingApproval": self.waiting_approval,
            },
        })
    }
}

async fn load_settings(db: &sqlx::PgPool, owner: uuid::Uuid) -> Settings {
    sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool)>(
        "SELECT show_title, turn_completed, turn_failed, turn_interrupted, \
         waiting_question, waiting_approval FROM notification_settings WHERE owner_id = $1",
    )
    .bind(owner)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .map(
        |(
            show_title,
            turn_completed,
            turn_failed,
            turn_interrupted,
            waiting_question,
            waiting_approval,
        )| Settings {
            show_title,
            turn_completed,
            turn_failed,
            turn_interrupted,
            waiting_question,
            waiting_approval,
        },
    )
    .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/push/subscriptions",
            post(create_subscription).get(list_subscriptions),
        )
        .route(
            "/push/subscriptions/{id}",
            put(update_subscription).delete(delete_subscription),
        )
        .route("/push/settings", get(get_settings).put(put_settings))
        .route("/push/vapid-public-key", get(get_vapid_public_key))
}

fn err(status: StatusCode, code: Code, message: &str) -> Response {
    api_error(
        status,
        code,
        message,
        &new_request_id(),
        serde_json::json!({}),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionBody {
    endpoint: String,
    keys: SubscriptionKeysBody,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionKeysBody {
    p256dh: String,
    auth: String,
}

async fn create_subscription(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<SubscriptionBody>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    if body.endpoint.is_empty() || body.keys.p256dh.is_empty() || body.keys.auth.is_empty() {
        return err(StatusCode::BAD_REQUEST, Code::InternalError, "订阅参数无效");
    }
    let row = sqlx::query_as::<_, (uuid::Uuid,)>(
        "INSERT INTO push_subscriptions (id, owner_id, endpoint, p256dh_key, auth_key) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4) \
         ON CONFLICT (owner_id, endpoint) DO UPDATE SET \
           p256dh_key = EXCLUDED.p256dh_key, auth_key = EXCLUDED.auth_key, invalidated_at = NULL \
         RETURNING id",
    )
    .bind(ident.owner_id)
    .bind(&body.endpoint)
    .bind(&body.keys.p256dh)
    .bind(&body.keys.auth)
    .fetch_one(&app.db)
    .await;
    match row {
        Ok((id,)) => (StatusCode::CREATED, Json(serde_json::json!({ "id": id }))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "insert push subscription failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "保存订阅失败",
            )
        }
    }
}

async fn list_subscriptions(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    match sqlx::query_as::<
        _,
        (
            uuid::Uuid,
            String,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        ),
    >(
        "SELECT id, endpoint, p256dh_key, auth_key, created_at, invalidated_at \
         FROM push_subscriptions WHERE owner_id = $1 ORDER BY created_at ASC",
    )
    .bind(ident.owner_id)
    .fetch_all(&app.db)
    .await
    {
        Ok(rows) => {
            let subscriptions: Vec<serde_json::Value> = rows
                .iter()
                .map(|(id, endpoint, p256dh, auth, created, invalidated)| {
                    serde_json::json!({
                        "id": id,
                        "endpoint": endpoint,
                        "keys": { "p256dh": p256dh, "auth": auth },
                        "createdAt": created.to_rfc3339(),
                        "invalidatedAt": invalidated.map(|t| t.to_rfc3339()),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({ "subscriptions": subscriptions })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "list push subscriptions failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "查询订阅失败",
            )
        }
    }
}

async fn update_subscription(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<SubscriptionBody>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let result = sqlx::query(
        "UPDATE push_subscriptions SET endpoint = $3, p256dh_key = $4, auth_key = $5, \
         invalidated_at = NULL WHERE id = $1 AND owner_id = $2",
    )
    .bind(id)
    .bind(ident.owner_id)
    .bind(&body.endpoint)
    .bind(&body.keys.p256dh)
    .bind(&body.keys.auth)
    .execute(&app.db)
    .await;
    match result {
        Ok(res) if res.rows_affected() > 0 => (StatusCode::NO_CONTENT,).into_response(),
        Ok(_) => err(StatusCode::NOT_FOUND, Code::SessionNotFound, "订阅不存在"),
        Err(e) => {
            tracing::error!(error = %e, "update push subscription failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "更新订阅失败",
            )
        }
    }
}

async fn delete_subscription(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let result = sqlx::query("DELETE FROM push_subscriptions WHERE id = $1 AND owner_id = $2")
        .bind(id)
        .bind(ident.owner_id)
        .execute(&app.db)
        .await;
    match result {
        Ok(res) if res.rows_affected() > 0 => (StatusCode::NO_CONTENT,).into_response(),
        Ok(_) => err(StatusCode::NOT_FOUND, Code::SessionNotFound, "订阅不存在"),
        Err(e) => {
            tracing::error!(error = %e, "delete push subscription failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "删除订阅失败",
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBody {
    #[serde(default)]
    show_title: Option<bool>,
    #[serde(default)]
    events: Option<EventsBody>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsBody {
    #[serde(default)]
    turn_completed: Option<bool>,
    #[serde(default)]
    turn_failed: Option<bool>,
    #[serde(default)]
    turn_interrupted: Option<bool>,
    #[serde(default)]
    waiting_question: Option<bool>,
    #[serde(default)]
    waiting_approval: Option<bool>,
}

async fn get_settings(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let settings = load_settings(&app.db, ident.owner_id).await;
    (StatusCode::OK, Json(settings.to_json())).into_response()
}

/// VAPID application server key(只读;未配置时 publicKey 为 null,前端据此隐藏订阅入口)。
async fn get_vapid_public_key(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = require_identity(&app, addr, &headers) {
        return resp;
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "enabled": app.push.enabled,
            "publicKey": app.push.public_key,
        })),
    )
        .into_response()
}

async fn put_settings(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<SettingsBody>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let current = load_settings(&app.db, ident.owner_id).await;
    let events = body.events.unwrap_or(EventsBody {
        turn_completed: None,
        turn_failed: None,
        turn_interrupted: None,
        waiting_question: None,
        waiting_approval: None,
    });
    let show_title = body.show_title.unwrap_or(current.show_title);
    let turn_completed = events.turn_completed.unwrap_or(current.turn_completed);
    let turn_failed = events.turn_failed.unwrap_or(current.turn_failed);
    let turn_interrupted = events.turn_interrupted.unwrap_or(current.turn_interrupted);
    let waiting_question = events.waiting_question.unwrap_or(current.waiting_question);
    let waiting_approval = events.waiting_approval.unwrap_or(current.waiting_approval);
    let result = sqlx::query(
        "INSERT INTO notification_settings (owner_id, show_title, turn_completed, turn_failed, \
         turn_interrupted, waiting_question, waiting_approval, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, now()) \
         ON CONFLICT (owner_id) DO UPDATE SET \
           show_title = EXCLUDED.show_title, turn_completed = EXCLUDED.turn_completed, \
           turn_failed = EXCLUDED.turn_failed, turn_interrupted = EXCLUDED.turn_interrupted, \
           waiting_question = EXCLUDED.waiting_question, waiting_approval = EXCLUDED.waiting_approval, \
           updated_at = now()",
    )
    .bind(ident.owner_id)
    .bind(show_title)
    .bind(turn_completed)
    .bind(turn_failed)
    .bind(turn_interrupted)
    .bind(waiting_question)
    .bind(waiting_approval)
    .execute(&app.db)
    .await;
    match result {
        Ok(_) => {
            let settings = Settings {
                show_title,
                turn_completed,
                turn_failed,
                turn_interrupted,
                waiting_question,
                waiting_approval,
            };
            (StatusCode::OK, Json(settings.to_json())).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "update notification settings failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "更新通知设置失败",
            )
        }
    }
}
