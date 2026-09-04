//! 浏览器认证(§20):dev-toolbox 内部接口客户端、HTTP 身份头信任、
//! 浏览器 WSS(ticket 认证 + Hello 握手)与定期 introspection。
//!
//! 日志约束(§25.3):不记录 Cookie、Authorization、WS subprotocol ticket、CSRF;
//! 连接日志只输出固定协议名与稳定错误码。

pub mod browser_ws;

use std::{net::SocketAddr, time::Duration};

use axum::{http::HeaderMap, response::Response};
use base64::Engine;
use serde::Deserialize;

use crate::state::{
    api_error, is_trusted_proxy, limits, new_request_id, AppState, StableErrorCode,
};

// ---------------------------------------------------------------------------
// dev-toolbox 内部接口客户端
// ---------------------------------------------------------------------------

/// ticket 消费拒绝原因(dev-toolbox 稳定原因)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketReason {
    Consumed,
    Expired,
    NotFound,
    SessionInvalid,
}

impl TicketReason {
    pub fn stable_code(&self) -> StableErrorCode {
        match self {
            TicketReason::Consumed => StableErrorCode::WsTicketConsumed,
            TicketReason::Expired => StableErrorCode::WsTicketExpired,
            TicketReason::NotFound | TicketReason::SessionInvalid => {
                StableErrorCode::WsTicketInvalid
            }
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "CONSUMED" => Some(TicketReason::Consumed),
            "EXPIRED" => Some(TicketReason::Expired),
            "NOT_FOUND" => Some(TicketReason::NotFound),
            "SESSION_INVALID" => Some(TicketReason::SessionInvalid),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumeOk {
    pub auth_session_id: uuid::Uuid,
    pub owner_id: uuid::Uuid,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

pub enum ConsumeOutcome {
    Valid(ConsumeOk),
    Rejected(TicketReason),
    /// dev-toolbox 不可达(§20.5 宽限策略)。
    Unavailable,
}

pub enum IntrospectOutcome {
    Valid {
        expires_at: chrono::DateTime<chrono::Utc>,
    },
    Invalid,
    Unavailable,
}

#[derive(Deserialize)]
struct ConsumeResp {
    #[serde(default)]
    valid: bool,
    #[serde(default)]
    #[serde(rename = "authSessionId")]
    auth_session_id: Option<String>,
    #[serde(default)]
    #[serde(rename = "ownerId")]
    owner_id: Option<String>,
    #[serde(default)]
    #[serde(rename = "expiresAt")]
    expires_at: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct IntrospectResp {
    #[serde(default)]
    valid: bool,
    #[serde(default)]
    #[serde(rename = "expiresAt")]
    expires_at: Option<String>,
}

fn parse_expires(raw: &Option<String>) -> Option<chrono::DateTime<chrono::Utc>> {
    raw.as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
}

/// dev-toolbox 内部接口客户端(§20.3)。
#[derive(Clone)]
pub struct ToolboxClient {
    http: reqwest::Client,
    base_url: Option<String>,
    token: Option<String>,
}

impl ToolboxClient {
    pub fn new(base_url: Option<String>, token: Option<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            base_url,
            token,
        }
    }

    fn configured(&self) -> bool {
        self.base_url.is_some() && self.token.is_some()
    }

    /// 原子消费一次性 WS ticket(§20.2)。
    pub async fn consume_ticket(&self, ticket: &str) -> ConsumeOutcome {
        if !self.configured() {
            return ConsumeOutcome::Unavailable;
        }
        let url = format!(
            "{}/internal/agent-console/ws-tickets/consume",
            self.base_url.as_ref().unwrap().trim_end_matches('/')
        );
        let body = serde_json::json!({ "ticket": ticket });
        let request = self
            .http
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""));
        match request.json(&body).send().await {
            Ok(resp) => match resp.json::<ConsumeResp>().await {
                Ok(parsed) if parsed.valid => {
                    match (
                        parsed
                            .auth_session_id
                            .as_deref()
                            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
                        parsed
                            .owner_id
                            .as_deref()
                            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
                        parse_expires(&parsed.expires_at),
                    ) {
                        (Some(a), Some(o), Some(e)) => ConsumeOutcome::Valid(ConsumeOk {
                            auth_session_id: a,
                            owner_id: o,
                            expires_at: e,
                        }),
                        _ => ConsumeOutcome::Unavailable,
                    }
                }
                Ok(parsed) => {
                    let reason = parsed
                        .reason
                        .as_deref()
                        .and_then(TicketReason::from_str)
                        .unwrap_or(TicketReason::NotFound);
                    ConsumeOutcome::Rejected(reason)
                }
                Err(_) => ConsumeOutcome::Unavailable,
            },
            Err(_) => ConsumeOutcome::Unavailable,
        }
    }

    /// 无 touch introspection(§20.3:不得更新 last_seen)。
    pub async fn introspect(&self, auth_session_id: uuid::Uuid) -> IntrospectOutcome {
        if !self.configured() {
            return IntrospectOutcome::Unavailable;
        }
        let url = format!(
            "{}/internal/agent-console/auth-sessions/introspect",
            self.base_url.as_ref().unwrap().trim_end_matches('/')
        );
        let body = serde_json::json!({ "authSessionId": auth_session_id.to_string() });
        let request = self
            .http
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""));
        match request.json(&body).send().await {
            Ok(resp) => match resp.json::<IntrospectResp>().await {
                Ok(parsed) if parsed.valid => match parse_expires(&parsed.expires_at) {
                    Some(expires_at) => IntrospectOutcome::Valid { expires_at },
                    None => IntrospectOutcome::Unavailable,
                },
                Ok(_) => IntrospectOutcome::Invalid,
                Err(_) => IntrospectOutcome::Unavailable,
            },
            Err(_) => IntrospectOutcome::Unavailable,
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP 身份(§20.4:只信任网关网络来源的身份头)
// ---------------------------------------------------------------------------

/// 浏览器 HTTP 请求身份(来自可信代理重建的身份头)。
#[derive(Debug, Clone)]
pub struct BrowserHttpIdentity {
    pub auth_session_id: uuid::Uuid,
    pub owner_id: uuid::Uuid,
    /// 网关可选传递的会话到期时间;dev-toolbox verify 当前只注入
    /// Session-Id/Owner-Id,缺失时跳过本地预过期检查——网关 forward-auth
    /// 已拒绝过期会话,长连接撤销由 WS introspection 循环负责(§20.5)。
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

const HDR_SESSION: &str = "x-agent-console-session-id";
const HDR_OWNER: &str = "x-agent-console-owner-id";
const HDR_EXPIRES: &str = "x-agent-console-session-expires";

/// 从请求提取浏览器身份。
///
/// 只信任 `TRUSTED_PROXY_CIDRS` 来源携带的 X-Agent-Console-Session-Id /
/// X-Agent-Console-Owner-Id / X-Agent-Console-Session-Expires;非可信来源
/// 携带这些头一律剥离视为未认证(§20.4)。Cookie 不由 Relay 校验
/// (网关 forward-auth 已完成;Relay 不重复校验,也不接触 Cookie)。
pub fn browser_http_identity(
    app: &AppState,
    addr: SocketAddr,
    headers: &HeaderMap,
) -> Result<BrowserHttpIdentity, Response> {
    let request_id = new_request_id();
    if !is_trusted_proxy(&app.config, addr) {
        // 不可信来源:身份头视为剥离,一律未认证。
        return Err(api_error(
            axum::http::StatusCode::UNAUTHORIZED,
            StableErrorCode::AuthRequired,
            "缺少有效的网关身份",
            &request_id,
            serde_json::json!({}),
        ));
    }
    let parse_uuid = |v: Option<&str>| v.and_then(|s| uuid::Uuid::parse_str(s).ok());
    let session = parse_uuid(headers.get(HDR_SESSION).and_then(|v| v.to_str().ok()));
    let owner = parse_uuid(headers.get(HDR_OWNER).and_then(|v| v.to_str().ok()));
    let expires = headers
        .get(HDR_EXPIRES)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc));
    let (Some(auth_session_id), Some(owner_id)) = (session, owner) else {
        return Err(api_error(
            axum::http::StatusCode::UNAUTHORIZED,
            StableErrorCode::AuthRequired,
            "请先登录",
            &request_id,
            serde_json::json!({}),
        ));
    };
    if let Some(expires_at) = expires {
        if expires_at <= chrono::Utc::now() {
            return Err(api_error(
                axum::http::StatusCode::UNAUTHORIZED,
                StableErrorCode::AuthExpired,
                "登录已过期",
                &request_id,
                serde_json::json!({}),
            ));
        }
    }
    Ok(BrowserHttpIdentity {
        auth_session_id,
        owner_id,
        expires_at: expires,
    })
}

/// handler 内联使用的身份提取宏式辅助(返回 Result 以便 `?`)。
pub fn require_identity(
    app: &AppState,
    addr: SocketAddr,
    headers: &HeaderMap,
) -> Result<BrowserHttpIdentity, Response> {
    browser_http_identity(app, addr, headers)
}

// ---------------------------------------------------------------------------
// 定期 introspection(§20.5)
// ---------------------------------------------------------------------------

/// 每 60s introspect 所有活跃 browser 连接的 auth session:
/// - 明确 invalid/revoked/expired → 立即以稳定 close reason 关闭;
/// - 达到原 expires_at → 本地即关;
/// - dev-toolbox 不可达 → 停止接受写命令,宽限内只保留已有只读订阅,
///   恢复后重校验,宽限尽关闭。
pub async fn introspection_loop(app: AppState) {
    let mut ticker =
        tokio::time::interval(app.config.introspect_interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // 首个 tick 立即返回,跳过。
    loop {
        ticker.tick().await;
        let sessions = app.hub.browser_sessions();
        let mut unreachable = false;
        let mut to_close: Vec<(uuid::Uuid, &'static str)> = Vec::new();
        let mut valid_seen: std::collections::HashSet<uuid::Uuid> =
            std::collections::HashSet::new();
        for (conn_id, session_id, expires_at) in &sessions {
            // 本地过期:无需远程查询也关闭(§20.5)。
            if *expires_at <= chrono::Utc::now() {
                to_close.push((*conn_id, "AUTH_EXPIRED"));
                continue;
            }
            if !valid_seen.insert(*session_id) {
                continue; // 同一 auth session 只 introspect 一次。
            }
            match app.toolbox.introspect(*session_id).await {
                IntrospectOutcome::Valid { .. } => {}
                IntrospectOutcome::Invalid => {
                    // revoked/expired/idle/password changed:立即关闭(稳定 close reason)。
                    for (c, ..) in sessions.iter().filter(|(_, sid, _)| *sid == *session_id) {
                        to_close.push((*c, stable_close_for_invalid()));
                    }
                }
                IntrospectOutcome::Unavailable => unreachable = true,
            }
        }
        for (conn_id, reason) in to_close {
            app.hub.close_browser(conn_id, reason);
        }
        if unreachable {
            match app.hub.degraded() {
                None => {
                    app.hub
                        .set_degraded(Some(std::time::Instant::now() + app.config.auth_grace));
                    tracing::warn!(target: "relay::auth", code = "INTERNAL_ERROR", "auth backend unreachable, degraded grace started");
                }
                Some(until) => {
                    if std::time::Instant::now() >= until {
                        // 宽限结束:关闭全部浏览器连接(§20.5)。
                        tracing::warn!(target: "relay::auth", code = "AUTH_EXPIRED", "auth grace exhausted, closing browser connections");
                        app.hub.close_all_browsers("AUTH_EXPIRED");
                        app.hub.set_degraded(None);
                    }
                }
            }
        } else if app.hub.degraded().is_some() {
            // 恢复:清除降级;下个周期重新全量校验(§20.5)。
            app.hub.set_degraded(None);
            tracing::info!(target: "relay::auth", "auth backend recovered");
        }
    }
}

fn stable_close_for_invalid() -> &'static str {
    "AUTH_EXPIRED"
}

/// 供 WS 关闭原因使用的稳定码名(避免在热路径重复映射)。
pub fn close_reason_of(code: StableErrorCode) -> &'static str {
    crate::state::stable_code_ref_name(&code)
}

/// base64url 解码 ticket(subprotocol 携带,无填充)。
pub fn decode_ticket(raw: &str) -> Option<String> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .or_else(|| Some(raw.to_string()))
}

/// 从 Sec-WebSocket-Protocol 提取 (固定协议名确认, ticket)。
/// 期望客户端同时提供 `agent-console.v1` 与 `agent-console.ticket-<base64url>`。
pub fn parse_ws_subprotocols(headers: &HeaderMap) -> Result<String, Response> {
    let request_id = new_request_id();
    let offered = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mut has_fixed = false;
    let mut ticket: Option<String> = None;
    for token in offered.split(',').map(str::trim) {
        if token == limits::WS_SUBPROTOCOL {
            has_fixed = true;
        } else if let Some(rest) = token.strip_prefix(limits::WS_TICKET_PROTOCOL_PREFIX) {
            ticket = Some(rest.to_string());
        }
    }
    match (has_fixed, ticket) {
        (true, Some(t)) => Ok(t),
        _ => Err(api_error(
            axum::http::StatusCode::UNAUTHORIZED,
            StableErrorCode::WsTicketInvalid,
            "缺少有效的 WebSocket ticket",
            &request_id,
            serde_json::json!({}),
        )),
    }
}

/// ticket 拒绝 → HTTP 响应(升级前返回,稳定错误码)。
pub fn ticket_reject_response(reason: TicketReason) -> Response {
    let code = reason.stable_code();
    api_error(
        crate::state::status_for_code(&code),
        code,
        "WebSocket ticket 无效",
        &new_request_id(),
        serde_json::json!({}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_parse_requires_fixed_name_and_ticket() {
        let mut headers = HeaderMap::new();
        // 缺失 → 拒绝。
        assert!(parse_ws_subprotocols(&headers).is_err());
        // 只有固定名 → 拒绝。
        headers.insert(
            "sec-websocket-protocol",
            "agent-console.v1".parse().unwrap(),
        );
        assert!(parse_ws_subprotocols(&headers).is_err());
        // 固定名 + ticket → 通过。
        headers.insert(
            "sec-websocket-protocol",
            "agent-console.v1, agent-console.ticket-cGlvdA"
                .parse()
                .unwrap(),
        );
        let ticket = parse_ws_subprotocols(&headers).unwrap();
        assert_eq!(ticket, "cGlvdA");
    }

    #[test]
    fn ticket_reason_maps_to_stable_codes() {
        assert_eq!(
            TicketReason::Consumed.stable_code(),
            StableErrorCode::WsTicketConsumed
        );
        assert_eq!(
            TicketReason::Expired.stable_code(),
            StableErrorCode::WsTicketExpired
        );
        assert_eq!(
            TicketReason::NotFound.stable_code(),
            StableErrorCode::WsTicketInvalid
        );
        assert_eq!(
            TicketReason::SessionInvalid.stable_code(),
            StableErrorCode::WsTicketInvalid
        );
        assert_eq!(
            TicketReason::from_str("CONSUMED"),
            Some(TicketReason::Consumed)
        );
    }
}
