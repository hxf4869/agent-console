//! 设备面:设备管理 API(§27.2)、Bridge WSS 端点与凭据认证(§21.9)、pairing(§21)。

pub mod bridge_ws;
pub mod pairing;

use axum::{
    extract::{ConnectInfo, Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Json, Router,
};
use serde::Deserialize;
use sqlx::Row;

use crate::{
    auth::require_identity,
    state::{
        api_error, new_request_id, record_audit, status_for_code, AppState, AuditEvent,
        StableErrorCode,
    },
};

/// 设备管理 + 配对路由(挂在 /agent-console/api 下,经浏览器身份认证;§27.2)。
/// 配对发起方向为 Bridge-first(§21):浏览器只 lookup/approve/查询/取消。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/devices", get(list_devices))
        .route("/devices/{id}", patch(patch_device).delete(revoke_device))
        .route("/pairing/lookup", post(pairing::lookup))
        .route("/pairing/approve", post(pairing::approve_challenge))
        .route("/pairing/challenges", get(list_challenges))
        .route("/pairing/challenges/{id}", delete(cancel_challenge))
}

#[derive(Debug, sqlx::FromRow)]
pub struct DeviceRow {
    pub id: uuid::Uuid,
    pub owner_id: uuid::Uuid,
    pub display_name: String,
    pub platform: String,
    pub arch: String,
    pub bridge_version: String,
    pub paired_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    pub compatibility_summary: String,
    pub control_summary: String,
    pub privacy_hide_titles: bool,
}

async fn list_devices(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let rows = match sqlx::query_as::<_, DeviceRow>(
        "SELECT id, owner_id, display_name, platform, arch, bridge_version, paired_at, \
         revoked_at, last_seen_at, compatibility_summary, control_summary, privacy_hide_titles \
         FROM devices WHERE owner_id = $1 ORDER BY paired_at ASC",
    )
    .bind(ident.owner_id)
    .fetch_all(&app.db)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "list devices failed");
            return api_error(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "查询设备失败",
                &new_request_id(),
                serde_json::json!({}),
            );
        }
    };
    let devices: Vec<serde_json::Value> = rows
        .iter()
        .map(|d| {
            serde_json::json!({
                "id": d.id,
                "displayName": d.display_name,
                "platform": d.platform,
                "arch": d.arch,
                "bridgeVersion": d.bridge_version,
                "connection": if app.hub.is_online(d.id) { "CONNECTION_ONLINE" } else { "CONNECTION_OFFLINE" },
                "lastSeenAt": d.last_seen_at.map(|t| t.to_rfc3339()),
                "pairedAt": d.paired_at.to_rfc3339(),
                "revoked": d.revoked_at.is_some(),
                "compatibilityState": d.compatibility_summary,
                "controlMode": d.control_summary,
                "privacyHideTitles": d.privacy_hide_titles,
            })
        })
        .collect();
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({ "devices": devices })),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchDeviceBody {
    display_name: Option<String>,
    privacy_hide_titles: Option<bool>,
}

async fn patch_device(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<PatchDeviceBody>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let request_id = new_request_id();
    let display_name = body
        .display_name
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .map(str::to_string);
    let result = sqlx::query(
        "UPDATE devices SET \
           display_name = COALESCE($3, display_name), \
           privacy_hide_titles = COALESCE($4, privacy_hide_titles) \
         WHERE id = $1 AND owner_id = $2 AND revoked_at IS NULL \
         RETURNING display_name, privacy_hide_titles",
    )
    .bind(id)
    .bind(ident.owner_id)
    .bind(display_name)
    .bind(body.privacy_hide_titles)
    .fetch_optional(&app.db)
    .await;
    match result {
        Ok(Some(row)) => {
            record_audit(
                &app.db,
                AuditEvent {
                    owner_id: ident.owner_id,
                    device_id: Some(id),
                    session_id: None,
                    request_id: Some(request_id.clone()),
                    operation: "device_rename".into(),
                    result: "OK".into(),
                    latency_ms: None,
                },
            )
            .await;
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({
                    "id": id,
                    "displayName": row.get::<String, _>(0),
                    "privacyHideTitles": row.get::<bool, _>(1),
                })),
            )
                .into_response()
        }
        Ok(None) => api_error(
            axum::http::StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "设备不存在",
            &request_id,
            serde_json::json!({}),
        ),
        Err(e) => {
            tracing::error!(error = %e, "patch device failed");
            api_error(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "更新设备失败",
                &request_id,
                serde_json::json!({}),
            )
        }
    }
}

/// 撤销设备:立即关闭其 Bridge socket(§21.9),后续凭据认证失败。
async fn revoke_device(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let request_id = new_request_id();
    let result = sqlx::query(
        "UPDATE devices SET revoked_at = now() WHERE id = $1 AND owner_id = $2 AND revoked_at IS NULL RETURNING id",
    )
    .bind(id)
    .bind(ident.owner_id)
    .fetch_optional(&app.db)
    .await;
    match result {
        Ok(Some(_)) => {
            // 立即断开该设备的 Bridge 连接(§21.9)。
            app.hub.force_close_bridge(id);
            record_audit(
                &app.db,
                AuditEvent {
                    owner_id: ident.owner_id,
                    device_id: Some(id),
                    session_id: None,
                    request_id: Some(request_id.clone()),
                    operation: "device_revoke".into(),
                    result: "OK".into(),
                    latency_ms: None,
                },
            )
            .await;
            (axum::http::StatusCode::NO_CONTENT,).into_response()
        }
        Ok(None) => api_error(
            axum::http::StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "设备不存在",
            &request_id,
            serde_json::json!({}),
        ),
        Err(e) => {
            tracing::error!(error = %e, "revoke device failed");
            api_error(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "撤销设备失败",
                &request_id,
                serde_json::json!({}),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// pairing HTTP API(浏览器身份,§21/§27.2)
// lookup/approve 处理器在 devices::pairing(需身份头 + 来源限速 + 单赢家批准)。
// ---------------------------------------------------------------------------

async fn list_challenges(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    // 本 owner 待处理挑战(§21 Bridge-first:owner 在批准时回填,
    // 待处理 = 已批准但 Bridge 尚未 claim 领取凭据)。
    let rows = sqlx::query(
        "SELECT id, device_name, platform, arch, bridge_version, expires_at, approved_at \
         FROM pairing_challenges \
         WHERE owner_id = $1 AND approved_at IS NOT NULL AND consumed_at IS NULL \
           AND credential_delivered_at IS NULL AND expires_at > now() \
         ORDER BY created_at DESC LIMIT 20",
    )
    .bind(ident.owner_id)
    .fetch_all(&app.db)
    .await;
    match rows {
        Ok(rows) => {
            let challenges: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "challengeId": r.get::<uuid::Uuid, _>(0),
                        "deviceName": r.get::<Option<String>, _>(1),
                        "platform": r.get::<Option<String>, _>(2),
                        "arch": r.get::<Option<String>, _>(3),
                        "bridgeVersion": r.get::<Option<String>, _>(4),
                        "expiresAt": r.get::<chrono::DateTime<chrono::Utc>, _>(5).to_rfc3339(),
                        "approvedAt": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>(6).map(|t| t.to_rfc3339()),
                    })
                })
                .collect();
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({ "challenges": challenges })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "list pairing challenges failed");
            api_error(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "查询配对请求失败",
                &new_request_id(),
                serde_json::json!({}),
            )
        }
    }
}

async fn cancel_challenge(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let result = sqlx::query(
        "UPDATE pairing_challenges SET consumed_at = now() \
         WHERE id = $1 AND owner_id = $2 AND consumed_at IS NULL RETURNING id",
    )
    .bind(id)
    .bind(ident.owner_id)
    .fetch_optional(&app.db)
    .await;
    match result {
        Ok(Some(_)) => (axum::http::StatusCode::NO_CONTENT,).into_response(),
        Ok(None) => api_error(
            status_for_code(&StableErrorCode::SessionNotFound),
            StableErrorCode::SessionNotFound,
            "配对请求不存在",
            &new_request_id(),
            serde_json::json!({}),
        ),
        Err(e) => {
            tracing::error!(error = %e, "cancel pairing challenge failed");
            api_error(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "取消配对失败",
                &new_request_id(),
                serde_json::json!({}),
            )
        }
    }
}

/// Bridge CapabilitySnapshot → devices 表兼容/控制摘要(§18.1)。
pub async fn update_device_capability_summary(
    db: &sqlx::PgPool,
    device: uuid::Uuid,
    cap: &agent_console_protocol::v1::CapabilitySnapshot,
) -> anyhow::Result<()> {
    let compat = agent_console_protocol::v1::CompatibilityState::try_from(cap.compatibility_state)
        .map(|e| e.as_str_name().to_string())
        .unwrap_or_else(|_| format!("COMPATIBILITY_STATE_{}", cap.compatibility_state));
    let control = agent_console_protocol::v1::ControlMode::try_from(cap.control_mode)
        .map(|e| e.as_str_name().to_string())
        .unwrap_or_else(|_| format!("CONTROL_MODE_{}", cap.control_mode));
    sqlx::query(
        "UPDATE devices SET compatibility_summary = $2, control_summary = $3 WHERE id = $1",
    )
    .bind(device)
    .bind(compat)
    .bind(control)
    .execute(db)
    .await?;
    Ok(())
}
