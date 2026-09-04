//! 审计事件(§18.7/§25/§27.2):
//! - `record`/`insert`:命令、配对、设备管理等写路径的最小落库(仅元数据);
//! - `GET /agent-console/api/audit`:分页(keyset 稳定 cursor,默认 50、最大 200)。
//!
//! 隐私边界(§18.7/§25.2):不保存标题、项目路径、prompt、回答、审批正文、
//! 输出或文件名;日志只输出白名单字段(§25.3)。30 天保留由 main.rs
//! 维护任务清理,此处不重复。

use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    auth::require_identity,
    state::{api_error, limits, new_request_id, AppState, AuditEvent, StableErrorCode as Code},
};

/// 审计事件落库(仅元数据,§18.7)。失败只记日志,不阻塞主流程。
pub async fn insert(db: &sqlx::PgPool, event: &AuditEvent) {
    let result = sqlx::query(
        "INSERT INTO audit_events (owner_id, device_id, session_id, request_id, operation, result, latency_ms) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(event.owner_id)
    .bind(event.device_id)
    .bind(event.session_id)
    .bind(event.request_id.as_deref())
    .bind(&event.operation)
    .bind(&event.result)
    .bind(event.latency_ms)
    .execute(db)
    .await;
    if let Err(e) = result {
        tracing::warn!(target: "relay::audit", error = %e, "audit insert failed");
    }
}

/// 供 state::record_audit 调用的落库入口(保持调用点签名不变)。
pub async fn record(db: &sqlx::PgPool, event: AuditEvent) {
    insert(db, &event).await;
}

// ---------------------------------------------------------------------------
// 查询 API
// ---------------------------------------------------------------------------

pub fn routes() -> Router<AppState> {
    Router::new().route("/audit", get(list_audit))
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
struct AuditQuery {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

async fn list_audit(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<AuditQuery>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let limit = q
        .limit
        .unwrap_or(limits::PAGE_DEFAULT)
        .clamp(1, limits::PAGE_MAX);
    // 稳定 keyset cursor:base64("created_at_rfc3339|id"),按 (created_at, id) 倒序。
    let cursor = match q.cursor.as_deref().map(decode_cursor).transpose() {
        Ok(c) => c,
        Err(()) => {
            return err(StatusCode::BAD_REQUEST, Code::InternalError, "cursor 无效");
        }
    };
    let rows = if let Some((before_created, before_id)) = cursor {
        sqlx::query_as::<_, (i64, Option<uuid::Uuid>, Option<uuid::Uuid>, Option<String>, String, String, Option<i64>, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, device_id, session_id, request_id, operation, result, latency_ms, created_at \
             FROM audit_events WHERE owner_id = $1 AND (created_at, id) < ($2, $3) \
             ORDER BY created_at DESC, id DESC LIMIT $4",
        )
        .bind(ident.owner_id)
        .bind(before_created)
        .bind(before_id)
        .bind(limit + 1)
        .fetch_all(&app.db)
        .await
    } else {
        sqlx::query_as::<_, (i64, Option<uuid::Uuid>, Option<uuid::Uuid>, Option<String>, String, String, Option<i64>, chrono::DateTime<chrono::Utc>)>(
            "SELECT id, device_id, session_id, request_id, operation, result, latency_ms, created_at \
             FROM audit_events WHERE owner_id = $1 \
             ORDER BY created_at DESC, id DESC LIMIT $2",
        )
        .bind(ident.owner_id)
        .bind(limit + 1)
        .fetch_all(&app.db)
        .await
    };
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "list audit events failed");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::InternalError,
                "查询审计事件失败",
            );
        }
    };
    let mut next_cursor = None;
    if rows.len() as i64 > limit {
        rows.pop();
        if let Some(last) = rows.last() {
            next_cursor = Some(encode_cursor(last.7, last.0));
        }
    }
    let events: Vec<serde_json::Value> = rows
        .iter()
        .map(
            |(id, device_id, session_id, request_id, operation, result, latency_ms, created_at)| {
                json!({
                    "id": id,
                    "deviceId": device_id,
                    "sessionId": session_id,
                    "requestId": request_id,
                    "operation": operation,
                    "result": result,
                    "latencyMs": latency_ms,
                    "createdAt": created_at.to_rfc3339(),
                })
            },
        )
        .collect();
    (
        StatusCode::OK,
        Json(json!({ "events": events, "nextCursor": next_cursor })),
    )
        .into_response()
}

fn encode_cursor(created_at: chrono::DateTime<chrono::Utc>, id: i64) -> String {
    use base64::Engine;
    let raw = format!("{}|{id}", created_at.to_rfc3339());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

fn decode_cursor(cursor: &str) -> Result<(chrono::DateTime<chrono::Utc>, i64), ()> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ())?;
    let raw = String::from_utf8(raw).map_err(|_| ())?;
    let (ts, id) = raw.rsplit_once('|').ok_or(())?;
    let created_at = chrono::DateTime::parse_from_rfc3339(ts)
        .map_err(|_| ())?
        .with_timezone(&chrono::Utc);
    let id: i64 = id.parse().map_err(|_| ())?;
    Ok((created_at, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_cursor_roundtrip() {
        let created = chrono::Utc::now();
        let encoded = encode_cursor(created, 42);
        let (ts, id) = decode_cursor(&encoded).unwrap();
        assert_eq!(id, 42);
        assert_eq!(ts, created);
        assert!(decode_cursor("not-a-cursor").is_err());
    }
}
