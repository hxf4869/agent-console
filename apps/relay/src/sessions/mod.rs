//! 会话面 HTTP API(§27.2/§27.3/§27.5):
//! - GET  /sessions            分页列表(默认 50、最大 200、稳定 keyset cursor)
//! - PATCH /sessions/{id}      pin/mute/archive(Relay 自身偏好,§12)
//! - GET  /sessions/{id}/runtime   在线 Bridge QueryRequest → RuntimeSnapshot
//! - GET  /sessions/{id}/history   在线 Bridge QueryRequest → HistoryPage
//! - GET  /sessions/{id}/output    完整 command output 分页
//! - GET  /sessions/{id}/git       Git 只读摘要(§23.1)
//! - GET  /sessions/{id}/git/diff  单文件 staged/unstaged Diff(§23.1)
//! - GET  /sessions/{id}/files/metadata  按 file_handle 查询文件元数据(§22.2)
//! - GET  /sessions/{id}/requests  request receipts 查询
//! - GET  /requests/{requestId}    单条回执查询
//!
//! 设备离线时返回稳定 DEVICE_OFFLINE,不返回服务器上的陈旧正文(§11)。

pub mod json;
pub mod store;

use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::{get, patch},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use agent_console_protocol::v1::{
    query_request, query_response, CommandOutputPageQuery, FileMetadataQuery, GitFileDiffQuery,
    GitSummaryQuery, HistoryPageQuery, QueryRequest, RuntimeSnapshotQuery, SessionKey,
};

use crate::{
    auth::require_identity,
    state::{
        api_error, limits, new_request_id, record_audit, status_for_code, AppState, AuditEvent,
        StableErrorCode as Code,
    },
};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sessions", get(list_sessions))
        .route("/sessions/{id}", patch(patch_session))
        .route("/sessions/{id}/runtime", get(session_runtime))
        .route("/sessions/{id}/history", get(session_history))
        .route("/sessions/{id}/output", get(session_output))
        .route("/sessions/{id}/git", get(session_git))
        .route("/sessions/{id}/git/diff", get(session_git_diff))
        .route("/sessions/{id}/files/metadata", get(session_file_metadata))
        .route("/sessions/{id}/requests", get(session_requests))
        .route("/requests/{request_id}", get(get_request))
}

fn err(code: Code, message: &str) -> Response {
    let status = status_for_code(&code);
    api_error(status, code, message, &new_request_id(), json!({}))
}

/// 校验会话归属(owner 的未撤销设备)。
async fn owned_session(
    app: &AppState,
    owner: uuid::Uuid,
    id: uuid::Uuid,
) -> Result<store::SessionRow, Response> {
    let row = store::get_session(&app.db, id)
        .await
        .map_err(|_| err(Code::InternalError, "查询会话失败"))?;
    let Some(row) = row else {
        return Err(err(Code::SessionNotFound, "会话不存在"));
    };
    let owned = sqlx::query_as::<_, (uuid::Uuid,)>(
        "SELECT owner_id FROM devices WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(row.device_id)
    .fetch_optional(&app.db)
    .await
    .ok()
    .flatten();
    match owned {
        Some((o,)) if o == owner => Ok(row),
        _ => Err(err(Code::SessionNotFound, "会话不存在")),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    archived: Option<bool>,
}

fn session_row_json(row: &store::SessionRow) -> serde_json::Value {
    json!({
        "id": row.id,
        "deviceId": row.device_id,
        "agentKind": row.agent_kind,
        "nativeSessionId": row.native_session_id,
        "title": row.title,
        "projectDisplayName": row.project_display_name,
        "currentBranch": row.current_branch,
        "deviceConnection": row.device_connection,
        "deviceLastSeenAt": row.device_last_seen_at.map(|t| t.to_rfc3339()),
        "degradedReason": row.degraded_reason,
        "controlMode": row.control_mode,
        "compatibilityState": row.compatibility_state,
        "activeTurnPhase": row.active_turn_phase,
        "pendingAttentionCount": row.pending_attention_count,
        "pendingAttentionKinds": row.pending_attention_kinds,
        "queueState": row.queue_state,
        "lastTurnOutcome": row.last_turn_outcome,
        "lastUpdatedAt": row.last_updated_at.to_rfc3339(),
        "pinned": row.pinned,
        "muted": row.muted,
        "archived": row.archived,
    })
}

async fn list_sessions(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let cursor = q.cursor.as_deref().and_then(store::decode_cursor);
    if q.cursor.is_some() && cursor.is_none() {
        return err(Code::InternalError, "cursor 无效");
    }
    let params = store::ListParams {
        limit: q.limit.unwrap_or(limits::PAGE_DEFAULT),
        archived: q.archived,
        before_updated_at: cursor.as_ref().map(|(t, _)| *t),
        before_id: cursor.as_ref().map(|(_, id)| *id),
    };
    let mut rows = match store::list_sessions(&app.db, ident.owner_id, &params).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "list sessions failed");
            return err(Code::InternalError, "查询会话失败");
        }
    };
    let mut next_cursor = None;
    if rows.len() as i64 > params.limit.clamp(1, limits::PAGE_MAX) {
        rows.pop();
        if let Some(last) = rows.last() {
            next_cursor = Some(store::encode_cursor(last));
        }
    }
    let sessions: Vec<serde_json::Value> = rows.iter().map(session_row_json).collect();
    (
        axum::http::StatusCode::OK,
        Json(json!({
            "sessions": sessions,
            "nextCursor": next_cursor,
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchSessionBody {
    pinned: Option<bool>,
    muted: Option<bool>,
    archived: Option<bool>,
}

async fn patch_session(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<PatchSessionBody>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match store::patch_prefs(&app.db, row.id, body.pinned, body.muted, body.archived).await {
        Ok(true) => {
            record_audit(
                &app.db,
                AuditEvent {
                    owner_id: ident.owner_id,
                    device_id: Some(row.device_id),
                    session_id: Some(row.id),
                    request_id: Some(new_request_id()),
                    operation: "session_prefs".into(),
                    result: "OK".into(),
                    latency_ms: None,
                },
            )
            .await;
            (axum::http::StatusCode::NO_CONTENT,).into_response()
        }
        Ok(false) => err(Code::SessionNotFound, "会话不存在"),
        Err(e) => {
            tracing::error!(error = %e, "patch session prefs failed");
            err(Code::InternalError, "更新会话失败")
        }
    }
}

fn query_session_key(row: &store::SessionRow) -> SessionKey {
    SessionKey {
        device_id: row.device_id.to_string(),
        agent_kind: agent_console_protocol::v1::AgentKind::CodexDesktop as i32,
        native_session_id: row.native_session_id.clone(),
        relay_session_uuid: row.id.to_string(),
    }
}

fn query_response_json(resp: &agent_console_protocol::v1::QueryResponse) -> Response {
    use query_response::Result as R;
    if resp.error_code != 0 {
        let code = Code::try_from(resp.error_code).unwrap_or(Code::InternalError);
        return err(code, "查询失败");
    }
    match resp.result.as_ref() {
        Some(R::RuntimeSnapshot(s)) => (
            axum::http::StatusCode::OK,
            Json(json!({ "runtimeSnapshot": json::runtime_snapshot_json(s) })),
        )
            .into_response(),
        Some(R::HistoryPage(h)) => (
            axum::http::StatusCode::OK,
            Json(json!({ "historyPage": json::history_page_json(h) })),
        )
            .into_response(),
        Some(R::CommandOutputPage(p)) => (
            axum::http::StatusCode::OK,
            Json(json!({
                "commandOutputPage": {
                    "itemId": p.item_id.as_ref().map(|i| json!({"id": i.id, "synthetic": i.synthetic})),
                    "nextCursor": if p.next_cursor.is_empty() { serde_json::Value::Null } else { json!(p.next_cursor) },
                    "bytesBase64": base64_std(&p.bytes),
                    "isFinal": p.is_final,
                    "channel": agent_console_protocol::v1::OutputChannel::try_from(p.channel)
                        .map(|c| c.as_str_name().to_string())
                        .unwrap_or_else(|_| "OUTPUT_CHANNEL_UNSPECIFIED".to_string()),
                }
            })),
        )
            .into_response(),
        Some(R::GitSummary(g)) => (
            axum::http::StatusCode::OK,
            Json(json!({ "gitSummary": {
                "branch": g.branch,
                "detachedHead": g.detached_head,
                "headShort": g.head_short,
                "headFull": g.head_full,
                "rootDisplayName": g.root_display_name,
                "entries": g.entries.iter().map(|e| json!({
                    "relativePath": e.relative_path,
                    "status": e.status,
                    "staged": e.staged,
                })).collect::<Vec<_>>(),
                "insertions": g.insertions,
                "deletions": g.deletions,
                "binaryFiles": g.binary_files,
            } })),
        )
            .into_response(),
        Some(R::GitFileDiff(d)) => (
            axum::http::StatusCode::OK,
            Json(json!({ "gitFileDiff": {
                "relativePath": d.relative_path,
                "staged": d.staged,
                "patchText": d.patch_text,
                "truncated": d.truncated,
                "totalBytes": d.total_bytes,
                "binary": d.binary,
            } })),
        )
            .into_response(),
        Some(R::FileMetadata(m)) => (
            axum::http::StatusCode::OK,
            Json(json!({ "fileMetadata": {
                "displayName": m.display_name,
                "mimeType": m.mime_type,
                "sizeBytes": m.size_bytes,
                "previewKind": m.preview_kind,
                "notPreviewableReason": if m.not_previewable_reason.is_empty() {
                    serde_json::Value::Null
                } else {
                    json!(m.not_previewable_reason)
                },
                "fileHandle": m.file_handle,
            } })),
        )
            .into_response(),
        None => err(Code::CodexUnavailable, "Bridge 未返回结果"),
    }
}

fn base64_std(b: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(b)
}

async fn session_runtime(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let request = QueryRequest {
        session_key: Some(query_session_key(&row)),
        query: Some(query_request::Query::RuntimeSnapshot(
            RuntimeSnapshotQuery {},
        )),
    };
    match app.hub.device_query(row.device_id, request).await {
        Ok(resp) => query_response_json(&resp),
        Err(code) => err(code, "设备离线或查询失败"),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryQuery {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    page_size: Option<u32>,
}

async fn session_history(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let page_size = q.page_size.unwrap_or(0).min(limits::PAGE_MAX as u32);
    let request = QueryRequest {
        session_key: Some(query_session_key(&row)),
        query: Some(query_request::Query::HistoryPage(HistoryPageQuery {
            cursor: q.cursor.unwrap_or_default(),
            page_size,
        })),
    };
    match app.hub.device_query(row.device_id, request).await {
        Ok(resp) => query_response_json(&resp),
        Err(code) => err(code, "设备离线或查询失败"),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutputQuery {
    item_id: String,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    page_size: Option<u32>,
}

/// 完整 command output 分页(§27.3;单页最大 256 KiB 由 Bridge 侧约束)。
async fn session_output(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(q): Query<OutputQuery>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let request = QueryRequest {
        session_key: Some(query_session_key(&row)),
        query: Some(query_request::Query::CommandOutputPage(
            CommandOutputPageQuery {
                item_id: Some(agent_console_protocol::v1::ItemId {
                    id: q.item_id,
                    synthetic: false,
                }),
                cursor: q.cursor.unwrap_or_default(),
                page_size: q.page_size.unwrap_or(0),
            },
        )),
    };
    match app.hub.device_query(row.device_id, request).await {
        Ok(resp) => query_response_json(&resp),
        Err(code) => err(code, "设备离线或查询失败"),
    }
}

/// Git 只读摘要(§23.1):转发 git_summary 查询到在线 Bridge。
async fn session_git(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Response {
    query_session(
        &app,
        addr,
        &headers,
        id,
        query_request::Query::GitSummary(GitSummaryQuery {}),
    )
    .await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitDiffQuery {
    path: String,
    #[serde(default)]
    staged: Option<bool>,
}

/// 单文件 staged/unstaged Diff(§23.1)。
async fn session_git_diff(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(q): Query<GitDiffQuery>,
) -> Response {
    query_session(
        &app,
        addr,
        &headers,
        id,
        query_request::Query::GitFileDiff(GitFileDiffQuery {
            relative_path: q.path,
            staged: q.staged.unwrap_or(false),
        }),
    )
    .await
}

/// 按 file_handle 读取文件元数据(§22.2/§27.3)。
async fn session_file_metadata(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(q): Query<FileMetadataParams>,
) -> Response {
    query_session(
        &app,
        addr,
        &headers,
        id,
        query_request::Query::FileMetadata(FileMetadataQuery {
            file_handle: q.handle,
        }),
    )
    .await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileMetadataParams {
    handle: String,
}

/// git/file 查询共用转发:仿 session_runtime 的 QueryRequest 模式(§27.3)。
async fn query_session(
    app: &AppState,
    addr: SocketAddr,
    headers: &HeaderMap,
    session_id: uuid::Uuid,
    query: query_request::Query,
) -> Response {
    let ident = match require_identity(app, addr, headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(app, ident.owner_id, session_id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let request = QueryRequest {
        session_key: Some(query_session_key(&row)),
        query: Some(query),
    };
    match app.hub.device_query(row.device_id, request).await {
        Ok(resp) => query_response_json(&resp),
        Err(code) => err(code, "设备离线或查询失败"),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestsQuery {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

async fn session_requests(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Query(q): Query<RequestsQuery>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let row = match owned_session(&app, ident.owner_id, id).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if let Some(request_id) = q.request_id.as_deref() {
        return match store::get_receipt(&app.db, request_id).await {
            Ok(Some(receipt)) if receipt.owner_id == ident.owner_id => {
                receipt_json_response(&receipt)
            }
            Ok(_) => err(Code::SessionNotFound, "回执不存在"),
            Err(_) => err(Code::InternalError, "查询回执失败"),
        };
    }
    match store::list_receipts_for_session(&app.db, row.id, q.limit.unwrap_or(50)).await {
        Ok(rows) => {
            let receipts: Vec<serde_json::Value> = rows
                .iter()
                .filter(|r| r.owner_id == ident.owner_id)
                .map(receipt_json)
                .collect();
            (
                axum::http::StatusCode::OK,
                Json(json!({ "receipts": receipts })),
            )
                .into_response()
        }
        Err(_) => err(Code::InternalError, "查询回执失败"),
    }
}

async fn get_request(
    State(app): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(request_id): Path<String>,
) -> Response {
    let ident = match require_identity(&app, addr, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    match store::get_receipt(&app.db, &request_id).await {
        Ok(Some(receipt)) if receipt.owner_id == ident.owner_id => receipt_json_response(&receipt),
        Ok(_) => err(Code::SessionNotFound, "回执不存在"),
        Err(_) => err(Code::InternalError, "查询回执失败"),
    }
}

fn receipt_json(r: &store::ReceiptRow) -> serde_json::Value {
    json!({
        "requestId": r.request_id,
        "sessionId": r.session_id,
        "operation": r.operation,
        "status": r.status,
        "errorCode": r.error_code,
        "createdAt": r.created_at.to_rfc3339(),
        "updatedAt": r.updated_at.to_rfc3339(),
    })
}

fn receipt_json_response(r: &store::ReceiptRow) -> Response {
    (axum::http::StatusCode::OK, Json(receipt_json(r))).into_response()
}
