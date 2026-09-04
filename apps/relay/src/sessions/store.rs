//! 会话摘要持久化与回执存储(§11/§18.3/§18.4)。
//!
//! 隐私边界(§25.2):只保存摘要元数据;不保存 cwd/正文/Diff/文件名列表。

use agent_console_protocol::v1::{CommandReceiptStatus, SessionSummaryBatch};
use chrono::Utc;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionRow {
    pub id: uuid::Uuid,
    pub device_id: uuid::Uuid,
    pub agent_kind: String,
    pub native_session_id: String,
    pub title: String,
    pub project_display_name: String,
    pub current_branch: String,
    pub device_connection: String,
    pub device_last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    pub degraded_reason: String,
    pub control_mode: String,
    pub compatibility_state: String,
    pub active_turn_phase: String,
    pub pending_attention_count: i32,
    pub pending_attention_kinds: Vec<String>,
    pub queue_state: String,
    pub last_turn_outcome: String,
    pub runtime_revision: i64,
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
    pub pinned: bool,
    pub muted: bool,
    pub archived: bool,
}

fn ts_to_rfc3339(ts: &Option<prost_types::Timestamp>) -> Option<chrono::DateTime<chrono::Utc>> {
    ts.as_ref()
        .map(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32))
        .flatten()
        .map(|t| t.with_timezone(&Utc))
}

/// Bridge SessionSummaryBatch → session_summaries upsert(§11/§18.3)。
pub async fn upsert_summary_batch(
    db: &sqlx::PgPool,
    device: uuid::Uuid,
    batch: &SessionSummaryBatch,
) -> anyhow::Result<()> {
    let device_uuid = uuid::Uuid::parse_str(&summary_device_id(batch, device)).unwrap_or(device);
    for summary in &batch.summaries {
        let Some(key) = summary.session_key.as_ref() else {
            continue;
        };
        // 原生复合键(§9.1);agent_kind 未知枚举按数值保留。
        let agent_kind = crate::realtime::agent_kind_name(key.agent_kind);
        let native = &key.native_session_id;
        let title = summary.title.clone();
        let project = summary.project_display_name.clone();
        let branch = summary.current_branch.clone();
        let conn = enum_or(
            summary.device_connection,
            |e: agent_console_protocol::v1::DeviceConnection| e.as_str_name().to_string(),
            "CONNECTION_OFFLINE",
        );
        let degraded = summary.degraded_reason.clone();
        let control = enum_or(
            summary.control_mode,
            |e: agent_console_protocol::v1::ControlMode| e.as_str_name().to_string(),
            "CONTROL_MODE_UNSPECIFIED",
        );
        let compat = enum_or(
            summary.compatibility_state,
            |e: agent_console_protocol::v1::CompatibilityState| e.as_str_name().to_string(),
            "COMPATIBILITY_STATE_UNSPECIFIED",
        );
        let phase = enum_or(
            summary.active_turn_phase,
            |e: agent_console_protocol::v1::ActiveTurnPhase| e.as_str_name().to_string(),
            "ACTIVE_TURN_PHASE_UNSPECIFIED",
        );
        let attention = summary.pending_attention_count as i32;
        let kinds: Vec<String> = summary
            .pending_attention_kinds
            .iter()
            .map(|k| {
                agent_console_protocol::v1::PendingAttentionKind::try_from(*k)
                    .map(|e| e.as_str_name().to_string())
                    .unwrap_or_else(|_| format!("PENDING_ATTENTION_{k}"))
            })
            .collect();
        let queue = enum_or(
            summary.queue_state,
            |e: agent_console_protocol::v1::QueueState| e.as_str_name().to_string(),
            "QUEUE_STATE_UNSPECIFIED",
        );
        let outcome = enum_or(
            summary.last_turn_outcome,
            |e: agent_console_protocol::v1::LastTurnOutcome| e.as_str_name().to_string(),
            "LAST_TURN_OUTCOME_UNSPECIFIED",
        );
        let last_seen = ts_to_rfc3339(&summary.device_last_seen_at);
        let updated = ts_to_rfc3339(&summary.updated_at);

        sqlx::query(
            "INSERT INTO session_summaries (\
                id, device_id, agent_kind, native_session_id, title, project_display_name, current_branch, \
                device_connection, device_last_seen_at, degraded_reason, control_mode, compatibility_state, \
                active_turn_phase, pending_attention_count, pending_attention_kinds, queue_state, \
                last_turn_outcome, last_updated_at\
             ) VALUES (\
                gen_random_uuid(), $1, $2, $3, $4, $5, $6, \
                $7, $8, $9, $10, $11, \
                $12, $13, $14, $15, \
                $16, COALESCE($17, now())\
             )\
             ON CONFLICT (device_id, agent_kind, native_session_id) DO UPDATE SET \
                title = EXCLUDED.title, \
                project_display_name = EXCLUDED.project_display_name, \
                current_branch = EXCLUDED.current_branch, \
                device_connection = EXCLUDED.device_connection, \
                device_last_seen_at = EXCLUDED.device_last_seen_at, \
                degraded_reason = EXCLUDED.degraded_reason, \
                control_mode = EXCLUDED.control_mode, \
                compatibility_state = EXCLUDED.compatibility_state, \
                active_turn_phase = EXCLUDED.active_turn_phase, \
                pending_attention_count = EXCLUDED.pending_attention_count, \
                pending_attention_kinds = EXCLUDED.pending_attention_kinds, \
                queue_state = EXCLUDED.queue_state, \
                last_turn_outcome = EXCLUDED.last_turn_outcome, \
                last_updated_at = EXCLUDED.last_updated_at",
        )
        .bind(device_uuid)
        .bind(&agent_kind)
        .bind(native)
        .bind(&title)
        .bind(&project)
        .bind(&branch)
        .bind(&conn)
        .bind(last_seen)
        .bind(&degraded)
        .bind(&control)
        .bind(&compat)
        .bind(&phase)
        .bind(attention)
        .bind(&kinds)
        .bind(&queue)
        .bind(&outcome)
        .bind(updated)
        .execute(db)
        .await?;
    }
    Ok(())
}

/// 批内 summaries 可能未带 device_id;此时用上行连接的设备。
fn summary_device_id(batch: &SessionSummaryBatch, fallback: uuid::Uuid) -> String {
    batch
        .summaries
        .first()
        .and_then(|s| s.session_key.as_ref())
        .map(|k| k.device_id.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn enum_or<T, F: Fn(T) -> String>(v: i32, f: F, default: &str) -> String
where
    T: TryFrom<i32>,
{
    T::try_from(v)
        .map(f)
        .unwrap_or_else(|_| default.to_string())
}

/// 设备连接状态批量更新(上下线时,§10.1)。
pub async fn set_device_connection(
    db: &sqlx::PgPool,
    device: uuid::Uuid,
    connection: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE session_summaries SET device_connection = $2, \
         device_last_seen_at = CASE WHEN $2 = 'CONNECTION_OFFLINE' THEN now() ELSE device_last_seen_at END \
         WHERE device_id = $1",
    )
    .bind(device)
    .bind(connection)
    .execute(db)
    .await?;
    Ok(())
}

pub async fn find_session(
    db: &sqlx::PgPool,
    device: uuid::Uuid,
    agent_kind: &str,
    native_session_id: &str,
) -> anyhow::Result<Option<SessionRow>> {
    let row = sqlx::query_as::<_, SessionRow>(
        "SELECT id, device_id, agent_kind, native_session_id, title, project_display_name, current_branch, \
         device_connection, device_last_seen_at, degraded_reason, control_mode, compatibility_state, \
         active_turn_phase, pending_attention_count, pending_attention_kinds, queue_state, \
         last_turn_outcome, runtime_revision, last_updated_at, pinned, muted, archived \
         FROM session_summaries \
         WHERE device_id = $1 AND agent_kind = $2 AND native_session_id = $3",
    )
    .bind(device)
    .bind(agent_kind)
    .bind(native_session_id)
    .fetch_optional(db)
    .await?;
    Ok(row)
}

pub async fn get_session(db: &sqlx::PgPool, id: uuid::Uuid) -> anyhow::Result<Option<SessionRow>> {
    let row = sqlx::query_as::<_, SessionRow>(
        "SELECT id, device_id, agent_kind, native_session_id, title, project_display_name, current_branch, \
         device_connection, device_last_seen_at, degraded_reason, control_mode, compatibility_state, \
         active_turn_phase, pending_attention_count, pending_attention_kinds, queue_state, \
         last_turn_outcome, runtime_revision, last_updated_at, pinned, muted, archived \
         FROM session_summaries WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db)
    .await?;
    Ok(row)
}

/// 列表分页(§27.5):默认 50、最大 200;稳定 keyset cursor(§11.1/§26.3)。
pub struct ListParams {
    pub limit: i64,
    pub archived: Option<bool>,
    /// keyset cursor:(last_updated_at, id) DESC。
    pub before_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub before_id: Option<uuid::Uuid>,
}

pub async fn list_sessions(
    db: &sqlx::PgPool,
    owner: uuid::Uuid,
    params: &ListParams,
) -> anyhow::Result<Vec<SessionRow>> {
    let limit = params.limit.clamp(1, crate::state::limits::PAGE_MAX);
    let rows = sqlx::query_as::<_, SessionRow>(
        "SELECT s.id, s.device_id, s.agent_kind, s.native_session_id, s.title, s.project_display_name, \
         s.current_branch, s.device_connection, s.device_last_seen_at, s.degraded_reason, s.control_mode, \
         s.compatibility_state, s.active_turn_phase, s.pending_attention_count, s.pending_attention_kinds, \
         s.queue_state, s.last_turn_outcome, s.runtime_revision, s.last_updated_at, s.pinned, s.muted, s.archived \
         FROM session_summaries s JOIN devices d ON d.id = s.device_id \
         WHERE d.owner_id = $1 AND d.revoked_at IS NULL \
           AND ($2::bool IS NULL OR s.archived = $2) \
           AND ($3::timestamptz IS NULL OR (s.last_updated_at, s.id) < ($3, $4::uuid)) \
         ORDER BY s.last_updated_at DESC, s.id DESC \
         LIMIT $5",
    )
    .bind(owner)
    .bind(params.archived)
    .bind(params.before_updated_at)
    .bind(params.before_id)
    .bind(limit + 1) // 多取一条判断 has_more
    .fetch_all(db)
    .await?;
    Ok(rows)
}

/// 稳定 cursor 编码:`v1:<updated_at_rfc3339>:<id>` 的 base64url。
pub fn encode_cursor(row: &SessionRow) -> String {
    use base64::Engine;
    let raw = format!("v1:{}:{}", row.last_updated_at.to_rfc3339(), row.id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

pub fn decode_cursor(cursor: &str) -> Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let rest = raw.strip_prefix("v1:")?;
    let (ts, id) = rest.rsplit_once(':')?;
    Some((
        chrono::DateTime::parse_from_rfc3339(ts)
            .ok()?
            .with_timezone(&Utc),
        uuid::Uuid::parse_str(id).ok()?,
    ))
}

/// pin/mute/archive 偏好(§12:只改 Relay 自身偏好,不写回 Codex)。
pub async fn patch_prefs(
    db: &sqlx::PgPool,
    session: uuid::Uuid,
    pinned: Option<bool>,
    muted: Option<bool>,
    archived: Option<bool>,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE session_summaries SET pinned = COALESCE($2, pinned), \
         muted = COALESCE($3, muted), archived = COALESCE($4, archived) \
         WHERE id = $1 RETURNING id",
    )
    .bind(session)
    .bind(pinned)
    .bind(muted)
    .bind(archived)
    .fetch_optional(db)
    .await?;
    Ok(result.is_some())
}

// ---------------------------------------------------------------------------
// request_receipts(§18.4/§15.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReceiptRow {
    pub request_id: String,
    pub owner_id: uuid::Uuid,
    pub session_id: Option<uuid::Uuid>,
    pub operation: String,
    pub payload_digest: String,
    pub status: String,
    pub error_code: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// 插入回执;返回 false 表示已存在(重试去重,§15.2)。
pub async fn insert_receipt(
    db: &sqlx::PgPool,
    request_id: &str,
    owner: uuid::Uuid,
    session_id: Option<uuid::Uuid>,
    operation: &str,
    payload_digest: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "INSERT INTO request_receipts \
           (request_id, owner_id, session_id, operation, payload_digest, status) \
         VALUES ($1, $2, $3, $4, $5, 'RECEIVED') \
         ON CONFLICT (request_id) DO NOTHING RETURNING request_id",
    )
    .bind(request_id)
    .bind(owner)
    .bind(session_id)
    .bind(operation)
    .bind(payload_digest)
    .fetch_optional(db)
    .await?;
    Ok(result.is_some())
}

pub async fn update_receipt(
    db: &sqlx::PgPool,
    request_id: &str,
    status: &str,
    error_code: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE request_receipts SET status = $2, error_code = $3, updated_at = now() WHERE request_id = $1",
    )
    .bind(request_id)
    .bind(status)
    .bind(error_code)
    .execute(db)
    .await?;
    Ok(())
}

pub async fn get_receipt(
    db: &sqlx::PgPool,
    request_id: &str,
) -> anyhow::Result<Option<ReceiptRow>> {
    let row = sqlx::query_as::<_, ReceiptRow>(
        "SELECT request_id, owner_id, session_id, operation, payload_digest, status, error_code, created_at, updated_at \
         FROM request_receipts WHERE request_id = $1",
    )
    .bind(request_id)
    .fetch_optional(db)
    .await?;
    Ok(row)
}

pub async fn list_receipts_for_session(
    db: &sqlx::PgPool,
    session_id: uuid::Uuid,
    limit: i64,
) -> anyhow::Result<Vec<ReceiptRow>> {
    let rows = sqlx::query_as::<_, ReceiptRow>(
        "SELECT request_id, owner_id, session_id, operation, payload_digest, status, error_code, created_at, updated_at \
         FROM request_receipts WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(session_id)
    .bind(limit.clamp(1, 200))
    .fetch_all(db)
    .await?;
    Ok(rows)
}

/// 状态字符串 → CommandReceiptStatus(HTTP JSON 输出用)。
pub fn receipt_status_code(status: &str) -> Option<CommandReceiptStatus> {
    match status {
        "RECEIVED" => Some(CommandReceiptStatus::ReceiptReceived),
        "ACCEPTED_BY_BRIDGE" => Some(CommandReceiptStatus::ReceiptAcceptedByBridge),
        "DISPATCHED_TO_CODEX" => Some(CommandReceiptStatus::ReceiptDispatchedToCodex),
        "COMPLETED" => Some(CommandReceiptStatus::ReceiptCompleted),
        "REJECTED" => Some(CommandReceiptStatus::ReceiptRejected),
        "OUTCOME_UNKNOWN" => Some(CommandReceiptStatus::ReceiptOutcomeUnknown),
        _ => None,
    }
}
