//! 设备绑定(§21 Bridge-first):
//!
//! 1. Bridge 本地生成 32 字节高熵 challenge(base64url 无填充,43 字符),
//!    `POST /agent-console/bridge/pairing/register` {challenge, deviceName, platform,
//!    arch, bridgeVersion} → Relay 生成 challenge_id(UUID)与 6 位短码,只存两者
//!    SHA-256 摘要(明文不落库、不进日志),TTL 5 分钟,按来源限速;
//!    200 {challengeId, shortCode, expiresAt}(短码明文只此一次返回给 Bridge 展示)。
//! 2. 浏览器(已登录)`POST /agent-console/api/pairing/lookup` {shortCode} → 挑战详情;
//!    锁定 → 429 RATE_LIMITED;找不到/过期/取消/已批准 → 404(details.reason)。
//! 3. 浏览器 `POST /agent-console/api/pairing/approve` {shortCode, challengeId?} →
//!    原子 UPDATE...RETURNING 单赢家;批准即回填 owner_id。Origin/CSRF 由网关
//!    forward-auth 对非 GET 请求 fail-closed 校验(§20.4),Relay 无 Cookie 面。
//! 4. Bridge 轮询 `POST /agent-console/bridge/pairing/claim` {challengeId, challenge} →
//!    未批准 200 {status:"pending"};已批准且未发放 → 事务内生成 device_id 与
//!    32 字节高熵 device_credential(库内只存摘要,单次交付),200 {status:"ready",
//!    deviceId, deviceCredential};已发放再次 claim → 409(details.reason CONSUMED);
//!    过期 → 410。claim 亦按来源限速(防暴力)。
//!
//! 明文 challenge/短码/凭据不落库、不进任何日志(§21/§25.3)。

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::OnceLock;

use crate::state::{
    api_error, client_ip_for_limit, ct_eq_hex, high_entropy_token, limits, new_request_id,
    record_audit, sha256_hex, six_digit_code, AppState, AuditEvent, IpRateLimiter, StableErrorCode,
};

/// register 来源限速(§21:建议 10 次/5 分钟)。
static REGISTER_LIMITER: OnceLock<IpRateLimiter> = OnceLock::new();
/// claim 轮询来源限速(防暴力)。
static CLAIM_LIMITER: OnceLock<IpRateLimiter> = OnceLock::new();
/// lookup 来源限速(短码暴力枚举)。
static LOOKUP_LIMITER: OnceLock<IpRateLimiter> = OnceLock::new();
/// approve 来源限速。
static APPROVE_LIMITER: OnceLock<IpRateLimiter> = OnceLock::new();

fn register_limiter() -> &'static IpRateLimiter {
    REGISTER_LIMITER.get_or_init(IpRateLimiter::new)
}

fn claim_limiter() -> &'static IpRateLimiter {
    CLAIM_LIMITER.get_or_init(IpRateLimiter::new)
}

fn lookup_limiter() -> &'static IpRateLimiter {
    LOOKUP_LIMITER.get_or_init(IpRateLimiter::new)
}

fn approve_limiter() -> &'static IpRateLimiter {
    APPROVE_LIMITER.get_or_init(IpRateLimiter::new)
}

/// 带稳定 reason 的错误响应(details.reason 供前端/CLI 判定流程)。
fn err(status: StatusCode, code: StableErrorCode, message: &str, reason: &str) -> Response {
    api_error(
        status,
        code,
        message,
        &new_request_id(),
        serde_json::json!({ "reason": reason }),
    )
}

// ---------------------------------------------------------------------------
// Bridge:register(发起挑战,§21.1)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBody {
    challenge: String,
    device_name: String,
    platform: String,
    arch: String,
    bridge_version: String,
}

/// 32 字节 challenge 的 base64url 无填充校验(43 字符,URL 安全字母表)。
fn is_valid_challenge(s: &str) -> bool {
    s.len() == 43
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub async fn register(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> Response {
    // 来源速率限制(§21:可信代理头解析,复用 TRUSTED_PROXY_CIDRS)。
    if register_limiter().hit(
        client_ip_for_limit(&app.config, peer, &headers),
        limits::PAIRING_REGISTER_RATE_LIMIT,
        limits::PAIRING_RATE_WINDOW,
    ) {
        return err(
            StatusCode::TOO_MANY_REQUESTS,
            StableErrorCode::RateLimited,
            "配对请求过于频繁",
            "RATE_LIMITED",
        );
    }
    if !is_valid_challenge(body.challenge.trim()) {
        return err(
            StatusCode::UNAUTHORIZED,
            StableErrorCode::AuthRequired,
            "challenge 格式无效",
            "INVALID_CHALLENGE",
        );
    }
    let id = uuid::Uuid::new_v4();
    let short_code = six_digit_code();
    let result = sqlx::query(
        "INSERT INTO pairing_challenges \
           (id, owner_id, challenge_digest, short_code_digest, device_name, platform, arch, \
            bridge_version, expires_at, registered_at) \
         VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8), now())",
    )
    .bind(id)
    // 只存摘要:challenge/短码明文不落库(§21.1/§25.3)。
    .bind(sha256_hex(body.challenge.trim().as_bytes()))
    .bind(sha256_hex(short_code.as_bytes()))
    .bind(
        body.device_name
            .trim()
            .chars()
            .take(128)
            .collect::<String>(),
    )
    .bind(body.platform.trim().chars().take(64).collect::<String>())
    .bind(body.arch.trim().chars().take(32).collect::<String>())
    .bind(
        body.bridge_version
            .trim()
            .chars()
            .take(64)
            .collect::<String>(),
    )
    .bind(limits::PAIRING_TTL.as_secs() as i32)
    .execute(&app.db)
    .await;
    match result {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "challengeId": id,
                "shortCode": short_code,
                "expiresAt": (chrono::Utc::now() + limits::PAIRING_TTL).to_rfc3339(),
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "pairing register failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "配对注册失败",
                "INTERNAL_ERROR",
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Bridge:claim(轮询领取凭据,§21.6/§21.7)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimBody {
    challenge_id: uuid::Uuid,
    challenge: String,
}

pub async fn claim(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<ClaimBody>,
) -> Response {
    if claim_limiter().hit(
        client_ip_for_limit(&app.config, peer, &headers),
        limits::PAIRING_CLAIM_RATE_LIMIT,
        limits::PAIRING_RATE_WINDOW,
    ) {
        return err(
            StatusCode::TOO_MANY_REQUESTS,
            StableErrorCode::RateLimited,
            "配对领取过于频繁",
            "RATE_LIMITED",
        );
    }
    let row = sqlx::query_as::<
        _,
        (
            String,
            Option<chrono::DateTime<chrono::Utc>>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        ),
    >(
        "SELECT challenge_digest, approved_at, expires_at, credential_delivered_at \
         FROM pairing_challenges WHERE id = $1",
    )
    .bind(body.challenge_id)
    .fetch_optional(&app.db)
    .await;
    let Ok(Some((stored_digest, approved_at, expires_at, delivered_at))) = row else {
        return err(
            StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "配对请求不存在",
            "NOT_FOUND",
        );
    };
    // challenge 摘要常量时间校验(§21:持 challenge 明文者才能领取)。
    if !ct_eq_hex(
        &sha256_hex(body.challenge.trim().as_bytes()),
        &stored_digest,
    ) {
        return err(
            StatusCode::UNAUTHORIZED,
            StableErrorCode::AuthRequired,
            "challenge 不匹配",
            "CHALLENGE_MISMATCH",
        );
    }
    if expires_at <= chrono::Utc::now() {
        return err(
            StatusCode::GONE,
            StableErrorCode::ApprovalExpired,
            "配对请求已过期",
            "EXPIRED",
        );
    }
    if delivered_at.is_some() {
        // 单次交付:再次 claim 拒绝(§21.7)。
        return err(
            StatusCode::CONFLICT,
            StableErrorCode::DuplicateRequestMismatch,
            "凭据已被领取",
            "CONSUMED",
        );
    }
    let Some(_) = approved_at else {
        // 未批准:Bridge 继续 pending 轮询。
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "pending" })),
        )
            .into_response();
    };
    // 单次交付事务:原子标记 delivered + 创建设备行(凭据只存摘要,§21.6/§21.8)。
    let credential = high_entropy_token();
    let device_id = uuid::Uuid::new_v4();
    let mut tx = match app.db.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "pairing claim tx begin failed");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "配对领取失败",
                "INTERNAL_ERROR",
            );
        }
    };
    let delivered = sqlx::query_as::<
        _,
        (
            uuid::Uuid,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "UPDATE pairing_challenges SET credential_delivered_at = now(), consumed_at = now() \
         WHERE id = $1 AND approved_at IS NOT NULL AND credential_delivered_at IS NULL \
           AND expires_at > now() \
         RETURNING owner_id, device_name, platform, arch, bridge_version",
    )
    .bind(body.challenge_id)
    .fetch_optional(&mut *tx)
    .await;
    let delivered = match delivered {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "pairing claim update failed");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "配对领取失败",
                "INTERNAL_ERROR",
            );
        }
    };
    let Some((owner_id, device_name, platform, arch, bridge_version)) = delivered else {
        // 并发下另一 claim 已交付。
        return err(
            StatusCode::CONFLICT,
            StableErrorCode::DuplicateRequestMismatch,
            "凭据已被领取",
            "CONSUMED",
        );
    };
    let inserted = sqlx::query(
        "INSERT INTO devices \
           (id, owner_id, display_name, platform, arch, bridge_version, credential_digest, paired_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
    )
    .bind(device_id)
    .bind(owner_id)
    .bind(device_name.unwrap_or_else(|| "Mac".to_string()))
    .bind(platform.unwrap_or_default())
    .bind(arch.unwrap_or_default())
    .bind(bridge_version.unwrap_or_default())
    .bind(sha256_hex(credential.as_bytes()))
    .execute(&mut *tx)
    .await;
    if let Err(e) = inserted {
        tracing::error!(error = %e, "pairing claim device insert failed");
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            StableErrorCode::InternalError,
            "设备创建失败",
            "INTERNAL_ERROR",
        );
    }
    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, "pairing claim commit failed");
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            StableErrorCode::InternalError,
            "配对领取失败",
            "INTERNAL_ERROR",
        );
    }
    record_audit(
        &app.db,
        AuditEvent {
            owner_id,
            device_id: Some(device_id),
            session_id: None,
            request_id: Some(new_request_id()),
            operation: "pairing_claim".into(),
            result: "OK".into(),
            latency_ms: None,
        },
    )
    .await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ready",
            "deviceId": device_id,
            "deviceCredential": credential,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Browser:lookup(输入短码查看设备信息,§21.3/§21.4)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupBody {
    short_code: String,
}

/// 按短码摘要取最新一条已注册挑战(含终态,供 lookup/approve 错误分类)。
async fn newest_by_short_code(
    db: &sqlx::PgPool,
    code_digest: &str,
) -> sqlx::Result<
    Option<(
        uuid::Uuid,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        i32,
    )>,
> {
    sqlx::query_as(
        "SELECT id, expires_at, approved_at, consumed_at, attempt_count \
         FROM pairing_challenges \
         WHERE short_code_digest = $1 AND registered_at IS NOT NULL \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(code_digest)
    .fetch_optional(db)
    .await
}

pub async fn lookup(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<LookupBody>,
) -> Response {
    // 身份(经网关 forward-auth 注入,§20.4)。
    if let Err(resp) = crate::auth::require_identity(&app, peer, &headers) {
        return resp;
    }
    // 短码枚举防护:lookup 亦按来源限速。
    if lookup_limiter().hit(
        client_ip_for_limit(&app.config, peer, &headers),
        limits::PAIRING_LOOKUP_RATE_LIMIT,
        limits::PAIRING_RATE_WINDOW,
    ) {
        return err(
            StatusCode::TOO_MANY_REQUESTS,
            StableErrorCode::RateLimited,
            "查询过于频繁",
            "RATE_LIMITED",
        );
    }
    let code_digest = sha256_hex(body.short_code.trim().as_bytes());
    let row = newest_by_short_code(&app.db, &code_digest).await;
    let row = match row {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "pairing lookup failed");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "查询配对请求失败",
                "INTERNAL_ERROR",
            );
        }
    };
    // 找不到/过期/取消/已批准 → 404 + details.reason(沿用现有 404 约定);
    // 已锁定 → 429(§21.5)。
    let Some((id, expires_at, approved_at, consumed_at, attempts)) = row else {
        return err(
            StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "短码无效",
            "NOT_FOUND",
        );
    };
    if expires_at <= chrono::Utc::now() {
        return err(
            StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "配对请求已过期",
            "EXPIRED",
        );
    }
    if consumed_at.is_some() {
        return err(
            StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "配对请求已取消",
            "CANCELLED",
        );
    }
    if approved_at.is_some() {
        return err(
            StatusCode::NOT_FOUND,
            StableErrorCode::SessionNotFound,
            "配对请求已批准",
            "ALREADY_APPROVED",
        );
    }
    if attempts >= limits::PAIRING_MAX_ATTEMPTS as i32 {
        return err(
            StatusCode::TOO_MANY_REQUESTS,
            StableErrorCode::RateLimited,
            "短码错误次数过多,配对已锁定",
            "LOCKED",
        );
    }
    let info = sqlx::query_as::<
        _,
        (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT device_name, platform, arch, bridge_version FROM pairing_challenges WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&app.db)
    .await;
    match info {
        Ok((device_name, platform, arch, bridge_version)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "challengeId": id,
                "deviceName": device_name,
                "platform": platform,
                "arch": arch,
                "bridgeVersion": bridge_version,
                "expiresAt": expires_at.to_rfc3339(),
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "pairing lookup info failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "查询配对请求失败",
                "INTERNAL_ERROR",
            )
        }
    }
}

pub async fn approve_challenge(
    State(app): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<ApproveBody>,
) -> Response {
    // 身份(经网关 forward-auth 注入;写请求 Origin/CSRF 由网关 fail-closed,§20.4)。
    let ident = match crate::auth::require_identity(&app, peer, &headers) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    if approve_limiter().hit(
        client_ip_for_limit(&app.config, peer, &headers),
        limits::PAIRING_APPROVE_RATE_LIMIT,
        limits::PAIRING_RATE_WINDOW,
    ) {
        return err(
            StatusCode::TOO_MANY_REQUESTS,
            StableErrorCode::RateLimited,
            "批准请求过于频繁",
            "RATE_LIMITED",
        );
    }
    approve(&app, ident.owner_id, body).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApproveBody {
    short_code: String,
    /// lookup 返回的 challengeId;提供时短码错误可归因计数(每 challenge 5 次锁定)。
    challenge_id: Option<uuid::Uuid>,
}

/// 批准(单赢家):原子 UPDATE ... RETURNING;owner 在批准时回填(§21)。
pub async fn approve(app: &AppState, owner_id: uuid::Uuid, body: ApproveBody) -> Response {
    let code_digest = sha256_hex(body.short_code.trim().as_bytes());
    // 可归因短码错误计数:浏览器传入 lookup 得到的 challengeId 时,
    // 重输短码不一致 → attempt_count + 1,达上限锁定(§21.5)。
    if let Some(challenge_id) = body.challenge_id {
        let stored = sqlx::query_as::<_, (String,)>(
            "SELECT short_code_digest FROM pairing_challenges WHERE id = $1",
        )
        .bind(challenge_id)
        .fetch_optional(&app.db)
        .await
        .ok()
        .flatten();
        if let Some((stored,)) = stored {
            if !ct_eq_hex(&code_digest, &stored) {
                let attempts = sqlx::query_scalar::<_, i32>(
                    "UPDATE pairing_challenges SET attempt_count = attempt_count + 1 \
                     WHERE id = $1 RETURNING attempt_count",
                )
                .bind(challenge_id)
                .fetch_one(&app.db)
                .await
                .unwrap_or(i32::MAX);
                if attempts > limits::PAIRING_MAX_ATTEMPTS as i32 {
                    return err(
                        StatusCode::TOO_MANY_REQUESTS,
                        StableErrorCode::RateLimited,
                        "短码错误次数过多,配对已锁定",
                        "LOCKED",
                    );
                }
                return err(
                    StatusCode::UNAUTHORIZED,
                    StableErrorCode::AuthRequired,
                    "短码不正确",
                    "CODE_MISMATCH",
                );
            }
        }
    }
    // 单赢家原子批准(并发只有一个成功;owner 批准时回填)。
    // FOR UPDATE 必须在子查询内:锁等待发生在筛选阶段,READ COMMITTED 下
    // 赢家提交后其余并发者的子查询按新行版本重估(approved_at 非空)返回空,
    // 单赢家语义才真正成立(外层 id 相等条件本身不会被 EvalPlanQual 淘汰)。
    let approved = sqlx::query_as::<_, (uuid::Uuid, chrono::DateTime<chrono::Utc>)>(
        "UPDATE pairing_challenges SET approved_at = now(), owner_id = $3 \
         WHERE id = (SELECT id FROM pairing_challenges \
                     WHERE short_code_digest = $1 AND registered_at IS NOT NULL \
                       AND approved_at IS NULL AND consumed_at IS NULL \
                       AND expires_at > now() AND attempt_count < $2 \
                     ORDER BY created_at DESC LIMIT 1 FOR UPDATE) \
         RETURNING id, approved_at",
    )
    .bind(&code_digest)
    .bind(limits::PAIRING_MAX_ATTEMPTS as i32)
    .bind(owner_id)
    .fetch_optional(&app.db)
    .await;
    match approved {
        Ok(Some((challenge_id, approved_at))) => {
            record_audit(
                &app.db,
                AuditEvent {
                    owner_id,
                    device_id: None,
                    session_id: None,
                    request_id: Some(new_request_id()),
                    operation: "pairing_approve".into(),
                    result: "OK".into(),
                    latency_ms: None,
                },
            )
            .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "challengeId": challenge_id,
                    "approvedAt": approved_at.to_rfc3339(),
                })),
            )
                .into_response()
        }
        Ok(None) => {
            // 分类:找不到 / 过期 / 取消 / 已批准 / 锁定。
            let row = newest_by_short_code(&app.db, &code_digest)
                .await
                .ok()
                .flatten();
            match row {
                None => err(
                    StatusCode::NOT_FOUND,
                    StableErrorCode::SessionNotFound,
                    "短码无效",
                    "NOT_FOUND",
                ),
                Some((_, expires_at, _, _, _attempts)) if expires_at <= chrono::Utc::now() => err(
                    StatusCode::GONE,
                    StableErrorCode::ApprovalExpired,
                    "配对请求已过期",
                    "EXPIRED",
                ),
                Some((_, _, _, Some(_), _)) => err(
                    StatusCode::NOT_FOUND,
                    StableErrorCode::SessionNotFound,
                    "配对请求已取消",
                    "CANCELLED",
                ),
                Some((_, _, Some(_), _, _)) => err(
                    StatusCode::CONFLICT,
                    StableErrorCode::DuplicateRequestMismatch,
                    "配对请求已批准",
                    "ALREADY_APPROVED",
                ),
                Some((_, _, _, _, attempts)) if attempts >= limits::PAIRING_MAX_ATTEMPTS as i32 => {
                    err(
                        StatusCode::TOO_MANY_REQUESTS,
                        StableErrorCode::RateLimited,
                        "短码错误次数过多,配对已锁定",
                        "LOCKED",
                    )
                }
                Some(_) => err(
                    StatusCode::NOT_FOUND,
                    StableErrorCode::SessionNotFound,
                    "短码无效",
                    "NOT_FOUND",
                ),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "pairing approve failed");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                StableErrorCode::InternalError,
                "批准失败",
                "INTERNAL_ERROR",
            )
        }
    }
}
