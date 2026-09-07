//! Relay 全局状态、配置与集中常量。
//!
//! 常量集中定义(§26.2/§17.6/§27.5):心跳、离线判定、退避、缓冲三重上限、
//! 每 Browser 发送队列上限、全局内存上限、单帧/输出分块上限等禁止散落 magic number。
//! 部分时长允许用环境变量按部署覆盖(默认取常量),不建立庞大配置系统。

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

pub use agent_console_protocol::v1::StableErrorCode;
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use serde_json::json;

use crate::realtime::Hub;

// ---------------------------------------------------------------------------
// 集中常量(§26.2/§17.6)
// ---------------------------------------------------------------------------

pub mod limits {
    use std::time::Duration;

    /// 心跳间隔(§26.2:heartbeat 15s)。
    pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
    /// 连续约 45s 无有效心跳视为 offline(§26.2)。
    pub const OFFLINE_AFTER: Duration = Duration::from_secs(45);
    /// Bridge 重连指数退避下限 1s(§26.2;退避执行方为 Bridge,此处供文档/测试引用)。
    pub const BRIDGE_RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(1);
    /// Bridge 重连指数退避上限 30s,带 jitter(§26.2)。
    pub const BRIDGE_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);

    /// per-stream 缓冲保留时长 15 分钟(§17.6,三限取先到者)。
    pub const BUFFER_RETENTION: Duration = Duration::from_secs(15 * 60);
    /// per-stream 缓冲条数上限 10,000(§17.6)。
    pub const BUFFER_MAX_EVENTS: usize = 10_000;
    /// per-stream 缓冲序列化字节上限 16 MiB(§17.6)。
    pub const BUFFER_MAX_BYTES: usize = 16 * 1024 * 1024;
    /// 全局内存事件上限(按序列化字节数估算)(§17.6)。
    pub const GLOBAL_BUFFER_MAX_BYTES: usize = 256 * 1024 * 1024;
    /// 每 Browser 发送队列上限(帧数)(§17.6)。
    pub const BROWSER_QUEUE_MAX_FRAMES: usize = 2_048;
    /// 每 Bridge 出站队列上限(帧数)。
    pub const BRIDGE_QUEUE_MAX_FRAMES: usize = 1_024;
    /// 每上游快照在途暂存事件条数上限(§17.4 步骤 4:快照生成期间的新事件
    /// 只入暂存不定序;条数与 `BUFFER_MAX_BYTES` 字节双上限,压力下只挤出
    /// 非第 1 类事件,关键事件绝不静默丢弃,见 `DStream::push_pending`)。
    pub const SNAPSHOT_PENDING_MAX_EVENTS: usize = 1_024;

    /// Browser WS auth session introspection 周期(§20.5:至少每 60 秒)。
    pub const INTROSPECT_INTERVAL: Duration = Duration::from_secs(60);
    /// dev-toolbox 不可达时的有界宽限(§20.5:2 分钟)。
    pub const AUTH_GRACE: Duration = Duration::from_secs(120);

    /// 配对挑战有效期 5 分钟(§18.2/§21)。
    pub const PAIRING_TTL: Duration = Duration::from_secs(5 * 60);
    /// 每 challenge 尝试上限(§21)。
    pub const PAIRING_MAX_ATTEMPTS: u32 = 5;
    /// 配对来源限速窗口(§21:5 分钟)。
    pub const PAIRING_RATE_WINDOW: Duration = Duration::from_secs(5 * 60);
    /// register 来源限速(§21:建议 10 次/5 分钟)。
    pub const PAIRING_REGISTER_RATE_LIMIT: u32 = 10;
    /// claim 轮询来源限速(允许约 2 次/秒持续整个 TTL,防暴力)。
    pub const PAIRING_CLAIM_RATE_LIMIT: u32 = 600;
    /// lookup 来源限速(短码枚举防护)。
    pub const PAIRING_LOOKUP_RATE_LIMIT: u32 = 30;
    /// approve 来源限速。
    pub const PAIRING_APPROVE_RATE_LIMIT: u32 = 30;

    /// 分页默认值(§27.5):默认 50、最大 200。
    pub const PAGE_DEFAULT: i64 = 50;
    pub const PAGE_MAX: i64 = 200;

    /// 握手等待超时。
    pub const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
    /// HTTP → Bridge 在线查询超时(§27.3)。
    pub const UPSTREAM_QUERY_TIMEOUT: Duration = Duration::from_secs(85);
    /// Browser WS auth 本地过期检查兜底周期(expires_at 精确判定在每次写入与 introspect)。
    pub const LOCAL_EXPIRY_CHECK: Duration = Duration::from_secs(30);

    /// 浏览器固定 WS subprotocol 名(§20.2);Relay 只回显该名字。
    pub const WS_SUBPROTOCOL: &str = "agent-console.v1";
    /// ticket 以独立 subprotocol token 携带:`agent-console.ticket-<base64url>`。
    pub const WS_TICKET_PROTOCOL_PREFIX: &str = "agent-console.ticket-";

    /// 有界维护任务周期(§18:过期 pairing/receipt/audit 清理)。
    pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(3600);
    /// audit_events 保留 30 天(§18.7)。
    pub const AUDIT_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
    /// request_receipts / 过期 pairing 的有界保留(未规定值,取与 audit 相同的 30 天上界)。
    pub const RECEIPT_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);
    pub const PAIRING_RETENTION: Duration = Duration::from_secs(24 * 3600);
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    /// 调 dev-toolbox 内部接口用的 Bearer token(与 dev-toolbox 侧
    /// AGENT_CONSOLE_INTERNAL_TOKEN 同值);不写入日志。
    pub internal_token: Option<String>,
    pub devtoolbox_base_url: Option<String>,
    /// 信任的身份头来源网段(默认 loopback)。
    pub trusted_proxy_cidrs: Vec<Cidr>,
    /// 以下为可覆盖的时长(默认取 limits 常量;仅测试/部署调优用)。
    pub introspect_interval: Duration,
    pub auth_grace: Duration,
    pub heartbeat_interval: Duration,
    pub offline_after: Duration,
}

fn secs_from_env(env: &str, default: Duration) -> Duration {
    std::env::var(env)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|s| Duration::from_secs(s))
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        let database_url = std::env::var("RELAY_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("RELAY_DATABASE_URL 未设置"))?;
        let bind_addr =
            std::env::var("RELAY_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into());
        let trusted =
            std::env::var("TRUSTED_PROXY_CIDRS").unwrap_or_else(|_| "127.0.0.0/8,::1/128".into());
        let trusted_proxy_cidrs = trusted
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Cidr::from_str)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Config {
            database_url,
            bind_addr: bind_addr.parse()?,
            internal_token: std::env::var("RELAY_INTERNAL_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
            devtoolbox_base_url: std::env::var("DEVTOOLBOX_INTERNAL_BASE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            trusted_proxy_cidrs,
            introspect_interval: secs_from_env(
                "RELAY_INTROSPECT_SECS",
                limits::INTROSPECT_INTERVAL,
            ),
            auth_grace: secs_from_env("RELAY_AUTH_GRACE_SECS", limits::AUTH_GRACE),
            heartbeat_interval: secs_from_env("RELAY_HEARTBEAT_SECS", limits::HEARTBEAT_INTERVAL),
            offline_after: secs_from_env("RELAY_OFFLINE_AFTER_SECS", limits::OFFLINE_AFTER),
        })
    }
}

// ---------------------------------------------------------------------------
// CIDR(最小实现,避免新增依赖)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cidr {
    V4 { net: u32, prefix: u8 },
    V6 { net: u128, prefix: u8 },
}

impl FromStr for Cidr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, p.parse::<u8>()?),
            // 无前缀 = 单主机地址。
            None => (s, u8::MAX),
        };
        let ip: IpAddr = addr.parse()?;
        match ip {
            IpAddr::V4(v4) => {
                let max = 32u8;
                let prefix = prefix.min(max);
                let mask = if prefix == 0 {
                    0
                } else {
                    u32::MAX << (max - prefix)
                };
                Ok(Cidr::V4 {
                    net: u32::from(v4) & mask,
                    prefix,
                })
            }
            IpAddr::V6(v6) => {
                let max = 128u8;
                let prefix = prefix.min(max);
                let mask = if prefix == 0 {
                    0
                } else {
                    u128::MAX << (max - prefix)
                };
                Ok(Cidr::V6 {
                    net: u128::from(v6) & mask,
                    prefix,
                })
            }
        }
    }
}

impl Cidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Cidr::V4 { net, prefix }, IpAddr::V4(v4)) => {
                let mask = if *prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - *prefix)
                };
                u32::from(v4) & mask == *net
            }
            (Cidr::V6 { net, prefix }, IpAddr::V6(v6)) => {
                let mask = if *prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - *prefix)
                };
                u128::from(v6) & mask == *net
            }
            // v4-mapped v6 地址按 v4 比较。
            (Cidr::V4 { net, prefix }, IpAddr::V6(v6)) => {
                if let Some(v4) = v6.to_ipv4_mapped() {
                    let mask = if *prefix == 0 {
                        0
                    } else {
                        u32::MAX << (32 - *prefix)
                    };
                    u32::from(v4) & mask == *net
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// 稳定错误码(§27.5/§27.6)
// ---------------------------------------------------------------------------

/// StableErrorCode 的稳定名称(与 HTTP/WS 共用同一 code 集合)。
pub fn stable_code_name(code: i32) -> &'static str {
    match StableErrorCode::try_from(code) {
        Ok(c) => stable_code_ref_name(&c),
        Err(_) => "INTERNAL_ERROR",
    }
}

pub fn stable_code_ref_name(code: &StableErrorCode) -> &'static str {
    match code {
        StableErrorCode::Unspecified => "INTERNAL_ERROR",
        StableErrorCode::AuthRequired => "AUTH_REQUIRED",
        StableErrorCode::AuthExpired => "AUTH_EXPIRED",
        StableErrorCode::CsrfInvalid => "CSRF_INVALID",
        StableErrorCode::WsTicketInvalid => "WS_TICKET_INVALID",
        StableErrorCode::WsTicketExpired => "WS_TICKET_EXPIRED",
        StableErrorCode::WsTicketConsumed => "WS_TICKET_CONSUMED",
        StableErrorCode::DeviceOffline => "DEVICE_OFFLINE",
        StableErrorCode::DeviceRevoked => "DEVICE_REVOKED",
        StableErrorCode::CodexUnavailable => "CODEX_UNAVAILABLE",
        StableErrorCode::CodexVersionUnverified => "CODEX_VERSION_UNVERIFIED",
        StableErrorCode::ControlReadOnly => "CONTROL_READ_ONLY",
        StableErrorCode::CapabilityUnsupported => "CAPABILITY_UNSUPPORTED",
        StableErrorCode::SessionNotFound => "SESSION_NOT_FOUND",
        StableErrorCode::StaleTurn => "STALE_TURN",
        StableErrorCode::DuplicateRequestMismatch => "DUPLICATE_REQUEST_MISMATCH",
        StableErrorCode::OutcomeUnknown => "OUTCOME_UNKNOWN",
        StableErrorCode::ResyncRequired => "RESYNC_REQUIRED",
        StableErrorCode::QueueAlreadyExists => "QUEUE_ALREADY_EXISTS",
        StableErrorCode::QueuePaused => "QUEUE_PAUSED",
        StableErrorCode::QuestionExpired => "QUESTION_EXPIRED",
        StableErrorCode::ApprovalExpired => "APPROVAL_EXPIRED",
        StableErrorCode::SettingCombinationUnsupported => "SETTING_COMBINATION_UNSUPPORTED",
        StableErrorCode::FileHandleInvalid => "FILE_HANDLE_INVALID",
        StableErrorCode::FileOutsideScope => "FILE_OUTSIDE_SCOPE",
        StableErrorCode::FileChanged => "FILE_CHANGED",
        StableErrorCode::FileTypeNotPreviewable => "FILE_TYPE_NOT_PREVIEWABLE",
        StableErrorCode::TransferExpired => "TRANSFER_EXPIRED",
        StableErrorCode::TransferTooLarge => "TRANSFER_TOO_LARGE",
        StableErrorCode::TransferRangeInvalid => "TRANSFER_RANGE_INVALID",
        StableErrorCode::DiffTooLarge => "DIFF_TOO_LARGE",
        StableErrorCode::RateLimited => "RATE_LIMITED",
        StableErrorCode::InternalError => "INTERNAL_ERROR",
    }
}

/// 稳定错误码名 → 枚举(供从字符串来源恢复时使用)。
pub fn stable_code_from_name(name: &str) -> StableErrorCode {
    match name {
        "AUTH_REQUIRED" => StableErrorCode::AuthRequired,
        "AUTH_EXPIRED" => StableErrorCode::AuthExpired,
        "CSRF_INVALID" => StableErrorCode::CsrfInvalid,
        "WS_TICKET_INVALID" => StableErrorCode::WsTicketInvalid,
        "WS_TICKET_EXPIRED" => StableErrorCode::WsTicketExpired,
        "WS_TICKET_CONSUMED" => StableErrorCode::WsTicketConsumed,
        "DEVICE_OFFLINE" => StableErrorCode::DeviceOffline,
        "DEVICE_REVOKED" => StableErrorCode::DeviceRevoked,
        "CODEX_UNAVAILABLE" => StableErrorCode::CodexUnavailable,
        "CODEX_VERSION_UNVERIFIED" => StableErrorCode::CodexVersionUnverified,
        "CONTROL_READ_ONLY" => StableErrorCode::ControlReadOnly,
        "CAPABILITY_UNSUPPORTED" => StableErrorCode::CapabilityUnsupported,
        "SESSION_NOT_FOUND" => StableErrorCode::SessionNotFound,
        "STALE_TURN" => StableErrorCode::StaleTurn,
        "DUPLICATE_REQUEST_MISMATCH" => StableErrorCode::DuplicateRequestMismatch,
        "OUTCOME_UNKNOWN" => StableErrorCode::OutcomeUnknown,
        "RESYNC_REQUIRED" => StableErrorCode::ResyncRequired,
        "QUEUE_ALREADY_EXISTS" => StableErrorCode::QueueAlreadyExists,
        "QUEUE_PAUSED" => StableErrorCode::QueuePaused,
        "QUESTION_EXPIRED" => StableErrorCode::QuestionExpired,
        "APPROVAL_EXPIRED" => StableErrorCode::ApprovalExpired,
        "SETTING_COMBINATION_UNSUPPORTED" => StableErrorCode::SettingCombinationUnsupported,
        "FILE_HANDLE_INVALID" => StableErrorCode::FileHandleInvalid,
        "FILE_OUTSIDE_SCOPE" => StableErrorCode::FileOutsideScope,
        "FILE_CHANGED" => StableErrorCode::FileChanged,
        "FILE_TYPE_NOT_PREVIEWABLE" => StableErrorCode::FileTypeNotPreviewable,
        "TRANSFER_EXPIRED" => StableErrorCode::TransferExpired,
        "TRANSFER_TOO_LARGE" => StableErrorCode::TransferTooLarge,
        "TRANSFER_RANGE_INVALID" => StableErrorCode::TransferRangeInvalid,
        "DIFF_TOO_LARGE" => StableErrorCode::DiffTooLarge,
        "RATE_LIMITED" => StableErrorCode::RateLimited,
        _ => StableErrorCode::InternalError,
    }
}

/// 统一 JSON 错误响应(§27.5):
/// `{"error":{"code","message","requestId","details"}}`。
pub fn api_error(
    status: StatusCode,
    code: StableErrorCode,
    message: &str,
    request_id: &str,
    details: serde_json::Value,
) -> Response {
    let body = json!({
        "error": {
            "code": stable_code_ref_name(&code),
            "message": message,
            "requestId": request_id,
            "details": details,
        }
    });
    (status, Json(body)).into_response()
}

/// 常用状态码映射:认证 401、未找到 404、限流 429、设备离线/上游不可用 503、其余 500。
pub fn status_for_code(code: &StableErrorCode) -> StatusCode {
    match code {
        StableErrorCode::AuthRequired
        | StableErrorCode::AuthExpired
        | StableErrorCode::WsTicketInvalid
        | StableErrorCode::WsTicketExpired
        | StableErrorCode::WsTicketConsumed => StatusCode::UNAUTHORIZED,
        StableErrorCode::SessionNotFound => StatusCode::NOT_FOUND,
        StableErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        StableErrorCode::DeviceOffline
        | StableErrorCode::CodexUnavailable
        | StableErrorCode::DeviceRevoked => StatusCode::SERVICE_UNAVAILABLE,
        StableErrorCode::StaleTurn
        | StableErrorCode::DuplicateRequestMismatch
        | StableErrorCode::QueueAlreadyExists => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ---------------------------------------------------------------------------
// 审计占位(§18.7;持久化由 relay-data workstream 实现)
// ---------------------------------------------------------------------------

/// 审计事件元数据;不含正文/标题/路径(§25)。
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub owner_id: uuid::Uuid,
    pub device_id: Option<uuid::Uuid>,
    pub session_id: Option<uuid::Uuid>,
    pub request_id: Option<String>,
    pub operation: String,
    pub result: String,
    pub latency_ms: Option<i64>,
}

/// 审计记录调用点:devices/sessions 写路径统一经过此函数;
/// 落库到 audit_events(§18.7;relay-data workstream 实现)。
pub async fn record_audit(db: &sqlx::PgPool, event: AuditEvent) {
    // 日志只允许白名单字段(§25.3)。
    tracing::debug!(
        target: "relay::audit",
        operation = %event.operation,
        result = %event.result,
        latency_ms = event.latency_ms,
        "audit_event"
    );
    crate::audit::record(db, event).await;
}

// ---------------------------------------------------------------------------
// 应用状态
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: sqlx::PgPool,
    pub hub: Arc<Hub>,
    /// dev-toolbox 内部接口客户端(§20.3)。
    pub toolbox: crate::auth::ToolboxClient,
    /// 文件 transfer 注册表(仅内存,TTL 过期清理,§22)。
    pub transfers: Arc<crate::transfers::TransferRegistry>,
    /// Web Push 发送状态(VAPID/fake sink 装配,§24)。
    pub push: Arc<crate::push::PushState>,
}

/// 生成请求 ID(HTTP requestId / WS correlation 用)。
pub fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 对明文凭据/challenge/短码计算 SHA-256 hex 摘要。
pub fn sha256_hex(input: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

/// 生成 32 字节高熵随机值的 base64url(无填充)编码(约 43 字符)。
pub fn high_entropy_token() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// 生成 6 位数字短码(字符串形式,含前导零)。
pub fn six_digit_code() -> String {
    use rand::Rng;
    format!("{:06}", rand::rngs::OsRng.gen_range(0..1_000_000))
}

/// 常量时间比较两个 hex 摘要。
pub fn ct_eq_hex(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// 来源是否属于可信代理网段(§20.4:只信任网关网络携带的身份头)。
pub fn is_trusted_proxy(config: &Config, addr: SocketAddr) -> bool {
    config
        .trusted_proxy_cidrs
        .iter()
        .any(|c| c.contains(addr.ip()))
}

/// 限速用客户端来源 IP(§21:可信代理头)。
/// 直连方为可信代理时取 `X-Forwarded-For` 最左值(网关注入的原始客户端),
/// 否则使用直连地址;头解析失败回退直连地址。
pub fn client_ip_for_limit(config: &Config, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    if is_trusted_proxy(config, peer) {
        if let Some(xff) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim)
        {
            if let Ok(ip) = xff.parse::<IpAddr>() {
                return ip;
            }
        }
    }
    peer.ip()
}

/// 简单的每 IP 速率限制器(配对 register 等内部入口用)。
#[derive(Default)]
pub struct IpRateLimiter {
    inner: Mutex<std::collections::HashMap<IpAddr, (u32, std::time::Instant)>>,
}

impl IpRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记一次并返回是否超限(窗口内超过 limit 返回 true)。
    pub fn hit(&self, ip: IpAddr, limit: u32, window: Duration) -> bool {
        let mut guard = self.inner.lock();
        let now = std::time::Instant::now();
        let count = {
            let entry = guard.entry(ip).or_insert((0, now));
            if now.duration_since(entry.1) > window {
                *entry = (0, now);
            }
            entry.0 += 1;
            entry.0
        };
        // 顺带有界清理,避免长期运行的 map 无界增长。
        if guard.len() > 10_000 {
            guard.retain(|_, (_, start)| now.duration_since(*start) <= window);
        }
        count > limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn cidr_parse_and_contains() {
        let c: Cidr = "127.0.0.0/8".parse().unwrap();
        assert!(c.contains(IpAddr::V4(Ipv4Addr::new(127, 9, 9, 9))));
        assert!(!c.contains(IpAddr::V4(Ipv4Addr::new(128, 0, 0, 1))));

        let v6: Cidr = "::1/128".parse().unwrap();
        assert!(v6.contains(IpAddr::from_str("::1").unwrap()));
        assert!(!v6.contains(IpAddr::from_str("::2").unwrap()));

        // 无前缀 = 单地址(全前缀)。
        let host: Cidr = "10.1.2.3".parse().unwrap();
        assert!(host.contains(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
        assert!(!host.contains(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 4))));

        // v4-mapped v6 按 v4 网段比较。
        let mapped = IpAddr::from_str("::ffff:127.0.0.1").unwrap();
        assert!(c.contains(mapped));
    }

    #[test]
    fn stable_code_roundtrip() {
        for code in [
            StableErrorCode::AuthRequired,
            StableErrorCode::DeviceOffline,
            StableErrorCode::ResyncRequired,
            StableErrorCode::InternalError,
        ] {
            assert_eq!(stable_code_from_name(stable_code_ref_name(&code)), code);
        }
        assert_eq!(stable_code_name(999), "INTERNAL_ERROR");
    }

    #[test]
    fn rate_limiter_windows() {
        let lim = IpRateLimiter::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for _ in 0..3 {
            assert!(!lim.hit(ip, 3, Duration::from_secs(60)));
        }
        assert!(lim.hit(ip, 3, Duration::from_secs(60)));
    }
}
