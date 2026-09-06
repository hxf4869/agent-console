//! Bridge 运行配置(环境变量 + 默认值)。
//!
//! 权威规格:docs/ZCODE-BACKEND-EXECUTION-SPEC.md
//! - §19:数据目录、headless binary、凭据不进配置。
//! - §26.2:心跳 15s、约 45s 无心跳判离线、重连退避 1s–30s 带 jitter;
//!   这些值集中定义,不散落 magic number。
//!
//! 本模块不出现任何凭据(§19:环境变量中也不得有凭据;凭据只在 Keychain)。

use std::path::PathBuf;
use std::time::Duration;

// ---------------------------------------------------------------------------
// 集中常量(§26.2 建议初始值)
// ---------------------------------------------------------------------------

/// 应用层心跳发送间隔(§26.2:heartbeat 15s)。
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// 连续无任何入站帧判为离线的时长(§26.2:约 45s 无有效 heartbeat 视为 offline)。
pub const HEARTBEAT_DEAD_AFTER: Duration = Duration::from_secs(45);
/// 重连退避初始值(§26.2:1s)。
pub const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// 重连退避上限(§26.2:30s)。
pub const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// 重连退避随机抖动上限,避免惊群。
pub const RECONNECT_JITTER: Duration = Duration::from_millis(250);
/// 连接(含 WS 握手)单次预算:超时即按连接失败进入退避(§26.2 初始值;
/// 网络黑洞下不能依赖 OS 级分钟超时,也不能让 shutdown 等待连接)。
pub const CONNECT_BUDGET: Duration = Duration::from_secs(10);
/// 单帧写出预算:超过即判链路失活,断开进入退避重连(§26.2/§26.4;
/// 不重放结果未知的写:出站帧由上层按 epoch/快照语义重同步)。
pub const SEND_BUDGET: Duration = Duration::from_secs(10);
/// 对 Relay 发送队列容量(有界,§17.6;满时调用方立即得到错误,不阻塞)。
pub const OUTBOUND_QUEUE_CAPACITY: usize = 256;
/// 上层接收分发队列容量;满时断开重连以触发重同步(§26.4 慢 consumer 策略)。
pub const INBOUND_QUEUE_CAPACITY: usize = 1024;
/// 上传临时目录名(位于 data_dir 下,权限 0700)。
pub const UPLOADS_DIR_NAME: &str = "uploads";

// ---------------------------------------------------------------------------
// Relay 端点路径常量(与 Relay 侧合同对齐;完整 URL 只由 RelayUrls 派生)
// ---------------------------------------------------------------------------

/// Bridge WebSocket 公网路径([`RelayUrls::bridge_ws_url`] 拼接;§17/§21)。
pub const BRIDGE_WS_PATH: &str = "/agent-console/bridge/ws";
/// 配对注册端点路径(Bridge → Relay,§21 步骤 1-2)。
pub const PAIRING_REGISTER_PATH: &str = "/agent-console/bridge/pairing/register";
/// 配对 claim(轮询)端点路径(§21 步骤 6-7)。
pub const PAIRING_CLAIM_PATH: &str = "/agent-console/bridge/pairing/claim";
/// 配对二维码深链路径(前端页面;Bridge 只生成数据字符串)。
pub const PAIRING_DEEP_LINK_PATH: &str = "/agent-console/pair";
/// producer 端点路径前缀:`POST {origin}/agent-console/transfers/producer/{transfer_id}`(§22.4)。
pub const PRODUCER_PATH_PREFIX: &str = "/agent-console/transfers/producer/";
/// consumer 端点路径前缀:`GET {origin}/agent-console/transfers/consumer/{transfer_id}`(§22.5)。
pub const CONSUMER_PATH_PREFIX: &str = "/agent-console/transfers/consumer/";

// ---------------------------------------------------------------------------
// 环境变量名
// ---------------------------------------------------------------------------

pub const ENV_RELAY_URL: &str = "AGENT_CONSOLE_RELAY_URL";

// ---------------------------------------------------------------------------
// RelayUrls:AGENT_CONSOLE_RELAY_URL 的唯一派生点
// ---------------------------------------------------------------------------

/// `AGENT_CONSOLE_RELAY_URL` 配置错误。错误信息只描述合同,不回显原始输入,
/// 避免把可能携带凭据的 URL 片段带进日志(§25.3)。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RelayUrlError {
    /// 用户把完整 WebSocket 地址(如 `wss://host/agent-console/ws`)填进了
    /// 基地址变量:给出专门提示。
    #[error(
        "AGENT_CONSOLE_RELAY_URL 应为 Relay 公网基地址:该值是完整 WebSocket 地址, \
         请提供公网基地址(不含 /agent-console/... 路径),例如 https://toolbox.example.com"
    )]
    FullWebSocketAddress,
    /// 带了 `/agent-console` 之外的其他路径。
    #[error(
        "AGENT_CONSOLE_RELAY_URL 不允许携带路径:请提供公网基地址(origin), \
         各端点路径由 Bridge 统一派生"
    )]
    PathNotAllowed,
    /// scheme 不在 http/https/ws/wss 范围内。
    #[error("AGENT_CONSOLE_RELAY_URL scheme 不支持: {0}(允许 http/https/ws/wss)")]
    UnsupportedScheme(String),
    /// 空值、相对 URL、缺少 host、带 query/fragment 等无法形成 origin 的输入。
    #[error("AGENT_CONSOLE_RELAY_URL 无法解析为带 host 的绝对 URL(允许 http/https/ws/wss)")]
    Invalid,
}

/// `AGENT_CONSOLE_RELAY_URL` 的唯一语义与派生入口:**Relay 公网基地址(origin)**。
///
/// 合同(全 crate 只在此处派生 URL;§17/§21/§22):
/// - 输入允许 http/https/ws/wss,ws/wss 内部规范化为 http/https;
/// - host 必须存在;port 可选;path 必须为空或 `/`(尾斜杠容忍,规范化去掉);
/// - path 以 `/agent-console` 开头 → [`RelayUrlError::FullWebSocketAddress`]
///   (常见误填完整 WS 地址);其他 path → [`RelayUrlError::PathNotAllowed`]。
///
/// 派生结果(示例输入 `https://toolbox.example.com`):
/// - [`Self::bridge_ws_url`] = `wss://toolbox.example.com/agent-console/bridge/ws`
/// - [`Self::pairing_register_url`] / [`Self::pairing_claim_url`] = `.../bridge/pairing/{register|claim}`
/// - [`Self::transfer_producer_url`] / [`Self::transfer_consumer_url`] = `.../transfers/{producer|consumer}/{id}`
/// - [`Self::pairing_deep_link`] = `{origin}/agent-console/pair?code={shortCode}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayUrls {
    /// 规范化 http(s) origin(无路径、无尾斜杠、无 userinfo)。
    http_origin: String,
}

impl RelayUrls {
    /// 校验并解析基地址。
    pub fn parse(input: &str) -> Result<Self, RelayUrlError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(RelayUrlError::Invalid);
        }
        // Url::parse 会把 scheme 与 host 小写化,天然容忍大小写。
        let mut url = url::Url::parse(trimmed).map_err(|_| RelayUrlError::Invalid)?;
        match url.scheme() {
            "ws" => url.set_scheme("http").map_err(|_| RelayUrlError::Invalid)?,
            "wss" => url
                .set_scheme("https")
                .map_err(|_| RelayUrlError::Invalid)?,
            "http" | "https" => {}
            other => return Err(RelayUrlError::UnsupportedScheme(other.to_owned())),
        }
        if url.host_str().map_or(true, str::is_empty) {
            return Err(RelayUrlError::Invalid);
        }
        let path = url.path();
        if !(path.is_empty() || path == "/") {
            if path.starts_with("/agent-console") {
                return Err(RelayUrlError::FullWebSocketAddress);
            }
            return Err(RelayUrlError::PathNotAllowed);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(RelayUrlError::PathNotAllowed);
        }
        Ok(Self {
            http_origin: url.origin().ascii_serialization(),
        })
    }

    /// 规范化 http(s) origin(无尾斜杠;存储与展示用)。
    pub fn http_origin(&self) -> &str {
        &self.http_origin
    }

    /// Bridge WebSocket 连接地址(https→wss / http→ws)。
    pub fn bridge_ws_url(&self) -> String {
        format!("{}{BRIDGE_WS_PATH}", self.ws_root())
    }

    /// 配对注册端点(§21 步骤 2)。
    pub fn pairing_register_url(&self) -> String {
        format!("{}{PAIRING_REGISTER_PATH}", self.http_origin)
    }

    /// 配对 claim(轮询)端点(§21 步骤 6-7)。
    pub fn pairing_claim_url(&self) -> String {
        format!("{}{PAIRING_CLAIM_PATH}", self.http_origin)
    }

    /// producer 端点完整 URL(§22.4:Bridge 推送文件正文)。
    pub fn transfer_producer_url(&self, transfer_id: &str) -> String {
        format!("{}{PRODUCER_PATH_PREFIX}{transfer_id}", self.http_origin)
    }

    /// consumer 端点完整 URL(§22.5:Bridge 接收上传正文)。
    pub fn transfer_consumer_url(&self, transfer_id: &str) -> String {
        format!("{}{CONSUMER_PATH_PREFIX}{transfer_id}", self.http_origin)
    }

    /// 配对二维码数据字符串(深链):`{origin}/agent-console/pair?code={shortCode}`。
    /// origin 已是 http/https(用户填 ws/wss 时规范化),不含任何凭据。
    pub fn pairing_deep_link(&self, short_code: &str) -> String {
        format!(
            "{}{PAIRING_DEEP_LINK_PATH}?code={short_code}",
            self.http_origin
        )
    }

    /// https origin → wss,http origin → ws。
    fn ws_root(&self) -> String {
        if let Some(rest) = self.http_origin.strip_prefix("https://") {
            format!("wss://{rest}")
        } else {
            self.http_origin.replacen("http://", "ws://", 1)
        }
    }
}
pub const ENV_DATA_DIR: &str = "AGENT_CONSOLE_DATA_DIR";
pub const ENV_CODEX_HOME: &str = "AGENT_CONSOLE_CODEX_HOME";
pub const ENV_IPC_SOCKET: &str = "AGENT_CONSOLE_IPC_SOCKET";
/// 配对时上报的设备显示名(可选;不额外引入主机名依赖,缺失时用固定占位)。
pub const ENV_DEVICE_NAME: &str = "AGENT_CONSOLE_DEVICE_NAME";

// ---------------------------------------------------------------------------
// BridgeConfig
// ---------------------------------------------------------------------------

/// Bridge 运行配置。仅含非敏感项;凭据永远只从 Keychain 读取。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeConfig {
    /// Relay 公网基地址(origin,如 `https://toolbox.example.com`;经
    /// [`RelayUrls`] 校验派生后使用。`--relay-url` 优先于环境变量)。未绑定时为 None。
    pub relay_url: Option<String>,
    /// 应用数据目录(macOS 默认 `~/Library/Application Support/agent-console`);
    /// SQLite、uploads 临时目录都在其下。测试用 [`BridgeConfig::with_data_dir`]
    /// 覆盖到临时目录。
    pub data_dir: PathBuf,
    /// Codex home(默认 `~/.codex`)。Bridge 只读其下内容,绝不写入。
    pub codex_home: PathBuf,
    /// Codex Desktop IPC socket 路径覆盖项。默认 None:由 adapter 的 owner
    /// 发现机制动态定位;设置后 adapter 优先使用该路径。
    pub ipc_socket_path: Option<PathBuf>,
    pub heartbeat_interval: Duration,
    pub heartbeat_dead_after: Duration,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    pub reconnect_jitter: Duration,
    pub outbound_queue_capacity: usize,
    pub inbound_queue_capacity: usize,
}

impl BridgeConfig {
    /// 从环境变量 + 默认值构造(不读取任何凭据)。
    pub fn from_env() -> Self {
        let relay_url = std::env::var(ENV_RELAY_URL).ok().filter(|s| !s.is_empty());
        let data_dir = std::env::var(ENV_DATA_DIR)
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(default_data_dir);
        let codex_home = std::env::var(ENV_CODEX_HOME)
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(default_codex_home);
        let ipc_socket_path = std::env::var(ENV_IPC_SOCKET)
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        Self {
            relay_url,
            data_dir,
            codex_home,
            ipc_socket_path,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_dead_after: HEARTBEAT_DEAD_AFTER,
            reconnect_initial_backoff: RECONNECT_INITIAL_BACKOFF,
            reconnect_max_backoff: RECONNECT_MAX_BACKOFF,
            reconnect_jitter: RECONNECT_JITTER,
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            inbound_queue_capacity: INBOUND_QUEUE_CAPACITY,
        }
    }

    /// 覆盖 data_dir(测试场景)。
    pub fn with_data_dir(mut self, dir: PathBuf) -> Self {
        self.data_dir = dir;
        self
    }

    /// 覆盖 relay_url(CLI `--relay-url` 优先于环境变量)。
    pub fn with_relay_url(mut self, url: impl Into<String>) -> Self {
        self.relay_url = Some(url.into());
        self
    }

    /// 上传临时目录:data_dir/uploads(由 local_store 负责以 0700 创建)。
    pub fn uploads_dir(&self) -> PathBuf {
        self.data_dir.join(UPLOADS_DIR_NAME)
    }
}

/// 默认数据目录:macOS `~/Library/Application Support/agent-console`;
/// directories 不可用时退回 `~/.agent-console`。
fn default_data_dir() -> PathBuf {
    if let Some(dirs) = directories::ProjectDirs::from("", "", "agent-console") {
        return dirs.data_dir().to_path_buf();
    }
    let mut fallback = std::env::home_dir().unwrap_or_else(|| PathBuf::from("."));
    fallback.push(".agent-console");
    fallback
}

/// 默认 Codex home:`~/.codex`。
fn default_codex_home() -> PathBuf {
    let mut home = std::env::home_dir().unwrap_or_else(|| PathBuf::from("."));
    home.push(".codex");
    home
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_concentrated_spec_values() {
        let cfg = BridgeConfig::from_env();
        assert_eq!(cfg.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(cfg.heartbeat_dead_after, Duration::from_secs(45));
        assert_eq!(cfg.reconnect_initial_backoff, Duration::from_secs(1));
        assert_eq!(cfg.reconnect_max_backoff, Duration::from_secs(30));
        assert!(cfg.outbound_queue_capacity > 0);
        assert!(cfg.inbound_queue_capacity > cfg.outbound_queue_capacity);
    }

    #[test]
    fn uploads_dir_is_under_data_dir() {
        let cfg = BridgeConfig::from_env().with_data_dir(PathBuf::from("/tmp/bridge-test"));
        assert_eq!(cfg.uploads_dir(), PathBuf::from("/tmp/bridge-test/uploads"));
    }

    #[test]
    fn config_contains_no_secret_fields() {
        // §19:凭据不进配置。用调试输出锁定:新增敏感字段应放 Keychain 而非这里。
        let cfg = BridgeConfig::from_env();
        let debug = format!("{cfg:?}").to_lowercase();
        assert!(!debug.contains("token"));
        assert!(!debug.contains("credential"));
        assert!(!debug.contains("password"));
    }

    // -------------------------------------------------------------------------
    // RelayUrls 测试矩阵(任务合同:各派生 URL 断言全文)
    // -------------------------------------------------------------------------

    /// 合法输入矩阵 → 各派生 URL 全文断言。
    #[test]
    fn relay_urls_derive_all_endpoints_from_bare_origin() {
        let u = RelayUrls::parse("https://example.test").unwrap();
        assert_eq!(u.http_origin(), "https://example.test");
        assert_eq!(
            u.bridge_ws_url(),
            "wss://example.test/agent-console/bridge/ws"
        );
        assert_eq!(
            u.pairing_register_url(),
            "https://example.test/agent-console/bridge/pairing/register"
        );
        assert_eq!(
            u.pairing_claim_url(),
            "https://example.test/agent-console/bridge/pairing/claim"
        );
        assert_eq!(
            u.transfer_producer_url("t-1"),
            "https://example.test/agent-console/transfers/producer/t-1"
        );
        assert_eq!(
            u.transfer_consumer_url("t-1"),
            "https://example.test/agent-console/transfers/consumer/t-1"
        );
        assert_eq!(
            u.pairing_deep_link("012345"),
            "https://example.test/agent-console/pair?code=012345"
        );
    }

    #[test]
    fn relay_urls_accept_wss_trailing_slash_local_port() {
        // wss 内部规范化为 https。
        let u = RelayUrls::parse("wss://example.test").unwrap();
        assert_eq!(u.http_origin(), "https://example.test");
        assert_eq!(
            u.bridge_ws_url(),
            "wss://example.test/agent-console/bridge/ws"
        );
        // 尾斜杠容忍并规范化去掉。
        assert_eq!(
            RelayUrls::parse("https://example.test/")
                .unwrap()
                .http_origin(),
            "https://example.test"
        );
        // 本地开发:带端口的 http 基地址。
        let local = RelayUrls::parse("http://127.0.0.1:8081").unwrap();
        assert_eq!(local.http_origin(), "http://127.0.0.1:8081");
        assert_eq!(
            local.bridge_ws_url(),
            "ws://127.0.0.1:8081/agent-console/bridge/ws"
        );
        assert_eq!(
            local.pairing_deep_link("654321"),
            "http://127.0.0.1:8081/agent-console/pair?code=654321"
        );
    }

    #[test]
    fn relay_urls_tolerate_scheme_case() {
        let u = RelayUrls::parse("WSS://Example.Test").unwrap();
        assert_eq!(u.http_origin(), "https://example.test");
        let u = RelayUrls::parse("HTTP://LocalHost:8081").unwrap();
        assert_eq!(u.http_origin(), "http://localhost:8081");
    }

    #[test]
    fn relay_urls_reject_full_websocket_address_with_specific_hint() {
        let err = RelayUrls::parse("https://x/agent-console/ws").unwrap_err();
        assert_eq!(err, RelayUrlError::FullWebSocketAddress);
        let text = err.to_string();
        assert!(text.contains("完整 WebSocket 地址"), "actual: {text}");
        assert!(text.contains("公网基地址"), "actual: {text}");
        // wss 变体同样命中专门提示。
        assert_eq!(
            RelayUrls::parse("wss://x/agent-console/bridge/ws").unwrap_err(),
            RelayUrlError::FullWebSocketAddress
        );
    }

    #[test]
    fn relay_urls_reject_other_paths_and_queries() {
        assert_eq!(
            RelayUrls::parse("https://x/other").unwrap_err(),
            RelayUrlError::PathNotAllowed
        );
        assert_eq!(
            RelayUrls::parse("https://x/api/v1").unwrap_err(),
            RelayUrlError::PathNotAllowed
        );
        assert_eq!(
            RelayUrls::parse("https://x/?a=b").unwrap_err(),
            RelayUrlError::PathNotAllowed
        );
    }

    #[test]
    fn relay_urls_reject_unsupported_scheme_empty_and_relative() {
        assert!(matches!(
            RelayUrls::parse("ftp://x").unwrap_err(),
            RelayUrlError::UnsupportedScheme(_)
        ));
        assert!(matches!(
            RelayUrls::parse("").unwrap_err(),
            RelayUrlError::Invalid
        ));
        assert!(matches!(
            RelayUrls::parse("   ").unwrap_err(),
            RelayUrlError::Invalid
        ));
        assert!(matches!(
            RelayUrls::parse("example.test").unwrap_err(),
            RelayUrlError::Invalid
        ));
        assert!(matches!(
            RelayUrls::parse("/relative/path").unwrap_err(),
            RelayUrlError::Invalid
        ));
        assert!(matches!(
            RelayUrls::parse("https://").unwrap_err(),
            RelayUrlError::Invalid
        ));
    }

    #[test]
    fn relay_url_errors_do_not_echo_input() {
        // 错误串不得回显输入(可能含 userinfo 等敏感片段,§25.3)。
        let secret = "super-secret-pass";
        let inputs = [
            format!("https://user:{secret}@x/agent-console/ws"),
            format!("https://user:{secret}@x/other"),
            format!("https://user:{secret}@x"),
        ];
        for input in inputs {
            if let Ok(u) = RelayUrls::parse(&input) {
                // 合法输入的派生结果只含 origin,凭据片段被丢弃。
                assert!(!u.bridge_ws_url().contains(secret));
            } else {
                let err = RelayUrls::parse(&input).unwrap_err().to_string();
                assert!(!err.contains(secret), "error echoed input: {err}");
            }
        }
    }
}
