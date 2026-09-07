//! Bridge headless CLI(权威规格 §19)。
//!
//! 子命令:`run` / `doctor` / `pair` / `unpair` / `workspace` / `inspect`。
//!
//! 集成边界:runtime 已接通 —— `run` 组装 BridgeRuntime(adapter 事件 →
//! transport 转发、命令/查询/文件流分发、观察策略与电源轮询);`doctor`
//! 输出 discovery 报告与能力矩阵;`inspect` 走 adapter 只读数据源。
//!
//! 日志边界(§25.3):data_dir 等路径只出现在 doctor 的 stdout,不进普通运行
//! 日志;任何命令不输出凭据、会话正文与绝对工作区路径。

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};

use bridge::adapter::codex::{CodexAdapter, CodexAdapterConfig};
use bridge::commands::{CommandGateway, PowerCoordinator, UploadLifecycle};
use bridge::config::{BridgeConfig, ENV_DEVICE_NAME};
use bridge::domain::SessionKey;
use bridge::files::{FileGrantManager, TransferConfig};
use bridge::git::GitService;
use bridge::keychain::{KeychainStore, MacKeychainStore};
use bridge::local_store::{BindingStatus, LocalStore};
use bridge::power::{PowerSource, WakePolicy};
use bridge::runtime::{BridgeRuntime, FsUploadCleaner, RuntimeParts};
use bridge::transport::{DeviceCredential, OnConnected, RelayClientOptions, RelayHandle};

fn main() -> std::process::ExitCode {
    init_tracing();
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match rt.block_on(dispatch(cli)) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("错误: {err:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // stderr:helper 子命令(zcode-hook / mcp-stdio)的 stdout 只承载协议。
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

// ---------------------------------------------------------------------------
// CLI 定义
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(
    name = "bridge",
    about = "Agent Console Bridge headless CLI(§19)",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// 启动常驻连接(Relay 连接 + Codex adapter 事件循环 + 观察策略)。
    Run {
        /// 覆盖 Relay 公网基地址(如 https://toolbox.example.com;优先级高于
        /// AGENT_CONSOLE_RELAY_URL。Bridge 自行派生 /agent-console/... 各端点路径)。
        #[arg(long)]
        relay_url: Option<String>,
    },
    /// 只输出版本、路径可用性、keychain、绑定与能力矩阵;不输出会话正文。
    Doctor,
    /// 配对(§21 Bridge 发起):本地生成 challenge 向 Relay 注册,打印短码与
    /// 二维码数据,等待浏览器批准后取回凭据。
    Pair {
        /// Relay 公网基地址(如 https://toolbox.example.com;本地 ws://127.0.0.1:8081)。
        /// 缺省时读 AGENT_CONSOLE_RELAY_URL。不接受完整 WebSocket 地址。
        #[arg(long)]
        relay_url: Option<String>,
    },
    /// 清除本机凭据与绑定(显式动作,必须 --confirm)。
    Unpair {
        #[arg(long)]
        confirm: bool,
    },
    /// 管理允许新建任务与显式打开文件的工作区根目录。
    Workspace {
        #[command(subcommand)]
        action: WorkspaceAction,
    },
    /// 开发验证用只读命令(数据来自 adapter;不输出会话正文与路径)。
    Inspect {
        #[command(subcommand)]
        action: InspectAction,
    },
    /// ZCode Hook helper(04 §8.4):stdin 为原生 Hook JSON;stdout 仅输出
    /// 协议 JSON(诊断走 stderr)。由插件 hooks.json 以固定 argv 调用。
    ZcodeHook {
        /// Hook socket 路径覆盖(缺省 data_dir/zcode-hook.sock)。
        #[arg(long)]
        socket: Option<PathBuf>,
        /// 远程等待预算 ms(默认 45000;ZCode 官方 Hook 预算 60s)。
        #[arg(long, default_value_t = bridge::zcode::contract::DEFAULT_REMOTE_WAIT_MS)]
        wait_ms: u64,
    },
    /// ZCode MCP stdio server(04 §8.8):工具 agent_console.ask_user。
    McpStdio {
        /// Hook socket 路径覆盖(缺省 data_dir/zcode-hook.sock)。
        #[arg(long)]
        socket: Option<PathBuf>,
        /// 问答等待预算 ms(默认 45000;超时返回 expired)。
        #[arg(long, default_value_t = bridge::zcode::contract::DEFAULT_REMOTE_WAIT_MS)]
        wait_ms: u64,
    },
}

#[derive(Debug, Subcommand)]
enum WorkspaceAction {
    /// 列出已授权工作区。
    List,
    /// 授权一个根目录(路径必须存在;将保存 canonical path)。
    Authorize {
        #[arg(long)]
        path: String,
        /// 显示名(默认取目录名)。
        #[arg(long)]
        name: Option<String>,
    },
    /// 撤销授权。
    Revoke {
        #[arg(long)]
        path: String,
    },
}

#[derive(Debug, Subcommand)]
enum InspectAction {
    /// 只读:会话列表摘要(仅 ID 与多维状态,无标题正文)。
    Sessions,
    /// 只读:单个会话运行快照(仅稳定维度,无输出正文)。
    Session {
        /// 会话原生 ID(§9.1)。
        id: String,
    },
}

async fn dispatch(cli: Cli) -> anyhow::Result<std::process::ExitCode> {
    match cli.command {
        Command::Run { relay_url } => {
            let cfg = build_config(relay_url);
            cmd_run(cfg, Arc::new(MacKeychainStore)).await?;
        }
        Command::Doctor => {
            let cfg = BridgeConfig::from_env();
            let report = cmd_doctor(cfg, Arc::new(MacKeychainStore)).await?;
            println!("{report}");
        }
        Command::Pair { relay_url } => {
            let cfg = build_config(relay_url);
            cmd_pair(cfg, Arc::new(MacKeychainStore)).await?;
        }
        Command::Unpair { confirm } => {
            let cfg = BridgeConfig::from_env();
            cmd_unpair(cfg, Arc::new(MacKeychainStore), confirm).await?;
        }
        Command::Workspace { action } => {
            let cfg = BridgeConfig::from_env();
            match action {
                WorkspaceAction::List => cmd_workspace_list(cfg).await?,
                WorkspaceAction::Authorize { path, name } => {
                    cmd_workspace_authorize(cfg, path, name).await?
                }
                WorkspaceAction::Revoke { path } => cmd_workspace_revoke(cfg, path).await?,
            }
        }
        Command::Inspect { action } => {
            let cfg = BridgeConfig::from_env();
            match action {
                InspectAction::Sessions => cmd_inspect_sessions(cfg).await?,
                InspectAction::Session { id } => cmd_inspect_session(cfg, id).await?,
            }
        }
        Command::ZcodeHook { socket, wait_ms } => {
            let config = bridge::zcode::helper::HelperConfig {
                socket_path: socket
                    .unwrap_or_else(bridge::zcode::helper::default_socket_path),
                wait_ms,
            };
            let code = bridge::zcode::helper::run_hook(config).await;
            return Ok(std::process::ExitCode::from(code as u8));
        }
        Command::McpStdio { socket, wait_ms } => {
            let config = bridge::zcode::helper::HelperConfig {
                socket_path: socket
                    .unwrap_or_else(bridge::zcode::helper::default_socket_path),
                wait_ms,
            };
            bridge::zcode::mcp::serve(config, tokio::io::stdin(), tokio::io::stdout()).await?;
        }
    }
    Ok(std::process::ExitCode::SUCCESS)
}

fn build_config(relay_override: Option<String>) -> BridgeConfig {
    let cfg = BridgeConfig::from_env();
    match relay_override {
        Some(url) => cfg.with_relay_url(url),
        None => cfg,
    }
}

/// 从 Keychain 读出的凭据(缓存于启动时;rotation 需重启或重新 pair)。
struct KeychainBackedCredential {
    token: String,
    device_id: String,
}

impl std::fmt::Debug for KeychainBackedCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeychainBackedCredential")
            .field("device_id", &self.device_id)
            .finish_non_exhaustive()
    }
}

impl DeviceCredential for KeychainBackedCredential {
    fn bearer_token(&self) -> String {
        self.token.clone()
    }
}

/// inspect 用的设备 ID(绑定则用绑定 ID,否则 dev 占位;只进本地 SessionKey)。
async fn inspect_device_id(store: &LocalStore) -> anyhow::Result<String> {
    let binding = store.get_binding().await?;
    if binding.device_id.is_empty() {
        Ok("local-inspect".to_string())
    } else {
        Ok(binding.device_id)
    }
}

/// 构造 adapter 配置(共享:doctor/run/inspect;codex_home 取配置,
/// 绝不触碰 Codex 之外的任何数据)。
///
/// 写方法 probe 采用逐版本写能力矩阵注入
/// (`bridge::capabilities::verified_write_probes`);协议/只读兼容版本表
/// (`ipc::discovery::VERIFIED_VERSIONS`)与写能力矩阵相互独立。
fn adapter_config(
    cfg: &BridgeConfig,
    device_id: &str,
    version_report: Option<String>,
) -> CodexAdapterConfig {
    let mut config = CodexAdapterConfig::new(device_id);
    config.ipc_socket = cfg.ipc_socket_path.clone();
    config.codex_home = Some(cfg.codex_home.clone());
    config.write_method_probes =
        bridge::capabilities::verified_write_probes(version_report.as_deref());
    config.version_report = version_report;
    // attach 生命周期按同一固定 argv 重新探测版本:Desktop 运行中更新为
    // 未知版本时,写能力保持关闭(§5),只恢复可证明的读取能力。
    config.version_binary = Some(bridge::adapter::codex::ipc::discovery::default_version_binary());
    config
}

// ---------------------------------------------------------------------------
// bridge run
// ---------------------------------------------------------------------------

async fn cmd_run(cfg: BridgeConfig, keychain: Arc<dyn KeychainStore>) -> anyhow::Result<()> {
    let relay_url = cfg
        .relay_url
        .clone()
        .context("缺少 Relay 地址:使用 --relay-url 或环境变量 AGENT_CONSOLE_RELAY_URL")?;
    // AGENT_CONSOLE_RELAY_URL = Relay 公网基地址;WS 目标在此唯一派生,
    // 带路径的完整 WebSocket 地址在配置阶段即报错(含专门提示)。
    let urls = bridge::config::RelayUrls::parse(&relay_url)?;

    let store = LocalStore::open(&cfg.data_dir)
        .await
        .context("打开本地库失败")?;
    let binding = store.get_binding().await?;
    if binding.status != BindingStatus::Paired || binding.device_id.is_empty() {
        bail!("设备未绑定,请先执行: bridge pair --relay-url <url>");
    }
    let token = keychain
        .get_device_credential(&binding.device_id)
        .await
        .context("Keychain 不可用,Bridge 进入不可控制状态(不回退明文存储,§19)")?
        .context("Keychain 中无设备凭据,请重新执行 bridge pair")?;
    let device_id = binding.device_id.clone();

    // ---- Codex adapter(§12:IPC 不可用降级 catalog-only,不重启 Desktop) ----
    let version = bridge::adapter::codex::ipc::discovery::probe_version(None)
        .await
        .ok();
    let adapter = CodexAdapter::connect(adapter_config(&cfg, &device_id, version))
        .await
        .map_err(|err| anyhow::anyhow!("Codex adapter 连接失败: {err}"))?;

    // ---- 各模块组装 ----
    let gateway = CommandGateway::new(adapter.clone(), store.clone());
    let grants = FileGrantManager::new();
    let git = match GitService::new() {
        Ok(git) => Some(Arc::new(git)),
        Err(err) => {
            tracing::warn!(error = %err, "git executable unavailable; git queries disabled");
            None
        }
    };
    let uploads = Arc::new(UploadLifecycle::new(
        Arc::new(FsUploadCleaner::new(cfg.uploads_dir())),
        store.clone(),
    ));
    let power = PowerCoordinator::new(Arc::new(WakePolicy::system()), PowerSource::Ac);
    let credential: Arc<dyn DeviceCredential> = Arc::new(KeychainBackedCredential {
        token,
        device_id: device_id.clone(),
    });

    let mut options = RelayClientOptions::new(&urls).with_device_id(device_id.clone());
    options.heartbeat_interval = cfg.heartbeat_interval;
    options.heartbeat_dead_after = cfg.heartbeat_dead_after;
    options.reconnect_initial_backoff = cfg.reconnect_initial_backoff;
    options.reconnect_max_backoff = cfg.reconnect_max_backoff;
    options.reconnect_jitter = cfg.reconnect_jitter;
    options.outbound_capacity = cfg.outbound_queue_capacity;
    options.inbound_capacity = cfg.inbound_queue_capacity;

    // §26.2 on_connected 重同步在 BridgeRuntime::resync 内完成;
    // runtime 在 transport 启动后构造,经 OnceLock 回填。
    let runtime_slot: Arc<OnceLock<Arc<BridgeRuntime>>> = Arc::new(OnceLock::new());
    let on_connected: OnConnected = {
        let slot = runtime_slot.clone();
        Arc::new(move || {
            if let Some(runtime) = slot.get() {
                runtime.resync();
            }
        })
    };

    let credential_for_transport = credential.clone();
    let (handle, mut inbound) =
        bridge::transport::start(options, credential_for_transport, Some(on_connected));

    let runtime = Arc::new(BridgeRuntime::new(RuntimeParts {
        config: cfg.clone(),
        device_id,
        store,
        adapter,
        gateway,
        grants,
        git,
        uploads,
        power,
        credential,
        outbound: Arc::new(handle.clone()),
        http: reqwest::Client::new(),
        upload_root: cfg.uploads_dir(),
        transfer_config: TransferConfig::default(),
    }));
    let _ = runtime_slot.set(runtime.clone());
    let observer = runtime.start_observation();
    let power_poller = spawn_power_poller(runtime.clone());

    // ---- ZCode Hook 审批通路(ZC-01 原型;socket 绑定失败不阻断 run) ----
    let zcode_hooks = Arc::new(bridge::zcode::ZcodeHooks::new(
        runtime.device_id().to_string(),
        cfg.data_dir.join("zcode-hook.sock"),
    ));
    zcode_hooks.set_runtime(&runtime);
    runtime.attach_zcode_hooks(zcode_hooks.clone());
    let zcode_server = match bridge::zcode::serve(
        bridge::zcode::HookServerConfig {
            socket_path: zcode_hooks.socket_path().to_path_buf(),
        },
        zcode_hooks.clone(),
    )
    .await
    {
        Ok(task) => Some(task),
        Err(err) => {
            tracing::warn!(error = %err, "zcode hook socket unavailable; remote approval disabled");
            None
        }
    };
    tracing::info!("bridge run started");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal received");
                break;
            }
            envelope = inbound.recv() => match envelope {
                Some(env) => runtime.handle_envelope(env).await,
                None => {
                    tracing::info!("transport stopped");
                    break;
                }
            },
        }
    }
    // §26.4 优雅停机:gateway 停止接受并给未完成命令补 OUTCOME_UNKNOWN →
    // 停观察与电源断言 → 停 ZCode hook socket(旧 pending 随进程失效)→
    // 断开 Relay。
    runtime.shutdown().await;
    power_poller.abort();
    observer.abort();
    if let Some(task) = zcode_server {
        task.abort();
        bridge::zcode::server::remove_socket(zcode_hooks.socket_path());
    }
    shutdown_relay(&handle).await;
    Ok(())
}

async fn shutdown_relay(handle: &RelayHandle) {
    handle.shutdown();
    // 给后台任务一个短暂窗口完成断开与状态落盘(§26.4)。
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------------------
// 电源来源检测(§19:固定 argv 系统工具,不经 shell)
// ---------------------------------------------------------------------------

/// pmset 固定路径(fixed argv;不解析 PATH)。
pub const PMSET_PATH: &str = "/usr/bin/pmset";
/// 电源来源轮询周期(§14 电池降频输入)。
pub const POWER_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// `/usr/bin/pmset -g batt` 输出 → 电源来源;无法判定时返回 None(保持现值)。
async fn detect_power_source() -> Option<PowerSource> {
    let output = tokio::process::Command::new(PMSET_PATH)
        .arg("-g")
        .arg("batt")
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    if text.contains("AC Power") {
        Some(PowerSource::Ac)
    } else if text.contains("Battery Power") {
        Some(PowerSource::Battery)
    } else {
        None
    }
}

/// 周期检测电源来源并注入 PowerCoordinator / 观察降频输入(§14/§19)。
fn spawn_power_poller(runtime: Arc<BridgeRuntime>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Some(source) = detect_power_source().await {
                runtime.set_power_source(source).await;
            }
            tokio::time::sleep(POWER_POLL_INTERVAL).await;
        }
    })
}

// ---------------------------------------------------------------------------
// bridge doctor
// ---------------------------------------------------------------------------

/// Keychain 探测账户:doctor 只读探测可达性,不写入任何数据。
pub const DOCTOR_PROBE_ACCOUNT: &str = "doctor-probe";

async fn cmd_doctor(cfg: BridgeConfig, keychain: Arc<dyn KeychainStore>) -> anyhow::Result<String> {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("bridge 版本: {}", env!("CARGO_PKG_VERSION")));

    // data_dir(stdout 允许出现;不进运行日志)。
    lines.push(format!("data_dir: {}", cfg.data_dir.display()));
    let store = match LocalStore::open(&cfg.data_dir).await {
        Ok(store) => {
            lines.push("  本地库: 正常".to_string());
            Some(store)
        }
        Err(err) => {
            lines.push(format!("  本地库: 打开失败({err})"));
            None
        }
    };
    lines.push(format!("  uploads 目录: {}", cfg.uploads_dir().display()));

    // codex home。
    lines.push(format!("codex_home: {}", cfg.codex_home.display()));
    lines.push(format!("  存在: {}", cfg.codex_home.exists()));

    // IPC socket。
    match &cfg.ipc_socket_path {
        Some(path) => {
            lines.push(format!("ipc_socket: {}", path.display()));
            lines.push(format!("  存在: {}", path.exists()));
        }
        None => lines.push("ipc_socket: 未配置(由 adapter owner 发现动态定位)".to_string()),
    }

    // Relay 地址(AGENT_CONSOLE_RELAY_URL 合同校验;输出派生 WS 地址便于诊断,
    // 不输出任何凭据)。
    match cfg.relay_url.as_deref() {
        Some(raw) => match bridge::config::RelayUrls::parse(raw) {
            Ok(urls) => {
                lines.push(format!("Relay 基地址: {}", urls.http_origin()));
                lines.push(format!("  Bridge WS: {}", urls.bridge_ws_url()));
            }
            Err(err) => lines.push(format!("Relay 地址: 配置错误({err})")),
        },
        None => {
            lines.push("Relay 地址: 未配置(AGENT_CONSOLE_RELAY_URL 或 --relay-url)".to_string())
        }
    }

    // Codex discovery 报告(版本/socket/clientId;无会话正文)。
    let device_id = if let Some(store) = &store {
        match store.get_binding().await {
            Ok(binding) if !binding.device_id.is_empty() => binding.device_id,
            _ => "doctor".to_string(),
        }
    } else {
        "doctor".to_string()
    };
    let version = bridge::adapter::codex::ipc::discovery::probe_version(None)
        .await
        .ok();
    match CodexAdapter::connect(adapter_config(&cfg, &device_id, version)).await {
        Ok(adapter) => {
            let report = adapter.discover().await;
            match report.codex_version {
                Some(version) => lines.push(format!("Codex 版本: {version}")),
                None => lines.push("Codex 版本: 未知(未探测到 CLI/Desktop)".to_string()),
            }
            lines.push(format!(
                "  IPC: 已连接={} clientId={}",
                report.ipc_connected,
                report.client_id.as_deref().unwrap_or("-")
            ));
            if let Some(path) = &report.socket_path {
                lines.push(format!("  socket: {}", path.display()));
            }
            if let Some(catalog) = &report.catalog {
                lines.push(format!(
                    "  catalog: state={:?} history={:?} sessions_listable={} history_readable={}",
                    catalog.state_db,
                    catalog.history_db,
                    catalog.sessions_listable,
                    catalog.history_readable
                ));
            } else {
                lines.push("  catalog: 不可用".to_string());
            }
            // 能力矩阵摘要(§5/§10.2/§10.3)。
            let caps = adapter.capabilities();
            lines.push(format!(
                "capability 矩阵: control={:?} compatibility={:?}",
                caps.control_mode, caps.compatibility_state
            ));
            let ops: Vec<String> = caps
                .supported_operations
                .iter()
                .map(|op| serde_json::to_value(op).unwrap_or_default().to_string())
                .collect();
            lines.push(format!("  写操作: [{}]", ops.join(", ")));
            let limits = &caps.transfer_limits;
            lines.push(format!(
                "TransferLimits: text={}MiB image={}MiB pdf={}MiB download={}MiB upload={}MiB \
                 并发(browser/device)={}/{}",
                limits.text_inline_max_bytes / 1024 / 1024,
                limits.image_inline_max_bytes / 1024 / 1024,
                limits.pdf_range_max_bytes / 1024 / 1024,
                limits.download_max_bytes / 1024 / 1024,
                limits.upload_max_bytes / 1024 / 1024,
                limits.max_concurrent_per_browser,
                limits.max_concurrent_per_device,
            ));
        }
        Err(err) => {
            lines.push(format!("Codex discovery: 失败({err})"));
        }
    }

    // Keychain 可达性(只读探测)。
    match keychain.get_device_credential(DOCTOR_PROBE_ACCOUNT).await {
        Ok(_) => lines.push("keychain: 可达".to_string()),
        Err(err) => lines.push(format!("keychain: 不可用({err});Bridge 将保持未绑定态")),
    }

    // 绑定状态。
    if let Some(store) = store {
        let binding = store.get_binding().await?;
        if binding.device_id.is_empty() || binding.status == BindingStatus::Unbound {
            lines.push("绑定: 未绑定".to_string());
        } else {
            lines.push(format!(
                "绑定: {:?} device_id={}",
                binding.status, binding.device_id
            ));
        }
        if !binding.relay_url.is_empty() {
            lines.push(format!("Relay: {}", binding.relay_url));
        }
    }

    Ok(lines.join("\n"))
}

// ---------------------------------------------------------------------------
// bridge pair / unpair
// ---------------------------------------------------------------------------

/// 配对(§21 Bridge 发起):PairingClient 分步执行 —— register 后立即展示
/// 短码与二维码数据,轮询期间打印等待提示;批准后凭据先入 Keychain 再落绑定。
async fn cmd_pair(cfg: BridgeConfig, keychain: Arc<dyn KeychainStore>) -> anyhow::Result<()> {
    let relay_base = cfg
        .relay_url
        .clone()
        .context("缺少 Relay 基地址:使用 --relay-url 或环境变量 AGENT_CONSOLE_RELAY_URL")?;
    let store = LocalStore::open(&cfg.data_dir)
        .await
        .context("打开本地库失败")?;
    let device_name =
        std::env::var(ENV_DEVICE_NAME).unwrap_or_else(|_| "Agent Console Bridge".to_string());
    let client = bridge::pairing::PairingClient::new(&relay_base)?;
    let registration = client
        .register(&device_name, env!("CARGO_PKG_VERSION"))
        .await?;
    println!(
        "{}",
        bridge::pairing::short_code_banner(&registration.short_code)
    );
    println!("二维码数据: {}", registration.qr_data);
    println!(
        "在已登录浏览器的「设备配对」页输入短码或扫描二维码并批准…(5 分钟内有效,可 Ctrl+C 取消)"
    );
    let (device_id, credential) = client
        .wait_approval(&registration, bridge::pairing::PairingOptions::default())
        .await?;
    bridge::pairing::bind(keychain, &store, client.origin(), &device_id, &credential).await?;
    println!("配对完成:device 已绑定({device_id})");
    Ok(())
}

async fn cmd_unpair(
    cfg: BridgeConfig,
    keychain: Arc<dyn KeychainStore>,
    confirm: bool,
) -> anyhow::Result<()> {
    if !confirm {
        bail!("unpair 需要显式确认:追加 --confirm");
    }
    let store = LocalStore::open(&cfg.data_dir)
        .await
        .context("打开本地库失败")?;
    let binding = store.get_binding().await?;
    if !binding.device_id.is_empty() {
        keychain
            .delete_device_credential(&binding.device_id)
            .await
            .context("删除 Keychain 凭据失败")?;
    }
    store.clear_binding().await?;
    println!("已清除本机凭据与绑定");
    Ok(())
}

// ---------------------------------------------------------------------------
// bridge workspace
// ---------------------------------------------------------------------------

async fn open_store(cfg: &BridgeConfig) -> anyhow::Result<LocalStore> {
    LocalStore::open(&cfg.data_dir)
        .await
        .context("打开本地库失败")
}

async fn cmd_workspace_list(cfg: BridgeConfig) -> anyhow::Result<()> {
    let store = open_store(&cfg).await?;
    for ws in store.list_workspaces().await? {
        println!(
            "{}\t{}\t{}",
            ws.root_path.display(),
            ws.display_name,
            ws.authorized_at
        );
    }
    Ok(())
}

async fn cmd_workspace_authorize(
    cfg: BridgeConfig,
    path: String,
    name: Option<String>,
) -> anyhow::Result<()> {
    let root = PathBuf::from(&path);
    let canonical = root
        .canonicalize()
        .with_context(|| format!("工作区路径不存在或不可访问: {path}"))?;
    let display_name = name.unwrap_or_else(|| {
        canonical
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.clone())
    });
    let store = open_store(&cfg).await?;
    store.authorize_workspace(&canonical, &display_name).await?;
    println!("已授权: {}", canonical.display());
    Ok(())
}

async fn cmd_workspace_revoke(cfg: BridgeConfig, path: String) -> anyhow::Result<()> {
    let root = PathBuf::from(&path);
    let canonical = root.canonicalize().unwrap_or(root);
    let store = open_store(&cfg).await?;
    if store.revoke_workspace(&canonical).await? {
        println!("已撤销: {}", canonical.display());
    } else {
        println!("未找到该工作区授权");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// bridge inspect(只读;数据来自 adapter;不输出会话正文与绝对路径)
// ---------------------------------------------------------------------------

async fn cmd_inspect_sessions(cfg: BridgeConfig) -> anyhow::Result<()> {
    let store = open_store(&cfg).await?;
    let device_id = inspect_device_id(&store).await?;
    let adapter = CodexAdapter::connect(adapter_config(&cfg, &device_id, None))
        .await
        .map_err(|err| anyhow::anyhow!("Codex adapter 连接失败: {err}"))?;
    let page = adapter
        .list_sessions(None, 50, false)
        .await
        .map_err(|err| anyhow::anyhow!("会话列表读取失败: {err}"))?;
    println!("sessions: {}", page.sessions.len());
    for session in page.sessions {
        // 只输出稳定维度(§25.3):ID/阶段/计数/队列/结果;标题与正文不输出。
        println!(
            "{}\tphase={:?}\tattention={}\tqueue={:?}\tlast={:?}",
            session.session_key.native_session_id,
            session.active_turn_phase,
            session.pending_attention_count,
            session.queue_state,
            session.last_turn_outcome,
        );
    }
    Ok(())
}

async fn cmd_inspect_session(cfg: BridgeConfig, id: String) -> anyhow::Result<()> {
    let store = open_store(&cfg).await?;
    let device_id = inspect_device_id(&store).await?;
    let adapter = CodexAdapter::connect(adapter_config(&cfg, &device_id, None))
        .await
        .map_err(|err| anyhow::anyhow!("Codex adapter 连接失败: {err}"))?;
    let snapshot = adapter
        .runtime_snapshot(&SessionKey::codex(device_id, id))
        .await
        .map_err(|err| anyhow::anyhow!("运行快照读取失败: {err}"))?;
    // 只输出稳定维度;当前 turn 用 ID,不输出任何输出/消息正文。
    match &snapshot.current_turn {
        Some(turn) => println!("current_turn: {}\tphase={:?}", turn.turn, turn.phase),
        None => println!("current_turn: -\tphase=IDLE"),
    }
    println!(
        "revision={}\tquestions={}\tapprovals={}\trunning_cmds={}\tbackground={}\
         \tqueue={:?}\toutput_cursors={}",
        snapshot.runtime_revision,
        snapshot.pending_questions.len(),
        snapshot.pending_approvals.len(),
        snapshot.running_commands.len(),
        snapshot.background_command_count,
        snapshot.queue.state,
        snapshot.recent_output_cursors.len(),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 单元测试:CLI 解析 + doctor 输出(bin target 单测,tests/ 目录无法导入 bin)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bridge::capabilities::ProbeResult;
    use bridge::domain::Operation;
    use bridge::keychain::InMemoryKeychainStore;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("bridge").chain(args.iter().copied()))
    }

    /// doctor/inspect 单测用的隔离配置(codex_home 指向空临时目录,
    /// 绝不读取真实 ~/.codex)。
    fn isolated_cfg(dir: &std::path::Path) -> BridgeConfig {
        let mut cfg = BridgeConfig::from_env().with_data_dir(dir.join("data"));
        cfg.codex_home = dir.join("codex-home");
        cfg.ipc_socket_path = None;
        cfg
    }

    #[test]
    fn parses_run_with_relay_override() {
        let cli = parse(&["run", "--relay-url", "wss://r.example.com"]).unwrap();
        match cli.command {
            Command::Run { relay_url } => {
                assert_eq!(relay_url.as_deref(), Some("wss://r.example.com"))
            }
            other => panic!("unexpected: {other:?}"),
        }
        let cli = parse(&["run"]).unwrap();
        assert!(matches!(cli.command, Command::Run { relay_url: None }));
    }

    #[test]
    fn parses_doctor_pair_unpair() {
        assert!(matches!(
            parse(&["doctor"]).unwrap().command,
            Command::Doctor
        ));
        // pair:--relay-url 可选(缺省回退 AGENT_CONSOLE_RELAY_URL);不再接受 --code。
        match parse(&["pair", "--relay-url", "http://127.0.0.1:8081"])
            .unwrap()
            .command
        {
            Command::Pair { relay_url } => {
                assert_eq!(relay_url.as_deref(), Some("http://127.0.0.1:8081"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        match parse(&["pair"]).unwrap().command {
            Command::Pair { relay_url } => assert!(relay_url.is_none()),
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            parse(&["pair", "--code", "012345"]).is_err(),
            "pair 不再接受 --code(§21:Bridge 发起配对)"
        );
        // unpair 缺 --confirm 时仍可解析(运行时拒绝),语义由 cmd_unpair 保证。
        match parse(&["unpair"]).unwrap().command {
            Command::Unpair { confirm } => assert!(!confirm),
            other => panic!("unexpected: {other:?}"),
        }
        match parse(&["unpair", "--confirm"]).unwrap().command {
            Command::Unpair { confirm } => assert!(confirm),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_workspace_and_inspect() {
        let cli = parse(&[
            "workspace",
            "authorize",
            "--path",
            "/tmp/ws",
            "--name",
            "demo",
        ])
        .unwrap();
        match cli.command {
            Command::Workspace {
                action: WorkspaceAction::Authorize { path, name },
            } => {
                assert_eq!(path, "/tmp/ws");
                assert_eq!(name.as_deref(), Some("demo"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            parse(&["workspace", "authorize"]).is_err(),
            "缺 --path 必须报错"
        );
        assert!(matches!(
            parse(&["workspace", "list"]).unwrap().command,
            Command::Workspace {
                action: WorkspaceAction::List
            }
        ));
        assert!(matches!(
            parse(&["inspect", "sessions"]).unwrap().command,
            Command::Inspect {
                action: InspectAction::Sessions
            }
        ));
        match parse(&["inspect", "session", "native-1"]).unwrap().command {
            Command::Inspect {
                action: InspectAction::Session { id },
            } => assert_eq!(id, "native-1"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn doctor_report_on_tempdir_is_complete_and_secret_free() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = isolated_cfg(dir.path());
        let keychain = InMemoryKeychainStore::new();
        let secret = "pairing-secret-do-not-print";
        keychain
            .set_device_credential("device-doctor-test", secret)
            .await
            .unwrap();

        let report = cmd_doctor(cfg, Arc::new(keychain)).await.unwrap();
        assert!(report.contains("bridge 版本:"));
        assert!(report.contains("keychain: 可达"));
        assert!(report.contains("绑定: 未绑定"));
        assert!(report.contains("Codex 版本:"), "discovery 报告含版本行");
        assert!(report.contains("capability 矩阵:"), "含能力矩阵摘要");
        assert!(report.contains("ipc_socket: 未配置"));
        // 路径允许出现在 doctor stdout(§25.3 只限制日志)。
        assert!(report.contains(dir.path().to_str().unwrap()));
        // 凭据绝不出现。
        assert!(!report.contains(secret));
    }

    #[tokio::test]
    async fn doctor_report_shows_paired_binding_without_secret() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = isolated_cfg(dir.path());
        let store = LocalStore::open(&cfg.data_dir).await.unwrap();
        store
            .set_binding(
                "wss://relay.example.com",
                "device-doctor-test",
                BindingStatus::Paired,
            )
            .await
            .unwrap();
        let keychain = InMemoryKeychainStore::new();
        let secret = "another-secret";
        keychain
            .set_device_credential("device-doctor-test", secret)
            .await
            .unwrap();

        let report = cmd_doctor(cfg, Arc::new(keychain)).await.unwrap();
        assert!(report.contains("device_id=device-doctor-test"));
        assert!(report.contains("wss://relay.example.com"));
        assert!(!report.contains(secret));
    }

    #[test]
    fn pair_rejects_code_and_qr_payload_is_derived() {
        // 二维码数据由 RelayUrls 唯一派生(完整矩阵见 config 模块测试)。
        let urls = bridge::config::RelayUrls::parse("wss://toolbox.example.com").unwrap();
        assert_eq!(
            urls.pairing_deep_link("012345"),
            "https://toolbox.example.com/agent-console/pair?code=012345"
        );
        // 短码大字号横幅在 pair stdout 展示。
        assert!(bridge::pairing::short_code_banner("012345").contains("0 1 2 3 4 5"));
    }

    #[tokio::test]
    async fn doctor_reports_derived_bridge_ws_and_config_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = isolated_cfg(dir.path());
        cfg.relay_url = Some("https://toolbox.example.com".to_string());
        let report = cmd_doctor(cfg, Arc::new(InMemoryKeychainStore::new()))
            .await
            .unwrap();
        assert!(report.contains("Relay 基地址: https://toolbox.example.com"));
        assert!(report.contains("Bridge WS: wss://toolbox.example.com/agent-console/bridge/ws"));

        // 完整 WebSocket 地址 → 配置错误 + 专门提示,不派生任何地址。
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = isolated_cfg(dir.path());
        cfg.relay_url = Some("wss://toolbox.example.com/agent-console/ws".to_string());
        let report = cmd_doctor(cfg, Arc::new(InMemoryKeychainStore::new()))
            .await
            .unwrap();
        assert!(report.contains("配置错误"), "actual: {report}");
        assert!(report.contains("完整 WebSocket 地址"), "actual: {report}");
    }

    #[tokio::test]
    async fn detect_power_source_parses_pmset_output_or_missing_tool() {
        // 无 shell;固定 argv。pmset 不存在(非 macOS CI)时必须返回 None,
        // 不 panic、不改变当前值。
        let source = detect_power_source().await;
        if let Some(source) = source {
            assert!(matches!(source, PowerSource::Ac | PowerSource::Battery));
        }
    }

    #[test]
    fn write_probes_unverified_version_stays_all_not_probed() {
        // 未探测到 / 未知版本 / 仅只读协议兼容版本:全部 NotProbed
        // (§5 只读降级,不凭白名单开启写;0.153.0-alpha.5 无写验证证据)。
        for version in [
            None,
            Some(""),
            Some("codex-cli 0.999.0"),
            Some("codex-cli 0.153.0-alpha.4"),
            Some("codex-cli 0.153.0-alpha.5"),
        ] {
            let probes = bridge::capabilities::verified_write_probes(version);
            assert_eq!(probes.start_turn, ProbeResult::NotProbed);
            assert_eq!(probes.steer_turn, ProbeResult::NotProbed);
            assert_eq!(probes.interrupt_turn, ProbeResult::NotProbed);
            assert_eq!(probes.answer_question, ProbeResult::NotProbed);
            assert_eq!(probes.update_settings, ProbeResult::NotProbed);
        }
    }

    #[test]
    fn write_probes_verified_version_injects_whitelist_only() {
        // 逐版本写能力矩阵:仅 §9 真机 VERIFIED 的操作注入 Passed(§9.2);
        // 设置写能力产品侧关闭(§9),保持 NotProbed。
        let probes = bridge::capabilities::verified_write_probes(Some("codex-cli 0.153.1"));
        assert_eq!(
            bridge::capabilities::verified_write_probes(Some("codex-cli 0.153.4")),
            probes
        );
        assert_eq!(probes.start_turn, ProbeResult::Passed);
        assert_eq!(probes.interrupt_turn, ProbeResult::Passed);
        assert_eq!(probes.steer_turn, ProbeResult::Passed);
        assert_eq!(probes.update_settings, ProbeResult::NotProbed);
        assert_eq!(probes.answer_question, ProbeResult::NotProbed);

        // 能力矩阵:白名单操作开启;无 IPC 方法的操作保持 UNSUPPORTED。
        let input = bridge::capabilities::CapabilityProbeInput {
            version_report: Some("codex-cli 0.153.1".to_string()),
            version_verified: true,
            ipc_handshake_ok: true,
            owner_confirmed: true,
            write_methods: probes,
            ..Default::default()
        };
        let caps = bridge::capabilities::probe(&input);
        assert!(caps.supports(Operation::StartTurn));
        assert!(caps.supports(Operation::InterruptTurn));
        assert!(caps.supports(Operation::SteerTurn));
        assert!(!caps.supports(Operation::UpdateSettings));
        assert!(!caps.supports(Operation::AnswerQuestion));
        assert!(!caps.supports(Operation::CreateSession));
        assert!(!caps.supports(Operation::RenameSession));
        assert!(!caps.supports(Operation::ArchiveSession));
        assert!(!caps.supports(Operation::UnarchiveSession));
        assert!(!caps.supports(Operation::ForkSession));
        assert!(!caps.supports(Operation::StopBackgroundCommand));
        assert!(!caps.supports(Operation::StopAllBackgroundCommands));
    }

    #[tokio::test]
    async fn run_without_binding_reports_pair_first() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BridgeConfig::from_env()
            .with_data_dir(dir.path().join("data"))
            .with_relay_url("ws://127.0.0.1:9");
        let err = cmd_run(cfg, Arc::new(InMemoryKeychainStore::new()))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("未绑定"), "actual: {err}");
    }

    #[tokio::test]
    async fn run_without_relay_url_reports_config_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BridgeConfig::from_env().with_data_dir(dir.path().join("data"));
        cfg.relay_url = None;
        let err = cmd_run(cfg, Arc::new(InMemoryKeychainStore::new()))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Relay 地址"), "actual: {err}");
    }

    #[tokio::test]
    async fn run_with_full_ws_address_reports_config_error() {
        // 历史验收阻塞:完整 WebSocket 地址必须在校验阶段报错,不被原样当连接目标。
        let dir = tempfile::tempdir().unwrap();
        let cfg = BridgeConfig::from_env()
            .with_data_dir(dir.path().join("data"))
            .with_relay_url("wss://toolbox.example.com/agent-console/ws");
        let err = cmd_run(cfg, Arc::new(InMemoryKeychainStore::new()))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("完整 WebSocket 地址"), "actual: {err}");
        assert!(err.contains("公网基地址"), "actual: {err}");
    }
}
