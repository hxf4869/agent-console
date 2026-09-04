//! 真实网关端到端链(e2e_gateway)专用支撑。
//!
//! 组件编排(全部测试内拉起、Drop/teardown 清理):
//! - PostgreSQL:pgvector/pgvector:pg16 单实例,两个库
//!   `dev_toolbox_test`(dev-toolbox 业务库,迁移用其 cmd/migrate 应用)与
//!   `agent_console_test`(relay 独立库 + 独立用户,§18);
//! - dev-toolbox API:真实 Go 进程(仓库预构建二进制);owner 用其
//!   `dev-toolbox admin init-owner` CLI 创建;浏览器走真实
//!   `POST /api/v1/auth/login` + TOTP 注册(confirm),不绕过任何验证逻辑;
//! - Caddy:挂载 dev-toolbox/deploy/Caddyfile **原文件**(只读,零字节修改),
//!   仅允许两类适配且全部经 env 完成:
//!   (a) 站点地址 `{$APP_DOMAIN}` 注入本地 http origin(显式 http scheme
//!       自动禁用自动 HTTPS);
//!   (b) 上游解析:自定义 docker 网络放两个 socat 容器,网络别名
//!       `api`(→ 宿主 dev-toolbox 端口)与 `agent-console-relay`
//!       (→ 宿主 relay 端口),Caddyfile 原样的 `api:8080`、
//!       `agent-console-relay:8081` 原样命中;
//!   另注入 `AGENT_CONSOLE_INTERNAL_TOKEN`、`CADDY_TRUSTED_PROXY_CIDRS`
//!   (与路由语义无关);启动前先 `caddy validate`。
//! - Relay / Bridge:进程内(`relay::serve` + `BridgeRuntime`),复用
//!   `support`/`e2e_support` 的模式。
//!
//! 环境要求:docker(OrbStack)、go(默认 /opt/homebrew/bin/go,可用
//! `AC_E2E_GO` 覆盖)、dev-toolbox 仓库路径(`AC_E2E_DEVTOOLBOX` 可覆盖)。

#![allow(dead_code)]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::support::{free_port, Ws};

/// docker 资源统一前缀(启动前防御性清理残留)。
pub const RES_PREFIX: &str = "ac-gw-e2e";

fn debug_enabled() -> bool {
    std::env::var("AC_E2E_DEBUG").is_ok()
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker 命令执行失败")
}

fn docker_ok(args: &[&str], what: &str) {
    let out = docker(args);
    assert!(
        out.status.success(),
        "{what} 失败: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// 启动前清理本套件残留容器/网络(测试进程异常退出的兜底)。
pub fn cleanup_stale() {
    let out = docker(&["ps", "-aq", "--filter", &format!("name={RES_PREFIX}-")]);
    if let Ok(ids) = String::from_utf8(out.stdout) {
        let ids: Vec<&str> = ids.split_whitespace().collect();
        if !ids.is_empty() {
            let _ = docker(&[&["rm", "-f"], ids.as_slice()].concat());
        }
    }
    let out = docker(&["network", "ls", "--format", "{{.Name}}"]);
    if let Ok(names) = String::from_utf8(out.stdout) {
        for name in names.split_whitespace() {
            if name.starts_with(RES_PREFIX) {
                let _ = docker(&["network", "rm", name]);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL(pgvector/pgvector:pg16;两库 + relay 独立用户)
// ---------------------------------------------------------------------------

pub struct GatewayPostgres {
    pub name: String,
    pub port: u16,
}

impl GatewayPostgres {
    pub async fn start() -> GatewayPostgres {
        cleanup_stale();
        let name = format!("{RES_PREFIX}-pg-{}", uuid::Uuid::new_v4().simple());
        let port = free_port();
        docker_ok(
            &[
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "-e",
                "POSTGRES_PASSWORD=test",
                "-e",
                "POSTGRES_USER=test",
                "-p",
                &format!("127.0.0.1:{port}:5432"),
                "pgvector/pgvector:pg16",
            ],
            "docker run pgvector/pgvector:pg16",
        );
        let admin = format!("postgres://test:test@127.0.0.1:{port}/postgres?sslmode=disable");
        for _ in 0..240 {
            if let Ok(pool) = sqlx::postgres::PgPool::connect(&admin).await {
                // relay 独立用户(§18:不与 dev-toolbox 业务共用用户)。
                sqlx::query("CREATE ROLE agent_console_gw LOGIN PASSWORD 'gw-pw'")
                    .execute(&pool)
                    .await
                    .expect("create relay role");
                sqlx::query("CREATE DATABASE dev_toolbox_test")
                    .execute(&pool)
                    .await
                    .expect("create dev_toolbox_test");
                sqlx::query("CREATE DATABASE agent_console_test OWNER agent_console_gw")
                    .execute(&pool)
                    .await
                    .expect("create agent_console_test");
                pool.close().await;
                return GatewayPostgres { name, port };
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let _ = docker(&["rm", "-f", &name]);
        panic!("pgvector 容器未按时就绪");
    }

    /// dev-toolbox 业务库连接串(迁移/服务/CLI 共用)。
    pub fn toolbox_db_url(&self) -> String {
        format!(
            "postgres://test:test@127.0.0.1:{}/dev_toolbox_test?sslmode=disable",
            self.port
        )
    }

    /// relay 独立库连接串(独立用户,§18)。
    pub fn relay_db_url(&self) -> String {
        format!(
            "postgres://agent_console_gw:gw-pw@127.0.0.1:{}/agent_console_test?sslmode=disable",
            self.port
        )
    }
}

impl Drop for GatewayPostgres {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

// ---------------------------------------------------------------------------
// dev-toolbox Go 二进制构建 + CLI(migrate / init-owner)
// ---------------------------------------------------------------------------

/// dev-toolbox 仓库根(默认取 agent-console 的同级目录，
/// `AC_E2E_DEVTOOLBOX` 覆盖)。
pub fn devtoolbox_root() -> PathBuf {
    std::env::var("AC_E2E_DEVTOOLBOX")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../..")
                .join("dev-toolbox")
        })
}

fn go_bin() -> String {
    std::env::var("AC_E2E_GO").unwrap_or_else(|_| "/opt/homebrew/bin/go".to_string())
}

pub struct GoBinaries {
    _dir: tempfile::TempDir,
    pub server: PathBuf,
    pub migrate: PathBuf,
    pub cli: PathBuf,
}

/// 构建真实 dev-toolbox 二进制(cmd/server、cmd/migrate、cmd/dev-toolbox)。
pub fn build_go_binaries() -> GoBinaries {
    let dir = tempfile::tempdir().expect("go build tempdir");
    let api_dir = devtoolbox_root().join("apps/api");
    let out = Command::new(go_bin())
        .current_dir(&api_dir)
        .env_remove("GOFLAGS")
        .args(["build", "-o", dir.path().to_str().unwrap()])
        .args(["./cmd/server", "./cmd/migrate", "./cmd/dev-toolbox"])
        .output()
        .expect("go build 启动失败");
    assert!(
        out.status.success(),
        "go build dev-toolbox 失败: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    GoBinaries {
        server: dir.path().join("server"),
        migrate: dir.path().join("migrate"),
        cli: dir.path().join("dev-toolbox"),
        _dir: dir,
    }
}

/// 应用 dev-toolbox 迁移(cmd/migrate,tern)。
pub fn run_migrations(bin: &Path, db_url: &str) {
    let out = Command::new(bin)
        .env("DATABASE_URL", db_url)
        .env(
            "MIGRATIONS_PATH",
            devtoolbox_root().join("apps/api/migrations"),
        )
        .output()
        .expect("启动 cmd/migrate 失败");
    assert!(
        out.status.success(),
        "dev-toolbox 迁移失败: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// 创建 owner(cmd/dev-toolbox admin init-owner;密码走 --password-file)。
pub fn init_owner(bin: &Path, db_url: &str, username: &str, password: &str, work: &Path) {
    let password_file = work.join("owner-password.txt");
    std::fs::write(&password_file, format!("{password}\n")).expect("write password file");
    let out = Command::new(bin)
        .env("DATABASE_URL", db_url)
        .args([
            "admin",
            "init-owner",
            "--username",
            username,
            "--password-file",
            password_file.to_str().unwrap(),
        ])
        .output()
        .expect("启动 init-owner 失败");
    assert!(
        out.status.success(),
        "init-owner 失败: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ---------------------------------------------------------------------------
// dev-toolbox API 服务器(真实进程)
// ---------------------------------------------------------------------------

pub struct ToolboxServer {
    child: Child,
    pub port: u16,
    pub base: String,
}

impl ToolboxServer {
    /// 启动真实 server 并等待 /health/ready。`app_origin` 必须等于 Caddy
    /// 公网 origin(登录写校验 OriginOnly)。
    pub async fn start(
        bin: &Path,
        db_url: &str,
        port: u16,
        app_origin: &str,
        internal_token: &str,
        work: &Path,
    ) -> ToolboxServer {
        let master_key_file = work.join("master.key");
        std::fs::write(
            &master_key_file,
            base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
        )
        .expect("write master key");
        let mut cmd = Command::new(bin);
        cmd.env("DATABASE_URL", db_url)
            .env("HTTP_ADDRESS", format!("127.0.0.1:{port}"))
            .env("APP_ORIGIN", app_origin)
            .env("APP_MASTER_KEY_FILE", &master_key_file)
            // ≥32 字符的测试内部令牌;真实校验逻辑不变。
            .env(
                "INTERNAL_SERVICE_TOKEN",
                format!("{internal_token}-tb-internal"),
            )
            .env("AGENT_CONSOLE_INTERNAL_TOKEN", internal_token)
            .env(
                "ARTIFACT_SCHEMA_PATH",
                devtoolbox_root().join("docs/contracts/ai-artifact.schema.json"),
            )
            .env("ATTACHMENT_ROOT", work.join("attachments"))
            .env("OPERATIONS_STATUS_ROOT", work.join("operations"));
        if debug_enabled() {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
        let child = cmd.spawn().expect("启动 dev-toolbox server 失败");
        let server = ToolboxServer {
            child,
            port,
            base: format!("http://127.0.0.1:{port}"),
        };
        server.wait_ready().await;
        server
    }

    async fn wait_ready(&self) {
        // 真实 readiness 同时验证数据库可达且 dev-toolbox 的 schema 版本
        // 与最新迁移一致；不能只以进程存活替代可接流量。
        let client = reqwest::Client::new();
        for _ in 0..150 {
            if let Ok(resp) = client
                .get(format!("{}/health/ready", self.base))
                .send()
                .await
            {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("dev-toolbox server 未按时就绪");
    }
}

impl Drop for ToolboxServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Caddy 网关(原 Caddyfile 只读挂载 + env 适配;socat 上游别名)
// ---------------------------------------------------------------------------

pub struct Gateway {
    pub network: String,
    containers: Vec<String>,
    pub port: u16,
    /// Caddy 公网 origin(http://127.0.0.1:<port>;Bridge
    /// AGENT_CONSOLE_RELAY_URL 与浏览器请求基地址)。
    pub base: String,
}

/// Caddyfile 路由合同锚点(证明挂载的是原文件、路由语义未被改)。
pub const CADDYFILE_ANCHORS: &[&str] = &[
    "handle /agent-console/api/*",
    "forward_auth api:8080",
    "uri /internal/agent-console/auth-sessions/verify",
    "handle /internal/*",
    "respond 404",
    "handle /agent-console/ws",
    "handle /agent-console/bridge/ws",
    "handle /agent-console/transfers/*",
    "handle /agent-console/*",
    "try_files {path} /agent-console/index.html",
    "{$AGENT_CONSOLE_RELAY_UPSTREAM:agent-console-relay:8081}",
    "reverse_proxy api:8080",
];

/// Caddyfile 顺序 bug 的上游修复校验。
///
/// 历史:dev-toolbox Caddyfile 曾在 `handle /agent-console/api/*` 块内放两行
/// 顶层 `request_header -X-Agent-Console-Session-Id / -Owner-Id`;Caddy 指令
/// 顺序表中 `forward_auth` 先于 `request_header` 执行,删除会落在
/// forward_auth `copy_headers` 重建身份头**之后**,把刚注入的身份头又删掉,
/// 导致 `/agent-console/api/*` 恒 401(e2e_gateway 实测,2026-09-04)。
/// 该 bug 已在 dev-toolbox/deploy/Caddyfile 上游修复(删除两行并加注释);
/// 本函数现在校验上游文件保持已修复状态:api 块内不得再出现顶层
/// `request_header -X-Agent-Console-*` 删除行。防伪造语义由 forward_auth
/// 保证:verify 401 时请求不会到达 relay;verify 2xx 时 copy_headers 的
/// set 无条件覆盖客户端伪造的同名头(断言 L 验证)。
fn patch_caddyfile(source: &str) -> String {
    let mut in_api_block = false;
    let mut block_indent = 0usize;
    for line in source.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        if in_api_block && trimmed == "}" && indent == block_indent {
            in_api_block = false;
        }
        if trimmed.starts_with("handle /agent-console/api/*") {
            in_api_block = true;
            block_indent = indent;
        }
        let is_bug_line = in_api_block
            && (trimmed == "request_header -X-Agent-Console-Session-Id"
                || trimmed == "request_header -X-Agent-Console-Owner-Id");
        assert!(
            !is_bug_line,
            "dev-toolbox Caddyfile api 块重现了顺序 bug 行(request_header -X-Agent-Console-*):\
             它会在 forward_auth copy_headers 之后删除身份头,导致 /agent-console/api/* 恒 401"
        );
    }
    // 文件保持原样(仅行尾规范化),路由 stanzas 零改动。
    let mut normalized = source.to_owned();
    if !normalized.ends_with('\n') {
        normalized.push('\n');
    }
    normalized
}

impl Gateway {
    /// 拉起网关:自定义网络 + 两个 socat 上游别名 + Caddy(validate 先行)。
    ///
    /// Caddyfile 适配共两类,均有注释/锚点证明:
    /// (a) 站点地址与令牌走 env(`APP_DOMAIN`、`AGENT_CONSOLE_INTERNAL_TOKEN`、
    ///     `CADDY_TRUSTED_PROXY_CIDRS`),Caddyfile 占位符原样生效;
    /// (b) 上游解析:网络别名 `api`/`agent-console-relay` 命中原样的
    ///     `api:8080` 与 `{$AGENT_CONSOLE_RELAY_UPSTREAM:agent-console-relay:8081}`;
    /// 另经 `patch_caddyfile` 校验上游文件保持已修复状态(api 块无顶层
    /// `request_header -X-Agent-Console-*` 删除行),文件内容零改动,
    /// 全部路由 stanzas(handle/forward_auth/rewrite/copy_headers)原样挂载。
    ///
    /// - `caddyfile`:dev-toolbox/deploy/Caddyfile 原路径(只读参照与锚点校验);
    /// - `api_port`/`relay_port`:宿主上真实 dev-toolbox / relay 监听端口;
    /// - `site_port`:Caddy 公网端口(站点地址 `http://127.0.0.1:<site_port>`)。
    pub async fn start(
        caddyfile: &Path,
        api_port: u16,
        relay_port: u16,
        site_port: u16,
        internal_token: &str,
    ) -> Gateway {
        let source = std::fs::read_to_string(caddyfile).expect("读取 Caddyfile");
        for anchor in CADDYFILE_ANCHORS {
            assert!(
                source.contains(anchor),
                "Caddyfile 缺少合同锚点 {anchor:?};挂载文件不是原始网关配置"
            );
        }
        // 校验上游已修复顺序 bug;文件内容零改动(仅行尾规范化)。
        let patched = patch_caddyfile(&source);
        let patched_dir = tempfile::tempdir().expect("caddyfile tempdir");
        let patched_path = patched_dir.path().join("Caddyfile");
        std::fs::write(&patched_path, &patched).expect("write patched caddyfile");
        // 副本(含所属 tempdir)须在 Gateway 存活期间有效;测试结束由进程退出回收。
        std::mem::forget(patched_dir);
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..12];
        let network = format!("{RES_PREFIX}-net-{suffix}");
        let socat_api = format!("{RES_PREFIX}-socat-api-{suffix}");
        let socat_relay = format!("{RES_PREFIX}-socat-relay-{suffix}");
        let caddy = format!("{RES_PREFIX}-caddy-{suffix}");

        docker_ok(&["network", "create", &network], "docker network create");
        // socat 别名 `api`:8080 → 宿主 dev-toolbox(OrbStack 的
        // host.docker.internal 可达宿主 127.0.0.1 绑定端口)。
        docker_ok(
            &[
                "run",
                "-d",
                "--rm",
                "--name",
                &socat_api,
                "--network",
                &network,
                "--network-alias",
                "api",
                "alpine/socat:1.8.0.0",
                "tcp-listen:8080,fork,reuseaddr",
                &format!("tcp:host.docker.internal:{api_port}"),
            ],
            "启动 socat api 上游",
        );
        // socat 别名 `agent-console-relay`:8081 → 宿主 relay。
        docker_ok(
            &[
                "run",
                "-d",
                "--rm",
                "--name",
                &socat_relay,
                "--network",
                &network,
                "--network-alias",
                "agent-console-relay",
                "alpine/socat:1.8.0.0",
                "tcp-listen:8081,fork,reuseaddr",
                &format!("tcp:host.docker.internal:{relay_port}"),
            ],
            "启动 socat relay 上游",
        );

        // 适配 (a):仅 env 注入,不改文件。
        let envs: Vec<(&str, String)> = vec![
            ("APP_DOMAIN", format!("http://127.0.0.1:{site_port}")),
            ("AGENT_CONSOLE_INTERNAL_TOKEN", internal_token.to_string()),
            (
                "CADDY_TRUSTED_PROXY_CIDRS",
                "127.0.0.1/8 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 ::1/128".to_string(),
            ),
        ];
        let caddyfile_arg = format!("{}:/etc/caddy/Caddyfile:ro", patched_path.display());
        let mut validate = Command::new("docker");
        validate.args(["run", "--rm", "-v", &caddyfile_arg]);
        for (k, v) in &envs {
            validate.args(["-e", &format!("{k}={v}")]);
        }
        validate
            .args([
                "caddy:2",
                "caddy",
                "validate",
                "--config",
                "/etc/caddy/Caddyfile",
            ])
            .args(["--adapter", "caddyfile"]);
        let out = validate.output().expect("caddy validate 启动失败");
        assert!(
            out.status.success(),
            "caddy validate 失败: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        let mut run = Command::new("docker");
        run.args(["run", "-d", "--rm", "--name", &caddy, "--network", &network])
            .args([
                "-p",
                &format!("127.0.0.1:{site_port}:{site_port}"),
                "-v",
                &caddyfile_arg,
            ]);
        // 交互式浏览器烟测可把已构建的同域静态根只读挂到 /srv；常规
        // e2e_gateway 不设置该变量，仍只验证原 API/WS 网关合同。
        if let Ok(static_root) = std::env::var("AC_E2E_STATIC_ROOT") {
            let mount = format!("{static_root}:/srv:ro");
            run.args(["-v", &mount]);
        }
        for (k, v) in &envs {
            run.args(["-e", &format!("{k}={v}")]);
        }
        run.arg("caddy:2");
        let out = run.output().expect("caddy 启动失败");
        assert!(
            out.status.success(),
            "docker run caddy 失败: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        let gateway = Gateway {
            network,
            containers: vec![caddy, socat_api, socat_relay],
            port: site_port,
            base: format!("http://127.0.0.1:{site_port}"),
        };
        gateway.wait_ready().await;
        gateway
    }

    /// 就绪探测:GET /health/live 经 Caddy → socat → 真实 dev-toolbox,
    /// 同时验证 api 上游链路(live 不做 schema 版本断言,理由见上)。
    async fn wait_ready(&self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        for _ in 0..150 {
            if let Ok(resp) = client
                .get(format!("{}/health/live", self.base))
                .send()
                .await
            {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("Caddy 网关未按时就绪(/health/ready 不通)");
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        for name in &self.containers {
            let _ = docker(&["rm", "-f", name]);
        }
        let _ = docker(&["network", "rm", &self.network]);
    }
}

// ---------------------------------------------------------------------------
// TOTP(RFC 6238:SHA1/6 位/30s,与 dev-toolbox totp.go 参数一致)
// ---------------------------------------------------------------------------

/// RFC 4648 base32 解码(容忍大小写与填充;pquerna/otp secret 为无填充大写)。
fn base32_decode(input: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = Vec::new();
    let mut buffer = 0u64;
    let mut bits = 0u32;
    for ch in input.bytes() {
        if ch == b'=' || ch == b' ' {
            continue;
        }
        let upper = ch.to_ascii_uppercase();
        let Some(idx) = ALPHABET.iter().position(|&c| c == upper) else {
            panic!("TOTP secret 含非 base32 字符");
        };
        buffer = (buffer << 5) | idx as u64;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    out
}

/// 当前 TOTP 码(30s 窗口;dev-toolbox 校验 skew=1,本机时钟一致)。
pub fn totp_now(secret_base32: &str) -> String {
    let key = base32_decode(secret_base32);
    let counter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 30)
        .to_be_bytes();
    let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(&key).expect("hmac key");
    mac.update(&counter);
    let digest = mac.finalize().into_bytes();
    let offset = (digest[19] & 0x0f) as usize;
    let bin = u32::from_be_bytes([
        digest[offset],
        digest[offset + 1],
        digest[offset + 2],
        digest[offset + 3],
    ]) & 0x7fff_ffff;
    format!("{:06}", bin % 1_000_000)
}

// ---------------------------------------------------------------------------
// 浏览器会话:真实登录(Cookie+CSRF)与经网关的 HTTP 客户端
// ---------------------------------------------------------------------------

pub struct BrowserSession {
    pub base: String,
    /// Caddy 公网 origin(= dev-toolbox APP_ORIGIN;Origin 校验值)。
    pub origin: String,
    /// 会话 Cookie 头原值(`__Host-dev-toolbox-session=<token>`)。
    pub cookie: String,
    pub csrf: String,
    pub session_id: String,
    pub owner_id: String,
    http: reqwest::Client,
}

impl BrowserSession {
    /// 真实登录(断言 A):login → (首次)TOTP 注册 setup → 本地 RFC 6238
    /// 生成码 → confirm;(已注册)直接 mfa/verify。不绕过任何 dev-toolbox
    /// 验证逻辑。返回 (会话, TOTP secret) —— secret 来自服务端下发/调用方
    /// 传入,供第二次登录走真实 mfa/verify 流程。
    pub async fn login(base: &str, username: &str, password: &str) -> (BrowserSession, String) {
        Self::login_inner(base, username, password, None).await
    }

    /// 已注册 TOTP 后的第二次真实登录(mfa/verify 流程)。
    pub async fn login_again(
        base: &str,
        username: &str,
        password: &str,
        secret: &str,
    ) -> BrowserSession {
        Self::login_inner(base, username, password, Some(secret))
            .await
            .0
    }

    async fn login_inner(
        base: &str,
        username: &str,
        password: &str,
        existing_secret: Option<&str>,
    ) -> (BrowserSession, String) {
        let http = reqwest::Client::new();
        let origin = base.to_string();

        // 步骤 1:密码登录(OriginOnly 校验;202 + challengeId)。
        let resp = http
            .post(format!("{base}/api/v1/auth/login"))
            .header("Origin", &origin)
            .json(&serde_json::json!({"username": username, "password": password}))
            .send()
            .await
            .expect("login 请求失败");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::ACCEPTED,
            "密码登录应进入 MFA 挑战"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        let challenge_id = body["challengeId"]
            .as_str()
            .expect("challengeId")
            .to_string();
        let secret = match (body["status"].as_str(), existing_secret) {
            (Some("mfa_setup_required"), _) => {
                // 步骤 2(TOTP 注册):setup → 服务端生成密钥(只此一次下发)。
                let resp = http
                    .post(format!("{base}/api/v1/auth/totp/setup"))
                    .header("Origin", &origin)
                    .json(&serde_json::json!({"challengeId": challenge_id}))
                    .send()
                    .await
                    .expect("totp setup 请求失败");
                assert!(resp.status().is_success(), "totp setup 失败");
                let provisioning: serde_json::Value = resp.json().await.unwrap();
                let secret = provisioning["manualKey"]
                    .as_str()
                    .expect("manualKey")
                    .to_string();
                let code = totp_now(&secret);
                let resp = http
                    .post(format!("{base}/api/v1/auth/totp/confirm"))
                    .header("Origin", &origin)
                    .json(&serde_json::json!({
                        "challengeId": challenge_id,
                        "code": code,
                        "trustDevice": false,
                    }))
                    .send()
                    .await
                    .expect("totp confirm 请求失败");
                assert_eq!(
                    resp.status(),
                    reqwest::StatusCode::OK,
                    "TOTP confirm 失败(真实验证逻辑拒绝)"
                );
                (secret, resp)
            }
            (Some("mfa_required"), Some(secret)) => {
                // 已注册:直接 verify(真实 TOTP 校验)。
                let code = totp_now(secret);
                let resp = http
                    .post(format!("{base}/api/v1/auth/mfa/verify"))
                    .header("Origin", &origin)
                    .json(&serde_json::json!({
                        "challengeId": challenge_id,
                        "method": "totp",
                        "code": code,
                        "trustDevice": false,
                    }))
                    .send()
                    .await
                    .expect("mfa verify 请求失败");
                assert_eq!(
                    resp.status(),
                    reqwest::StatusCode::OK,
                    "mfa/verify 失败(真实验证逻辑拒绝)"
                );
                (secret.to_string(), resp)
            }
            (other, _) => panic!("登录挑战状态异常: {other:?}"),
        };
        let (secret, resp) = secret;
        let cookie_headers = resp.headers().clone();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "authenticated", "确认响应: {body}");
        let csrf = body["csrfToken"].as_str().expect("csrfToken").to_string();
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session.id")
            .to_string();
        let owner_id = body["owner"]["id"].as_str().expect("owner.id").to_string();

        // 从 Set-Cookie 提取会话 Cookie(名称固定)。
        let set_cookie = cookie_headers
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .find_map(|v| {
                let s = v.to_str().ok()?;
                s.starts_with("__Host-dev-toolbox-session=")
                    .then(|| s.to_string())
            })
            .expect("登录响应应设置会话 Cookie");
        let cookie = set_cookie
            .split(';')
            .next()
            .expect("Set-Cookie 首段")
            .to_string();
        assert!(
            cookie.len() > "__Host-dev-toolbox-session=".len(),
            "会话 Cookie 为空"
        );

        (
            BrowserSession {
                base: base.to_string(),
                origin,
                cookie,
                csrf,
                session_id,
                owner_id,
                http,
            },
            secret,
        )
    }

    fn identity(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::COOKIE, self.cookie.parse().unwrap());
        headers.insert(reqwest::header::ORIGIN, self.origin.parse().unwrap());
        headers
    }

    /// 经网关的 GET(Cookie + Origin)。
    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .get(format!("{}{path}", self.base))
            .headers(self.identity())
    }

    /// 经网关的写请求(Cookie + Origin + X-CSRF-Token)。
    pub fn post(&self, path: &str) -> reqwest::RequestBuilder {
        let mut headers = self.identity();
        headers.insert("X-CSRF-Token", self.csrf.parse().unwrap());
        self.http
            .post(format!("{}{path}", self.base))
            .headers(headers)
    }

    pub fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        let mut headers = self.identity();
        headers.insert("X-CSRF-Token", self.csrf.parse().unwrap());
        self.http
            .delete(format!("{}{path}", self.base))
            .headers(headers)
    }

    /// 断言 B:ticket 申请经网关成功且响应 no-store(§20.2)。
    pub async fn request_ws_ticket(&self) -> String {
        let resp = self
            .post("/api/v1/agent-console/ws-tickets")
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect("ws-tickets 请求失败");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "ticket 申请失败");
        assert_eq!(
            resp.headers()
                .get(reqwest::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store"),
            "ticket 响应必须 no-store(§20.2)"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        body["ticket"].as_str().expect("ticket").to_string()
    }
}

// ---------------------------------------------------------------------------
// 浏览器 WS(经网关;Sec-WebSocket-Protocol 携带 ticket)+ tracing 捕获
// ---------------------------------------------------------------------------

/// 以 `agent-console.v1` + `agent-console.ticket-<b64url>` subprotocol 经
/// 网关连接浏览器 WS 并完成握手;失败返回 HTTP 状态码(断言 C)。
pub async fn connect_browser_ws(base: &str, ticket: &str) -> Result<Ws, u16> {
    let subprotocol = format!(
        "{}, agent-console.ticket-{}",
        "agent-console.v1",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ticket.as_bytes())
    );
    let ws_base = base.replacen("http://", "ws://", 1);
    let mut request = format!("{ws_base}/agent-console/ws")
        .into_client_request()
        .expect("request");
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", subprotocol.parse().unwrap());
    match tokio_tungstenite::connect_async(request).await {
        Ok((ws, _)) => {
            let mut ws = ws;
            let hello = crate::support::handshake_browser(&mut ws).await;
            assert!(matches!(
                hello.payload,
                Some(agent_console_protocol::v1::envelope::Payload::ServerHello(
                    _
                ))
            ));
            Ok(ws)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("browser connect failed: {e}"),
    }
}

/// Bridge 设备 WS 经网关连接(Bearer 设备凭据);返回原始流(供撤销关闭
/// 断言);握手失败返回 HTTP 状态码。
pub async fn connect_bridge_ws(base: &str, device_id: &str, credential: &str) -> Result<Ws, u16> {
    let ws_base = base.replacen("http://", "ws://", 1);
    let mut request = format!("{ws_base}/agent-console/bridge/ws")
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    match tokio_tungstenite::connect_async(request).await {
        Ok((ws, _)) => {
            let mut ws = ws;
            let hello = crate::support::handshake_bridge(&mut ws, device_id).await;
            assert!(matches!(
                hello.payload,
                Some(agent_console_protocol::v1::envelope::Payload::ServerHello(
                    _
                ))
            ));
            Ok(ws)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("bridge connect failed: {e}"),
    }
}

// ---------------------------------------------------------------------------
// tracing 全量捕获(进程内 relay + bridge;§25.3 断言用)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedBufferWriter {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl std::io::Write for SharedBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for SharedBufferWriter {
    type Writer = SharedBufferWriter;
    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

pub struct LogCapture {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl LogCapture {
    pub fn init() -> LogCapture {
        static INIT: std::sync::Once = std::sync::Once::new();
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        INIT.call_once(|| {
            let filter = tracing_subscriber::EnvFilter::new("warn,relay=debug,bridge=debug");
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(SharedBufferWriter {
                    buffer: buffer.clone(),
                })
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        LogCapture { buffer }
    }

    pub fn assert_free_of(&self, secrets: &[&str]) {
        let captured = String::from_utf8_lossy(&self.buffer.lock().unwrap()).into_owned();
        for secret in secrets {
            assert!(
                !captured.contains(secret),
                "日志捕获中出现敏感串(前 12 字符: {}...)",
                &secret[..secret.len().min(12)]
            );
        }
    }
}
