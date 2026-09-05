# 本地开发指南(LOCAL-DEVELOPMENT)

面向后端/联调开发者。前端本地栈从 `docs/FRONTEND-INTEGRATION.md` §1 进入。

## 1. 环境要求

| 工具 | 版本要求 | 用途 |
|---|---|---|
| Rust toolchain | 仓库 `rust-toolchain.toml` 固定;`cargo 1.8x+` | bridge / relay / protocol crate |
| protoc + buf | `scripts/gen-protobuf.sh` 所需(protoc ≥ 25 或按脚本提示) | 协议再生成 |
| Node.js + pnpm | Node ≥ 20;pnpm(workspace) | `packages/protocol-ts` 构建 |
| Docker | 本机统一使用 OrbStack 提供的 `docker`(PATH 或 `~/.orbstack/bin/docker`,orbstack context) | 开发 PostgreSQL;不使用 Docker Desktop |
| macOS | Bridge 只在 macOS 运行(Keychain、Codex Desktop 依赖) | 真实 Bridge 链路 |

## 2. 完整本地栈

### 2.1 数据库

```bash
cp .env.example .env        # 设置 POSTGRES_PASSWORD(仅开发库用)
docker compose --env-file .env -f deploy/compose.yaml up -d postgres
```

- 库名/用户:`agent_console`;Relay 迁移在启动时自动执行
  (`sqlx::migrate!("./migrations")`),空库可直接启动。
- 独立性要求(§18):不与 dev-toolbox 共用数据库或用户;本仓库 PostgreSQL
  也不接入 dev-toolbox 的 `backend` 网络。
- 宿主机直跑 relay 时,临时放开 `deploy/compose.yaml` 中注释的
  `127.0.0.1:5433:5432` 端口映射。

### 2.2 Relay

```bash
export RELAY_DATABASE_URL=postgres://agent_console:<密码>@127.0.0.1:5433/agent_console
export RELAY_BIND_ADDR=127.0.0.1:8081
export RELAY_INTERNAL_TOKEN=<内部token>            # 与 dev-toolbox 侧同值
export DEVTOOLBOX_INTERNAL_BASE_URL=http://127.0.0.1:8080
cargo run -p relay
```

- 未配置 `RELAY_INTERNAL_TOKEN`/`DEVTOOLBOX_INTERNAL_BASE_URL` 时 Relay 仍可
  启动(设备面、配对可用),但浏览器 WS ticket 无法消费 → 浏览器端到端联调
  必须配置。
- 迁移失败会直接退出并打印错误;`relay-stdout.log` 为历史调试遗留文件,
  不是运行必需。

### 2.3 dev-toolbox(统一认证后端)

Agent Console 的登录、CSRF、ticket 签发、forward-auth verify 均由 dev-toolbox
提供。本地跑法以同级 **dev-toolbox 仓库文档为准**:`../dev-toolbox`
(见其 `docs/18-AGENT-CONSOLE-AUTH.md` 与 `AGENTS.md`;需要配置
`AGENT_CONSOLE_INTERNAL_TOKEN`,与 Relay 侧 `RELAY_INTERNAL_TOKEN` 同值)。
本仓库不复制其启动步骤,避免两处文档漂移。

### 2.4 Bridge(真实 / fake)

命令清单见 `docs/FRONTEND-INTEGRATION.md` §1.3–§1.4(`bridge
pair/doctor/run/workspace/inspect`;`fake-codex-owner` 脚本格式)。

## 3. 环境变量表(与 `.env.example` 对齐)

| 变量 | 服务 | 默认 | 说明 |
|---|---|---|---|
| `RELAY_DATABASE_URL` | relay | 必填 | PostgreSQL 连接串(独立库/用户) |
| `RELAY_BIND_ADDR` | relay | `127.0.0.1:8081` | 监听地址;容器内为 `0.0.0.0:8081` |
| `RELAY_INTERNAL_TOKEN` | relay | 空 | 调 dev-toolbox 内部接口的 Bearer(不写日志) |
| `DEVTOOLBOX_INTERNAL_BASE_URL` | relay | 空 | dev-toolbox 内部基地址 |
| `DEVTOOLBOX_PUBLIC_ORIGIN` | 部署/文档用 | — | 对外站点 origin |
| `RELAY_PUBLIC_WS_PATH` | 部署/文档用 | `/agent-console/ws` | 经反代后的公开 WS 路径 |
| `TRUSTED_PROXY_CIDRS` | relay | `127.0.0.0/8,::1/128` | 信任的身份头来源网段(生产为网关所在网段) |
| `RELAY_INTROSPECT_SECS` | relay | 60 | 浏览器 WS auth session introspection 周期 |
| `RELAY_AUTH_GRACE_SECS` | relay | 120 | dev-toolbox 不可达宽限 |
| `RELAY_HEARTBEAT_SECS` | relay | 15 | 心跳间隔 |
| `RELAY_OFFLINE_AFTER_SECS` | relay | 45 | 无心跳判离线 |
| `VAPID_PRIVATE_KEY_FILE` | relay | 空 | VAPID 私钥 PEM 路径;未配置时 push 发送禁用(订阅仍可存) |
| `VAPID_PUBLIC_KEY` / `VAPID_SUBJECT` | relay/前端 | 空 | VAPID 公钥(applicationServerKey)与 subject |
| `RELAY_PUSH_FAKE_SINK` | relay(测试) | 空 | fake push 转发地址(自动化测试,不调真实服务) |
| `POSTGRES_PASSWORD` | compose | 必填 | 开发库初始化密码 |
| `AGENT_CONSOLE_RELAY_URL` | bridge | — | Relay 公网基地址(origin,如 `https://toolbox.example.com`;本地 `ws://127.0.0.1:8081`;`--relay-url` 优先)。不得带 `/agent-console/...` 路径,Bridge WS/配对/传输端点由 `config::RelayUrls` 统一派生 |
| `AGENT_CONSOLE_DATA_DIR` | bridge | 用户应用数据目录 | Bridge SQLite/临时目录位置(macOS `~/Library/Application Support/agent-console/bridge`) |
| `AGENT_CONSOLE_CODEX_HOME` / `AGENT_CONSOLE_IPC_SOCKET` / `AGENT_CONSOLE_DEVICE_NAME` | bridge | `~/.codex` 等 | fake owner 联调时指向含 fake `ipc/ipc.sock` 的目录;显式指定 socket 路径与设备名 |

设备凭据明文只存 macOS Keychain(§19);任何环境变量/CLI/SQLite/日志中不得出现。

## 4. 常用诊断

```bash
# Relay 存活
curl -s http://127.0.0.1:8081/health        # → ok

# Bridge 体检:版本、~/.codex 路径、IPC socket、Keychain、绑定、能力矩阵(无会话正文)
cargo run -p bridge -- doctor

# 只读检查 adapter 可见数据(仅 ID 与状态维度,无标题正文/路径)
cargo run -p bridge -- inspect sessions
cargo run -p bridge -- inspect session <native-session-id>
```

- Relay 日志:JSON 输出到 stderr,`RUST_LOG`(EnvFilter)控制级别,默认
  `info`。字段按白名单构造(§25.3):不含 Cookie/凭据/ticket/prompt/输出/路径。
- Transfer/pairing/审计等有独立 target(`relay::transfers`、`relay::auth`、
  `relay::audit`),可按 target 过滤,如 `RUST_LOG=relay::transfers=debug`。
- 设备离线排查顺序:`bridge doctor` → Bridge 进程日志(重连退避 1s–30s)→
  `GET /agent-console/api/devices` 的 `connection`/`lastSeenAt`。

## 5. 测试

```bash
# 全部 Rust 测试(bridge + relay + protocol;仓库标准入口)
cargo test --workspace

# 协议 TS 包:再生成 + 构建(dist 产出,前端以 @agent-console/protocol 导入)
pnpm --filter @agent-console/protocol generate
pnpm --filter @agent-console/protocol build
```

说明:

- Relay 集成测试(`apps/relay/tests/`)自行拉起一次性 `postgres:17-alpine`
  容器(随机宿主端口,Drop 时清理),因此需要本机 `docker` 可用(OrbStack),
  不依赖也不触碰 `.env` 里的开发库。
- Push 相关测试使用 `RELAY_PUSH_FAKE_SINK` 转发型 fake sender,
  不调用真实 Push 服务(§24)。
- dev-toolbox 侧测试(`apps/api/integration/agent_console_test.go`)归
  dev-toolbox 仓库,见其 `docs/18-AGENT-CONSOLE-AUTH.md`。

## 6. macOS Bridge 常驻安装

从仓库构建 release 二进制、在未绑定时发起配对，并注册当前用户的
LaunchAgent（全程不需要管理员权限）：

```bash
./scripts/install-macos-bridge.sh --relay-url http://127.0.0.1:8080
```

已有预构建二进制时可跳过源码构建：

```bash
./scripts/install-macos-bridge.sh \
  --relay-url https://toolbox.example.com \
  --binary /absolute/path/to/bridge
```

固定路径：

- 二进制：`~/Library/Application Support/com.hxf.agent-console/bin/bridge`
- LaunchAgent：`~/Library/LaunchAgents/com.hxf.agent-console.bridge.plist`
- 日志：`~/Library/Logs/com.hxf.agent-console/bridge.stdout.log` 与
  `bridge.stderr.log`
- 数据：默认 `~/Library/Application Support/agent-console`；可用
  `--data-dir` 指向当前用户主目录内的其他专用目录

状态与卸载：

```bash
launchctl print "gui/$(id -u)/com.hxf.agent-console.bridge"
./scripts/uninstall-macos-bridge.sh
```

普通卸载保留 Keychain 绑定和本地数据，便于重装。只有明确需要完全清除时使用：

```bash
./scripts/uninstall-macos-bridge.sh --purge-data
```

安装器属于本地源码/预构建二进制安装入口，不包含签名、公证或自动更新。
LaunchAgent 通过 `/usr/bin/env -i` 只向 Bridge 传入 HOME、TMPDIR、最小 PATH、
日志级别和 Agent Console 配置，避免继承当前 GUI 会话中的无关凭据变量。
