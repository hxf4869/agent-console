# Agent Console

Agent Console 是一个自托管、浏览器优先的多 Agent 控制台:用户继续在 Mac 上
使用 Codex Desktop,本机 Bridge 读取并控制同一批 Desktop 任务;浏览器通过
自托管 Relay 查看进度、处理问题与审批、排队下一轮并传输文件。首个(也是
唯一的)Agent 接入目标为 macOS Codex Desktop。

- 权威后端规格:`docs/ZCODE-BACKEND-EXECUTION-SPEC.md`
- Web 实现与运行说明:`apps/web/README.md`

## 架构(§6 简化)

```text
浏览器 / PWA
  ├─ 同源 HTTPS JSON(/agent-console/api)──┐
  ├─ 同源 WSS Protobuf(/agent-console/ws)─┤
  └─ 同源临时 HTTP 文件流(/agent-console/transfers 浏览器侧)┤
                                    ▼
        dev-toolbox 网关/认证 ─── Agent Console Relay ─── Relay PostgreSQL
                                    ▲
                                    │ WSS(设备凭据)+ producer/consumer 出站流
                             Mac Headless Bridge
                               ├─ Codex Adapter ── 私有 IPC ── Codex Desktop
                               ├─ 只读 SQLite / 只读 Git
                               ├─ 文件授权与传输 / Bridge SQLite / Keychain
```

- Bridge 主动出站连接 Relay,Mac 不开入站端口;Codex Desktop 始终是任务
  运行方与唯一控制 owner,Relay 只代理不落正文。
- 身份:dev-toolbox 是唯一用户身份源(Cookie+CSRF+一次性 WS ticket);
  Bridge 使用独立设备凭据(明文只入 macOS Keychain)。

## 目录结构

```text
apps/bridge/        Mac headless Bridge(CLI:run/doctor/pair/unpair/workspace/inspect;fake-codex-owner)
apps/relay/         Rust/Tokio/Axum Relay(浏览器 HTTP+WS、设备面、文件面、push、audit)
apps/web/           Vue 3 PWA(默认真实 transport;显式 ?fixture=1 才使用脱敏 fixture)
crates/protocol/    Protobuf Rust 生成与窄编解码
packages/protocol-ts/  @agent-console/protocol:TS 生成类型 + encode/decode
proto/agent_console/v1/  协议定义(字段号冻结)
contracts/          openapi.yaml / error-codes.md / fixtures
deploy/             compose(dev/production)、Dockerfile.relay、反代参考模板
docs/               执行规格、前端对接、本地开发、部署、Codex 兼容性等文档
```

## 快速开始

见 **[docs/LOCAL-DEVELOPMENT.md](docs/LOCAL-DEVELOPMENT.md)**
(PostgreSQL/Relay/真实与 fake Bridge 启动、环境变量、诊断、测试)。
前端实现入口见 **[docs/FRONTEND-INTEGRATION.md](docs/FRONTEND-INTEGRATION.md)**。

macOS 上可从仓库一条命令构建、配对并安装 Bridge 用户级常驻服务：

```bash
./scripts/install-macos-bridge.sh --relay-url https://toolbox.example.com
```

安装器不需要管理员权限，设备凭据仍只写入 Keychain。卸载默认保留绑定数据；
完整清除使用 `./scripts/uninstall-macos-bridge.sh --purge-data`。路径、日志和
预构建二进制安装方式见 `docs/LOCAL-DEVELOPMENT.md`。

## 当前能力状态(摘要)

权威矩阵:`docs/CODEX-COMPATIBILITY.md`(当前验证环境:codex-cli `0.153.1`,
2026-09-04)。

- **读链已验证(真实 Desktop)**:会话发现与 owner、following 订阅、快照与
  Immer patch 流、load-complete-history 补偿、idle/active 运行状态、
  结束后权威校正。
- **核心轮次控制受限开放**:codex-cli `0.153.1` 的 start / steer /
  interrupt 已在真实 Desktop 验证；Bridge 单条下一轮队列复用已验证的
  start。问题回答、审批仍为 fixture-only，设置写入虽方法级验证但因没有
  动态可选值保持关闭。UI 只按服务端 capability 与 ControlMode 门控。
- **CREATE_TASK / rename / archive / fork = UNSUPPORTED(当前 IPC)**:
  Desktop 私有 app-server 通道不对外,经 asar 全量枚举确认;相关 UI 控件
  不应渲染。
- 输出合同:LIVE_PREVIEW(尽力预览)+ AUTHORITATIVE_FINAL(终态权威校正),
  不是逐字节无损终端。

## 前端对接入口清单

| 产物 | 位置 |
|---|---|
| HTTP 契约(39 个操作) | `contracts/openapi.yaml` |
| 稳定错误码与 HTTP 状态映射(32 码 + 补充码) | `contracts/error-codes.md` |
| UI 状态 fixtures(13 个,合成数据) | `contracts/fixtures/`(见其 README) |
| 对接指南(认证/WS 时序/命令/输出 reducer/文件面/push/deep link/示例) | `docs/FRONTEND-INTEGRATION.md` |
| TS 协议包 | `packages/protocol-ts`(`@agent-console/protocol`,`encodeEnvelope/decodeEnvelope`) |
| Codex 能力矩阵 / IPC 逆向文档 | `docs/CODEX-COMPATIBILITY.md` / `docs/CODEX-IPC-PROTOCOL.md` |
| 部署拓扑与安全清单 | `docs/DEPLOYMENT.md` |

> 隐私提示:Relay 持久化包含会话标题、项目显示名与当前分支(设备隐私开关
> `privacyHideTitles` 可隐藏标题),并非"服务器不含任何会话信息";prompt、
> 回复、输出、Diff、文件正文与绝对路径不落库(§25)。

## 许可证

[MIT](LICENSE)
