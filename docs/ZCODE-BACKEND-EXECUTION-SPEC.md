# Agent Console：ZCode 后端单轮执行规格

> 日期：2026-09-04  
> 状态：已确认，可直接执行  
> 目标工作目录：`<workspace>/agent-console`  
> 允许的关联仓库：`<workspace>/dev-toolbox`，仅限本文明确列出的认证与反向代理改动

## 0. 如何使用本规格

这是一份实现指令，不是候选方案。ZCode 必须在一轮任务中持续完成本文定义的后端范围，直到达到“前端可直接对接”的完成标准；不得只输出计划、只创建脚手架、只实现 happy path，或因为工作量较大主动缩减范围。

执行顺序必须遵循本文的阶段门。阶段门失败时，按“停止条件”报告事实，不能擅自切换到另一套 Codex 运行方式、增加基础设施或伪造能力。

本文中的“后端”包括：Mac 本机桥接逻辑、Codex Desktop Adapter、Relay、协议、数据库、设备绑定、统一认证后端、实时状态、控制命令、文件数据面、Git 只读信息、Push 后端、部署配置、契约、生成代码和测试。本文中的“前端/UI”包括 Vue 页面、组件、样式、交互布局、Stitch 设计、Tauri 设置窗口和菜单栏视觉界面。

优先级如下：

1. 用户在当前会话中的明确决定。
2. 本文件。
3. 仓库根 `AGENTS.md`。
4. `dev-toolbox/AGENTS.md`，仅适用于对该仓库的修改。
5. 其他架构评审材料，仅作背景参考。

不得修改本规格来迁就实现。如果发现事实不成立，保留规格并在最终报告中提出需要用户决定的变更。

## 1. 产品定义

Agent Console 是一个自托管、浏览器优先的多 Agent 控制台。用户继续在 Mac 上使用 Codex Desktop，安装在本机的 Bridge 读取并控制同一批 Desktop 任务；浏览器通过用户自己的 Relay 查看进度、处理问题和审批、调整会话设置并继续发送指令。

产品差异化是：

- 用户自托管。
- 普通浏览器和 PWA 可访问，不局限于官方移动端入口。
- 统一管理多台本机与未来多个 Agent；首版只实现 Codex。
- 能展示用户可见时间线、计划、命令、后台命令、文件、Git 分支和 Diff。
- 复用 dev-toolbox 身份认证和主题入口。

首版不是新的代码执行平台，也不是 Codex 的替代运行时。Codex Desktop 始终是任务的运行方和唯一控制 owner。

## 2. 本轮必须交付的结果

ZCode 本轮必须交付以下可运行结果：

1. 一个 headless Rust Bridge，可连接当前 Mac 上的 Codex Desktop，执行能力探测并暴露标准化数据和命令。
2. 一个 Rust/Tokio/Axum Relay，可接受 Bridge 与浏览器连接，完成路由、订阅、重连、背压、设备绑定和数据面转发。
3. 第一版小型 Protobuf 协议，以及 Rust 和 TypeScript 生成代码。
4. Relay 独立 PostgreSQL 迁移；Bridge 独立 SQLite 迁移；密钥进入 macOS Keychain。
5. dev-toolbox 的一次性 WebSocket ticket、无活跃时间刷新 introspection，以及同域代理配置。
6. 已有任务读取、分页历史、实时状态、实时输出预览和完成后权威校正。
7. 开始新轮、运行中单条下一轮队列、steer、interrupt、提问、审批、模型和运行设置控制。
8. 文件上传、常见格式预览数据流、Range 下载、文件句柄复验和临时文件清理。
9. Git 当前分支、HEAD、工作区状态、增删行和逐文件 Diff，只读。
10. 后台命令读取和受能力约束的停止操作，不提供终端输入。
11. Web Push 的订阅存储、触发策略和发送后端；Service Worker 与通知 UI 留给前端。
12. OpenAPI、前端集成说明、状态与错误码说明、可读 fixture、TypeScript 协议包。
13. 自动化测试与一条无 UI 的端到端测试链，证明前端可以开始接入。
14. 本地开发和生产部署模板，但不执行部署。

“前端可直接对接”必须同时满足：

- Relay 和 Bridge 能通过文档化命令启动。
- 契约文件与实现一致。
- TypeScript 可以导入生成的 Protobuf 类型。
- HTTP API 有 OpenAPI 描述和稳定错误码。
- 前端可用 fixture 覆盖在线、离线、执行中、等待审批、等待回答、完成、失败、队列暂停、输出缺口和文件变化状态。
- 一条测试客户端能完成：认证票据 → WebSocket → 订阅 → snapshot → command accepted → command result → reconnect/resync。
- 不需要前端开发者阅读 Rust 内部实现或猜测 Codex 原生字段。

## 3. 文件与仓库所有权

### 3.1 ZCode 可以创建或修改

在 `agent-console` 仓库：

- 根 Cargo Workspace 配置、Rust 工具链配置和后端构建脚本。
- `apps/bridge/**`
- `apps/relay/**`
- `proto/**`
- `crates/protocol/**`，只允许共享协议生成与编解码。
- `packages/protocol-ts/**`，只允许生成协议类型和薄编解码入口。
- `contracts/**`
- `deploy/**`
- `docs/**` 中的后端运行、协议和前端对接文档。
- `.env.example`、本地开发 Compose、CI 中与后端相关的最小检查。

在 `dev-toolbox` 仓库，仅可修改：

- `apps/api/internal/auth/**` 中新增或最小提取的 Agent Console 认证逻辑。
- `apps/api/migrations/**` 中一次性 ticket 所需迁移。
- `apps/api/internal/platform/config/**` 中所需内部服务凭据配置。
- `apps/api/cmd/server/main.go` 中路由注册或构造参数。
- 对应的 Go 单元/集成测试。
- `deploy/Caddyfile`、外置 Nginx 模板、Compose 网络和环境示例中 Agent Console 同域路由所需的最小改动。
- 一份窄的集成说明。

### 3.2 ZCode 不得创建或修改

- `agent-console/apps/web/**`，该目录归 UI/前端任务所有。
- `agent-console/apps/desktop/**` 的 Tauri 页面、托盘菜单和设置窗口。
- `dev-toolbox/apps/web/**`、现有登录页面、导航和视觉样式。
- dev-toolbox 的知识库、AI worker、工具、Provider、备份或无关数据库表。
- 用户的 Codex 数据库、Codex 配置、Codex 安装文件或任意真实项目文件。
- 生产服务器、DNS、证书、对象存储、发布版本、Git tag 和远程分支。

看到 UI 工作流已创建的文件时必须保留并适配，不得重置、移动或格式化其目录。

## 4. 明确不做

以下项目不得以“顺便完善”“未来扩展”或“标准架构”为理由实现：

- Windows Bridge 或安装包。
- ZCode、Claude Code 或其他 Agent Adapter。
- 动态插件市场、第三方 Adapter ABI、脚本式 Provider。
- `agent-core`、`relay-core`、通用 Repository/BaseService/Manager/EventBus。
- 独立共享 UI 包。
- Codex App Server 启动、App Server owned task 或第二套 Codex 会话列表。
- 对 Codex SQLite 的任何写入。
- 任意远程 Shell、命令行 stdin、网页终端或用户自定义系统命令。
- Git checkout、switch、branch、worktree、add、commit、reset、push、merge 等写操作。
- 代码审查、代码审计、漏洞扫描或自动修复；Git 区域只展示当前任务所在分支和改动。
- MinIO、S3 热路径、kkFileView、Redis、Kafka、RabbitMQ、NATS、Elasticsearch、Kubernetes、服务网格。
- 微服务拆分、多租户、组织、RBAC、注册和第二套用户体系。
- 提示词、回复、命令输出、Diff 或文件内容的 Relay 长期存储。
- 应用层 E2EE。
- macOS 签名、公证、自动更新服务、正式安装器和生产发布。
- 前端页面、Tauri UI、Stitch 设计或为了展示而写的临时产品 UI。
- 没有失败结果会改变行动的测试、全库审计、无消费者字段和推测性兼容层。

## 5. 已验证事实与不可变前提

实现不得把以下事实重新解释成假设：

- 当前已验证 Codex CLI/桌面相关版本为 `0.153.0-alpha.5`；版本会变化，因此不能只匹配这个字符串。
- 当前 Codex 数据可从 `~/.codex/state_5.sqlite` 与 `~/.codex/thread_history_1.sqlite` 读取；路径和 schema 必须探测，不能硬编码为永久合同。
- Desktop 内部 IPC 当前位于 `~/.codex/ipc/ipc.sock`，使用长度前缀 JSON frame；它是私有协议，必须隔离在 Adapter 内。
- 已验证独立进程可以发现当前会话 owner、获取快照、观察运行状态、steer 当前轮，以及在 idle 后启动下一轮。
- follower 不保证收到每一段命令输出 delta；快照补偿也曾短暂漏掉首段输出。
- 因此：运行生命周期必须权威；执行中输出只承诺最佳努力预览；命令或轮次结束后必须重新读取并替换为权威最终结果。
- Codex Desktop 是所有首版任务的 owner。Bridge 只通过已验证 Desktop 控制路径操作，不能用 App Server 创建平行任务。
- Codex 本地数据库只是读取和补偿来源，不代表另一种会话 ownership。
- 浏览器不能直接访问 Unix socket、SQLite、本机文件或 NAT 后的 Mac，因此本机 Bridge 是必需组件。
- Relay 是自托管的公网连接点；Mac 和浏览器都主动连接 Relay。

任何写能力只有在当前 Desktop 版本、IPC owner 和具体操作都通过 capability probe 后才能开启。未知版本默认只读；不得通过“看起来字段相同”自动开启写入。

## 6. 固定总体架构

~~~text
浏览器 / PWA
  ├─ 同源 HTTPS JSON ───────────────┐
  ├─ 同源 WSS Protobuf ────────────┤
  └─ 同源临时 HTTP 文件流 ─────────┤
                                     ▼
dev-toolbox Gateway / Auth ─── Agent Console Relay ─── Agent Console PostgreSQL
                                     ▲
                                     │ WSS Protobuf + 临时 HTTPS producer/consumer
                                     │
                              Mac Headless Bridge
                                ├─ Codex Adapter ── 私有 IPC ── Codex Desktop
                                ├─ 只读 SQLite ──────────────── Codex 状态库
                                ├─ File Grant Manager ───────── 授权工作区
                                ├─ Git Reader ────────────────── 固定只读 Git argv
                                ├─ Bridge SQLite
                                └─ macOS Keychain
~~~

信任边界：

- Mac 是完整会话、任务状态、工作区文件和写命令执行的事实源。
- Relay 可在内存中看到转发正文，但不能长期保存正文。
- dev-toolbox 是唯一用户身份源。
- Browser 只持有 dev-toolbox Cookie、内存中的 CSRF 和短期一次性连接 ticket。
- Bridge 使用独立设备凭据；设备凭据与用户登录 Cookie 完全分离。

## 7. 仓库最终结构

本轮完成后应接近以下结构；不得为了目录对称创建空包：

~~~text
agent-console/
├── AGENTS.md
├── README.md
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── pnpm-workspace.yaml
├── pnpm-lock.yaml                  # 只有实际生成 TS 包依赖时创建
├── .env.example
├── apps/
│   ├── bridge/                     # 一个 crate，可同时提供 lib.rs 与 headless bin
│   │   ├── Cargo.toml
│   │   ├── migrations/
│   │   └── src/
│   │       ├── adapter/codex/
│   │       ├── capabilities/
│   │       ├── commands/
│   │       ├── files/
│   │       ├── git/
│   │       ├── local_store/
│   │       ├── power/
│   │       ├── transport/
│   │       ├── lib.rs
│   │       └── main.rs
│   ├── relay/
│   │   ├── Cargo.toml
│   │   ├── migrations/
│   │   └── src/
│   │       ├── auth/
│   │       ├── devices/
│   │       ├── sessions/
│   │       ├── realtime/
│   │       ├── transfers/
│   │       ├── push/
│   │       ├── audit/
│   │       └── main.rs
│   └── web/                        # UI 任务所有；ZCode 不得写入
├── crates/
│   └── protocol/                   # Protobuf Rust 生成和窄编解码
├── packages/
│   └── protocol-ts/                # Protobuf TypeScript 生成结果
├── proto/
│   └── agent_console/v1/
├── contracts/
│   ├── openapi.yaml
│   ├── error-codes.md
│   └── fixtures/
├── deploy/
│   ├── compose.yaml
│   ├── compose.production.yaml
│   ├── Dockerfile.bridge           # 仅开发/测试需要时创建；Mac 正式分发不使用容器
│   ├── Dockerfile.relay
│   └── reverse-proxy/
└── docs/
    ├── ZCODE-BACKEND-EXECUTION-SPEC.md
    ├── FRONTEND-INTEGRATION.md
    ├── LOCAL-DEVELOPMENT.md
    ├── CODEX-COMPATIBILITY.md
    └── DEPLOYMENT.md
~~~

禁止创建 `crates/agent-core`、`crates/relay-core`、`packages/ui`。未来出现第二个真实 Adapter 或第二个真实消费者后再抽取。

## 8. 执行阶段和阶段门

### 阶段 A：建立工程，不冻结远程协议

必须完成：

- 创建最小 Cargo Workspace。
- 创建 `apps/bridge`，先使用 Rust 内部强类型结构和可读 JSON 调试输出。
- 创建脱敏的 fake IPC owner 与 SQLite fixture。
- 提供 `bridge doctor`、`bridge inspect sessions`、`bridge inspect session <id>` 等 headless 命令；命令输出用于技术验证，不是产品 UI。
- 不创建 Protobuf 大模型，不创建 Relay，不创建 Vue 页面。

通过条件：本地 fixture 测试通过，并能对当前 Codex Desktop 进行只读 probe，不修改任何 Codex 文件。

### 阶段 B：Codex Desktop 本机闭环

必须按顺序验证：

1. 发现 Desktop 进程、版本、IPC socket 和 owner。
2. 以只读方式发现未归档主任务、归档任务和主/子 Agent 关系。
3. 分页读取任务历史，不一次加载完整任务。
4. 识别当前 turn、计划、工具、命令、审批、问题和后台命令。
5. 观察 `idle → active → idle`。
6. 在专用测试任务中 start next turn。
7. 在专用测试任务中 steer active turn。
8. 回答原生问题和风险审批。
9. interrupt 当前轮。
10. 动态读取并变更模型、思考深度、Fast/服务等级、权限和协作模式。
11. 通过 Desktop 控制路径新建任务；若无法验证，不得用 App Server 替代。
12. 完成输出预览和结束后权威校正。

必须生成 `docs/CODEX-COMPATIBILITY.md`，记录“已验证版本 × 能力”，只记 schema/能力，不记任何真实会话内容。

阶段门：start、steer、问题/审批、完成状态、最终输出校正中任何核心能力无法安全工作时，停止进入远程写链路，并按第 30 节报告。

### 阶段 C：冻结最小协议并实现 Relay

只有阶段 B 的真实事件语义通过后才允许：

- 建立 `proto/agent_console/v1`。
- 建立 Rust 和 TypeScript 生成流程。
- 实现 Relay、Bridge WSS、Browser WSS、订阅、ACK、snapshot、resync、请求回执和有界队列。
- 实现独立 PostgreSQL 迁移。
- 实现设备绑定和 Bridge 凭据。

阶段门：mock 端到端链必须证明断线重试不会重复执行命令，慢客户端不会阻塞 Bridge。

### 阶段 D：接入 dev-toolbox 统一认证

必须：

- 完整读取 `dev-toolbox/AGENTS.md`。
- 实现一次性 WebSocket ticket。
- 实现不更新 `last_seen_at` 的 session introspection。
- 实现同域 HTTP forward-auth 契约和 WebSocket 定期撤销检查。
- 补充 Caddy 与外置 Nginx 路由模板。
- 保持现有登录、TOTP、可信设备、PAT 和注销语义不变。

阶段门：登录会话撤销后，现有 WebSocket 在规定窗口内关闭，新的 HTTP 写请求被拒绝；introspection 测试证明不会延长 24 小时闲置窗口。

### 阶段 E：补齐用户操作与数据面

必须：

- 单条本机下一轮队列。
- 审批、问题、模型与运行设置。
- 文件 producer/consumer 流、Range、上传临时目录和清理。
- Git 只读状态与逐文件 Diff。
- 后台命令状态与能力约束停止。
- Push 订阅和触发后端。

阶段门：安全边界测试、文件流背压测试、Git 写操作缺失检查全部通过。

### 阶段 F：前端交接

必须：

- OpenAPI 与实现一致。
- Protobuf Rust/TS 生成结果一致。
- `docs/FRONTEND-INTEGRATION.md` 给出启动、认证、连接、订阅、错误恢复和文件预览示例。
- fixture 覆盖本文列出的状态。
- 提供一条真实 Bridge 或 fake IPC 的 headless 联调方式。
- 不创建产品页面。

只有阶段 F 完成，才能报告“已到达前端对接点”。

## 9. 统一领域标识

### 9.1 会话键

任何会话都使用复合原生键：

~~~text
(device_id, agent_kind, native_session_id)
~~~

首版 `agent_kind` 只有 `CODEX_DESKTOP`。同时可分配 Relay 内部 UUID 方便 URL 与日志关联，但不能用内部 UUID 覆盖原生键。

### 9.2 轮次与条目

- `turn_id` 使用 Codex 原生 ID；无法取得时生成 Adapter scoped 稳定 ID，并明确 `synthetic=true`。
- `item_id` 同理。
- 不使用消息数组下标作为稳定 ID。
- 子 Agent 必须带父任务/父 turn 关系。
- 后台命令必须带稳定 command ID；无法稳定识别时只提供总数和全局停止 capability，不伪造单项停止。

### 9.3 可见内容边界

可以标准化并传输：

- 用户消息。
- Assistant commentary/final。
- Codex 提供给用户的可见 reasoning summary。
- Plan 与步骤状态。
- 工具调用、命令、状态、时长和用户可见输出。
- 文件变化、Diff、图片、引用文件。
- Web/MCP 调用的用户可见记录。
- 子 Agent 状态。
- 问题、候选项、审批。
- Token/context 用量。

不得读取、推断或展示隐藏 chain-of-thought、私有系统提示、原始 IPC 内部调试字段或不面向用户的凭据。

## 10. 多维状态模型

禁止使用单一 `session_status` 表示所有状态。必须分别实现：

### 10.1 DeviceConnection

- `CONNECTING`
- `ONLINE`
- `DEGRADED`
- `OFFLINE`

同时提供 `last_seen_at` 和 degraded reason。无法区分关机、休眠与断网时统一为 OFFLINE，不猜测原因。

### 10.2 ControlMode

- `FULL_CONTROL`
- `LIMITED_CONTROL`
- `READ_ONLY`
- `UNAVAILABLE`

由 Adapter capability probe 决定，不由网页自行推断。

### 10.3 CompatibilityState

- `VERIFIED`
- `DEGRADED`
- `UNSUPPORTED`

未知版本默认 `DEGRADED + READ_ONLY`，只有具体读取能力已验证时才显示相应数据。

### 10.4 ActiveTurnPhase

- `IDLE`
- `RUNNING`
- `FINISHING`

等待用户和审批不作为互斥 phase，而放入 PendingAttention；这样 RUNNING 可以同时等待用户。

### 10.5 PendingAttention

列表类型，首版支持：

- `USER_QUESTION`
- `RISK_APPROVAL`

每项必须包含稳定原生 ID、所属 turn、创建时间、可用选项和是否仍有效。回答后按原生事件移除。

### 10.6 QueueState

- `EMPTY`
- `QUEUED`
- `PAUSED`

### 10.7 LastTurnOutcome

- `COMPLETED`
- `FAILED`
- `INTERRUPTED`
- `UNKNOWN`

失败和中断只描述上一轮，不把任务永久标成失败。

### 10.8 BackgroundCommandState

- `RUNNING`
- `COMPLETED`
- `FAILED`
- `STOPPED`
- `UNKNOWN`

主 turn 完成后允许仍显示后台命令数量，不能据此发明“所有 Agent 完成”状态。

## 11. 会话摘要、运行快照、历史和详情

必须分成四类，禁止一个接口返回完整任务：

1. `SessionSummary`：标题、设备、Agent、项目显示名、当前分支、更新时间、多维状态摘要、待处理数量、置顶/静音/归档。
2. `RuntimeSnapshot`：当前 turn、plan、pending attention、正在运行的命令、后台命令、队列、能力和最近输出游标。
3. `HistoryPage`：按 turn/item 稳定游标分页；默认从最新向前读取。
4. `OnDemandDetail`：完整命令输出、Diff、文件内容、图片和 PDF，仅在请求时读取。

Relay 可以从 PostgreSQL 返回 SessionSummary。RuntimeSnapshot、HistoryPage 和 OnDemandDetail 必须由在线 Bridge 提供；设备离线时返回稳定 `DEVICE_OFFLINE`，不能返回服务器上的陈旧正文。

Relay 重启后只需从 Bridge 恢复摘要与活跃 snapshot，不得要求上传完整历史。

## 12. Codex Adapter 责任与禁区

Adapter 必须完全封装以下变化面：

- Desktop/CLI 版本发现。
- IPC socket 和 frame 编解码。
- owner 发现与 follower snapshot/patch。
- SQLite schema 发现和只读查询。
- 原生事件 → 统一领域模型。
- 统一命令 → 原生 Desktop 操作。
- 原生错误 → 稳定错误码。

SQLite 规则：

- 只使用只读连接，启用 query-only 或等价保护。
- 不运行 migration、VACUUM、PRAGMA 写入、修复或索引创建。
- 不复制完整数据库到 Relay。
- 对 schema 变化做列/表 capability probe；缺失字段只关闭对应能力。
- 查询必须分页并有超时，不能锁住 Desktop。

IPC 规则：

- 连接失败不得删除 socket、终止 Codex 或重启 Desktop。
- 所有写操作必须先确认 owner 与目标 session/turn。
- 一次连接的 frame、队列和消息尺寸有上限。
- 不把原生 JSON 直接透传到 Browser。
- 未识别的关键 patch 触发 snapshot；未识别写命令直接拒绝。

会话创建规则：

- 网页新建任务必须最终由 Codex Desktop 创建并出现在同一 Desktop 列表。
- 只允许 Codex 最近使用工作区或 Bridge 已授权根目录。
- 不允许任意路径浏览器。
- 如果 Desktop 私有控制路径无法稳定创建任务，标记 capability unsupported 并触发阶段门；不得启动 App Server 顶替。

会话管理规则：

- rename、archive、unarchive、fork 只有 Desktop 暴露且当前版本验证过对应能力时才执行。
- pin、mute 属于 Agent Console 自身偏好，保存在 Relay，不写回 Codex 数据库。
- 不提供永久删除任务。
- 主任务作为列表主体，子 Agent 嵌套在父任务/turn 下，不把子 Agent 提升为无归属的顶层任务。
- Desktop 端发生重命名、归档或新建后，Bridge 必须把变化同步回 Relay；Relay 不能把自己的旧摘要反向覆盖 Desktop。
- 切换分支、创建 worktree、提交或推送代码只允许通过给 Codex 发送自然语言指令完成，不增加专用 Git 写接口。

## 13. 实时输出真实性合同

### 13.1 用户承诺

必须向前端明确区分：

- `LIVE_PREVIEW`：执行中尽量实时，允许延迟、合并或暂时缺片。
- `AUTHORITATIVE_FINAL`：命令或 turn 结束后从 Desktop 权威来源重读并校正。

生命周期状态、问题和审批不能按“最佳努力”处理，必须可靠恢复。

### 13.2 输出游标

每个可输出 item 至少维护：

- `item_id`
- `revision`
- `byte_length`
- `is_final`
- 原生可区分时的 `stdout/stderr`；不可区分时使用 `combined`，不得猜测。

实时事件允许：

- `OutputAppend(expected_offset, bytes)`
- `OutputReplace(revision, bytes_or_page_cursor)`
- `OutputFinal(revision, byte_length)`

前端游标与 `expected_offset` 不一致时不得盲目拼接；返回 item snapshot 或 `RESYNC_REQUIRED`。所有分块必须在 UTF-8 边界切分，二进制输出只提供受限字节预览或下载句柄。

### 13.3 结束校正

命令或 turn 进入 terminal state 后，Bridge 必须：

1. 等待 Desktop 状态落稳的短窗口；不得用固定长 sleep。
2. 从权威 snapshot/history 再读一次最终条目。
3. 对比 revision/长度；不同则发 `OutputReplace`。
4. 标记 `AUTHORITATIVE_FINAL`。
5. 若最终结果仍无法读取，标记 `FINAL_OUTPUT_UNAVAILABLE`，不能把当前预览冒充完整结果。

### 13.4 必测场景

使用脱敏脚本或 fake owner 覆盖：

- 连续输出编号 `1..N`，检查最终无缺号。
- stdout 与 stderr 交替。
- 无换行小片段。
- 突发大输出。
- 命令失败、被停止和 turn interrupt。
- 输出过程中 Browser 断线重连。
- follower 缺少中间 delta，但 final snapshot 完整。
- final snapshot 也不完整时正确暴露不可用状态。

禁止宣称逐字节无损实时终端。

## 14. 事件优先与自适应观察

Bridge 必须优先消费 Desktop 原生事件，snapshot 只用于补偿、校正和低频确认。

初始观察策略：

- 当前有详情订阅且 turn 正在运行：允许约 500ms 的轻量观察起点。
- turn 正在运行但没有详情订阅：约 2s 检查轻量状态，不读取完整历史。
- idle 且有列表订阅：只读取摘要变化，起点约 30s；原生事件到达时立即更新。
- 无 Browser 订阅：保留设备心跳和活跃 turn 完成检测；不轮询 idle 历史。
- 发生断线、sequence 缺口、owner 切换或原生未知 patch：立即取一次完整 RuntimeSnapshot。
- 电池供电、系统压力或活跃任务数增加时降低频率；正在等待用户的 attention 不需要高频读取完整输出。

这些是 M1 的初始值，不是产品 SLA。ZCode 必须用实际 snapshot 大小、耗时和 CPU 结果决定是否调整，并把结果记录到兼容文档。不得为所有会话做 250ms 深度 JSON diff。

本地差分只比较稳定 ID、revision、游标、长度和状态字段。禁止每个 tick 重新序列化并深比较整个历史树。

## 15. 写命令、一致性和单条队列

### 15.1 所有写操作的公共字段

每条写操作必须带：

- `request_id`：调用方生成的 UUID。
- `operation`。
- `session_key`。
- `expected_turn_id`，不适用时为空。
- `expected_runtime_revision`。
- payload 的本地规范化摘要，用于检测同 ID 不同内容。

内容摘要属于协议去重用途，可以使用固定加密摘要算法；摘要仅用于比较，不用于内容真实性证明。Relay 不保存正文。

### 15.2 回执状态

- `RECEIVED`：Relay 已收到，不能显示为已发送到 Codex。
- `ACCEPTED_BY_BRIDGE`：Bridge 已持久化去重记录并接受。
- `DISPATCHED_TO_CODEX`：已交给 Desktop owner。
- `COMPLETED`：操作有明确最终结果。
- `REJECTED`：附稳定错误码。
- `OUTCOME_UNKNOWN`：连接中断且无法证明 Desktop 是否执行。

Browser 只有收到 `ACCEPTED_BY_BRIDGE` 才能显示“已发送/已排队”。

相同 ID 与相同 target/operation/payload 重试时返回已有回执，不重复执行。相同 ID 对应不同内容时返回 `DUPLICATE_REQUEST_MISMATCH`。目标 turn/revision 已变化时返回 `STALE_TURN`。

### 15.3 下一轮队列

- 每个 session 最多一条。
- 正文只保存在 Bridge SQLite。
- Relay 只保存“有队列”和状态，不保存正文。
- 支持读取、替换、取消。
- 队列绑定 `after_turn_id` 与接受时的 runtime revision。
- 当前 turn 正常完成、无 PendingAttention、Desktop 未启动更新 turn 时自动发送。
- 当前 turn failed/interrupted、设备断线、Desktop 用户抢先开始新 turn 或 ownership 变化时改为 PAUSED。
- PAUSED 只能由用户重新确认，不能自动改绑到新 turn。
- Browser 关闭不取消已由 Bridge 接受的队列。
- 设备离线时禁止创建或替换队列，Relay 不代存。

### 15.4 发送语义

- IDLE：默认 start new turn。
- RUNNING：默认写入单条 next-turn queue。
- RUNNING 且用户显式选择：steer 当前 turn。
- Pending question：只允许按原生 question ID 回答。
- Pending approval：只允许按原生选项批准/拒绝。
- interrupt：要求 expected turn；前端负责二次确认，后端不得把普通发送误映射为 interrupt。

## 16. Codex 设置、问题和审批

### 16.1 动态能力

以下选项必须从当前 Desktop/会话动态读取，不得在 Relay 或前端契约硬编码具体列表：

- 模型。
- 思考深度。
- Fast 或 service tier。
- 权限模式，包括 Codex 原生“帮我批准”。
- 协作/Plan 模式。

协议可定义通用 option 结构，但 option ID 和是否可选来自 Adapter capability snapshot。

### 16.2 设置生效

- 已有 session 的设置变更作用于下一次 turn；原生支持即时变化时仍要明确 effective scope。
- 新 session 继承 Bridge 读取到的全局默认值。
- 默认权限使用 Codex 原生“帮我批准”，绝不能实现外部无条件自动批准。
- 不支持的组合返回 `SETTING_COMBINATION_UNSUPPORTED`，不得静默降级到另一个模型或权限。
- 设置写入仍须经过 request ID、expected revision 和 capability probe。

### 16.3 问题

必须保留：原生 question ID、标题、说明、选项 ID、选项标签、是否多选、是否允许自由文本、所属 turn 和失效状态。Browser 回答发送 option ID 或明确文本，不能只发送展示文案。

### 16.4 审批

必须保留：原生 approval ID、风险说明、请求动作、可用决定和作用范围。Bridge 只能调用 Desktop 提供的原生决定，不得创造“永远允许”或扩大范围的选项。

## 17. Protobuf 与 WebSocket 合同

### 17.1 定型时机

阶段 B 之前使用 Rust 类型与 JSON 调试输出。只有真实 Codex 语义验证通过后才冻结 `.proto`。一旦前端消费，字段号不得复用；删除字段必须 `reserved`。

### 17.2 Envelope 必需字段

第一版 Envelope 至少包含：

- `protocol_version`
- `message_id`
- `correlation_id`
- `sent_at`
- `device_id`
- `agent_kind`
- `stream_id`
- `stream_epoch`
- `sequence`
- `payload oneof`

不得把 Cookie、设备明文凭据、文件正文或任意本机绝对路径放入 Envelope。

### 17.3 第一版 payload

只实现有真实消费者的消息：

- `ClientHello` / `ServerHello`
- `CapabilitySnapshot`
- `DevicePresence`
- `Subscribe` / `Subscribed`
- `Unsubscribe`
- `Ack`
- `SessionSummaryBatch`
- `RuntimeSnapshot`
- `EventBatch`
- `ResyncRequired` / `ResyncRequest`
- `QueryRequest` / `QueryResponse`
- `CommandRequest`
- `CommandAccepted`
- `CommandResult`
- `TransferOffer` / `TransferReady` / `TransferResult`
- `Heartbeat` / `HeartbeatAck`
- `ProtocolError`

不得为未来 ZCode/Claude Code 枚举大量专属消息。保留 `agent_kind`，以及一个带明确 type URL/版本的 provider extension；首版只允许 Codex Adapter 写入，未知关键扩展不能执行。

### 17.4 订阅和快照竞争

正确顺序必须是：

1. Browser 发送 Subscribe。
2. Relay 为该 subscriber 建立有界队列，并向 Bridge 建立/复用上游订阅。
3. Bridge 固定 snapshot 对应的 `stream_epoch/base_sequence`。
4. snapshot 生成期间的新事件进入缓冲。
5. Relay 先发 RuntimeSnapshot，再发 `base_sequence` 之后的事件。
6. Browser 应用后发送 Ack。

禁止“先读取 snapshot，再开始监听”。

列表订阅只含 SessionSummary。详情订阅才包含 RuntimeSnapshot、item 变化、输出和 attention。

### 17.5 sequence 语义

- sequence 只保证同一 `stream_id + stream_epoch` 内的顺序。
- epoch 变化必须从新 snapshot 开始。
- 重复 sequence 幂等忽略。
- 缺口仍在 Relay 内存窗口时补发。
- 超出窗口、Relay 重启或 epoch 变化时返回 ResyncRequired。
- sequence 不承担业务幂等；业务幂等由 request ID 完成。

### 17.6 有界缓冲和优先级

每个活跃 stream 同时限制：

- 保留时长。
- 事件条数。
- 序列化总字节。

开发初始上限可使用“15 分钟、10,000 条、16 MiB，取先到者”，但 M1/M2 测量可以用证据收紧；不得只配置其中一种。Relay 还必须有全局内存上限和每 Browser 发送队列上限。

事件优先级：

1. 不可静默丢弃：问题、审批、turn 生命周期、command accepted/result、设备撤销。
2. 可由 snapshot 恢复：计划、命令状态、文件/Git 摘要。
3. 可合并：高频输出增量、Token 使用更新、presence 心跳。

队列压力持续时，合并第 3 类；仍不足则向慢客户端发 ResyncRequired 并断开该 subscriber，不能阻塞 Bridge 或丢第 1 类后继续假装同步。

结构化 Protobuf 单帧默认不得超过 1 MiB；输出分块默认不超过 64 KiB；文件正文禁止进入 Protobuf。

## 18. Relay PostgreSQL

Relay 使用独立数据库和独立数据库用户。生产可复用现有 PostgreSQL 实例，但不得读写 dev-toolbox 业务表，也不得使用同一数据库用户。开发 Compose 可以提供独立 PostgreSQL 容器。

只创建以下有真实消费者的表；字段可按 Rust/SQL 类型细化，但含义不得扩大：

### 18.1 devices

- 内部 device UUID、owner UUID。
- display name、platform、arch、Bridge version。
- 设备凭据摘要，不保存明文。
- paired/revoked/last_seen 时间。
- compatibility/control summary。
- privacy setting：是否保存标题。

### 18.2 pairing_challenges

- challenge ID。
- 高熵 challenge 摘要和 6 位短码摘要。
- Bridge 展示信息。
- expires_at、attempt_count、approved_at、consumed_at。
- 5 分钟过期、单次使用、有限尝试。

### 18.3 session_summaries

- 内部 session UUID。
- `(device_id, agent_kind, native_session_id)` 唯一约束。
- title、project display、branch；设备隐私模式开启时为空。
- 多维状态摘要、attention count、last updated。
- pinned、muted、archived。
- 不保存绝对 cwd、消息、输出、Diff 或文件名列表。

### 18.4 request_receipts

- request ID、session UUID、operation、状态、时间。
- 不保存 prompt、answer、approval 说明或 payload 正文。
- Bridge 本地回执仍是执行去重事实源；Relay 记录只用于 Browser 查询和审计。

### 18.5 push_subscriptions

- owner/browser subscription、endpoint、加密所需公钥、创建/失效时间。
- endpoint 属于敏感数据，日志不得输出。

### 18.6 notification_settings

- 默认通用文案。
- 是否允许显示 title。
- 事件开关和 session mute 的最终组合。

### 18.7 audit_events

- owner、device、session 内部 ID、request ID、operation、result、时间、延迟。
- 30 天保留。
- 不保存标题、项目路径、prompt、回答、审批正文、输出或文件名。

过期 pairing、receipt、audit 的清理由 Relay 单实例内有界维护任务或操作时顺带清理；不得为此引入消息队列、Redis 或独立定时服务。

## 19. Bridge SQLite 与 Keychain

Bridge SQLite 只保存：

- Relay URL、device ID 和非敏感绑定状态。
- 已授权工作区根目录。
- 最近 request 回执与 payload 摘要。
- 每个 session 最多一条 next-turn queue 正文及绑定 turn/revision。
- capability probe 结果与本地 schema 版本。
- 必要 cursor、隐私设置和临时上传清理记录。

不得复制完整 Codex 会话、历史输出或 Diff。

SQLite 文件必须位于用户应用数据目录，权限限制为当前用户。schema 由 Bridge 自己 migration，绝不能对 Codex 数据库运行 migration。

设备明文凭据只能存 macOS Keychain。环境变量、CLI 参数、SQLite、日志和 crash report 中不得出现凭据。Keychain 不可用时 Bridge 进入未绑定/不可控制状态，不回退到明文文件。

`apps/bridge` 可同时提供 library 和 headless binary，供未来 Tauri shell 作为真实消费者；不要额外创建 `bridge-core` crate。

Headless binary 至少提供：

- `bridge run`：启动常驻连接。
- `bridge doctor`：只输出版本、路径可用性、owner 和 capability，不输出会话正文。
- `bridge pair` / `bridge unpair`：完成绑定或清除本机凭据；清除必须是显式动作。
- `bridge workspace list|authorize|revoke`：管理允许新建任务和显式打开文件的根目录。
- `bridge inspect ...`：仅开发验证使用的只读命令。

Bridge 必须实现可由未来 Tauri 调用的生命周期 API，而不是把逻辑写死在 CLI 参数解析中。首版电源策略为：

- 接通电源时允许系统保持在线，但不阻止显示器休眠。
- 电池供电时，仅在 Codex turn 活跃或等待用户处理时持有系统唤醒断言；turn 结束且无 attention 后释放。
- 使用 macOS 系统 API或固定 argv 的系统工具，不经过 shell；句柄必须随状态变化和进程退出可靠释放。
- 自动登录启动的视觉开关和 Tauri 插件接线归桌面 UI 任务；ZCode 只提供可测试的启动/停止和 WakePolicy 后端。

## 20. dev-toolbox 统一认证合同

### 20.1 用户体验

- 用户访问 `/agent-console/...`。
- 未登录时前端跳转现有 dev-toolbox 登录页。
- 登录成功返回原始 Agent Console 相对路径。
- return path 只允许同源 `/agent-console/` 前缀，拒绝绝对 URL、协议相对 URL 和其他路径。
- 已登录用户无需第二次输入账号或 TOTP。

登录跳转 UI 由前端任务实现；ZCode 只提供后端能力和契约。

### 20.2 WebSocket ticket

在 dev-toolbox 新增：

- `POST /api/v1/agent-console/ws-tickets`
- 必须使用现有 session-only `AuthenticateMutation`，校验 Cookie、Origin 与 CSRF。
- ticket 使用高熵随机值，明文只返回一次，服务端只存摘要。
- 默认 30 秒过期，单次消费。
- ticket 绑定 auth session ID、owner ID、允许 audience 和到期时间。
- 响应必须 `Cache-Control: no-store`。

建议新增 dev-toolbox 表 `agent_console_ws_tickets`。Relay 通过内部接口原子消费 ticket；实现应使用 DELETE/UPDATE ... RETURNING 或事务锁，两个并发消费者只能有一个成功。

Browser 不把 ticket 放进 URL query。使用 `Sec-WebSocket-Protocol` 中的固定协议名与 Base64URL ticket，Relay 只回显固定协议名，不在日志中记录完整 header。

### 20.3 内部消费和 introspection

在 dev-toolbox 新增仅内部网络可达、使用独立内部服务 Token 的接口：

- `POST /internal/agent-console/ws-tickets/consume`
- `POST /internal/agent-console/auth-sessions/introspect`

消费返回最小字段：valid、auth session ID、owner ID、session expires_at。introspection 按 session ID 检查 revoked、expires、24 小时 idle 和 password_changed_at，**不得更新 `last_seen_at`**。

现有 `authenticateSession` 会按 5 分钟更新 last_seen。允许做最小提取，将“查询并验证”与“是否 touch last_seen”分开；所有现有浏览器接口继续使用 touch 行为，Agent Console introspection 明确 `touch=false`。不得改变现有登录有效期。

### 20.4 HTTP forward-auth

同域 `/agent-console/api/*` 的 Browser HTTP 请求先由 gateway 调用 dev-toolbox 的无 touch 验证入口，再转发 Relay。要求：

- Gateway 覆盖并移除客户端提交的内部身份 header。
- Relay 只信任来自 gateway 网络、且带有效内部服务证明的身份 header。
- 写请求必须把原始 Origin 与 `X-CSRF-Token` 交给 dev-toolbox 校验。
- Cookie 不继续转发给 Relay。
- Relay 响应不能设置或修改 dev-toolbox Cookie。

Caddy 与外置 Nginx 两种现有部署模式都要给出配置和最小配置测试。不得直接暴露 Relay 内部端口。

### 20.5 长连接撤销

Relay 至少每 60 秒 introspect 一次 Browser WebSocket 对应 auth session，并且不得刷新 idle。规则：

- 明确 invalid/revoked/expired：立即关闭，使用稳定 close reason。
- 达到原 session expires_at：无需远程查询也关闭。
- dev-toolbox 暂时不可达：停止接受写操作；在 2 分钟有界宽限内只允许已有只读订阅，恢复后重新校验；宽限结束关闭。
- 用户重新登录必须申请新 ticket，不复活旧连接。

### 20.6 允许的 dev-toolbox 数据改动

只允许新增 ticket 表和必要索引。不得让 Relay 直连 dev-toolbox 数据库，不得共享 auth master key，不得给 Relay 用户密码/TOTP 数据，不得新增 OAuth/OIDC 服务。

## 21. 设备绑定

Bridge 未绑定时建立受限 pairing 通道：

1. Bridge 生成高熵 challenge，同时请求 Relay 分配 challenge ID 和 6 位短码。
2. CLI 可输出短码和二维码数据；正式二维码 UI 留给 Tauri。
3. 已登录 Browser 输入短码或打开二维码深链接。
4. Browser 显示 Bridge 上报的设备名、macOS、架构和版本并确认。
5. Relay 限制每 challenge 尝试次数、来源速率和 5 分钟有效期。
6. 批准后 Relay 生成高熵 device credential，明文只经 pairing 通道发送一次。
7. Bridge 写入 Keychain；成功确认后 challenge consumed。
8. Relay 只保存 credential 摘要。
9. 撤销设备立即关闭 Bridge socket，后续凭据认证失败。

首版虽只有一台 Mac，数据库和路由仍使用 device ID；不要写死 singleton 行或全局连接。

## 22. 文件数据面

### 22.1 固定原则

- WebSocket 只协调 transfer，不传文件正文。
- Mac 只主动建立出站连接，不开放本机端口。
- Relay 不把正文写入磁盘、PostgreSQL 或日志。
- 不使用 MinIO、S3、kkFileView。
- 用户打开文件时才传输；时间线只传 metadata。

### 22.2 file_handle

Browser 不能提交本机绝对路径。Bridge 只对以下来源签发短期 handle：

- 当前会话上传或生成的文件。
- 当前会话工具、Diff 或文件变化明确引用的文件。
- 用户在已授权工作区内从会话上下文明确打开的文件。

handle 绑定：device、session、授权根、相对路径、允许动作、文件系统 identity、size、mtime/revision、过期时间。handle 使用高熵随机引用；Relay 不需要知道本机路径。

每次读取前：

- 重新从已打开授权根解析。
- Unix 阻止 symlink escape；Windows 暂不实现，但接口不得假设 Unix path 可移植。
- 打开后用文件描述符再次核对 identity，防止检查与使用之间替换。
- 文件变化返回 `FILE_CHANGED`，要求刷新 handle，不静默读取新目标。
- 不为版本检查扫描整个文件或计算内容哈希。

### 22.3 预览与下载策略

前端可直接预览的首版类型：

- UTF-8/可识别文本、源码、Markdown、JSON、日志。
- PNG、JPEG、WebP、GIF。
- PDF，支持单 Range。

HTML 与 SVG 默认按源码文本返回，不作为活动页面执行。其他格式使用 attachment 下载。DOCX/XLSX/PPTX 等不引入服务端转换。

首版默认限制：文本/源码内联预览最多 2 MiB；图片内联预览最多 25 MiB；PDF Range 预览文件最多 100 MiB；普通下载最多 512 MiB。超过预览限制但未超过下载限制时返回 `FILE_TYPE_NOT_PREVIEWABLE` 的大小原因并允许下载；超过下载限制返回 `TRANSFER_TOO_LARGE`。限制集中定义并在能力响应中暴露，不能散落在 handler。

Bridge 以实际 MIME 探测与允许列表为准，不能只信扩展名。响应设置 `nosniff`、受限 CSP、正确 `Content-Disposition` 和 `Cache-Control: private, no-store`。

### 22.4 Browser 下载/预览

1. Browser 通过已认证 HTTP 请求 preview/download，并带 file_handle 与可选 Range。
2. Relay 创建短期 transfer ID，通知对应 Bridge。
3. Bridge 复验 handle 与 Range。
4. Bridge 主动连接 Relay 的 device producer HTTPS endpoint，并使用 device credential + transfer token。
5. Relay 使用有界 channel 将 producer body 直接 pipe 给 Browser。
6. 任一端取消或超时，另外两端立即取消；不继续读取。

Relay 只支持单 Range；无效、多段或越界 Range 返回稳定错误，不能把整个文件作为回退。

### 22.5 Browser 上传

1. Browser 发起流式 upload，先声明文件名、MIME 和长度。
2. Relay 创建 transfer 并通知 Bridge。
3. Bridge 建立出站 consumer HTTPS 连接。
4. consumer ready 后 Relay 才持续读取 Browser body，并通过有界 channel 转发。
5. Bridge 写入自己的私有临时目录，执行实际大小和 MIME 校验。
6. 成功后生成只属于当前 session/turn 的 upload handle，再交给 Codex Desktop。
7. turn 接受、取消、失败或 TTL 到期后按生命周期清理；不得删除用户原始文件。

单文件初始上限 20 MiB。若前端需要更大文件，必须通过真实样本重新确认，不得自动提高到网关全局上限。

### 22.6 背压与资源

- 每 Browser 和每 device 限制并发 transfer；首版默认各 2 条。
- channel 固定小缓冲，不读取完整文件进内存。
- 文件流优先级低于审批、问题和 command result。
- producer/consumer rendezvous、首字节和空闲读取都有超时。
- Relay 重启或 Bridge 断开时 transfer 失败，Browser 显示可重试，不把 transfer 放入离线队列。

## 23. Git 与后台命令

### 23.1 Git 只读能力

仅对 session cwd 所属且已授权的 Git worktree 提供：

- 当前 branch；detached HEAD 明确显示。
- HEAD commit 短 ID 和完整 ID。
- repository/worktree 根的本机内部识别；Browser 只见安全显示名和相对路径。
- staged、unstaged、untracked、added、modified、deleted、renamed 状态。
- 总增删行；二进制文件标记为 binary。
- 按文件请求 staged/unstaged Diff。

允许使用系统 Git，但必须通过 `std::process::Command` 固定 executable 和 argv，禁止 shell 字符串。设置 `--no-ext-diff`、关闭 pager，并限制输出、时间和 pathspec。也可使用成熟 Git 库，但不得为了抽象同时维护两套实现。

禁止所有 Git 写操作。网页要求切分支、建 worktree 或提交时，产品路径是向 Codex 发送自然语言指令；Bridge 不提供专用写 API。

Diff 只按需加载，限制单响应大小。超限返回分页/截断 metadata 和文件下载选择，不把巨大 Diff 放入 SessionSummary 或 RuntimeSnapshot。

### 23.2 后台命令

展示原生可取得的：command ID、命令显示、cwd 安全显示名、开始时间、持续时间、状态和最近输出。

- 原生支持单项停止时暴露该 capability。
- 只有全局 clean 能力时只暴露“停止全部后台命令”，并在响应契约中带 warning code。
- 不提供 stdin、交互终端、任意 PID kill 或用户提交的系统命令。
- 无法可靠识别后台命令时显示 unavailable，不扫描系统全部进程猜测归属。

## 24. Web Push 后端

Relay 实现：

- 注册、更新、删除 Push subscription 的 HTTP API。
- VAPID/private key 只从 secret 文件或环境注入，不进仓库和日志。
- 触发事件：turn completed、failed、interrupted、等待问题、等待风险审批。
- 普通进度、输出增量和 Token 更新不推送。
- session mute 覆盖事件开关。
- 默认通知正文只写通用状态，不包含标题、prompt、文件名、分支或项目。
- 用户显式开启详细通知后可包含 title；不得包含正文。
- payload 带内部 session ID 和安全相对 deep link，不能带本机路径。
- Push 发送失败不改变任务真实状态；失效 endpoint 标记并清理。

ZCode 只实现 Relay 侧。Service Worker、权限请求、安装引导和通知展示由前端任务实现。自动化测试使用本地 fake push sender，不调用真实 Push 服务。

## 25. 隐私、安全和日志

### 25.1 Relay 可持久化

- 设备绑定与版本。
- SessionSummary。
- title/project display/branch，除非设备开启隐藏任务信息。
- 请求与审计元数据。
- Push subscription 与通知设置。

必须在文档中明确 title、project 和 branch 可能敏感，不能笼统宣称服务器“不含任何内容”。

### 25.2 Relay 不得持久化

- 用户 prompt 和 Assistant 回复。
- reasoning summary、plan 正文。
- 命令正文与输出。
- 问题/审批正文。
- Diff、文件内容、图片和 PDF。
- 绝对路径。
- 原始实时 EventBatch。

### 25.3 日志字段白名单

允许：request ID、内部 device/session ID、agent kind、operation、稳定错误码、字节数、耗时、sequence gap 数和版本。

禁止：Cookie、Authorization、WS subprotocol ticket、设备凭据、CSRF、Push endpoint、prompt、回复、输出、Diff、文件名、绝对路径、原生 IPC frame。

结构化日志必须按字段白名单构造，不能先记录完整请求再靠日志系统过滤。

### 25.4 进程权限

- Bridge 以当前用户运行，不请求管理员权限。
- Relay 容器非 root、只读根文件系统，只有必要临时目录。
- PostgreSQL 与 Relay 使用独立凭据。
- 只有 gateway 暴露公网 80/443。
- Relay internal/admin 端口不映射宿主机。
- 文件 producer/consumer 虽经公网 gateway 可达，但必须同时校验 device credential、短期 transfer token、目标 device 和方向。

## 26. 性能、资源与可靠性

### 26.1 首版目标

以下是联调目标，不应通过隐藏数据丢失来达成：

- 原生事件到 Browser 状态更新：健康网络 P95 不高于 500ms。
- snapshot 补偿的运行输出：P95 不高于 1.5s。
- Browser command 到 Bridge accepted：P95 不高于 1s，不含 Codex 执行时间。
- 临时断线后恢复当前任务：P95 不高于 5s。
- 2,000 条 SessionSummary 分页数据下首屏无需一次加载全部记录。

如果当前硬件无法达到，报告测量结果与瓶颈，不得通过删除审批可靠性、最终校正或鉴权降低延迟。

### 26.2 连接与心跳

建议初始值：

- heartbeat 15s。
- 连续约 45s 无有效 heartbeat 视为 offline。
- Bridge reconnect 指数退避 1s 到 30s，带 jitter。
- 正常网络恢复后立刻同步 capability、summary 和活跃 RuntimeSnapshot。

这些值集中定义，不散落 magic number；除真正部署变化值外不要建立庞大配置系统。

### 26.3 合并与分页

- 高频 output delta 在 Bridge 侧按约 50–100ms 或 64KiB 先到者合并。
- SessionSummary、HistoryPage 有稳定 cursor 与最大 page size。
- 前端是否虚拟滚动由 UI 任务决定；后端必须保证分页，不把虚拟滚动当作无限 payload 的补救。
- Diff 与完整 output 使用按需分页/流。

### 26.4 Tokio/Axum 边界

- 所有连接、subscriber、transfer channel 有容量上限。
- 不持有跨 await 的数据库事务或全局 mutex。
- 慢 consumer 被 resync/断开，不向上游无限反压。
- shutdown 时停止接受新命令，给已接受命令回执明确状态，关闭连接并刷入必要 metadata。
- 不对非幂等 Desktop 命令做自动新 ID 重试。

## 27. HTTP API 与前端契约

ZCode 必须在 `contracts/openapi.yaml` 冻结至少以下 HTTP 资源。实际 path 可保持此前缀，但不得把多个动作塞进万能 endpoint。

### 27.1 dev-toolbox API

- `POST /api/v1/agent-console/ws-tickets`
- 内部 ticket consume。
- 内部 auth session introspection/forward-auth verify。

### 27.2 Relay 管理 API

- 获取、重命名、撤销设备。
- 创建/查询/批准/取消 pairing challenge。
- 获取 SessionSummary 分页、置顶、静音、归档/取消归档。
- 重命名和 fork 的 Browser 操作最终必须通过 Bridge/Desktop capability，不能只改 Relay 摘要。
- 获取审计事件分页。
- 管理 Push subscription 与通知设置。

### 27.3 在线任务查询 API

- RuntimeSnapshot。
- HistoryPage。
- 完整 command output 分页。
- Git summary 与逐文件 Diff。
- file metadata、preview/download。

这些请求由 Relay 向在线 Bridge query；设备离线时不得返回服务器伪造正文。

### 27.4 WebSocket 命令

- create Desktop task。
- start turn。
- set/replace/cancel next-turn queue。
- steer。
- interrupt。
- answer question。
- answer approval。
- update session settings。
- rename、archive、unarchive、fork Desktop task。
- stop background command/all supported commands。

### 27.5 统一响应

JSON 错误格式至少包含：

~~~json
{
  "error": {
    "code": "STABLE_CODE",
    "message": "面向用户的简体中文说明",
    "requestId": "...",
    "details": {}
  }
}
~~~

前端不得依赖自由文本判断流程。Protobuf `ProtocolError` 与 HTTP code 使用同一稳定 code 集合。

分页默认值必须固定：SessionSummary 和 HistoryPage 默认 50、最大 200；完整命令输出单页最大 256 KiB；单个 Diff 响应最大 2 MiB。继续读取使用稳定 cursor，不使用页码加可变排序。

### 27.6 必需稳定错误码

至少定义并测试：

- `AUTH_REQUIRED`
- `AUTH_EXPIRED`
- `CSRF_INVALID`
- `WS_TICKET_INVALID`
- `WS_TICKET_EXPIRED`
- `WS_TICKET_CONSUMED`
- `DEVICE_OFFLINE`
- `DEVICE_REVOKED`
- `CODEX_UNAVAILABLE`
- `CODEX_VERSION_UNVERIFIED`
- `CONTROL_READ_ONLY`
- `CAPABILITY_UNSUPPORTED`
- `SESSION_NOT_FOUND`
- `STALE_TURN`
- `DUPLICATE_REQUEST_MISMATCH`
- `OUTCOME_UNKNOWN`
- `RESYNC_REQUIRED`
- `QUEUE_ALREADY_EXISTS`
- `QUEUE_PAUSED`
- `QUESTION_EXPIRED`
- `APPROVAL_EXPIRED`
- `SETTING_COMBINATION_UNSUPPORTED`
- `FILE_HANDLE_INVALID`
- `FILE_OUTSIDE_SCOPE`
- `FILE_CHANGED`
- `FILE_TYPE_NOT_PREVIEWABLE`
- `TRANSFER_EXPIRED`
- `TRANSFER_TOO_LARGE`
- `TRANSFER_RANGE_INVALID`
- `DIFF_TOO_LARGE`
- `RATE_LIMITED`
- `INTERNAL_ERROR`

## 28. 前端交接物

`docs/FRONTEND-INTEGRATION.md` 必须包含：

1. 本地启动 PostgreSQL、Relay、fake Bridge 和真实 Bridge 的命令。
2. 如何通过 dev-toolbox 获取 session/CSRF 和一次性 WS ticket。
3. WS subprotocol、Hello、Subscribe、Snapshot、Event、Ack、Resync 时序。
4. 每个 control operation 的状态前置条件与错误处理。
5. 多维状态如何组合成 UI 展示，不能给一个综合枚举让前端猜。
6. 输出 append/replace/final 的 reducer 规则。
7. 文件 preview/upload/download 的完整时序和取消方式。
8. Push 后端 API 与默认隐私文案。
9. Deep link 和登录 return path 规则。
10. Browser 断线、设备离线、auth 失效和 outcome unknown 的处理。
11. 所有 capability 如何决定控件是否显示/禁用。
12. curl 只读示例和一个 TypeScript WS 连接示例；示例中不得出现真实凭据。

`contracts/fixtures/` 至少提供脱敏 JSON fixture：

- device-online.json
- device-offline.json
- session-list.json
- runtime-running.json
- runtime-waiting-question.json
- runtime-waiting-approval.json
- runtime-completed-background-command.json
- runtime-read-only-unverified.json
- queue-queued.json
- queue-paused.json
- output-gap.json
- file-changed-error.json
- git-dirty-summary.json

fixture 必须通过与生产响应相同的 schema/类型校验；不得复制用户真实会话。

`packages/protocol-ts` 只能包含生成类型、编解码和稳定导出，不得包含 Vue store、fetch client、UI reducer 或样式。

## 29. 验证要求

### 29.1 Bridge 单元/集成测试

- Codex schema capability probe。
- IPC frame 分包、粘包、未知 frame 和尺寸上限。
- owner 变化与未知版本降级。
- history 分页与主/子 Agent 关系。
- 输出 append、缺口、replace 和 final 校正。
- request ID 重试、冲突和 stale turn。
- 单条 queue 的替换、取消、自动执行和暂停。
- question/approval 过期。
- file handle、symlink escape、TOCTOU、Range 和临时文件清理。
- Git 固定 argv、超时、detached HEAD、binary 和大 Diff。
- Keychain 使用接口以 fake backend 测试；测试日志无凭据。

### 29.2 Relay 测试

- Bridge/Browser 握手与协议版本协商。
- Subscribe → snapshot → buffered event 顺序。
- Ack/replay/resync/epoch change。
- 有界缓冲、优先级和慢 consumer。
- device credential 撤销。
- pairing 短码过期、重放、尝试上限和并发消费。
- request receipt 状态。
- Relay 重启后从 Bridge snapshot 恢复。
- transfer producer/consumer rendezvous、取消、超时、限流和无磁盘落地。
- Push 触发和静音，使用 fake sender。
- 数据库迁移从空库成功，回滚策略按项目约定说明。
- 日志捕获测试确认敏感字段不存在。

### 29.3 dev-toolbox 测试

- ticket 创建必须要求 session、Origin 和 CSRF。
- ticket 只显示一次，数据库只存摘要。
- 并发 consume 只有一次成功。
- expired/revoked session 无法创建和消费。
- introspection 不更新 last_seen。
- password change、logout、logout-all 和单 session revoke 后返回 invalid。
- 现有 `/api/v1/auth/session` 仍按原逻辑 touch last_seen。
- Caddy/Nginx 配置拒绝直接伪造内部身份 header。

### 29.4 无 UI 端到端测试

使用 fake IPC owner + Bridge + Relay + PostgreSQL + test Browser client，至少覆盖：

1. 设备已绑定并上线。
2. Browser 取得测试 ticket 并连接。
3. 订阅 session list 和详情。
4. 收到 snapshot 和 active event。
5. 发送 command，收到 Bridge accepted。
6. 模拟连接断开，在 accepted 回执丢失后用同一 request ID 重试，Desktop 只执行一次。
7. 模拟输出 gap，Browser 收到 resync 并取得 final output。
8. 模拟问题和审批并回传原生 option ID。
9. 模拟 1 个后台命令在 turn 完成后继续运行。
10. 文件预览流通过 Relay，但 Relay 临时目录和数据库中无正文。
11. 撤销 auth session 后 Browser WS 关闭。

### 29.5 真实 Codex Desktop 验证

只允许创建专用测试任务，不读取或修改用户项目文件。验证：

- 列表和历史。
- active/idle。
- steer。
- idle 后下一轮。
- 一个无风险的原生问题/审批路径，如无法安全触发则用 fixture 并明确未做真实验证。
- 编号输出的实时预览和 final 校正。

不得自动批准风险操作，不得为了测试修改 Codex 全局权限。

### 29.6 不要求的测试

- Windows。
- 真实 Web Push 外部调用。
- 生产部署。
- 全浏览器 UI E2E。
- MinIO、kkFileView、Redis 或多 Relay 集群。
- 第二 Agent Adapter。
- 大规模压测；只做能验证有界内存和目标延迟的窄测试。

## 30. 完成标准、停止条件与最终报告

### 30.1 完成标准

以下项目必须全部成立：

- `cargo test --workspace` 或仓库定义的等价最小后端入口通过。
- Protobuf 生成检查通过且工作树没有未解释的生成差异。
- `packages/protocol-ts` 可构建和导入。
- Relay migration 在空 PostgreSQL 运行成功。
- Bridge migration 在空 SQLite 运行成功。
- dev-toolbox 相关 Go 测试通过，现有认证核心测试未回归。
- 无 UI 端到端测试通过。
- OpenAPI、error codes、fixture 和实现一致。
- 敏感数据检查未发现正文/凭据进入 Relay 持久化或日志。
- `docs/FRONTEND-INTEGRATION.md` 足以让另一任务直接实现页面。
- `apps/web` 没有被 ZCode 修改。
- 未部署、未推送、未创建 tag。

### 30.2 必须停止并报告的条件

遇到以下情况不得自行换路线：

1. 无法确认 Desktop owner，却需要执行写命令。
2. Desktop 私有 IPC 无法稳定 start/steer/answer/approve；不得启用 App Server fallback。
3. Desktop 创建新任务无法落入同一 Desktop 会话列表。
4. turn terminal 状态无法可靠识别。
5. terminal 后仍无法取得权威最终输出。
6. 未验证版本必须通过猜测字段才能写入。
7. dev-toolbox 统一认证需要共享密码、TOTP、master key 或直接共享 auth 表。
8. HTTP 同域鉴权无法在不改变现有登录语义的前提下完成。
9. 文件流只有通过 Relay 落盘、MinIO 或无限内存缓冲才能实现。
10. 文件路径安全必须允许任意绝对路径才能工作。
11. 完成后端必须修改 UI 所有目录或大范围重构 dev-toolbox。
12. 需要新增本规格禁止的生产服务或付费资源。

停止报告必须写：失败的验收项、复现步骤、实际观测、已排除原因、受影响能力和最小可选决策。不得只写“技术限制”。未受影响的只读工作可以继续完成。

### 30.3 最终报告格式

ZCode 完成时只报告以下内容：

- 实际实现的能力，按 Bridge、Relay、dev-toolbox、数据面、契约分组。
- 修改的仓库和关键文件。
- 实际运行的测试命令及结果。
- 真实 Codex Desktop 已验证与仅 fixture 验证的能力分别列出。
- 前端接入入口、协议包、OpenAPI、fixture 和运行命令。
- 明确未实现项及原因。
- 所有触发的停止条件和需要用户决定的问题。

不得把未运行的检查写成通过，不得把 fixture 验证写成真实 Desktop 验证，不得宣称 Windows、多 Agent、无损实时终端或生产就绪。

## 31. 给 ZCode 的直接开工指令

在 `<workspace>/agent-console` 开始工作。先完整阅读根 `AGENTS.md` 和本文；涉及同级 `<workspace>/dev-toolbox` 前再阅读该仓库 `AGENTS.md`。严格按阶段 A 到 F 实现并验证，持续到第 30.1 节全部完成或命中第 30.2 节真实停止条件。

不要设计 UI，不要修改 `apps/web`，不要启动 Codex App Server，不要增加 MinIO/kkFileView/Redis/消息队列，不要执行 Git 写操作或生产部署。所有 Codex 真实验证使用专用测试任务且不得修改项目文件。交付必须到达前端可直接对接状态，而不是计划或脚手架状态。
