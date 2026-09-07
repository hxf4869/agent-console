# ZCode 接入（ZC-01 原型 + MCP 问答 + 状态观察 + Z3 调查）

> 状态：原型（ZC-01）+ 正式身份路由（ZC-02）。本文件只陈述已验证事实与
> 已实现通路；FIXTURE 与 NATIVE 证据严格区分，禁止把 fixture 结果表述为
> 真实可用。关联任务书：04-后续阶段方案 §8；联调记录模板：03 §3/§4/§5。
> ZC-02 交付：proto `AgentKind` 增补 `ZCODE_DESKTOP = 2`（枚举值只追加）；
> Bridge runtime 缓存/输出/watcher/队列/详情流按 `(agentKind, nativeSessionId)`
> 槽位隔离；Relay TargetTag/上游流键含 agentKind（DB 唯一键 0003 起已含，
> 无需迁移）；web session key 缓存/订阅/命令携带 agentKind，ZCode 卡片在
> UX-01/05 体系内展示并标注"能力来源：官方 Hook"；ZCode 写能力仅 Hook
> 审批/问答，start/steer/interrupt/历史/输出/队列/Git 明确
> CAPABILITY_UNSUPPORTED。同机同 nativeSessionId 隔离回归见
> `apps/bridge/tests/dual_agent_same_id.rs`。

## 1. 官方能力核对（2026-09-06，本机 ZCode Desktop 3.11.2）

| 事实 | 来源 |
|---|---|
| Hook 事件共 7 个：`SessionStart` / `UserPromptSubmit` / `PreToolUse` / `PermissionRequest` / `PostToolUse` / `PostToolUseFailure` / `Stop` | 官方文档 zcode.z.ai/en/docs/hooks + 本机官方插件 `zcode-guide/diagnosing-hooks` |
| `PermissionRequest` stdin：`session_id` / `transcript_path` / `cwd` / `permission_mode` / `hook_event_name` / `tool_name` / `tool_input`；`tool_use_id` 仅实际携带时存在 | 同上（官方样例逐字段） |
| `PermissionRequest` 输出：`{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}`；deny 带 `message`；不能越过硬性权限限制 | 同上 |
| Hook 配置按**新会话**快照取得；根级默认超时 60000ms；stdout 上限 32KiB；exit 0=成功 / 2=阻断 | 同上 |
| 项目级 `<workspace>/.zcode/config.json` 的 hooks **不执行**（日志记 `config_project_hooks_ignored`） | 官方文档；用户配置 hooks 需 `hooks.enabled:true`；插件 hooks 自动启用 runner |
| matcher 是大小写敏感正则；`"*"` 不是合法通配（非法正则静默不匹配）；全匹配应**省略 matcher** | 本机官方插件 diagnosing-hooks |
| MCP 配置：用户 `~/.zcode/cli/config.json → mcp.servers`；stdio 需要 `command`（+`args`/`env`/`timeoutMs`），schema 严格（未知键丢弃） | 官方文档 + 本机官方插件 diagnosing-mcp |
| 本机日志证据：用户配置的 stdio server 以 `mcpIsolation:"session"` **每会话独立连接**（`mcp.pool.lease.acquired`，leaseId 绑定 sessionId） | `~/.zcode/cli/log/zcode-*.jsonl` |
| Remote Control 官方页面：手机页面 + Bot Channel；**无公开第三方附着 API/协议** | 官方文档 remote-control |

## 2. 已实现通路（本轮交付）

```text
ZCode 原生 PermissionRequest
  │ stdin 单行 JSON（官方字段）
  ▼
helper：bridge zcode-hook（apps/bridge/src/zcode/helper.rs）
  │ Unix socket（data_dir/zcode-hook.sock；0700/0600；peer euid 校验）
  ▼
Bridge hook server（apps/bridge/src/zcode/server.rs）
  │ PendingRegistry 内存注册表（pending.rs；状态机+原子决定）
  ├─ 现有事件通道：PendingAttentionAdded / PendingAttentionRemoved
  │   （runtime.rs publish_external_event；不改 proto）
  └─ 浏览器命令 AnswerApproval(allow|deny) / AnswerQuestion 命中注册表
     → 原子决定 → 回执 COMPLETED / APPROVAL_EXPIRED
  ▼
helper stdout：官方 decision JSON（空输出 = 回原生确认，不自动允许）
```

MCP 问答（`bridge mcp-stdio`，`agent_console.ask_user`）复用同一注册表；
工具结果 `answered / cancelled / expired`；`notifications/cancelled` 在
tools/call 等待期间仍被处理（调用在独立任务等待），被取消请求不回响应。

#### 交付确认（ack 合同）

决定写回 helper 后，socket server 有限等待 3s，只接受绑定该 `invoke_id`
的确认行；helper 在官方 decision JSON 实际写出 stdout 成功后才回 ack，
MCP 侧在 JSON-RPC 结果写出成功后才回 ack。确认到达 → Delivered → 回执
`RECEIPT_COMPLETED`——它只表示"已确认输出原生协议结果"，不代表工具已在
ZCode 中执行；写回失败、确认缺失或超时（含旧版 helper 不回 ack 的安全
收尾）一律按 `OUTCOME_UNKNOWN` 处理，不自动重新投递。runtime 命令面对
交付确认有限等待 5s 后出终态回执。

### 关键设计决定

- `invoke_id` 由 helper 生成（UUID v4）；同命令两次执行 = 两个独立请求。
- ZC-01 原型期以 `zcode:` 前缀命名空间隔离；ZC-02 起改为正式
  `SessionKey { agent_kind: ZCODE_DESKTOP, native_session_id }`
  （MCP 问答 = 独立 `plugin-ask` 会话键），与同机 Codex 会话按
  agent_kind 维度隔离；同机同 native id 双 Agent 隔离由
  `tests/dual_agent_same_id.rs` 回归锁定。
- 审批卡片固定二元决定（allow/deny），不创造"永远允许"；toolInput 仅
  本机内存摘要展示（≤400 字符），不进日志/不落盘/不拼 shell；>256KiB
  整单拒绝。
- deadline：远程等待默认 45s（<官方 60s 预算）；超时 → Expired → helper
  空 stdout 回原生确认；连接断开（原生取消/helper 消失）→ HandledLocally
  → 迟到决定一律 `APPROVAL_EXPIRED`/`Cancelled`。
- Bridge 重启 = 注册表消失 = 旧 pending 全部失效（无可复活路径）。
- 决定前连接出现 EOF/多余输入即撤销远程卡片。

### MCP session 绑定判定（04 §8.8）

判定：**分支②（运行时支持每会话子进程/连接绑定）+ invokeId 兜底**。
证据：本机日志 `mcp.pool.lease.acquired` 显示用户配置的 stdio server
（serena、context7）以 `mcpIsolation:"session"` 独立连接并绑定 sessionId；
同时实现不依赖该行为——每个 ask_user 调用是独立 invoke，回答按 invokeId
路由，即使隔离模式退化为 workspace（跨会话共享进程）也不会串线，只是
展示分组退化为"插件问答任务"（分支③展示）。未发现 MCP 工具调用携带
原生 session 身份的文档或证据，不按 cwd/最近会话猜测（分支①不成立）。

## 3. 最小状态观察（04 §8.7）

- 数据结构：`observe.rs::ZcodeSessionMeta`（discovered / turn_started /
  turn_running / awaiting_approval / last_tool_ok / turn_stopped）+
  `HookStatusEvent` 映射（纯函数）。
- 不读 transcript、不复制对话；Stop ≠ session 关闭；无 token stream 时
  能力表述为"运行中（最后状态更新于 Hook 事件）"。
- fixtures：`integrations/zcode/fixtures/*.sample.json`（7 份官方事件
  样本），由 `tests/zcode_hooks.rs::repo_fixtures_parse_and_map_to_minimal_metadata`
  消费。web 展示归后续任务。

## 4. 本机部署状态（2026-09-06）

- Hook/MCP 已通过 `integrations/zcode/scripts/install.sh` 注册到用户配置
  （`~/.zcode/cli/config.json`，备份
  `config.json.bak.ac-zcode-20260906_040039`）：6 事件用户级 hooks +
  `mcp.servers["agent-console"]`；既有 9 个 MCP server 未触碰。
- 插件副本：`~/.agent-console/zcode-test/plugin/`（占位符已替换）。
- **恢复**：`integrations/zcode/scripts/restore.sh`（已在配置副本上验证：
  精确移除本仓库条目，回到安装前形态）。
- 生效条件：ZCode **新会话**（配置按会话快照取得）。

## 5. NATIVE 验证入口（待用户参与，BLOCKED_AUTH）

前置：`bridge run`（已配对设备）保持运行。

1. 在 ZCode Desktop 打开一个无害测试工作区，**新建会话**（Hook 配置仅新
   会话生效）。
2. 让 Agent 执行一条会触发权限请求的无害命令（例如让它在受限权限下跑
   `ls`）。触发后：
   - 远程允许一次：在 Agent Console 浏览器/手机卡片点「允许」；
   - 远程拒绝一次：触发新请求后点「拒绝」；
   - 超时回退：触发后不操作，等 45s → 观察回到原生确认。
3. 可自动化的对照已由 FIXTURE 测试覆盖（见下）；需要人 deciding 的两次
   点击即上述步骤 2 前两项。
4. 验证后如需还原：执行 `integrations/zcode/scripts/restore.sh`，并在
   Settings → Plugin Management / MCP 核对无残留。

## 6. Z3 调查结论（只读，2026-09-06，Desktop 3.11.2）

结论：**未发现可附着当前 Desktop 的原生协议入口；按 04 §8.10 该分支停止
实现，交付停留在 ZC-01/Z2 真实能力。**

证据链：

1. 官方 Remote Control 文档明确无第三方附着 API（仅手机页面 + Bot
   Channel，链接即凭据）。
2. 本机无独立 `zcode` CLI 可执行（`which zcode` 无；PATH 无）。
3. 进程取证：`zcode-cli`（两实例）与 `zcode-host-local-1` 均为 Desktop 的
   Electron Helper 进程（进程名重命名），非可独立调用的运行时；desktop ↔
   agent 经进程内 `zcode_protocol` entrypoint 通信（日志元数据）。
4. 网络取证：全部 ZCode/zcode-cli/zcode-host 进程无 TCP LISTEN socket；
   唯一本地 unix socket 为 computer-use broker（token 文件保护，属 GUI
   自动化通道，非会话控制 API，且该类方案在范围内明确排除）。
5. `~/.zcode/v2/config.json` 为 OpenCode schema（内嵌 runtime 配置）；即
   使其支持 server 模式，也属"启动新运行时"，不满足"附着当前 Desktop"。
6. asar 静态检索 `mcpIsolation`/`mcpSource` 仅命中遥测 schema，无对外
   控制面。

解锁条件：Desktop 版本升级或官方公开本地控制 API 后，重跑 §8.10 步骤
1–4（只读握手/列会话/owner 识别先行）。

## 7. 测试与命令

- FIXTURE/AUTO 全场景：`cargo test --locked -p bridge`（本轮全绿；含
  `tests/zcode_hooks.rs` 6 项：socket 权限 / peer euid / 全链路 allow、
  deny / 重复 invoke 拒绝 / 超时+迟到拒绝 / EOF 取消 / cancel 事件 /
  状态事件 / 命令拦截与事件通道 / 过期回执 / 同机隔离 / fixtures 同步）。
- 单测覆盖：参数解析与 stdout 协议、socket 权限、invokeId 唯一性、
  deadline 分支（决定/超时/不可达/取消）、原子决定与迟到拒绝、Bridge
  重启失效、同输入两次独立、MCP 协议层与取消传播。
- 真实 ZCode NATIVE 往返：BLOCKED_AUTH（§5 入口已就绪）。
