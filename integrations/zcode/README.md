# integrations/zcode — ZCode Hook 审批原型（ZC-01）

本目录是 agent-console 对 ZCode 桌面 Agent 的本机 Hook 原型（04-后续阶段方案
§8.3–§8.8）。范围：原生 `PermissionRequest` → 本机 helper → Bridge 内存
pending → 现有事件通道（远程卡片）→ 原子决定 → 同一 helper stdout。

**状态：原型。** 本目录内容不宣称"手机审批可用"；浏览器卡片渲染、ZCode
agentKind 正式路由（ZC-02）均属后续任务。

## 目录结构

```text
integrations/zcode/
├── README.md                         # 本文件
├── plugin/
│   ├── .zcode-plugin/plugin.json     # 插件 manifest（name 必填）
│   └── hooks/hooks.json              # Hook 注册（/ABSOLUTE/PATH/ 占位符）
├── fixtures/                         # 官方事件输入样本（脱敏，测试消费）
└── scripts/
    ├── install.sh                    # 本机测试安装（备份 + 防重复 + 不覆盖他人条目 + 可恢复）
    ├── restore.sh                    # 按安装清单恢复安装前的注册状态
    └── test-install-restore.sh       # install→restore 全流程副本测试（临时目录，不碰真实配置）
```

## 工作原理

1. **helper**：`bridge zcode-hook`（bridge crate 的受控子命令）。stdin 读
   ZCode 官方单行 JSON；PermissionRequest 经本机 Unix socket（默认
   `<data_dir>/zcode-hook.sock`，目录 0700 / socket 0600，macOS 校验 peer
   euid）送 Bridge；等待远程决定约 45s（官方 Hook 预算 60s 内）。
   - allow → `{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}`
   - deny → 同结构 `behavior:"deny"` + message
   - 超时 / Bridge 不可达 / 过期 → **空 stdout + 退出码 0**，回 ZCode 原生
     确认（不自动允许）。
   - 其余事件（SessionStart/UserPromptSubmit/PostToolUse/PostToolUseFailure/
     Stop）转发为状态元数据，即时返回，不干预会话。
2. **MCP 问答**：`bridge mcp-stdio`，工具 `agent_console.ask_user`，
   复用同一 pending 通路，返回 answered/cancelled/expired。
3. **Bridge 侧**：`bridge run` 启动 hook socket；pending 以
   `PendingAttentionAdded/Removed` 事件走现有出站通道；浏览器命令
   `AnswerApproval`（allow/deny）与 `AnswerQuestion` 命中注册表时原子决定。

## 已知约束（原型轮，如实声明）

- **项目级 `.zcode/config.json` 的 hooks 不执行**（官方文档明确），不要把
  hooks 写进仓库的 `.zcode/`。
- matcher 是大小写敏感正则：`"*"` 不是合法通配（正则中会被静默忽略），
  需要"全部匹配"时**省略 matcher 字段**（本目录 hooks.json 已省略）。
- Hook 配置按**新会话**取得；已运行会话不受配置修改影响。
- 官方 stdout 上限 32KiB：helper 的 stdout 只输出协议 JSON；诊断走 stderr。
- 原始 toolInput 只在本机内存中用于授权展示摘要，不进日志、不落盘；
  超过 256KiB 的输入整单拒绝，不截断后批准未知动作。
- Bridge 重启后旧 pending 一律失效（注册表仅内存）。
- 浏览器卡片 UI 未适配（web 归后续任务）；本轮数据通路已通，事件在
  Bridge 边界可见。

## 安装（本机测试；需要用户已授权）

```bash
integrations/zcode/scripts/install.sh            # 默认 debug 构建
BRIDGE_BIN=/path/to/bridge install.sh            # 指定已构建二进制
```

install.sh 做三件事（全部先备份、防重复、可恢复）：

1. 把 `plugin/` 复制到 `~/.agent-console/zcode-test/plugin/`，并将
   hooks.json 中 `/ABSOLUTE/PATH/` 替换为实际 helper 绝对路径（插件方式
   启用：ZCode **Settings → Plugin Management → Discover → + 添加本地
   marketplace/目录**，选择该副本；插件 Hook 启用后 hook runner 自动生效）。
2. 在 `~/.zcode/cli/config.json` 注册**用户级测试 hooks**
   （`hooks.enabled: true` + 六个事件 → helper；与插件方式二选一即可，
   **不要同时启用，否则同一原生请求会产生两条远程卡片**——install 脚本
   会检测并拒绝重复注册）。
3. 在 `~/.zcode/cli/config.json` 的 `mcp.servers` 注册 `agent-console`
   stdio MCP（`<helper> mcp-stdio`）。

所有备份写在 `~/.zcode/cli/config.json.bak.ac-zcode-<时间戳>`。

### 管理边界（只动本项目拥有的条目）

install.sh 对用户配置的写入以**识别标记**划界，用户已有配置一律不动：

- **本项目 hook 条目**：`type: "process"` 且 `args` 含 `"zcode-hook"`
  （本项目 helper 专属子命令，普通配置不会使用）。同一条目内用户自己的
  hook 不受影响；安装即整条目级替换为本项目当前 helper 的新条目，重复
  安装不产生重复条目。
- **本项目 MCP 条目**：`mcp.servers["agent-console"]` 且
  `args == ["mcp-stdio"]`。同名条目若非该形态（用户自己配置的
  agent-console），install **拒绝安装并退出（退出码 1），不写任何配置、
  不覆盖**；如需注册请先移除或改名用户条目。
- `hooks.enabled` 会被 install 置为 `true`（hook 注册生效的前提），原值
  见下方安装清单，restore 时还原。

### 安装清单

被修改键的原值记录在 `~/.agent-console/zcode-test/install-manifest.json`
（与时间戳备份分开；重复安装保留第一次记录的最初原值）：

```json
{
  "version": 1,
  "installed_at": "…",
  "helper": "/abs/path/to/bridge",
  "config_path": "/Users/…/.zcode/cli/config.json",
  "hooks_previous": { "present": true, "enabled": false }
}
```

`hooks_previous.present` = 安装前是否存在 `hooks` 键；`enabled` = 安装前
`hooks.enabled` 的值（键不存在时为 `null`）。restore.sh 以此为还原依据。

## 验证（新会话；配置按会话快照取得）

1. 重启或新建 ZCode 会话（工作区选一个无害测试目录）。
2. `~/.zcode/cli/log/zcode-<date>.jsonl` 应出现插件/配置 hook 解析记录。
3. 让 ZCode 执行一条会触发权限请求的无害命令（如 `ls` 在受限模式下）。
4. Bridge 侧（`bridge run` 已连接）应收到 pending 卡片事件；本机人工在
   桌面原生确认上允许/拒绝 = 对照组；远程决定链路见仓库
   `apps/bridge/tests/zcode_hooks.rs`（FIXTURE 级全场景）。

## 恢复

```bash
integrations/zcode/scripts/restore.sh
```

只恢复 install.sh 做过的改动（有改动时先做时间戳备份再写文件；无改动
不写）：

- **hooks 条目**：从各事件条目中移除本项目 hook（`args` 含 `zcode-hook`
  的 process hook）；用户同一条目内的 hook 原样保留；纯本项目条目整条
  移除；事件为空则删除该事件。
- **`hooks.enabled` / `hooks` 键**：按安装清单还原安装前原值；安装前无
  `hooks` 键且恢复后只剩空壳时，整体删除该键。
- **`mcp.servers["agent-console"]`**：仅当条目为本项目形态
  （`args == ["mcp-stdio"]`）时移除；用户自己配置的同名条目一律不动；
  因本项目而变空的 `servers`/`mcp` 层级一并删除。
- **旧版脚本安装（无清单）兼容**：从最早的、不含本项目条目的
  `.bak.ac-zcode-*` 备份推断安装前 hooks 状态并还原；推不出则不动
  `hooks.enabled` 并提示对照备份。
- 插件副本目录与安装清单 `~/.agent-console/zcode-test/` 一并删除。若曾
  通过 Settings UI 启用插件，请在同一界面禁用/移除。

副本级回归测试（临时 HOME，不触碰真实配置）：

```bash
bash integrations/zcode/scripts/test-install-restore.sh
```

覆盖：enabled=false 还原、用户同名 MCP 拒绝安装、用户 hook 混合条目共存、
重复安装幂等、干净配置 install→restore 语义等价、旧版安装状态（无清单）
按安装前备份还原。
