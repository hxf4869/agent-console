# Codex 兼容性矩阵(已验证版本 × 能力)

- 验证日期:2026-09-04
- 验证环境:macOS(darwin 25.6.0 arm64),ChatGPT Desktop bundle `152.0.7977.64`,内置 codex `codex-cli 0.153.0-alpha.5`(`/Applications/ChatGPT.app/Contents/Resources/codex`)。
- 协议细节见 `docs/CODEX-IPC-PROTOCOL.md`。
- 图例:
  - **VERIFIED**:在真实 Desktop 上执行并观察成功。
  - **方法级 VERIFIED**:方法本身已在真实 Desktop 上执行并观察成功,但对应产品 capability 因缺少动态可选值等条件保持关闭(如 update-thread-settings,见 §6/§9.2);不等于产品操作可用。
  - **FIXTURE_ONLY**:schema 来自 asar 逆向,仅经 fake server 回放测试,未在真实 Desktop 上执行。**该状态只证明 schema 可解析,不等于能力可用;任何依赖它的产品链路都不得宣称"已验证"。**
  - **UNSUPPORTED**:Desktop 该版本不提供此能力(经 asar 全量枚举确认)。

### 结论速览(三档交付语义)

| 档 | 含义 | 依据 |
|---|---|---|
| fixture 开发可开始 | 前端/联调可用 `contracts/fixtures/` 与 fake owner 驱动 UI 开发,不依赖真实写验证 | FIXTURE_ONLY 能力 + fixtures 生产类型校验(`apps/relay/tests/fixtures_schema.rs`) |
| 真实只读联调可开始 | 真实 Desktop 链路上列表/快照/历史/输出等只读能力已 VERIFIED,可进行真实只读联调 | §2 只读观察能力全部 VERIFIED |
| 0.153.1 的核心轮次控制 start/steer/interrupt 已验收 | start/steer/interrupt 已在 0.153.1 真机 VERIFIED(§9);设置写入仅方法级 VERIFIED(update-thread-settings,effort 实测),因 Desktop 未提供动态可选值列表,产品 UpdateSettings 保持关闭、生产白名单 NotProbed(§6/§9.2);审批与原生问题回答未自然出现(FIXTURE_ONLY),均不得在产品中标榜其可用 | §3/§4/§6/§9 |

## 1. 发现与连接

| 能力 | 状态 | 证据 |
|---|---|---|
| socket 路径发现(`~/.codex/ipc/ipc.sock`) | VERIFIED | 真实 socket 存在;Desktop asar 中 f9() 定义同路径 |
| 帧格式:u32 LE 长度前缀 + UTF-8 JSON | VERIFIED | 真实探针收发全部帧;asar 解码器 `m9` 同实现(上限 256 MiB,0/超长断连) |
| initialize 握手(clientType → clientId) | VERIFIED | 真实探针:`success` + `result.clientId`(uuid) |
| client-status-changed 广播(connected/disconnected) | FIXTURE_ONLY | asar 确认;真实探针未捕获到状态变化事件 |
| client-discovery-request 自动应答 canHandle=false | VERIFIED | 真实探针收到对 `ide-context` 的 discovery 并正确应答 |
| 版本探测(`codex --version`,固定 argv) | VERIFIED | 输出 `codex-cli 0.153.0-alpha.5` |
| 版本表(version 字段)与 hostId→Ev+1 规则 | VERIFIED | 真实探针:load-complete-history 以 version 2(hostId=local)被接受 |

## 2. 只读观察能力

| 能力 | 状态 | 证据 |
|---|---|---|
| thread-owner-discovery(发现会话 owner) | VERIFIED | 真实探针:`success`,`handledByClientId` = owner uuid,`result.supportsUntrustedAppInput=true` |
| owner 不存在 → `no-client-found` | VERIFIED(语义) | 路由器源码固定行为;真实探针未构造无 owner 场景(列表会话均在 Desktop 中打开) |
| following 订阅(thread-stream-following-changed) | VERIFIED | 真实探针:follow 后 owner 定向回发快照 |
| thread-stream-state-changed snapshot | VERIFIED | 真实探针:revision 从 1 起单调;conversationState 含 id/title/cwd/threadRuntimeStatus/turnHistory 等(只记结构) |
| thread-stream-state-changed patches(Immer 格式) | VERIFIED | 真实探针:baseRevision→revision 连续收到;格式 `{op,path[],value}` |
| thread-follower-load-complete-history(快照补偿) | VERIFIED | 真实探针:返回 `{revision: 8}`,期间 owner 连发新快照 |
| 会话列表(全部会话) | UNSUPPORTED(IPC) | IPC 无列表方法;列表由 Desktop SQLite(只读)提供,Bridge 另行实现 |
| 观察运行状态 idle/active(threadRuntimeStatus) | VERIFIED | 快照含 `threadRuntimeStatus{type,activeFlags[]}`(type 为 6 字符枚举,值域待写真验证) |

## 3. 写能力(全部要求先确认 owner)

| 能力 | 方法 | 状态 | 证据 |
|---|---|---|---|
| start 下一轮 | thread-follower-start-turn(v2;hostId→3) | VERIFIED(0.153.1,§9) | 0.153.1 专用会话真机逐操作验收:S2/S5 证据见 §9.1;0.153.0-alpha.5 仍为 asar schema + fake server 测试(写能力 NotProbed,见 §9.2) |
| steer 当前轮 | thread-follower-steer-turn(v1;hostId→2) | VERIFIED(0.153.1,§9) | 0.153.1 专用会话真机逐操作验收:params 带 restoreMessage{cwd,context} 后同轮 steer 被接受,R1 证据见 §9.1;0.153.0-alpha.5 仍为 asar schema + fake server 测试(写能力 NotProbed,见 §9.2) |
| interrupt 当前轮 | thread-follower-interrupt-turn(v4;hostId→5;legacy 3) | VERIFIED(0.153.1,§9) | 0.153.1 专用会话真机逐操作验收:S5 证据见 §9.1;0.153.0-alpha.5 仍为 asar schema + fake server 测试(写能力 NotProbed,见 §9.2) |
| 回答原生问题 | thread-follower-submit-user-input | FIXTURE_ONLY | asar schema(requestId + response);0.153.1 真机未自然出现原生问题(§9.1) |
| 命令审批 | thread-follower-command-approval-decision | FIXTURE_ONLY | asar schema;0.153.1 真机全程未自然出现审批(§9.1) |
| 文件审批 | thread-follower-file-approval-decision | FIXTURE_ONLY | asar schema;0.153.1 真机全程未自然出现审批(§9.1) |
| 权限审批 | thread-follower-permissions-request-approval-response | FIXTURE_ONLY | asar schema;0.153.1 真机全程未自然出现审批(§9.1) |
| MCP elicitation | thread-follower-submit-mcp-server-elicitation-response | FIXTURE_ONLY | asar schema |
| 变更线程设置(模型/思考深度/服务等级/权限) | thread-follower-update-thread-settings | 方法级 VERIFIED(§9;仅 effort 实测;产品 capability 关闭) | 0.153.1 真机:方法、Ev 版本与参数形状已验证(effort max→high 写入、快照反映、新轮正常、还原全通过,R2 证据见 §9.1,发送侧 Ev 版本修复后);但 Desktop 0.153.1 不提供动态可选值列表(availableValues 恒空),产品 Operation::UpdateSettings 保持关闭、生产白名单 NotProbed(§6/§9.2);model/serviceTier/permissionMode/collaborationMode 不可写 |
| 排队后续输入 | thread-follower-set-queued-follow-ups-state | FIXTURE_ONLY | asar schema;Bridge 侧单条下一轮队列走 start-turn(§9.1 已 VERIFIED) |
| 编辑上一条用户消息 | thread-follower-edit-last-user-turn | FIXTURE_ONLY | asar schema |
| 压缩会话 | thread-follower-compact-thread | FIXTURE_ONLY | asar schema |
| **新建 thread** | — | **UNSUPPORTED(IPC)** | asar 全量枚举:Desktop 在 IPC 上只注册 initialize/thread-owner-discovery/thread-follower-*;新建走 Desktop 私有 app-server 通道,不在 ipc.sock 上 |
| rename / archive / fork | — | UNSUPPORTED(IPC) | 仅有归档状态广播(thread-archived/unarchived),无 IPC 写方法 |

## 4. B.11 阶段门:Desktop 控制路径新建任务

- **结论:当前版本(ipc.sock)无法由外部进程新建 thread。** 按执行规格 §12“如果 Desktop 私有控制路径无法稳定创建任务,标记 capability unsupported 并触发阶段门”处理。
- 复现证据:
  1. asar(152.0.7977.64)`src-*` 模块中 `Tde` 注册函数是 Desktop 唯一的 IPC request handler 注册点,方法列表仅 `thread-owner-discovery` + 12 个 `thread-follower-*`;`initialize` 由路由器自身处理。
  2. asar 内容全量扫描(8525 个文件)仅 3 个文件命中 follower 协议字符串,均不含新建会话方法。
  3. 渲染进程新建会话使用 Desktop 私有 app-server 连接(stdio JSON-RPC,例如 `thread/start`、`thread/compact/start`),该通道不经过 `ipc.sock`,外部进程无法复用。
- 连锁影响:阶段 B.6–B.9(start/steer/审批/interrupt 的真实验证)要求在"专用测试任务"上执行;由于无法经 IPC 创建专用任务,这些能力的真实写验证全部停在 FIXTURE_ONLY,不得在远程写链路(阶段 C 之后)开启。**2026-09-04 更新:B.6(start)、B.7(steer,带 restoreMessage)与 B.9(interrupt)已在专用测试会话完成真机验证(§9.1);B.8(审批)未自然出现,维持 FIXTURE_ONLY,详见 §9。**
- 后续解锁条件:Desktop 版本升级后在 asar 中重新枚举;若出现新建方法(或官方公开控制接口),重跑本文件的写能力验证。

## 5. 输出真实性与最终校正

| 能力 | 状态 | 证据 |
|---|---|---|
| 执行中输出预览(patches 流) | VERIFIED(传输面) | patch 流可达; follower 不保证每段 delta(规格 §5 已验证事实,Desktop 侧同样按快照补偿设计) |
| 结束后权威重读(load-complete-history → 全量快照) | VERIFIED | 真实探针:补偿请求返回新 revision 且收到全量快照 |
| 输出逐字节无损实时终端 | UNSUPPORTED | Desktop 本身不提供;按 LIVE_PREVIEW/AUTHORITATIVE_FINAL 两级合同实现(§13) |

## 6. 版本门

- 版本判定与写能力注入分三层,互不共用同一判断:
  1. **只读协议兼容版本**:`VERIFIED_VERSIONS = ["0.153.0-alpha.5", "0.153.1"]`(asar 复核 + 真机只读复验;0.153.1 Ev 版本表 22 项零 diff,见 §8)。命中即 `CompatibilityState::Verified`,语义仅为协议/只读兼容,不等于任何写能力开放。
  2. **每版本实际验证过的写操作**:生产写白名单独立按版本注入(逐版本矩阵见 §9.2)——0.153.1 注入 start/steer/interrupt = Passed(§9.1 真机验证);0.153.0-alpha.5(写验证仍 FIXTURE_ONLY,§3)与一切未知版本的全部写操作保持 NotProbed;未知版本另加 DEGRADED + READ_ONLY 只读降级。
  3. **方法已验证但产品未开放的设置能力**:update-thread-settings 方法级已真机验证(§9.1,effort max→high),但因 Desktop 0.153.1 不提供动态可选值列表(availableValues 恒空),产品 UpdateSettings capability 保持关闭、生产白名单 update_settings = NotProbed;解锁条件与命令层防御见 §9.2。
- 版本表(asar `Ev`)随 Desktop 演进;`version` 字段不匹配时对端拒绝(`request-version-mismatch`),Bridge 的 capability probe 必须覆盖该表。

## 7. 任务创建路径调查(2026-09-04,共享 app-server daemon 假设)

- **结论:REFUTED——"通过 Desktop 已拥有的共享 app-server daemon 创建 thread"这条路径在本版本/本机配置下不成立。** §4 的 B.11 阶段门结论不变:ipc.sock 无新建方法,且不存在可复用的共享 daemon。按 §12 会话创建规则维持 capability unsupported。
- 调查问题:外部进程能否经 Desktop 拥有的共享 app-server daemon 新建 thread,使其(a)出现在同一 Desktop 列表且 `thread-owner-discovery` 能找到 Desktop 窗口 owner,(b)随后可经 ipc.sock `thread-follower-*` 控制。
- 证据链(全部只读取证,未连接任何 socket、未运行 `codex agents`/`daemon start`/`proxy`):
  1. **Desktop 的 app-server 是私有 stdio 实例,无监听 socket。** Desktop 拉起的 `codex … app-server`(argv 含 `features.code_mode_host=true` 与 `mcp_servers.codex_app`)无 `--listen` 参数(默认 `stdio://`);`lsof` 显示其全部 unix fd 均为有对端连接(0/1/2 = 与 Desktop 主进程的 stdio),无监听 socket。
  2. **CLI 的"共享本地 daemon"未运行。** codex 二进制字符串给出 daemon 固定布局:`$CODEX_HOME/app-server-daemon/{app-server.pid,app-server-updater.pid,daemon.lock,settings.json}` 与控制 socket `app-server-control/app-server-control.sock`;两者在 `~/.codex/` 下均不存在(`ls: …/app-server-daemon: No such file or directory`),即本机从未启动过 managed daemon。`codex agents` 自述连接的正是这个 daemon。
  3. **发现机制带自动拉起/临时 spawn 语义,禁止连接。** `codex app-server daemon start --help` 明示 "Start the local app server daemon **if it is not already running**";二进制字符串含 "skipping default app-server daemon socket"、"timed out probing default app-server daemon socket"、"failed to start embedded app server"、"Background server started. Run `codex agents` in another terminal"——即 daemon 不在时 CLI 走**内嵌临时 app-server**。连接探测本身就有产生第二个 app-server 进程的风险,且 Desktop 实例 socket 不存在、CLI 不可能"复用"它 → 按调查边界停止,不做连接。`codex app-server daemon version` 的 spawn 行为未验证,出于同一硬边界也未运行。
  4. **Desktop 默认不用 daemon;daemon 模式是 env 显式开启项。** asar 主进程 transport `connect()`:仅当 `CODEX_APP_SERVER_USE_LOCAL_DAEMON === "1"` 且 `CODEX_APP_SERVER_FORCE_CLI !== "1"` 且 `CODEX_CLI_PATH` 未设且 `codex app-server daemon version` 探测(2.5s 超时)通过时,才连 `app-server-control/app-server-control.sock`(websocket `ws://localhost/rpc`);否则回退 spawn 私有 stdio app-server。实测 Desktop 主进程与 app-server 进程均未设置 `CODEX_APP_SERVER_*` 环境变量 → 当前为默认私有 stdio 模式。结论:**"Desktop 创建 = daemon 创建"不成立**,Desktop 的"新建会话"落在其私有连接上的 `thread/start`/`thread/resume`/`thread/fork`,并按 connection 观察 `thread/started` 通知入列表。
  5. **即使 daemon 存在,daemon 创建的 thread 也不满足验收 (a)。** ipc.sock `thread-owner-discovery` 的 owner 是打开该会话的 Desktop 渲染窗口;daemon 属 CLI 管理的独立 app-server 实例,其新建 thread 在创建时刻没有任何 Desktop 窗口 owner(`no-client-found`),`thread-follower-*` 全部无从谈起。且启用 daemon 本身需要启动 App Server,命中规格 §5"不能用 App Server 创建平行任务"与 §31"不要启动 Codex App Server"。
  6. **Desktop 侧唯一真实的创建路径是 agent 工具 `create_thread`,不是外部 API。** Desktop 主进程经 browser-use 原生管道(`/tmp/codex-browser-use/<uuid>.sock`,实测由 ChatGPT 主进程持有)向会话内 agent 提供 `codex_app` 命名空间工具(`create_thread`/`send_message_to_thread`/`fork_thread` 等);asar 内置提示词明确 "Threads created this way are user-owned: they appear in the sidebar"。但该路径由模型在既有会话内调用、app-server 配置中 `approval_mode="prompt"`(需用户批准)、协议为 Desktop 私有管道——外部进程(Bridge)不可用,也不满足"由外部稳定创建"的验收。
- 进程安全留证:调查前后 `ps` 快照(`/tmp/ac-probe/ps-before.txt`、`ps-after.txt`)对比,codex/app-server 进程集合无变化(3 个 app-server:Desktop 的 82727 与两个先前存在的 `--listen stdio://` 实例),全程未产生第二个 app-server/daemon。
- 对 Bridge 实现的约束更新:任务创建能力维持 UNSUPPORTED(§4);新增禁止项——Bridge 不得调用 `codex agents`、`codex app-server daemon start/restart/bootstrap`、`codex app-server proxy`,也不得以任何 env 组合诱导 Desktop 进入 daemon 模式(需重启 Desktop,超出边界)。解锁条件与 §4 相同:Desktop 版本升级后重查 asar 中 `CODEX_APP_SERVER_USE_LOCAL_DAEMON` 默认值与 ipc.sock 新建方法;若未来 Desktop 默认运行 managed daemon 且 thread 落入其列表,重跑本节验证。

## 8. 版本矩阵:codex-cli 0.153.1(bundle 26.901.31953)复核

- 复核日期:2026-09-04(Desktop 当日 17:18 自动更新;`codex --version` = `codex-cli 0.153.1`,`CFBundleShortVersionString=26.901.31953`,`CFBundleVersion=7868`)。
- 复核方式:新版 asar 静态逆向(`.vite/build/src-VqXTPopo.js` 路由/客户端库 + `webview/assets/app-initial-caa927532ffb.js` 渲染进程)+ 真实 socket 只读探针(仅 initialize / thread-owner-discovery / thread-stream-following-changed / thread-follower-load-complete-history;**未发送任何写方法**,专用测试会话)。
- **判定:COMPATIBLE(协议未变,仅 conversationState 载荷新增键)。逐项证据见 `docs/CODEX-IPC-PROTOCOL.md` §10。**

| 能力 | 0.153.0-alpha.5 状态 | 0.153.1 状态 | 证据 |
|---|---|---|---|
| socket 发现 / 帧格式 / 权限 | VERIFIED | VERIFIED(不变) | 解码器逐行同旧版;实测 dir 0700 / socket 0600 |
| initialize 握手 | VERIFIED | VERIFIED | 真实探针 success + clientId |
| Ev 版本表与 hostId→Ev+1 规则 | VERIFIED | VERIFIED(表逐字节相同) | asar Ev 对象 22 项与 §4 相同;负版本探针:v1+hostId=local 被 owner 版本门禁拒绝(discovery 期 canHandle=false → 10s 后 `no-client-found`),v2 同刻成功 |
| thread-owner-discovery | VERIFIED | VERIFIED | 真实探针 success + `supportsUntrustedAppInput:true` |
| following 订阅 → 快照 | VERIFIED | VERIFIED | 真实探针:定向快照,broadcast version=11,revision 单调(1→2→4→5) |
| load-complete-history(v2) | VERIFIED | VERIFIED | 真实探针:`{revision:N}` 且 owner 重发全量快照 |
| thread-stream-state-changed patches | VERIFIED | VERIFIED(静态) | 渲染进程仍含 Immer `enablePatches`/`produceWithPatches` 与 `baseRevision` 通路;本轮会话空闲未捕获实时 patches(机制与 0.153.0 同源,无代码级变化) |
| conversationState 顶层键 | 21 键实测 | 39 键实测,**新增 17 键** | 新键清单见协议文档 §10.3;`projection.rs` 未知键容忍策略已覆盖,仅计数不投影 |
| thread-follower-* 写方法 schema | FIXTURE_ONLY | FIXTURE_ONLY(schema 不变) | asar 注册清单(13 个)与各方法参数读取字段逐一与 §3/§7 相同;无新增/删除/改名 |
| **新建 thread** | UNSUPPORTED(IPC) | UNSUPPORTED(IPC,复确认) | 全 asar 仅 1 处 `addRequestHandler` 字面注册(`thread-owner-discovery`)+ `Tde` 批量注册 follower 方法;仍无新建方法 |
| daemon 模式默认值 | 关闭(§7) | 关闭(不变) | 仍需显式 `CODEX_APP_SERVER_USE_LOCAL_DAEMON==="1"`,默认私有 stdio |

### 8.1 版本门与下一步(未执行,仅清单)

- `VERIFIED_VERSIONS`(`apps/bridge/src/adapter/codex/ipc/discovery.rs:21`)当前仍为 `["0.153.0-alpha.5"]`,0.153.1 被正确只读降级——本轮未修改。
- **写验证前置改动清单**(全部待写验证通过后由后续步骤实施,本轮不改代码):
  1. `discovery.rs::VERIFIED_VERSIONS` 增加 `"0.153.1"`(前提:0.153.1 真机写验证通过)。
  2. `messages.rs::table_version` **无需改动**(0.153.1 Ev 表与现常量完全一致)。
  3. 可选:`projection.rs::KNOWN_STATE_KEYS` 增补协议文档 §10.3 的 17 个新键(非必须:未知键容忍已生效,增补仅消除 doctor 计数噪音)。
- 0.153.1 写验证仍受 B.11 阶段门约束(无新建 thread 方法),仅可在既有专用测试会话上执行。
- 2026-09-04 更新:条目 1(`VERIFIED_VERSIONS` 增加 `0.153.1`)与条目 3(`projection.rs` 增补新键)已实施;`VERIFIED_VERSIONS` 现语义收敛为只读协议兼容版本表(§6),写能力白名单独立按版本注入。0.153.1 真机写验证已执行,结果见 §9(部分通过:start/steer/interrupt VERIFIED;update-settings 方法级 VERIFIED 但产品 capability 关闭;审批/原生问题回答 FIXTURE_ONLY)。

## 9. 真机写验证(2026-09-04,专用会话,codex-cli 0.153.1)

- 验证环境:与 §8 同机,`codex --version` = `codex-cli 0.153.1`;唯一写目标为专用测试会话(目录库只读断言 + 运行时快照断言 cwd 完全一致,IPC 发送层逐条断言 conversationId == 专用会话,不等即拒发)。
- harness:`apps/bridge/tests/real_desktop_write.rs`(S0-S9 剧本;提示词仅要求纯文本回复或只读 shell;不自动批准风险操作;不动 permission 档位;SQLite 只读;版本门禁不命中即零写入 abort)。
- 执行记录:完整剧本共两轮(第一轮失败原因见 §9.1 steer 条注;修复 harness 对 0.153.1 状态树的读键来源后整剧本重跑一次,以下为重跑结果)。第二轮结束现场已恢复:会话 idle、无 pending attention、`effort` 保持原值 `max`、thread 未归档(S9 断言 + 事后只读快照复核一致)。
- 收尾诊断重试(同日第二轮,聚焦剧本 `real_desktop_retry_steer_and_settings`,仅覆盖 steer 与设置两项):steer 失败根因是 params 缺 `restoreMessage`(owner 侧读其 `cwd` 字段),按协议 §7.2 补齐后 PASS;update-settings 失败根因是发送侧版本 bug(`table_version` 漏 `thread-follower-update-thread-settings` Ev=1 → 错发 1,owner 期望 2 → 被拒后路由器回 `no-client-found`),修复 `apps/bridge/src/adapter/codex/ipc/messages.rs` 后 PASS。最终一轮 ALL STEPS PASS:R1 steer(接受/同轮/序列 1..30 完整/含 done/completed/轮数差 0)、R2a 写入(max→high)被接受且快照反映、R2b 变更后新轮正常、R2c 恢复原值且快照确认、R-final idle/无 attention/未归档。过程发现:effort=minimal 变更后新 turn 在 Desktop 内无终态,会话停留 `threadRuntimeStatus.type="systemError"` 不回 idle(外部投影仍显示 IDLE);该态下 follower 直接 start-turn 仍被接受,turn 完成后回 idle(恢复测试 `real_desktop_recover_system_error` PASS,现场已复原)。

### 9.1 操作级结论

| 写方法 | 状态 | 证据(状态迁移与断言) |
|---|---|---|
| thread-follower-start-turn | VERIFIED | S2:start 被接受,idle→active→completed;终态权威重读(load-complete-history)后 assistant 回复含 ok;期间 patch 自动补偿 0 次;S5 收尾新轮同样正常完成(会话可继续) |
| thread-follower-interrupt-turn | VERIFIED | S5:数数输出开始后 interrupt(user-stop + expectedTurnId)回执 ok=true 且 interruptedTurnId 匹配目标轮;会话回 idle;权威终态 status="interrupted";随后新轮正常完成 |
| thread-follower-steer-turn | VERIFIED | S4+R1:params 按协议 §7.2 补 `restoreMessage{cwd, context:{workspaceRoots:[cwd], collaborationMode:<快照 latestCollaborationMode 实测值>}}` 后,turn 活动中 steer 被同轮接受(无 peer error;此前的 `undefined (reading 'cwd')` 为 owner 侧读该缺省字段所致);聚焦重试剧本两轮真机 PASS:同轮(轮数差 0)、序列 1..30 完整、含 done、turn completed |
| thread-follower-submit-user-input(回答原生问题) | FIXTURE_ONLY | S6:240s 内专用会话未自然出现原生问题(会话正常结束);未猜测字段发送 |
| thread-follower-command/file/permissions-approval-decision | FIXTURE_ONLY | S7:全程未制造风险操作,自然未出现原生审批(结构计数 0) |
| thread-follower-update-thread-settings | 方法级 VERIFIED(仅 effort;产品 capability 关闭) | 方法、Ev 版本与参数形状已验证:根因为发送侧版本 bug(`table_version` 漏 Ev=1 错发 version=1,owner 期望 2 → canHandle:false → 路由器超时回 `no-client-found`(discovery 成功后同刻仍被拒,"owner 离线"解读不成立));修复后 R2 真机全链 PASS:effort max→high 写入被接受(R2a)、快照反映新值、变更后新一轮正常完成、恢复原值并断言还原(R2c,两次写入均被接受);未触碰 permission;附:effort=minimal 变更后新 turn 在 Desktop 内无终态并使会话停留 `threadRuntimeStatus.type="systemError"`(该态下 start-turn 仍被接受,完成后回 idle)。产品层:Desktop 0.153.1 不提供动态可选值列表(availableValues 恒空),Operation::UpdateSettings 保持关闭、生产白名单 NotProbed(§6/§9.2) |

- steer 条注:第一轮剧本的 S2-S6 全部失败是 harness 自身问题 —— 0.153.1 该会话 `historyMode="paginated"`,`turns[]` 数组恒空,turn 对象实际存于 `turnHistory.history.entitiesByKey`(按 `turnStartedAtMs` 排序);harness 已改为兼容两种来源。该问题只影响观测读键,不影响已发送的写本身。
- 输出预览(观测面,S3):执行中命令输出经 patches 实时可见(观测到 51 字节命令输出),但单轮内仅一次到达,未满足"多次增长"门;终态权威重读输出 1..20 完整无缺号 —— 与 §5 "LIVE_PREVIEW 尽力而为 + AUTHORITATIVE_FINAL 权威" 两级合同一致,§5 结论不变。

### 9.2 生产白名单与版本条件

- 生产写白名单按版本独立注入(`apps/bridge/src/main.rs`),不与只读兼容版本表(`VERIFIED_VERSIONS`,§6)共用判断;逐版本矩阵:
  - **0.153.1**:start-turn、steer-turn(须带 restoreMessage)、interrupt-turn 注入 `ProbeResult::Passed`(§9.1 真机验证);answer-question = `NotProbed`(真机未自然出现);update-settings = `NotProbed`(方法级已验证但产品未开放,见下)。
  - **0.153.0-alpha.5**:全部写操作 `NotProbed`(写验证仍 FIXTURE_ONLY,见开头图例;不得凭 schema 开启)。
  - **未知版本**:全部写操作 `NotProbed`,另加 DEGRADED + READ_ONLY 只读降级。
- steer 约束:生产链路发送 steer 时必须构造 `restoreMessage{cwd, context:{workspaceRoots:[cwd], collaborationMode:<快照 latestCollaborationMode>}}`,缺省即复现 owner 侧 `undefined (reading 'cwd')`(0.153.1 实测)。
- 设置写入约束(UpdateSettings 保持关闭期间的命令层防御;将来开放也按此收紧):仅接受 `ReasoningEffort`,值必须存在于 owner 动态下发的 availableValues,空列表一律以 `SETTING_COMBINATION_UNSUPPORTED` 拒绝;model/serviceTier/permissionMode/collaborationMode 不可写。已实测字段仅 `effort`(max→high 往返);`minimal` 在 0.153.1 实测会引发 turn 无终态并使会话停留 systemError,同样只在真实出现于动态选项时才允许。
- UpdateSettings 解锁条件:Desktop 后续版本提供可靠动态 availableValues 后,按上一条约束开放;开放前不得凭方法级 VERIFIED(§9.1)宣称设置写入可用。
- CREATE_TASK / RENAME / ARCHIVE / UNARCHIVE / FORK / 后台命令停止:IPC 无对应方法(§3/§4/§7),保持 UNSUPPORTED,不在白名单。
