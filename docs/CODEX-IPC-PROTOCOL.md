# Codex Desktop IPC 协议(逆向文档)

- 对象:`codex-cli 0.153.0-alpha.5` / ChatGPT Desktop(bundle `152.0.7977.64`,asar `app.asar` 模块 `src-*.js`、`app-initial-*.js`);**2026-09-04 已对 `codex-cli 0.153.1`(bundle `26.901.31953`)完成只读复核,结论:协议逐项未变,见 §10。**
- 来源:Electron 主进程 asar 反汇编(`IpcRouter` / `IpcClient` / `IpcRouterManager` / thread-stream manager)+ 对 `~/.codex/ipc/ipc.sock` 的真实只读探针(2026-09-04)。
- 隐私:本文档只记录 schema、方法名、字段名与常量,不包含任何会话内容、用户路径或凭据。
- 本协议为私有协议,只能被 Bridge 的 Codex Adapter 内部使用;不得把原生 JSON 透传到 Browser。

## 1. 传输与帧格式

- 传输:Unix domain socket,默认路径 `~/.codex/ipc/ipc.sock`(`$CODEX_HOME/ipc/ipc.sock`);备选路径 `$(tmpdir)/codex-ipc/ipc-<uid>.sock`。Windows 为 named pipe `\\.\pipe\codex-ipc`(首版不涉及)。
- socket 目录权限:0700 且必须属于当前用户;socket 文件权限 0600。Desktop 启动时若目录/socket 属主不对则拒绝启动。
- 帧格式:**4 字节小端无符号长度前缀 + UTF-8 JSON payload**。
- 长度校验:`len == 0 || len > 268_435_456`(256 MiB)→ 对端视为协议错误并直接断开连接。Bridge 侧上限应远小于此(见 §12 规格),建议默认 64 MiB。
- 服务端(路由器)不主动发帧;连接建立后客户端必须先发 `initialize`。
- 谁监听:任何 IpcClient 进程在连不上现有 socket 时会自行创建监听(先到先得)。正常情况下 ChatGPT Desktop 主进程是监听方(路由器角色),其余进程(codex 二进制、Desktop 各窗口、我们的 Bridge)是客户端角色。

## 2. 消息信封

所有帧解析为 JSON 对象,以 `type` 字段区分:

### 2.1 request

```json
{
  "type": "request",
  "requestId": "<uuid v4>",
  "sourceClientId": "<客户端 initialize 后获得的 id;初始化前为 \"initializing-client\">",
  "version": <整数,见 §4>,
  "method": "<方法名>",
  "params": { ... },
  "targetClientId": "<可选,定向转发>",
  "hostId": "<可选,\"local\" 或 \"remote-control:<envId>\">",
  "timeoutMs": <可选,默认 5000>
}
```

- 路由:`targetClientId` 存在时定向转发;否则路由器向所有其他客户端发 `client-discovery-request`,第一个 canHandle=true 的客户端接收;无人可处理 → 响应 `error: "no-client-found"`。
- 超时:路由器按 `timeoutMs`(默认 5000ms,discovery 阶段 10000ms)计时,超时回 `error: "request-timeout"`。
- 处理方版本不匹配 → `error: "request-version-mismatch"`;处理方没有该方法的 handler → `error: "no-handler-for-request"`。

### 2.2 response

```json
{
  "type": "response",
  "requestId": "<对应 request 的 id>",
  "resultType": "success" | "error",
  "method": "<可选,handler 回填>",
  "handledByClientId": "<可选,实际处理方 client id>",
  "result": { ... },
  "error": "<resultType=error 时的错误字符串>"
}
```

错误字符串(已验证来源):`no-client-found`、`request-timeout`、`client-disconnected`、`server-closed`、`request-version-mismatch`、`no-handler-for-request`,以及 handler 抛出的任意 message(如 `SteerTurnInactiveError`、`NoActiveTurn`、`thread-follower-response-method-mismatch`)。

### 2.3 broadcast

```json
{
  "type": "broadcast",
  "method": "<方法名>",
  "sourceClientId": "<发送方 client id>",
  "targetClientIds": ["<可选;缺省 = 广播给除发送方外的全部客户端>"],
  "params": { ... },
  "version": <整数,见 §4>
}
```

广播不产生响应。路由器只做转发(改写 `sourceClientId` 为注册后的 id)。

### 2.4 client-discovery-request / client-discovery-response(路由器内部)

```json
{ "type": "client-discovery-request", "requestId": "<uuid>", "request": { <原 request> } }
{ "type": "client-discovery-response", "requestId": "<uuid>", "response": { "canHandle": true|false } }
```

任何客户端都可能收到 discovery(例如 codex 二进制在等待 IDE context 时会向所有客户端发 `ide-context` 的 discovery)。不支持的客户端必须回 `canHandle:false`(实测 Desktop 也会向我们发 `ide-context` discovery)。

## 3. 握手与连接生命周期

1. connect 到 socket 路径。
2. 客户端发送 `initialize` 请求:`method:"initialize"`、`version:0`、`params:{ "clientType": "<自由字符串>" }`。已观测 clientType:`desktop`(Desktop 主进程/窗口)、IDE 扩展、codex 二进制。路由器自身不 initialize。
3. 路由器为客户端分配 uuid `clientId`,响应:
   `{ "type":"response", "resultType":"success", "method":"initialize", "handledByClientId":"<id>", "result":{ "clientId":"<id>" } }`
   重复 initialize(同 socket)返回已注册 id。
4. 客户端在收到 initialize 成功响应前不得发送其他 request(IpcClient 侧守卫;服务端不强制)。
5. 注册后路由器向其他客户端广播 `client-status-changed { status:"connected" }`;断开时广播 `disconnected`。
6. 连接断开后客户端等待约 1000ms 重连并重新 initialize,`clientId` 会变化。

## 4. 方法版本表与版本规则

路由器/处理方按 `version` 字段校验(不匹配即拒绝)。当前版本表(152.0.7977.64):

| 方法 | Ev |
|---|---|
| thread-stream-state-changed | 11 |
| thread-stream-following-changed | 1 |
| thread-stream-following-status-requested | 1 |
| ipc-connection-reset | 1 |
| thread-read-state-changed | 2 |
| thread-archived | 2 |
| thread-unarchived | 1 |
| thread-owner-discovery | 1 |
| thread-follower-start-turn | 2 |
| thread-follower-load-complete-history | 1 |
| thread-follower-compact-thread | 1 |
| thread-follower-steer-turn | 1 |
| thread-follower-interrupt-turn | 4 |
| thread-follower-update-thread-settings | 1 |
| thread-follower-edit-last-user-turn | 2 |
| thread-follower-command-approval-decision | 1 |
| thread-follower-file-approval-decision | 1 |
| thread-follower-permissions-request-approval-response | 1 |
| thread-follower-submit-user-input | 1 |
| thread-follower-submit-mcp-server-elicitation-response | 1 |
| thread-follower-set-queued-follow-ups-state | 1 |
| thread-queued-followups-changed | 1 |
| initialize / client-status-changed / ide-context / automation-* / app-connect-oauth-callback-received / query-cache-invalidate | 0 |

发送规则(已用 load-complete-history 实测):

- broadcast:`version = Ev[method] ?? 0`。
- request:`version = Ev[method] ?? 0`;但若 `hostId != null` 且方法名以 `thread-follower-` 开头,则 `version = Ev + 1`;`thread-follower-interrupt-turn` 且 params 缺少 `expectedTurnId` 时为 `3`(向后兼容 legacy)。
- 接收校验:版本必须等于按上述规则计算的期望值(`thread-follower-interrupt-turn` 在 hostId==null 时额外接受 3)。
- 未知方法 Ev=0,version 必须为 0。

含义:方法/参数 schema 随 Desktop 版本演进,`version` 是实际的能力探针锚点。Bridge 应在 capability probe 中校验版本表,未知版本只读降级(§5 规格)。

## 5. owner / follower 模型

- 本地会话(`hostId:"local"`)的 owner 是 ChatGPT Desktop 窗口(渲染进程 viewService,经主进程 IPC client 代理)。远程环境 hostId 形如 `remote-control:<envId>`。
- owner 通过 `thread-owner-discovery` 的 discovery 应答表明自己可以处理某 `conversationId` 的 follower 请求,并返回 `result:{ "supportsUntrustedAppInput": true }`;响应的 `handledByClientId` 即 owner 的 clientId(实测)。
- 没有任何 client 可处理(会话未打开/owner 不存在)→ `error:"no-client-found"`。这是"该会话当前无 owner"的权威信号。
- follower(我们)通过订阅获得快照与增量;follower 不保证收到全部 delta,断流/未知 patch 时用 `thread-follower-load-complete-history` 触发快照补偿(实测 revisions 单调递增)。
- owner 切换:原 owner 断开时其他客户端会收到 `client-status-changed (disconnected)`;新 owner 就绪后会广播 `thread-stream-following-status-requested`,旧 follower 应对该 `conversationId` 重新发送 `thread-stream-following-changed (following=true, targetClientIds=[新 owner])` 以重新订阅。`client-status-changed (connected)` 到达时 follower 同样向新客户端重发 following=true(实测 broadcast 参数含 `targetClientIds`)。

## 6. 只读方法(会话观察)

### 6.1 thread-owner-discovery(request,version 1)

```json
params: { "hostId": "local", "conversationId": "<uuid>" }
result: { "supportsUntrustedAppInput": true }
error:  "no-client-found" | 其他
// handledByClientId = owner clientId
```
状态:VERIFIED(真实 Desktop)。

### 6.2 订阅:broadcast thread-stream-following-changed(version 1)

```json
params: { "conversationId": "<uuid>", "hostId": "local", "following": true, "targetClientIds": ["<可选>"] }
```

- 无响应。owner 收到后:首个 follower → 向所有 follower 广播全量快照;后续 follower → 定向发送快照(带 `targetClientIds`)。
- following=false 退订。
- 状态:VERIFIED。

### 6.3 thread-stream-state-changed(broadcast,version 11)

```json
params: {
  "conversationId": "<uuid>",
  "hostId": "local",
  "targetClientIds": ["<可选>"],
  "change":
    { "type": "snapshot", "revision": <u64>, "conversationState": { <渲染进程会话状态对象,见下> } }
  | { "type": "patches", "baseRevision": <u64>, "revision": <u64>, "patches": [ <Immer patch> ] }
}
```

- revision 每 conversation 单调递增,从 1 开始(实测)。
- **patches 是 Immer patch 格式**(不是 RFC6902):`{ "op":"add"|"replace"|"remove"|..., "path":[<segment,...>], "value":<可选> }`;path 是数组而非 JSON Pointer 字符串。
- follower 应用规则(源码语义,Bridge 必须复刻):仅当 `sourceClientId == 我的 owner` 且 `当前 revision == baseRevision` 时应用;否则静默丢弃并触发快照补偿(load-complete-history)。
- `conversationState`(snapshot 载荷,实测结构键):`id`(uuid)、`title`、`cwd`、`hostId`、`resumeState`、`threadRuntimeStatus{type,activeFlags[]}`、`threadSource`、`source`、`ephemeral`、`hasUnreadTurn`、`latestModel`、`latestReasoningEffort`、`latestCollaborationMode{mode,settings{model,reasoning_effort,developer_instructions}}`、`latestTokenUsageInfo{last{inputTokens,outputTokens,...},total{...},modelContextWindow}`、`gitInfo{branch,originUrl,sha}`、`currentPermissions{approvalPolicy,approvalsReviewer,sandboxPolicy,...}`、`turns[]`、`turnHistory{kind,history{generation,entitiesByKey,islands[]}}`、`turnsPagination`、`createdAt/updatedAt/recencyAt`。
  - turn 对象:`turnId`(uuid)、`status`("inProgress" 等字符串)、`items[]`、`params{threadId,input[],cwd,approvalPolicy,effort,model,summary,...}`、`durationMs`、`error`、`diff`、`itemsPagination`、`hookRuns[]`。
  - item 对象:`id`、`type`、`content[]`、`summary[]` 等(type 字符串集合随 item 类型变化)。
  - Bridge 对该载荷只做受控投影:提取稳定 ID、状态、计数、游标;不得记录或转发未消化的原始 JSON(§25 规格)。
- 状态:VERIFIED(snapshot 与 patches 均实测)。

### 6.4 thread-follower-load-complete-history(request,version: hostId!=null 时 2)

```json
params: { "conversationId": "<uuid>" }
hostId: "local"
result: { "revision": <u64> }
```

owner 在处理时会重读完整历史并(重新)向 follower 发送快照;返回值是最新 revision。Bridge 的快照补偿 = "等到 revision >= 返回值的 snapshot"。状态:VERIFIED。

### 6.5 其他广播(观察面)

- `thread-stream-following-status-requested` `{conversationId, hostId}`(version 1):owner 选举后的重新订阅信号。
- `thread-archived` / `thread-unarchived`(version 2/1):归档状态变化。
- `thread-read-state-changed`、`thread-queued-followups-changed`、`query-cache-invalidate`、`client-status-changed {clientId, clientType, isSelf?, status}`、`ipc-connection-reset`。
- `automation-capability-event`、`automation-run-triggered-event`、`app-connect-oauth-callback-received`(与首版无关,记录存在)。

## 7. 写方法(follower → owner;全部需要已确认 owner)

所有方法的 params 都含 `conversationId`(uuid);返回 `resultType:"success"` 表示 owner 已受理。**除下列方法外,IPC 上不存在任何其他写能力;尤其不存在“新建 thread”方法(见 §8)。**

### 7.1 thread-follower-start-turn(Ev=2;hostId!=null 时发送 3)

```json
params: {
  "conversationId": "<uuid>",
  "turnStart": {
    "request": {
      "threadId": "<同 conversationId>",
      "input": [ { "type": "text", "text": "<用户输入>", "text_elements": [] } ]
      // 可选: cwd/approvalPolicy/effort/model/summary/collaborationMode/turnTrigger 等(与 app-server turn 参数同名)
    },
    "context": { // 全部可选
      "localTurnMetadata": {}, "attachments": [], "commentAttachments": [],
      "useAppServerPermissionDefault": bool, "usePermissionSelection": bool,
      "inheritThreadSettings": bool, "writingBlockContextPrepared": bool,
      "threadStartKind": "<字符串>", "mcpAppModelContextAttachments": [],
      "responseItems": []
    }
  }
}
result: { "result": { <turn 对象,含 turnId/status> } }
```

语义:owner 空闲时开始下一轮;若该会话已有进行中的 turn,行为由 owner 决定(排队/拒绝,未实测)。
状态:FIXTURE_ONLY(参数 schema 来自 asar 逆向,未在真实 Desktop 上执行)。

### 7.2 thread-follower-steer-turn(version 1;hostId!=null 时 2)

```json
params: {
  "conversationId": "<uuid>",
  "clientUserMessageId": "<uuid,可选>",
  "input": [ { "type": "text", "text": "...", "text_elements": [] } ],
  "serviceTier": <可选>,
  "attachments": <可选>,
  "additionalContext": <可选>,
  "toolOutput": <可选>,
  "restoreMessage": { "cwd": "...", "context": { "workspaceRoots": [...], "collaborationMode": ... } } // 可选
}
result: { "result": <steer 结果对象> }
```

错误:无活跃 turn 时 `SteerTurnInactiveError` / `NoActiveTurn`。
状态:FIXTURE_ONLY。

### 7.3 thread-follower-interrupt-turn(version 4;hostId!=null 时 5;hostId==null 且无 expectedTurnId 时 3)

```json
params: { "conversationId": "<uuid>", "mode": "user-stop" | "system" | "descendant-cleanup", "expectedTurnId": "<uuid,可选但建议必填>" }
result: { "interruptedTurnId": "<uuid>", "ok": true, "goalPauseError": "<可选>" }
```

状态:FIXTURE_ONLY。

### 7.4 审批与问答

- `thread-follower-command-approval-decision` `{conversationId, requestId, decision}` → `{ok:true}`
- `thread-follower-file-approval-decision` `{conversationId, requestId, decision}` → `{ok:true}`
- `thread-follower-permissions-request-approval-response` `{conversationId, requestId, response}` → `{ok:true}`
- `thread-follower-submit-user-input` `{conversationId, requestId, response}` → `{ok:true}`(回答原生问题)
- `thread-follower-submit-mcp-server-elicitation-response` `{conversationId, requestId, response}` → `{ok:true}`

状态:全部 FIXTURE_ONLY(requestId 来自 patch 流中的审批项)。

### 7.5 设置与会话管理

- `thread-follower-update-thread-settings` `{conversationId, threadSettings}` → `{ok:true}`(threadSettings 形如快照中的 `latestThreadSettings`:model/effort/serviceTier/personality/summary/sandboxPolicy/...)
- `thread-follower-edit-last-user-turn` `{conversationId, ...}` → `{ok:true}`
- `thread-follower-compact-thread` `{conversationId}` → `{ok:true}`
- `thread-follower-set-queued-follow-ups-state` `{conversationId, state: { "<conversationId>": [...] }}` → `{ok:true}`

状态:FIXTURE_ONLY。

### 7.6 ide-context(request,version 0;由 IDE 客户端提供)

`params:{ "workspaceRoot": "..." }` → `result:{ "ideContext": {...} }`。Desktop/codex 二进制会向所有客户端发 discovery 询问;非 IDE 客户端一律 `canHandle:false`。状态:VERIFIED(discovery 方向实测)。

## 8. 新建 thread:不存在

- 全量枚举 Desktop 注册的 request handler:仅 `thread-owner-discovery` + §7 的 `thread-follower-*`(源:`Tde` 注册函数);路由器自身仅处理 `initialize`。**没有新建/克隆 thread 的 IPC 方法。**
- 渲染进程新建会话走 Desktop 私有的 app-server 连接(stdio JSON-RPC,如 `thread/start`、`thread/compact/start`),不经过 ipc.sock;`client-new-thread:` 前缀只是 UI 侧临时 clientThreadId,不构成外部创建入口。
- 结论:网页发起的“新建任务”无法经 ipc.sock 落地,执行规格 §12 会话创建规则命中“Desktop 私有控制路径无法创建任务” → capability unsupported,B.11 阶段门(详见 CODEX-COMPATIBILITY.md)。

## 9. Bridge 实现注意事项

- 帧读侧必须流式解析(半包/粘包),UTF-8 边界安全;未知 `type` 或未知 method 不 panic:未知 broadcast 忽略,未知 request 响应 `no-handler-for-request`,关键 patch 应用失败/版本缺口触发 snapshot 补偿。
- 单帧、发送队列与消息尺寸上限由 Bridge 自行设值(§12 规格),不得照抄 256 MiB。
- 连接失败不得删除 socket、不得终止或重启 Desktop;重连退避由上层调度。
- 所有写操作前必须先 `thread-owner-discovery` 确认 owner,并把写目标限定在授权会话。
- 会话列表:ipc.sock 没有列表方法;列表来源是 Desktop 的 SQLite(只读),ipc.sock 只提供已打开会话的运行时观察与控制。两路数据以 `conversationId`(= threads.id)对齐。

## 10. 0.153.1 复核(2026-09-04,只读)

对象:ChatGPT Desktop 自动更新后 `codex --version` = `codex-cli 0.153.1`,bundle `CFBundleShortVersionString=26.901.31953`、`CFBundleVersion=7868`(Electron Framework 目录名仍为 `152.0.7977.64`,与 bundle 版本号解耦)。复核方式:新版 `app.asar` 静态逆向 + 真实 socket 只读探针。**未发送任何写方法。**

### 10.1 asar 结构与不变项

- 路由器/共享客户端库移至 `.vite/build/src-VqXTPopo.js`;渲染进程为 `webview/assets/app-initial-caa927532ffb.js`。帧解码器:4 字节小端长度前缀 + UTF-8 JSON,单帧上限仍为 `256*1024*1024` 字节(源码 `lde=256*1024*1024`),0/超长断连;`initializing-client` 哨兵仍在。socket 路径与权限逻辑(`f9()`:目录 0700 且属主校验、socket 0600、备选 `$(tmpdir)/codex-ipc/ipc-<uid>.sock`)逐行同旧版;实测目录 `drwx------`、socket `srw-------`。
- 信封 `type` 集合不变:`request`/`response`/`broadcast`/`client-discovery-request`/`client-discovery-response`。
- **Ev 版本表逐字节与 §4 相同**(22 项,`thread-stream-state-changed=11`、`thread-follower-interrupt-turn=4`、`thread-follower-start-turn=2`、`thread-follower-edit-last-user-turn=2`、其余 follower/owner 方法 1,广播 2/1/1,未列方法 0)。版本规则函数(Ov/kv)语义不变:hostId≠null 且 `thread-follower-*` → Ev+1;`interrupt-turn` 无 `expectedTurnId` 且 hostId==null → 3(legacy,仍保留)。
- Desktop 侧 request handler 注册点仍唯一(`Tde` 函数):`thread-owner-discovery` + 13 个 `thread-follower-*`(§7 全部方法,无新增/删除/改名)。**仍无新建/克隆 thread 方法**(§8 结论在 0.153.1 上复确认;全 asar 仅 1 处 `addRequestHandler` 字面注册)。广播 dispatch 集合(§6.5)亦无增减。
- 写方法参数读取字段逐一与 §7 相同:steer 读 `input/restoreMessage/serviceTier/attachments/clientUserMessageId/additionalContext/toolOutput`;interrupt 读 `mode/expectedTurnId` 并回 `{interruptedTurnId, ok, goalPauseError?}`;审批/问答读 `requestId + decision|response`;`set-queued-follow-ups-state` 读 `state[conversationId]`;start-turn 读 `conversationId + turnStart`。
- snapshot/patches 机制不变:owner 侧仍以 Immer patches(`enablePatches`/`produceWithPatches` 存在于渲染进程)产出 `{op,path[],value}` 增量;`thread-stream-state-changed` 广播实测 `version=11`;revision 每 conversation 单调递增(实测跨快照发布 1→2→4→5)。

### 10.2 版本校验位置(对 §2.1/§4 的语义精化,非变更)

Ev/kv 校验发生在**处理方客户端**而非路由器:候选处理方在 discovery 阶段以 kv 校验版本,不符 → `canHandle:false`;全部候选拒绝后路由器在 discovery 超时(10s)后回 `no-client-found`。仅当请求被定向(`targetClientId`)或已被选中后,处理方 `handleRequest` 才回 `request-version-mismatch`。实测:v1+hostId=local 的 `load-complete-history`(期望 2)在 owner 在场时 10s 后得到 `no-client-found`,同刻 v2 请求 0.01s 内成功 → Ev+1 规则在 0.153.1 上仍被强制执行。Bridge 现行发送规则无需调整。

### 10.3 conversationState 新增顶层键(对 §6.3 的增补)

真实快照(专用测试会话)顶层键 39 个;相对 Bridge `KNOWN_STATE_KEYS` 新增 17 个,全部为渲染进程会话状态扩展,**帧格式与既有键含义未变**,投影侧未知键容忍策略(计数并忽略)已覆盖:

`agentNickname`、`forkedFromId`、`generatedTitle`、`historyMode`、`latestThreadSettings`、`mode`、`modelProvider`、`previousTurnModel`、`projectlessOutputDirectory`、`requests`、`rolloutPath`、`sessionId`、`shellEnvironmentPolicy`、`sideConversation`、`threadStartKind`、`workspaceBrowserRoot`、`workspaceKind`。

(`pendingQuestions`/`pendingApprovals`/`queuedFollowUps` 是 Bridge 侧扩展读取点,会话无挂起项时缺省,非 Desktop 删除。)

### 10.4 Desktop 内部行为变化(不经线上协议,Bridge 无感)

- 新增 owner 不可用恢复流:`markConversationNeedsResumeForUnavailableOwner` / `resumeConversationForUnavailableOwner`(follower 请求命中 `timeout`/`request-version-mismatch`/`no-client-found` 时的 Desktop 自愈,渲染进程内部)。
- Desktop 发送侧在 `start-turn` 携带非空 `context.responseItems` 时,先对目标 owner 复查 `thread-owner-discovery`(校验 `handledByClientId` 与 `supportsUntrustedAppInput`);外部发送方不受影响。
- 渲染进程移除了一条内部 `thread/settings/update`(私有 app-server 通道)回退路径;`thread-follower-update-thread-settings` 线上 schema 不变。
- app-server 传输选择逻辑不变:仍需显式 `CODEX_APP_SERVER_USE_LOCAL_DAEMON==="1"` 才考虑 daemon,默认私有 stdio(CODEX-COMPATIBILITY.md §7 约束继续有效)。

### 10.5 真实只读探针结论(0.153.1,2026-09-04)

initialize → success+clientId;`thread-owner-discovery` → success + `supportsUntrustedAppInput:true` + owner clientId;follow(version 1)→ 定向快照(broadcast version 11);`load-complete-history`(v2,hostId=local)→ `{revision:N}` 且 owner 重发全量快照;负版本探针见 §10.2;全程未发送任何 `thread-follower-*` 写方法,探针前后进程集合无 Codex 相关变化。
