# 前端对接指南(FRONTEND-INTEGRATION)

读者:`apps/web`(及未来 Tauri 桌面壳)的实现者。目标:不读 Rust 内部实现、
不猜 Codex 原生字段,直接完成认证、连接、订阅、命令、文件面与恢复逻辑。

- HTTP 契约:`contracts/openapi.yaml`(路由真相在 `apps/relay/src/*`)
- 稳定错误码:`contracts/error-codes.md`
- UI 状态 fixture:`contracts/fixtures/`
- 协议类型:`@agent-console/protocol`(生成自 `proto/agent_console/v1`)
- 能力矩阵(哪些操作真实可用):`docs/CODEX-COMPATIBILITY.md`

---

## 1. 本地启动(PostgreSQL / Relay / 真实 Bridge / fake Bridge)

完整环境与变量说明见 `docs/LOCAL-DEVELOPMENT.md`;此处只给最小联调链。

### 1.1 PostgreSQL(独立库/用户)

```bash
# 仓库自带开发库容器(推荐;用 OrbStack 提供的 docker)
cp .env.example .env            # 设置 POSTGRES_PASSWORD
docker compose --env-file .env -f deploy/compose.yaml up -d postgres
# 宿主机直跑 relay 调试时,临时放开 compose.yaml 中注释的 5433 端口映射
```

### 1.2 Relay

```bash
export RELAY_DATABASE_URL=postgres://agent_console:<密码>@127.0.0.1:5433/agent_console
export RELAY_BIND_ADDR=127.0.0.1:8081
# 与 dev-toolbox 侧 AGENT_CONSOLE_INTERNAL_TOKEN 同值;本地不跑 dev-toolbox 时可留空,
# 此时浏览器 WS ticket 无法消费(联调写链路必须配置)
export RELAY_INTERNAL_TOKEN=<内部token>
export DEVTOOLBOX_INTERNAL_BASE_URL=http://127.0.0.1:8080
cargo run -p relay
# 存活检查: curl http://127.0.0.1:8081/health  → ok
```

### 1.3 真实 Bridge(连接本机 Codex Desktop)

```bash
# 1) 配对(§21 Bridge 发起):Bridge 生成 challenge 向 Relay 注册,打印短码与
#    二维码数据;在浏览器「设备配对」中输入短码或扫码批准后自动完成绑定
cargo run -p bridge -- pair --relay-url ws://127.0.0.1:8081
# 2) 体检:输出版本/路径/keychain/绑定/派生 Bridge WS 地址/能力矩阵,不输出会话正文
cargo run -p bridge -- doctor
# 3) 常驻运行
cargo run -p bridge -- run --relay-url ws://127.0.0.1:8081
# 4) 工作区授权(新建任务与显式打开文件的根目录)
cargo run -p bridge -- workspace list
cargo run -p bridge -- workspace authorize --path /path/to/workspace --name my-project
cargo run -p bridge -- workspace revoke --path /path/to/workspace
# 5) 只读开发命令(仅技术验证)
cargo run -p bridge -- inspect sessions
cargo run -p bridge -- inspect session <native-session-id>
# 6) 清除本机凭据(显式动作)
cargo run -p bridge -- unpair --confirm
```

`pair`/`run` 的 `--relay-url` 与环境变量 `AGENT_CONSOLE_RELAY_URL` 是同一
合同:**Relay 公网基地址(origin)**,不允许携带 `/agent-console/...` 路径
(误填完整 WebSocket 地址会给出专门报错)。Bridge 由此统一派生各端点:

- Bridge WS:`{origin}/agent-console/bridge/ws`
- 配对注册/轮询:`{origin}/agent-console/bridge/pairing/register|claim`
- 文件传输:`{origin}/agent-console/transfers/producer|consumer/{transferId}`
- 配对二维码深链:`{origin}/agent-console/pair?code=<shortCode>`

`http(s)`/`ws(s)` scheme 均可(内部规范化);生产填
`https://<域名>`,本地直连 Relay 用 `ws://127.0.0.1:8081`。配对方向为
Bridge 主动注册并轮询等待浏览器批准,`pair` 不再接受 `--code`。

### 1.4 fake Bridge 联调(不依赖真实 Codex Desktop)

```bash
cargo run -p bridge --bin fake-codex-owner /tmp/fixture-codex-ipc fixture-script.json
```

- 参数 1:IPC socket 路径;Bridge 侧用 `CODEX_HOME` 指到含该 `ipc/ipc.sock`
  的目录即可让 adapter 连上 fake owner(见 `bridge doctor` 的路径探测输出)。
- 参数 2:脚本 JSON。格式(全部合成数据,与
  `apps/bridge/src/bin/fake-codex-owner.rs` 头注释一致):

```json
{
  "sessions": [{
    "conversationId": "11111111-1111-4111-8111-111111111111",
    "title": "fixture-alpha",
    "cwd": "/tmp/fixture-alpha",
    "branch": "fixture-branch",
    "model": "gpt-5.3-fixture",
    "reasoningEffort": "medium",
    "approvalPolicy": "untrusted",
    "pendingQuestions": [],
    "pendingApprovals": [],
    "turn": {
      "outputLines": ["1", "2", "3"],
      "lineDelayMs": 30,
      "finalAnswer": "fixture-final",
      "finalOutput": null,
      "fail": false,
      "questionAfterLine": null,
      "question": null,
      "approval": null
    }
  }]
}
```

`finalOutput` 与预览不同可驱动 OutputReplace 权威校正;`question`/`approval`
触发 attention 并暂停,直到回答/审批。

---

## 2. 认证:session/CSRF → 一次性 WS ticket → subprotocol

1. 浏览器访问 `/agent-console/...`;未登录跳转 dev-toolbox 登录页
   (return path 规则见 §9)。登录后浏览器持有 dev-toolbox session Cookie
   与 CSRF(CSRF 由 dev-toolbox 现有页面机制提供,前端在内存/存储中按其现有约定携带)。
2. 建立 WebSocket 前调用:

```bash
curl -X POST "https://<域名>/api/v1/agent-console/ws-tickets" \
  -H "Origin: https://<域名>" -H "X-CSRF-Token: <csrf>" \
  --cookie "<dev-toolbox session cookie>"     # 示例用;浏览器 fetch 自动带 cookie
# 200 {"ticket":"<base64url>","expiresAt":"...","audience":"relay"} ;Cache-Control: no-store
```

3. ticket **不进 URL**。连接 `/agent-console/ws` 时通过
   `Sec-WebSocket-Protocol` 同时提供两个子协议:

```
Sec-WebSocket-Protocol: agent-console.v1, agent-console.ticket-<Base64URL(ticket)>
```

Relay 只回显固定协议名 `agent-console.v1`;ticket 30 秒有效、单次消费。
ticket 无效/过期/已消费时握手以 HTTP 401 失败,错误码为
`WS_TICKET_INVALID` / `WS_TICKET_EXPIRED` / `WS_TICKET_CONSUMED` → 重新取票。
HTTP API(`/agent-console/api/*`)不需要 ticket:网关 forward-auth 后以内部
身份头传给 Relay,浏览器照常带 Cookie + CSRF 即可。

---

## 3. WS 时序:Hello → Subscribe → Snapshot → Event → Ack → 重连/Resync

所有帧为二进制 Protobuf `agent_console.v1.Envelope`
(`@agent-console/protocol` 的 `encodeEnvelope/decodeEnvelope`,单帧 ≤ 1 MiB)。

```text
浏览器                                   Relay(→ Bridge)
  │ ClientHello{protocol_version:1, client_kind:CLIENT_BROWSER}
  │──────────────────────────────────────▶│
  │◀──────────────────────────────────── ServerHello{accepted_protocol_version:1,
  │            capabilities:["list","session","query","command"]}
  │ Subscribe{list:{}}                    │  (列表订阅:只含 SessionSummary)
  │ 或 Subscribe{session:{SessionKey}}    │  (详情订阅:RuntimeSnapshot/item/输出/attention)
  │──────────────────────────────────────▶│ 建立/复用上游订阅;snapshot 期间事件进缓冲
  │◀──── Subscribed{stream_id, stream_epoch, base_sequence}
  │◀──── RuntimeSnapshot | SessionSummaryBatch{snapshot:true}
  │◀──── EventBatch{stream_id, events[...]}(sequence 连续)
  │ Ack{stream_id, sequence}              │
```

要点(§17.4/§17.5):

- 列表订阅快照是 `SessionSummaryBatch{snapshot:true}`;增量也是
  `SessionSummaryBatch{snapshot:false}` 或详情流事件
  `SessionSummaryChanged`(携带完整 summary)。
- sequence 只在 `stream_id + stream_epoch` 内单调;重复 sequence 幂等忽略;
  缺口在 Relay 内存窗口内会补发;超出窗口、Relay 重启或 epoch 变化 →
  `ResyncRequired{stream_id, reason_code:RESYNC_REQUIRED}`。
- `Subscribed.base_sequence` 是**已应用水位**:快照内容覆盖到该序号,
  后续帧 sequence 从 base+1 起连续;前端应用快照后
  `lastSequence = baseSequence`。快照覆盖水位模型:Relay 按设备缓存最新
  快照,新快照到达时只删除同设备且上游批号 ≤ 覆盖水位的缓冲帧;断线恢复
  优先完整重放,存活窗口有空洞时锚定最新快照重放(客户端从快照重建)。
- 已知 streamId 再次收到 `Subscribed`(新 epoch)时在同一连接上重绑:
  接受新 epoch 并按 base_sequence 重建坐标,等待新快照应用,无需重连。
- 收到 ResyncRequired 或本地状态可疑时,发
  `ResyncRequest{stream_id}` → 服务端重新走 Subscribed → 快照 → 缓冲事件。
- **epoch 变化必须放弃本地状态、从新 snapshot 重建。**
- 心跳:发 `Heartbeat{}`,等 `HeartbeatAck{}`;15s 间隔,约 45s 无响应视为断线。
- 断线重连:重新申请 ticket(旧 ticket 已消费),携本地最后
  `stream_epoch/sequence` 语义由服务端裁决——客户端只管重新 Subscribe;
  若服务端还能补发则续流,否则 ResyncRequired → 重新快照。
- 详情订阅的写命令与查询同样走该 WS(见 §4);`QueryRequest` 也可由 HTTP
  端点替代(二者最终都到 Bridge,HTTP 形状见 openapi.yaml)。

---

## 4. 写命令:每个 operation 的前置状态与错误处理

公共字段(§15.1):`request_id`(调用方 UUID,重试必须复用同一 ID)、
`operation`、`session_key{device_id, agent_kind, native_session_id,
relay_session_uuid?}`、`expected_turn_id`(不适用省略)、
`expected_runtime_revision`(取最近 RuntimeSnapshot.runtime_revision)、
`payload_digest`(payload 的 SHA-256 hex;仅协议去重比较)。

回执:`CommandAccepted`(status = RECEIVED → ACCEPTED_BY_BRIDGE → …)→
`CommandResult`(COMPLETED / REJECTED+稳定码 / OUTCOME_UNKNOWN)。
**只有收到 ACCEPTED_BY_BRIDGE 才能显示"已发送/已排队"。**
相同 request_id 同内容重试返回已有回执不重复执行;同 ID 不同内容 →
`DUPLICATE_REQUEST_MISMATCH`。

| operation | 前置状态(§15.4) | 关键错误 → 前端动作 |
|---|---|---|
| `OPERATION_START_TURN` | IDLE(无 PendingAttention) | STALE_TURN → 刷快照确认;CONTROL_READ_ONLY → 禁用输入;DEVICE_OFFLINE → 禁止(设备离线时一切写不可用) |
| `OPERATION_QUEUE_SET` | RUNNING 且队列为 EMPTY | QUEUE_ALREADY_EXISTS → 改用 QUEUE_REPLACE;设备离线时 Relay 直接拒(不代存队列) |
| `OPERATION_QUEUE_REPLACE` | 已有队列(QUEUED/PAUSED) | STALE_TURN(runtime_revision 已变)→ 刷快照后由用户重新确认 |
| `OPERATION_QUEUE_CANCEL` | 已有队列 | 队列已被自动发送 → REJECTED;以最新快照为准 |
| `OPERATION_STEER` | RUNNING 且**用户显式选择 steer**(不得把普通发送自动映射成 steer) | STALE_TURN → 刷快照;普通发送在 RUNNING 下默认进队列,而不是 steer |
| `OPERATION_INTERRUPT` | RUNNING;必须带 expected_turn_id;前端负责二次确认 | STALE_TURN → turn 已变,放弃或重新确认 |
| `OPERATION_ANSWER_QUESTION` | 存在 PendingQuestion 且 valid;只发原生 questionId + optionIds / free_text | QUESTION_EXPIRED → 置灰问题卡 |
| `OPERATION_ANSWER_APPROVAL` | 存在 PendingApproval 且 valid;只发原生 approvalId + decisionId | APPROVAL_EXPIRED → 置灰审批卡;不得创造"永远允许"选项 |
| `OPERATION_UPDATE_SETTINGS` | 目标 SettingOption.mutable=true;value 必须来自 availableValues | SETTING_COMBINATION_UNSUPPORTED → 回显当前值,不静默降级;生效范围=下一次 turn(原生支持即时变化时以能力快照为准) |
| `OPERATION_RENAME_TASK` / `ARCHIVE_TASK` / `UNARCHIVE_TASK` / `FORK_TASK` / `OPERATION_CREATE_TASK` | 需 Desktop 暴露并验证过对应能力 | **当前 Desktop 版本均为 UNSUPPORTED**(见 §11 与 CODEX-COMPATIBILITY);supportedOperations 不含这些值时控件不渲染;误发将得到 CAPABILITY_UNSUPPORTED |
| `OPERATION_STOP_BACKGROUND_COMMAND` | command ID 稳定可识别(capability 含本操作) | CAPABILITY_UNSUPPORTED → 只保留全局停止 |
| `OPERATION_STOP_ALL_BACKGROUND_COMMANDS` | 仅全局停止能力;结果 warnings 含 `BACKGROUND_STOP_GLOBAL_ONLY` | UI 提示"已请求停止全部后台命令(无法逐项停止)" |

`OUTCOME_UNKNOWN`:连接中断且无法证明 Desktop 是否执行 → **不得自动重发**
(见 §10)。

---

## 5. 多维状态 → UI 映射(禁止单一综合枚举)

会话没有 `session_status`。以下 8 个维度独立渲染(§10):

| 维度 | 值 | UI 映射 |
|---|---|---|
| `deviceConnection`(Device) | CONNECTING/ONLINE/DEGRADED/OFFLINE | OFFLINE:整个会话详情降级为摘要视图(在线数据一律来自 Relay 摘要,不请求 runtime/history/output/git/files);DEGRADED:黄点 + `degradedReason`(OFFLINE 时为空,不猜测关机/休眠/断网) |
| `controlMode` | FULL_CONTROL/LIMITED_CONTROL/READ_ONLY/UNAVAILABLE | 决定写控件可用性的上限(与 capability.supportedOperations 取交集,见 §11) |
| `compatibilityState` | VERIFIED/DEGRADED/UNSUPPORTED | DEGRADED/UNSUPPORTED:顶部"版本未验证,只读"横幅;数据面板只显示已验证能力对应内容 |
| `activeTurnPhase` | IDLE/RUNNING/FINISHING | 进度区主状态;RUNNING 与"等待用户"**不互斥** |
| `pendingQuestions` / `pendingApprovals` | 列表 | 角标 = `pendingAttentionCount`;卡片按 `valid` 启用/置灰;RUNNING 中弹出同样成立 |
| `queueState` | EMPTY/QUEUED/PAUSED | QUEUED:"下一轮已排队"提示条 + 可替换/取消;PAUSED:"已暂停,需重新确认"提示条 |
| `lastTurnOutcome` | COMPLETED/FAILED/INTERRUPTED/UNKNOWN | 只标注**上一轮**结果徽章;FAILED/INTERRUPTED 不把会话永久标红;UNKNOWN 显示"结果未知" |
| `backgroundCommands` / `backgroundCommandCount` | RUNNING/COMPLETED/FAILED/STOPPED/UNKNOWN | 主 turn 完成后仍显示后台命令区;count 与列表可能不一致(无法识别单项时只有 count)→ 只显示"有 N 条后台命令" + 全局停止 |

组合示例(与 fixtures 对应):

- `session-list.json` 第 1 条:ONLINE + FULL_CONTROL + RUNNING + 审批角标 →
  列表项显示"运行中"与审批角标,点开先渲染审批卡。
- `runtime-read-only-unverified.json`:DEGRADED + READ_ONLY + 空
  supportedOperations → 输入框、队列、审批操作全部隐藏,只保留只读时间线。
- `runtime-completed-background-command.json`:IDLE + COMPLETED + 后台 RUNNING →
  显示"上一轮已完成" + 后台命令面板。

---

## 6. 输出 reducer 规则(§13)

每个可输出 item 本地维护 `{offset, revision, byteLength, isFinal, chunks}`;
`runtimeSnapshot.recentOutputCursors` 是对齐起点。

- **OutputAppend**:`bytes` 仅当 `local.offset === expected_offset` 时拼接
  (拼后 `offset += bytes.length`,`revision` 取事件流最新值)。
  不一致 → **不得拼接**:停止追加、保留已拼前缀,等
  `OutputReplace` 或发 `ResyncRequest`(见 fixtures/output-gap.json)。
- **OutputReplace**:整体替换本地内容。`bytes` 存在 → 直接替换并置
  `offset = bytes.length`;`page_cursor` 存在 → 调
  `GET /sessions/{id}/output?itemId=<id>&cursor=<page_cursor>` 分页拉全量后替换。
- **OutputFinal**:定稿。核对 `revision` 与 `byteLength`:本地长度一致 →
  标记 `AUTHORITATIVE_FINAL`;不一致 → 用 output 分页查询重拉。
  `OutputFinal` 之后不再接受该 item 的 OutputAppend。
- **两级承诺**:
  - `LIVE_PREVIEW`:执行中事件是尽力而为(允许延迟/合并/暂时缺片),
    UI 必须呈现"预览中"暗示,不宣称逐字节无损终端。
  - `AUTHORITATIVE_FINAL`:命令/turn 终态后由 Bridge 权威重读校正;
    校正失败时 Bridge 以稳定码表达 `FINAL_OUTPUT_UNAVAILABLE`,
    UI 显示"最终结果不可用",不得把预览冒充完整结果。
- **UTF-8 边界**:所有分块在 UTF-8 边界切分;解码时仍建议用
  `TextDecoder({stream:true})` 增量解码以防万一。二进制输出只提供受限
  字节预览或下载句柄,不做文本渲染。
- 通道:原生可区分时 `OUTPUT_CHANNEL_STDOUT/STDERR`,不可区分恒为
  `COMBINED`——不得猜测。

历史/回放:`GET /sessions/{id}/history` 的条目内容与实时 ItemUpsert 同构,
content 为单键包装:`userMessage|assistantMessage|reasoningSummary|plan|
toolCall|commandStatus|fileChange|subagentStatus|question|approval|tokenUsage`
(`assistantMessage.final` 区分 final/commentary)。

---

## 7. 文件 preview / download / upload 完整时序

共同前提:只使用 Bridge 签发的 `file_handle`(来自会话时间线的文件变化、
工具引用或已授权工作区内用户显式打开);**绝不提交本机路径**。
文件元数据先经 `GET /sessions/{id}/files/metadata?handle=<handle>`
(响应可能刷新 handle,后续请求用新值)。

### 7.1 预览/下载(两步:metadata → POST 流)

```text
POST /agent-console/api/sessions/{id}/files/preview     (或 /download)
     body: {"fileHandle":"...","fileName":"可选","range":{"start":0,"endInclusive":null} 可选}
     → 200/206 字节流(Content-Type 由 Bridge MIME 嗅探决定;
       Content-Disposition: inline|attachment;Content-Range 单 Range 时)
```

- 安全头由 Relay 统一设置:`X-Content-Type-Options: nosniff`、
  `Content-Security-Policy: default-src 'none'`、
  `Cache-Control: private, no-store`。HTML/SVG 按源码文本返回,
  **不要**作为活动页面执行(建议 iframe sandbox 或代码高亮渲染)。
- Range:JSON body 的 `range` 或 HTTP `Range` 头二选一;仅支持**单 Range**,
  多段/逆序/越界 → `TRANSFER_RANGE_INVALID`(400),不整文件回退。
- 限制(以 capability.transferLimits 为准,fixture 值即默认):文本内联
  2 MiB、图片 25 MiB、PDF 单 Range 文件 100 MiB、下载 512 MiB。
  超预览限但未超下载限 → metadata.notPreviewableReason 给出大小原因,
  提供下载入口;超下载限 → `TRANSFER_TOO_LARGE`。
- **取消**:直接 `AbortController.abort()` 该请求。Relay 检测断开后立即
  取消 Bridge 侧 producer(三端联动,§22.4 第 6 步);无需额外取消 API。
- 失败:`FILE_CHANGED`(刷新 handle 后经用户确认重试一次)、
  `FILE_HANDLE_INVALID`(重新 metadata)、`DEVICE_OFFLINE`、
  `TRANSFER_EXPIRED`(504,可重试)。

### 7.2 上传(两步:declare → PUT 正文流)

```text
1) POST /agent-console/api/sessions/{id}/files/upload
     body: {"fileName":"report.pdf","mime":"application/pdf","length":1234567}
     → 201 {"transferId":"...","uploadUrl":"/agent-console/api/sessions/{id}/files/upload/<transferId>"}
2) PUT <uploadUrl>   body = 文件字节流(流式发送,不要整体读入内存)
     → 200 {"transferId":"...","outcome":"TRANSFER_OUTCOME_COMPLETED",
            "errorCode":null,"uploadFileHandle":"<session-scoped handle>"}
```

- 上限 20 MiB(声明与实际都校验;超限 `TRANSFER_TOO_LARGE` 413)。
- 并发:每浏览器/每设备各 2 条,超限 `RATE_LIMITED` 429 → 退避重试。
- transfer TTL 10 分钟;Bridge 就绪前 Relay 不读请求体;首字节 30s、
  空闲读 30s 超时 → `TRANSFER_EXPIRED`。
- **取消**:abort PUT 即三端联动取消;Bridge 侧临时文件按生命周期清理,
  不动用户原始文件。上传成功产生的是只属于当前 session/turn 的 upload
  handle,交给 Codex Desktop 使用。
- **不要**把 transfer 放入离线队列:Relay 重启或 Bridge 断开即失败,UI 显示可重试。

---

## 8. Push API 与默认隐私文案

后端只负责订阅存储与触发;Service Worker、权限请求、通知展示归前端。

- 订阅:`POST /agent-console/api/push/subscriptions`
  `{"endpoint":"...","keys":{"p256dh":"...","auth":"..."}}`(同 owner+endpoint 幂等);
  `PUT/DELETE /push/subscriptions/{id}` 维护;`GET` 列表。
  `applicationServerKey` 使用部署方 VAPID 公钥(环境 `VAPID_PUBLIC_KEY`,
  由部署提供给前端配置,不入本仓库)。
- 设置:`GET/PUT /agent-console/api/push/settings`
  `{"showTitle":false,"events":{"turnCompleted":true,"turnFailed":true,
  "turnInterrupted":true,"waitingQuestion":true,"waitingApproval":true}}`。
- 触发白名单:turn completed/failed/interrupted、等待问题、等待风险审批;
  进度/输出/token 更新不推送。session `muted=true` 覆盖一切事件开关。
- **默认隐私文案**(payload 由 Relay 生成,前端展示时保持):

```json
{
  "sessionId": "<内部 uuid>",
  "deepLink": "/agent-console/s/<uuid>",
  "title": null,
  "body": "任务已完成" | "任务未完成,已失败" | "任务已中断"
        | "任务在等待你的回答" | "任务在等待风险审批"
}
```

`title` 仅在用户显式开启 `showTitle` 后携带会话标题,正文永不含
prompt/文件名/分支/项目。前端通知点击 → `deepLink`(§9)。

---

## 9. Deep link 与登录 return path

- 会话深链:`/agent-console/s/{sessionUuid}`(内部 session UUID,即
  `sessions[0].id`;Push payload 与列表点击共用)。
- 配对深链:`/agent-console/pair?code=<shortCode>`(§21 Bridge 发起配对;
  Bridge CLI 二维码即此地址,origin 与用户访问的同域站点一致。页面读取
  `code` 后走 `POST /agent-console/api/pairing/lookup` → approve 流程)。
- 未登录访问任意 `/agent-console/*`:跳转 dev-toolbox 登录页,登录成功后
  回跳原始**相对路径**。
- return path 校验规则(前端与网关共同遵守):
  - 只接受同源 `/agent-console/` 前缀;
  - 拒绝绝对 URL(`https://...`)、协议相对 URL(`//...`)与其他路径前缀;
  - 实现建议:`const p = new URL(raw, location.origin); ok = p.origin === location.origin && p.pathname.startsWith('/agent-console/')`。

---

## 10. 断线 / 设备离线 / auth 失效 / OUTCOME_UNKNOWN

| 场景 | 表现 | 前端处理 |
|---|---|---|
| 浏览器 WS 断线 | close 事件 | 立即置"重连中"态;指数退避重连;每次重连**重新取 ticket**(旧票已消费);带不完整状态时发 ResyncRequest,收到 ResyncRequired 则以新快照重建(§3) |
| close code 1008(reason=AUTH_EXPIRED 等) | 登录被撤销/过期/密码变更 | 不自动重连;跳登录,return path 回原页(§9) |
| close 1002 PROTOCOL_VERSION_MISMATCH | 协议版本不匹配 | 提示刷新/升级前端,不循环重试 |
| 设备离线(presence 事件 / 列表 deviceConnection=OFFLINE) | 查询/文件/命令均不可用 | 列表项显示离线;详情只显示 Relay 摘要;**不发** runtime/history/output/git/files 请求(会得到 DEVICE_OFFLINE 503);禁止创建/替换队列 |
| auth 后端暂不可达(Relay 宽限 ≤2 分钟) | 写命令被拒,CommandResult.details.reason=AUTH_BACKEND_UNAVAILABLE | 提示"写入已暂停";保持只读订阅;恢复后自动可写,宽限尽连接被 AUTH_EXPIRED 关闭 |
| `OUTCOME_UNKNOWN` 回执 | 命令结果未知 | **绝不自动重发**;展示"结果未知"并提供"查询回执"(GET /requests/{requestId})与"查看当前状态"(拉 runtime)两个入口,由用户决定是否重新提交(新 request_id) |
| `STALE_TURN` / `DUPLICATE_REQUEST_MISMATCH` | 写命令冲突 | 刷快照后由用户确认重发;后者必须换新 request_id |

---

## 11. capability 驱动的控件显隐矩阵

唯一事实源 = `runtimeSnapshot.capabilities`(或设备级 CapabilitySnapshot)。
前端**不得**用版本字符串推断能力;未知版本必须只读
(CODEX-COMPATIBILITY:写链 FIXTURE_ONLY,`VERIFIED_VERSIONS` 未覆盖时
supportedOperations 为空)。

| UI 控件 | 显示条件 | 禁用条件(显示但不可用) |
|---|---|---|
| 输入框 + 发送(START_TURN) | controlMode ∈ {FULL_CONTROL, LIMITED_CONTROL} 且 operation ∈ supportedOperations | deviceConnection ≠ ONLINE;activeTurnPhase = RUNNING(此时发送入队,不是 start);存在未处理 valid attention |
| 队列输入(QUEUE_SET/REPLACE/CANCEL) | 同上且含对应 operation | queueState = EMPTY 时 REPLACE/CANCEL 不渲染;设备离线时整个队列编辑器禁用 |
| Steer 按钮 | operation ∈ supportedOperations **且** 用户在"发送方式"中显式选择 steer | phase ≠ RUNNING;expected turn 未知 |
| Interrupt 按钮 | operation ∈ supportedOperations | phase ≠ RUNNING;点击后必须二次确认,携带 expected_turn_id |
| 问题卡提交 | `pendingQuestions[].valid === true` | 过期/回答后(valid=false)置灰 |
| 审批卡提交 | `pendingApprovals[].valid === true` | 同上;decisions 只渲染原生列表,不附加选项 |
| 设置面板 | `capabilities.settings` 非空 | 单项 `mutable=false` 置灰;可选项只渲染 availableValues |
| 停止单条后台命令 | `OPERATION_STOP_BACKGROUND_COMMAND` ∈ supportedOperations 且该项有稳定 commandId | 否则只显示全局停止 |
| 停止全部后台命令 | `OPERATION_STOP_ALL_BACKGROUND_COMMANDS` ∈ supportedOperations | 触发后按 warnings 含 BACKGROUND_STOP_GLOBAL_ONLY 提示 |
| 新建任务 / 重命名 / 归档 / fork | **当前版本均不渲染**(CREATE_TASK/rename/archive/fork = UNSUPPORTED,CODEX-COMPATIBILITY §3/§4) | 若未来 supportedOperations 出现对应值才渲染 |
| 时间线/输出/历史 | READ_ONLY 下仍可用 | 设备 OFFLINE 时整体隐藏(改摘要视图) |

---

## 12. 示例:curl 只读链 + TypeScript WS 连接

> 示例中全部为占位符,无真实凭据。

### 12.1 curl(登录后)

```bash
BASE=https://toolbox.example.com

# 1. 一次性 ticket(需要 dev-toolbox 登录 Cookie + CSRF)
curl -s -X POST "$BASE/api/v1/agent-console/ws-tickets" \
  -H "Origin: $BASE" -H "X-CSRF-Token: $CSRF" \
  -H "Cookie: $SESSION_COOKIE"    # 浏览器内 fetch 无需手动拼
# → {"ticket":"<base64url>","expiresAt":"...","audience":"relay"}

# 2. 会话列表(经网关 forward-auth;浏览器内只需 Cookie)
curl -s "$BASE/agent-console/api/sessions?limit=50" -H "Cookie: $SESSION_COOKIE"

# 3. 运行快照(设备必须在线)
curl -s "$BASE/agent-console/api/sessions/<session-uuid>/runtime" -H "Cookie: $SESSION_COOKIE"

# 4. Git 摘要与单文件 Diff
curl -s "$BASE/agent-console/api/sessions/<session-uuid>/git" -H "Cookie: $SESSION_COOKIE"
curl -s "$BASE/agent-console/api/sessions/<session-uuid>/git/diff?path=src/main.rs&staged=false" \
  -H "Cookie: $SESSION_COOKIE"
```

### 12.2 TypeScript(WS 连接 + 订阅 + Ack)

```ts
import {
  encodeEnvelope, decodeEnvelope, newMessageId, PROTOCOL_VERSION,
  EnvelopeSchema, ClientHelloSchema, SubscribeSchema, AckSchema,
  ClientKind, Envelope,
} from "@agent-console/protocol";
import { create } from "@bufbuild/protobuf";

// ticket: POST /api/v1/agent-console/ws-tickets 返回(30s 有效,单次)
async function getTicket(): Promise<string> {
  const res = await fetch("/api/v1/agent-console/ws-tickets", {
    method: "POST",
    headers: { "X-CSRF-Token": csrfToken }, // Origin/Cookie 由浏览器自动带
  });
  if (!res.ok) throw new Error(`ticket failed: ${res.status}`);
  const { ticket } = await res.json();
  return ticket;
}

function b64url(s: string): string {
  return btoa(String.fromCharCode(...new TextEncoder().encode(s)))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export async function connect(): Promise<WebSocket> {
  const ticket = await getTicket();
  const ws = new WebSocket(
    `wss://${location.host}/agent-console/ws`,
    // ticket 只进子协议,不进 URL(§20.2)
    ["agent-console.v1", `agent-console.ticket-${b64url(ticket)}`],
  );
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    const hello = create(EnvelopeSchema, {
      protocolVersion: PROTOCOL_VERSION,
      messageId: newMessageId(),
      payload: { case: "clientHello", value: create(ClientHelloSchema, {
        protocolVersion: PROTOCOL_VERSION,
        clientKind: ClientKind.CLIENT_BROWSER,
      }) },
    });
    ws.send(encodeEnvelope(hello));
  };
  return ws;
}

function subscribeList(ws: WebSocket) {
  const env = create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: newMessageId(),
    payload: { case: "subscribe", value: create(SubscribeSchema, { target: { case: "list", value: {} } }) },
  });
  ws.send(encodeEnvelope(env));
}

function ack(ws: WebSocket, streamId: string, sequence: bigint) {
  const env = create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: newMessageId(),
    payload: { case: "ack", value: create(AckSchema, { streamId, sequence }) },
  });
  ws.send(encodeEnvelope(env));
}

ws.onmessage = (ev) => {
  const env: Envelope = decodeEnvelope(new Uint8Array(ev.data));
  switch (env.payload?.case) {
    case "serverHello":  /* 握手完成 → subscribe */ break;
    case "subscribed":   /* 记录 streamId/streamEpoch/baseSequence */ break;
    case "sessionSummaryBatch": /* snapshot===true 时重建列表 */ break;
    case "runtimeSnapshot":     /* 重建详情 */ break;
    case "eventBatch":
      for (const e of env.payload.value.events) applyEvent(e);
      ack(ws, env.payload.value.streamId, env.sequence);
      break;
    case "resyncRequired":      /* 丢弃本地状态,重新 subscribe */ break;
    case "protocolError":       /* 按 env.payload.value.errorCode 处理 */ break;
  }
};
```

重连提示:断开后 `getTicket()` 重新取票再 `connect()`;收到
`resyncRequired` 时对同一会话重新 `Subscribe`。
