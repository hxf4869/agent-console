# 稳定错误码(§27.6)与 HTTP 状态映射

- 码集合的权威来源:`proto/agent_console/v1/common.proto` 的 `StableErrorCode`
  (数值冻结;新增只能追加)。HTTP JSON `error.code`、Protobuf
  `ProtocolError.error_code`、`QueryResponse.error_code`、`CommandResult.error_code`
  使用同一集合。
- HTTP 状态映射权威来源:`apps/relay/src/state.rs` 的 `status_for_code()`
  (本文照代码登记;个别 handler 有覆盖,见"特例"列)。
- 错误响应统一形状:

```json
{
  "error": {
    "code": "STABLE_CODE",
    "message": "面向用户的简体中文说明",
    "requestId": "<relay 生成的 uuid>",
    "details": {}
  }
}
```

前端不得依赖 `message`/`details` 自由文本判断流程;`code` 是唯一分支依据。

## §27.6 全部 32 个稳定码

| 码 | 语义 | 典型 HTTP 状态 | 前端处理建议 |
|---|---|---|---|
| `AUTH_REQUIRED` | 未登录 / 缺少有效网关身份头 / 设备或 transfer 凭据缺失无效;配对 v2 中 challenge 格式无效(register)、challenge 不匹配(claim)、短码不正确(approve)也用本码 | 401 | 跳转 dev-toolbox 登录,登录后回跳 `/agent-console/` 前缀内的原路径;配对错误按 details.reason 区分(INVALID_CHALLENGE / CHALLENGE_MISMATCH / CODE_MISMATCH) |
| `AUTH_EXPIRED` | 登录过期(身份头 expires_at 已过;WS 关闭 reason 亦用此名) | 401 | 同上;WS 断开 reason=AUTH_EXPIRED 时必须重新走 ticket 流程,不自动重连旧连接 |
| `CSRF_INVALID` | 写请求 Origin/CSRF 校验失败(dev-toolbox 侧) | 500¹ | 网关 verify 拦截阶段发生;刷新页面重新建立 CSRF 上下文后重试 |
| `WS_TICKET_INVALID` | ticket 不存在或绑定 session 已失效 | 401 | 重新 `POST /api/v1/agent-console/ws-tickets` 取新 ticket 再连 WS |
| `WS_TICKET_EXPIRED` | ticket 超过 30 秒有效期 | 401 | 同上(重新申请,勿重试旧 ticket) |
| `WS_TICKET_CONSUMED` | ticket 已被消费(单次使用) | 401 | 同上;通常意味着重复连接尝试 |
| `DEVICE_OFFLINE` | 目标设备 Bridge 未连接,在线查询/文件/命令无法完成 | 503 | 展示设备离线态;不轮询重试查询,等 WS presence 事件恢复;禁止创建/替换队列 |
| `DEVICE_REVOKED` | 设备已被撤销 | 503 | 引导重新配对;WS reason=DEVICE_REVOKED 时不重连 |
| `CODEX_UNAVAILABLE` | Bridge 在线但 Codex/Desktop 侧不可用或未返回结果 | 503 | 提示"Codex 不可用",可稍后手动重试 |
| `CODEX_VERSION_UNVERIFIED` | Desktop 版本未通过兼容验证 | 500¹ | 提示版本不受支持;UI 按 READ_ONLY 渲染,不重试写操作 |
| `CONTROL_READ_ONLY` | 当前会话/设备只读,写操作被拒 | 500¹ | 隐藏/禁用写控件(控件显隐应由 capability 驱动,见 FRONTEND-INTEGRATION §11) |
| `CAPABILITY_UNSUPPORTED` | 当前版本不支持该操作 | 500¹ | 隐藏对应控件;不自动降级为其他操作 |
| `SESSION_NOT_FOUND` | 会话/设备/回执/配对/订阅不存在或不属于当前用户 | 404 | 刷新列表;深链接失效时回到会话列表 |
| `STALE_TURN` | expected turn/runtime revision 已变化 | 409 | 拉取最新 RuntimeSnapshot 后让用户确认重发;不静默重发 |
| `DUPLICATE_REQUEST_MISMATCH` | 同一 request_id 但内容不同;配对 v2 中凭据已被领取(claim,409 details.reason=CONSUMED)与重复批准(approve,409 details.reason=ALREADY_APPROVED)复用本码 | 409 | 生成新 request_id 重新提交;不得复用旧 ID;配对场景按"刷新配对状态/提示凭据已交付"处理 |
| `OUTCOME_UNKNOWN` | 连接中断且无法证明 Desktop 是否执行 | 500¹ | **不得自动重发**;展示"结果未知",以 RuntimeSnapshot/receipt 查询为准让用户决策 |
| `RESYNC_REQUIRED` | sequence 缺口超出窗口 / epoch 变化 / Relay 重启 | 500¹(WS 用) | 收到 ResyncRequired 后重新 Subscribe 并以新 snapshot 重建本地状态 |
| `QUEUE_ALREADY_EXISTS` | 已有单条队列又尝试 set | 409 | 先读队列状态;替换需用 QUEUE_REPLACE 语义 |
| `QUEUE_PAUSED` | 队列处于 PAUSED,不会自动发送 | 500¹ | 展示"已暂停",只能由用户显式重新确认,不能自动改绑新 turn |
| `QUESTION_EXPIRED` | 原生问题已失效 | 500¹ | 置灰问题卡片,以最新 snapshot 的 valid 状态为准 |
| `APPROVAL_EXPIRED` | 原生审批已失效;配对请求已过期(v2:claim/approve 均 410,details.reason=EXPIRED) | 500¹(410⁷) | 同上;配对过期需在 Bridge 重新发起配对(register 领新短码) |
| `SETTING_COMBINATION_UNSUPPORTED` | 设置组合不受支持 | 500¹ | 回显设置面板当前值;不静默降级到其他模型/权限 |
| `FILE_HANDLE_INVALID` | file handle 无效/过期 | 500¹(400²) | 重新请求 files/metadata 刷新 handle 后重试一次 |
| `FILE_OUTSIDE_SCOPE` | 文件不在授权范围内 | 500¹ | 不重试;提示无权限访问该文件 |
| `FILE_CHANGED` | 打开后文件被替换(TOCTOU 复验失败) | 500¹ | 展示"文件已变化",重新获取 metadata/handle,不静默读新内容(见 fixtures/file-changed-error.json) |
| `FILE_TYPE_NOT_PREVIEWABLE` | 类型/大小不可内联预览(可下载) | 500¹ | 依据 metadata.notPreviewableReason 提供下载入口 |
| `TRANSFER_EXPIRED` | transfer 不存在/过期/对端未就绪/被取消 | 504³(404⁴) | 提示可重试;不放入离线队列 |
| `TRANSFER_TOO_LARGE` | 超过下载 512 MiB / 上传 20 MiB 上限 | 500¹(413⁵) | 上传前本地预检大小;下载超限时只提供提示 |
| `TRANSFER_RANGE_INVALID` | 多段/逆序/越界或格式错误的 Range | 500¹(400²) | 修正 Range 逻辑后重试;不整文件回退 |
| `DIFF_TOO_LARGE` | Diff 超过单响应上限 | 500¹ | 改用分文件/分页方式;GitFileDiff.truncated=true 时提供下载 |
| `RATE_LIMITED` | 并发 transfer 超限 / 配对 v2 各端点来源限速(register/claim/lookup/approve)/ 短码错误次数超限锁定(lookup/approve,429 details.reason=LOCKED) | 429 | 退避后重试;LOCKED 时配对已锁定,需重新发起配对 |
| `INTERNAL_ERROR` | 未分类内部错误(也是 UNSPECIFIED 的对外名) | 500 | 展示通用错误;可安全重试只读查询 |

状态列脚注(以代码为准的 handler 覆盖,均已逐一对齐 `apps/relay/src`):

1. `status_for_code()` 默认映射为 500(INTERNAL_ERROR 同)。
2. files preview/download handler 显式返回 400(请求体无效、`FILE_HANDLE_INVALID` 缺失、`TRANSFER_RANGE_INVALID`)。
3. `transfers::transfer_error`:`TRANSFER_EXPIRED` → 504。
4. upload_stream / producer / consumer 中 transfer 不存在或已过期 → 404。
5. upload 声明与实际字节超限 → 413(PAYLOAD_TOO_LARGE)。
6. (已随配对 v2 废弃)旧版"配对状态冲突 → 409 + RATE_LIMITED"不再存在;见脚注 7。
7. 配对 v2 的冲突/过期覆盖(`apps/relay/src/devices/pairing.rs`):
   - claim 凭据已被领取 → 409 + `DUPLICATE_REQUEST_MISMATCH`(details.reason=CONSUMED);
   - approve 已被批准(并发单赢家)→ 409 + `DUPLICATE_REQUEST_MISMATCH`(details.reason=ALREADY_APPROVED);
   - claim/approve 挑战过期 → 410 + `APPROVAL_EXPIRED`(details.reason=EXPIRED);
   - lookup/approve 短码无效/已取消 → 404 + `SESSION_NOT_FOUND`(details.reason=NOT_FOUND/CANCELLED);
   - 短码错误超限锁定与来源限速 → 429 + `RATE_LIMITED`(details.reason=LOCKED/RATE_LIMITED)。

## 补充码(非 §27.6 集合,前端需要识别)

| 码/值 | 类别 | 出处 | 语义与前端处理 |
|---|---|---|---|
| `BACKGROUND_STOP_GLOBAL_ONLY` | CommandResult warning | `apps/bridge/src/commands/background.rs` | 只具备全局停止能力时,"停止全部后台命令"结果的 warnings 携带本码;UI 应提示"已请求停止全部后台命令(无法逐项停止)",不显示逐项停止控件 |
| `SESSION_INVALID` | ticket consume reason | dev-toolbox `apps/api/internal/auth/agent_console.go`(`ticketReasonSessionInvalid`) | 内部 consume 失败原因之一;Relay 将其与 NOT_FOUND 一起映射为 `WS_TICKET_INVALID`。前端在 WS 升级 401 时统一按"重新取 ticket"处理 |
| `CONSUMED` / `EXPIRED` / `NOT_FOUND` | ticket consume reason | 同上 | 分别映射 `WS_TICKET_CONSUMED` / `WS_TICKET_EXPIRED` / `WS_TICKET_INVALID`(见 `apps/relay/src/auth/mod.rs` TicketReason) |
| `REVOKED` / `EXPIRED` / `IDLE_TIMEOUT` / `PASSWORD_CHANGED` / `NOT_FOUND` | introspect revokedReason | dev-toolbox 内部 introspect 响应 | 仅内部;Relay 收到非 valid 一律以 close reason `AUTH_EXPIRED`(1008)关闭浏览器 WS |
| `PROTOCOL_VERSION_MISMATCH` | WS close reason(1002) | `apps/relay/src/auth/browser_ws.rs` | 握手协议版本不匹配;提示刷新/升级前端后重连 |
| `AUTH_BACKEND_UNAVAILABLE` | CommandResult details.reason | `apps/relay/src/realtime/mod.rs` | dev-toolbox 不可达宽限期内拒绝写命令;提示"认证服务暂不可用,写入已暂停",恢复后重试 |
| `FINAL_OUTPUT_UNAVAILABLE` | 输出终态标记(规格 §13.3) | 规格/事件语义 | 最终输出无法读取时不得把预览冒充完整结果;UI 展示"最终结果不可用",可尝试 output 分页查询 |

## 代码/文档不一致登记(1/3/6 已在最终审计批次修复)

1. ~~身份头 `X-Agent-Console-Session-Expires` 缺失~~ **已修复**:Relay(`apps/relay/src/auth/mod.rs`)已将 `expires_at` 改为可选头——缺失时跳过本地预过期检查(网关 forward-auth 已拒绝过期会话;长连接撤销由 WS introspection 循环负责,§20.5)。dev-toolbox verify 只需返回 Session-Id/Owner-Id 两个头即可打通。
2. **`contracts/` 此前不存在**:规格 §28 要求的 openapi/fixtures 交接物为本轮首次创建;此前仓库没有任何契约文件。
3. ~~`deploy/reverse-proxy/` 模板落后于 dev-toolbox 实现~~ **已修复**:caddy.snippet.example 与 nginx.agent-console.example.conf 已改为与 dev-toolbox 实际实现一致(`X-Agent-Console-*` 头名、`GET /internal/agent-console/auth-sessions/verify` forward-auth、Bearer 内部 Token)。
4. **设备 404 复用 `SESSION_NOT_FOUND`**:设备不存在/订阅不存在等 404 均以 `SESSION_NOT_FOUND` 承载,无独立 `DEVICE_NOT_FOUND` 码;前端按 404 + 上下文区分。
5. **配对 v2 错误语义**(`apps/relay/src/devices/pairing.rs`;旧 Browser-first 的
   `/internal/pairing/register|complete` 端点与"冲突复用 RATE_LIMITED"约定已删除):
   配对方向为 Bridge 发起(register/claim 设备面 + lookup/approve/challenges 浏览器面),
   错误映射见上表脚注 7;前端按 `details.reason` 区分流程分支,`code` 仍是唯一稳定码依据。
6. ~~`.env.example` 的 Bridge 变量名与代码不一致~~ **已修复**:`.env.example` 已改为 `AGENT_CONSOLE_RELAY_URL`/`AGENT_CONSOLE_DATA_DIR` 等代码实际读取的变量名。
