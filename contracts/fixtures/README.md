# Fixtures(§28 清单;全部为合成数据,不含任何真实会话内容)

每个 fixture 与生产响应 **相同 schema/键名/枚举值**:

- RuntimeSnapshot 系列与 `GET /agent-console/api/sessions/{id}/runtime` 响应同形
  (`{"runtimeSnapshot": ...}`,键名来自 `apps/relay/src/sessions/json.rs` 的
  `runtime_snapshot_json()`,camelCase,与 Protobuf JSON 映射一致)。
- `session-list.json` 与 `GET /agent-console/api/sessions` 分页响应同形。
- `device-online.json` / `device-offline.json` 与 `GET /agent-console/api/devices` 同形。
- `git-dirty-summary.json` 与 `GET .../git` 响应同形。
- `file-changed-error.json` 与统一错误对象同形(state.rs `api_error`)。
- 枚举值为 proto 枚举名(as_str_name 输出,如 `TURN_PHASE_RUNNING`、
  `QUEUE_STATE_QUEUED`、`BACKGROUND_CMD_RUNNING`、`COMPATIBILITY_DEGRADED`)。

例外:`output-gap.json` 描述的是 **WS 详情流事件**(二进制 Protobuf,无法直接
放 JSON fixture),按 `events.proto` 的 camelCase JSON 映射给出等效视图,文件内
注明该限制;`bytesBase64` 字段对应 proto 的 `bytes`。

| 文件 | UI 状态 | 消费端点/来源 |
|---|---|---|
| `device-online.json` | 设备列表 · 在线(可查询/命令/文件面) | `GET /agent-console/api/devices` |
| `device-offline.json` | 设备列表 · 离线(全功能禁用;degradedReason 为空,不猜测原因) | 同上 |
| `session-list.json` | 会话列表:运行中+审批角标、排队中、离线只读三种组合 | `GET /agent-console/api/sessions`(分页 `{sessions,nextCursor}`) |
| `runtime-running.json` | 详情 · TURN_PHASE_RUNNING:计划进度、运行中命令、LIVE_PREVIEW 输出游标、能力/设置/transfer 限制 | `GET /agent-console/api/sessions/{id}/runtime` 或 WS 详情订阅快照 |
| `runtime-waiting-question.json` | 详情 · 等待用户回答(`valid:true` 时才能提交;只发 optionId) | 同上 / WS `PendingAttentionAdded(question)` |
| `runtime-waiting-approval.json` | 详情 · 等待风险审批(只按原生 decisionId 决定) | 同上 / WS `PendingAttentionAdded(approval)` |
| `runtime-completed-background-command.json` | 详情 · 上一轮 COMPLETED(idle,currentTurn=null)+ 后台命令仍 RUNNING/COMPLETED | 同上 |
| `runtime-read-only-unverified.json` | 详情 · 未知版本降级:COMPATIBILITY_DEGRADED + CONTROL_MODE_READ_ONLY,supportedOperations 空 → 写控件全部隐藏/禁用 | 同上 |
| `queue-queued.json` | 详情 · 队列 QUEUED(绑定 afterTurnId + acceptedRuntimeRevision;正文不在快照中) | 同上 / WS `QueueStateChanged` |
| `queue-paused.json` | 详情 · 队列 PAUSED(只能用户重新确认,不能自动改绑) | 同上 |
| `output-gap.json` | 详情 · 输出缺口:offset 0+17 → 2048 不连续,暂停拼接等 OutputReplace / RESYNC_REQUIRED | WS 详情流 EventBatch(JSON 视图) |
| `file-changed-error.json` | 文件面板 · FILE_CHANGED:刷新 handle 后经用户确认再重试 | `POST .../files/preview`、`POST .../files/download` 的错误响应 |
| `git-dirty-summary.json` | Git 面板 · 工作区脏:modified/added/deleted + binary 标记、总增删行 | `GET /agent-console/api/sessions/{id}/git` |

隐私约束:所有 ID 为保留段合成 UUID/字符串,标题、路径、输出均为 `fixture`
字样合成内容;不得用真实用户会话数据回填(§28)。
