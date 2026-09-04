# Agent Console Web

Vue 3 + TypeScript + Vite 的响应式 PWA 前端。默认使用同源 `RealConsoleTransport` 接入 dev-toolbox 认证、Relay HTTP/WS、Bridge 与 Desktop owner；只有 URL 显式带 `?fixture=1`（或构建环境显式设置 `VITE_AGENT_CONSOLE_FIXTURE=true`）时才进入标注为 `Fixture only` 的脱敏演示模式。它不会启动第二套 Codex runtime，也不会在浏览器执行任意 Shell、stdin 或 Git 写操作。

## 本地运行

`apps/web` 是自管理的 pnpm 项目，不在仓库根 `pnpm-workspace.yaml` 中。
开发、类型检查、测试和构建使用本地协议包的 `source` 导出（已跟踪源码），
不要求预先生成或保留被忽略的协议 `dist`。

```bash
cd apps/web
pnpm install --ignore-workspace
pnpm dev
```

开发地址为 `http://localhost:4174/agent-console/`。验证命令：

```bash
pnpm check
pnpm test
pnpm build
```

## 页面

- `/`：问题和风险审批组成的 attention-first 首页。
- `/tasks`：支持搜索和显式分页的任务索引。
- `/tasks/:sessionId`：计划、commentary、命令输出、后台命令、问题/审批、单条下一轮队列和运行设置。
- `/tasks/session-offline`：离线、只读、`OUTCOME_UNKNOWN` 和 `FINAL_OUTPUT_UNAVAILABLE` 的可验收状态。
- `/tasks/:sessionId/git`：只读文件预览、分支和逐文件统一 Diff。
- `/devices`：设备绑定、连接路径、能力快照与离线契约。

深色为默认主题，也支持浅色主题和系统减少动效偏好。桌面采用高密度 master-detail，手机采用单列 attention-first 顺序。生产构建会注册只缓存静态资源的 Service Worker；`/api/` 从不进入缓存。

## 前端边界

`src/transport/types.ts` 是窄前端契约；`RealConsoleTransport` 是默认实现，`FixtureConsoleTransport` 仅用于显式演示和状态验收。真实 transport 负责同源会话检查、CSRF、一次性 WS ticket、Protobuf 帧、心跳/重连、订阅顺序与 resync。输出状态由 `output-reducer.ts` 按 UTF-8 byte offset 处理：

- `append` 只在 `expectedOffset` 连续时追加；
- `replace` 是发现缺口后的恢复点；
- `final` 只有 byte length 一致时才标记为 `AUTHORITATIVE_FINAL`；
- 无法恢复时明确显示 `FINAL_OUTPUT_UNAVAILABLE`，不会伪装成成功。

所有写操作都先经过 connection、control mode、compatibility 和 operation capability 四层判定。下一轮队列最多一条，正文应由 Bridge 保存；前端只展示并提交受支持的意图。

## 已接入的后端边界

网络逻辑保持在 transport，不散落到视图：

1. `@agent-console/protocol` 负责 WS Envelope 的 Protobuf encode/decode，展示模型仍在前端映射层内。
2. 未登录访问会跳转 dev-toolbox；登录成功只接受同源 `/agent-console/` return path。
3. 任务索引、runtime snapshot、历史游标、输出补偿、设备绑定/撤销、只读 Git 与临时文件传输均走真实 Relay 接口。
4. 控制命令携带 `requestId`、`expectedTurnId` 和 `expectedRuntimeRevision`，重连按 request ID 查询/回放回执，不把未知结果伪装为成功。
5. 所有控制按设备连接、ControlMode、兼容状态和 operation capability 动态门控。当前 0.153.1 只开放已验证的 start / steer / interrupt 及 Bridge 单条下一轮队列；问题/审批、设置写入和 IPC 不支持的任务管理操作保持关闭。
6. Relay 只持久化摘要和回执；prompt、回复、输出、Diff、文件正文与绝对路径不落 Relay 数据库。
