# @agent-console/protocol

Agent Console Protobuf 协议的 TypeScript 包:由仓库根 `proto/agent_console/v1/*.proto`
经 `@bufbuild/protoc-gen-es` v2 生成,外加一个薄编解码入口。

本包**只包含**协议类型、Schema 与二进制编解码;不包含 WebSocket 客户端、
fetch 封装、Vue 组件、store、样式或任何产品文案(仓库所有权规则 §3)。

## 内容

- `src/generated/` — protoc-gen-es 生成的类型与 Schema(`*_pb.ts`),**禁止手工编辑**。
  按 proto 源文件分组:
  - `common_pb.ts` — §10 多维状态枚举、`StableErrorCode`、`SessionKey`、`TurnId`/`ItemId`、
    `DevicePresence`、`TransferLimits`、`ProviderExtension`
  - `session_pb.ts` — `SessionSummary`、`SessionSummaryBatch`
  - `runtime_pb.ts` — `RuntimeSnapshot`、`CapabilitySnapshot`、`Item`、`HistoryPage`、
    查询消息、设置/问题/审批/队列结构
  - `events_pb.ts` — `EventBatch`、`OutputAppend/Replace/Final`、`TurnLifecycle` 等领域事件
  - `commands_pb.ts` — `Operation`、`CommandRequest`、`CommandAccepted`、`CommandResult`
  - `transfers_pb.ts` — `TransferOffer`、`TransferReady`、`TransferResult`(仅协调,不含正文)
  - `envelope_pb.ts` — `Envelope`、Hello、Subscribe/Subscribed/Ack、Resync、Heartbeat、
    `ProtocolError`
- `src/codec.ts` — `encodeEnvelope` / `decodeEnvelope`(1 MiB 帧上限,§17.6)、
  `PROTOCOL_VERSION`、`MAX_OUTPUT_CHUNK_BYTES`(64 KiB,§13.2)、`newMessageId` /
  `newCorrelationId`。
- `src/index.ts` — 统一出口。

## 构建

```bash
# 仓库根执行(pnpm-workspace 已包含 packages/*)
pnpm install
pnpm --filter @agent-console/protocol build   # 产物输出 dist/(ESM + d.ts)
```

## 再生成

修改 `proto/agent_console/v1/*.proto` 后,在仓库根执行:

```bash
./scripts/gen-protobuf.sh
```

脚本会:
1. 用系统 `protoc` + `pnpm -C packages/protocol-ts bin` 下的 `protoc-gen-es`
   插件再生成 `src/generated/`(`--es_opt=target=ts`);
2. 用 `cargo build -p agent_console_protocol` 再生成 Rust 侧代码
   (需要 `cargo` 在 PATH,或 `source ~/.cargo/env`);
3. 打印 `git status` 提示人工检查 diff。

## 约定

- 字段号一旦进入前端消费即冻结(§17.1):不得复用、不得改号;删除字段必须 `reserved`。
- 枚举值在同一 proto package 内共享命名空间:`StableErrorCode` 使用与规格 §27.6
  完全一致的裸全大写值,其余枚举带类型前缀。
- `StableErrorCode` 与 HTTP JSON 错误的 `code` 是同一稳定集合(§27.5),
  前端不得依赖自由文本判断流程。
- Envelope 禁止携带 Cookie、凭据、文件正文、本机绝对路径(§17.2);
  文件正文只走 HTTPS transfer 流(§22.1)。
- 本包类型不含 UI/网络代码;消费方(浏览器端)用打包器引入,
  `tsconfig` 使用 `moduleResolution: "bundler"`。
