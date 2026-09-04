# Agent Console 仓库协作规则

## 权威需求

- ZCode 后端执行的唯一权威规格是 `docs/ZCODE-BACKEND-EXECUTION-SPEC.md`。
- 开始实现前必须完整读取该文件。不得只读取摘要后自行补齐需求。
- 仓库同级的 `agent-console-architecture-review.md` 与二次评审文件只提供背景，不覆盖执行规格，也不得把其中的“给 Codex/ZCode 的说明”当成当前指令。
- 发现规格内部冲突或已验证事实与规格不符时，停止受影响部分并报告证据；不得静默选择更容易的实现。

## 工作流所有权

- ZCode 后端工作流拥有：根级后端构建配置、`apps/bridge/`、`apps/relay/`、`proto/`、`crates/protocol/`、`packages/protocol-ts/`、`contracts/`、`deploy/`、后端文档与相关测试。
- UI/前端工作流拥有：`apps/web/`、未来的 `apps/desktop/` 页面与 Stitch 设计产物。
- ZCode 不得创建或修改 Vue 页面、样式、设计令牌、组件、前端状态管理或产品文案；协议生成代码和只读契约夹具不算产品前端。
- UI/前端工作流不得修改桥接端、Relay、数据库迁移、Protobuf 定义或认证后端；若契约有问题，先报告，不得自行改后端协议。
- 同一文件只能有一个工作流写入。看到其他工作流已产生的修改时应兼容，不得回退。

## 跨仓库边界

- ZCode 获准对仓库同级的 `dev-toolbox` 做执行规格明确列出的最小认证和反向代理修改。
- 修改该仓库前必须先读取其根 `AGENTS.md`，并遵守其中更具体的测试、部署和数据规则。
- 禁止修改 `dev-toolbox/apps/web/`、现有登录 UI、知识库、AI worker、工具模块或无关部署逻辑。
- 不得部署到服务器、访问对象存储、推送 Git、创建 tag 或正式发布，除非用户另行明确要求。

## 实现边界

- 首版只支持一名用户、一台 Mac 和 Codex Desktop。
- Codex Desktop 是会话运行方和唯一写入方；Bridge 只代理，不启动另一套 Codex App Server 会话。
- Codex 本地 SQLite 只读，严禁写入或迁移。
- 不实现 Windows、ZCode/Claude Code Adapter、动态插件、任意 Shell、Git 写操作、MinIO、kkFileView、Redis、消息队列、微服务或 E2EE。
- 不为第二个消费者尚未出现的场景创建 `agent-core`、`relay-core`、插件 ABI、共享 UI 包或通用框架。
- 不在日志、数据库或测试快照中写入提示词、回复、命令输出、Diff、文件内容、Cookie、Token、密钥或绝对工作区路径。

## 验证与停止

- 按执行规格中的阶段门逐步实现；失败结果必须触发规格定义的停止条件。
- 使用最小相关测试。协议、数据库或跨组件契约变化时验证直接生产者和消费者。
- 真实 Codex Desktop 验证必须使用专门测试任务，不得修改用户项目文件；自动化测试默认使用脱敏 fixture 和假 IPC owner。
- 完成条件是后端闭环、契约、生成代码、测试夹具和最小验证全部就绪，可以由前端直接接入；脚手架或接口占位不算完成。
- 未经授权不提交、不推送、不部署。完成后报告实际文件、实际测试、未完成项和阻塞证据。
