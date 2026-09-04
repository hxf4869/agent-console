# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Vue 3 + TypeScript + Vite。浏览器优先，可安装为 PWA；首版桌面和手机共用同一响应式 Web 应用。

## Users

首版服务一名开发者。用户继续在一台 Mac 上使用 Codex Desktop，并在桌面浏览器或手机 PWA 中查看、判断和控制同一批任务。

## Product Purpose

Agent Console 是自托管的多 Agent 控制台首版。它让用户离开 Mac 时仍能查看 Codex Desktop 任务进度，集中处理提问与风险审批，调整下一轮设置并继续发送指令。成功意味着用户能快速识别待处理事项、理解当前运行事实，并在能力与风险边界清楚的前提下采取下一步。

## Positioning

浏览器不另起一套执行会话；本机 Bridge 读取并控制 Codex Desktop 已拥有的任务，Relay 只负责认证、路由和临时数据转发。Codex Desktop 始终是运行方和唯一写入 owner。

## Operating Context

- 桌面端是高信息密度控制台，用于同时浏览任务集合、运行时间线、计划、命令、文件和 Git 只读信息。
- 手机端以待处理提问、审批和运行异常为首要入口，使用单栏与显式两级导航。
- 网络可能断开，设备可能离线，未知 Codex 版本可能降级为只读；执行中输出只是最佳努力预览，终态后由权威结果替换或确认。
- 用户可能面对长历史、大段命令输出、逐文件 Diff、文件预览和仍在运行的后台命令。

## Capabilities and Constraints

- 首版只支持 Codex Desktop、一名用户和一台 Mac。
- 支持设备绑定、任务分页、运行详情、Codex 提问、风险审批、模型/思考深度/Fast/权限/协作模式、单条下一轮队列、计划、后台命令、文件预览与 Git 只读 Diff。
- 所有写控件由实时 capability、control mode、compatibility、turn revision 和连接状态共同决定；前端不得猜测或静默降级。
- 不提供任意 Shell、终端 stdin、Git 写操作、任务永久删除或第二套会话运行时。
- Relay 不长期保存提示词、回复、命令输出、Diff、文件内容或本机绝对路径。
- 协议生成包未就绪时，前端只使用窄 transport 接口与脱敏 fixture；不发明后端字段。

## Brand Commitments

- 产品名为“Agent Console”。
- 继承 `dev-toolbox/DESIGN.md` 的 Graphite 深浅表面、克制蓝色强调、发丝边界、系统无衬线与紧凑工程工具气质。
- 默认深色并完整支持浅色；不用渐变、玻璃拟态、大面积发光、营销式装饰或无意义动画。
- 除技术专有名词外使用简体中文。

## Evidence on Hand

- 权威功能与状态合同：`docs/ZCODE-BACKEND-EXECUTION-SPEC.md`。
- 视觉基线：同级 `dev-toolbox/DESIGN.md`、其现有壳层、令牌和基础组件，以及精确的 R1 Stitch Screen。
- 开发阶段只使用脱敏 fixture；没有可用于产品展示的真实用户任务内容，也不得虚构商业证明。

## Product Principles

- 待处理优先：问题、审批、断线、结果不确定和需要用户决定的状态先于装饰性概览。
- 事实分层：实时预览、权威最终结果、只读数据和不可用状态始终清楚区分。
- 能力诚实：控件随能力和连接状态显式启用、禁用或解释，不隐藏失败边界。
- 一处主操作：每个视图保持一个最明确的下一步，其余操作降级但可发现。
- 跨设备延续：桌面保持密度，手机重排任务优先级，不把桌面工作台缩成横向滚动缩略图。

## Accessibility & Inclusion

目标为 WCAG 2.2 AA。关键路径必须支持键盘、可见焦点、语义化控件、屏幕阅读器状态播报、至少 44×44 CSS px 的手机触控目标、200% 缩放与 `prefers-reduced-motion`。
