# Agent Console Stitch 设计交接

## 项目

- Stitch 项目：[Agent Console · Desktop & Mobile Control Center](https://stitch.withgoogle.com/projects/14285974708785281253)
- Project ID：`14285974708785281253`
- Graphite Precision design system asset：`0cd0f4e04e7c4db6b28edf78a37d66ac`
- 来源：只读导入 `dev-toolbox/DESIGN.md`，并以 `dev-toolbox` 的 shell、tokens、基础组件和响应式文档为视觉基线。

## R2 最终参考屏

| 场景 | Stitch screen ID | 生产实现 |
| --- | --- | --- |
| 桌面行动工作区 | `e1eb35a786914c0daf9ddb7608dd6b44` | `src/components/AppShell.vue`、`src/views/AttentionView.vue`、`src/views/TaskDetailView.vue` |
| 手机待处理主方案 | `43267f0ed185436086923d86fe90c349` | `src/views/AttentionView.vue` |
| 手机待处理补充方案 | `4c4e8ee20d414ee3a8874e3a6a648343` | `src/views/AttentionView.vue`、`src/views/DevicesView.vue` |
| 手机任务工作区 | `244533aa215447d1be8691d15291d55a` | `src/views/TaskDetailView.vue`、`src/views/GitWorkspaceView.vue` |

上传并转换的设计文档 screen instance 为 `5491690731850859827`。

## 采用的设计方向

- 核心不是聊天框、指标看板或协议说明页，而是“扫列表 → 聚焦事实 → 完成动作”的行动工作区。
- 桌面首屏使用紧凑图标轨道、工作列表、证据区域和属性检查器；不放架构说明、大标题或无消费者的解释卡。
- 手机只保留一个当前工作区。任务详情通过“动态／计划／文件／设置”横向分段切换；审批、提问和设备详情进入内容自适应底部操作层，不把桌面模块纵向堆叠。
- 默认深色 Graphite 世界：安静表面、细描边、系统无衬线正文、等宽运行数据，仅用蓝色强调当前结构，黄色表示需要判断，红色表示风险或断线。
- 风险审批只有一个主动作；普通问题选项保持次级；离线时所有写控件均明确禁用并解释原因。
- Stitch 输出只作为设计参考。生产代码是手写 Vue 组件，没有复制生成 HTML，也没有引入平行设计系统。

## 实现时修正

- 手机任务列表把状态、队列、结果和更新时间压进同一条横向状态带，避免四行元数据占满屏幕。
- 1024px 任务页把下一轮队列收敛为单行停靠编辑器；390px 下固定在底部主导航上方。
- 待处理、提问和设备详情操作层使用 dialog 语义；打开后焦点进入操作层，Escape 或关闭后焦点回到触发项。
- Git 手机页使用单文件选择器，只显示一个代码工作区；横向滚动留在代码区域内部，不让页面产生横向溢出。
- 离线提示只说明当前事实和恢复方式，不展示 Bridge/Relay 架构解释。
