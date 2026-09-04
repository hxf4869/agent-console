---
name: Agent Console R2
description: "Graphite 发丝分隔、边到边列表与按需操作层构成的行动工作区。"
colors:
  app-bg: "#151716"
  nav-bg: "#191b1a"
  surface-bg: "#1f2220"
  elevated-bg: "#272a28"
  code-bg: "#171918"
  border-subtle: "#343835"
  border-strong: "#4a504c"
  text-primary: "#f1f4f2"
  text-secondary: "#b5bbb7"
  text-muted: "#8a928d"
  accent: "#6e9bff"
  accent-contrast: "#101b31"
  accent-soft: "#243557"
  success: "#58b879"
  warning: "#e0a34a"
  danger: "#e36b63"
  sensitive: "#f07a45"
  focus-ring: "#8db0ff"
  scrim: "rgb(0 0 0 / 64%)"
  light-app-bg: "#f4f6f5"
  light-nav-bg: "#ecefee"
  light-surface-bg: "#ffffff"
  light-elevated-bg: "#f7f9f8"
  light-code-bg: "#f1f3f2"
  light-border-subtle: "#d9dedb"
  light-border-strong: "#b8c0bb"
  light-text-primary: "#1b201d"
  light-text-secondary: "#4e5751"
  light-text-muted: "#626b66"
  light-accent: "#2e65d5"
  light-accent-contrast: "#ffffff"
  light-accent-soft: "#e7eeff"
  light-success: "#247a43"
  light-warning: "#9a5b00"
  light-danger: "#b42318"
  light-sensitive: "#b54708"
  light-focus-ring: "#245fd1"
  light-scrim: "rgb(21 23 22 / 48%)"
typography:
  headline:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    fontSize: "24px"
    fontWeight: 700
    lineHeight: 1.3
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    fontSize: "18px"
    fontWeight: 700
    lineHeight: 1.55
    letterSpacing: "-0.02em"
  body:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.55
  label:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    fontSize: "11px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "0.02em"
  micro:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif"
    fontSize: "10px"
    fontWeight: 700
    lineHeight: 1.2
  code:
    fontFamily: "ui-monospace, 'SFMono-Regular', Menlo, Monaco, Consolas, 'Liberation Mono', monospace"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: 1.65
rounded:
  control: "6px"
  card: "8px"
  dialog: "12px"
  sheet-top: "14px 14px 0 0"
  pill: "999px"
spacing:
  space-1: "4px"
  space-2: "8px"
  space-3: "12px"
  space-4: "16px"
  space-5: "20px"
  space-6: "24px"
  space-8: "32px"
  space-10: "40px"
components:
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.accent-contrast}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
    height: "36px"
  button-secondary:
    backgroundColor: "{colors.elevated-bg}"
    textColor: "{colors.text-primary}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
    height: "36px"
  button-danger:
    textColor: "{colors.danger}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
    height: "36px"
  button-quiet:
    backgroundColor: "transparent"
    textColor: "{colors.text-secondary}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
    height: "36px"
  status-badge:
    backgroundColor: "{colors.surface-bg}"
    textColor: "{colors.text-secondary}"
    rounded: "{rounded.pill}"
    padding: "2px 8px"
    height: "24px"
  icon-rail:
    backgroundColor: "{colors.nav-bg}"
    width: "56px"
    padding: "8px 6px"
  inbox-row:
    backgroundColor: "transparent"
    textColor: "{colors.text-secondary}"
    padding: "8px 10px 8px 12px"
    height: "66px"
  evidence-card:
    backgroundColor: "{colors.elevated-bg}"
    textColor: "{colors.text-primary}"
    rounded: "{rounded.card}"
    padding: "13px"
  queue-composer-compact:
    backgroundColor: "{colors.elevated-bg}"
    padding: "6px 10px"
    height: "56px"
  mobile-task-tabs:
    backgroundColor: "{colors.nav-bg}"
    textColor: "{colors.text-muted}"
    height: "46px"
  mobile-action-sheet:
    backgroundColor: "{colors.elevated-bg}"
    textColor: "{colors.text-primary}"
    rounded: "{rounded.sheet-top}"
    padding: "0 14px 12px"
---

# Design System: Agent Console R2

## Overview

**Creative North Star: "行动收件箱"**

Agent Console R2 是为扫读、聚焦事实和完成动作设计的行动工作区，不是协议说明书。它以紧凑工具栏、Graphite 发丝分隔和边到边工作列表作为第一层，将证据、属性与动作保持在同一任务语境中。蓝色只标记选择和主动作；复杂度通过分段视图与内容自适应操作层按需出现。

这是一套 Operate 模式的界面：桌面第一视口是紧凑图标轨道、工作列表、决策或运行证据和属性检查器；手机只呈现列表或单一工作区，并由固定底部导航与底部操作层承接动作。视觉来源为行动收件箱结构（候选 3/7，seed `f812eac5`），任务详情沿用同一世界中的横向工作区，而不是大标题、说明卡或桌面模块的纵向复刻。

**Key Characteristics:**

- 紧凑、事实优先、可操作。
- Graphite 表面通过发丝边界而非装饰卡片分层。
- 桌面横向协作，手机分段聚焦。
- 蓝色稀缺，状态色只承载真实语义。
- 操作层在需要时出现，关闭后焦点回到触发点。

## Colors

默认深色主题把近黑绿色 Graphite 表面排成 `app → nav → surface → elevated/code` 的安静层次；浅色主题使用 frontmatter 中同名 `light-*` 值逐项替换，语义不变。

### Primary

- **冷静蓝 / `accent`**：只用于当前选择、活动结构、链接与视图中的唯一主动作；`accent-soft` 提供低噪声选中底色，`accent-contrast` 只服务实心主按钮文字。
- **焦点蓝 / `focus-ring`**：专用于键盘可见焦点和输入聚焦，不作为装饰强调。

### Secondary

- **在线绿 / `success`**：在线、已完成动作与新增 Diff；不表示命令或模型结果正确。
- **判断琥珀 / `warning`**：审批、暂停、结果未知和输出缺口。
- **风险红 / `danger`**：风险、离线、失败和危险动作。
- **敏感橙 / `sensitive`**：只标记敏感信息语义，不替代 warning 或 danger。

### Neutral

- **Graphite 表面组**：`app-bg` 是画布，`nav-bg` 承载导航与工具栏，`surface-bg` 承载标题栏和字段，`elevated-bg` 承载需要边界的局部证据，`code-bg` 承载输出和 Diff。
- **Graphite 文字组**：`text-primary` 用于事实与标题，`text-secondary` 用于正文，`text-muted` 用于时间、路径和上下文元数据。
- **Graphite 边界组**：`border-subtle` 是默认 1px 发丝分隔，`border-strong` 只用于字段、队列和操作层上缘；`scrim` 只在模态操作层出现。

**The Sparse Blue Rule.** 蓝色只用于当前选择、结构链接与唯一主动作；任何信息仅凭“重要”不得染蓝。

**The Semantic State Rule.** success、warning、danger 与 sensitive 必须保留各自事实语义，不能拿状态色做页面装饰。

## Typography

**Display Font:** 无；R2 不设置营销式 display 层级。

**Body Font:** Inter 与系统无衬线回退。

**Label/Mono Font:** 状态与控件标签沿用 UI 字体；命令、路径、ID、修订、时间和 Diff 使用系统等宽栈。

**Character:** 字体服务于操作密度：短标题清楚但不喊叫，正文保持 14px 的稳定阅读节奏，技术事实依靠等宽字形和表格数字对齐。字重和大小建立层级，不用超大标题制造“文档首页”。

### Hierarchy

- **Headline**（700，24px，1.3）：只用于确实需要的页面级标题；多数 R2 工作区使用更紧凑的 Title。
- **Title**（700，18px，1.55）：列表页、收件箱和设备页标题。
- **Body**（400，14px，1.55）：说明、问题正文和常规内容。
- **Label**（700，11px，1.2）：按钮、状态徽标、分段导航和列语义。
- **Micro**（700，10px，1.2）：空间受限的眉题、时间和辅助元数据；不得承担长正文。
- **Code**（400，12px，1.65）：命令输出、Diff、文件预览与技术标识。

**The Working-Type Rule.** 页面标题不得升级成 hero；先让列表、证据和动作进入首屏，再用字重而非体量建立层级。

## Layout

桌面以横向工作台组织事实。大于 1100px 时，`AppShell` 在 64px 全局头部下使用 56px 图标轨道；任务与设备路由保留宽度自适应的模块侧栏，工作区把主要内容与检查器并排。待处理视图进一步展开为 326px 收件箱、弹性决策证据区和 286px 属性检查器；任务详情使用弹性运行内容与宽度自适应检查器；Git 使用文件栏、代码区和文件上下文三栏。

- **1250px 及以下**：全局搜索隐藏，避免工具栏拥挤。
- **1120px 及以下**：待处理属性检查器隐藏，保留列表与决策证据两栏。
- **1100px 及以下**：图标轨道隐藏，主导航进入顶栏；模块侧栏隐藏并由导航 dialog 提供，主体保持单一内容列。`QueueComposer` 同时切换为紧凑单行输入，因此 1024px 不出现桌面双行编辑器。
- **900px 及以下**：任务表格转为边到边行，任务名下方的运行状态、下一轮、结果和更新时间仍保持同一条横向状态带；Git 隐藏右侧文件上下文。
- **759px 及以下**：顶部为 56px 紧凑栏，底部固定三项主导航并计入 safe area。待处理和设备首先只显示列表，点选后打开底部 dialog；任务详情只显示“动态 / 计划 / 文件 / 设置”四个分段中的一个；Git 隐藏桌面文件列表，改用工具栏中的单文件选择器并保持单一代码区。
- **599px 及以下**：文本字段使用 16px 字号，核心交互至少 44×44 CSS px；任务页次要文字动作收为图标。

页面级滚动不得横向溢出。长代码、Diff 和输出只在自身区域滚动；移动端通过显式视图切换重新编排信息，而不是把桌面模块从上到下堆叠。

**The One Workspace Rule.** 手机一次只显示一个工作语境；列表、分段视图或底部操作层之间切换，不复制桌面多栏。

## Elevation & Depth

系统平面优先。静态层级主要由 Graphite 色阶与 1px 发丝边界建立，普通列表行和工具栏不投影。只有脱离文档流的导航 dialog、底部操作层和桌面队列编辑器获得阴影；其余“抬升”应先用背景与边界表达。

### Shadow Vocabulary

- **Overlay**（`0 16px 40px rgb(0 0 0 / 22%)`）：桌面或中等宽度导航 dialog。
- **Bottom action layer**（`0 -12px 36px rgb(0 0 0 / 24%)`）：手机待处理、设备与提问操作层。
- **Queue lift**（`0 -10px 32px rgb(0 0 0 / 8%)`）：大于 1100px 的队列编辑器；紧凑单行模式取消阴影。

**The Border Before Shadow Rule.** 静态内容先用一条发丝边界分层；只有覆盖现有内容的操作层才使用明显阴影。

## Shapes

形状克制且工具化：按钮、输入、列表选择与图标底板使用轻微圆角（6px）；证据卡和设置卡使用中等圆角（8px）；独立导航 dialog 使用较大圆角（12px）。状态徽标、通知计数和进度轨道使用完整药丸（999px）。手机底部操作层只圆上方两个角（14px），下沿与固定导航或视口贴合。

边到边列表在窄屏取消外框与卡片圆角，以水平发丝线维持节奏。圆角不用于把每段说明包装成卡片，也不用于削弱表格、Diff 和属性行的工程感。

**The Edge-to-Edge Rule.** 手机工作列表和时间线以连续行呈现；只有真正独立、需要内部边界的证据或设置对象保留卡片轮廓。

## Components

### Buttons

- **Shape:** 轻微圆角（6px），中号最小高度 36px、小号 30px；599px 及以下至少 44px。
- **Primary:** 冷静蓝实心，仅用于当前视图唯一主动作；hover 只做轻微亮度变化。
- **Secondary:** 强边界配 elevated 表面，用于可逆次要动作。
- **Danger:** 风险红的轻量混合边界与底色，用于中断等真实危险语义，不做第二个普通主按钮。
- **Quiet:** 透明底，hover 才进入 accent-soft；用于低强调和工具动作。
- **Focus / Disabled:** 全局 `:focus-visible` 为 2px focus ring 与 2px offset；禁用态使用 muted 文字并保留不可用原因。

### Status Badges

`StatusBadge` 是 24px 高的药丸，只表达 `neutral / accent / success / warning / danger` 状态和可选圆点。普通时间、路径、计数和说明继续作为文本，不把所有元数据药丸化。

### Rows and Evidence Containers

收件箱、任务表与文件列表以整行点击目标和 1px 分隔线构成；选中行使用 accent-soft 和单一蓝色信号。桌面时间线、运行检查器和设置保留 8px 证据卡；手机时间线移除卡片外框，回到边到边行。卡片只容纳自包含事实，不承担说明书式分章节。

### Navigation

大于 1100px 使用 56px 图标轨道，活动项以 accent-soft 和冷静蓝标记；1100px 及以下主导航进入顶栏，模块导航进入原生 dialog；759px 及以下固定三项底部导航。任务详情的手机导航是四项 sticky 分段栏，每次只展示一个工作区。

### Queue Composer

大于 1100px 时，`QueueComposer` 是带状态、双行文本域、快捷键说明和动作组的局部操作面。1100px 及以下使用 56px 紧凑单行：44px 输入、可选 44px 取消与 44px 蓝色提交；1024px 必须保持这一形态，不回退为纵向编辑卡。

### Mobile Action Sheets

待处理决策、设备详情和任务提问在 759px 及以下使用位于固定底部导航之上的内容自适应 dialog。操作层有 scrim、强上边界、仅顶部圆角和 180ms 上移动效；长决策内容限制在 `min(68dvh, 610px)` 并在正文内部滚动，设备与提问按内容高度展开。打开后焦点进入 dialog，Escape、关闭按钮或 scrim 关闭后恢复到原触发项；若触发项消失，则回到所属列表或内容面板。

### Git Workspace

Git 始终只读。桌面保留文件列表、Diff/预览代码区和文件上下文；手机隐藏文件列表与上下文，只在工具栏提供单文件 select，代码区独立横向滚动。绝不把“文件列表在上、代码在下”作为手机布局。

## Do's and Don'ts

### Do:

- **Do** 让用户先扫边到边列表，再聚焦一个事实，最后在同一语境完成一个动作。
- **Do** 在桌面把工作列表、证据与属性横向并置，在手机通过四分段或单一工作区切换。
- **Do** 保持蓝色稀缺，只给选择、活动结构和唯一主动作。
- **Do** 让待处理、设备和提问在手机进入内容自适应底部 dialog，并完整恢复焦点。
- **Do** 在 1024px 使用紧凑单行队列编辑器，在 759px 以下保留固定底部导航和 safe area。
- **Do** 让代码、Diff 与输出在自身区域滚动，并明确展示状态、修订和只读边界。

### Don't:

- **Don't** 把网页做成带大标题、说明卡和章节导语的协议说明书。
- **Don't** 在手机把桌面列表、内容和检查器从上到下纵向堆叠。
- **Don't** 在手机 Git 中同时显示完整文件列表和代码区；使用单文件选择器。
- **Don't** 把卡片当作每一段内容的默认容器，或把所有元数据变成药丸。
- **Don't** 用蓝色装饰普通信息，或混用 success、warning、danger 与 sensitive 的语义。
- **Don't** 引入令牌之外的品牌色、渐变、玻璃拟态、大面积发光或常驻装饰阴影。
- **Don't** 牺牲 44px 移动触控目标、可见焦点、Escape 关闭或焦点恢复来换取密度。
