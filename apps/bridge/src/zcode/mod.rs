//! ZCode 桌面 Agent 本机 Hook 接入(04-后续阶段方案 §8.3–§8.9;ZC-01 原型,
//! ZC-02 起以正式 `agent_kind = ZCODE_DESKTOP` 参与路由)。
//!
//! 链路:ZCode 原生 `PermissionRequest` →(stdin 单行 JSON)→ 本机 helper
//! (`bridge zcode-hook` 子命令)→ 本机 Unix socket(用户私有目录 + peer UID
//! 校验)→ Bridge 内存 pending 注册表 →(复用现有出站连接与鉴权)→
//! `PendingAttentionAdded` 事件 + 最小 `SessionSummaryChanged` → 浏览器决定
//! → Bridge 原子决定 → 同一等待中的 helper → stdout 输出 ZCode decision JSON。
//!
//! 边界:
//! - 不新增协议消息;审批/问答经现有 `PendingAttentionAdded/Removed` 事件与
//!   `AnswerApproval`/`AnswerQuestion` 命令表达。
//! - ZCode 会话以 `SessionKey { agent_kind: ZcodeDesktop, native_session_id }`
//!   与同机 Codex 会话隔离(ZC-02;不再依赖 `zcode:` 前缀)。
//! - 无 start/steer/interrupt/输出/队列能力:摘要与 RuntimeSnapshot 的
//!   capability 如实上报(仅 Hook 决定),其余操作由 gateway 明确拒绝。
//! - 原始 toolInput 只在本机内存中用于授权展示摘要,不进日志、不落盘。
//! - helper stdout 仅输出协议 JSON(官方 stdout 上限 32KiB),日志走 stderr。

pub mod contract;
pub mod helper;
pub mod link;
pub mod mcp;
pub mod observe;
pub mod pending;
pub mod server;

pub use contract::{HookInvoke, HookReply, NativeHookInput};
pub use link::ZcodeHooks;
pub use pending::{PendingRegistry, PendingState};
pub use server::{serve, HookServerConfig, ServerError};
