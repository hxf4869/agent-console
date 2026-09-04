//! 能力集合(§5/§12/§16.1/§22.3):Adapter capability probe 的产出。
//!
//! 未知版本默认 DEGRADED + READ_ONLY;写能力只对已验证版本且对应 probe
//! 通过的操作开放。设置选项列表动态读取,不得硬编码(§16.1)。

use super::states::{CompatibilityState, ControlMode};
use super::text::OutputText;
use serde::{Deserialize, Serialize};

/// 统一写操作集合(§27.4 WebSocket 命令的领域形态)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// 新建 Desktop 任务(§12:当前版本 IPC 无此方法,capability unsupported)。
    CreateSession,
    StartTurn,
    /// 单条下一轮队列的设置/替换/取消(§15.3;Bridge 本地管理,发送时走 StartTurn)。
    QueueNextTurn,
    SteerTurn,
    InterruptTurn,
    AnswerQuestion,
    SubmitApproval,
    UpdateSettings,
    RenameSession,
    ArchiveSession,
    UnarchiveSession,
    ForkSession,
    StopBackgroundCommand,
    StopAllBackgroundCommands,
}

/// 设置类别(§16.1 动态能力)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingKind {
    Model,
    ReasoningEffort,
    ServiceTier,
    PermissionMode,
    CollaborationMode,
}

impl SettingKind {
    /// Desktop `thread-follower-update-thread-settings.threadSettings` 的原生键。
    pub fn native_key(self) -> &'static str {
        match self {
            SettingKind::Model => "model",
            SettingKind::ReasoningEffort => "effort",
            SettingKind::ServiceTier => "serviceTier",
            SettingKind::PermissionMode => "approvalPolicy",
            SettingKind::CollaborationMode => "collaborationMode",
        }
    }
}

/// 设置单个可选值(原生值标识 + 展示标签)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SettingValue {
    pub value: String,
    pub label: OutputText,
}

/// 会话级可变更设置选项(§16.1):option ID 与可选值动态来自 Desktop,
/// 绝不在协议或前端硬编码具体列表。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SettingOption {
    pub option_id: String,
    pub kind: SettingKind,
    pub label: OutputText,
    /// 当前生效值(原生值标识)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_value: Option<String>,
    /// 当前可选值列表;Desktop 未提供时为空(只显示当前值)。
    #[serde(default)]
    pub available_values: Vec<SettingValue>,
    /// 当前是否可变更(如运行中锁定)。
    pub mutable: bool,
}

/// 设置更新(命令载荷)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SettingUpdate {
    pub kind: SettingKind,
    /// 原生值标识;必须来自动态选项,组合不支持时上层返回
    /// SETTING_COMBINATION_UNSUPPORTED(§16.2)。
    pub value: String,
}

/// 文件预览/下载/上传限制(§22.3 集中定义并在能力响应中暴露)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TransferLimits {
    /// 文本/源码内联预览上限,默认 2 MiB。
    pub text_inline_max_bytes: u64,
    /// 图片内联预览上限,默认 25 MiB。
    pub image_inline_max_bytes: u64,
    /// PDF(单 Range)预览文件上限,默认 100 MiB。
    pub pdf_range_max_bytes: u64,
    /// 普通下载上限,默认 512 MiB。
    pub download_max_bytes: u64,
    /// 单文件上传上限,默认 20 MiB(§22.5)。
    pub upload_max_bytes: u64,
    /// 每 Browser 并发 transfer 上限,默认 2(§22.6)。
    pub max_concurrent_per_browser: u32,
    /// 每 device 并发 transfer 上限,默认 2(§22.6)。
    pub max_concurrent_per_device: u32,
    /// 可直接预览的 MIME 前缀允许列表;HTML/SVG 按源码文本返回。
    pub previewable_mime_prefixes: Vec<String>,
}

impl Default for TransferLimits {
    fn default() -> Self {
        const MIB: u64 = 1024 * 1024;
        Self {
            text_inline_max_bytes: 2 * MIB,
            image_inline_max_bytes: 25 * MIB,
            pdf_range_max_bytes: 100 * MIB,
            download_max_bytes: 512 * MIB,
            upload_max_bytes: 20 * MIB,
            max_concurrent_per_browser: 2,
            max_concurrent_per_device: 2,
            previewable_mime_prefixes: vec![
                "text/".to_string(),
                "application/json".to_string(),
                "application/xml".to_string(),
                "application/x-yaml".to_string(),
                "image/png".to_string(),
                "image/jpeg".to_string(),
                "image/webp".to_string(),
                "image/gif".to_string(),
                "application/pdf".to_string(),
            ],
        }
    }
}

/// 能力集合:probe 结果的完整表达(Envelope 的 CapabilitySnapshot 来源)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CapabilitySet {
    pub control_mode: ControlMode,
    pub compatibility_state: CompatibilityState,
    /// 探测到的 Desktop/CLI 版本;仅作显示与兼容判断(§5)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_version: Option<String>,
    /// 当前 probe 已验证可用的写操作集合。
    pub supported_operations: Vec<Operation>,
    /// 会话级可变更设置(§16.1,动态)。
    pub settings: Vec<SettingOption>,
    pub transfer_limits: TransferLimits,
}

impl CapabilitySet {
    /// 未知版本/不可用 Desktop 的默认能力:DEGRADED + READ_ONLY(§5)。
    pub fn read_only_degraded(codex_version: Option<String>) -> Self {
        Self {
            control_mode: ControlMode::ReadOnly,
            compatibility_state: CompatibilityState::Degraded,
            codex_version,
            supported_operations: Vec::new(),
            settings: Vec::new(),
            transfer_limits: TransferLimits::default(),
        }
    }

    /// Desktop 完全不可达。
    pub fn unavailable() -> Self {
        Self {
            control_mode: ControlMode::Unavailable,
            compatibility_state: CompatibilityState::Unsupported,
            codex_version: None,
            supported_operations: Vec::new(),
            settings: Vec::new(),
            transfer_limits: TransferLimits::default(),
        }
    }

    pub fn supports(&self, op: Operation) -> bool {
        self.supported_operations.contains(&op)
    }
}
