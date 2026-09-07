//! Capability probe(权威规格 §5、§10.2、§10.3、§16.1)。
//!
//! 输入聚合四路证据:已验证版本表、schema probe 结果、IPC 握手结果、实时
//! 观察结果;输出 [`CapabilitySet`] + ControlMode + CompatibilityState:
//!
//! - 写方法全部可用 → FULL_CONTROL;部分 → LIMITED_CONTROL;只读 → READ_ONLY;
//!   不可用 → UNAVAILABLE(§10.2)。
//! - 版本已验证 → VERIFIED;未知 → DEGRADED;确认不兼容 → UNSUPPORTED(§10.3)。
//! - 未知版本默认 DEGRADED + READ_ONLY;写能力只认版本命中且对应方法 probe
//!   通过(§5)。
//! - 设置选项从 Desktop snapshot/catalog 动态提取(调用方注入),此处不硬编码
//!   任何列表(§16.1);默认权限档位 = Codex 原生"帮我批准"(原生值透传)。
//!
//! 写方法 probe 对真实 Desktop 无法无副作用验证(执行即产生 turn),因此
//! probe 结果由 adapter 以"逐版本写能力矩阵命中([`verified_write_probes`])
//! + 握手 + owner 确认 + (fake 环境)回放验证"方式注入;未 probe 的方法
//! 一律不开启对应写操作。

use crate::adapter::codex::ipc::discovery::normalized_version;
use crate::domain::{
    CapabilitySet, CompatibilityState, ControlMode, Operation, SettingOption, TransferLimits,
};

/// 单个写方法的 probe 结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProbeResult {
    /// 未 probe:一律不开启对应写操作(§5:不得凭字段猜测开启)。
    #[default]
    NotProbed,
    /// probe 通过(版本命中 + 握手/owner 确认 + fake 环境回放验证)。
    Passed,
    /// probe 失败(对端拒绝、版本不匹配等)。
    Failed,
}

/// 各写方法的 probe 结果(方法名与 docs/CODEX-IPC-PROTOCOL.md §7 一致)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteMethodProbes {
    /// thread-follower-start-turn。
    pub start_turn: ProbeResult,
    /// thread-follower-steer-turn。
    pub steer_turn: ProbeResult,
    /// thread-follower-interrupt-turn。
    pub interrupt_turn: ProbeResult,
    /// thread-follower-submit-user-input(回答问题)+ 各审批决策方法。
    pub answer_question: ProbeResult,
    /// thread-follower-update-thread-settings。
    pub update_settings: ProbeResult,
}

impl WriteMethodProbes {
    fn all_passed(&self) -> bool {
        self.start_turn == ProbeResult::Passed
            && self.steer_turn == ProbeResult::Passed
            && self.interrupt_turn == ProbeResult::Passed
            && self.answer_question == ProbeResult::Passed
            && self.update_settings == ProbeResult::Passed
    }

    fn any_passed(&self) -> bool {
        [
            self.start_turn,
            self.steer_turn,
            self.interrupt_turn,
            self.answer_question,
            self.update_settings,
        ]
        .contains(&ProbeResult::Passed)
    }
}

/// 0.153.1 / 0.153.4 真机逐操作写验证白名单
/// (docs/CODEX-COMPATIBILITY.md §9/§10:专用测试会话真机逐操作验收)。
/// `thread-follower-start-turn`、
/// `thread-follower-steer-turn`(steer 需带 restoreMessage{cwd,context};§9/§10)
/// 与 `thread-follower-interrupt-turn` 通过;审批、原生问题回答未自然出现,
/// 保持 `NotProbed`。`thread-follower-update-thread-settings` 方法级
/// effort 已真机验证,但产品因 Desktop 不提供动态可选值列表而关闭设置写能力
/// (方法级 VERIFIED 记录见 §9)。新建/rename/archive/unarchive/fork/后台
/// 命令停止无 IPC 方法(§3 UNSUPPORTED),永不在白名单。
const REAL_DESKTOP_VERIFIED_CORE_OPS: WriteMethodProbes = WriteMethodProbes {
    start_turn: ProbeResult::Passed,
    steer_turn: ProbeResult::Passed,
    interrupt_turn: ProbeResult::Passed,
    answer_question: ProbeResult::NotProbed,
    update_settings: ProbeResult::NotProbed,
};

/// 逐版本写能力矩阵:与协议/只读兼容版本表(`ipc::discovery::VERIFIED_VERSIONS`)
/// 相互独立。0.153.1 与 0.153.4 有真机逐操作写验证证据;0.153.0-alpha.5
/// 只有只读协议兼容证据(CompatibilityState 仍为 Verified),其余版本一律
/// 全 `NotProbed`(§5:不凭版本表命中开启写)。
pub fn verified_write_probes(version_report: Option<&str>) -> WriteMethodProbes {
    match version_report.map(normalized_version) {
        Some("0.153.1") | Some("0.153.4") => REAL_DESKTOP_VERIFIED_CORE_OPS,
        _ => WriteMethodProbes::default(),
    }
}

/// CapabilityProbe 的输入:四路证据 + 动态设置。
#[derive(Debug, Clone, Default)]
pub struct CapabilityProbeInput {
    /// `codex --version` 的输出(ipc::discovery::probe_version;未知时 None)。
    pub version_report: Option<String>,
    /// 版本是否命中 VERIFIED_VERSIONS(经 `ipc::discovery::version_is_verified`)。
    pub version_verified: bool,
    /// 确认不兼容(如 IPC 版本表不匹配被对端拒绝,§10.3)。
    pub confirmed_incompatible: bool,
    /// IPC 握手成功(initialize 拿到 clientId)。
    pub ipc_handshake_ok: bool,
    /// owner 发现成功(写目标 session 的 owner 已确认,§12)。
    pub owner_confirmed: bool,
    pub write_methods: WriteMethodProbes,
    /// catalog schema probe:会话目录可读。
    pub catalog_sessions_listable: bool,
    /// catalog schema probe:历史可读。
    pub catalog_history_readable: bool,
    /// 实时观察:收到过合法 conversationState snapshot。
    pub snapshot_observed: bool,
    /// 从 Desktop snapshot/catalog 动态提取的设置选项(§16.1)。
    pub settings: Vec<SettingOption>,
}

/// 执行 capability probe,产出 [`CapabilitySet`]。
pub fn probe(input: &CapabilityProbeInput) -> CapabilitySet {
    // ---- CompatibilityState(§10.3) ----
    let compatibility_state = if input.confirmed_incompatible {
        CompatibilityState::Unsupported
    } else if input.version_verified {
        CompatibilityState::Verified
    } else {
        CompatibilityState::Degraded
    };

    // ---- 支持的操作(§5:写能力只认版本命中 + probe 通过) ----
    let writes_open = input.version_verified && input.ipc_handshake_ok && input.owner_confirmed;
    let wm = &input.write_methods;
    let mut supported_operations: Vec<Operation> = Vec::new();
    if writes_open && wm.start_turn == ProbeResult::Passed {
        supported_operations.push(Operation::StartTurn);
        // 单条下一轮队列的发送路径是 idle 后的 start-turn(§15.3),因此队列
        // 能力跟随 start-turn 开启;队列正文/状态由 Bridge 本地管理。
        supported_operations.push(Operation::QueueNextTurn);
    }
    if writes_open && wm.steer_turn == ProbeResult::Passed {
        supported_operations.push(Operation::SteerTurn);
    }
    if writes_open && wm.interrupt_turn == ProbeResult::Passed {
        supported_operations.push(Operation::InterruptTurn);
    }
    if writes_open && wm.answer_question == ProbeResult::Passed {
        supported_operations.push(Operation::AnswerQuestion);
        supported_operations.push(Operation::SubmitApproval);
    }
    if writes_open && wm.update_settings == ProbeResult::Passed {
        supported_operations.push(Operation::UpdateSettings);
    }
    // CreateSession / Rename / Archive / Unarchive / Fork / Stop*Background*:
    // 当前版本 IPC 无对应方法(docs/CODEX-COMPATIBILITY.md §3 UNSUPPORTED),
    // probe 永不开启;出现新的已验证方法时在此追加。

    // ---- ControlMode(§10.2) ----
    let any_local_source =
        input.ipc_handshake_ok || input.catalog_sessions_listable || input.catalog_history_readable;
    let control_mode = if !any_local_source {
        ControlMode::Unavailable
    } else if writes_open && wm.all_passed() {
        ControlMode::FullControl
    } else if writes_open && wm.any_passed() {
        ControlMode::LimitedControl
    } else {
        ControlMode::ReadOnly
    };

    CapabilitySet {
        control_mode,
        compatibility_state,
        codex_version: input.version_report.clone(),
        supported_operations,
        settings: dedupe_settings(input.settings.clone()),
        transfer_limits: TransferLimits::default(),
    }
}

/// 设置按 option_id 去重并稳定排序(同一 option 的多个来源以先到者为准)。
fn dedupe_settings(mut settings: Vec<SettingOption>) -> Vec<SettingOption> {
    settings.sort_by(|a, b| a.option_id.cmp(&b.option_id));
    settings.dedup_by(|a, b| a.option_id == b.option_id);
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{OutputText, SettingKind};

    fn verified_input() -> CapabilityProbeInput {
        CapabilityProbeInput {
            version_report: Some("codex-cli 0.153.0-alpha.5".to_string()),
            version_verified: true,
            ipc_handshake_ok: true,
            owner_confirmed: true,
            write_methods: WriteMethodProbes {
                start_turn: ProbeResult::Passed,
                steer_turn: ProbeResult::Passed,
                interrupt_turn: ProbeResult::Passed,
                answer_question: ProbeResult::Passed,
                update_settings: ProbeResult::Passed,
            },
            catalog_sessions_listable: true,
            catalog_history_readable: true,
            snapshot_observed: true,
            settings: vec![],
            confirmed_incompatible: false,
        }
    }

    #[test]
    fn verified_and_all_probes_pass_gives_full_control() {
        let caps = probe(&verified_input());
        assert_eq!(caps.control_mode, ControlMode::FullControl);
        assert_eq!(caps.compatibility_state, CompatibilityState::Verified);
        assert!(caps.supports(Operation::StartTurn));
        assert!(caps.supports(Operation::SteerTurn));
        assert!(caps.supports(Operation::InterruptTurn));
        assert!(caps.supports(Operation::AnswerQuestion));
        assert!(caps.supports(Operation::UpdateSettings));
        // IPC 上不存在的新建/改名/归档/停止能力永不开启。
        assert!(!caps.supports(Operation::CreateSession));
        assert!(!caps.supports(Operation::RenameSession));
        assert!(!caps.supports(Operation::StopAllBackgroundCommands));
    }

    #[test]
    fn unknown_version_defaults_degraded_read_only() {
        let mut input = verified_input();
        input.version_verified = false;
        input.version_report = Some("codex-cli 9.9.9".to_string());
        let caps = probe(&input);
        // §5:未知版本默认 DEGRADED + READ_ONLY,即使 probe 字段全部"通过"。
        assert_eq!(caps.control_mode, ControlMode::ReadOnly);
        assert_eq!(caps.compatibility_state, CompatibilityState::Degraded);
        assert!(caps.supported_operations.is_empty());
        assert_eq!(caps.codex_version.as_deref(), Some("codex-cli 9.9.9"));
    }

    #[test]
    fn partial_probe_failure_gives_limited_control() {
        let mut input = verified_input();
        input.write_methods.steer_turn = ProbeResult::Failed;
        input.write_methods.answer_question = ProbeResult::NotProbed;
        let caps = probe(&input);
        assert_eq!(caps.control_mode, ControlMode::LimitedControl);
        assert!(caps.supports(Operation::StartTurn));
        assert!(!caps.supports(Operation::SteerTurn));
        assert!(!caps.supports(Operation::AnswerQuestion));
    }

    #[test]
    fn all_probes_failed_but_version_verified_is_read_only() {
        let mut input = verified_input();
        input.write_methods = WriteMethodProbes::default();
        let caps = probe(&input);
        assert_eq!(caps.control_mode, ControlMode::ReadOnly);
        assert_eq!(caps.compatibility_state, CompatibilityState::Verified);
        assert!(caps.supported_operations.is_empty());
    }

    #[test]
    fn confirmed_incompatible_marks_unsupported() {
        let mut input = verified_input();
        input.confirmed_incompatible = true;
        input.version_verified = false;
        let caps = probe(&input);
        assert_eq!(caps.compatibility_state, CompatibilityState::Unsupported);
    }

    #[test]
    fn no_sources_at_all_is_unavailable() {
        let input = CapabilityProbeInput::default();
        let caps = probe(&input);
        assert_eq!(caps.control_mode, ControlMode::Unavailable);
        assert_eq!(caps.compatibility_state, CompatibilityState::Degraded);
    }

    #[test]
    fn catalog_only_desktop_offline_is_read_only() {
        let mut input = verified_input();
        input.ipc_handshake_ok = false;
        input.owner_confirmed = false;
        let caps = probe(&input);
        assert_eq!(caps.control_mode, ControlMode::ReadOnly);
        assert!(caps.supported_operations.is_empty());
    }

    #[test]
    fn settings_are_passthrough_and_deduped() {
        let mut input = verified_input();
        input.settings = vec![
            setting("model", SettingKind::Model, Some("gpt-5.3-fixture")),
            setting("model", SettingKind::Model, Some("other")),
            setting("effort", SettingKind::ReasoningEffort, Some("medium")),
        ];
        let caps = probe(&input);
        assert_eq!(caps.settings.len(), 2);
        let model = caps
            .settings
            .iter()
            .find(|s| s.option_id == "model")
            .expect("model option kept");
        assert_eq!(model.current_value.as_deref(), Some("gpt-5.3-fixture"));
    }

    fn setting(id: &str, kind: SettingKind, current: Option<&str>) -> SettingOption {
        SettingOption {
            option_id: id.to_string(),
            kind,
            label: OutputText::new(id),
            current_value: current.map(String::from),
            available_values: vec![],
            mutable: true,
        }
    }

    fn all_not_probed(probes: &WriteMethodProbes) {
        assert_eq!(probes.start_turn, ProbeResult::NotProbed);
        assert_eq!(probes.steer_turn, ProbeResult::NotProbed);
        assert_eq!(probes.interrupt_turn, ProbeResult::NotProbed);
        assert_eq!(probes.answer_question, ProbeResult::NotProbed);
        assert_eq!(probes.update_settings, ProbeResult::NotProbed);
    }

    #[test]
    fn verified_write_probes_alpha5_is_all_not_probed() {
        // 0.153.0-alpha.5 只读协议兼容(fixture 证据),无任何写验证。
        for version in ["0.153.0-alpha.5", "codex-cli 0.153.0-alpha.5"] {
            let probes = verified_write_probes(Some(version));
            all_not_probed(&probes);
        }
    }

    #[test]
    fn verified_write_probes_real_versions_match_real_device_matrix() {
        // 0.153.1/0.153.4:§9/§10 真机逐操作验收;设置写能力产品侧关闭。
        for version in [
            "0.153.1",
            "codex-cli 0.153.1",
            "0.153.4",
            "codex-cli 0.153.4",
        ] {
            let probes = verified_write_probes(Some(version));
            assert_eq!(probes.start_turn, ProbeResult::Passed);
            assert_eq!(probes.steer_turn, ProbeResult::Passed);
            assert_eq!(probes.interrupt_turn, ProbeResult::Passed);
            assert_eq!(probes.answer_question, ProbeResult::NotProbed);
            assert_eq!(probes.update_settings, ProbeResult::NotProbed);
        }
    }

    #[test]
    fn verified_write_probes_unknown_or_missing_is_all_not_probed() {
        for version in [None, Some(""), Some("codex-cli 0.999.0"), Some("0.153.2")] {
            let probes = verified_write_probes(version);
            assert_eq!(probes, WriteMethodProbes::default());
            all_not_probed(&probes);
        }
    }

    #[test]
    fn probe_0_153_1_matrix_gives_limited_control_without_settings_or_answers() {
        let input = CapabilityProbeInput {
            version_report: Some("codex-cli 0.153.1".to_string()),
            version_verified: true,
            ipc_handshake_ok: true,
            owner_confirmed: true,
            write_methods: verified_write_probes(Some("codex-cli 0.153.1")),
            ..Default::default()
        };
        let caps = probe(&input);
        assert_eq!(caps.control_mode, ControlMode::LimitedControl);
        assert_eq!(caps.compatibility_state, CompatibilityState::Verified);
        assert!(caps.supports(Operation::StartTurn));
        assert!(caps.supports(Operation::SteerTurn));
        assert!(caps.supports(Operation::InterruptTurn));
        assert!(!caps.supports(Operation::AnswerQuestion));
        assert!(!caps.supports(Operation::UpdateSettings));
    }

    #[test]
    fn probe_alpha5_matrix_is_verified_read_only() {
        // 只读协议兼容版本:CompatibilityState=Verified,但写操作全关。
        let input = CapabilityProbeInput {
            version_report: Some("codex-cli 0.153.0-alpha.5".to_string()),
            version_verified: true,
            ipc_handshake_ok: true,
            owner_confirmed: true,
            write_methods: verified_write_probes(Some("codex-cli 0.153.0-alpha.5")),
            ..Default::default()
        };
        let caps = probe(&input);
        assert!(caps.supported_operations.is_empty());
        assert_eq!(caps.control_mode, ControlMode::ReadOnly);
        assert_eq!(caps.compatibility_state, CompatibilityState::Verified);
    }
}
