//! 会话设置(权威规格 §16.1/§16.2)。
//!
//! - 选项 ID 与可选值动态来自 Adapter 能力快照,不硬编码(§16.1)。
//! - 写能力收紧(命令层防御):仅开放 [`SettingKind::ReasoningEffort`]
//!   (唯一逐项真机验证过的设置);其余 kind 一律
//!   `SETTING_COMBINATION_UNSUPPORTED`(未逐项真机验证/产品未开放)。
//! - value 必须存在于 Desktop 动态返回的 availableValues 中;可选值列表为空
//!   时同样拒绝(0.153.1 真机 snapshot 不携带任何动态可选值 → 设置写整体
//!   关闭),绝不跳过值域校验放行任意字符串。
//! - 组合不支持(类别未提供/值不在原生可选值列表/当前锁定)→
//!   `SETTING_COMBINATION_UNSUPPORTED`,不静默降级(§16.2)。
//! - 生效范围:v1 原生 `update-thread-settings` 的变更作用于下一次 turn;
//!   返回 [`SettingEffective::NextTurn`] 明确标记,不声称即时生效。
//! - 默认权限档位 = Codex 原生"帮我批准"(approvalPolicy 原生值透传,
//!   新会话从 Bridge 读到的全局默认继承);本模块只透传原生值,绝不实现
//!   外部无条件自动批准。
//! - 设置写入仍走 request_id 去重、expected revision 与 capability gate
//!   (经 [`CommandGateway::submit`])。

use super::gateway::{CommandGateway, Submission};
use crate::domain::{
    BridgeError, CommandPayload, CommandRequest, Operation, SessionKey, SettingKind, SettingOption,
    SettingUpdate, StableErrorCode,
};

/// 设置生效范围(§16.2)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingEffective {
    /// 作用于下一次 turn(v1 语义;原生支持即时变化时也必须明确范围)。
    NextTurn,
    /// 原生即时生效(v1 不产生)。
    Immediate,
}

/// 设置更新结果:提交回执 + 生效范围标记。
#[derive(Debug)]
pub struct SettingsSubmission {
    pub submission: Submission,
    pub effective: SettingEffective,
}

impl CommandGateway {
    /// 更新会话设置(作用于下一次 turn,§16.2)。
    pub async fn update_settings(
        &self,
        key: &SessionKey,
        updates: Vec<SettingUpdate>,
    ) -> Result<SettingsSubmission, BridgeError> {
        // 组合校验基于运行快照中的动态设置选项(含动态可选值与锁定态)。
        let snapshot = self
            .adapter()
            .runtime_snapshot(key)
            .await
            .map_err(adapter_err)?;
        validate_setting_updates(&snapshot.capabilities.settings, &updates)?;

        let request = CommandRequest {
            request_id: uuid::Uuid::new_v4(),
            operation: Operation::UpdateSettings,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: CommandPayload::UpdateSettings { values: updates },
        };
        let submission = self.submit(request).await?;
        Ok(SettingsSubmission {
            submission,
            // v1:变更作用于下一次 turn(RUNNING 中提交同样是 next_turn 语义)。
            effective: SettingEffective::NextTurn,
        })
    }
}

/// 逐 update 的设置组合校验(命令层防御,纯函数)。
///
/// 校验顺序(任一失败即 `SETTING_COMBINATION_UNSUPPORTED`):
/// 1. kind 白名单:仅 `ReasoningEffort`(唯一逐项真机验证的设置),
///    其余 kind 产品未开放写入;
/// 2. option 存在:Desktop 当前确实提供该设置类别;
/// 3. 动态可选值非空且包含目标值:availableValues 为空表示 Desktop 未提供
///    动态选项(0.153.1 真机形状),此时设置写整体关闭,绝不跳过值域校验;
/// 4. mutable:当前 turn 运行中锁定时拒绝。
fn validate_setting_updates(
    options: &[SettingOption],
    updates: &[SettingUpdate],
) -> Result<(), BridgeError> {
    for update in updates {
        if update.kind != SettingKind::ReasoningEffort {
            return Err(BridgeError::new(
                StableErrorCode::SettingCombinationUnsupported,
                format!(
                    "setting kind {:?} is not opened for writing: only reasoning effort is verified against a real desktop",
                    update.kind
                ),
            ));
        }
        let Some(option) = options.iter().find(|o| o.kind == update.kind) else {
            return Err(BridgeError::new(
                StableErrorCode::SettingCombinationUnsupported,
                format!(
                    "setting kind {:?} is not offered by the current desktop",
                    update.kind
                ),
            ));
        };
        if option.available_values.is_empty() {
            return Err(BridgeError::new(
                StableErrorCode::SettingCombinationUnsupported,
                format!(
                    "desktop provides no dynamic available values for {:?}; settings write is closed",
                    update.kind
                ),
            ));
        }
        if !option
            .available_values
            .iter()
            .any(|v| v.value == update.value)
        {
            return Err(BridgeError::new(
                StableErrorCode::SettingCombinationUnsupported,
                format!(
                    "value {:?} is not an available value for {:?}",
                    update.value, update.kind
                ),
            ));
        }
        if !option.mutable {
            return Err(BridgeError::new(
                StableErrorCode::SettingCombinationUnsupported,
                format!("setting {:?} is locked for the running turn", update.kind),
            ));
        }
    }
    Ok(())
}

fn adapter_err(err: crate::adapter::codex::AdapterError) -> BridgeError {
    BridgeError::new(err.code(), err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{OutputText, SettingValue};

    /// 构造纯函数校验用的 option(fixture 数据,不进入生产路径)。
    fn option(kind: SettingKind, values: &[&str], mutable: bool) -> SettingOption {
        SettingOption {
            option_id: kind.native_key().to_string(),
            kind,
            label: OutputText::new(kind.native_key()),
            current_value: None,
            available_values: values
                .iter()
                .map(|v| SettingValue {
                    value: (*v).to_string(),
                    label: OutputText::new(*v),
                })
                .collect(),
            mutable,
        }
    }

    fn update(kind: SettingKind, value: &str) -> SettingUpdate {
        SettingUpdate {
            kind,
            value: value.to_string(),
        }
    }

    #[test]
    fn effort_inside_dynamic_values_is_accepted() {
        let options = [option(SettingKind::ReasoningEffort, &["high", "max"], true)];
        assert_eq!(
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "high")]),
            Ok(())
        );
    }

    #[test]
    fn empty_available_values_rejects_even_known_values() {
        // 真机 0.153.1 形状:Desktop 未提供动态可选值 → 拒绝(不再跳过校验)。
        let options = [option(SettingKind::ReasoningEffort, &[], true)];
        let err =
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "minimal")]);
        assert_eq!(
            err.unwrap_err().code,
            StableErrorCode::SettingCombinationUnsupported
        );
    }

    #[test]
    fn value_outside_dynamic_values_rejects() {
        // minimal 只有真实出现在 Desktop 动态选项中才允许,不接受任意字符串。
        let options = [option(SettingKind::ReasoningEffort, &["high", "max"], true)];
        let err =
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "minimal")]);
        assert_eq!(
            err.unwrap_err().code,
            StableErrorCode::SettingCombinationUnsupported
        );
    }

    #[test]
    fn minimal_accepted_only_when_dynamically_offered() {
        let options = [option(
            SettingKind::ReasoningEffort,
            &["minimal", "low", "high"],
            true,
        )];
        assert_eq!(
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "minimal")]),
            Ok(())
        );
    }

    #[test]
    fn non_effort_kinds_are_rejected() {
        // kind 白名单:即使快照提供了非空动态可选值也一律拒绝。
        let options = [
            option(SettingKind::Model, &["m-a", "m-b"], true),
            option(SettingKind::ReasoningEffort, &["high"], true),
            option(SettingKind::ServiceTier, &["default"], true),
            option(SettingKind::PermissionMode, &["untrusted"], true),
            option(SettingKind::CollaborationMode, &["mode-a"], true),
        ];
        for kind in [
            SettingKind::Model,
            SettingKind::ServiceTier,
            SettingKind::PermissionMode,
            SettingKind::CollaborationMode,
        ] {
            let err = validate_setting_updates(&options, &[update(kind, "x")]);
            assert_eq!(
                err.unwrap_err().code,
                StableErrorCode::SettingCombinationUnsupported,
                "kind {kind:?} 应被白名单拒绝"
            );
        }
    }

    #[test]
    fn missing_option_rejects() {
        let options = [option(SettingKind::Model, &["m-a"], true)];
        let err =
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "high")]);
        assert_eq!(
            err.unwrap_err().code,
            StableErrorCode::SettingCombinationUnsupported
        );
    }

    #[test]
    fn locked_option_rejects() {
        let options = [option(
            SettingKind::ReasoningEffort,
            &["high", "max"],
            false,
        )];
        let err =
            validate_setting_updates(&options, &[update(SettingKind::ReasoningEffort, "high")]);
        assert_eq!(
            err.unwrap_err().code,
            StableErrorCode::SettingCombinationUnsupported
        );
    }
}
