//! 后台命令(权威规格 §23.2)。
//!
//! - 列表来自 runtime snapshot(状态/最近输出引用);无法可靠识别单项时
//!   snapshot 只有计数,不扫描系统进程猜测归属。
//! - 单项停止:仅当能力集合声明 `StopBackgroundCommand`(稳定 command ID
//!   必须可用)时放行;v1 Desktop/CLI 无已验证的原生停止方法,probe 不会
//!   开启该能力 → `CAPABILITY_UNSUPPORTED`。
//! - 只有全局 clean 能力(声明 `StopAllBackgroundCommands` 而无单项)时,
//!   停止全部的响应带 warning code [`BACKGROUND_STOP_GLOBAL_ONLY`](领域内
//!   登记的 warning 常量,不属于 §27.6 稳定错误码)。
//! - 不提供 stdin、交互终端、任意 PID kill 或用户提交的系统命令。

use super::gateway::{CommandGateway, Submission};
use crate::domain::{
    BackgroundCommand, BridgeError, CommandPayload, CommandRequest, Operation, SessionKey,
};

/// 全局停止的 warning code(§23.2:响应契约中携带;登记于实现报告)。
pub const BACKGROUND_STOP_GLOBAL_ONLY: &str = "BACKGROUND_STOP_GLOBAL_ONLY";

/// 后台停止操作结果:提交回执 + warning 列表。
#[derive(Debug)]
pub struct BackgroundStopOutcome {
    pub submission: Submission,
    pub warnings: Vec<&'static str>,
}
impl CommandGateway {
    /// 当前后台命令列表(§23.2 展示字段来自 snapshot)。
    pub async fn background_commands(
        &self,
        key: &SessionKey,
    ) -> Result<Vec<BackgroundCommand>, BridgeError> {
        let snapshot = self
            .adapter()
            .runtime_snapshot(key)
            .await
            .map_err(adapter_err)?;
        Ok(snapshot.background_commands)
    }

    /// 停止单个后台命令(仅当能力声明单项停止且 command ID 稳定)。
    pub async fn stop_background_command(
        &self,
        key: &SessionKey,
        command_id: &str,
    ) -> Result<BackgroundStopOutcome, BridgeError> {
        // capability gate 在 submit 内执行(无能力 → CAPABILITY_UNSUPPORTED)。
        let request = CommandRequest {
            request_id: uuid::Uuid::new_v4(),
            operation: Operation::StopBackgroundCommand,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: CommandPayload::StopBackgroundCommand {
                command_id: command_id.to_string(),
            },
        };
        let submission = self.submit(request).await?;
        Ok(BackgroundStopOutcome {
            submission,
            warnings: Vec::new(),
        })
    }

    /// 停止全部后台命令(全局 clean 能力;响应带 warning code)。
    pub async fn stop_all_background_commands(
        &self,
        key: &SessionKey,
    ) -> Result<BackgroundStopOutcome, BridgeError> {
        let mut warnings = Vec::new();
        if !self
            .current_capabilities()
            .supports(Operation::StopBackgroundCommand)
        {
            // 只暴露全局停止:响应契约带 warning(§23.2)。
            warnings.push(BACKGROUND_STOP_GLOBAL_ONLY);
        }
        let request = CommandRequest {
            request_id: uuid::Uuid::new_v4(),
            operation: Operation::StopAllBackgroundCommands,
            session_key: key.clone(),
            expected_turn_id: None,
            expected_runtime_revision: None,
            payload_digest: None,
            payload: CommandPayload::StopAllBackgroundCommands,
        };
        let submission = self.submit(request).await?;
        Ok(BackgroundStopOutcome {
            submission,
            warnings,
        })
    }
}

fn adapter_err(err: crate::adapter::codex::AdapterError) -> BridgeError {
    BridgeError::new(err.code(), err.to_string())
}
