//! 上传生命周期钩子(权威规格 §22.5:turn 接受、取消、失败或 TTL 到期后
//! 按生命周期清理;不删除用户原始文件)。
//!
//! 通过注入 [`UploadCleaner`] 与 files::upload 的清理入口解耦:生产实现包装
//! `files::upload::{remove_upload_dir, sweep_expired_uploads}`;本模块不直接
//! import 文件路径细节。清理记录写入本地库(upload_cleanup_log;目录名为
//! 相对 data_dir 名,不含正文与绝对路径)。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::adapter::codex::CodexAdapter;
use crate::domain::{DomainEvent, LastTurnOutcome, SessionKey};
use crate::local_store::LocalStore;

/// 与 files::upload::DEFAULT_UPLOAD_TTL 一致的默认清理龄期(1 小时;
/// turn 生命周期结束即清,两者取先)。
pub const DEFAULT_UPLOAD_TTL: Duration = Duration::from_secs(3600);

/// 上传清理入口(§22.5;由 files 工作流提供生产实现,测试注入 fake)。
#[async_trait]
pub trait UploadCleaner: Send + Sync {
    /// 删除一次上传的临时目录(包装 `files::upload::remove_upload_dir`)。
    async fn remove_upload_dir(&self, directory: &str) -> std::io::Result<()>;
    /// 清理早于 `older_than` 的上传目录;返回被删目录的相对名
    /// (包装 `files::upload::sweep_expired_uploads`)。
    async fn sweep_expired(&self, older_than: SystemTime) -> std::io::Result<Vec<String>>;
}

/// turn 终态 → 上传清理的接线器。克隆廉价。
#[derive(Clone)]
pub struct UploadLifecycle {
    cleaner: Arc<dyn UploadCleaner>,
    store: LocalStore,
    ttl: Duration,
}

impl std::fmt::Debug for UploadLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadLifecycle")
            .field("ttl_secs", &self.ttl.as_secs())
            .finish_non_exhaustive()
    }
}

impl UploadLifecycle {
    pub fn new(cleaner: Arc<dyn UploadCleaner>, store: LocalStore) -> Self {
        Self {
            cleaner,
            store,
            ttl: DEFAULT_UPLOAD_TTL,
        }
    }

    pub fn with_ttl(cleaner: Arc<dyn UploadCleaner>, store: LocalStore, ttl: Duration) -> Self {
        Self {
            cleaner,
            store,
            ttl,
        }
    }

    /// turn 终态(accept/cancel/fail)触发的清理:按 TTL 扫描过期上传目录。
    /// 返回被清理的相对目录名(已写入清理记录)。
    pub async fn on_turn_finished(&self, reason: &str) -> Vec<String> {
        let older_than = SystemTime::now()
            .checked_sub(self.ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        match self.cleaner.sweep_expired(older_than).await {
            Ok(removed) => {
                for directory in &removed {
                    let _ = self.store.record_upload_cleanup(directory, reason).await;
                }
                removed
            }
            Err(err) => {
                tracing::warn!(error = %err, "upload sweep after turn finish failed");
                Vec::new()
            }
        }
    }

    /// 显式删除一次上传目录(用户取消场景),成功后写清理记录。
    pub async fn remove_upload(&self, directory: &str, reason: &str) -> std::io::Result<()> {
        self.cleaner.remove_upload_dir(directory).await?;
        let _ = self.store.record_upload_cleanup(directory, reason).await;
        Ok(())
    }

    /// 订阅会话事件:turn 终态(accept/cancel/fail)时执行清理。
    /// 返回时 watcher 已在后台运行;随调用方 runtime 一起结束。
    pub async fn watch_session(
        self: &Arc<Self>,
        adapter: &CodexAdapter,
        key: &SessionKey,
    ) -> Result<(), crate::domain::BridgeError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<DomainEvent>(256);
        adapter
            .subscribe(key, tx)
            .await
            .map_err(|err| crate::domain::BridgeError::new(err.code(), err.to_string()))?;
        let lifecycle = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let DomainEvent::TurnLifecycle {
                    phase: crate::domain::ActiveTurnPhase::Idle,
                    outcome: Some(outcome),
                    ..
                } = event
                {
                    let reason = finished_reason(outcome);
                    lifecycle.on_turn_finished(reason).await;
                }
            }
        });
        Ok(())
    }
}

/// 终态 → 清理原因(进入 upload_cleanup_log.reason;不含会话内容)。
pub fn finished_reason(outcome: LastTurnOutcome) -> &'static str {
    match outcome {
        LastTurnOutcome::Completed => "turn_completed",
        LastTurnOutcome::Failed => "turn_failed",
        LastTurnOutcome::Interrupted => "turn_interrupted",
        LastTurnOutcome::Unknown => "turn_finished",
    }
}
