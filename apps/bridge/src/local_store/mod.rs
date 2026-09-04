//! Bridge 本地 SQLite 存储与迁移(权威规格 §19)。
//!
//! - 库文件位于 data_dir,权限 0600;schema 由 Bridge 自己 migration,
//!   **绝不**对 Codex SQLite 运行 migration,也绝不打开 Codex 库。
//! - 只保存 §19 清单内的数据:绑定状态、授权工作区、最近回执与摘要、
//!   单条 next-turn queue 正文(§15.3 唯一例外)、capability probe、
//!   cursor/隐私设置/上传清理记录。不复制会话历史、输出或 Diff。
//! - 不存任何凭据(凭据只在 Keychain)。

use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

pub use error::{StoreError, UploadCleanup};
pub use models::{
    AuthorizedWorkspace, Binding, BindingStatus, CapabilityCacheEntry, NextTurnEntry, QueueStatus,
    ReceiptUpsertOutcome, RequestReceipt, SessionKeyRef,
};
pub use receipt::payload_digest;

mod error;
mod models;
mod receipt;

/// 编译期内嵌迁移(相对 crate 根 apps/bridge/migrations)。
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Bridge 本地库句柄。克隆廉价(内部是连接池)。
#[derive(Debug, Clone)]
pub struct LocalStore {
    pool: SqlitePool,
    #[allow(dead_code)] // 供诊断/上报使用
    db_path: PathBuf,
}

impl LocalStore {
    /// 打开(必要时创建)data_dir 下的本地库并执行迁移。
    ///
    /// - data_dir 权限收紧为 0700;SQLite 主文件与 -wal/-shm 收紧为 0600。
    /// - 同时确保 uploads 临时目录存在且为 0700。
    pub async fn open(data_dir: &Path) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir)?;
        set_dir_mode_700(data_dir)?;
        ensure_uploads_dir(data_dir)?;

        let db_path = data_dir.join("bridge.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        MIGRATOR.run(&pool).await?;

        // 迁移完成后收紧权限(库文件此刻必然存在;wal/shm 可能存在)。
        set_file_mode_600(&db_path)?;
        for sidecar in ["bridge.sqlite3-wal", "bridge.sqlite3-shm"] {
            let p = data_dir.join(sidecar);
            if p.exists() {
                set_file_mode_600(&p)?;
            }
        }

        Ok(Self { pool, db_path })
    }

    /// 仅供测试/诊断:直接访问连接池。业务代码一律走方法。
    #[allow(dead_code)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // -----------------------------------------------------------------------
    // 绑定状态(非敏感)
    // -----------------------------------------------------------------------

    pub async fn get_binding(&self) -> Result<Binding, StoreError> {
        let row = sqlx::query_as::<_, (String, String, String)>(
            "SELECT relay_url, device_id, status FROM binding WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(Binding {
            relay_url: row.0,
            device_id: row.1,
            status: BindingStatus::parse(&row.2),
        })
    }

    /// 写入绑定状态(pair 成功后)。device_id 非空校验由调用方负责。
    pub async fn set_binding(
        &self,
        relay_url: &str,
        device_id: &str,
        status: BindingStatus,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE binding SET relay_url = ?, device_id = ?, status = ?, updated_at = ? \
             WHERE id = 1",
        )
        .bind(relay_url)
        .bind(device_id)
        .bind(status.as_str())
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 清除绑定(unpair):device_id 置空、状态回 UNBOUND;保留 relay_url
    /// 供下次配对展示。
    pub async fn clear_binding(&self) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE binding SET device_id = '', status = 'UNBOUND', updated_at = ? WHERE id = 1",
        )
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 授权工作区
    // -----------------------------------------------------------------------

    /// 授权(或更新显示名)。root 必须已是 canonical path。
    pub async fn authorize_workspace(
        &self,
        root: &Path,
        display_name: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO authorized_workspaces (root_path, display_name, authorized_at) \
             VALUES (?, ?, ?) \
             ON CONFLICT(root_path) DO UPDATE SET display_name = excluded.display_name, \
             authorized_at = excluded.authorized_at",
        )
        .bind(root.to_string_lossy().as_ref())
        .bind(display_name)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_workspaces(&self) -> Result<Vec<AuthorizedWorkspace>, StoreError> {
        let rows = sqlx::query_as::<_, (String, String, String)>(
            "SELECT root_path, display_name, authorized_at FROM authorized_workspaces \
             ORDER BY authorized_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(root_path, display_name, authorized_at)| AuthorizedWorkspace {
                    root_path: PathBuf::from(root_path),
                    display_name,
                    authorized_at,
                },
            )
            .collect())
    }

    /// 撤销授权;返回是否确有删除。
    pub async fn revoke_workspace(&self, root: &Path) -> Result<bool, StoreError> {
        let result = sqlx::query("DELETE FROM authorized_workspaces WHERE root_path = ?")
            .bind(root.to_string_lossy().as_ref())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // -----------------------------------------------------------------------
    // 回执(§15.1/§15.2:幂等 upsert,同 ID 不同 digest 冲突)
    // -----------------------------------------------------------------------

    /// 幂等写入回执:
    /// - 新 request_id → 插入,返回 [`ReceiptUpsertOutcome::Inserted`];
    /// - 同 ID 同 digest → 返回 [`ReceiptUpsertOutcome::Existing`](不重复执行);
    /// - 同 ID 不同 digest → [`StoreError::DuplicateRequestMismatch`]。
    pub async fn record_receipt(
        &self,
        receipt: &RequestReceipt,
    ) -> Result<ReceiptUpsertOutcome, StoreError> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query_as::<_, (String,)>(
            "SELECT payload_digest FROM request_receipts WHERE request_id = ?",
        )
        .bind(&receipt.request_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((existing_digest,)) = existing {
            if existing_digest != receipt.payload_digest {
                return Err(StoreError::DuplicateRequestMismatch {
                    request_id: receipt.request_id.clone(),
                });
            }
            tx.rollback().await.ok();
            return Ok(ReceiptUpsertOutcome::Existing);
        }

        sqlx::query(
            "INSERT INTO request_receipts (request_id, device_id, agent_kind, native_session_id, \
             operation, status, payload_digest, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&receipt.request_id)
        .bind(&receipt.session.device_id)
        .bind(receipt.session.agent_kind)
        .bind(&receipt.session.native_session_id)
        .bind(&receipt.operation)
        .bind(&receipt.status)
        .bind(&receipt.payload_digest)
        .bind(&receipt.created_at)
        .bind(&receipt.updated_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(ReceiptUpsertOutcome::Inserted)
    }

    /// 更新既有回执状态(Bridge 侧状态推进,如 ACCEPTED → COMPLETED)。
    /// request_id 不存在时返回 Ok(false)。
    pub async fn update_receipt_status(
        &self,
        request_id: &str,
        status: &str,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE request_receipts SET status = ?, updated_at = ? WHERE request_id = ?",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn get_receipt(
        &self,
        request_id: &str,
    ) -> Result<Option<RequestReceipt>, StoreError> {
        let row =
            sqlx::query_as::<_, (String, String, i64, String, String, String, String, String)>(
                "SELECT request_id, device_id, agent_kind, native_session_id, operation, status, \
             payload_digest, created_at FROM request_receipts WHERE request_id = ?",
            )
            .bind(request_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(
            |(
                request_id,
                device_id,
                agent_kind,
                native_session_id,
                operation,
                status,
                payload_digest,
                created_at,
            )| {
                let updated_at = created_at.clone();
                RequestReceipt {
                    request_id,
                    session: SessionKeyRef {
                        device_id,
                        agent_kind,
                        native_session_id,
                    },
                    operation,
                    status,
                    payload_digest,
                    created_at,
                    updated_at,
                }
            },
        ))
    }

    // -----------------------------------------------------------------------
    // 下一轮队列(§15.3:每 session 最多一条)
    // -----------------------------------------------------------------------

    /// 写入/替换队列项。`replace = false` 且已存在时返回
    /// [`StoreError::QueueAlreadyExists`]。
    pub async fn set_next_turn(
        &self,
        entry: &NextTurnEntry,
        replace: bool,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_as::<_, (i64,)>(
            "SELECT 1 FROM next_turn_queue WHERE device_id = ? AND agent_kind = ? \
             AND native_session_id = ?",
        )
        .bind(&entry.session.device_id)
        .bind(entry.session.agent_kind)
        .bind(&entry.session.native_session_id)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_some() && !replace {
            return Err(StoreError::QueueAlreadyExists);
        }

        if exists.is_some() {
            sqlx::query(
                "UPDATE next_turn_queue SET prompt = ?, after_turn_id = ?, runtime_revision = ?, \
                 status = ?, updated_at = ? WHERE device_id = ? AND agent_kind = ? \
                 AND native_session_id = ?",
            )
            .bind(&entry.prompt)
            .bind(&entry.after_turn_id)
            .bind(entry.runtime_revision)
            .bind(entry.status.as_str())
            .bind(now_rfc3339())
            .bind(&entry.session.device_id)
            .bind(entry.session.agent_kind)
            .bind(&entry.session.native_session_id)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO next_turn_queue (device_id, agent_kind, native_session_id, prompt, \
                 after_turn_id, runtime_revision, status, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&entry.session.device_id)
            .bind(entry.session.agent_kind)
            .bind(&entry.session.native_session_id)
            .bind(&entry.prompt)
            .bind(&entry.after_turn_id)
            .bind(entry.runtime_revision)
            .bind(entry.status.as_str())
            .bind(&entry.created_at)
            .bind(&entry.updated_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_next_turn(
        &self,
        session: &SessionKeyRef,
    ) -> Result<Option<NextTurnEntry>, StoreError> {
        let row = sqlx::query_as::<_, (String, String, i64, String, String)>(
            "SELECT prompt, after_turn_id, runtime_revision, status, created_at \
             FROM next_turn_queue WHERE device_id = ? AND agent_kind = ? AND native_session_id = ?",
        )
        .bind(&session.device_id)
        .bind(session.agent_kind)
        .bind(&session.native_session_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(prompt, after_turn_id, runtime_revision, status, created_at)| NextTurnEntry {
                session: session.clone(),
                prompt,
                after_turn_id,
                runtime_revision,
                status: QueueStatus::parse(&status),
                updated_at: created_at.clone(),
                created_at,
            },
        ))
    }

    /// 取消/清除队列项;返回是否确有删除。
    pub async fn clear_next_turn(&self, session: &SessionKeyRef) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "DELETE FROM next_turn_queue WHERE device_id = ? AND agent_kind = ? \
             AND native_session_id = ?",
        )
        .bind(&session.device_id)
        .bind(session.agent_kind)
        .bind(&session.native_session_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    // -----------------------------------------------------------------------
    // capability 缓存
    // -----------------------------------------------------------------------

    /// 覆盖写入 probe 结果(JSON:仅 schema/能力,无会话内容)与本地 schema 版本。
    pub async fn put_capability(
        &self,
        probe_json: &str,
        schema_version: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO capability_cache (id, probe_json, schema_version, updated_at) \
             VALUES (1, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE SET probe_json = excluded.probe_json, \
             schema_version = excluded.schema_version, updated_at = excluded.updated_at",
        )
        .bind(probe_json)
        .bind(schema_version)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_capability(&self) -> Result<Option<CapabilityCacheEntry>, StoreError> {
        let row = sqlx::query_as::<_, (String, i64, String)>(
            "SELECT probe_json, schema_version, updated_at FROM capability_cache WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(probe_json, schema_version, updated_at)| CapabilityCacheEntry {
                probe_json,
                schema_version,
                updated_at,
            },
        ))
    }

    // -----------------------------------------------------------------------
    // cursor / 隐私设置 / 上传清理记录
    // -----------------------------------------------------------------------

    pub async fn put_cursor(&self, key: &str, value: &str) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO cursors (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
             updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_cursor(&self, key: &str) -> Result<Option<String>, StoreError> {
        let row = sqlx::query_as::<_, (String,)>("SELECT value FROM cursors WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(v,)| v))
    }

    pub async fn set_privacy(&self, key: &str, enabled: bool) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO privacy_settings (key, enabled, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET enabled = excluded.enabled, \
             updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(i64::from(enabled))
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_privacy(&self, key: &str) -> Result<Option<bool>, StoreError> {
        let row = sqlx::query_as::<_, (i64,)>("SELECT enabled FROM privacy_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(v,)| v != 0))
    }

    /// 记录一次临时上传目录清理。`directory` 必须是相对 data_dir 的目录名,
    /// 不含文件内容,不含绝对用户路径(§25.3)。
    pub async fn record_upload_cleanup(
        &self,
        directory: &str,
        reason: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO upload_cleanup_log (directory, reason, cleaned_at) VALUES (?, ?, ?)",
        )
        .bind(directory)
        .bind(reason)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_upload_cleanups(&self) -> Result<Vec<UploadCleanup>, StoreError> {
        let rows = sqlx::query_as::<_, (i64, String, String, String)>(
            "SELECT id, directory, reason, cleaned_at FROM upload_cleanup_log ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, directory, reason, cleaned_at)| UploadCleanup {
                id,
                directory,
                reason,
                cleaned_at,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// 文件权限与目录
// ---------------------------------------------------------------------------

/// 确保 data_dir/uploads 存在,权限 0700;返回目录路径。
pub fn ensure_uploads_dir(data_dir: &Path) -> Result<PathBuf, StoreError> {
    let dir = data_dir.join(crate::config::UPLOADS_DIR_NAME);
    std::fs::create_dir_all(&dir)?;
    set_dir_mode_700(&dir)?;
    Ok(dir)
}

#[cfg(unix)]
fn set_dir_mode_700(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_dir_mode_700(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

#[cfg(unix)]
fn set_file_mode_600(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_mode_600(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}
