//! local_store 错误类型。

/// 存储层错误。文本不携带正文内容。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// 同一 request_id 携带不同 payload digest(§15.2 DUPLICATE_REQUEST_MISMATCH)。
    #[error("duplicate request mismatch: {request_id}")]
    DuplicateRequestMismatch { request_id: String },
    /// 该 session 已有队列项且未允许替换(§15.3 每 session 最多一条)。
    #[error("queue already exists for session")]
    QueueAlreadyExists,
}

/// 临时上传目录清理记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadCleanup {
    pub id: i64,
    /// 相对 data_dir 的目录名(不含绝对用户路径,§25.3)。
    pub directory: String,
    pub reason: String,
    pub cleaned_at: String,
}
