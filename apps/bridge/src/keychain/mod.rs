//! 设备凭据存储:macOS Keychain(权威规格 §19)。
//!
//! - 设备明文凭据只能存 macOS Keychain;环境变量、CLI 参数、SQLite、日志和
//!   crash report 中不得出现凭据。
//! - Keychain 不可用时上层进入未绑定/不可控制状态,**绝不回退明文文件**。
//!   该约束由 [`KeychainError::Unavailable`] 表达:调用方收到错误后必须停在
//!   未绑定态,不得另寻存储。
//!
//! service 固定为 [`KEYCHAIN_SERVICE`],account 为 device_id。

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

/// Keychain service 名。
pub const KEYCHAIN_SERVICE: &str = "agent-console";

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

/// Keychain 错误。错误文本只描述原因,绝不携带凭据内容。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeychainError {
    /// Keychain/安全服务不可用(平台错误)。上层必须停留在未绑定态。
    #[error("keychain unavailable: {0}")]
    Unavailable(String),
    /// 凭据读写被拒绝或失败(权限/过期等)。
    #[error("keychain access failed: {0}")]
    Access(String),
}

// ---------------------------------------------------------------------------
// trait
// ---------------------------------------------------------------------------

/// 设备凭据存储抽象。真实实现 [`MacKeychainStore`];测试用
/// [`InMemoryKeychainStore`]。
///
/// 实现约定:
/// - 任何日志/错误/Debug 输出不得包含 secret。
/// - [`KeychainStore::set_device_credential`] 在底层不可用时必须返回错误,
///   不允许静默降级到文件存储(§19)。
#[async_trait]
pub trait KeychainStore: Send + Sync {
    /// 保存/覆盖设备凭据。
    async fn set_device_credential(&self, account: &str, secret: &str)
        -> Result<(), KeychainError>;
    /// 读取设备凭据;不存在时返回 `Ok(None)`。
    async fn get_device_credential(&self, account: &str) -> Result<Option<String>, KeychainError>;
    /// 删除设备凭据;不存在时视为成功(幂等,unpair 需要)。
    async fn delete_device_credential(&self, account: &str) -> Result<(), KeychainError>;
}

// ---------------------------------------------------------------------------
// macOS Keychain 实现(keyring crate)
// ---------------------------------------------------------------------------

/// 真实实现:keyring crate → macOS Keychain。
///
/// keyring 调用是同步阻塞 API,统一放 `spawn_blocking`,不阻塞 runtime。
#[derive(Debug, Default, Clone, Copy)]
pub struct MacKeychainStore;

#[async_trait]
impl KeychainStore for MacKeychainStore {
    async fn set_device_credential(
        &self,
        account: &str,
        secret: &str,
    ) -> Result<(), KeychainError> {
        let account = account.to_owned();
        let secret = secret.to_owned();
        tokio::task::spawn_blocking(move || {
            let entry = keyring_entry(&account)?;
            entry
                .set_password(&secret)
                .map_err(|e| KeychainError::Access(keyring_error_text(&e)))
        })
        .await
        .map_err(|e| KeychainError::Unavailable(format!("join error: {e}")))?
    }

    async fn get_device_credential(&self, account: &str) -> Result<Option<String>, KeychainError> {
        let account = account.to_owned();
        tokio::task::spawn_blocking(move || {
            let entry = keyring_entry(&account)?;
            match entry.get_password() {
                Ok(secret) => Ok(Some(secret)),
                Err(keyring::Error::NoEntry) => Ok(None),
                Err(e) => Err(KeychainError::Access(keyring_error_text(&e))),
            }
        })
        .await
        .map_err(|e| KeychainError::Unavailable(format!("join error: {e}")))?
    }

    async fn delete_device_credential(&self, account: &str) -> Result<(), KeychainError> {
        let account = account.to_owned();
        tokio::task::spawn_blocking(move || {
            let entry = keyring_entry(&account)?;
            match entry.delete_credential() {
                Ok(()) => Ok(()),
                Err(keyring::Error::NoEntry) => Ok(()),
                Err(e) => Err(KeychainError::Access(keyring_error_text(&e))),
            }
        })
        .await
        .map_err(|e| KeychainError::Unavailable(format!("join error: {e}")))?
    }
}

fn keyring_entry(account: &str) -> Result<keyring::Entry, KeychainError> {
    keyring::Entry::new(KEYCHAIN_SERVICE, account)
        .map_err(|e| KeychainError::Unavailable(keyring_error_text(&e)))
}

/// keyring 错误的安全文本。keyring 的错误不携带密码内容,但这里仍统一走
/// Display,避免未来错误类型变化时把敏感数据带出去。
fn keyring_error_text(err: &keyring::Error) -> String {
    err.to_string()
}

// ---------------------------------------------------------------------------
// 内存 fake(测试与 doctor 场景注入)
// ---------------------------------------------------------------------------

/// 进程内 fake 后端。**绝不用于生产**:它就是 §19 禁止的"明文文件"等价物,
/// 只出现在测试与显式注入场景。
#[derive(Default)]
pub struct InMemoryKeychainStore {
    // 用 Mutex 存放,Debug 手工实现,避免 derive 打出 secret 内容。
    secrets: Mutex<HashMap<String, String>>,
}

impl InMemoryKeychainStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for InMemoryKeychainStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 只输出条目数量,不输出 account/secret。
        let count = self.secrets.lock().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("InMemoryKeychainStore")
            .field("entries", &count)
            .finish()
    }
}

#[async_trait]
impl KeychainStore for InMemoryKeychainStore {
    async fn set_device_credential(
        &self,
        account: &str,
        secret: &str,
    ) -> Result<(), KeychainError> {
        self.secrets
            .lock()
            .map_err(|e| KeychainError::Unavailable(e.to_string()))?
            .insert(account.to_owned(), secret.to_owned());
        Ok(())
    }

    async fn get_device_credential(&self, account: &str) -> Result<Option<String>, KeychainError> {
        Ok(self
            .secrets
            .lock()
            .map_err(|e| KeychainError::Unavailable(e.to_string()))?
            .get(account)
            .cloned())
    }

    async fn delete_device_credential(&self, account: &str) -> Result<(), KeychainError> {
        self.secrets
            .lock()
            .map_err(|e| KeychainError::Unavailable(e.to_string()))?
            .remove(account);
        Ok(())
    }
}
