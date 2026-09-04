//! keychain 集成测试:trait 语义、无凭据泄漏(§19)。
//!
//! 真实 macOS Keychain 不在自动测试中写入(避免污染用户钥匙串);
//! `MacKeychainStore` 的行为由 keyring crate 平台实现保证,这里验证 fake 后端
//! 与 Debug/错误输出的隐私约定。

use bridge::keychain::{InMemoryKeychainStore, KeychainError, KeychainStore};

const SECRET: &str = "pairing-device-credential-SECRET";
const ACCOUNT: &str = "device-test-1";

#[tokio::test]
async fn fake_backend_set_get_delete_roundtrip() {
    let store = InMemoryKeychainStore::new();

    // 不存在 → Ok(None)。
    assert!(store
        .get_device_credential(ACCOUNT)
        .await
        .unwrap()
        .is_none());

    store.set_device_credential(ACCOUNT, SECRET).await.unwrap();
    assert_eq!(
        store
            .get_device_credential(ACCOUNT)
            .await
            .unwrap()
            .as_deref(),
        Some(SECRET)
    );

    // 覆盖写。
    store
        .set_device_credential(ACCOUNT, "rotated")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_device_credential(ACCOUNT)
            .await
            .unwrap()
            .as_deref(),
        Some("rotated")
    );

    // 删除幂等。
    store.delete_device_credential(ACCOUNT).await.unwrap();
    assert!(store
        .get_device_credential(ACCOUNT)
        .await
        .unwrap()
        .is_none());
    store.delete_device_credential(ACCOUNT).await.unwrap();
}

#[tokio::test]
async fn debug_output_never_contains_secret_or_account() {
    // §19/§25.3:日志、错误、Debug 中不得出现凭据。
    let store = InMemoryKeychainStore::new();
    store.set_device_credential(ACCOUNT, SECRET).await.unwrap();

    let debug = format!("{store:?}");
    assert!(!debug.contains(SECRET), "store Debug leaked secret");
    assert!(!debug.contains(ACCOUNT), "store Debug leaked account");

    let unavailable = KeychainError::Unavailable("platform failure".to_owned());
    assert!(!format!("{unavailable:?}").contains(SECRET));
    let access = KeychainError::Access("denied".to_owned());
    assert!(!format!("{access:?}").contains(SECRET));
}

#[tokio::test]
async fn keychain_error_variants_express_no_fallback() {
    // §19 契约:Keychain 不可用是错误,上层进入未绑定态,不回退明文文件。
    let err = KeychainError::Unavailable("service not reachable".to_owned());
    assert!(err.to_string().contains("unavailable"));
    let access = KeychainError::Access("denied".to_owned());
    assert!(access.to_string().contains("access failed"));
}
