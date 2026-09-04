//! local_store 集成测试:迁移、权限、queue 单条约束、回执幂等(§19/§15)。

use std::os::unix::fs::PermissionsExt;

use bridge::local_store::{
    payload_digest, BindingStatus, LocalStore, NextTurnEntry, QueueStatus, ReceiptUpsertOutcome,
    RequestReceipt, SessionKeyRef, StoreError,
};

async fn open_temp_store() -> (tempfile::TempDir, LocalStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalStore::open(dir.path()).await.expect("open store");
    (dir, store)
}

fn session(id: &str) -> SessionKeyRef {
    SessionKeyRef {
        device_id: "device-1".to_owned(),
        agent_kind: 1,
        native_session_id: id.to_owned(),
    }
}

fn receipt(request_id: &str, digest: &str) -> RequestReceipt {
    RequestReceipt {
        request_id: request_id.to_owned(),
        session: session("native-1"),
        operation: "start_turn".to_owned(),
        status: "ACCEPTED_BY_BRIDGE".to_owned(),
        payload_digest: digest.to_owned(),
        created_at: "2026-01-01T00:00:00+00:00".to_owned(),
        updated_at: "2026-01-01T00:00:00+00:00".to_owned(),
    }
}

fn queue_entry(prompt: &str, after_turn: &str) -> NextTurnEntry {
    NextTurnEntry {
        session: session("native-1"),
        prompt: prompt.to_owned(),
        after_turn_id: after_turn.to_owned(),
        runtime_revision: 3,
        status: QueueStatus::Queued,
        created_at: "2026-01-01T00:00:00+00:00".to_owned(),
        updated_at: "2026-01-01T00:00:00+00:00".to_owned(),
    }
}

#[tokio::test]
async fn empty_dir_migrates_and_sets_permissions() {
    let (dir, store) = open_temp_store().await;

    // 全部 §19 表可用:每张表经公开 API 各做一次最小读写(迁移成功的最小证明)。
    store.get_binding().await.expect("binding");
    store
        .authorize_workspace(std::path::Path::new("/tmp/ws"), "ws")
        .await
        .expect("authorized_workspaces");
    store
        .record_receipt(&receipt("req-smoke", "digest"))
        .await
        .expect("request_receipts");
    store
        .set_next_turn(&queue_entry("p", "turn"), false)
        .await
        .expect("next_turn_queue");
    store
        .put_capability("{}", 1)
        .await
        .expect("capability_cache");
    store.put_cursor("k", "v").await.expect("cursors");
    store
        .set_privacy("k", true)
        .await
        .expect("privacy_settings");
    store
        .record_upload_cleanup("uploads/t", "test")
        .await
        .expect("upload_cleanup_log");

    // 库文件 0600、data_dir 与 uploads 0700。
    let db = dir.path().join("bridge.sqlite3");
    let mode = std::fs::metadata(&db).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "sqlite file must be 0600");
    let dir_mode = std::fs::metadata(dir.path()).unwrap().permissions().mode();
    assert_eq!(dir_mode & 0o777, 0o700);
    let uploads = dir.path().join("uploads");
    assert!(uploads.is_dir());
    let uploads_mode = std::fs::metadata(&uploads).unwrap().permissions().mode();
    assert_eq!(uploads_mode & 0o777, 0o700);

    // 重复打开幂等(迁移不重复执行)。
    let reopened = LocalStore::open(dir.path()).await;
    assert!(reopened.is_ok());
}

#[tokio::test]
async fn binding_roundtrip_and_clear() {
    let (_dir, store) = open_temp_store().await;
    let binding = store.get_binding().await.unwrap();
    assert_eq!(binding.status, BindingStatus::Unbound);
    assert!(binding.device_id.is_empty());

    store
        .set_binding("wss://relay.example.com", "device-9", BindingStatus::Paired)
        .await
        .unwrap();
    let binding = store.get_binding().await.unwrap();
    assert_eq!(binding.device_id, "device-9");
    assert_eq!(binding.status, BindingStatus::Paired);

    store.clear_binding().await.unwrap();
    let binding = store.get_binding().await.unwrap();
    assert_eq!(binding.status, BindingStatus::Unbound);
    assert!(binding.device_id.is_empty());
    // relay_url 保留供再次配对展示。
    assert_eq!(binding.relay_url, "wss://relay.example.com");
}

#[tokio::test]
async fn queue_is_single_row_per_session() {
    let (_dir, store) = open_temp_store().await;

    store
        .set_next_turn(&queue_entry("第一条", "turn-1"), false)
        .await
        .unwrap();
    let got = store
        .get_next_turn(&session("native-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.prompt, "第一条");
    assert_eq!(got.after_turn_id, "turn-1");

    // 同 session 第二条:不允许替换 → QUEUE_ALREADY_EXISTS 语义。
    let err = store
        .set_next_turn(&queue_entry("第二条", "turn-2"), false)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::QueueAlreadyExists));

    // 允许替换 → 覆盖。
    store
        .set_next_turn(&queue_entry("第二条", "turn-2"), true)
        .await
        .unwrap();
    let got = store
        .get_next_turn(&session("native-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.prompt, "第二条");
    assert_eq!(got.after_turn_id, "turn-2");

    // 不同 session 各自一条。
    let mut other = queue_entry("其他会话", "turn-1");
    other.session = session("native-2");
    store.set_next_turn(&other, false).await.unwrap();

    // 取消。
    assert!(store.clear_next_turn(&session("native-1")).await.unwrap());
    assert!(store
        .get_next_turn(&session("native-1"))
        .await
        .unwrap()
        .is_none());
    // 再清除返回 false。
    assert!(!store.clear_next_turn(&session("native-1")).await.unwrap());
}

#[tokio::test]
async fn receipt_upsert_is_idempotent_and_detects_digest_mismatch() {
    let (_dir, store) = open_temp_store().await;

    let digest = payload_digest(b"payload-a");
    let outcome = store
        .record_receipt(&receipt("req-1", &digest))
        .await
        .unwrap();
    assert_eq!(outcome, ReceiptUpsertOutcome::Inserted);

    // 同 ID 同 digest:返回既有回执语义。
    let outcome = store
        .record_receipt(&receipt("req-1", &digest))
        .await
        .unwrap();
    assert_eq!(outcome, ReceiptUpsertOutcome::Existing);

    // 同 ID 不同 digest:DUPLICATE_REQUEST_MISMATCH。
    let other_digest = payload_digest(b"payload-b");
    let err = store
        .record_receipt(&receipt("req-1", &other_digest))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::DuplicateRequestMismatch { .. }));

    // 状态推进与读取。
    assert!(store
        .update_receipt_status("req-1", "COMPLETED")
        .await
        .unwrap());
    let got = store.get_receipt("req-1").await.unwrap().unwrap();
    assert_eq!(got.status, "COMPLETED");
    assert_eq!(got.payload_digest, digest);
    assert!(store.get_receipt("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn workspace_capability_cursor_privacy_and_cleanup() {
    let (_dir, store) = open_temp_store().await;

    // 工作区。
    let root = std::path::PathBuf::from("/tmp/ws-alpha");
    store.authorize_workspace(&root, "alpha").await.unwrap();
    store
        .authorize_workspace(&root, "alpha-renamed")
        .await
        .unwrap();
    let list = store.list_workspaces().await.unwrap();
    assert_eq!(list.len(), 1, "upsert 不产生重复行");
    assert_eq!(list[0].display_name, "alpha-renamed");
    assert!(store.revoke_workspace(&root).await.unwrap());
    assert!(!store.revoke_workspace(&root).await.unwrap());

    // capability 缓存。
    store
        .put_capability(r#"{"operations":["start_turn"]}"#, 3)
        .await
        .unwrap();
    store
        .put_capability(r#"{"operations":["start_turn","steer"]}"#, 4)
        .await
        .unwrap();
    let cap = store.get_capability().await.unwrap().unwrap();
    assert_eq!(cap.schema_version, 4);
    assert!(cap.probe_json.contains("steer"));

    // cursor / 隐私。
    store.put_cursor("list-cursor", "abc").await.unwrap();
    assert_eq!(
        store.get_cursor("list-cursor").await.unwrap().as_deref(),
        Some("abc")
    );
    store.set_privacy("hide_task_info", true).await.unwrap();
    assert_eq!(
        store.get_privacy("hide_task_info").await.unwrap(),
        Some(true)
    );

    // 清理记录(只存相对目录名)。
    store
        .record_upload_cleanup("uploads/t-1", "ttl-expired")
        .await
        .unwrap();
    let log = store.list_upload_cleanups().await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].directory, "uploads/t-1");
}
