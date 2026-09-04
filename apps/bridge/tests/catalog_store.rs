//! Codex SQLite 只读目录集成测试(§12):fixture 分页、缺列降级、归档过滤、
//! 父子关系、超时;fixture 生成器与 e2e 共用(catalog_fixture)。

use std::time::Duration;

use bridge::adapter::codex::catalog::{
    fixture as catalog_fixture, CatalogConfig, CatalogError, CodexCatalog,
};

const MAIN: &str = "11111111-1111-4111-8111-111111111111";

async fn open_fixture(tag: &str) -> (tempfile::TempDir, CodexCatalog) {
    let dir = tempfile::tempdir().unwrap();
    catalog_fixture::create_catalog_fixture(dir.path())
        .await
        .unwrap();
    let catalog = CodexCatalog::open(CatalogConfig {
        codex_home: Some(dir.path().to_path_buf()),
        ..CatalogConfig::default()
    })
    .await
    .unwrap();
    let _ = tag;
    (dir, catalog)
}

#[tokio::test]
async fn schema_probe_reports_full_capabilities() {
    let (_dir, catalog) = open_fixture("probe").await;
    let caps = catalog.capabilities();
    assert!(caps.sessions_listable);
    assert!(caps.history_readable);
    assert!(caps.spawn_edges_readable);
    assert!(caps.project_names_readable);
    assert_eq!(caps.state_db.as_deref(), Some("state_5.sqlite"));
    assert_eq!(caps.history_db.as_deref(), Some("thread_history_1.sqlite"));
}

#[tokio::test]
async fn list_sessions_excludes_archived_and_never_leaks_cwd() {
    let (_dir, catalog) = open_fixture("list").await;

    let page = catalog.list_sessions(None, 50, false).await.unwrap();
    assert_eq!(page.threads.len(), 2, "归档会话默认不出现在列表");
    assert!(!page.has_more);

    let main = page
        .threads
        .iter()
        .find(|t| t.id == MAIN)
        .expect("fixture-alpha in page");
    assert_eq!(main.title.as_deref(), Some("fixture-alpha"));
    // projects.name 优先于 cwd 尾段。
    assert_eq!(
        main.project_display_name.as_deref(),
        Some("fixture-project")
    );
    assert_eq!(main.git_branch.as_deref(), Some("fixture-branch"));
    assert_eq!(main.model.as_deref(), Some("gpt-5.3-fixture"));

    // 绝对路径绝不离开 adapter(§12):序列化整个页面后检查。
    let text = serde_json::to_string(&page).unwrap();
    assert!(!text.contains("/tmp/fixture"), "cwd leaked: {text}");

    // 归档会话在显式包含时可见。
    let all = catalog.list_sessions(None, 50, true).await.unwrap();
    assert_eq!(all.threads.len(), 3);
    let archived = all.threads.iter().find(|t| t.archived).unwrap();
    assert_eq!(archived.id, "22222222-2222-4222-8222-222222222222");
}

#[tokio::test]
async fn list_sessions_stable_cursor_paging() {
    let (_dir, catalog) = open_fixture("paging").await;
    let page1 = catalog.list_sessions(None, 1, true).await.unwrap();
    assert_eq!(page1.threads.len(), 1);
    assert!(page1.has_more);
    let cursor = page1.next_cursor.clone().expect("cursor for page 2");

    let page2 = catalog.list_sessions(Some(&cursor), 1, true).await.unwrap();
    assert_eq!(page2.threads.len(), 1);
    assert_ne!(page1.threads[0].id, page2.threads[0].id, "游标推进无重复");

    let page3 = catalog
        .list_sessions(Some(page2.next_cursor.as_deref().unwrap()), 1, true)
        .await
        .unwrap();
    assert_eq!(page3.threads.len(), 1);
    assert!(!page3.has_more);
    assert!(page3.next_cursor.is_none());
}

#[tokio::test]
async fn session_history_maps_items_and_turn_status() {
    let (_dir, catalog) = open_fixture("history").await;
    let page = catalog.session_history(MAIN, None, 50).await.unwrap();
    assert_eq!(page.entries.len(), 9);
    assert!(!page.has_more);

    // 最新(ordinal 最大)在最前:brandNewType → Opaque 保留类型名。
    let first = &page.entries[0].item;
    assert_eq!(first.item_id.id, "item-x1");
    match &first.content {
        bridge::domain::ItemContent::Opaque { native_type } => {
            assert_eq!(native_type, "brandNewType");
        }
        other => panic!("expected opaque, got {other:?}"),
    }

    // turn 状态映射(§10.4/§10.7)。
    let completed = page
        .turns
        .iter()
        .find(|t| t.turn_id.id == "turn-f1")
        .unwrap();
    assert_eq!(
        completed.outcome,
        Some(bridge::domain::LastTurnOutcome::Completed)
    );
    assert_eq!(completed.phase, bridge::domain::ActiveTurnPhase::Idle);
    let running = page
        .turns
        .iter()
        .find(|t| t.turn_id.id == "turn-f2")
        .unwrap();
    assert_eq!(running.outcome, None);
    assert_eq!(running.phase, bridge::domain::ActiveTurnPhase::Running);

    // 命令 item 的输出经投影进入命令条目(通道 COMBINED;正文在 item 内)。
    let cmd = page
        .entries
        .iter()
        .find(|e| e.item.item_id.id == "item-c1")
        .unwrap();
    match &cmd.item.content {
        bridge::domain::ItemContent::CommandStatus { state, .. } => {
            assert_eq!(*state, bridge::domain::CommandStatusState::Completed);
        }
        other => panic!("expected command status, got {other:?}"),
    }
}

#[tokio::test]
async fn session_history_forward_paging_by_ordinal() {
    let (_dir, catalog) = open_fixture("history-paging").await;
    let page1 = catalog.session_history(MAIN, None, 3).await.unwrap();
    assert_eq!(page1.entries.len(), 3);
    assert!(page1.has_more);
    let cursor = page1.next_cursor.clone().unwrap();

    let page2 = catalog
        .session_history(MAIN, Some(&cursor), 3)
        .await
        .unwrap();
    assert_eq!(page2.entries.len(), 3);
    // 无重复:页 1 的首条(最新)不得再出现。
    let seen: Vec<&str> = page1
        .entries
        .iter()
        .map(|e| e.item.item_id.id.as_str())
        .collect();
    for entry in &page2.entries {
        assert!(!seen.contains(&entry.item.item_id.id.as_str()));
    }
}

#[tokio::test]
async fn child_threads_nested_under_parent() {
    let (_dir, catalog) = open_fixture("children").await;
    let children = catalog.child_threads(MAIN).await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, "33333333-3333-4333-8333-333333333333");
    assert_eq!(
        children[0].project_display_name.as_deref(),
        Some("fixture-child"),
        "子任务 cwd 同样只以显示名出现"
    );
    // 无关父会话:无子。
    let none = catalog
        .child_threads("22222222-2222-4222-8222-222222222222")
        .await
        .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn missing_columns_close_capabilities_only() {
    let dir = tempfile::tempdir().unwrap();
    catalog_fixture::create_degraded_fixture(dir.path())
        .await
        .unwrap();
    let catalog = CodexCatalog::open(CatalogConfig {
        codex_home: Some(dir.path().to_path_buf()),
        ..CatalogConfig::default()
    })
    .await
    .unwrap();
    let caps = catalog.capabilities();
    // 核心(id/title/updated_at)在 → 会话可列;缺列只关闭对应能力(§12)。
    assert!(caps.sessions_listable);
    assert!(!caps.project_names_readable, "缺 projects 表");
    assert!(!caps.spawn_edges_readable, "缺 spawn edges 表");
    assert!(caps.history_readable, "历史所需列齐全");

    let page = catalog.list_sessions(None, 50, false).await.unwrap();
    assert_eq!(page.threads.len(), 1);
    // cwd 尾段兜底显示名,不暴露绝对路径。
    assert_eq!(
        page.threads[0].project_display_name.as_deref(),
        Some("fixture-degraded")
    );
    assert_eq!(page.threads[0].model, None, "缺列 → 字段为空");
}

#[tokio::test]
async fn missing_databases_disable_capabilities_without_panic() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = CodexCatalog::open(CatalogConfig {
        codex_home: Some(dir.path().to_path_buf()),
        ..CatalogConfig::default()
    })
    .await
    .unwrap();
    let caps = catalog.capabilities();
    assert_eq!(caps.state_db, None);
    assert_eq!(caps.history_db, None);
    assert!(!caps.sessions_listable);
    let err = catalog.list_sessions(None, 50, false).await.unwrap_err();
    assert!(matches!(err, CatalogError::Unavailable(_)));
}

#[tokio::test]
async fn query_timeout_is_enforced() {
    let (_dir, _catalog) = open_fixture("timeout").await;
    // 已打开的 catalog 使用打开时的超时配置;此处用极短超时的新实例验证超时路径。
    let tight = CodexCatalog::open(CatalogConfig {
        codex_home: Some(_dir.path().to_path_buf()),
        query_timeout: Duration::ZERO,
        ..CatalogConfig::default()
    })
    .await
    .unwrap();
    let err = tight.list_sessions(None, 50, false).await.unwrap_err();
    assert!(
        matches!(err, CatalogError::Timeout { .. }),
        "actual: {err:?}"
    );
}
