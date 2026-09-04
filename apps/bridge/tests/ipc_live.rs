//! 真实 Codex Desktop 只读探针(`#[ignore]`,需显式运行)。
//!
//! 运行方式:`cargo test -p bridge --test ipc_live -- --ignored --nocapture`
//!
//! 只做只读验证:initialize 握手、thread-owner-discovery、following 订阅、
//! snapshot/patches 观察、load-complete-history。**不发送任何写命令。**
//! 不打印会话内容:仅输出结构、数量与状态枚举。

use std::path::PathBuf;

use bridge::adapter::codex::ipc::client::{
    connect, IpcClientConfig, IpcEvent, StreamDecision, StreamSync,
};
use bridge::adapter::codex::ipc::messages::{StreamChange, StreamStateChangedParams};
use bridge::adapter::codex::ipc::{discovery, messages};
use serde_json::Value;

fn live_socket() -> Option<PathBuf> {
    discovery::socket_path(None, None)
}

/// 从本地 state 库取一个最近未归档会话的原生 ID(只读连接;只使用 ID)。
async fn pick_recent_thread_id() -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let db = PathBuf::from(home).join(".codex").join("state_5.sqlite");
    if !db.exists() {
        return None;
    }
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db)
        .read_only(true)
        .busy_timeout(std::time::Duration::from_secs(2));
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect_with(opts)
        .await
        .ok()?;
    let row: Option<(String,)> =
        sqlx::query_as("SELECT id FROM threads ORDER BY updated_at_ms DESC LIMIT 1")
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
    pool.close().await;
    row.map(|(id,)| id)
}

fn snapshot_shape(state: &Value) -> Value {
    // 结构指纹:只保留键名、类型与数量,不保留内容。
    match state {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let shape = match v {
                        Value::Object(_) => snapshot_shape(v),
                        Value::Array(a) => Value::String(format!("array[{}]", a.len())),
                        Value::String(s) => Value::String(format!("string:{}", s.len())),
                        Value::Number(_) => Value::String("number".into()),
                        Value::Bool(_) => Value::String("bool".into()),
                        Value::Null => Value::String("null".into()),
                    };
                    (k.clone(), shape)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

#[tokio::test]
#[ignore = "requires live Codex Desktop; read-only probe"]
async fn live_handshake_owner_snapshot_and_history() {
    let Some(socket) = live_socket() else {
        panic!("live socket not found");
    };
    assert!(
        discovery::socket_owned_by_current_user(&socket),
        "socket must be owned by current user"
    );

    let version = discovery::probe_version(None)
        .await
        .expect("codex --version");
    println!("codex version: {version}");
    let verified = discovery::version_is_verified(&version);
    println!("version verified: {verified}");

    let (client, mut events) = connect(IpcClientConfig::new(socket, "agent-console-bridge-probe"))
        .await
        .expect("connect + initialize");
    println!("client id assigned: {}", client.client_id());

    let Some(thread_id) = pick_recent_thread_id().await else {
        println!("no threads in local state db; handshake-only verification done");
        return;
    };
    println!(
        "probe target: latest thread (id withheld), id_len={}",
        thread_id.len()
    );

    // owner 发现(只读)。
    let owner = client
        .discover_owner("local", &thread_id)
        .await
        .expect("owner discovery");
    println!("owner present: {}", owner.is_some());
    let Some(_owner_client_id) = owner else {
        println!("thread not currently owned (not open in Desktop); stopping read-only probe");
        return;
    };

    // 订阅:follow → 期待定向快照。
    client
        .set_following(
            bridge::adapter::codex::ipc::messages::FollowingChangedParams {
                conversation_id: thread_id.clone(),
                host_id: "local".to_string(),
                following: true,
            },
            None,
        )
        .await
        .expect("follow broadcast");

    let mut sync = StreamSync::new();
    let mut saw_snapshot = false;
    let mut saw_patches = false;
    let mut resync_count = 0usize;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !saw_snapshot {
        let Ok(event) =
            tokio::time::timeout(std::time::Duration::from_secs(11), events.recv()).await
        else {
            break;
        };
        let Some(IpcEvent::StreamChanged(StreamStateChangedParams { change, .. })) = event else {
            continue;
        };
        match &change {
            StreamChange::Snapshot {
                revision,
                conversation_state,
            } => {
                saw_snapshot = true;
                println!("snapshot received: revision={revision}");
                let shape = snapshot_shape(conversation_state);
                if let Value::Object(map) = &shape {
                    println!("snapshot top-level keys: {}", map.len());
                    for key in [
                        "id",
                        "title",
                        "cwd",
                        "hostId",
                        "resumeState",
                        "threadRuntimeStatus",
                        "latestModel",
                        "latestReasoningEffort",
                    ] {
                        println!("  has key {key}: {}", map.contains_key(key));
                    }
                }
            }
            StreamChange::Patches {
                base_revision,
                revision,
                patches,
            } => {
                saw_patches = true;
                println!(
                    "patches received: base={base_revision} revision={revision} count={}",
                    patches.len()
                );
            }
        }
        if sync.observe(&change) == StreamDecision::Resync {
            resync_count += 1;
        }
    }
    println!("saw_snapshot={saw_snapshot} saw_patches={saw_patches} resync_needed={resync_count}");

    // 快照补偿请求(只读;owner 会回发快照)。
    let revision = client
        .load_complete_history("local", &thread_id)
        .await
        .expect("load-complete-history");
    println!("load-complete-history revision: {revision}");

    // 消化快照补偿触发的最新快照。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let Ok(event) =
            tokio::time::timeout(std::time::Duration::from_secs(6), events.recv()).await
        else {
            break;
        };
        if let Some(IpcEvent::StreamChanged(StreamStateChangedParams {
            change: StreamChange::Snapshot { revision, .. },
            ..
        })) = event
        {
            println!("post-resync snapshot revision: {revision}");
            break;
        }
    }

    // 退订。
    client
        .set_following(
            messages::FollowingChangedParams {
                conversation_id: thread_id,
                host_id: "local".to_string(),
                following: false,
            },
            None,
        )
        .await
        .expect("unfollow");

    assert!(saw_snapshot, "expected at least one snapshot from owner");
}
