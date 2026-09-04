use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::time::Duration;
#[tokio::test]
async fn zz_timeout_probe() {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            SqliteConnectOptions::new()
                .filename("/tmp/ac-debug-state.sqlite")
                .read_only(true),
        )
        .await
        .unwrap();
    // 先测 ready future:ZERO 是否保证 Elapsed。
    let a: Result<(), &'static str> =
        match tokio::time::timeout(Duration::ZERO, std::future::ready(Ok::<(), &str>(()))).await {
            Ok(_) => Ok(()),
            Err(_) => Err("elapsed"),
        };
    eprintln!("PROBE ready-future={a:?}");

    let b: Result<usize, String> = tokio::time::timeout(Duration::ZERO, async {
        sqlx::query("SELECT id, title FROM threads")
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len())
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| "elapsed".to_string())
    .and_then(|r| r.map_err(|e| e));
    eprintln!("PROBE sqlx-zero={b:?}");

    let c: Result<usize, String> = tokio::time::timeout(Duration::from_millis(30), async {
        sqlx::query("SELECT id, title FROM threads")
            .fetch_all(&pool)
            .await
            .map(|rows| rows.len())
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| "elapsed".to_string())
    .and_then(|r| r.map_err(|e| e));
    eprintln!("PROBE sqlx-30ms={c:?}");
}
