//! Agent Console Relay(§6/§27)library 入口。
//!
//! `serve` 组装:路由(browser WSS、bridge WSS、/agent-console/api/*、
//! /agent-console/bridge/*、/health)、PostgreSQL 连接池与迁移、后台维护任务
//! (introspection/内存监控/过期清理/transfer sweeper)与优雅停机(§26.4),
//! 返回绑定的地址与停机句柄,供 headless bin 与无 UI e2e(§29.4)复用。
//!
//! 结构化日志(§25.3 白名单字段)由调用方初始化;各调用点只按白名单构造字段,
//! 不记录请求头/正文。

pub mod audit;
pub mod auth;
pub mod devices;
pub mod push;
pub mod realtime;
pub mod sessions;
pub mod state;
pub mod transfers;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::get, routing::post, Router};
use tokio::sync::watch;
use tower_http::trace::TraceLayer;

use crate::state::{limits, AppState, Config};

/// 运行中的 Relay 服务器:绑定地址 + 优雅停机句柄。
pub struct RelayServer {
    /// 实际绑定的地址(配置 port=0 时由内核分配)。
    pub addr: SocketAddr,
    /// `http://<addr>` 基地址(开发/e2e 使用)。
    pub base_url: String,
    shutdown_tx: watch::Sender<bool>,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RelayServer {
    /// 触发优雅停机(§26.4:停止接受新命令、未完成命令回执 OUTCOME_UNKNOWN、
    /// 关闭浏览器连接、停后台任务)并等待服务器退出。
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.server.await??;
        Ok(())
    }

    /// 等待服务器任务退出(未被 shutdown 调用时,如监听失败)。
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        let handle = &mut self.server;
        handle
            .await
            .map_err(|e| anyhow::anyhow!("relay task: {e}"))?
    }
}

/// 组装并启动 Relay(监听 `config.bind_addr`;port 0 时由内核分配)。
/// 返回 [`RelayServer`];迁移在返回前完成。
pub async fn serve(config: Config) -> anyhow::Result<RelayServer> {
    let config = Arc::new(config);
    tracing::info!(bind = %config.bind_addr, "relay starting");

    // PostgreSQL 连接池 + 迁移(§18;独立数据库/独立用户由部署保证)。
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .connect(&config.database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&db).await?;
    tracing::info!("migrations applied");

    let toolbox = auth::ToolboxClient::new(
        config.devtoolbox_base_url.clone(),
        config.internal_token.clone(),
    );
    let hub = Arc::new(realtime::Hub::new());
    let app_state = AppState {
        config: config.clone(),
        db,
        hub: hub.clone(),
        toolbox,
        transfers: Arc::new(transfers::TransferRegistry::with_hub(hub)),
        push: push::PushState::from_env(),
    };

    // ------------------------------------------------------------------
    // 路由(§27;唯一路由合同:Bridge 面经公网网关同域 /agent-console/bridge/*,
    // Browser 面经网关 forward-auth /agent-console/api/*,不留 /internal 双份)
    // ------------------------------------------------------------------
    let api = devices::routes()
        .merge(sessions::routes())
        // 文件数据面 Browser 端点(§22.4/§22.5)。
        .merge(transfers::browser_routes())
        .merge(push::routes())
        .merge(audit::routes());

    let app = Router::new()
        .route("/health", get(health))
        // Bridge WSS(设备凭据摘要认证;不用浏览器 forward-auth,§21)。
        .route(
            "/agent-console/bridge/ws",
            get(devices::bridge_ws::bridge_ws_handler),
        )
        // Bridge pairing 入口(未绑定 Bridge;按来源限速,§21)。
        .route(
            "/agent-console/bridge/pairing/register",
            post(devices::pairing::register),
        )
        .route(
            "/agent-console/bridge/pairing/claim",
            post(devices::pairing::claim),
        )
        // Bridge producer/consumer 出站端点(§22.4/§22.5;公网经网关可达)。
        .merge(transfers::device_routes())
        // Browser WSS(ticket 认证)。
        .route(
            "/agent-console/ws",
            get(auth::browser_ws::browser_ws_handler),
        )
        // Browser HTTP API(可信代理身份头)。
        .nest("/agent-console/api", api)
        .layer(TraceLayer::new_for_http())
        .with_state(app_state.clone());

    // ------------------------------------------------------------------
    // 后台任务
    // ------------------------------------------------------------------
    let introspect_state = app_state.clone();
    let introspection =
        tokio::spawn(async move { auth::introspection_loop(introspect_state).await });

    let monitor_state = app_state.clone();
    let monitor = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            monitor_state.hub.sweep_global_memory();
        }
    });

    let maint_state = app_state.clone();
    let maintenance = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(limits::MAINTENANCE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // 跳过立即 tick
        loop {
            ticker.tick().await;
            cleanup_expired(&maint_state).await;
        }
    });
    // 文件 transfer 注册表过期清理(§22.6;接入既有维护 ticker)。
    let transfer_state = app_state.clone();
    let transfer_sweeper = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // 跳过立即 tick
        loop {
            ticker.tick().await;
            transfer_state.transfers.sweep();
        }
    });
    let _ = &transfer_sweeper;

    // ------------------------------------------------------------------
    // 服务 + 优雅停机(§26.4)
    // ------------------------------------------------------------------
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let addr = listener.local_addr()?;
    let shutdown_state = app_state.clone();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(async move {
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
            tracing::info!("shutdown: stopping new commands");
            // 停止接受新命令;给已接受命令明确回执状态并落库(§26.4)。
            let pendings = shutdown_state.hub.begin_shutdown();
            for (request_id, _operation) in &pendings {
                let _ = sessions::store::update_receipt(
                    &shutdown_state.db,
                    request_id,
                    "OUTCOME_UNKNOWN",
                    "OUTCOME_UNKNOWN",
                )
                .await;
            }
            shutdown_state.hub.close_all_browsers("INTERNAL_ERROR");
            introspection.abort();
            monitor.abort();
            maintenance.abort();
            transfer_sweeper.abort();
        })
        .await;
        tracing::info!("relay stopped");
        result.map_err(|e| anyhow::anyhow!("axum serve: {e}"))
    });

    Ok(RelayServer {
        addr,
        base_url: format!("http://{addr}"),
        shutdown_tx,
        server,
    })
}

/// 有界维护任务(§18):过期 pairing、receipt、audit 清理。
pub async fn cleanup_expired(app: &AppState) {
    let audit_days = (limits::AUDIT_RETENTION.as_secs() / 86400) as i32;
    let receipt_days = (limits::RECEIPT_RETENTION.as_secs() / 86400) as i32;
    let pairing_days = (limits::PAIRING_RETENTION.as_secs() / 86400) as i32;
    for (sql, label) in [
        (
            format!("DELETE FROM audit_events WHERE created_at < now() - make_interval(days => {audit_days})"),
            "audit_events",
        ),
        (
            format!("DELETE FROM request_receipts WHERE created_at < now() - make_interval(days => {receipt_days})"),
            "request_receipts",
        ),
        (
            format!("DELETE FROM pairing_challenges WHERE expires_at < now() - make_interval(days => {pairing_days})"),
            "pairing_challenges",
        ),
    ] {
        match sqlx::query(&sql).execute(&app.db).await {
            Ok(res) => {
                let n = res.rows_affected();
                if n > 0 {
                    tracing::debug!(target: "relay::maintenance", table = label, removed = n, "cleanup");
                }
            }
            Err(e) => {
                tracing::warn!(target: "relay::maintenance", table = label, error = %e, "cleanup failed");
            }
        }
    }
}

async fn health() -> &'static str {
    "ok"
}
