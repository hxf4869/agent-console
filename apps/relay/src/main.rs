//! Agent Console Relay headless bin(§6/§27)。
//!
//! 薄壳:初始化结构化日志(§25.3 白名单字段由各调用点构造)、组装 tokio
//! runtime,把实际装配与优雅停机委托给 [`relay::serve`](library 入口,
//! 供无 UI e2e(§29.4)与未来嵌入方复用)。

use relay::state::Config;

fn main() {
    // 结构化日志:env-filter;字段由各调用点按白名单构造,不记录请求头/正文(§25.3)。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .json()
        .flatten_event(false)
        .with_current_span(false)
        .with_span_list(false)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(e) = runtime.block_on(run()) {
        tracing::error!(error = %e, "relay exited with error");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let mut server = relay::serve(config).await?;
    tracing::info!(bind = %server.addr, "relay listening");

    tokio::select! {
        _ = wait_for_shutdown_signal() => {},
        result = server.wait() => {
            // 未收到信号即退出(如监听层错误):直接上抛。
            result?;
            return Ok(());
        }
    }
    server.shutdown().await?;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
