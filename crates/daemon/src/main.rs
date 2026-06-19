mod config;
mod db;
mod lyrics;
mod music;
mod server;
mod util;
mod wall;

use tracing_subscriber::{
    EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

const VERSION: &str = env!("SKWD_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().skip(1).any(|a| a == "--version" || a == "-V") {
        println!("skwd-daemon {VERSION}");
        return Ok(());
    }

    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var("HOME").ok().map(|h| std::path::PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join("skwd");
    let _ = std::fs::create_dir_all(&log_dir);
    let file_appender = tracing_appender::rolling::never(&log_dir, "skwd.log");
    let (file_writer, _file_guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = || {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr).with_filter(env_filter()))
        .with(fmt::layer().with_writer(file_writer).with_ansi(false).with_filter(env_filter()))
        .init();

    Box::leak(Box::new(_file_guard));

    tracing::info!(version = VERSION, log_dir = %log_dir.display(), "skwd-daemon starting; logs at ~/.cache/skwd/skwd.log");

    wall::apply::kill_orphan_paper_procs().await;

    server::run().await
}
