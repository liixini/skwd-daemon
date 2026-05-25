use anyhow::Result;
use clap::Parser;

mod fill_mode;
mod image_paper;
mod ipc;
mod watchdog;

use fill_mode::FillMode;

#[derive(Parser, Debug)]
#[command(name = "skwd-paper-still")]
#[command(about = "Lightweight wallpaper renderer for static images (wl_shm only, no GPU)")]
struct Cli {
    output: String,
    file: String,
    #[arg(long = "persist")]
    persist: bool,
    #[arg(long = "fill-mode", value_enum, default_value_t = FillMode::default())]
    fill_mode: FillMode,
    #[arg(long = "namespace", default_value = "skwd-paper")]
    namespace: String,
}

fn main() -> Result<()> {
    unsafe { libc::mallopt(libc::M_ARENA_MAX, 2) };
    unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, 1024 * 1024) };

    use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let log_dir = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var("HOME").ok().map(|h| std::path::PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join("skwd");
    let _ = std::fs::create_dir_all(&log_dir);
    let file_appender = tracing_appender::rolling::never(&log_dir, "skwd.log");
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    Box::leak(Box::new(file_guard));

    let env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr).with_filter(env_filter()))
        .with(
            fmt::layer()
                .with_writer(file_writer)
                .with_ansi(false)
                .with_filter(env_filter()),
        )
        .init();

    let cli = Cli::parse();
    tracing::info!(file = %cli.file, output = %cli.output, "starting skwd-paper-still");

    if let Some(limit_mb) = std::env::var("SKWD_PAPER_RSS_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        watchdog::start(limit_mb, 30);
    }

    let target = if cli.output == "*" {
        image_paper::OutputTarget::All
    } else {
        image_paper::OutputTarget::Named(cli.output)
    };
    image_paper::run(target, &cli.file, cli.persist, cli.fill_mode, &cli.namespace)
}
