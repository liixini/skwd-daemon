#![allow(clippy::manual_c_str_literals)]

use anyhow::Result;
use clap::Parser;

mod audio;
mod fill_mode;
mod image_paper;
mod ipc;
mod render;
mod transition_paper;
mod video_source;
mod watchdog;
mod wayland;

use fill_mode::FillMode;

#[derive(Parser, Debug)]
#[command(name = "skwd-paper")]
#[command(about = "Memory-efficient wallpaper daemon (video & images)")]
struct Cli {
    output: String,
    file: String,
    #[arg(short = 'o', long)]
    mpv_opts: Option<String>,
    #[arg(long = "transition-from")]
    transition_from: Option<String>,
    #[arg(long = "shader", default_value = "random")]
    shader: String,
    #[arg(long = "duration-ms", default_value_t = 600)]
    duration_ms: u64,
    #[arg(long = "thumbs", value_delimiter = ',')]
    thumbs: Vec<String>,
    #[arg(long = "persist")]
    persist: bool,
    #[arg(long = "fill-mode", value_enum, default_value_t = FillMode::default())]
    fill_mode: FillMode,
    #[arg(long = "mute", default_value_t = true, action = clap::ArgAction::Set)]
    mute: bool,
    #[arg(long = "volume", default_value_t = 80)]
    volume: u32,
    #[arg(long = "playback-rate", default_value_t = 1.0)]
    playback_rate: f64,
    #[arg(long = "layer", value_enum, default_value_t = LayerArg::Background)]
    layer: LayerArg,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum LayerArg {
    Background,
    Bottom,
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

    let env_filter = || {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr).with_filter(env_filter()))
        .with(fmt::layer().with_writer(file_writer).with_ansi(false).with_filter(env_filter()))
        .init();

    let cli = Cli::parse();
    let mode = if is_image_path(&cli.file) { "image" } else { "video" };
    tracing::info!(
        file = %cli.file,
        output = %cli.output,
        mode,
        "starting skwd-paper"
    );

    if let Some(limit_mb) = std::env::var("SKWD_PAPER_RSS_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        watchdog::start(limit_mb, 30);
        tracing::info!(limit_mb, "rss watchdog enabled");
    }

    if let Some(from) = cli.transition_from.as_deref() {
        let target = if cli.output == "*" {
            transition_paper::OutputTarget::All
        } else {
            transition_paper::OutputTarget::Named(cli.output.clone())
        };
        let layer = match cli.layer {
            LayerArg::Background => transition_paper::SurfaceLayer::Background,
            LayerArg::Bottom => transition_paper::SurfaceLayer::Bottom,
        };
        return transition_paper::run(target, from, &cli.file, &cli.shader, cli.duration_ms, &cli.thumbs, cli.persist, cli.fill_mode, cli.mute, cli.volume, layer);
    }

    if mode == "image" {
        let target = if cli.output == "*" {
            image_paper::OutputTarget::All
        } else {
            image_paper::OutputTarget::Named(cli.output)
        };
        image_paper::run(target, &cli.file, cli.persist, cli.fill_mode)
    } else {
        let mut mpv_opts = parse_mpv_opts(cli.mpv_opts.as_deref().unwrap_or(""));
        if (cli.playback_rate - 1.0).abs() > f64::EPSILON
            && !mpv_opts.iter().any(|(k, _)| k == "speed")
        {
            mpv_opts.push(("speed".into(), format!("{:.4}", cli.playback_rate)));
        }
        let target = if cli.output == "*" {
            wayland::OutputTarget::All
        } else {
            wayland::OutputTarget::Named(cli.output)
        };
        wayland::run(target, &cli.file, &mpv_opts, cli.persist, cli.fill_mode)
    }
}

fn is_image_path(p: &str) -> bool {
    let lower = p.to_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif", ".avif"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}

fn parse_mpv_opts(s: &str) -> Vec<(String, String)> {
    s.split(';')
        .filter(|p| !p.is_empty())
        .filter_map(|p| {
            let mut it = p.splitn(2, '=');
            let k = it.next()?.trim().to_string();
            let v = it.next()?.trim().to_string();
            Some((k, v))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_image_path_matches_image_extensions() {
        for p in ["a.jpg", "B.PNG", "c.webp", "/x/y.AVIF", "d.gif"] {
            assert!(is_image_path(p), "{p} should be image");
        }
        for p in ["a.mp4", "b.webm", "noext", "c.txt"] {
            assert!(!is_image_path(p), "{p} should not be image");
        }
    }

    #[test]
    fn parse_mpv_opts_splits_pairs_and_trims() {
        let opts = parse_mpv_opts(" loop = inf ; volume=50 ; ");
        assert_eq!(
            opts,
            vec![
                ("loop".to_string(), "inf".to_string()),
                ("volume".to_string(), "50".to_string()),
            ]
        );
        assert!(parse_mpv_opts("").is_empty());
        assert!(parse_mpv_opts("novalue").is_empty());
    }
}
