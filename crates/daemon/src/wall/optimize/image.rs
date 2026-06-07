use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::warn;

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::make_event;
use crate::util::{self, BatchJobState, CommandRunner, CommandSpec};

const MAX_JOBS: usize = 4;
const IMAGE_EXTS: &[&str] = &["png", "jpg", "jpeg", "gif"];

pub fn should_optimize(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    IMAGE_EXTS.contains(&ext.as_str())
}

pub async fn optimize_single_inline(
    runner: &dyn CommandRunner,
    config: &Config,
    db: &Arc<Mutex<Connection>>,
    src: &std::path::Path,
    _name: &str,
) -> anyhow::Result<(String, std::path::PathBuf)> {
    let preset_key = config.performance.image_optimize_preset.as_deref().unwrap_or("balanced");
    let resolution_key = config.performance.image_optimize_resolution.as_deref().unwrap_or("2k");
    let preset = get_preset(preset_key).ok_or_else(|| anyhow::anyhow!("unknown preset: {preset_key}"))?;
    let resolution = util::get_resolution(resolution_key).ok_or_else(|| anyhow::anyhow!("unknown resolution: {resolution_key}"))?;

    let wall_dir = config.wallpaper_dir();
    let cache_dir = config.cache_dir();
    let trash_dir = cache_dir.join("wallpaper/trash/images");
    let staging_dir = cache_dir.join("wallpaper/staging");

    let _ = tokio::fs::create_dir_all(&trash_dir).await;
    let _ = tokio::fs::create_dir_all(&staging_dir).await;

    let src_str = src.display().to_string();
    let opt = optimize_one(runner, &src_str, &wall_dir, &trash_dir, &staging_dir, preset.quality, resolution.max_w, resolution.max_h).await?;

    {
        let conn = db.lock().await;
        let _ = crate::db::upsert_image_optimize(
            &conn, &opt.final_dest, &opt.final_dest, preset_key, "webp",
            i64::from(opt.new_w),
            i64::from(opt.new_h),
            opt.orig_size.cast_signed(),
            opt.new_size.cast_signed(),
        );
        if opt.old_name != opt.new_name {
            let old_key = format!("static:{}", opt.old_name);
            let new_key = format!("static:{}", opt.new_name);
            let _ = crate::db::rename_meta_key(&conn, &old_key, &new_key, &opt.new_name);
        }
    }

    let final_path = wall_dir.join(&opt.new_name);
    crate::wall::apply::repoint_optimized_wallpaper(
        &cache_dir,
        &opt.old_name,
        &opt.new_name,
        &final_path.display().to_string(),
    )
    .await;
    Ok((opt.new_name, final_path))
}

pub fn presets() -> serde_json::Value {
    serde_json::json!({
        "light":    { "quality": 82, "formats": ["png", "jpg", "jpeg", "gif"] },
        "balanced": { "quality": 88, "formats": ["png", "jpg", "jpeg", "gif"] },
        "quality":  { "quality": 94, "formats": ["png", "jpg", "jpeg", "gif"] },
    })
}

pub fn resolutions() -> serde_json::Value {
    util::resolutions_json()
}

struct Preset {
    quality: u32,
}

fn get_preset(key: &str) -> Option<Preset> {
    match key {
        "light" => Some(Preset { quality: 82 }),
        "balanced" => Some(Preset { quality: 88 }),
        "quality" => Some(Preset { quality: 94 }),
        _ => None,
    }
}

pub type OptimizeState = BatchJobState;

pub async fn start(
    runner: Arc<dyn CommandRunner>,
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    state: Arc<Mutex<OptimizeState>>,
    preset_key: &str,
    resolution_key: &str,
) -> anyhow::Result<()> {
    let preset = get_preset(preset_key).ok_or_else(|| anyhow::anyhow!("unknown preset: {preset_key}"))?;
    let resolution = util::get_resolution(resolution_key).ok_or_else(|| anyhow::anyhow!("unknown resolution: {resolution_key}"))?;

    {
        let s = state.lock().await;
        if s.running {
            bail!("already running");
        }
    }

    let wall_dir = config.wallpaper_dir();
    let cache_dir = config.cache_dir();
    let trash_dir = cache_dir.join("wallpaper/trash/images");
    let staging_dir = cache_dir.join("wallpaper/staging");

    let _ = tokio::fs::create_dir_all(&trash_dir).await;
    let _ = tokio::fs::create_dir_all(&staging_dir).await;

    let files = util::scan_dir_by_ext(&wall_dir, IMAGE_EXTS).await;

    let already: HashMap<String, (String, String)> = {
        let conn = db.lock().await;
        match db::list_image_optimizations(&conn) {
            Ok(rows) => rows.into_iter().map(|(src, preset, format)| (src, (preset, format))).collect(),
            Err(_) => HashMap::new(),
        }
    };

    let mut queue = Vec::new();
    let mut skipped = 0usize;
    for src in &files {
        if let Some((p, fmt)) = already.get(src)
            && p == preset_key && fmt != "skip" {
                skipped += 1;
                continue;
            }
        queue.push(src.clone());
    }

    let total = queue.len() + skipped;

    {
        let mut s = state.lock().await;
        s.running = true;
        s.cancel = false;
        s.progress = skipped;
        s.total = total;
        s.succeeded = 0;
        s.skipped = skipped;
        s.failed = 0;
        s.current_file = String::new();
    }

    broadcast_progress(&event_tx, &state).await;

    if queue.is_empty() {
        let mut s = state.lock().await;
        s.running = false;
        broadcast_finished(&event_tx, &s);
        return Ok(());
    }

    let sem = Arc::new(Semaphore::new(MAX_JOBS));
    let mut handles = Vec::new();

    for src in queue {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let db = db.clone();
        let event_tx = event_tx.clone();
        let state = state.clone();
        let wall_dir = wall_dir.clone();
        let cache_dir = cache_dir.clone();
        let trash_dir = trash_dir.clone();
        let staging_dir = staging_dir.clone();
        let quality = preset.quality;
        let max_w = resolution.max_w;
        let max_h = resolution.max_h;
        let preset_name = preset_key.to_string();
        let runner = runner.clone();

        let handle = tokio::spawn(async move {
            {
                let s = state.lock().await;
                if s.cancel { drop(permit); return; }
            }

            let name = Path::new(&src).file_name().unwrap_or_default().to_string_lossy().to_string();
            {
                let mut s = state.lock().await;
                s.current_file = name.clone();
            }

            let result = optimize_one(&*runner, &src, &wall_dir, &trash_dir, &staging_dir, quality, max_w, max_h).await;

            match result {
                Ok(opt) => {
                    let conn = db.lock().await;
                    let _ = db::upsert_image_optimize(
                        &conn, &opt.final_dest, &opt.final_dest, &preset_name, "webp",
                        i64::from(opt.new_w), i64::from(opt.new_h), opt.orig_size.cast_signed(), opt.new_size.cast_signed(),
                    );
                    if opt.old_name != opt.new_name {
                        let old_key = format!("static:{}", opt.old_name);
                        let new_key = format!("static:{}", opt.new_name);
                        let _ = db::rename_meta_key(&conn, &old_key, &new_key, &opt.new_name);
                    }
                    drop(conn);

                    if opt.old_name != opt.new_name {
                        let _ = event_tx.send(make_event("skwd.wall.file_renamed", serde_json::json!({
                            "old_name": opt.old_name, "new_name": opt.new_name
                        })));
                        crate::wall::apply::repoint_optimized_wallpaper(
                            &cache_dir,
                            &opt.old_name,
                            &opt.new_name,
                            &wall_dir.join(&opt.new_name).display().to_string(),
                        )
                        .await;
                    }

                    let mut s = state.lock().await;
                    s.succeeded += 1;
                    s.progress += 1;
                }
                Err(e) => {
                    warn!("optimize failed for {}: {e}", name);
                    let conn = db.lock().await;
                    let _ = db::upsert_image_optimize(
                        &conn, &src, &src, &preset_name, "skip", 0, 0, 0, 0,
                    );
                    drop(conn);

                    let mut s = state.lock().await;
                    s.failed += 1;
                    s.progress += 1;
                }
            }

            broadcast_progress(&event_tx, &state).await;
            drop(permit);
        });

        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let mut s = state.lock().await;
    s.running = false;
    s.current_file.clear();
    broadcast_finished(&event_tx, &s);

    Ok(())
}

pub async fn cancel(state: &Arc<Mutex<OptimizeState>>) {
    let mut s = state.lock().await;
    s.cancel = true;
}

struct OptimizeResult {
    old_name: String,
    new_name: String,
    final_dest: String,
    orig_size: u64,
    new_size: u64,
    new_w: u32,
    new_h: u32,
}

async fn optimize_one(
    runner: &dyn CommandRunner,
    src: &str,
    wall_dir: &Path,
    trash_dir: &Path,
    staging_dir: &Path,
    quality: u32,
    max_w: u32,
    max_h: u32,
) -> anyhow::Result<OptimizeResult> {
    let src_path = Path::new(src);
    let old_name = src_path.file_name().unwrap_or_default().to_string_lossy().to_string();
    let stem = src_path.file_stem().unwrap_or_default().to_string_lossy().to_string();
    let ext = src_path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
    let new_name = format!("{stem}.webp");
    let staging_path = staging_dir.join(&new_name);

    let orig_size = tokio::fs::metadata(src).await.map(|m| m.len()).unwrap_or(0);

    let (new_size, new_w, new_h) = if ext == "gif" {
        optimize_gif(runner, src, staging_path.to_str().unwrap(), quality, max_w, max_h).await?
    } else {
        optimize_static(runner, src, staging_path.to_str().unwrap(), quality, max_w, max_h).await?
    };

    let trash_name = format!("{}_{}", &util::hash_prefix(src), old_name);
    let trash_path = trash_dir.join(&trash_name);
    tokio::fs::rename(src, &trash_path).await.map_err(|e| anyhow::anyhow!("trash move: {e}"))?;

    let final_path = wall_dir.join(&new_name);
    tokio::fs::rename(&staging_path, &final_path).await.map_err(|e| anyhow::anyhow!("final move: {e}"))?;

    Ok(OptimizeResult {
        old_name,
        new_name,
        final_dest: final_path.to_string_lossy().to_string(),
        orig_size,
        new_size,
        new_w,
        new_h,
    })
}

async fn optimize_static(
    runner: &dyn CommandRunner,
    src: &str,
    dest: &str,
    quality: u32,
    max_w: u32,
    max_h: u32,
) -> anyhow::Result<(u64, u32, u32)> {
    let output = runner
        .run(CommandSpec::new("magick").args(build_magick_resize_args(src, dest, quality, max_w, max_h)))
        .await
        .map_err(|e| anyhow::anyhow!("magick: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("magick failed: {stderr}");
    }

    let meta = tokio::fs::metadata(dest).await.map_err(|e| anyhow::anyhow!("stat: {e}"))?;
    let new_size = meta.len();

    let identify = runner
        .run(CommandSpec::new("magick").args(["identify", "-format", "%w %h", dest]))
        .await
        .map_err(|e| anyhow::anyhow!("identify: {e}"))?;

    let dims = String::from_utf8_lossy(&identify.stdout);
    let (w, h) = parse_identify_dims(&dims);

    Ok((new_size, w, h))
}

fn build_magick_resize_args(src: &str, dest: &str, quality: u32, max_w: u32, max_h: u32) -> Vec<String> {
    [
        "-limit", "memory", "512MiB", "-limit", "map", "1GiB",
        src, "-resize", &format!("{max_w}x{max_h}>"),
        "-quality", &quality.to_string(), dest,
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

fn parse_identify_dims(text: &str) -> (u32, u32) {
    let parts: Vec<&str> = text.split_whitespace().collect();
    let w = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    (w, h)
}

async fn optimize_gif(
    runner: &dyn CommandRunner,
    src: &str,
    dest: &str,
    quality: u32,
    max_w: u32,
    max_h: u32,
) -> anyhow::Result<(u64, u32, u32)> {
    let vf = build_gif_vf(max_w, max_h);

    let output = runner
        .run(CommandSpec::new("ffmpeg").args(build_ffmpeg_gif_args(src, dest, quality, &vf)))
        .await
        .map_err(|e| anyhow::anyhow!("ffmpeg gif: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("ffmpeg gif failed: {stderr}");
    }

    let meta = tokio::fs::metadata(dest).await.map_err(|e| anyhow::anyhow!("stat: {e}"))?;
    let new_size = meta.len();

    let probe = runner
        .run(CommandSpec::new("ffprobe").args([
            "-v", "error", "-select_streams", "v:0", "-show_entries",
            "stream=width,height", "-of", "csv=p=0", dest,
        ]))
        .await
        .map_err(|e| anyhow::anyhow!("ffprobe: {e}"))?;

    let dims = String::from_utf8_lossy(&probe.stdout);
    let (w, h) = parse_dims_csv(&dims);

    Ok((new_size, w, h))
}

fn build_gif_vf(max_w: u32, max_h: u32) -> String {
    format!("scale=min({max_w}\\,iw):min({max_h}\\,ih):force_original_aspect_ratio=decrease")
}

fn build_ffmpeg_gif_args(src: &str, dest: &str, quality: u32, vf: &str) -> Vec<String> {
    [
        "-y", "-i", src, "-vf", vf, "-c:v", "libwebp_anim",
        "-quality", &quality.to_string(), "-loop", "0", "-an", dest,
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

fn parse_dims_csv(text: &str) -> (u32, u32) {
    let parts: Vec<&str> = text.trim().split(',').collect();
    let w = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    (w, h)
}

async fn broadcast_progress(tx: &broadcast::Sender<String>, state: &Arc<Mutex<OptimizeState>>) {
    let s = state.lock().await;
    let _ = tx.send(make_event("skwd.wall.optimize.progress", serde_json::json!({
        "running": s.running,
        "progress": s.progress,
        "total": s.total,
        "currentFile": s.current_file,
        "optimized": s.succeeded,
        "skipped": s.skipped,
        "failed": s.failed,
    })));
}

fn broadcast_finished(tx: &broadcast::Sender<String>, s: &OptimizeState) {
    let _ = tx.send(make_event("skwd.wall.optimize.finished", serde_json::json!({
        "optimized": s.succeeded,
        "skipped": s.skipped,
        "failed": s.failed,
    })));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_optimize_recognises_image_extensions() {
        for name in ["a.png", "b.JPG", "c.jpeg", "d.GIF", "/path/to/e.Png"] {
            assert!(should_optimize(name), "{name} should optimize");
        }
        for name in ["a.webp", "b.mp4", "noext", "c.txt"] {
            assert!(!should_optimize(name), "{name} should not optimize");
        }
    }

    #[test]
    fn get_preset_known_qualities() {
        assert_eq!(get_preset("light").unwrap().quality, 82);
        assert_eq!(get_preset("balanced").unwrap().quality, 88);
        assert_eq!(get_preset("quality").unwrap().quality, 94);
        assert!(get_preset("ultra").is_none());
    }

    #[test]
    fn presets_quality_matches_get_preset() {
        let p = presets();
        for key in ["light", "balanced", "quality"] {
            assert_eq!(p[key]["quality"], get_preset(key).unwrap().quality);
        }
    }

    #[test]
    fn resolutions_delegates_to_util() {
        assert_eq!(resolutions(), util::resolutions_json());
    }

    #[test]
    fn build_magick_resize_args_includes_resize_and_quality() {
        let args = build_magick_resize_args("in.png", "out.webp", 88, 2560, 1440);
        assert_eq!(args[0], "-limit");
        assert_eq!(args[args.len() - 1], "out.webp");
        let joined = args.join(" ");
        assert!(joined.contains("in.png"));
        assert!(joined.contains("-resize 2560x1440>"));
        assert!(joined.contains("-quality 88"));
    }

    #[test]
    fn parse_identify_dims_reads_space_separated() {
        assert_eq!(parse_identify_dims("1920 1080"), (1920, 1080));
        assert_eq!(parse_identify_dims("  640   480  \n"), (640, 480));
        assert_eq!(parse_identify_dims(""), (0, 0));
        assert_eq!(parse_identify_dims("oops"), (0, 0));
    }

    #[test]
    fn build_gif_vf_and_args() {
        let vf = build_gif_vf(1280, 720);
        assert!(vf.contains("min(1280\\,iw)"));
        let args = build_ffmpeg_gif_args("a.gif", "a.webp", 75, &vf);
        let joined = args.join(" ");
        assert!(joined.contains("libwebp_anim"));
        assert!(joined.contains("-quality 75"));
        assert!(joined.contains("-loop 0"));
        assert_eq!(args[args.len() - 1], "a.webp");
    }

    #[test]
    fn parse_dims_csv_reads_comma_separated() {
        assert_eq!(parse_dims_csv("800,600\n"), (800, 600));
        assert_eq!(parse_dims_csv(""), (0, 0));
        assert_eq!(parse_dims_csv("100"), (100, 0));
    }

    // Contract: skwd-wall's ImageOptimizeService.qml reads these exact fields off the
    // skwd.wall.optimize.progress event (note the camelCase `currentFile`).
    #[tokio::test]
    async fn optimize_progress_event_contract() {
        let (tx, mut rx) = broadcast::channel(8);
        let state = Arc::new(Mutex::new(OptimizeState::default()));
        {
            let mut s = state.lock().await;
            s.running = true;
            s.progress = 3;
            s.total = 10;
            s.current_file = "pic.png".to_string();
            s.skipped = 1;
            s.succeeded = 2;
        }
        broadcast_progress(&tx, &state).await;
        let evt: serde_json::Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(evt["event"], "skwd.wall.optimize.progress");
        let d = &evt["data"];
        assert_eq!(d["running"], true);
        assert_eq!(d["progress"], 3);
        assert_eq!(d["total"], 10);
        assert_eq!(d["currentFile"], "pic.png");
        assert_eq!(d["skipped"], 1);
        assert_eq!(d["optimized"], 2);
    }

    // Full optimize flow under mock: fake magick writes the output webp, fake identify returns dims.
    #[tokio::test]
    async fn optimize_static_full_flow_with_side_effect_runner() {
        use crate::util::{FakeRunner, OutSpec};
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.png");
        std::fs::write(&src, vec![0u8; 4000]).unwrap();
        let dest = dir.path().join("out.webp");

        let runner = FakeRunner::new();
        runner.on_creating("magick", &["-resize"], OutSpec::LastArg, &vec![0u8; 1200]);
        runner.on("magick", &["identify"], b"1280 720", 0);

        let (new_size, w, h) = optimize_static(
            &runner, src.to_str().unwrap(), dest.to_str().unwrap(), 88, 2560, 1440,
        )
        .await
        .unwrap();
        assert_eq!(new_size, 1200);
        assert_eq!((w, h), (1280, 720));
        assert!(dest.exists());
    }

    #[tokio::test]
    async fn optimize_static_bails_on_magick_failure() {
        use crate::util::FakeRunner;
        let dir = tempfile::tempdir().unwrap();
        let runner = FakeRunner::new();
        runner.on("magick", &["-resize"], b"", 1);
        let res = optimize_static(
            &runner,
            dir.path().join("in.png").to_str().unwrap(),
            dir.path().join("out.webp").to_str().unwrap(),
            88, 2560, 1440,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn optimize_gif_full_flow_with_side_effect_runner() {
        use crate::util::{FakeRunner, OutSpec};
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("anim.gif");
        std::fs::write(&src, vec![0u8; 6000]).unwrap();
        let dest = dir.path().join("anim.webp");

        let runner = FakeRunner::new();
        runner.on_creating("ffmpeg", &["libwebp_anim"], OutSpec::LastArg, &vec![0u8; 900]);
        runner.on("ffprobe", &["csv=p=0"], b"640,480", 0);

        let (new_size, w, h) = optimize_gif(
            &runner, src.to_str().unwrap(), dest.to_str().unwrap(), 75, 1280, 720,
        )
        .await
        .unwrap();
        assert_eq!(new_size, 900);
        assert_eq!((w, h), (640, 480));
    }
}
