use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::warn;

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::make_event;
use crate::util::{self, BatchJobState};

const MAX_JOBS: usize = 4;
const IMAGE_EXTS: &[&str] = &["png", "jpg", "jpeg", "gif"];

pub fn should_optimize(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    IMAGE_EXTS.contains(&ext.as_str())
}

pub async fn optimize_single_inline(
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
    let opt = optimize_one(&src_str, &wall_dir, &trash_dir, &staging_dir, preset.quality, resolution.max_w, resolution.max_h).await?;

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
        let trash_dir = trash_dir.clone();
        let staging_dir = staging_dir.clone();
        let quality = preset.quality;
        let max_w = resolution.max_w;
        let max_h = resolution.max_h;
        let preset_name = preset_key.to_string();

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

            let result = optimize_one(&src, &wall_dir, &trash_dir, &staging_dir, quality, max_w, max_h).await;

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
        optimize_gif(src, staging_path.to_str().unwrap(), quality, max_w, max_h).await?
    } else {
        optimize_static(src, staging_path.to_str().unwrap(), quality, max_w, max_h).await?
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
    src: &str,
    dest: &str,
    quality: u32,
    max_w: u32,
    max_h: u32,
) -> anyhow::Result<(u64, u32, u32)> {
    let resize = format!("{max_w}x{max_h}>");
    let output = Command::new("magick")
        .args([
            "-limit", "memory", "512MiB",
            "-limit", "map", "1GiB",
            src,
            "-resize", &resize,
            "-quality", &quality.to_string(),
            dest,
        ])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("magick: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("magick failed: {stderr}");
    }

    let meta = tokio::fs::metadata(dest).await.map_err(|e| anyhow::anyhow!("stat: {e}"))?;
    let new_size = meta.len();

    let identify = Command::new("magick")
        .args(["identify", "-format", "%w %h", dest])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("identify: {e}"))?;

    let dims = String::from_utf8_lossy(&identify.stdout);
    let parts: Vec<&str> = dims.split_whitespace().collect();
    let w = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);

    Ok((new_size, w, h))
}

async fn optimize_gif(
    src: &str,
    dest: &str,
    quality: u32,
    max_w: u32,
    max_h: u32,
) -> anyhow::Result<(u64, u32, u32)> {
    let vf = format!(
        "scale=min({max_w}\\,iw):min({max_h}\\,ih):force_original_aspect_ratio=decrease"
    );

    let output = Command::new("ffmpeg")
        .args([
            "-y", "-i", src,
            "-vf", &vf,
            "-c:v", "libwebp_anim",
            "-quality", &quality.to_string(),
            "-loop", "0",
            "-an",
            dest,
        ])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("ffmpeg gif: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("ffmpeg gif failed: {stderr}");
    }

    let meta = tokio::fs::metadata(dest).await.map_err(|e| anyhow::anyhow!("stat: {e}"))?;
    let new_size = meta.len();

    let probe = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            dest,
        ])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("ffprobe: {e}"))?;

    let dims = String::from_utf8_lossy(&probe.stdout);
    let parts: Vec<&str> = dims.trim().split(',').collect();
    let w = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);

    Ok((new_size, w, h))
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
