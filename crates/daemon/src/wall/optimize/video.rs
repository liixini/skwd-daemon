use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::Connection;
use tokio::process::Command;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tracing::warn;

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::make_event;
use crate::util::{self, BatchJobState};

const MAX_JOBS: usize = 2;
use super::super::VIDEO_EXTS;

pub fn presets() -> serde_json::Value {
    serde_json::json!({
        "light":    { "crf": 28, "maxrate": "6M",  "bufsize": "12M" },
        "balanced": { "crf": 26, "maxrate": "10M", "bufsize": "20M" },
        "quality":  { "crf": 23, "maxrate": "16M", "bufsize": "32M" },
    })
}

pub fn resolutions() -> serde_json::Value {
    util::resolutions_json()
}

struct Preset {
    crf: u32,
    maxrate: &'static str,
    bufsize: &'static str,
}

fn get_preset(key: &str) -> Option<Preset> {
    match key {
        "light" => Some(Preset {
            crf: 28,
            maxrate: "6M",
            bufsize: "12M",
        }),
        "balanced" => Some(Preset {
            crf: 26,
            maxrate: "10M",
            bufsize: "20M",
        }),
        "quality" => Some(Preset {
            crf: 23,
            maxrate: "16M",
            bufsize: "32M",
        }),
        _ => None,
    }
}

pub type ConvertState = BatchJobState;

pub async fn start(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    state: Arc<Mutex<ConvertState>>,
    preset_key: &str,
    resolution_key: &str,
) -> anyhow::Result<()> {
    let preset = get_preset(preset_key).ok_or_else(|| anyhow::anyhow!("unknown preset: {preset_key}"))?;
    let resolution =
        util::get_resolution(resolution_key).ok_or_else(|| anyhow::anyhow!("unknown resolution: {resolution_key}"))?;

    {
        let s = state.lock().await;
        if s.running {
            bail!("already running");
        }
    }

    let video_dir = config.video_dir();
    let we_dir = if config.features.steam {
        config.we_dir()
    } else {
        PathBuf::new()
    };
    let cache_dir = config.cache_dir();
    let trash_dir = cache_dir.join("wallpaper/trash/videos");
    let converted_dir = cache_dir.join("wallpaper/converted-videos");

    let _ = tokio::fs::create_dir_all(&trash_dir).await;
    let _ = tokio::fs::create_dir_all(&converted_dir).await;

    let mut files = util::scan_dir_by_ext(&video_dir, VIDEO_EXTS).await;
    if config.features.steam && we_dir.is_dir() {
        scan_we_videos(&we_dir, &mut files).await;
    }

    let already: HashMap<String, String> = {
        let conn = db.lock().await;
        match db::list_video_conversions(&conn) {
            Ok(rows) => rows.into_iter().collect(),
            Err(_) => HashMap::new(),
        }
    };

    let mut queue = Vec::new();
    let mut skipped = 0usize;
    for src in &files {
        if let Some(p) = already.get(src)
            && p == preset_key
        {
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
        s.current_file.clear();
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
        let video_dir = video_dir.clone();
        let we_dir = we_dir.clone();
        let trash_dir = trash_dir.clone();
        let converted_dir = converted_dir.clone();
        let crf = preset.crf;
        let maxrate = preset.maxrate.to_string();
        let bufsize = preset.bufsize.to_string();
        let max_w = resolution.max_w;
        let max_h = resolution.max_h;
        let preset_name = preset_key.to_string();

        let handle = tokio::spawn(async move {
            {
                let s = state.lock().await;
                if s.cancel {
                    drop(permit);
                    return;
                }
            }

            let name = Path::new(&src)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            {
                let mut s = state.lock().await;
                s.current_file = name.clone();
            }

            let result = convert_one(
                &src,
                &video_dir,
                &we_dir,
                &trash_dir,
                &converted_dir,
                crf,
                &maxrate,
                &bufsize,
                max_w,
                max_h,
            )
            .await;

            match result {
                Ok(conv) => {
                    let conn = db.lock().await;
                    let _ = db::upsert_video_convert(
                        &conn,
                        &conv.final_dest,
                        &conv.final_dest,
                        &preset_name,
                        "hevc",
                        i64::from(conv.new_w),
                        i64::from(conv.new_h),
                        conv.orig_size.cast_signed(),
                        conv.new_size.cast_signed(),
                    );
                    if let Some(we_id) = &conv.we_id {
                        let _ = db::delete_meta_by_we_id(&conn, we_id);
                    }
                    drop(conn);

                    let mut s = state.lock().await;
                    s.succeeded += 1;
                    s.progress += 1;
                }
                Err(ConvertResult::Skip { orig_size, w, h, codec }) => {
                    let conn = db.lock().await;
                    let _ = db::upsert_video_convert(
                        &conn,
                        &src,
                        &src,
                        &preset_name,
                        &codec,
                        i64::from(w),
                        i64::from(h),
                        orig_size.cast_signed(),
                        orig_size.cast_signed(),
                    );
                    drop(conn);

                    let mut s = state.lock().await;
                    s.skipped += 1;
                    s.progress += 1;
                }
                Err(ConvertResult::Failed(e)) => {
                    warn!("convert failed for {}: {e}", name);
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

pub async fn cancel(state: &Arc<Mutex<ConvertState>>) {
    let mut s = state.lock().await;
    s.cancel = true;
}

struct ConvertOk {
    final_dest: String,
    orig_size: u64,
    new_size: u64,
    new_w: u32,
    new_h: u32,
    we_id: Option<String>,
}

enum ConvertResult {
    Skip {
        orig_size: u64,
        w: u32,
        h: u32,
        codec: String,
    },
    Failed(String),
}

async fn convert_one(
    src: &str,
    video_dir: &Path,
    we_dir: &Path,
    trash_dir: &Path,
    converted_dir: &Path,
    crf: u32,
    maxrate: &str,
    bufsize: &str,
    max_w: u32,
    max_h: u32,
) -> Result<ConvertOk, ConvertResult> {
    let src_path = Path::new(src);
    let old_name = src_path.file_name().unwrap_or_default().to_string_lossy().to_string();
    let stem = src_path.file_stem().unwrap_or_default().to_string_lossy().to_string();

    let orig_size = tokio::fs::metadata(src).await.map(|m| m.len()).unwrap_or(0);

    let (codec, width, height) = probe_video(src)
        .await
        .map_err(|e| ConvertResult::Failed(e.to_string()))?;

    if codec == "hevc" && width <= max_w && height <= max_h {
        return Err(ConvertResult::Skip {
            orig_size,
            w: width,
            h: height,
            codec,
        });
    }

    let we_id = if src_path.starts_with(we_dir) {
        src_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .map(std::string::ToString::to_string)
    } else {
        None
    };

    let dest_name = format!("{}_{}.mp4", stem, util::hash_prefix(src));
    let dest_path = converted_dir.join(&dest_name);

    let vf =
        format!("scale=min({max_w}\\,iw):min({max_h}\\,ih):force_original_aspect_ratio=decrease:force_divisible_by=2");

    let output = Command::new("ffmpeg")
        .args([
            "-y",
            "-i",
            src,
            "-c:v",
            "libx265",
            "-preset",
            "medium",
            "-crf",
            &crf.to_string(),
            "-maxrate",
            maxrate,
            "-bufsize",
            bufsize,
            "-vf",
            &vf,
            "-an",
            "-movflags",
            "+faststart",
            "-tag:v",
            "hvc1",
            &dest_path.to_string_lossy(),
        ])
        .output()
        .await
        .map_err(|e| ConvertResult::Failed(format!("ffmpeg: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ConvertResult::Failed(format!("ffmpeg: {stderr}")));
    }

    let new_meta = tokio::fs::metadata(&dest_path)
        .await
        .map_err(|e| ConvertResult::Failed(format!("stat: {e}")))?;
    let new_size = new_meta.len();

    let (_, new_w, new_h) = probe_video(dest_path.to_str().unwrap())
        .await
        .map_err(|e| ConvertResult::Failed(e.to_string()))?;

    let trash_name = format!("{}_{}", util::hash_prefix(src), old_name);
    let trash_path = trash_dir.join(&trash_name);
    let _ = tokio::fs::rename(src, &trash_path).await;

    let final_dir = if we_id.is_some() {
        video_dir
    } else {
        src_path.parent().unwrap_or(video_dir)
    };
    let final_dest = final_dir.join(&dest_name);
    let _ = tokio::fs::rename(&dest_path, &final_dest).await;

    Ok(ConvertOk {
        final_dest: final_dest.to_string_lossy().to_string(),
        orig_size,
        new_size,
        new_w,
        new_h,
        we_id,
    })
}

async fn probe_video(path: &str) -> anyhow::Result<(String, u32, u32)> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "quiet",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height",
            "-of",
            "csv=p=0",
            path,
        ])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("ffprobe: {e}"))?;

    let text = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = text.trim().split(',').collect();
    let codec = parts.first().unwrap_or(&"").to_string();
    let w = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    Ok((codec, w, h))
}

async fn scan_we_videos(we_dir: &Path, files: &mut Vec<String>) {
    let Ok(mut entries) = tokio::fs::read_dir(we_dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let sub = entry.path();
        if !sub.is_dir() {
            continue;
        }
        let Ok(mut sub_entries) = tokio::fs::read_dir(&sub).await else {
            continue;
        };
        while let Ok(Some(sub_entry)) = sub_entries.next_entry().await {
            let path = sub_entry.path();
            if !path.is_file() {
                continue;
            }
            let name = path.file_name().unwrap_or_default().to_string_lossy().to_lowercase();
            if name.starts_with("preview") {
                continue;
            }
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .map(str::to_lowercase)
                .unwrap_or_default();
            if VIDEO_EXTS.contains(&ext.as_str()) {
                files.push(path.to_string_lossy().to_string());
            }
        }
    }
}

async fn broadcast_progress(tx: &broadcast::Sender<String>, state: &Arc<Mutex<ConvertState>>) {
    let s = state.lock().await;
    let _ = tx.send(make_event(
        "skwd.wall.convert.progress",
        serde_json::json!({
            "running": s.running,
            "progress": s.progress,
            "total": s.total,
            "currentFile": s.current_file,
            "converted": s.succeeded,
            "skipped": s.skipped,
            "failed": s.failed,
        }),
    ));
}

fn broadcast_finished(tx: &broadcast::Sender<String>, s: &ConvertState) {
    let _ = tx.send(make_event(
        "skwd.wall.convert.finished",
        serde_json::json!({
            "converted": s.succeeded,
            "skipped": s.skipped,
            "failed": s.failed,
        }),
    ));
}
