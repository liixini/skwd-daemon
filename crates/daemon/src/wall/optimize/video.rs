use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tracing::warn;

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::make_event;
use crate::util::{self, BatchJobState, CommandRunner, CommandSpec};

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
    runner: Arc<dyn CommandRunner>,
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
        let cache_dir = cache_dir.clone();
        let trash_dir = trash_dir.clone();
        let converted_dir = converted_dir.clone();
        let crf = preset.crf;
        let maxrate = preset.maxrate.to_string();
        let bufsize = preset.bufsize.to_string();
        let max_w = resolution.max_w;
        let max_h = resolution.max_h;
        let preset_name = preset_key.to_string();
        let runner = runner.clone();

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
                &*runner,
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

                    let new_name = Path::new(&conv.final_dest)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&name)
                        .to_string();
                    if new_name != name {
                        crate::wall::apply::repoint_optimized_wallpaper(
                            &cache_dir, &name, &new_name, &conv.final_dest,
                        )
                        .await;
                    }

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

#[derive(Debug)]
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
    runner: &dyn CommandRunner,
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

    let (codec, width, height) = probe_video(runner, src)
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

    let vf = build_scale_vf(max_w, max_h);
    let dest_str = dest_path.to_string_lossy().to_string();
    let args = build_ffmpeg_convert_args(src, crf, maxrate, bufsize, &vf, &dest_str);

    let output = runner
        .run(CommandSpec::new("ffmpeg").args(args))
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

    let (_, new_w, new_h) = probe_video(runner, dest_path.to_str().unwrap())
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

async fn probe_video(runner: &dyn CommandRunner, path: &str) -> anyhow::Result<(String, u32, u32)> {
    let output = runner
        .run(CommandSpec::new("ffprobe").args([
            "-v", "quiet", "-select_streams", "v:0", "-show_entries",
            "stream=codec_name,width,height", "-of", "csv=p=0", path,
        ]))
        .await
        .map_err(|e| anyhow::anyhow!("ffprobe: {e}"))?;

    let text = String::from_utf8_lossy(&output.stdout);
    Ok(parse_ffprobe_csv(&text))
}

fn build_scale_vf(max_w: u32, max_h: u32) -> String {
    format!("scale=min({max_w}\\,iw):min({max_h}\\,ih):force_original_aspect_ratio=decrease:force_divisible_by=2")
}

fn build_ffmpeg_convert_args(
    src: &str,
    crf: u32,
    maxrate: &str,
    bufsize: &str,
    vf: &str,
    dest: &str,
) -> Vec<String> {
    [
        "-y", "-i", src, "-c:v", "libx265", "-preset", "medium", "-crf", &crf.to_string(),
        "-maxrate", maxrate, "-bufsize", bufsize, "-vf", vf, "-an", "-movflags", "+faststart",
        "-tag:v", "hvc1", dest,
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

fn parse_ffprobe_csv(text: &str) -> (String, u32, u32) {
    let parts: Vec<&str> = text.trim().split(',').collect();
    let codec = parts.first().unwrap_or(&"").to_string();
    let w = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    let h = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0u32);
    (codec, w, h)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ffprobe_csv_extracts_codec_and_dims() {
        assert_eq!(parse_ffprobe_csv("h264,1920,1080\n"), ("h264".to_string(), 1920, 1080));
        assert_eq!(parse_ffprobe_csv("hevc,3840,2160"), ("hevc".to_string(), 3840, 2160));
    }

    #[test]
    fn parse_ffprobe_csv_tolerates_garbage() {
        assert_eq!(parse_ffprobe_csv(""), (String::new(), 0, 0));
        assert_eq!(parse_ffprobe_csv("vp9"), ("vp9".to_string(), 0, 0));
        assert_eq!(parse_ffprobe_csv("av1,wide,tall"), ("av1".to_string(), 0, 0));
    }

    #[test]
    fn build_scale_vf_clamps_to_max_dims() {
        let vf = build_scale_vf(2560, 1440);
        assert!(vf.contains("min(2560\\,iw)"));
        assert!(vf.contains("min(1440\\,ih)"));
        assert!(vf.contains("force_divisible_by=2"));
    }

    #[test]
    fn build_ffmpeg_convert_args_has_expected_flags() {
        let args = build_ffmpeg_convert_args("in.mp4", 28, "5M", "10M", "scale=x", "out.mp4");
        assert_eq!(args[0], "-y");
        assert_eq!(args[args.len() - 1], "out.mp4");
        let joined = args.join(" ");
        assert!(joined.contains("libx265"));
        assert!(joined.contains("-crf 28"));
        assert!(joined.contains("-maxrate 5M"));
        assert!(joined.contains("hvc1"));
        assert!(joined.contains("-an"));
    }

    #[test]
    fn get_preset_known_values_and_presets_agree() {
        let p = get_preset("balanced").unwrap();
        assert_eq!((p.crf, p.maxrate, p.bufsize), (26, "10M", "20M"));
        assert!(get_preset("nope").is_none());
        let j = presets();
        for key in ["light", "balanced", "quality"] {
            let gp = get_preset(key).unwrap();
            assert_eq!(j[key]["crf"], gp.crf);
            assert_eq!(j[key]["maxrate"], gp.maxrate);
        }
    }

    // Contract: skwd-wall's VideoConvertService.qml reads these fields off the
    // skwd.wall.convert.progress event (camelCase `currentFile`).
    #[tokio::test]
    async fn convert_progress_event_contract() {
        let (tx, mut rx) = broadcast::channel(8);
        let state = Arc::new(Mutex::new(ConvertState::default()));
        {
            let mut s = state.lock().await;
            s.running = true;
            s.progress = 1;
            s.total = 4;
            s.current_file = "clip.mp4".to_string();
            s.succeeded = 1;
        }
        broadcast_progress(&tx, &state).await;
        let evt: serde_json::Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(evt["event"], "skwd.wall.convert.progress");
        let d = &evt["data"];
        assert_eq!(d["running"], true);
        assert_eq!(d["progress"], 1);
        assert_eq!(d["total"], 4);
        assert_eq!(d["currentFile"], "clip.mp4");
        assert_eq!(d["converted"], 1);
    }

    // Full convert flow under mock: ffprobe returns canned dims, the fake ffmpeg *creates* the
    // output file (side-effect), and we assert the whole chain (probe -> encode -> stat -> reprobe
    // -> trash src -> place output) end to end.
    #[tokio::test]
    async fn convert_one_full_flow_with_side_effect_runner() {
        use crate::util::{FakeRunner, OutSpec};
        let dir = tempfile::tempdir().unwrap();
        let video_dir = dir.path().join("vids");
        let converted = dir.path().join("converted-videos");
        let trash = dir.path().join("trash");
        for d in [&video_dir, &converted, &trash] {
            std::fs::create_dir_all(d).unwrap();
        }
        let src = video_dir.join("clip.mp4");
        std::fs::write(&src, vec![0u8; 5000]).unwrap();

        let runner = FakeRunner::new();
        runner.on("ffprobe", &["vids/clip.mp4"], b"h264,1920,1080", 0);
        runner.on_creating("ffmpeg", &["libx265"], OutSpec::LastArg, &vec![0u8; 1500]);
        runner.on("ffprobe", &["converted-videos"], b"hevc,1280,720", 0);

        let ok = convert_one(
            &runner,
            src.to_str().unwrap(),
            &video_dir,
            std::path::Path::new("/no-we"),
            &trash,
            &converted,
            26,
            "10M",
            "20M",
            1280,
            720,
        )
        .await
        .unwrap();

        assert_eq!((ok.new_w, ok.new_h), (1280, 720));
        assert_eq!(ok.orig_size, 5000);
        assert_eq!(ok.new_size, 1500);
        assert!(ok.we_id.is_none());
        assert!(!src.exists(), "source should have been moved to trash");
        assert!(video_dir.join(&ok.final_dest).exists() || std::path::Path::new(&ok.final_dest).exists());
    }

    // Skip path: already-hevc within bounds returns the Skip variant without invoking ffmpeg.
    #[tokio::test]
    async fn convert_one_skips_already_optimal() {
        use crate::util::FakeRunner;
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("ok.mp4");
        std::fs::write(&src, b"x").unwrap();
        let runner = FakeRunner::new();
        runner.on("ffprobe", &["ok.mp4"], b"hevc,1280,720", 0);

        let res = convert_one(
            &runner, src.to_str().unwrap(), dir.path(), std::path::Path::new("/no-we"),
            dir.path(), dir.path(), 26, "10M", "20M", 2560, 1440,
        )
        .await;
        assert!(matches!(res, Err(ConvertResult::Skip { codec, .. }) if codec == "hevc"));
        assert_eq!(runner.call_count(), 1, "ffmpeg must not run on skip");
    }
}
