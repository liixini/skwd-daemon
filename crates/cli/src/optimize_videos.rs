use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::Semaphore;

const VIDEO_EXTS: &[&str] = &["mp4", "mkv", "webm", "mov", "avi", "m4v"];
const PARALLEL_JOBS: usize = 2;

#[derive(Clone, Copy, Debug)]
enum Encoder {
    Nvenc,
    Vaapi,
    SoftwareFast,
}

impl Encoder {
    fn label(self) -> &'static str {
        match self {
            Encoder::Nvenc => "h264_nvenc (NVIDIA hardware)",
            Encoder::Vaapi => "h264_vaapi (VA-API hardware)",
            Encoder::SoftwareFast => "libx264 -preset veryfast (software)",
        }
    }
}

pub async fn run(args: &[String]) -> i32 {
    let dirs: Vec<PathBuf> = if args.is_empty() {
        default_dirs()
    } else {
        args.iter().map(PathBuf::from).collect()
    };

    let Some(cache_dir) = cache_dir() else {
        eprintln!("cannot resolve cache dir (HOME unset?)");
        return 1;
    };
    if let Err(e) = std::fs::create_dir_all(&cache_dir) {
        eprintln!("create_dir_all {}: {e}", cache_dir.display());
        return 1;
    }

    let encoder = detect_encoder().await;
    println!("using encoder: {}", encoder.label());

    let mut videos: Vec<PathBuf> = Vec::new();
    for d in &dirs {
        if d.is_file() {
            if is_video(d) {
                videos.push(d.clone());
            }
        } else {
            collect_videos(d, &mut videos);
        }
    }

    if videos.is_empty() {
        println!("no video files found");
        return 0;
    }
    println!(
        "scanning: {} video file(s) across {} location(s)",
        videos.len(),
        dirs.len()
    );

    let mut work: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut skipped = 0usize;
    for src in videos {
        let key = match cache_key(&src) {
            Ok(k) => k,
            Err(e) => {
                eprintln!("skip (key): {}: {e}", src.display());
                continue;
            }
        };
        let dst = cache_dir.join(format!("{key}.mp4"));
        if dst.exists() {
            skipped += 1;
            continue;
        }
        work.push((src, dst));
    }
    let total = work.len();
    println!(
        "{skipped} cached already, {total} to transcode (parallel={PARALLEL_JOBS})"
    );
    if total == 0 {
        println!("done (everything cached)");
        return 0;
    }

    let sem = Arc::new(Semaphore::new(PARALLEL_JOBS));
    let mut handles: Vec<tokio::task::JoinHandle<bool>> = Vec::with_capacity(total);
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for (src, dst) in work {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let counter = counter.clone();
        let handle: tokio::task::JoinHandle<bool> = tokio::spawn(async move {
            let started = Instant::now();
            let res = transcode(&src, &dst, encoder).await;
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            let ok = match res {
                Ok((orig, new)) => {
                    let pct = if orig > 0 {
                        (new as f64 / orig as f64) * 100.0
                    } else {
                        100.0
                    };
                    println!(
                        "[{n}/{total}] {} -> {:.1} MB ({:.0}% of orig) in {:.1}s",
                        src.file_name().unwrap_or_default().to_string_lossy(),
                        new as f64 / 1024.0 / 1024.0,
                        pct,
                        started.elapsed().as_secs_f64()
                    );
                    true
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&dst);
                    eprintln!("[{n}/{total}] FAILED: {}: {e}", src.display());
                    false
                }
            };
            drop(permit);
            ok
        });
        handles.push(handle);
    }

    let mut failed = 0;
    for h in handles {
        match h.await {
            Ok(true) => {}
            Ok(false) | Err(_) => failed += 1,
        }
    }

    println!("\ndone: {} new, {} cached, {} failed", total - failed, skipped, failed);
    println!("cache: {}", cache_dir.display());
    if failed > 0 { 1 } else { 0 }
}

async fn detect_encoder() -> Encoder {
    if probe_encoder("h264_nvenc").await {
        return Encoder::Nvenc;
    }
    if probe_encoder("h264_vaapi").await && Path::new("/dev/dri/renderD128").exists() {
        return Encoder::Vaapi;
    }
    Encoder::SoftwareFast
}

async fn probe_encoder(name: &str) -> bool {
    let Ok(out) = Command::new("ffmpeg")
        .args(["-hide_banner", "-encoders"])
        .output()
        .await
    else {
        return false;
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .lines()
        .any(|line| line.split_whitespace().nth(1) == Some(name))
}

fn cache_dir() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".cache/skwd/optimized"))
}

fn default_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        let home = PathBuf::from(home);
        let main = home.join("wallpaper");
        if main.exists() {
            dirs.push(main);
        }
        let we = home.join(".local/share/Steam/steamapps/workshop/content/431960");
        if we.exists() {
            dirs.push(we);
        }
    }
    dirs
}

fn is_video(p: &Path) -> bool {
    p.extension()
        .is_some_and(|e| {
            let lower = e.to_string_lossy().to_lowercase();
            VIDEO_EXTS.iter().any(|x| *x == lower)
        })
}

fn collect_videos(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(walker) = std::fs::read_dir(root) else {
        return;
    };
    for entry in walker.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_videos(&path, out);
        } else if is_video(&path) {
            out.push(path);
        }
    }
}

pub fn cache_key(path: &Path) -> std::io::Result<String> {
    let canonical = path.canonicalize()?;
    let meta = std::fs::metadata(&canonical)?;
    let size = meta.len();
    let mtime = meta
        .modified()
        .ok()
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs());
    let s = format!("{}|{size}|{mtime}", canonical.display());
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    Ok(format!("{:016x}", h.finish()))
}

async fn transcode(src: &Path, dst: &Path, encoder: Encoder) -> Result<(u64, u64), String> {
    let dst_tmp = {
        let stem = dst
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let parent = dst.parent().ok_or_else(|| "dst has no parent".to_string())?;
        parent.join(format!("{stem}.partial.mp4"))
    };
    let _ = std::fs::remove_file(&dst_tmp);

    let orig_size = std::fs::metadata(src).map(|m| m.len()).unwrap_or(0);

    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-hide_banner", "-loglevel", "error", "-y"]);

    if let Encoder::Vaapi = encoder {
        cmd.args([
            "-hwaccel",
            "vaapi",
            "-hwaccel_device",
            "/dev/dri/renderD128",
            "-hwaccel_output_format",
            "vaapi",
        ]);
    }

    cmd.arg("-i").arg(src);

    match encoder {
        Encoder::Nvenc => {
            cmd.args([
                "-vf",
                "scale=w=min(2560\\,iw):h=-2:flags=lanczos",
                "-c:v",
                "h264_nvenc",
                "-preset",
                "p4",
                "-tune",
                "hq",
                "-rc",
                "vbr",
                "-cq",
                "23",
                "-b:v",
                "0",
                "-bf",
                "0",
                "-refs:v",
                "2",
                "-r",
                "30",
                "-g",
                "60",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ac",
                "2",
                "-movflags",
                "+faststart",
            ]);
        }
        Encoder::Vaapi => {
            cmd.args([
                "-vf",
                "scale_vaapi=w=min(2560\\,iw):h=-2:format=nv12",
                "-c:v",
                "h264_vaapi",
                "-rc_mode",
                "VBR",
                "-qp",
                "23",
                "-bf",
                "0",
                "-refs:v",
                "2",
                "-r",
                "30",
                "-g",
                "60",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ac",
                "2",
                "-movflags",
                "+faststart",
            ]);
        }
        Encoder::SoftwareFast => {
            cmd.args([
                "-vf",
                "scale=w=min(2560\\,iw):h=-2:flags=lanczos",
                "-c:v",
                "libx264",
                "-profile:v",
                "main",
                "-preset",
                "veryfast",
                "-tune",
                "fastdecode",
                "-crf",
                "23",
                "-bf",
                "0",
                "-refs",
                "2",
                "-r",
                "30",
                "-g",
                "60",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ac",
                "2",
                "-movflags",
                "+faststart",
            ]);
        }
    }

    cmd.args(["-f", "mp4"]);
    cmd.arg(&dst_tmp);

    if std::env::var("SKWD_OPTIMIZE_DEBUG").is_ok() {
        let std_cmd = cmd.as_std();
        eprintln!("DBG ffmpeg argv:");
        eprintln!("  {:?}", std_cmd.get_program());
        for a in std_cmd.get_args() {
            eprintln!("    {a:?}");
        }
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("ffmpeg spawn: {e}"))?;

    if !output.status.success() {
        let _ = std::fs::remove_file(&dst_tmp);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "ffmpeg exit={}: {}",
            output.status,
            stderr.trim().lines().last().unwrap_or("")
        ));
    }

    let new_size = std::fs::metadata(&dst_tmp).map(|m| m.len()).unwrap_or(0);
    std::fs::rename(&dst_tmp, dst).map_err(|e| format!("rename: {e}"))?;
    Ok((orig_size, new_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_video_checks_extension_case_insensitively() {
        for p in ["a.mp4", "b.MKV", "/x/y.webm", "z.MOV", "c.m4v"] {
            assert!(is_video(Path::new(p)), "{p} should be video");
        }
        for p in ["a.png", "b.gif", "noext", "c.txt"] {
            assert!(!is_video(Path::new(p)), "{p} should not be video");
        }
    }

    #[test]
    fn cache_key_is_stable_and_size_sensitive() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("clip.mp4");
        std::fs::write(&file, b"hello").unwrap();

        let k1 = cache_key(&file).unwrap();
        assert_eq!(k1.len(), 16);
        assert_eq!(k1, cache_key(&file).unwrap());

        std::fs::write(&file, b"hello world, longer now").unwrap();
        assert_ne!(k1, cache_key(&file).unwrap());
    }

    #[test]
    fn cache_key_errors_on_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        assert!(cache_key(&dir.path().join("nope.mp4")).is_err());
    }
}
