use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use super::VIDEO_EXTS;

pub fn cache_dir() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".cache/skwd/optimized"))
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

fn is_video(path: &str) -> bool {
    let lower = path.to_lowercase();
    VIDEO_EXTS.iter().any(|e| lower.ends_with(&format!(".{e}")))
}

pub fn optimized_or(path: &str) -> String {
    if std::env::var("SKWD_NO_OPTIMIZED").is_ok() {
        return path.to_string();
    }
    if !is_video(path) {
        return path.to_string();
    }
    let Some(cdir) = cache_dir() else {
        return path.to_string();
    };
    let Ok(key) = cache_key(Path::new(path)) else {
        return path.to_string();
    };
    let cached = cdir.join(format!("{key}.mp4"));
    if cached.exists() {
        cached.to_string_lossy().into_owned()
    } else {
        path.to_string()
    }
}
