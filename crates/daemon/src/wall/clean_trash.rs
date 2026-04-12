use std::path::Path;
use std::time::{Duration, SystemTime};

use tracing::info;

use crate::config::Config;

pub async fn run(config: &Config) {
    let cache_dir = config.cache_dir().join("wallpaper/trash");

    if config.performance.auto_delete_image_trash {
        let dir = cache_dir.join("images");
        let days = config.performance.image_trash_days;
        let count = clean_dir(&dir, days).await;
        if count > 0 {
            info!("trash cleanup: deleted {count} image files older than {days} days");
        }
    }

    if config.performance.auto_delete_video_trash {
        let dir = cache_dir.join("videos");
        let days = config.performance.video_trash_days;
        let count = clean_dir(&dir, days).await;
        if count > 0 {
            info!("trash cleanup: deleted {count} video files older than {days} days");
        }
    }
}

async fn clean_dir(dir: &Path, days: u32) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };

    let cutoff = SystemTime::now() - Duration::from_secs(u64::from(days) * 86400);
    let mut count = 0;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Ok(mtime) = path.metadata().and_then(|m| m.modified()) else {
            continue;
        };
        if mtime < cutoff && tokio::fs::remove_file(&path).await.is_ok() {
            count += 1;
        }
    }

    count
}
