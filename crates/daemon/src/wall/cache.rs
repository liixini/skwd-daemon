use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tracing::{debug, info, warn};

use super::thumb;
use crate::config::Config;
use crate::db;
use crate::util::BatchJobState;

pub type CacheState = BatchJobState;

#[derive(Clone)]
struct WorkItem {
    wp_type: String,
    src: PathBuf,
    name: String,
    mtime: i64,
    thumb_path: PathBuf,
    thumb_sm_path: PathBuf,
    title: String,
    we_id: String,
    we_video: Option<PathBuf>,
}

struct CacheEntry {
    key: String,
    wp_type: String,
    name: String,
    thumb: String,
    thumb_sm: String,
    video_file: String,
    we_id: String,
    mtime: i64,
    hue: i64,
    sat: i64,
}

fn commit_and_broadcast(
    conn: &Connection,
    tx: &broadcast::Sender<String>,
    e: &CacheEntry,
) {
    if let Err(err) = db::upsert_cache_entry(
        conn, &e.key, &e.wp_type, &e.name, &e.thumb, &e.thumb_sm,
        &e.video_file, &e.we_id, e.mtime, e.hue, e.sat,
    ) {
        warn!("db upsert failed for {}: {err}", e.name);
    }
    let _ = broadcast_item_event(
        tx, &e.key, &e.wp_type, &e.name, &e.thumb,
        &e.video_file, &e.we_id, e.mtime, e.hue, e.sat,
    );
}

pub async fn nuke_and_rebuild(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    cache_state: Arc<Mutex<CacheState>>,
    event_tx: broadcast::Sender<String>,
) {
    info!("nuking all cached data");

    {
        let conn = db.lock().await;
        if let Err(e) = db::clear_all(&conn) {
            warn!("failed to clear database: {e}");
        }
    }

    let cache_dir = config.cache_dir().join("wallpaper");
    for sub in ["thumbs", "thumbs-sm", "video-thumbs", "we-thumbs"] {
        let dir = cache_dir.join(sub);
        if dir.is_dir()
            && let Err(e) = tokio::fs::remove_dir_all(&dir).await
        {
            warn!("failed to remove {}: {e}", dir.display());
        }
    }

    rebuild(config, db, cache_state, event_tx).await;
}

pub async fn rebuild(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    cache_state: Arc<Mutex<CacheState>>,
    event_tx: broadcast::Sender<String>,
) {
    {
        let mut cs = cache_state.lock().await;
        if cs.running {
            warn!("cache rebuild already running");
            return;
        }
        cs.running = true;
        cs.progress = 0;
        cs.total = 0;
    }

    let _ = broadcast_cache_event(&event_tx, "started", 0, 0);

    let cache_dir = config.cache_dir().join("wallpaper");
    let thumbs_dir = cache_dir.join("thumbs");
    let thumbs_sm_dir = cache_dir.join("thumbs-sm");
    let video_thumbs_dir = cache_dir.join("video-thumbs");
    let we_thumbs_dir = cache_dir.join("we-thumbs");

    for dir in [&thumbs_dir, &thumbs_sm_dir, &video_thumbs_dir, &we_thumbs_dir] {
        tokio::fs::create_dir_all(dir).await.ok();
    }

    let wall_dir = config.wallpaper_dir();
    let video_dir = config.video_dir();
    let mut work_items = Vec::new();

    info!("scanning static dir: {}", wall_dir.display());
    scan_media_dir(&wall_dir, &thumbs_dir, &thumbs_sm_dir, "static", IMAGE_EXTS, "", &mut work_items);
    info!("  found {} static wallpapers", work_items.len());

    let pre_video = work_items.len();
    if video_dir != wall_dir {
        info!("scanning video dir: {}", video_dir.display());
        scan_media_dir(&video_dir, &video_thumbs_dir, &thumbs_sm_dir, "video", VIDEO_EXTS, "vid-", &mut work_items);
    }
    scan_media_dir(&wall_dir, &video_thumbs_dir, &thumbs_sm_dir, "video", VIDEO_EXTS, "vid-", &mut work_items);
    info!("  found {} videos", work_items.len() - pre_video);

    if config.features.steam {
        let we_dir = config.we_dir();
        let pre_we = work_items.len();
        info!("scanning WE dir: {}", we_dir.display());
        scan_we_dir(&we_dir, &we_thumbs_dir, &thumbs_sm_dir, &mut work_items);
        info!("  found {} WE items", work_items.len() - pre_we);
    }
    info!("scan complete: {} total items found", work_items.len());

    let existing = {
        let conn = db.lock().await;
        db::get_cache_entries(&conn).unwrap_or_default()
    };
    let existing_map: HashMap<String, (String, i64)> = existing
        .into_iter()
        .map(|(ck, dk, mt)| (ck, (dk, mt)))
        .collect();

    let mut filtered = Vec::new();
    let mut seen_keys: HashSet<String> = HashSet::new();
    let mut skipped = 0;

    for item in work_items {
        let effective_type = if item.we_video.is_some() {
            "video"
        } else {
            &item.wp_type
        };
        let cache_key = format!(
            "{}:{}",
            effective_type,
            if item.we_id.is_empty() { &item.name } else { &item.we_id }
        );
        seen_keys.insert(cache_key.clone());

        if let Some((_db_key, cached_mtime)) = existing_map.get(&cache_key)
            && *cached_mtime == item.mtime
        {
            skipped += 1;
            continue;
        }
        filtered.push(item);
    }

    let stale_keys: Vec<String> = existing_map
        .iter()
        .filter(|(ck, _)| !seen_keys.contains(ck.as_str()))
        .map(|(_, (db_key, _))| db_key.clone())
        .collect();

    if !stale_keys.is_empty() {
        let conn = db.lock().await;
        let deleted = db::delete_entries(&conn, &stale_keys).unwrap_or(0);
        info!("removed {deleted} stale cache entries");
    }

    let total = filtered.len();
    info!("cache rebuild: {total} items to process, {skipped} unchanged");

    {
        let mut cs = cache_state.lock().await;
        cs.total = total + skipped;
        cs.progress = skipped;
    }

    if total == 0 {
        let mut cs = cache_state.lock().await;
        cs.running = false;
        let _ = broadcast_cache_event(&event_tx, "ready", cs.progress, cs.total);
        return;
    }

    let max_jobs = config.performance.max_thumb_jobs.max(1);
    info!("starting thumbnail generation ({total} items, {max_jobs} concurrent)");

    let ok_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fail_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(max_jobs));
    let mut handles = Vec::with_capacity(total);

    for (i, item) in filtered.into_iter().enumerate() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let db = db.clone();
        let event_tx = event_tx.clone();
        let cache_state = cache_state.clone();
        let ok_count = ok_count.clone();
        let fail_count = fail_count.clone();

        let handle = tokio::spawn(async move {
            debug!("[{}/{}] processing: {}", i + 1, total, item.name);
            match process_item(&item).await {
                Ok(tr) => {
                    ok_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let (db_type, video_file) = if let Some(vf) = &item.we_video {
                        ("video".into(), vf.display().to_string())
                    } else if item.wp_type == "video" {
                        ("video".into(), item.src.display().to_string())
                    } else {
                        (item.wp_type.clone(), String::new())
                    };
                    let entry = CacheEntry {
                        key: if item.we_id.is_empty() { thumb::cache_key(&tr.thumb_path) } else { item.we_id.clone() },
                        wp_type: db_type,
                        name: if item.title.is_empty() { item.name.clone() } else { item.title.clone() },
                        thumb: tr.thumb_path,
                        thumb_sm: tr.thumb_sm_path,
                        video_file,
                        we_id: item.we_id.clone(),
                        mtime: item.mtime,
                        hue: i64::from(thumb::hue_bucket(tr.hue, tr.sat)),
                        sat: i64::from(tr.sat),
                    };
                    let conn = db.lock().await;
                    commit_and_broadcast(&conn, &event_tx, &entry);
                    drop(conn);
                    debug!("[{}/{}] done: {}", i + 1, total, item.name);
                }
                Err(e) => {
                    fail_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!("[{}/{}] failed: {} — {e}", i + 1, total, item.name);
                }
            }

            {
                let mut cs = cache_state.lock().await;
                cs.progress += 1;
            }
            let cs = cache_state.lock().await;
            let _ = broadcast_cache_event(&event_tx, "progress", cs.progress, cs.total);
            drop(cs);
            drop(permit);
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    {
        let conn = db.lock().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let _ = conn.execute(
            "INSERT OR REPLACE INTO state(key, val) VALUES('last_rebuild', ?1)",
            rusqlite::params![now.to_string()],
        );
    }

    let mut cs = cache_state.lock().await;
    cs.running = false;
    let ok = ok_count.load(std::sync::atomic::Ordering::Relaxed);
    let fail = fail_count.load(std::sync::atomic::Ordering::Relaxed);
    info!(
        "cache rebuild complete: {}/{} — {} ok, {} failed",
        cs.progress, cs.total, ok, fail
    );
    let _ = broadcast_cache_event(&event_tx, "ready", cs.progress, cs.total);
}

async fn process_item(item: &WorkItem) -> anyhow::Result<thumb::ThumbResult> {
    match item.wp_type.as_str() {
        "static" => thumb::generate_static(&item.src, &item.thumb_path, &item.thumb_sm_path).await,
        "video" => thumb::generate_video(&item.src, &item.thumb_path, &item.thumb_sm_path, 0).await,
        "we" => {
            let we_dir = item.src.parent().unwrap_or(&item.src);
            let video_in_dir = find_we_video(we_dir);

            if let Some(video) = video_in_dir {
                match thumb::generate_video(&video, &item.thumb_path, &item.thumb_sm_path, 2).await {
                    Ok(r) => Ok(r),
                    Err(_) => {
                        thumb::generate_static(&item.src, &item.thumb_path, &item.thumb_sm_path).await
                    }
                }
            } else {
                thumb::generate_static(&item.src, &item.thumb_path, &item.thumb_sm_path).await
            }
        }
        _ => anyhow::bail!("unknown type: {}", item.wp_type),
    }
}

pub async fn process_single(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: &broadcast::Sender<String>,
    name: &str,
    src: &Path,
    wp_type: &str,
) {
    let fsize = tokio::fs::metadata(src).await.map(|m| m.len()).unwrap_or(0);
    debug!("[cache] process_single name={name} type={wp_type} size={fsize}");
    let cache_dir = config.cache_dir().join("wallpaper");
    let (thumb_dir, thumb_sm_dir) = match wp_type {
        "static" => (cache_dir.join("thumbs"), cache_dir.join("thumbs-sm")),
        "video" => (cache_dir.join("video-thumbs"), cache_dir.join("thumbs-sm")),
        _ => return,
    };

    let thumb_name = name.replace('/', "--") + ".webp";
    let thumb_path = thumb_dir.join(&thumb_name);
    let thumb_sm_path = thumb_sm_dir.join(if wp_type == "video" {
        format!("vid-{thumb_name}")
    } else {
        thumb_name.clone()
    });

    let src_mtime = file_mtime(src);

    if thumb_path.exists() {
        let thumb_mtime = file_mtime(&thumb_path);
        if thumb_mtime >= src_mtime {
            let key = thumb::cache_key(&thumb_path.display().to_string());
            let conn = db.lock().await;
            if db::has_entry(&conn, &key) {
                debug!("[cache] skip (thumb+db exist) name={name}");
                return;
            }
            drop(conn);

            let (hue, sat) = if !thumb_sm_path.exists() {
                thumb::generate_small_and_colors(&thumb_path, &thumb_sm_path)
                    .await
                    .unwrap_or((0, 0))
            } else {
                let tp = thumb_path.clone();
                tokio::task::spawn_blocking(move || -> Option<(u16, u16)> {
                    let img = image::open(&tp).ok()?;
                    Some(thumb::extract_hue_sat(&img))
                })
                .await
                .ok()
                .flatten()
                .unwrap_or((0, 0))
            };
            let video_file = if wp_type == "video" {
                src.display().to_string()
            } else {
                String::new()
            };

            let entry = CacheEntry {
                key,
                wp_type: wp_type.to_string(),
                name: name.to_string(),
                thumb: thumb_path.display().to_string(),
                thumb_sm: thumb_sm_path.display().to_string(),
                video_file,
                we_id: String::new(),
                mtime: src_mtime,
                hue: i64::from(thumb::hue_bucket(hue, sat)),
                sat: i64::from(sat),
            };
            let conn = db.lock().await;
            commit_and_broadcast(&conn, event_tx, &entry);
            drop(conn);
            return;
        }
    }

    let item = WorkItem {
        wp_type: wp_type.to_string(),
        src: src.to_path_buf(),
        name: name.to_string(),
        mtime: src_mtime,
        thumb_path,
        thumb_sm_path,
        title: String::new(),
        we_id: String::new(),
        we_video: None,
    };

    match process_item(&item).await {
        Ok(tr) => {
            debug!("[cache] thumb OK name={name}");
            let entry = CacheEntry {
                key: thumb::cache_key(&tr.thumb_path),
                wp_type: wp_type.to_string(),
                name: name.to_string(),
                thumb: tr.thumb_path,
                thumb_sm: tr.thumb_sm_path,
                video_file: if wp_type == "video" { src.display().to_string() } else { String::new() },
                we_id: String::new(),
                mtime: item.mtime,
                hue: i64::from(thumb::hue_bucket(tr.hue, tr.sat)),
                sat: i64::from(tr.sat),
            };
            let conn = db.lock().await;
            commit_and_broadcast(&conn, event_tx, &entry);
        }
        Err(e) => {
            warn!("[cache] thumb FAILED name={name}: {e}");
        }
    }
}

pub async fn process_we_single(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: &broadcast::Sender<String>,
    we_id: &str,
    we_dir: &Path,
) {
    debug!("[cache] process_we_single we_id={we_id}");
    let cache_dir = config.cache_dir().join("wallpaper");
    let thumb_dir = cache_dir.join("thumbs");
    let thumb_sm_dir = cache_dir.join("thumbs-sm");

    let preview = ["preview.jpg", "preview.png", "preview.gif"]
        .iter()
        .map(|p| we_dir.join(p))
        .find(|p| p.exists());

    let Some(preview) = preview else {
        warn!("[cache] no preview found for WE {we_id}");
        return;
    };

    let title = read_we_title(we_dir);
    let video = find_we_video(we_dir);
    let (db_type, video_file) = if let Some(ref vf) = video {
        ("video", vf.display().to_string())
    } else {
        ("we", String::new())
    };
    let thumb_path = thumb_dir.join(format!("{we_id}.webp"));
    let thumb_sm_path = thumb_sm_dir.join(format!("we-{we_id}.webp"));
    let mtime = file_mtime(we_dir);

    let item = WorkItem {
        wp_type: "we".into(),
        src: preview,
        name: we_id.to_string(),
        mtime,
        thumb_path,
        thumb_sm_path,
        title,
        we_id: we_id.to_string(),
        we_video: video,
    };

    match process_item(&item).await {
        Ok(tr) => {
            debug!("[cache] thumb OK we_id={we_id} db_type={db_type}");
            let entry = CacheEntry {
                key: thumb::cache_key(&tr.thumb_path),
                wp_type: db_type.to_string(),
                name: we_id.to_string(),
                thumb: tr.thumb_path,
                thumb_sm: tr.thumb_sm_path,
                video_file,
                we_id: we_id.to_string(),
                mtime,
                hue: i64::from(thumb::hue_bucket(tr.hue, tr.sat)),
                sat: i64::from(tr.sat),
            };
            let conn = db.lock().await;
            commit_and_broadcast(&conn, event_tx, &entry);
        }
        Err(e) => {
            warn!("[cache] thumb FAILED we_id={we_id}: {e}");
        }
    }
}

#[allow(dead_code)]
pub async fn clean_stale(config: &Config, db: Arc<Mutex<Connection>>) {
    let conn = db.lock().await;
    let Ok(entries) = db::get_cache_entries(&conn) else {
        return;
    };

    let wallpaper_dir = config.wallpaper_dir();
    let video_dir = config.video_dir();
    let we_dir = config.we_dir();

    let mut stale_keys = Vec::new();
    for (cache_key, db_key, _mtime) in &entries {
        let Some((wp_type, name)) = cache_key.split_once(':') else {
            continue;
        };
        let is_we = we_dir.join(name).exists();
        let exists = match wp_type {
            "static" => wallpaper_dir.join(name).exists(),
            "video" if is_we => true,
            "video" => video_dir.join(name).exists(),
            "we" => config.features.steam && is_we,
            _ => true,
        };
        if !exists {
            stale_keys.push(db_key.clone());
        }
    }

    if !stale_keys.is_empty() {
        let deleted = db::delete_entries(&conn, &stale_keys).unwrap_or(0);
        info!("removed {deleted} stale cache entries after scan");
    }
}


use super::{IMAGE_EXTS, VIDEO_EXTS};

fn scan_media_dir(
    dir: &Path,
    thumb_dir: &Path,
    thumb_sm_dir: &Path,
    wp_type: &str,
    exts: &[&str],
    sm_prefix: &str,
    items: &mut Vec<WorkItem>,
) {
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        let path = entry.path().to_path_buf();
        if !path.is_file() {
            continue;
        }
        let name = match path.strip_prefix(dir) {
            Ok(rel) => rel.to_string_lossy().to_string(),
            Err(_) => continue,
        };
        let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
        if !exts.contains(&ext.as_str()) {
            continue;
        }
        let thumb_name = name.replace('/', "--") + ".webp";
        let sm_name = if sm_prefix.is_empty() {
            thumb_name.clone()
        } else {
            format!("{sm_prefix}{thumb_name}")
        };
        items.push(WorkItem {
            wp_type: wp_type.into(),
            src: path.clone(),
            name: name.clone(),
            mtime: file_mtime(&path),
            thumb_path: thumb_dir.join(&thumb_name),
            thumb_sm_path: thumb_sm_dir.join(sm_name),
            title: String::new(),
            we_id: String::new(),
            we_video: None,
        });
    }
}

fn scan_we_dir(dir: &Path, thumb_dir: &Path, thumb_sm_dir: &Path, items: &mut Vec<WorkItem>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let we_id = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        let preview = ["preview.jpg", "preview.png", "preview.gif"]
            .iter()
            .map(|p| path.join(p))
            .find(|p| p.exists());

        let Some(preview) = preview else { continue };

        let title = read_we_title(&path);
        let video = find_we_video(&path);

        items.push(WorkItem {
            wp_type: "we".into(),
            src: preview,
            name: we_id.clone(),
            mtime: file_mtime(&path),
            thumb_path: thumb_dir.join(format!("{we_id}.webp")),
            thumb_sm_path: thumb_sm_dir.join(format!("we-{we_id}.webp")),
            title,
            we_id,
            we_video: video,
        });
    }
}

fn find_we_video(we_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(we_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name()?.to_str()?.to_lowercase();
        if name.starts_with("preview.") {
            continue;
        }
        let ext = name.rsplit('.').next().unwrap_or("");
        if ext == "mp4" || ext == "webm" {
            return Some(path);
        }
    }
    None
}

fn read_we_title(we_dir: &Path) -> String {
    let project = we_dir.join("project.json");
    if let Ok(text) = std::fs::read_to_string(&project)
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
        && let Some(title) = val.get("title").and_then(|v| v.as_str())
    {
        return title.to_string();
    }
    "Unknown".to_string()
}

fn file_mtime(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs().cast_signed())
}


fn broadcast_cache_event(
    tx: &broadcast::Sender<String>,
    status: &str,
    progress: usize,
    total: usize,
) -> Result<usize, broadcast::error::SendError<String>> {
    let evt = skwd_proto::Event {
        event: "skwd.wall.cache".to_string(),
        data: serde_json::json!({
            "status": status,
            "progress": progress,
            "total": total,
        }),
    };
    tx.send(serde_json::to_string(&evt).unwrap_or_default())
}

fn broadcast_item_event(
    tx: &broadcast::Sender<String>,
    key: &str,
    wp_type: &str,
    name: &str,
    thumb: &str,
    video_file: &str,
    we_id: &str,
    mtime: i64,
    hue: i64,
    sat: i64,
) -> Result<usize, broadcast::error::SendError<String>> {
    let evt = skwd_proto::Event {
        event: "skwd.wall.cached".to_string(),
        data: serde_json::json!({
            "key": key,
            "type": wp_type,
            "name": name,
            "thumb": thumb,
            "video_file": video_file,
            "we_id": we_id,
            "mtime": mtime,
            "hue": hue,
            "sat": sat,
        }),
    };
    let json = serde_json::to_string(&evt).unwrap_or_default();
    let result = tx.send(json);
    match &result {
        Ok(n) => debug!("[cache] broadcast skwd.wall.cached name={name} receivers={n}"),
        Err(_) => warn!("[cache] broadcast skwd.wall.cached name={name} FAILED (no receivers)"),
    }
    result
}
