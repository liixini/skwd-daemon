use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::Config;

const DEBOUNCE_SECS: f64 = 2.0;

pub type SuppressSet = std::sync::Arc<Mutex<HashSet<String>>>;

use super::{IMAGE_EXTS, VIDEO_EXTS};

#[derive(Debug, Clone)]
pub enum FsEvent {
    FileAdded {
        name: String,
        path: PathBuf,
        file_type: FileType,
    },
    FileRemoved {
        name: String,
        file_type: FileType,
    },
    FolderRemoved {
        prefix: String,
    },
    WeAdded {
        we_id: String,
        we_dir: PathBuf,
    },
    WeRemoved {
        we_id: String,
    },
    ScanDone,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    Static,
    Video,
}

impl FileType {
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Static => "static",
            FileType::Video => "video",
        }
    }
}

pub fn start(
    config: &Config,
    suppress: &SuppressSet,
) -> anyhow::Result<(mpsc::UnboundedReceiver<FsEvent>, RecommendedWatcher)> {
    let (tx, rx) = mpsc::unbounded_channel();

    let wall_dir = config.wallpaper_dir();
    let video_dir = config.video_dir();
    let we_dir = if config.features.steam {
        Some(config.we_dir())
    } else {
        None
    };

    scan_dir(&wall_dir, &tx, false);
    if video_dir != wall_dir {
        scan_dir(&video_dir, &tx, false);
    }
    if let Some(ref we) = we_dir {
        scan_we_dir(we, &tx);
    }
    let _ = tx.send(FsEvent::ScanDone);

    let wall_dir_c = wall_dir.clone();
    let video_dir_c = video_dir.clone();
    let we_dir_c = we_dir.clone().unwrap_or_default();
    let tx_c = tx.clone();

    let debounce: std::sync::Arc<Mutex<HashMap<PathBuf, Instant>>> = std::sync::Arc::new(Mutex::new(HashMap::new()));
    let debounce_c = debounce.clone();
    let suppress_c = suppress.clone();

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
        Ok(event) => handle_notify_event(
            &event,
            &wall_dir_c,
            &video_dir_c,
            &we_dir_c,
            &tx_c,
            &debounce_c,
            &suppress_c,
        ),
        Err(e) => warn!("watch error: {e}"),
    })?;

    if wall_dir.is_dir() {
        watcher.watch(&wall_dir, RecursiveMode::Recursive)?;
        info!("watching {}", wall_dir.display());
    }
    if video_dir != wall_dir && video_dir.is_dir() {
        watcher.watch(&video_dir, RecursiveMode::Recursive)?;
        info!("watching {}", video_dir.display());
    }
    if let Some(ref we) = we_dir
        && we.is_dir()
    {
        watcher.watch(we, RecursiveMode::NonRecursive)?;
        info!("watching {}", we.display());
    }

    Ok((rx, watcher))
}

fn scan_dir(dir: &Path, tx: &mpsc::UnboundedSender<FsEvent>, _is_video: bool) {
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
        if let Some(ft) = classify_file(&name) {
            let _ = tx.send(FsEvent::FileAdded {
                name,
                path,
                file_type: ft,
            });
        }
    }
}

fn scan_we_dir(dir: &Path, tx: &mpsc::UnboundedSender<FsEvent>) {
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
        let _ = tx.send(FsEvent::WeAdded { we_id, we_dir: path });
    }
}

fn handle_notify_event(
    event: &Event,
    wall_dir: &Path,
    video_dir: &Path,
    we_dir: &Path,
    tx: &mpsc::UnboundedSender<FsEvent>,
    debounce: &Mutex<HashMap<PathBuf, Instant>>,
    suppress: &Mutex<HashSet<String>>,
) {
    debug!("[watcher] notify event kind={:?} paths={:?}", event.kind, event.paths);
    for path in &event.paths {
        let (name, is_we) = if !we_dir.as_os_str().is_empty() && path.starts_with(we_dir) {
            match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => (n.to_string(), true),
                None => continue,
            }
        } else if path.starts_with(video_dir) && video_dir != wall_dir {
            match path.strip_prefix(video_dir) {
                Ok(rel) => (rel.to_string_lossy().to_string(), false),
                Err(_) => continue,
            }
        } else if path.starts_with(wall_dir) {
            match path.strip_prefix(wall_dir) {
                Ok(rel) => (rel.to_string_lossy().to_string(), false),
                Err(_) => continue,
            }
        } else {
            debug!("[watcher] no matching root for path={}", path.display());
            continue;
        };

        if name.is_empty() {
            continue;
        }

        {
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let set = suppress.lock().unwrap();
            if set.contains(fname) {
                debug!("[watcher] suppressed: {name}");
                continue;
            }
        }

        if is_we {
            let Some(_parent) = path.parent() else { continue };
            match event.kind {
                EventKind::Create(_) => {
                    if path.is_dir() {
                        let _ = tx.send(FsEvent::WeAdded {
                            we_id: name,
                            we_dir: path.clone(),
                        });
                    }
                }
                EventKind::Remove(_) => {
                    let _ = tx.send(FsEvent::WeRemoved { we_id: name });
                }
                _ => {}
            }
            continue;
        }

        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                let Some(ft) = classify_file(&name) else { continue };

                if !path.exists() {
                    debug!("[watcher] FileRemoved (gone) name={name}");
                    let _ = tx.send(FsEvent::FileRemoved { name, file_type: ft });
                    continue;
                }

                let now = Instant::now();
                let mut map = debounce.lock().unwrap();
                if let Some(last) = map.get(path)
                    && now.duration_since(*last).as_secs_f64() < DEBOUNCE_SECS
                {
                    debug!("[watcher] debounced: {name}");
                    continue;
                }
                map.insert(path.clone(), now);
                if map.len() > 200 {
                    map.retain(|_, t| now.duration_since(*t).as_secs_f64() < DEBOUNCE_SECS * 2.0);
                }
                drop(map);

                let fsize = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                debug!("[watcher] FileAdded name={name} size={fsize} type={}", ft.as_str());
                let _ = tx.send(FsEvent::FileAdded {
                    name,
                    path: path.clone(),
                    file_type: ft,
                });
            }
            EventKind::Remove(_) => {
                if let Some(ft) = classify_file(&name) {
                    let _ = tx.send(FsEvent::FileRemoved { name, file_type: ft });
                } else {
                    let prefix = if name.ends_with('/') { name } else { format!("{name}/") };
                    let _ = tx.send(FsEvent::FolderRemoved { prefix });
                }
            }
            _ => {}
        }
    }
}

fn classify_file(name: &str) -> Option<FileType> {
    let ext = name.rsplit('.').next()?.to_lowercase();
    if IMAGE_EXTS.contains(&ext.as_str()) {
        Some(FileType::Static)
    } else if VIDEO_EXTS.contains(&ext.as_str()) {
        Some(FileType::Video)
    } else {
        None
    }
}
