use std::sync::Arc;

use skwd_proto::{Request, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::db;
use crate::wall::analysis::AnalysisState;
use crate::wall::cache::CacheState;
use crate::wall::optimize::OptimizeState;
use crate::wall::steam::SteamState;
use crate::wall::{self, apply, cache, optimize, steam, watcher};
use notify::Watcher as _;

use super::*;

pub async fn run() -> anyhow::Result<()> {
    let sock_path = skwd_proto::socket_path();

    if let Some(parent) = sock_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if sock_path.exists() {
        tokio::fs::remove_file(&sock_path).await?;
    }

    let listener = UnixListener::bind(&sock_path)?;
    info!("listening on {}", sock_path.display());

    let (event_tx, _) = broadcast::channel::<String>(256);

    wall::bootstrap::run(&crate::config::load().unwrap_or_default()).await;
    let config = crate::config::load().expect("failed to load config");
    wall::clean_trash::run(&config).await;

    {
        let cfg_clone = config.clone();
        tokio::spawn(async move {
            if let Some(prev) = wall::overview_backdrop::resolve_source(&cfg_clone).await {
                wall::overview_backdrop::refresh(&prev, &cfg_clone).await;
            }
        });
    }

    let steam_state = Arc::new(Mutex::new(SteamState::new(&config)));

    let state = SharedState {
        config: Arc::new(RwLock::new(config.clone())),
        db: Arc::new(Mutex::new(db::open().expect("failed to open database"))),
        db_shared: Arc::new(Mutex::new(db::open().expect("failed to open shared db"))),
        ui: Arc::new(Mutex::new(ManagedProcess::new("wall-ui", "SKWD_WALL_INSTALL", resolve_shell_qml()))),
        host: Arc::new(Mutex::new(ManagedProcess::new("host", "SKWD_HOST_INSTALL", resolve_host_qml()))),
        music_proc: Arc::new(Mutex::new(ManagedProcess::new("music", "SKWD_MUSIC_INSTALL", resolve_music_qml()))),
        current_wallpaper: Arc::new(Mutex::new(None)),
        cache_state: Arc::new(Mutex::new(CacheState::default())),
        steam_state,
        optimize_state: Arc::new(Mutex::new(OptimizeState::default())),
        convert_state: Arc::new(Mutex::new(optimize::ConvertState::default())),
        analysis_state: Arc::new(Mutex::new(AnalysisState::default())),
        suppress_set: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        random_rotation: Arc::new(Mutex::new(None)),
        music: Arc::new(crate::music::MusicState::new(event_tx.clone())),
        workshop_browser: Arc::new(Mutex::new(None)),
        runner: Arc::new(crate::util::RealRunner),
    };
    {
        let mut st = state.steam_state.lock().await;
        st.attach_browser(state.workshop_browser.clone());
    }
    steam::recover_queue(&state.steam_state, &event_tx).await;
    state.music.auth.load_from_disk().await;

    if config.features.music && !is_headless() {
        start_music_module(&state).await;
    }

    if config.features.music {
        state.music_proc.lock().await.launch();
    }

    {
        let extra_env = build_host_env(&config).await;
        state.host.lock().await.launch_with_env(&extra_env);
    }

    {
        let state_for_diff = state.clone();
        let mut last_music = config.features.music;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                let cur = state_for_diff.config.read().await.features.music;
                if cur != last_music {
                    last_music = cur;
                    if cur {
                        info!("[modules] music feature enabled, starting");
                        start_music_module(&state_for_diff).await;
                        state_for_diff.music_proc.lock().await.launch();
                    } else {
                        info!("[modules] music feature disabled, stopping");
                        state_for_diff.music_proc.lock().await.kill();
                        stop_music_module(&state_for_diff).await;
                    }
                }
            }
        });
    }

    let _watcher_handle: Option<notify::RecommendedWatcher> = match watcher::start(&config, &state.suppress_set) {
        Ok((rx, handle)) => {
            let tx = event_tx.clone();
            let ws = state.clone();
            tokio::spawn(run_watcher_loop(rx, tx, ws));
            Some(handle)
        }
        Err(e) => {
            warn!("file watcher failed to start: {e}");
            None
        }
    };

    let _config_watcher: Option<notify::RecommendedWatcher> = {
        let config_path = crate::config::config_path();
        let config_dir = config_path.parent().map(std::path::Path::to_path_buf);
        let state = state.clone();
        let tx = event_tx.clone();

        let (cfg_tx, mut cfg_rx) = mpsc::unbounded_channel::<()>();
        let cfg_file = config_path.clone();

        let watcher = config_dir.and_then(|dir| {
            let mut w = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    let dominated = matches!(event.kind, notify::EventKind::Modify(_) | notify::EventKind::Create(_));
                    if dominated && event.paths.iter().any(|p| p == &cfg_file) {
                        let _ = cfg_tx.send(());
                    }
                }
            })
            .ok()?;
            w.watch(&dir, notify::RecursiveMode::NonRecursive).ok()?;
            info!("[config] watching {}", dir.display());
            Some(w)
        });

        tokio::spawn(async move {
            while cfg_rx.recv().await.is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(CONFIG_RELOAD_DELAY_MS)).await;
                while cfg_rx.try_recv().is_ok() {}

                match crate::config::load() {
                    Ok(new_cfg) => {
                        info!("[config] reloaded from {}", config_path.display());
                        let prev_engine = state.config.read().await.paper.engine;
                        let new_engine = new_cfg.paper.engine;
                        let release_browser = state.steam_state.lock().await.apply_runtime_config(&new_cfg);
                        if release_browser {
                            info!("[config] steam backend left the client, releasing steamworks browser");
                            *state.workshop_browser.lock().await = None;
                        }
                        *state.config.write().await = new_cfg;
                        let _ = broadcast_event(&tx, "skwd.wall.config_changed", serde_json::json!({}));

                        if prev_engine != new_engine {
                            info!(
                                "[config] paper.engine changed: {:?} -> {:?}, re-applying static wallpapers",
                                prev_engine, new_engine
                            );
                            let cfg_snapshot = state.config.read().await.clone();
                            tokio::spawn(async move {
                                if let Err(e) =
                                    crate::wall::apply::reapply_statics_for_engine_change(&cfg_snapshot).await
                                {
                                    warn!("[config] engine-change re-apply failed: {e}");
                                }
                            });
                        }
                    }
                    Err(e) => {
                        warn!("[config] reload failed: {e}");
                    }
                }
            }
        });

        watcher
    };

    loop {
        let (stream, _addr) = listener.accept().await?;
        info!("client connected");
        let event_tx = event_tx.clone();
        let event_rx = event_tx.subscribe();
        let state = state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, event_tx, event_rx, state).await {
                debug!("client disconnected: {e}");
            }
        });
    }
}

pub(super) fn file_removed_payload(name: &str, file_type: &watcher::FileType) -> serde_json::Value {
    let wp_type = if *file_type == watcher::FileType::Static { "static" } else { "video" };
    serde_json::json!({ "name": name, "type": wp_type })
}

pub(super) fn folder_removed_payload(prefix: &str, names: &[String]) -> serde_json::Value {
    serde_json::json!({ "prefix": prefix, "names": names })
}

pub(super) fn we_added_payload(we_id: &str, we_dir: &std::path::Path) -> serde_json::Value {
    serde_json::json!({ "we_id": we_id, "we_dir": we_dir.display().to_string() })
}

pub(super) fn we_removed_payload(we_id: &str) -> serde_json::Value {
    serde_json::json!({ "we_id": we_id })
}

async fn run_watcher_loop(
    mut rx: mpsc::UnboundedReceiver<watcher::FsEvent>,
    tx: broadcast::Sender<String>,
    state: SharedState,
) {
    enum WatcherPhase {
        Scanning,
        Ready,
    }
    let mut phase = WatcherPhase::Scanning;

    loop {
        let Some(evt) = rx.recv().await else { break };

        match &evt {
            watcher::FsEvent::FileAdded { name, path, file_type } => {
                if matches!(phase, WatcherPhase::Scanning) {
                    continue;
                }
                info!("[server] watcher FileAdded name={name} path={}", path.display());
                let wp_type = if *file_type == watcher::FileType::Static {
                    "static"
                } else {
                    "video"
                };
                let config = state.config.read().await.clone();
                let suppress = state.suppress_set.clone();
                let db = state.db_shared.clone();

                if wp_type == "static" && config.performance.auto_optimize_images && optimize::should_optimize(name) {
                    let stem = std::path::Path::new(name)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(name);
                    let new_name = format!("{stem}.webp");

                    {
                        let mut set = suppress.lock().unwrap();
                        set.insert(name.clone());
                        set.insert(new_name.clone());
                    }

                    match optimize::optimize_single_inline(&*state.runner, &config, &db, path, name).await {
                        Ok((final_name, final_path)) => {
                            info!("[server] optimized {name} -> {final_name}");
                            cache::process_single(&config, db, &tx, &final_name, &final_path, wp_type).await;
                        }
                        Err(e) => {
                            warn!("[server] optimize failed for {name}: {e}, caching original");
                            cache::process_single(&config, db, &tx, name, path, wp_type).await;
                        }
                    }

                    {
                        let mut set = suppress.lock().unwrap();
                        set.remove(name);
                        set.remove(&new_name);
                    }
                } else {
                    cache::process_single(&config, db, &tx, name, path, wp_type).await;
                }
            }
            watcher::FsEvent::FileRemoved { name, file_type } => {
                let wp_type = if *file_type == watcher::FileType::Static { "static" } else { "video" };
                let config = state.config.read().await.clone();
                {
                    let db = state.db_shared.clone();
                    let conn = db.lock().await;
                    let _ = db::delete_by_name(&conn, name);
                    let src_path = if *file_type == watcher::FileType::Static {
                        config.wallpaper_dir().join(name)
                    } else {
                        config.video_dir().join(name)
                    };
                    let _ = db::delete_optimize_by_src(&conn, &src_path.display().to_string());
                }
                {
                    let cache_dir = config.cache_dir().join("wallpaper");
                    let thumb_name = name.replace('/', "--") + ".webp";
                    if wp_type == "static" {
                        let _ = std::fs::remove_file(cache_dir.join("thumbs").join(&thumb_name));
                        let _ = std::fs::remove_file(cache_dir.join("thumbs-sm").join(&thumb_name));
                    } else {
                        let _ = std::fs::remove_file(cache_dir.join("video-thumbs").join(&thumb_name));
                        let _ = std::fs::remove_file(cache_dir.join("thumbs-sm").join(format!("vid-{thumb_name}")));
                    }
                }
                let _ = broadcast_event(&tx, "skwd.wall.file_removed", file_removed_payload(name, file_type));
            }
            watcher::FsEvent::FolderRemoved { prefix } => {
                let db = state.db_shared.clone();
                let deleted = db::delete_by_name_prefix(&*db.lock().await, prefix).unwrap_or_default();
                if !deleted.is_empty() {
                    let config = state.config.read().await.clone();
                    let cache_dir = config.cache_dir().join("wallpaper");
                    for name in &deleted {
                        let thumb_name = name.replace('/', "--") + ".webp";
                        for sub in &["thumbs", "video-thumbs"] {
                            let _ = std::fs::remove_file(cache_dir.join(sub).join(&thumb_name));
                        }
                        let _ = std::fs::remove_file(cache_dir.join("thumbs-sm").join(&thumb_name));
                        let _ = std::fs::remove_file(cache_dir.join("thumbs-sm").join(format!("vid-{thumb_name}")));
                    }
                    let _ = broadcast_event(&tx, "skwd.wall.folder_removed", folder_removed_payload(prefix, &deleted));
                }
            }
            watcher::FsEvent::WeAdded { we_id, we_dir } => {
                if matches!(phase, WatcherPhase::Scanning) {
                    continue;
                }
                info!("[server] watcher WeAdded we_id={we_id} dir={}", we_dir.display());
                let _ = broadcast_event(&tx, "skwd.wall.we_added", we_added_payload(we_id, we_dir));
                let config = state.config.read().await.clone();
                let db = state.db_shared.clone();
                cache::process_we_single(&config, db, &tx, we_id, we_dir).await;
            }
            watcher::FsEvent::WeRemoved { we_id } => {
                let _ = broadcast_event(&tx, "skwd.wall.we_removed", we_removed_payload(we_id));
            }
            watcher::FsEvent::ScanDone => {
                phase = WatcherPhase::Ready;
                let _ = broadcast_event(&tx, "skwd.wall.scan_done", serde_json::json!({}));
                info!("initial directory scan complete, starting cache rebuild");

                let config = state.config.read().await.clone();
                let db = state.db_shared.clone();
                cache::rebuild(&config, db.clone(), state.cache_state.clone(), tx.clone()).await;
                auto_optimize_if_enabled(state.runner.clone(), &config, db, tx.clone(), state.optimize_state.clone()).await;

                if config.restore_on_startup {
                    match apply::restore(&config).await {
                        Ok(name) => info!("auto-restored wallpaper: {name}"),
                        Err(e) => info!("no wallpaper to restore: {e}"),
                    }
                } else {
                    info!("startup restore disabled by config");
                }
            }
        }
    }
}

async fn handle_client(
    stream: tokio::net::UnixStream,
    event_tx: broadcast::Sender<String>,
    mut event_rx: broadcast::Receiver<String>,
    state: SharedState,
) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();
    let reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(writer));
    let mut lines = reader.lines();

    let subscriptions: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let wall_was_shown = std::sync::atomic::AtomicBool::new(false);

    let writer_clone = writer.clone();
    let subs_clone = subscriptions.clone();
    let event_forwarder = tokio::spawn(async move {
        let mut write_errors = 0u32;
        loop {
            match event_rx.recv().await {
                Ok(line) => {
                    let subs = subs_clone.lock().await;
                    let dominated = subs.is_empty() || subs.iter().any(|prefix| line.contains(prefix));
                    drop(subs);

                    if dominated {
                        info!(
                            "[server] forwarding event to client: {}",
                            &line[..line.floor_char_boundary(120)]
                        );
                        let mut w = writer_clone.lock().await;
                        let ok = w.write_all(line.as_bytes()).await.is_ok() && w.write_all(b"\n").await.is_ok();
                        if ok {
                            write_errors = 0;
                        } else {
                            write_errors += 1;
                            warn!("[server] event write failed ({write_errors} consecutive)");
                            if write_errors >= 3 {
                                warn!("[server] too many write failures, dropping event forwarder");
                                break;
                            }
                        }
                    } else {
                        info!(
                            "[server] event filtered out (no matching sub): {}",
                            &line[..line.floor_char_boundary(80)]
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("client lagged, dropped {n} events");
                }
                Err(_) => break,
            }
        }
    });

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let err_resp = Response::err(0, -1, format!("parse error: {e}"));
                let mut w = writer.lock().await;
                let _ = w
                    .write_all(format!("{}\n", serde_json::to_string(&err_resp)?).as_bytes())
                    .await;
                continue;
            }
        };

        debug!(method = %req.method, id = req.id, "<- request");
        match req.method.as_str() {
            "wall.show" => {
                wall_was_shown.store(true, std::sync::atomic::Ordering::Release);
            }
            "wall.hide" => {
                wall_was_shown.store(false, std::sync::atomic::Ordering::Release);
            }
            "wall.toggle" => {
                let cur = wall_was_shown.load(std::sync::atomic::Ordering::Acquire);
                wall_was_shown.store(!cur, std::sync::atomic::Ordering::Release);
            }
            _ => {}
        }
        let event_tx = event_tx.clone();
        let subscriptions = subscriptions.clone();
        let state = state.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            let response = dispatch_request(&req, &event_tx, &subscriptions, &state).await;
            let mut w = writer.lock().await;
            let _ = w.write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes()).await;
        });
    }

    event_forwarder.abort();
    if wall_was_shown.load(std::sync::atomic::Ordering::Acquire) {
        let was_long_lived = !subscriptions.lock().await.is_empty();
        if was_long_lived {
            info!("client disconnected with wall picker open; cleaning up");
            crate::wall::apply::on_wall_hide().await;
            state.ui.lock().await.kill();
        }
    }
    info!("client disconnected");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn file_removed_payload_contract() {
        let s = file_removed_payload("a/b.webp", &watcher::FileType::Static);
        assert_eq!(s["name"], "a/b.webp");
        assert_eq!(s["type"], "static");

        let v = file_removed_payload("clip.mp4", &watcher::FileType::Video);
        assert_eq!(v["type"], "video");
    }

    #[test]
    fn folder_removed_payload_contract() {
        let p = folder_removed_payload("pack", &["pack/a.webp".to_string(), "pack/b.webp".to_string()]);
        assert_eq!(p["prefix"], "pack");
        assert_eq!(p["names"].as_array().unwrap().len(), 2);
        assert_eq!(p["names"][0], "pack/a.webp");
    }

    #[test]
    fn we_added_payload_contract() {
        let p = we_added_payload("123", &PathBuf::from("/we/123"));
        assert_eq!(p["we_id"], "123");
        assert_eq!(p["we_dir"], "/we/123");
    }

    #[test]
    fn we_removed_payload_contract() {
        let p = we_removed_payload("123");
        assert_eq!(p["we_id"], "123");
    }
}
