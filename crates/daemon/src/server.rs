use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, params};
use skwd_proto::{Event, Request, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db;
use crate::util::CommandExt;
use crate::wall::analysis::AnalysisState;
use crate::wall::cache::CacheState;
use crate::wall::optimize::OptimizeState;
use crate::wall::steam::SteamState;
use crate::wall::watcher::SuppressSet;
use crate::wall::{self, analysis, apply, cache, optimize, steam, watcher};

use notify::Watcher as _;

const CONFIG_RELOAD_DELAY_MS: u64 = 200;

pub struct ManagedProcess {
    child: Option<tokio::process::Child>,
    shell_qml: PathBuf,
    label: &'static str,
    env_key: &'static str,
}

impl ManagedProcess {
    fn new(label: &'static str, env_key: &'static str, shell_qml: PathBuf) -> Self {
        Self {
            child: None,
            shell_qml,
            label,
            env_key,
        }
    }

    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => {
                    self.child = None;
                    false
                }
                Ok(None) => true,
            }
        } else {
            false
        }
    }

    pub fn launch(&mut self) {
        if self.is_running() {
            return;
        }
        info!("launching {}: quickshell -p {}", self.label, self.shell_qml.display());
        let install_dir = self.shell_qml.parent().unwrap_or(Path::new("/usr/share/skwd-wall"));
        match tokio::process::Command::new("quickshell")
            .arg("-p")
            .arg(&self.shell_qml)
            .env(self.env_key, install_dir)
            .silent()
            .spawn()
        {
            Ok(child) => {
                self.child = Some(child);
            }
            Err(e) => {
                warn!("failed to launch {}: {e}", self.label);
            }
        }
    }

    pub fn kill(&mut self) {
        if let Some(ref mut child) = self.child {
            info!("killing {} process", self.label);
            let _ = child.start_kill();
            self.child = None;
        }
    }

    pub fn toggle(&mut self) {
        if self.is_running() {
            self.kill();
        } else {
            self.launch();
        }
    }
}

fn resolve_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SHELL_QML") {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("shell.qml");
    if local.exists() {
        return std::fs::canonicalize(&local).unwrap_or(local);
    }
    let sibling = PathBuf::from("../skwd-wall/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-wall/shell.qml")
}

fn resolve_bar_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_BAR_QML") {
        return PathBuf::from(p);
    }
    let sibling = PathBuf::from("../skwd-bar/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-bar/shell.qml")
}

fn resolve_launch_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_LAUNCH_QML") {
        return PathBuf::from(p);
    }
    let sibling = PathBuf::from("../skwd-launch/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-launch/shell.qml")
}

fn resolve_switch_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SWITCH_QML") {
        return PathBuf::from(p);
    }
    let sibling = PathBuf::from("../skwd-switch/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-switch/shell.qml")
}

fn resolve_notification_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_NOTIFICATION_QML") {
        return PathBuf::from(p);
    }
    let sibling = PathBuf::from("../skwd-notification/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-notification/shell.qml")
}

async fn fdo_notifications_owned() -> bool {
    use zbus::Connection;
    use zbus::fdo::DBusProxy;

    let Ok(conn) = Connection::session().await else {
        return false;
    };
    let Ok(proxy) = DBusProxy::new(&conn).await else {
        return false;
    };
    let Ok(name) = "org.freedesktop.Notifications".try_into() else {
        return false;
    };
    proxy.name_has_owner(name).await.unwrap_or(false)
}

async fn should_launch_notification(config: &Config) -> bool {
    use crate::config::NotificationsBuiltIn::{Always, Auto, Never};
    match config.notifications.built_in {
        Never => false,
        Always => true,
        Auto => {
            let owned = fdo_notifications_owned().await;
            if owned {
                info!("[notif] org.freedesktop.Notifications already owned; skipping built-in");
            }
            !owned
        }
    }
}

fn switch_fifo_path() -> PathBuf {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(format!("{}/skwd/switch-cmd", runtime_dir))
}

fn send_switch_cmd(cmd: &str) {
    let fifo = switch_fifo_path();
    let cmd = format!("{}\n", cmd);
    tokio::spawn(async move {
        match tokio::fs::OpenOptions::new().write(true).open(&fifo).await {
            Ok(mut f) => {
                let _ = tokio::io::AsyncWriteExt::write_all(&mut f, cmd.as_bytes()).await;
            }
            Err(e) => {
                warn!("failed to write to switch FIFO: {e}");
            }
        }
    });
}

pub struct RandomRotation {
    pub handle: tokio::task::JoinHandle<()>,
    pub interval_secs: u64,
    pub types: Vec<String>,
    pub favourites_only: bool,
}

#[derive(Clone)]
pub struct SharedState {
    pub config: Arc<RwLock<Config>>,
    pub db: Arc<Mutex<Connection>>,
    pub db_shared: Arc<Mutex<Connection>>,
    pub ui: Arc<Mutex<ManagedProcess>>,
    pub bar: Arc<Mutex<ManagedProcess>>,
    pub launcher: Arc<Mutex<ManagedProcess>>,
    pub switch: Arc<Mutex<ManagedProcess>>,
    pub notification: Arc<Mutex<ManagedProcess>>,
    pub current_wallpaper: Arc<Mutex<Option<String>>>,
    pub cache_state: Arc<Mutex<CacheState>>,
    pub steam_state: Arc<Mutex<SteamState>>,
    pub optimize_state: Arc<Mutex<OptimizeState>>,
    pub convert_state: Arc<Mutex<optimize::ConvertState>>,
    pub analysis_state: Arc<Mutex<AnalysisState>>,
    pub suppress_set: SuppressSet,
    pub random_rotation: Arc<Mutex<Option<RandomRotation>>>,
}

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
    let config = crate::config::load().expect("failed to load config");

    wall::bootstrap::run(&config).await;
    wall::clean_trash::run(&config).await;

    let steam_state = Arc::new(Mutex::new(SteamState::new(&config)));
    steam::recover_queue(&steam_state).await;

    let state = SharedState {
        config: Arc::new(RwLock::new(config.clone())),
        db: Arc::new(Mutex::new(db::open().expect("failed to open database"))),
        db_shared: Arc::new(Mutex::new(db::open().expect("failed to open shared db"))),
        ui: Arc::new(Mutex::new(ManagedProcess::new("wall-ui", "SKWD_WALL_INSTALL", resolve_shell_qml()))),
        bar: Arc::new(Mutex::new(ManagedProcess::new("bar", "SKWD_BAR_INSTALL", resolve_bar_qml()))),
        launcher: Arc::new(Mutex::new(ManagedProcess::new("launcher", "SKWD_LAUNCH_INSTALL", resolve_launch_qml()))),
        switch: Arc::new(Mutex::new(ManagedProcess::new("switch", "SKWD_SWITCH_INSTALL", resolve_switch_qml()))),
        notification: Arc::new(Mutex::new(ManagedProcess::new("notification", "SKWD_NOTIFICATION_INSTALL", resolve_notification_qml()))),
        current_wallpaper: Arc::new(Mutex::new(None)),
        cache_state: Arc::new(Mutex::new(CacheState::default())),
        steam_state,
        optimize_state: Arc::new(Mutex::new(OptimizeState::default())),
        convert_state: Arc::new(Mutex::new(optimize::ConvertState::default())),
        analysis_state: Arc::new(Mutex::new(AnalysisState::default())),
        suppress_set: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        random_rotation: Arc::new(Mutex::new(None)),
    };

    if should_launch_notification(&config).await {
        state.notification.lock().await.launch();
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
                        *state.config.write().await = new_cfg;
                        let _ = broadcast_event(&tx, "skwd.wall.config_changed", serde_json::json!({}));
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

                    match optimize::optimize_single_inline(&config, &db, path, name).await {
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
                let _ = broadcast_event(
                    &tx,
                    "skwd.wall.file_removed",
                    serde_json::json!({
                        "name": name,
                        "type": wp_type
                    }),
                );
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
                    let _ = broadcast_event(
                        &tx,
                        "skwd.wall.folder_removed",
                        serde_json::json!({
                            "prefix": prefix,
                            "names": deleted
                        }),
                    );
                }
            }
            watcher::FsEvent::WeAdded { we_id, we_dir } => {
                if matches!(phase, WatcherPhase::Scanning) {
                    continue;
                }
                info!("[server] watcher WeAdded we_id={we_id} dir={}", we_dir.display());
                let _ = broadcast_event(
                    &tx,
                    "skwd.wall.we_added",
                    serde_json::json!({
                        "we_id": we_id, "we_dir": we_dir.display().to_string()
                    }),
                );
                let config = state.config.read().await.clone();
                let db = state.db_shared.clone();
                cache::process_we_single(&config, db, &tx, we_id, we_dir).await;
            }
            watcher::FsEvent::WeRemoved { we_id } => {
                let _ = broadcast_event(
                    &tx,
                    "skwd.wall.we_removed",
                    serde_json::json!({
                        "we_id": we_id
                    }),
                );
            }
            watcher::FsEvent::ScanDone => {
                phase = WatcherPhase::Ready;
                let _ = broadcast_event(&tx, "skwd.wall.scan_done", serde_json::json!({}));
                info!("initial directory scan complete, starting cache rebuild");

                let config = state.config.read().await.clone();
                let db = state.db_shared.clone();
                cache::rebuild(&config, db.clone(), state.cache_state.clone(), tx.clone()).await;
                auto_optimize_if_enabled(&config, db, tx.clone(), state.optimize_state.clone()).await;

                match apply::restore(&config).await {
                    Ok(name) => info!("auto-restored wallpaper: {name}"),
                    Err(e) => info!("no wallpaper to restore: {e}"),
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
    info!("client disconnected");
    Ok(())
}

async fn dispatch_bar(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("bar.").unwrap_or(&req.method);
    match method {
        "toggle" => {
            let mut bar = state.bar.lock().await;
            bar.toggle();
            let running = bar.is_running();
            drop(bar);
            let _ = broadcast_event(event_tx, "skwd.bar.toggle", serde_json::json!({"visible": running}));
            Response::ok(req.id, serde_json::json!({"toggled": true, "visible": running}))
        }
        "show" => {
            state.bar.lock().await.launch();
            let _ = broadcast_event(event_tx, "skwd.bar.show", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "hide" => {
            state.bar.lock().await.kill();
            let _ = broadcast_event(event_tx, "skwd.bar.hide", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

async fn dispatch_launcher(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("launcher.").unwrap_or(&req.method);
    match method {
        "toggle" => {
            let mut launcher = state.launcher.lock().await;
            launcher.toggle();
            let running = launcher.is_running();
            drop(launcher);
            let _ = broadcast_event(event_tx, "skwd.launcher.toggle", serde_json::json!({"visible": running}));
            Response::ok(req.id, serde_json::json!({"toggled": true, "visible": running}))
        }
        "show" => {
            state.launcher.lock().await.launch();
            let _ = broadcast_event(event_tx, "skwd.launcher.show", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "hide" => {
            state.launcher.lock().await.kill();
            let _ = broadcast_event(event_tx, "skwd.launcher.hide", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

async fn dispatch_switch(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("switch.").unwrap_or(&req.method);
    match method {
        "open" => {
            let mut sw = state.switch.lock().await;
            if !sw.is_running() {
                sw.launch();
            } else {
                send_switch_cmd("open");
            }
            drop(sw);
            let _ = broadcast_event(event_tx, "skwd.switch.open", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "next" => {
            {
                let mut sw = state.switch.lock().await;
                if !sw.is_running() {
                    sw.launch();
                    return Response::ok(req.id, serde_json::json!({"ok": true}));
                }
            }
            send_switch_cmd("next");
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "prev" => {
            send_switch_cmd("prev");
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "confirm" => {
            send_switch_cmd("confirm");
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "cancel" => {
            send_switch_cmd("cancel");
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "close" => {
            send_switch_cmd("close");
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "hide" => {
            state.switch.lock().await.kill();
            let _ = broadcast_event(event_tx, "skwd.switch.hide", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

async fn dispatch_request(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    subscriptions: &Arc<Mutex<Vec<String>>>,
    state: &SharedState,
) -> Response {
    if req.method.starts_with("wall.") {
        return wall::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("bar.") {
        return dispatch_bar(req, event_tx, state).await;
    }
    if req.method.starts_with("launcher.") {
        return dispatch_launcher(req, event_tx, state).await;
    }
    if req.method.starts_with("switch.") {
        return dispatch_switch(req, event_tx, state).await;
    }
    if req.method.starts_with("steam.") {
        return steam::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("optimize.") || req.method.starts_with("video_convert.") {
        return optimize::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("analysis.") {
        return analysis::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("lyrics.") {
        return crate::lyrics::dispatch(req, event_tx, state).await;
    }
    match req.method.as_str() {
        "subscribe" => {
            if let Some(events) = req.params.get("events").and_then(|v| v.as_array()) {
                let mut subs = subscriptions.lock().await;
                for e in events {
                    if let Some(s) = e.as_str() {
                        let prefix = s.trim_end_matches('*').to_string();
                        if !subs.contains(&prefix) {
                            subs.push(prefix);
                        }
                    }
                }
            }
            Response::ok(req.id, serde_json::json!({"subscribed": true}))
        }

        "status" => {
            let wp = state.current_wallpaper.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "current_wallpaper": *wp,
                }),
            )
        }

        "theme.colors" => Response::ok(req.id, serde_json::json!({"colors": {}})),

        "state.get" => {
            let key = req.str_param("key", "");
            if key.is_empty() {
                return Response::err(req.id, -32602, "missing key".to_string());
            }
            let db = state.db.lock().await;
            let val: Option<String> = db
                .query_row(
                    "SELECT val FROM state WHERE key=?1",
                    params![key],
                    |r| r.get(0),
                )
                .ok();
            Response::ok(req.id, serde_json::json!({ "value": val }))
        }

        "state.set" => {
            let key = req.str_param("key", "");
            let val = req.opt_str("value");
            if key.is_empty() {
                return Response::err(req.id, -32602, "missing key".to_string());
            }
            let db = state.db.lock().await;
            match val {
                Some(v) => {
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO state(key, val) VALUES(?1, ?2)",
                        params![key, v],
                    );
                }
                None => {
                    let _ = db.execute("DELETE FROM state WHERE key=?1", params![key]);
                }
            }
            Response::ok(req.id, serde_json::json!({ "ok": true }))
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

pub fn broadcast_event(
    tx: &broadcast::Sender<String>,
    event: &str,
    data: serde_json::Value,
) -> Result<usize, broadcast::error::SendError<String>> {
    tx.send(make_event(event, data))
}

#[must_use]
pub fn make_event(event: &str, data: serde_json::Value) -> String {
    serde_json::to_string(&Event {
        event: event.to_string(),
        data,
    })
    .unwrap_or_default()
}

pub async fn auto_optimize_if_enabled(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    optimize_state: Arc<Mutex<OptimizeState>>,
) {
    if !config.performance.auto_optimize_images {
        return;
    }
    let preset = config
        .performance
        .image_optimize_preset
        .as_deref()
        .unwrap_or("balanced");
    let resolution = config.performance.image_optimize_resolution.as_deref().unwrap_or("2k");
    info!("auto-optimizing images (preset={preset}, resolution={resolution})");
    if let Err(e) = optimize::start_optimize(config, db, event_tx, optimize_state, preset, resolution).await {
        warn!("auto-optimize failed: {e}");
    }
}
