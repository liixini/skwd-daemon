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
        self.launch_with_env(&[]);
    }

    pub fn launch_with_env(&mut self, extra_env: &[(&str, String)]) {
        if self.is_running() {
            return;
        }
        let _ = std::process::Command::new("pkill")
            .arg("-f")
            .arg(format!("quickshell .*{}", self.shell_qml.display()))
            .status();
        info!("launching {}: quickshell -p {}", self.label, self.shell_qml.display());
        let install_dir = self.shell_qml.parent().unwrap_or(Path::new("/usr/share/skwd-wall"));
        let mut cmd = tokio::process::Command::new("quickshell");
        cmd.arg("-p").arg(&self.shell_qml).env(self.env_key, install_dir);
        cmd.kill_on_drop(true);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        match cmd.silent().spawn() {
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
        self.toggle_with_env(&[]);
    }

    pub fn toggle_with_env(&mut self, extra_env: &[(&str, String)]) {
        if self.is_running() {
            self.kill();
        } else {
            self.launch_with_env(extra_env);
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
    resolve_dev_or_system("skwd-bar", "SKWD_BAR_QML")
}


fn resolve_dev_or_system(name: &str, env_var: &str) -> PathBuf {
    if let Ok(p) = std::env::var(env_var) {
        return PathBuf::from(p);
    }
    if let Ok(install) = std::env::var("SKWD_INSTALL") {
        let p = PathBuf::from(install).join(name).join("shell.qml");
        if p.exists() {
            return p;
        }
    }
    let umbrella = PathBuf::from(format!("../skwd-shell/{name}/shell.qml"));
    if umbrella.exists() {
        return std::fs::canonicalize(&umbrella).unwrap_or(umbrella);
    }
    let sibling = PathBuf::from(format!("../{name}/shell.qml"));
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    let suite_path = PathBuf::from(format!("/usr/share/skwd/{name}/shell.qml"));
    if suite_path.exists() {
        return suite_path;
    }
    PathBuf::from(format!("/usr/share/{name}/shell.qml"))
}

fn resolve_launch_qml() -> PathBuf {
    resolve_dev_or_system("skwd-launch", "SKWD_LAUNCH_QML")
}

fn resolve_launch_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_LAUNCH_SHELL_QML") {
        return PathBuf::from(p);
    }
    let launch_shell_qml = resolve_launch_qml();
    let candidate = launch_shell_qml
        .parent()
        .map(|p| p.join("qml/launcher/LauncherShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-launch/qml/launcher/LauncherShell.qml")
}

fn resolve_bar_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_BAR_SHELL_QML") {
        return PathBuf::from(p);
    }
    let bar_qml = resolve_bar_qml();
    let candidate = bar_qml.parent().map(|p| p.join("qml/bar/BarShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-bar/qml/bar/BarShell.qml")
}

fn resolve_switch_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SWITCH_SHELL_QML") {
        return PathBuf::from(p);
    }
    let switch_qml = resolve_switch_qml();
    let candidate = switch_qml
        .parent()
        .map(|p| p.join("qml/switcher/SwitchShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-switch/qml/switcher/SwitchShell.qml")
}

fn install_dir_of(shell_qml: &Path) -> String {
    shell_qml
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

async fn build_host_env(config: &Config) -> Vec<(&'static str, String)> {
    let launch_shell   = resolve_launch_shell_qml();
    let bar_shell      = resolve_bar_shell_qml();
    let switch_shell   = resolve_switch_shell_qml();
    let settings_shell = resolve_settings_shell_qml();
    let power_shell    = resolve_power_shell_qml();

    let launch_install   = install_dir_of(&resolve_launch_qml());
    let bar_install      = install_dir_of(&resolve_bar_qml());
    let switch_install   = install_dir_of(&resolve_switch_qml());
    let settings_install = install_dir_of(&resolve_settings_qml());
    let power_install    = install_dir_of(&resolve_power_qml());

    let mut env: Vec<(&'static str, String)> = vec![
        ("SKWD_LAUNCH_SHELL",     launch_shell.display().to_string()),
        ("SKWD_BAR_SHELL",        bar_shell.display().to_string()),
        ("SKWD_SWITCH_SHELL",     switch_shell.display().to_string()),
        ("SKWD_SETTINGS_SHELL",   settings_shell.display().to_string()),
        ("SKWD_POWER_SHELL",      power_shell.display().to_string()),
        ("SKWD_LAUNCH_INSTALL",   launch_install),
        ("SKWD_BAR_INSTALL",      bar_install),
        ("SKWD_SWITCH_INSTALL",   switch_install),
        ("SKWD_SETTINGS_INSTALL", settings_install),
        ("SKWD_POWER_INSTALL",    power_install),
    ];
    if should_launch_notification(config).await {
        let notification_shell = resolve_notification_shell_qml();
        let notification_install = install_dir_of(&resolve_notification_qml());
        env.push(("SKWD_NOTIFICATION_SHELL", notification_shell.display().to_string()));
        env.push(("SKWD_NOTIFICATION_INSTALL", notification_install));
    }
    env
}

fn resolve_notification_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_NOTIFICATION_SHELL_QML") {
        return PathBuf::from(p);
    }
    let notification_qml = resolve_notification_qml();
    let candidate = notification_qml
        .parent()
        .map(|p| p.join("qml/NotificationShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-notification/qml/NotificationShell.qml")
}

fn resolve_power_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_POWER_SHELL_QML") {
        return PathBuf::from(p);
    }
    let power_qml = resolve_power_qml();
    let candidate = power_qml
        .parent()
        .map(|p| p.join("qml/power/PowerShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-power/qml/power/PowerShell.qml")
}

fn resolve_settings_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SETTINGS_SHELL_QML") {
        return PathBuf::from(p);
    }
    let settings_qml = resolve_settings_qml();
    let candidate = settings_qml
        .parent()
        .map(|p| p.join("qml/SettingsShell.qml"));
    if let Some(p) = candidate {
        if p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-settings/qml/SettingsShell.qml")
}

fn resolve_host_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_HOST_QML") {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("data/host/shell.qml");
    if local.exists() {
        return std::fs::canonicalize(&local).unwrap_or(local);
    }
    let sibling = PathBuf::from("../skwd-daemon/data/host/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    if let Ok(install) = std::env::var("SKWD_INSTALL") {
        let p = PathBuf::from(install).join("skwd-daemon/host/shell.qml");
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-daemon/host/shell.qml")
}

fn resolve_switch_qml() -> PathBuf {
    resolve_dev_or_system("skwd-switch", "SKWD_SWITCH_QML")
}

fn resolve_notification_qml() -> PathBuf {
    resolve_dev_or_system("skwd-notification", "SKWD_NOTIFICATION_QML")
}

fn resolve_music_qml() -> PathBuf {
    resolve_dev_or_system("skwd-music", "SKWD_MUSIC_QML")
}

fn resolve_power_qml() -> PathBuf {
    resolve_dev_or_system("skwd-power", "SKWD_POWER_QML")
}

fn resolve_settings_qml() -> PathBuf {
    resolve_dev_or_system("skwd-settings", "SKWD_SETTINGS_QML")
}

async fn start_music_module(state: &SharedState) {
    use crate::music::mpris;
    {
        let g = state.music.mpris_server.lock().await;
        if g.is_some() {
            return;
        }
    }
    let music_state = state.music.clone();
    let mpris_state = music_state.mpris_state.clone();
    let cmd_tx = music_state.mpris_cmd_tx.clone();
    let server = match mpris::launch(mpris_state, cmd_tx).await {
        Ok(s) => s,
        Err(e) => {
            warn!("music: MPRIS server failed to launch: {e:#}");
            return;
        }
    };
    info!("music: MPRIS server started at org.mpris.MediaPlayer2.skwd-music");

    let server_for_events = server.clone();
    let ms_for_events = music_state.clone();
    tokio::spawn(async move {
        let mut sub = ms_for_events.event_tx.subscribe();
        while let Ok(_payload) = sub.recv().await {
            let _ = mpris::emit_state_update(&server_for_events).await;
        }
    });

    let cmd_rx_slot = music_state.mpris_cmd_rx.clone();
    let player = music_state.player.clone();
    let ms_for_cmds = music_state.clone();
    tokio::spawn(async move {
        let mut rx = match cmd_rx_slot.lock().await.take() {
            Some(r) => r,
            None => return,
        };
        while let Some(cmd) = rx.recv().await {
            match cmd {
                mpris::MprisCommand::PlayPause => {
                    let playing = ms_for_cmds.mpris_state.lock().await.playing;
                    if playing { let _ = player.pause().await; }
                    else { let _ = player.play().await; }
                }
                mpris::MprisCommand::Play => { let _ = player.play().await; }
                mpris::MprisCommand::Pause => { let _ = player.pause().await; }
                mpris::MprisCommand::Next => { let _ = player.next().await; }
                mpris::MprisCommand::Previous => { let _ = player.previous().await; }
                mpris::MprisCommand::SetVolume(v) => {
                    let scaled = (v.clamp(0.0, 1.0) * 65535.0) as u16;
                    let _ = player.set_volume(scaled).await;
                }
            }
        }
    });

    *state.music.mpris_server.lock().await = Some(server);
}

async fn stop_music_module(state: &SharedState) {
    state.music.player.shutdown().await;
    state.music.session.disconnect().await;
    let server = state.music.mpris_server.lock().await.take();
    if let Some(s) = server {
        let _ = s.release_bus_name().await;
        info!("music: MPRIS server released");
        drop(s);
    }
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
    pub host: Arc<Mutex<ManagedProcess>>,
    pub music_proc: Arc<Mutex<ManagedProcess>>,
    pub current_wallpaper: Arc<Mutex<Option<String>>>,
    pub cache_state: Arc<Mutex<CacheState>>,
    pub steam_state: Arc<Mutex<SteamState>>,
    pub optimize_state: Arc<Mutex<OptimizeState>>,
    pub convert_state: Arc<Mutex<optimize::ConvertState>>,
    pub analysis_state: Arc<Mutex<AnalysisState>>,
    pub suppress_set: SuppressSet,
    pub random_rotation: Arc<Mutex<Option<RandomRotation>>>,
    pub music: Arc<crate::music::MusicState>,
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
    };
    state.music.auth.load_from_disk().await;

    if config.features.music {
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

async fn dispatch_power(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("power.").unwrap_or(&req.method);
    let cmd = match method {
        "toggle" | "show" | "hide" => method,
        _ => return Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    };

    {
        let mut host = state.host.lock().await;
        if !host.is_running() {
            let config = state.config.read().await.clone();
            let extra_env = build_host_env(&config).await;
            host.launch_with_env(&extra_env);
        }
    }

    let _ = broadcast_event(
        event_tx,
        &format!("skwd.power.{}", cmd),
        serde_json::json!({}),
    );
    Response::ok(req.id, serde_json::json!({"ok": true}))
}

async fn dispatch_bar(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    _state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("bar.").unwrap_or(&req.method);
    match method {
        "toggle" => {
            let _ = broadcast_event(event_tx, "skwd.bar.toggle", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "show" => {
            let _ = broadcast_event(event_tx, "skwd.bar.show", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "hide" => {
            let _ = broadcast_event(event_tx, "skwd.bar.hide", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }
        "mouseover" => match toggle_shell_path_bool(
            &["components", "bar", "mouseoverEnabled"],
            req.params.get("state").and_then(|v| v.as_bool()),
            true,
        ) {
            Ok(new_state) => {
                let _ = broadcast_event(
                    event_tx,
                    "skwd.bar.mouseover",
                    serde_json::json!({ "enabled": new_state }),
                );
                Response::ok(req.id, serde_json::json!({ "enabled": new_state }))
            }
            Err(e) => Response::err(req.id, -32603, format!("config update failed: {e}")),
        },
        "visualizer.clean" => match toggle_shell_path_bool(
            &["components", "bar", "music", "cleanVisualizer"],
            req.params.get("state").and_then(|v| v.as_bool()),
            false,
        ) {
            Ok(new_state) => {
                let _ = broadcast_event(
                    event_tx,
                    "skwd.bar.visualizer.clean",
                    serde_json::json!({ "enabled": new_state }),
                );
                Response::ok(req.id, serde_json::json!({ "enabled": new_state }))
            }
            Err(e) => Response::err(req.id, -32603, format!("config update failed: {e}")),
        },
        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

fn toggle_shell_path_bool(
    path: &[&str],
    explicit: Option<bool>,
    default_when_missing: bool,
) -> anyhow::Result<bool> {
    let cfg_path = crate::config::shell_config_path();
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut root: serde_json::Value = if cfg_path.exists() {
        let text = std::fs::read_to_string(&cfg_path)?;
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    let current = path
        .iter()
        .try_fold(&root, |acc, k| acc.get(*k))
        .and_then(|v| v.as_bool())
        .unwrap_or(default_when_missing);

    let next = explicit.unwrap_or(!current);

    let mut node = &mut root;
    for (i, key) in path.iter().enumerate() {
        if i == path.len() - 1 {
            if let serde_json::Value::Object(map) = node {
                map.insert((*key).to_string(), serde_json::Value::Bool(next));
            }
        } else {
            if !node.is_object() {
                *node = serde_json::Value::Object(serde_json::Map::new());
            }
            let map = node.as_object_mut().unwrap();
            if !map.contains_key(*key) {
                map.insert((*key).to_string(), serde_json::json!({}));
            }
            node = map.get_mut(*key).unwrap();
        }
    }

    let serialized = serde_json::to_string_pretty(&root)?;
    std::fs::write(&cfg_path, serialized + "\n")?;
    Ok(next)
}

async fn dispatch_launcher(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("launcher.").unwrap_or(&req.method);
    let cmd = match method {
        "toggle" | "show" | "hide" => method,
        _ => return Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    };

    {
        let mut host = state.host.lock().await;
        if !host.is_running() {
            let config = state.config.read().await.clone();
            let extra_env = build_host_env(&config).await;
            host.launch_with_env(&extra_env);
        }
    }

    let _ = broadcast_event(
        event_tx,
        &format!("skwd.launcher.{}", cmd),
        serde_json::json!({}),
    );
    Response::ok(req.id, serde_json::json!({"ok": true}))
}

async fn dispatch_dev(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
) -> Response {
    let action = req
        .method
        .strip_prefix("dev.")
        .unwrap_or("toggle");
    let explicit = match action {
        "enable" | "on"  => Some(true),
        "disable" | "off" => Some(false),
        "toggle"          => None,
        "status"          => Some(read_dev_flag()),
        other => return Response::err(req.id, -32601, format!("unknown dev action: {other}")),
    };
    if action == "status" {
        return Response::ok(req.id, serde_json::json!({ "enabled": explicit.unwrap_or(false) }));
    }
    match toggle_shell_path_bool(&["dev"], explicit, false) {
        Ok(new_state) => {
            let _ = broadcast_event(
                event_tx,
                "skwd.dev",
                serde_json::json!({ "enabled": new_state }),
            );
            Response::ok(req.id, serde_json::json!({ "enabled": new_state }))
        }
        Err(e) => Response::err(req.id, -32603, format!("config update failed: {e}")),
    }
}

fn read_dev_flag() -> bool {
    let cfg_path = crate::config::shell_config_path();
    if !cfg_path.exists() {
        return false;
    }
    let text = match std::fs::read_to_string(&cfg_path) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let root: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({}));
    root.get("dev").and_then(|v| v.as_bool()).unwrap_or(false)
}

async fn dispatch_settings(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("settings.").unwrap_or(&req.method);
    let cmd = match method {
        "toggle" | "show" | "hide" => method,
        _ => return Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    };

    {
        let mut host = state.host.lock().await;
        if !host.is_running() {
            let config = state.config.read().await.clone();
            let extra_env = build_host_env(&config).await;
            host.launch_with_env(&extra_env);
        }
    }

    let _ = broadcast_event(
        event_tx,
        &format!("skwd.settings.{}", cmd),
        serde_json::json!({}),
    );
    Response::ok(req.id, serde_json::json!({"ok": true}))
}

async fn dispatch_switch(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    _state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("switch.").unwrap_or(&req.method);
    match method {
        "open" | "next" | "prev" | "confirm" | "cancel" | "close" | "hide" => {}
        _ => return Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    };
    let _ = broadcast_event(event_tx, &format!("skwd.switch.{}", method), serde_json::json!({}));
    Response::ok(req.id, serde_json::json!({"ok": true}))
}

async fn dispatch_request(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    subscriptions: &Arc<Mutex<Vec<String>>>,
    state: &SharedState,
) -> Response {
    if req.method == "paper.ready" {
        if let Some(pid) = req.params.get("pid").and_then(|v| v.as_u64()) {
            wall::apply::signal_paper_ready(pid as u32).await;
        }
        return Response::ok(req.id, serde_json::json!({"ok": true}));
    }
    if req.method.starts_with("wall.") {
        return wall::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("bar.") {
        return dispatch_bar(req, event_tx, state).await;
    }
    if req.method.starts_with("launcher.") {
        return dispatch_launcher(req, event_tx, state).await;
    }
    if req.method.starts_with("settings.") {
        return dispatch_settings(req, event_tx, state).await;
    }
    if req.method.starts_with("dev.") || req.method == "dev" {
        return dispatch_dev(req, event_tx).await;
    }
    if req.method.starts_with("switch.") {
        return dispatch_switch(req, event_tx, state).await;
    }
    if req.method.starts_with("power.") {
        return dispatch_power(req, event_tx, state).await;
    }
    if req.method.starts_with("steam.") {
        if !state.config.read().await.features.steam {
            return Response::err(req.id, -32601, "steam module is disabled");
        }
        return steam::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("optimize.") || req.method.starts_with("video_convert.") {
        return optimize::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("analysis.") {
        if !state.config.read().await.features.analysis {
            return Response::err(req.id, -32601, "analysis module is disabled");
        }
        return analysis::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("lyrics.") {
        if !state.config.read().await.features.lyrics {
            return Response::err(req.id, -32601, "lyrics module is disabled");
        }
        return crate::lyrics::dispatch(req, event_tx, state).await;
    }
    if req.method.starts_with("music.") {
        if !state.config.read().await.features.music {
            return Response::err(req.id, -32601, "music module is disabled");
        }
        return crate::music::dispatch(req, event_tx, state).await;
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
