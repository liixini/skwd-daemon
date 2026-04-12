use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::server::{SharedState, make_event};

use skwd_proto::{Request, Response};

const STEAM_APP_ID: &str = "431960";


pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    let method = req.method.strip_prefix("steam.").unwrap_or(&req.method);
    match method {
        "download" => {
            let workshop_id = req.str_param("id", "");
            if workshop_id.is_empty() {
                return Response::err(req.id, 1, "missing 'id' parameter");
            }
            let ok = request_download(&state.steam_state, event_tx, workshop_id).await;
            if ok {
                Response::ok(req.id, serde_json::json!({"queued": workshop_id}))
            } else {
                Response::err(req.id, 7, "invalid or already queued")
            }
        }

        "retry" => {
            retry_auth_failed(&state.steam_state, event_tx).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "status" => {
            let st = state.steam_state.lock().await;
            Response::ok(req.id, st.to_status_json())
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}


pub struct SteamState {
    pub downloads: HashMap<String, DownloadEntry>,
    pub active_id: String,
    pub active_message: String,
    pub queue_length: usize,
    pub auth_paused: bool,
    queue: Vec<String>,
    batch_running: bool,
    failed_auth: Vec<String>,
    steam_dir: PathBuf,
    username: String,
    status_path: PathBuf,
}

#[derive(Clone, serde::Serialize)]
pub struct DownloadEntry {
    pub status: String,
    pub progress: f64,
}

impl SteamState {
    pub fn new(config: &Config) -> Self {
        let steam_dir = config
            .we_dir()
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .unwrap_or(&PathBuf::from(""))
            .to_path_buf();

        Self {
            downloads: HashMap::new(),
            active_id: String::new(),
            active_message: String::new(),
            queue_length: 0,
            auth_paused: false,
            queue: Vec::new(),
            batch_running: false,
            failed_auth: Vec::new(),
            steam_dir,
            username: config.steam_username().to_string(),
            status_path: config.cache_dir().join("wallpaper/steam-dl-status.json"),
        }
    }

    fn to_status_json(&self) -> serde_json::Value {
        let mut downloads = serde_json::Map::new();
        for (id, entry) in &self.downloads {
            downloads.insert(
                id.clone(),
                serde_json::json!({"status": entry.status, "progress": entry.progress}),
            );
        }
        serde_json::json!({
            "downloads": downloads,
            "activeId": self.active_id,
            "activeMessage": self.active_message,
            "queueLength": self.queue_length,
            "authPaused": self.auth_paused,
            "authFailedCount": self.failed_auth.len(),
        })
    }
}


pub async fn recover_queue(state: &Arc<Mutex<SteamState>>) {
    let status_path = { state.lock().await.status_path.clone() };
    let Ok(t) = tokio::fs::read_to_string(&status_path).await else {
        return;
    };
    let Ok(obj) = serde_json::from_str::<serde_json::Value>(&t) else {
        return;
    };
    let Some(downloads) = obj.get("downloads").and_then(|v| v.as_object()) else {
        return;
    };

    let mut to_queue = Vec::new();
    for (id, entry) in downloads {
        if let Some(status) = entry.get("status").and_then(|s| s.as_str())
            && matches!(status, "queued" | "downloading" | "auth_error")
        {
            to_queue.push(id.clone());
        }
    }

    if !to_queue.is_empty() {
        info!(
            "steam: recovering {} incomplete downloads from status file",
            to_queue.len()
        );
        let mut st = state.lock().await;
        for id in &to_queue {
            if st.downloads.contains_key(id) {
                continue;
            }
            st.downloads.insert(
                id.clone(),
                DownloadEntry {
                    status: "queued".into(),
                    progress: 0.0,
                },
            );
            st.queue.push(id.clone());
        }
        st.queue_length = st.queue.len();
    }
}

pub async fn request_download(
    state: &Arc<Mutex<SteamState>>,
    event_tx: &broadcast::Sender<String>,
    workshop_id: &str,
) -> bool {
    let safe_id: String = workshop_id.chars().filter(char::is_ascii_digit).collect();
    if safe_id.is_empty() {
        return false;
    }

    let should_drain = {
        let mut st = state.lock().await;
        if st.downloads.contains_key(&safe_id) {
            return false;
        }
        info!("steam: queuing download {safe_id}");
        st.downloads.insert(
            safe_id.clone(),
            DownloadEntry {
                status: "queued".into(),
                progress: 0.0,
            },
        );
        st.queue.push(safe_id);
        st.queue_length = st.queue.len();
        write_status(&st).await;
        !st.batch_running
    };

    if should_drain {
        drain_queue(state, event_tx).await;
    }
    true
}

pub async fn retry_auth_failed(state: &Arc<Mutex<SteamState>>, event_tx: &broadcast::Sender<String>) {
    let should_drain = {
        let mut st = state.lock().await;
        if st.failed_auth.is_empty() {
            return;
        }
        info!("steam: retrying {} auth-failed downloads", st.failed_auth.len());
        st.auth_paused = false;
        let ids: Vec<String> = st.failed_auth.drain(..).collect();
        for id in ids {
            st.downloads.insert(
                id.clone(),
                DownloadEntry {
                    status: "queued".into(),
                    progress: 0.0,
                },
            );
            st.queue.push(id);
        }
        st.queue_length = st.queue.len();
        write_status(&st).await;
        !st.batch_running
    };

    if should_drain {
        drain_queue(state, event_tx).await;
    }
}

async fn drain_queue(state: &Arc<Mutex<SteamState>>, event_tx: &broadcast::Sender<String>) {
    let batch = {
        let mut st = state.lock().await;
        if st.batch_running || st.queue.is_empty() {
            return;
        }
        st.batch_running = true;
        std::mem::take(&mut st.queue)
    };
    spawn_batch(state.clone(), event_tx.clone(), batch).await;
}

async fn spawn_batch(state: Arc<Mutex<SteamState>>, event_tx: broadcast::Sender<String>, ids: Vec<String>) {
    let (steam_dir, username) = {
        let mut st = state.lock().await;
        st.active_id = ids[0].clone();
        st.active_message = "Starting steamcmd...".into();
        if let Some(entry) = st.downloads.get_mut(&ids[0]) {
            entry.status = "downloading".into();
        }
        st.queue_length = ids.len();
        write_status(&st).await;
        (st.steam_dir.clone(), st.username.clone())
    };

    info!("steam: spawning batch of {} items: {}", ids.len(), ids.join(", "));

    let mut cmd = Command::new("steamcmd");
    if !steam_dir.as_os_str().is_empty() {
        cmd.arg("+force_install_dir").arg(&steam_dir);
    }
    cmd.arg("+login").arg(&username);
    for id in &ids {
        cmd.arg("+workshop_download_item").arg(STEAM_APP_ID).arg(id);
    }
    cmd.arg("+quit");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!("steam: failed to spawn steamcmd: {e}");
            let mut st = state.lock().await;
            for id in &ids {
                if let Some(entry) = st.downloads.get_mut(id) {
                    entry.status = "error".into();
                }
            }
            st.batch_running = false;
            st.active_id.clear();
            st.active_message.clear();
            st.queue_length = 0;
            write_status(&st).await;
            return;
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let state_c = state.clone();
    let event_tx_c = event_tx.clone();
    let ids_c = ids.clone();
    let stdout_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        let mut current_id = ids_c[0].clone();
        let mut done_ids: HashSet<String> = HashSet::new();
        let mut credential_error = false;

        while let Ok(Some(line)) = reader.next_line().await {
            debug!("steam stdout: {line}");

            if line.contains("Downloading item")
                && let Some(id) = extract_workshop_id(&line, &ids_c, &done_ids)
            {
                current_id = id.clone();
                let mut st = state_c.lock().await;
                st.active_id = id.clone();
                st.active_message = "Downloading workshop item...".into();
                if let Some(entry) = st.downloads.get_mut(&id) {
                    entry.status = "downloading".into();
                }
                write_status(&st).await;
                broadcast_steam_event(&event_tx_c, &st);
            }

            if line.contains("Success") || line.contains("fully installed") {
                let success_id = extract_workshop_id(&line, &ids_c, &done_ids).unwrap_or_else(|| current_id.clone());
                if done_ids.insert(success_id.clone()) {
                    let mut st = state_c.lock().await;
                    if let Some(entry) = st.downloads.get_mut(&success_id) {
                        entry.status = "done".into();
                        entry.progress = 1.0;
                    }
                    st.active_message = "Download complete".into();
                    write_status(&st).await;
                    broadcast_steam_event(&event_tx_c, &st);
                    if let Some(next) = ids_c.iter().find(|id| !done_ids.contains(*id)) {
                        current_id = next.clone();
                    }
                }
            } else if let Some(pct) = extract_percent(&line) {
                let mut st = state_c.lock().await;
                if let Some(entry) = st.downloads.get_mut(&current_id) {
                    entry.progress = pct;
                }
                st.active_message = format!("Downloading {}%", (pct * 100.0).round());
                write_status(&st).await;
                broadcast_steam_event(&event_tx_c, &st);
            }

            if line.contains("Cached credentials not found") || line.contains("Login Failure") {
                credential_error = true;
                let mut st = state_c.lock().await;
                st.auth_paused = true;
                st.active_message = format!("Steam login required. Run: steamcmd +login {} +quit", st.username);
                write_status(&st).await;
                broadcast_steam_event(&event_tx_c, &st);
            }

            if line.contains("Checking for available update") {
                let mut st = state_c.lock().await;
                st.active_message = "Checking for updates...".into();
                broadcast_steam_event(&event_tx_c, &st);
            } else if line.contains("Loading Steam API") {
                let mut st = state_c.lock().await;
                st.active_message = "Connecting to Steam...".into();
                broadcast_steam_event(&event_tx_c, &st);
            } else if line.contains("Logging in") || line.contains("Waiting for user info") {
                let mut st = state_c.lock().await;
                st.active_message = "Logging in...".into();
                broadcast_steam_event(&event_tx_c, &st);
            }
        }
        (done_ids, credential_error)
    });

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            debug!("steam stderr: {line}");
        }
    });

    let exit_status = child.wait().await;
    let (done_ids, _credential_error) = stdout_task.await.unwrap_or_default();
    stderr_task.await.ok();

    let exit_code = exit_status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
    info!(
        "steam: batch exited code {exit_code}, {}/{} completed",
        done_ids.len(),
        ids.len()
    );

    let mut st = state.lock().await;
    let auth_paused = st.auth_paused;
    for id in &ids {
        if let Some(entry) = st.downloads.get_mut(id)
            && (entry.status == "downloading" || entry.status == "queued")
        {
            if auth_paused {
                entry.status = "auth_error".into();
                st.failed_auth.push(id.clone());
            } else {
                entry.status = "error".into();
            }
        }
    }
    st.batch_running = false;

    if st.auth_paused {
        let remaining: Vec<String> = st.queue.drain(..).collect();
        for id in remaining {
            if let Some(entry) = st.downloads.get_mut(&id) {
                entry.status = "auth_error".into();
            }
            st.failed_auth.push(id);
        }
        st.queue_length = 0;
    } else if !st.queue.is_empty() {
        let next_batch: Vec<String> = std::mem::take(&mut st.queue);
        st.batch_running = true;
        st.queue_length = next_batch.len();
        write_status(&st).await;
        broadcast_steam_event(&event_tx, &st);
        drop(st);
        Box::pin(spawn_batch(state.clone(), event_tx.clone(), next_batch)).await;
        return;
    } else {
        st.active_id.clear();
        st.active_message.clear();
        st.queue_length = 0;
    }

    write_status(&st).await;
    broadcast_steam_event(&event_tx, &st);
}


fn extract_workshop_id(line: &str, ids: &[String], done: &HashSet<String>) -> Option<String> {
    line.split(|c: char| !c.is_ascii_digit())
        .filter(|s| s.len() >= 7)
        .rev()
        .map(String::from)
        .find(|s| ids.contains(s) && !done.contains(s))
}

fn extract_percent(line: &str) -> Option<f64> {
    let pct = line.find('%')?;
    let before = line[..pct].trim_end();
    let start = before
        .rfind(|c: char| !c.is_ascii_digit() && c != '.')
        .map_or(0, |i| i + 1);
    before[start..].parse::<f64>().ok().map(|v| v / 100.0)
}

async fn write_status(st: &SteamState) {
    let json = st.to_status_json();
    let text = serde_json::to_string_pretty(&json).unwrap_or_default();
    tokio::fs::write(&st.status_path, text.as_bytes()).await.ok();
}

fn broadcast_steam_event(tx: &broadcast::Sender<String>, st: &SteamState) {
    let evt = make_event("skwd.wall.steam.status", st.to_status_json());
    let _ = tx.send(evt);
}
