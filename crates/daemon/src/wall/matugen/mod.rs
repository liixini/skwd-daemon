use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::process::Command;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tracing::warn;

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::{SharedState, make_event};
use crate::util::{self, BatchJobState};

use skwd_proto::{Request, Response};

const MAX_JOBS: usize = 4;
use super::IMAGE_EXTS;

pub type MatugenState = BatchJobState;


pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    let method = req.method.strip_prefix("matugen.").unwrap_or(&req.method);
    match method {
        "start" => {
            let config = state.config.read().await.clone();
            let matugen_state = state.matugen_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = start(&config, db, tx, matugen_state).await {
                    warn!("matugen start failed: {e}");
                }
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "cancel" => {
            cancel(&state.matugen_state).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "status" => {
            let s = state.matugen_state.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "running": s.running,
                    "progress": s.progress,
                    "total": s.total,
                }),
            )
        }

        "process_one" => {
            let path = req.str_param("path", "");
            let key = req.str_param("key", "");
            if path.is_empty() || key.is_empty() {
                return Response::err(req.id, 1, "missing 'path' or 'key'");
            }
            match process_one(path, key, &state.db_shared, event_tx).await {
                Ok(colors) => Response::ok(req.id, serde_json::json!({"key": key, "colors": colors})),
                Err(e) => Response::err(req.id, 8, format!("{e:#}")),
            }
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}


pub async fn start(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    state: Arc<Mutex<MatugenState>>,
) -> anyhow::Result<()> {
    {
        let s = state.lock().await;
        if s.running {
            bail!("already running");
        }
    }

    let wall_dir = config.wallpaper_dir();
    let cache_dir = config.cache_dir().join("wallpaper");
    let thumbs_dir = cache_dir.join("thumbs");
    let we_thumbs_dir = cache_dir.join("we-thumbs");

    let existing: HashSet<String> = {
        let conn = db.lock().await;
        load_existing_cache(&conn)
    };

    let mut work_items: Vec<(String, String)> = Vec::new();

    collect_images(&wall_dir, &mut work_items, &existing).await;

    collect_thumbs(&we_thumbs_dir, &mut work_items, &existing).await;

    collect_thumbs(&thumbs_dir, &mut work_items, &existing).await;

    let mut seen = HashSet::new();
    work_items.retain(|(_, key)| seen.insert(key.clone()));

    let total = work_items.len();

    if work_items.is_empty() {
        let _ = event_tx.send(make_event("skwd.wall.matugen.ready", serde_json::json!({})));
        return Ok(());
    }

    {
        let mut s = state.lock().await;
        s.running = true;
        s.cancel = false;
        s.progress = 0;
        s.total = total;
    }

    broadcast_progress(&event_tx, &state).await;

    let sem = Arc::new(Semaphore::new(MAX_JOBS));
    let mut handles = Vec::new();
    let unsaved: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    for (path, key) in work_items {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let event_tx = event_tx.clone();
        let state = state.clone();
        let unsaved = unsaved.clone();

        let handle = tokio::spawn(async move {
            {
                let s = state.lock().await;
                if s.cancel {
                    drop(permit);
                    return;
                }
            }

            match run_matugen(&path).await {
                Ok(colors) => {
                    let json = serde_json::to_string(&colors).unwrap_or_else(|_| "{}".into());
                    unsaved.lock().await.push((key.clone(), json));

                    let _ = event_tx.send(make_event(
                        "skwd.wall.matugen.one",
                        serde_json::json!({
                            "key": key, "colors": colors,
                        }),
                    ));
                }
                Err(e) => {
                    warn!("matugen failed for {}: {e}", key);
                }
            }

            {
                let mut s = state.lock().await;
                s.progress += 1;
            }
            broadcast_progress(&event_tx, &state).await;
            drop(permit);
        });

        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let batch = std::mem::take(&mut *unsaved.lock().await);
    if !batch.is_empty() {
        let conn = db.lock().await;
        let _ = db::set_matugen_batch(&conn, &batch);
    }

    {
        let mut s = state.lock().await;
        s.running = false;
    }

    let _ = event_tx.send(make_event("skwd.wall.matugen.ready", serde_json::json!({})));

    Ok(())
}

pub async fn cancel(state: &Arc<Mutex<MatugenState>>) {
    let mut s = state.lock().await;
    s.cancel = true;
}

pub async fn process_one(
    path: &str,
    key: &str,
    db: &Arc<Mutex<Connection>>,
    event_tx: &broadcast::Sender<String>,
) -> anyhow::Result<serde_json::Value> {
    let colors = run_matugen(path).await?;
    let json = serde_json::to_string(&colors).unwrap_or_else(|_| "{}".into());

    {
        let conn = db.lock().await;
        let _ = db::set_matugen(&conn, key, &json);
    }

    let _ = event_tx.send(make_event(
        "skwd.wall.matugen.one",
        serde_json::json!({
            "key": key, "colors": colors,
        }),
    ));

    Ok(colors)
}


async fn run_matugen(path: &str) -> anyhow::Result<serde_json::Value> {
    let mut cmd = Command::new("matugen");
    cmd.args(["image", path, "--dry-run", "--json", "hex", "--source-color-index", "0"])
        .stderr(std::process::Stdio::piped());
    let output = util::timed_output(&mut cmd, util::CMD_TIMEOUT).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("matugen failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).map_err(|e| anyhow::anyhow!("parse matugen json: {e}"))?;

    extract_palette(&parsed)
}

fn extract_palette(data: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let colors = data
        .get("colors")
        .ok_or_else(|| anyhow::anyhow!("missing 'colors' in matugen output"))?;

    let mut result = serde_json::Map::new();

    if let Some(obj) = colors.as_object() {
        for (key, variants) in obj {
            let camel = snake_to_camel(key);
            let hex = variants
                .get("dark")
                .or_else(|| variants.get("default"))
                .and_then(|v| v.as_str())
                .unwrap_or("#888888");
            result.insert(camel, serde_json::Value::String(hex.to_string()));
        }
    }

    Ok(serde_json::Value::Object(result))
}

fn snake_to_camel(s: &str) -> String {
    let mut parts = s.split('_');
    let first = parts.next().unwrap_or_default();
    parts.fold(first.to_string(), |mut acc, part| {
        let mut chars = part.chars();
        if let Some(c) = chars.next() {
            acc.push(c.to_ascii_uppercase());
        }
        acc.extend(chars);
        acc
    })
}


fn load_existing_cache(conn: &Connection) -> HashSet<String> {
    let Ok(mut stmt) = conn.prepare("SELECT key FROM meta WHERE matugen IS NOT NULL") else {
        return HashSet::new();
    };
    stmt.query_map([], |row| row.get::<_, String>(0))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .collect()
}

async fn collect_images(dir: &Path, items: &mut Vec<(String, String)>, existing: &HashSet<String>) {
    for path_str in util::scan_dir_by_ext(dir, IMAGE_EXTS).await {
        let path = std::path::Path::new(&path_str);
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let key = format!("static:{name}");
        if !existing.contains(&key) {
            items.push((path_str, key));
        }
    }
}

async fn collect_thumbs(dir: &Path, items: &mut Vec<(String, String)>, existing: &HashSet<String>) {
    let thumb_exts: &[&str] = &["webp", "jpg", "jpeg", "png"];
    let parent_name = dir.file_name().unwrap_or_default().to_string_lossy().to_string();
    let wp_type = match parent_name.as_str() {
        "we-thumbs" => "we",
        "video-thumbs" => "video",
        _ => "static",
    };
    for path_str in util::scan_dir_by_ext(dir, thumb_exts).await {
        let stem = std::path::Path::new(&path_str)
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let key = format!("{wp_type}:{stem}");
        if !existing.contains(&key) {
            items.push((path_str, key));
        }
    }
}

async fn broadcast_progress(tx: &broadcast::Sender<String>, state: &Arc<Mutex<MatugenState>>) {
    let s = state.lock().await;
    let _ = tx.send(make_event(
        "skwd.wall.matugen.progress",
        serde_json::json!({
            "running": s.running,
            "progress": s.progress,
            "total": s.total,
        }),
    ));
}
