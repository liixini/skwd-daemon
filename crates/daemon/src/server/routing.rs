#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use std::sync::Arc;

use rusqlite::params;
use skwd_proto::{Request, Response};
use tokio::sync::{Mutex, broadcast};

use crate::wall::{self, analysis, optimize, steam};

use super::*;

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
        &format!("skwd.power.{cmd}"),
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
            req.params.get("state").and_then(serde_json::Value::as_bool),
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
            req.params.get("state").and_then(serde_json::Value::as_bool),
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
        .and_then(serde_json::Value::as_bool)
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
        &format!("skwd.launcher.{cmd}"),
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
    let Ok(text) = std::fs::read_to_string(&cfg_path) else {
        return false;
    };
    let root: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({}));
    root.get("dev").and_then(serde_json::Value::as_bool).unwrap_or(false)
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
        &format!("skwd.settings.{cmd}"),
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
    let _ = broadcast_event(event_tx, &format!("skwd.switch.{method}"), serde_json::json!({}));
    Response::ok(req.id, serde_json::json!({"ok": true}))
}

pub(super) async fn dispatch_request(
    req: &Request,
    event_tx: &broadcast::Sender<String>,
    subscriptions: &Arc<Mutex<Vec<String>>>,
    state: &SharedState,
) -> Response {
    if req.method == "paper.ready" {
        if let Some(pid) = req.params.get("pid").and_then(serde_json::Value::as_u64) {
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
    if req.method.starts_with("effects.") {
        return wall::effects::dispatch(req, event_tx, state).await;
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
