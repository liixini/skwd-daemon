use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::warn;

use super::{apply, cache};
use crate::server::{RandomRotation, SharedState, broadcast_event};

pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    let method = req.method.strip_prefix("wall.").unwrap_or(&req.method);
    match method {
        "toggle" => {
            let mut ui = state.ui.lock().await;
            ui.toggle();
            let running = ui.is_running();
            drop(ui);
            let _ = broadcast_event(event_tx, "skwd.wall.toggle", serde_json::json!({"visible": running}));
            Response::ok(req.id, serde_json::json!({"toggled": true, "visible": running}))
        }

        "show" => {
            state.ui.lock().await.launch();
            let _ = broadcast_event(event_tx, "skwd.wall.show", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "hide" => {
            state.ui.lock().await.kill();
            let _ = broadcast_event(event_tx, "skwd.wall.hide", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "list" => {
            let fav_only = req.bool_param("favourites", false);
            let db = state.db.lock().await;
            match crate::db::list_wallpapers(&db, fav_only) {
                Ok(walls) => Response::ok(
                    req.id,
                    serde_json::json!({
                        "count": walls.len(),
                        "wallpapers": walls,
                    }),
                ),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
        }

        "import" => {
            let db = state.db.lock().await;
            match crate::db::import_from_qml(&db) {
                Ok(count) => Response::ok(
                    req.id,
                    serde_json::json!({
                        "imported": count,
                        "db_path": crate::db::db_path().display().to_string(),
                    }),
                ),
                Err(e) => Response::err(req.id, 3, format!("{e}")),
            }
        }

        "apply" => {
            let path = req.str_param("path", "");
            let we_id = req.str_param("we_id", "");
            let wp_type = req.str_param("type", "static");
            let outputs: Vec<String> = req
                .params
                .get("outputs")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                .unwrap_or_default();

            let config = state.config.read().await.clone();

            let result = match wp_type {
                "static" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_static(path, &outputs, &config).await
                }
                "video" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_video(path, &outputs, &config).await
                }
                "we" => {
                    if we_id.is_empty() {
                        return Response::err(req.id, 1, "missing 'we_id' parameter");
                    }
                    let screens: Vec<String> = req
                        .params
                        .get("screens")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                        .unwrap_or_default();
                    apply::apply_we(we_id, &screens, &config).await
                }
                _ => Err(anyhow::anyhow!("unknown type: {wp_type}")),
            };

            match result {
                Ok(()) => {
                    let name = if !path.is_empty() {
                        path.rsplit('/').next().unwrap_or(path).to_string()
                    } else {
                        we_id.to_string()
                    };
                    *state.current_wallpaper.lock().await = Some(name.clone());
                    let _ = broadcast_event(
                        event_tx,
                        "skwd.wall.applied",
                        serde_json::json!({"type": wp_type, "name": &name, "path": path, "we_id": we_id}),
                    );
                    Response::ok(req.id, serde_json::json!({"applied": name}))
                }
                Err(e) => Response::err(req.id, 4, format!("{e}")),
            }
        }

        "restore" => {
            let config = state.config.read().await.clone();
            match apply::restore(&config).await {
                Ok(name) => {
                    *state.current_wallpaper.lock().await = Some(name.clone());
                    Response::ok(req.id, serde_json::json!({"restored": name}))
                }
                Err(e) => Response::err(req.id, 5, format!("{e}")),
            }
        }

        "retheme" => {
            let scheme = req.params.get("scheme").and_then(|v| v.as_str()).map(String::from);
            let mode = req.params.get("mode").and_then(|v| v.as_str()).map(String::from);
            let config = state.config.read().await.clone();
            match apply::retheme(&config, scheme.as_deref(), mode.as_deref()).await {
                Ok(()) => Response::ok(req.id, serde_json::json!({"rethemed": true})),
                Err(e) => Response::err(req.id, 6, format!("{e}")),
            }
        }

        "cache_rebuild" => {
            let config = state.config.read().await.clone();
            let cache_state = state.cache_state.clone();
            let optimize_state = state.optimize_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                cache::rebuild(&config, db.clone(), cache_state, tx.clone()).await;
                crate::server::auto_optimize_if_enabled(&config, db, tx, optimize_state).await;
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "clear_data" => {
            let config = state.config.read().await.clone();
            let cache_state = state.cache_state.clone();
            let optimize_state = state.optimize_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                cache::nuke_and_rebuild(&config, db.clone(), cache_state, tx.clone()).await;
                crate::server::auto_optimize_if_enabled(&config, db, tx, optimize_state).await;
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "cache_status" => {
            let cs = state.cache_state.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "running": cs.running,
                    "progress": cs.progress,
                    "total": cs.total,
                }),
            )
        }

        "set_favourite" => {
            let key = req.str_param("key", "");
            let fav = req.bool_param("favourite", true);
            if key.is_empty() {
                return Response::err(req.id, 1, "missing 'key' parameter");
            }
            let db = state.db.lock().await;
            match crate::db::set_favourite(&db, key, fav) {
                Ok(true) => Response::ok(req.id, serde_json::json!({"key": key, "favourite": fav})),
                Ok(false) => Response::err(req.id, 6, format!("key not found: {key}")),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
        }

        "update_analysis" => {
            let key = req.str_param("key", "");
            if key.is_empty() {
                return Response::err(req.id, 1, "missing 'key' parameter");
            }
            let tags = req.opt_str("tags");
            let colors = req.opt_str("colors");
            let analyzed_by = req.opt_str("analyzed_by");
            let hue = req.opt_i64("hue");
            let sat = req.opt_i64("sat");
            let weather = req.opt_str("weather");

            let db = state.db.lock().await;
            match crate::db::update_analysis(&db, key, tags, colors, analyzed_by, hue, sat, weather) {
                Ok(true) => Response::ok(req.id, serde_json::json!({"updated": key})),
                Ok(false) => Response::err(req.id, 6, format!("key not found: {key}")),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
        }

        "update_metadata" => {
            let key = req.str_param("key", "");
            if key.is_empty() {
                return Response::err(req.id, 1, "missing 'key' parameter");
            }
            let filesize = req.params.get("filesize").and_then(|v| v.as_i64()).unwrap_or(0);
            let width = req.params.get("width").and_then(|v| v.as_i64()).unwrap_or(0);
            let height = req.params.get("height").and_then(|v| v.as_i64()).unwrap_or(0);

            let db = state.db.lock().await;
            match crate::db::update_meta_dimensions(&db, key, filesize, width, height) {
                Ok(()) => Response::ok(req.id, serde_json::json!({"updated": key})),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
        }

        "delete" => {
            let name = req.str_param("name", "");
            let wp_type = req.str_param("type", "static");
            let we_id = req.str_param("we_id", "");

            if name.is_empty() {
                return Response::err(req.id, 1, "missing 'name' parameter");
            }

            {
                let db = state.db.lock().await;
                let _ = crate::db::delete_by_name(&db, name);
            }
            let config = state.config.read().await.clone();

            let file_path = match wp_type {
                "we" => {
                    if !config.features.steam {
                        return Response::err(req.id, 1, "Steam feature is disabled");
                    }
                    if !we_id.is_empty() {
                        config.we_dir().join(we_id)
                    } else {
                        return Response::err(req.id, 1, "missing 'we_id' for WE type");
                    }
                }
                "video" => config.video_dir().join(name),
                _ => config.wallpaper_dir().join(name),
            };

            if wp_type == "we" {
                let _ = tokio::fs::remove_dir_all(&file_path).await;
            } else {
                let _ = tokio::fs::remove_file(&file_path).await;
            }

            let _ = broadcast_event(
                event_tx,
                "skwd.wall.file_removed",
                serde_json::json!({
                    "name": name, "type": wp_type
                }),
            );

            Response::ok(req.id, serde_json::json!({"deleted": name}))
        }

        "suppress" => {
            let name = req.str_param("name", "");
            if !name.is_empty() {
                let mut set = state.suppress_set.lock().unwrap();
                set.insert(name.to_string());
            }
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "unsuppress" => {
            let name = req.str_param("name", "");
            if !name.is_empty() {
                let mut set = state.suppress_set.lock().unwrap();
                set.remove(name);
            }
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "weather" => {
            let locale = state.config.read().await.general.locale.clone();
            match super::weather::current_conditions(&locale).await {
                Ok(conditions) => Response::ok(
                    req.id,
                    serde_json::json!({
                        "locale": locale,
                        "conditions": conditions,
                    }),
                ),
                Err(e) => Response::err(req.id, 10, format!("{e}")),
            }
        }

        "random_start" => {
            let interval_secs = req.params.get("interval").and_then(|v| v.as_u64()).unwrap_or(300);
            if interval_secs == 0 {
                return Response::err(req.id, 1, "interval must be > 0");
            }

            {
                let mut rot = state.random_rotation.lock().await;
                if let Some(prev) = rot.take() {
                    prev.handle.abort();
                }
            }

            let db = state.db.clone();
            let config = state.config.clone();
            let current_wp = state.current_wallpaper.clone();
            let tx = event_tx.clone();

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                let mut last_name: Option<String> = current_wp.lock().await.clone();
                loop {
                    interval.tick().await;
                    let name = {
                        let conn = db.lock().await;
                        crate::db::random_image_name(&conn, last_name.as_deref()).ok().flatten()
                    };
                    let Some(name) = name else {
                        warn!("[random] no wallpapers in database, skipping");
                        continue;
                    };
                    let cfg = config.read().await.clone();
                    let path = cfg.wallpaper_dir().join(&name);
                    let path_str = path.display().to_string();
                    match apply::apply_static(&path_str, &[], &cfg).await {
                        Ok(()) => {
                            last_name = Some(name.clone());
                            *current_wp.lock().await = Some(name.clone());
                            let _ = broadcast_event(
                                &tx,
                                "skwd.wall.applied",
                                serde_json::json!({"type": "static", "name": &name, "path": &path_str, "random": true}),
                            );
                        }
                        Err(e) => {
                            warn!("[random] failed to apply {path_str}: {e}");
                        }
                    }
                }
            });

            {
                let mut rot = state.random_rotation.lock().await;
                *rot = Some(RandomRotation { handle, interval_secs });
            }
            let _ = broadcast_event(event_tx, "skwd.wall.random_started", serde_json::json!({"interval": interval_secs}));
            Response::ok(req.id, serde_json::json!({"started": true, "interval": interval_secs}))
        }

        "random_stop" => {
            let mut rot = state.random_rotation.lock().await;
            if let Some(prev) = rot.take() {
                prev.handle.abort();
                let _ = broadcast_event(event_tx, "skwd.wall.random_stopped", serde_json::json!({}));
                Response::ok(req.id, serde_json::json!({"stopped": true}))
            } else {
                Response::ok(req.id, serde_json::json!({"stopped": false, "reason": "not running"}))
            }
        }

        "random_status" => {
            let rot = state.random_rotation.lock().await;
            match rot.as_ref() {
                Some(r) if !r.handle.is_finished() => {
                    Response::ok(req.id, serde_json::json!({"running": true, "interval": r.interval_secs}))
                }
                _ => Response::ok(req.id, serde_json::json!({"running": false})),
            }
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}
