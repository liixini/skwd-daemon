use skwd_proto::{Request, Response};
use tokio::sync::broadcast;

use super::{apply, cache};
use crate::server::{SharedState, broadcast_event};

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

            let config = state.config.read().await.clone();

            let result = match wp_type {
                "static" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_static(path, &config).await
                }
                "video" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_video(path, &config).await
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

        "set_matugen" => {
            let key = req.str_param("key", "");
            let matugen = req.str_param("matugen", "");
            if key.is_empty() || matugen.is_empty() {
                return Response::err(req.id, 1, "missing 'key' or 'matugen' parameter");
            }
            let db = state.db.lock().await;
            match crate::db::set_matugen(&db, key, matugen) {
                Ok(_) => Response::ok(req.id, serde_json::json!({"key": key})),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
        }

        "set_matugen_batch" => {
            let entries = req.params.get("entries").and_then(|v| v.as_array());
            match entries {
                Some(arr) => {
                    let batch: Vec<(String, String)> = arr
                        .iter()
                        .filter_map(|e| {
                            let k = e.get("key")?.as_str()?;
                            let m = e.get("matugen")?.as_str()?;
                            Some((k.to_string(), m.to_string()))
                        })
                        .collect();
                    if batch.is_empty() {
                        return Response::err(req.id, 1, "empty or invalid entries array");
                    }
                    let db = state.db.lock().await;
                    match crate::db::set_matugen_batch(&db, &batch) {
                        Ok(count) => Response::ok(req.id, serde_json::json!({"updated": count})),
                        Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
                    }
                }
                None => Response::err(req.id, 1, "missing 'entries' array"),
            }
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

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}
