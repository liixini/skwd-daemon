#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::warn;

use super::{apply, cache};
use crate::server::{RandomRotation, SharedState, broadcast_event};

pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    let method = req.method.strip_prefix("wall.").unwrap_or(&req.method);
    match method {
        "refresh_overview_backdrop" => {
            let config = state.config.read().await.clone();

            let Some(source) = super::overview_backdrop::resolve_source(&config).await else {
                return Response::err(req.id, 1, "no recent wallpaper recorded");
            };

            let source_for_resp = source.clone();
            tokio::spawn(async move {
                super::overview_backdrop::refresh(&source, &config).await;
            });

            Response::ok(req.id, serde_json::json!({"started": true, "source": source_for_resp}))
        }

        "toggle" => {
            let mut ui = state.ui.lock().await;
            ui.toggle();
            let running = ui.is_running();
            drop(ui);
            let config = state.config.read().await.clone();
            if running {
                apply::on_wall_show(&config).await;
            } else {
                apply::on_wall_hide().await;
            }
            let _ = broadcast_event(event_tx, "skwd.wall.toggle", serde_json::json!({"visible": running}));
            Response::ok(req.id, serde_json::json!({"toggled": true, "visible": running}))
        }

        "show" => {
            state.ui.lock().await.launch();
            let config = state.config.read().await.clone();
            apply::on_wall_show(&config).await;
            let _ = broadcast_event(event_tx, "skwd.wall.show", serde_json::json!({}));
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "hide" => {
            state.ui.lock().await.kill();
            apply::on_wall_hide().await;
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
            let neighbors: Vec<String> = req
                .params
                .get("neighbors")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                .unwrap_or_default();
            let all_screens: Vec<String> = req
                .params
                .get("screens")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                .unwrap_or_default();

            let outputs_audio: std::collections::HashMap<String, bool> = req
                .params
                .get("outputs_audio")
                .and_then(|v| v.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_bool().map(|b| (k.clone(), b)))
                        .collect()
                })
                .unwrap_or_default();
            let outputs_volume: std::collections::HashMap<String, u32> = req
                .params
                .get("outputs_volume")
                .and_then(|v| v.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| {
                            v.as_u64().map(|n| (k.clone(), (n as u32).min(100)))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let config = state.config.read().await.clone();

            let result = match wp_type {
                "static" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_static(path, &outputs, &neighbors, &all_screens, &config).await
                }
                "video" => {
                    if path.is_empty() {
                        return Response::err(req.id, 1, "missing 'path' parameter");
                    }
                    apply::apply_video(path, &outputs, &neighbors, &all_screens, &outputs_audio, &outputs_volume, &config).await
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
                    apply::apply_we(we_id, &screens, &outputs_audio, &outputs_volume, &config).await
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

                    
                    let key: String = if !we_id.is_empty() {
                        we_id.to_string()
                    } else {
                        name.rsplit_once('.').map_or(name.as_str(), |(s, _)| s).to_string()
                    };
                    if !key.is_empty() {
                        let conn = state.db.lock().await;
                        let _ = crate::db::bump_apply_count(&conn, &key);
                        drop(conn);
                    }

                    let _ = broadcast_event(
                        event_tx,
                        "skwd.wall.applied",
                        serde_json::json!({"type": wp_type, "name": &name, "path": path, "we_id": we_id, "key": key}),
                    );

                    {
                        let cfg_clone = config.clone();
                        tokio::spawn(async move {
                            if let Some(src) = super::overview_backdrop::resolve_source(&cfg_clone).await {
                                super::overview_backdrop::refresh(&src, &cfg_clone).await;
                            }
                        });
                    }

                    Response::ok(req.id, serde_json::json!({"applied": name}))
                }
                Err(e) => Response::err(req.id, 4, format!("{e}")),
            }
        }

        "preheat" => {
            let path = req.str_param("path", "");
            apply::preheat(path);
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "backdrop" => {
            let path = req.str_param("path", "").to_string();
            let config = state.config.read().await.clone();
            let source = if path.trim().is_empty() {
                super::overview_backdrop::resolve_source(&config).await
            } else {
                Some(path)
            };
            match source {
                Some(src) => {
                    super::overview_backdrop::refresh(&src, &config).await;
                    Response::ok(req.id, serde_json::json!({"backdrop": src}))
                }
                None => Response::ok(req.id, serde_json::json!({"backdrop": null})),
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
            let color_index = req
                .params
                .get("color_index")
                .and_then(serde_json::Value::as_u64)
                .map(|n| (n as u32).min(3));
            tracing::info!(?scheme, ?mode, ?color_index, "wall.retheme received");
            let config = state.config.read().await.clone();
            match apply::retheme(&config, scheme.as_deref(), mode.as_deref(), color_index).await {
                Ok(()) => Response::ok(req.id, serde_json::json!({"rethemed": true})),
                Err(e) => {
                    tracing::warn!(error = %e, "wall.retheme failed");
                    Response::err(req.id, 6, format!("{e}"))
                }
            }
        }

        "full_palette" | "theme_full_palette" => {
            let config_guard = state.config.read().await.clone();
            let scheme = req
                .params
                .get("scheme")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| config_guard.matugen_scheme())
                .to_string();
            let mode = req
                .params
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| config_guard.matugen_mode())
                .to_string();
            let color_index = req
                .params
                .get("color_index")
                .and_then(serde_json::Value::as_u64).map_or_else(|| config_guard.matugen_color_index(), |n| (n as u32).min(3));
            tracing::info!(scheme, mode, color_index, "theme.full_palette");
            match apply::theme_full_palette(&*state.runner, &config_guard, &scheme, &mode, color_index).await {
                Ok(palette) => Response::ok(req.id, palette),
                Err(e) => {
                    tracing::warn!(error = %e, "theme.full_palette failed");
                    Response::err(req.id, 6, format!("{e}"))
                }
            }
        }

        "theme_preview" => {
            let scheme = req
                .params
                .get("scheme")
                .and_then(|v| v.as_str())
                .unwrap_or("scheme-fidelity")
                .to_string();
            let mode = req
                .params
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("dark")
                .to_string();
            let color_index = req
                .params
                .get("color_index")
                .and_then(serde_json::Value::as_u64)
                .map_or(0, |n| (n as u32).min(3));
            let config = state.config.read().await.clone();
            tracing::info!(scheme, mode, color_index, "theme_preview");
            match apply::theme_preview(&config, &scheme, &mode, color_index).await {
                Ok(palette) => Response::ok(req.id, palette),
                Err(e) => {
                    tracing::warn!(error = %e, "theme_preview failed");
                    Response::err(req.id, 6, format!("{e}"))
                }
            }
        }

        "cache_rebuild" => {
            let config = state.config.read().await.clone();
            let cache_state = state.cache_state.clone();
            let optimize_state = state.optimize_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            let runner = state.runner.clone();
            tokio::spawn(async move {
                cache::rebuild(&config, db.clone(), cache_state, tx.clone()).await;
                crate::server::auto_optimize_if_enabled(runner, &config, db, tx, optimize_state).await;
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "recompute_colors" => {
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                cache::recompute_colors(db, tx).await;
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "clear_data" => {
            let config = state.config.read().await.clone();
            let cache_state = state.cache_state.clone();
            let optimize_state = state.optimize_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            let runner = state.runner.clone();
            tokio::spawn(async move {
                cache::nuke_and_rebuild(&config, db.clone(), cache_state, tx.clone()).await;
                crate::server::auto_optimize_if_enabled(runner, &config, db, tx, optimize_state).await;
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

        "list_cached" => {
            let favourite_only = req.params.get("favourite_only")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let db = state.db.lock().await;
            match crate::db::list_wallpapers(&db, favourite_only) {
                Ok(items) => Response::ok(req.id, serde_json::json!({"items": items})),
                Err(e) => Response::err(req.id, 2, format!("db error: {e}")),
            }
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
            let filesize = req.params.get("filesize").and_then(serde_json::Value::as_i64).unwrap_or(0);
            let width = req.params.get("width").and_then(serde_json::Value::as_i64).unwrap_or(0);
            let height = req.params.get("height").and_then(serde_json::Value::as_i64).unwrap_or(0);

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

        "outputs" => {
            let cfg = state.config.read().await.clone();
            let map = apply::read_outputs_state(&cfg.cache_dir()).await;
            Response::ok(req.id, serde_json::json!({"outputs": map}))
        }

        "set_audio" => {
            let mute = req.params.get("mute").and_then(serde_json::Value::as_bool);
            let volume = req
                .params
                .get("volume")
                .and_then(serde_json::Value::as_u64)
                .map(|v| v.min(100) as u32);
            let outputs: Option<Vec<String>> = req
                .params
                .get("outputs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                });
            let cfg = state.config.read().await.clone();
            apply::set_audio_for(&cfg, mute, volume, outputs).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }


        "we_properties" => {
            let we_id = req.str_param("we_id", "");
            if we_id.is_empty() {
                return Response::err(req.id, 1, "missing 'we_id' parameter");
            }
            let cfg = state.config.read().await.clone();
            let res = apply::we_properties_for(&cfg, we_id).await;
            Response::ok(req.id, res)
        }

        "set_we_screen_overrides" => {
            let outputs: Option<Vec<String>> = req
                .params
                .get("outputs")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                });
            let scaling = req.params.get("scaling").and_then(|v| v.as_str()).map(String::from);
            let clamp = req.params.get("clamp").and_then(|v| v.as_str()).map(String::from);
            let cfg = state.config.read().await.clone();
            apply::set_we_screen_overrides(&cfg, outputs, scaling, clamp).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "set_we_props" => {
            let we_id = req.str_param("we_id", "");
            if we_id.is_empty() {
                return Response::err(req.id, 1, "missing 'we_id' parameter");
            }
            let values = req
                .params
                .get("values")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let cfg = state.config.read().await.clone();
            apply::set_we_props(&cfg, we_id, values).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
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
            let interval_secs = req.params.get("interval").and_then(serde_json::Value::as_u64).unwrap_or(300);
            if interval_secs == 0 {
                return Response::err(req.id, 1, "interval must be > 0");
            }

            let types: Vec<String> = req
                .params
                .get("types")
                .and_then(|v| v.as_array()).map_or_else(|| vec!["static".into(), "video".into(), "we".into()], |arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .filter(|s| matches!(*s, "static" | "video" | "we"))
                        .map(str::to_string)
                        .collect()
                });
            let favourites_only = req.bool_param("favourites_only", false);
            if types.is_empty() {
                return Response::err(req.id, 1, "types must contain at least one of static|video|we");
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
            let task_types = types.clone();

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                let mut last_name: Option<String> = current_wp.lock().await.clone();
                loop {
                    interval.tick().await;
                    let pick = {
                        let conn = db.lock().await;
                        let type_refs: Vec<&str> = task_types.iter().map(String::as_str).collect();
                        crate::db::random_pick(&conn, last_name.as_deref(), &type_refs, favourites_only).ok().flatten()
                    };
                    let Some((_key, wp_type, name, video_file, we_id)) = pick else {
                        warn!("[random] no matching wallpapers, skipping");
                        continue;
                    };
                    let cfg = config.read().await.clone();

                    let empty_audio: std::collections::HashMap<String, bool> = Default::default();
                    let empty_vol: std::collections::HashMap<String, u32> = Default::default();
                    let result = match wp_type.as_str() {
                        "video" => {
                            let path = if video_file.is_empty() {
                                cfg.video_dir().join(&name)
                            } else {
                                std::path::PathBuf::from(&video_file)
                            };
                            let path_str = path.display().to_string();
                            apply::apply_video(&path_str, &[], &[], &[], &empty_audio, &empty_vol, &cfg).await
                                .map(|()| ("video", path_str))
                        }
                        "we" => {
                            apply::apply_we(&we_id, &[], &empty_audio, &empty_vol, &cfg).await
                                .map(|()| ("we", we_id.clone()))
                        }
                        _ => {
                            let path = cfg.wallpaper_dir().join(&name);
                            let path_str = path.display().to_string();
                            apply::apply_static(&path_str, &[], &[], &[], &cfg).await
                                .map(|()| ("static", path_str))
                        }
                    };

                    match result {
                        Ok((kind, path_str)) => {
                            last_name = Some(name.clone());
                            *current_wp.lock().await = Some(name.clone());
                            let _ = broadcast_event(
                                &tx,
                                "skwd.wall.applied",
                                serde_json::json!({"type": kind, "name": &name, "path": &path_str, "random": true}),
                            );
                        }
                        Err(e) => {
                            warn!("[random] failed to apply {wp_type} {name}: {e}");
                        }
                    }
                }
            });

            {
                let mut rot = state.random_rotation.lock().await;
                *rot = Some(RandomRotation {
                    handle,
                    interval_secs,
                    types: types.clone(),
                    favourites_only,
                });
            }
            let _ = broadcast_event(
                event_tx,
                "skwd.wall.random_started",
                serde_json::json!({
                    "interval": interval_secs,
                    "types": &types,
                    "favourites_only": favourites_only,
                }),
            );
            Response::ok(
                req.id,
                serde_json::json!({
                    "started": true,
                    "interval": interval_secs,
                    "types": types,
                    "favourites_only": favourites_only,
                }),
            )
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
                Some(r) if !r.handle.is_finished() => Response::ok(
                    req.id,
                    serde_json::json!({
                        "running": true,
                        "interval": r.interval_secs,
                        "types": &r.types,
                        "favourites_only": r.favourites_only,
                    }),
                ),
                _ => Response::ok(req.id, serde_json::json!({"running": false})),
            }
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

#[cfg(test)]
mod tests {
    use crate::server::test_state;

    fn insert(h: &crate::server::TestHarness, key: &str, name: &str) {
        let db = h.state.db.try_lock().unwrap();
        crate::db::upsert_cache_entry(&db, key, "static", name, "t", "ts", "", "", 1, 0, 0, 0).unwrap();
    }

    #[tokio::test]
    async fn cache_status_reports_idle() {
        let h = test_state();
        let r = h.dispatch("wall.cache_status", serde_json::json!({})).await.result.unwrap();
        assert_eq!(r["running"], false);
        assert_eq!(r["progress"], 0);
        assert_eq!(r["total"], 0);
    }

    #[tokio::test]
    async fn list_cached_reflects_db_and_favourite_filter() {
        let h = test_state();
        assert_eq!(
            h.dispatch("wall.list_cached", serde_json::json!({})).await.result.unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        insert(&h, "static:a", "a");
        let items = h.dispatch("wall.list_cached", serde_json::json!({})).await.result.unwrap();
        assert_eq!(items["items"].as_array().unwrap().len(), 1);
        let fav = h
            .dispatch("wall.list_cached", serde_json::json!({ "favourite_only": true }))
            .await
            .result
            .unwrap();
        assert_eq!(fav["items"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn set_favourite_success_and_errors() {
        let h = test_state();
        insert(&h, "static:f", "f");
        let ok = h
            .dispatch("wall.set_favourite", serde_json::json!({ "key": "static:f", "favourite": true }))
            .await;
        assert!(ok.error.is_none());
        assert_eq!(ok.result.unwrap()["favourite"], true);

        let missing = h.dispatch("wall.set_favourite", serde_json::json!({})).await;
        assert_eq!(missing.error.unwrap().code, 1);

        let notfound = h
            .dispatch("wall.set_favourite", serde_json::json!({ "key": "static:nope" }))
            .await;
        assert_eq!(notfound.error.unwrap().code, 6);
    }

    #[tokio::test]
    async fn update_analysis_writes_and_validates_key() {
        let h = test_state();
        insert(&h, "static:a", "a");
        let ok = h
            .dispatch(
                "wall.update_analysis",
                serde_json::json!({ "key": "static:a", "tags": "x,y", "hue": 120 }),
            )
            .await;
        assert_eq!(ok.result.unwrap()["updated"], "static:a");

        let rows = h.dispatch("wall.list", serde_json::json!({})).await.result.unwrap();
        assert_eq!(rows["wallpapers"][0]["tags"], "x,y");
        assert_eq!(rows["wallpapers"][0]["hue"], 120);

        assert_eq!(
            h.dispatch("wall.update_analysis", serde_json::json!({})).await.error.unwrap().code,
            1
        );
        assert_eq!(
            h.dispatch("wall.update_analysis", serde_json::json!({ "key": "nope" }))
                .await
                .error
                .unwrap()
                .code,
            6
        );
    }

    #[tokio::test]
    async fn update_metadata_sets_dimensions() {
        let h = test_state();
        insert(&h, "static:a", "a");
        let ok = h
            .dispatch(
                "wall.update_metadata",
                serde_json::json!({ "key": "static:a", "filesize": 4096, "width": 1920, "height": 1080 }),
            )
            .await;
        assert!(ok.error.is_none());
        let rows = h.dispatch("wall.list", serde_json::json!({})).await.result.unwrap();
        assert_eq!(rows["wallpapers"][0]["width"], 1920);
        assert_eq!(
            h.dispatch("wall.update_metadata", serde_json::json!({})).await.error.unwrap().code,
            1
        );
    }

    #[tokio::test]
    async fn suppress_and_unsuppress_mutate_set() {
        let h = test_state();
        h.dispatch("wall.suppress", serde_json::json!({ "name": "wall.png" })).await;
        assert!(h.state.suppress_set.lock().unwrap().contains("wall.png"));
        h.dispatch("wall.unsuppress", serde_json::json!({ "name": "wall.png" })).await;
        assert!(!h.state.suppress_set.lock().unwrap().contains("wall.png"));
    }

    #[tokio::test]
    async fn random_status_idle() {
        let h = test_state();
        assert_eq!(
            h.dispatch("wall.random_status", serde_json::json!({})).await.result.unwrap()["running"],
            false
        );
    }

    #[tokio::test]
    async fn outputs_reads_empty_cache_dir() {
        let h = test_state();
        let dir = tempfile::tempdir().unwrap();
        h.state.config.write().await.paths.cache = Some(dir.path().to_string_lossy().to_string());
        assert_eq!(
            h.dispatch("wall.outputs", serde_json::json!({})).await.result.unwrap()["outputs"],
            serde_json::json!({})
        );
    }

    #[tokio::test]
    async fn unknown_wall_method_errors() {
        let h = test_state();
        let e = h.dispatch("wall.bogus", serde_json::json!({})).await.error.unwrap();
        assert_eq!(e.code, -32601);
    }

    // Contract: skwd-wall's WallpaperSelectorService.qml reads these exact fields off each
    // wall.list item. A rename/drop here silently breaks the wallpaper grid.
    #[tokio::test]
    async fn wall_list_item_contract_matches_frontend() {
        let h = test_state();
        {
            let db = h.state.db.try_lock().unwrap();
            crate::db::upsert_cache_entry(
                &db, "static:cat", "static", "cat", "/t.webp", "/sm.webp", "v.mp4", "we42", 12345, 200, 80, 5,
            )
            .unwrap();
            crate::db::set_favourite(&db, "static:cat", true).unwrap();
            crate::db::update_analysis(
                &db, "static:cat", Some("a,b"), Some("#fff"), Some("ollama"), Some(200), Some(80), Some("sunny"),
            )
            .unwrap();
            crate::db::bump_apply_count(&db, "static:cat").unwrap();
            crate::db::update_meta_dimensions(&db, "static:cat", 9000, 1920, 1080).unwrap();
        }
        let r = h.dispatch("wall.list", serde_json::json!({})).await.result.unwrap();
        assert_eq!(r["count"], 1);
        let item = &r["wallpapers"][0];
        assert_eq!(item["key"], "static:cat");
        assert_eq!(item["name"], "cat");
        assert_eq!(item["type"], "static");
        assert_eq!(item["thumb"], "/t.webp");
        assert_eq!(item["thumb_sm"], "/sm.webp");
        assert_eq!(item["favourite"], 1);
        assert_eq!(item["hue"], 200);
        assert_eq!(item["sat"], 80);
        assert_eq!(item["tags"], "a,b");
        assert_eq!(item["colors"], "#fff");
        assert_eq!(item["video_file"], "v.mp4");
        assert_eq!(item["we_id"], "we42");
        assert_eq!(item["mtime"], 12345);
        assert_eq!(item["richness"], 5);
        assert_eq!(item["apply_count"], 1);
        assert_eq!(item["width"], 1920);
        assert_eq!(item["height"], 1080);
        assert_eq!(item["weather"], "sunny");
    }
}
