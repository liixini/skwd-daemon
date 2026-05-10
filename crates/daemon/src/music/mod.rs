use anyhow::Result;
use librespot::core::authentication::Credentials;
use serde_json::{Value, json};
use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::warn;

use crate::server::SharedState;

pub mod api;
pub mod auth;
pub mod discovery;
pub mod mpris;
pub mod oauth_server;
pub mod player;
pub mod session;
pub mod state;

pub use state::MusicState;

pub async fn dispatch(
    req: &Request,
    _event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("music.").unwrap_or(&req.method);
    let music = state.music.clone();

    match method {
        "auth.start" => {
            let cid = req
                .params
                .get("clientId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if cid.is_empty() {
                return Response::err(req.id, -32602, "clientId required");
            }
            music.set_client_id(cid.clone()).await;
            let verifier = auth::random_verifier();
            let challenge = auth::challenge_for(&verifier);
            let st = auth::random_verifier()
                .chars()
                .take(32)
                .collect::<String>();
            *music.pending_oauth.lock().await = Some(state::PendingOAuth {
                verifier,
                state: st.clone(),
                client_id: cid.clone(),
            });
            let url = auth::build_authorize_url(&cid, &challenge, &st);

            let mut running = music.callback_running.lock().await;
            if !*running {
                *running = true;
                drop(running);
                let m = music.clone();
                tokio::spawn(async move {
                    if let Err(e) = oauth_server::run_one_shot(m.clone()).await {
                        warn!("oauth loopback ended: {e:#}");
                    }
                    *m.callback_running.lock().await = false;
                });
            }
            Response::ok(req.id, json!({"authorizeUrl": url}))
        }
        "auth.status" => {
            let tokens = music.auth.current().await;
            let authenticated = tokens.is_some();
            let expires_at = tokens.as_ref().map(|t| t.expires_at_secs).unwrap_or(0);
            Response::ok(
                req.id,
                json!({"authenticated": authenticated, "expiresAtSecs": expires_at}),
            )
        }
        "auth.logout" => {
            music.auth.clear().await;
            music.player.shutdown().await;
            music.session.disconnect().await;
            Response::ok(req.id, json!({"ok": true}))
        }

        "device.set" => {
            let name = req
                .params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("skwd-music")
                .to_string();
            music.set_device(name).await;
            Response::ok(req.id, json!({"ok": true}))
        }

        "player.start" => match start_player(music.clone()).await {
            Ok(_) => Response::ok(req.id, json!({"ok": true})),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },
        "player.stop" => {
            music.player.shutdown().await;
            Response::ok(req.id, json!({"ok": true}))
        }
        "player.play" => {
            let did = ensure_target_device(&music).await;
            match api_call(&music, move |a| {
                let did = did.clone();
                async move { a.play_resume(did.as_deref()).await.map(|_| Value::Null) }
            }).await {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "player.pause" => {
            let did = ensure_target_device(&music).await;
            match api_call(&music, move |a| {
                let did = did.clone();
                async move { a.pause(did.as_deref()).await.map(|_| Value::Null) }
            }).await {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "player.next" => {
            let did = ensure_target_device(&music).await;
            match api_call(&music, move |a| {
                let did = did.clone();
                async move { a.skip_next(did.as_deref()).await.map(|_| Value::Null) }
            }).await {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "player.previous" => {
            let did = ensure_target_device(&music).await;
            match api_call(&music, move |a| {
                let did = did.clone();
                async move { a.skip_previous(did.as_deref()).await.map(|_| Value::Null) }
            }).await {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "player.volume" => {
            let v = req.params.get("volume").and_then(Value::as_u64).unwrap_or(0) as u16;
            let percent = ((v as u32 * 100) / 65535).min(100) as u8;
            let did = ensure_target_device(&music).await;
            match api_call(&music, move |a| {
                let did = did.clone();
                async move { a.set_volume(percent, did.as_deref()).await.map(|_| Value::Null) }
            }).await {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "status" => match api_call(&music, |a| async move { a.currently_playing().await }).await {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },
        "devices" => match api_call(&music, |a| async move { a.devices().await }).await {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },

        "play.uris" => {
            let uris: Vec<String> = req
                .params
                .get("uris")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let device = req
                .params
                .get("deviceId")
                .and_then(Value::as_str)
                .map(String::from);
            let target = match device {
                Some(d) => Some(d),
                None => ensure_target_device(&music).await,
            };
            match api_call(&music, move |a| {
                let t = target.clone();
                async move { a.play_uris(&uris, t.as_deref()).await.map(|_| Value::Null) }
            })
            .await
            {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "play.context" => {
            let ctx = req
                .params
                .get("contextUri")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let off = req
                .params
                .get("offsetUri")
                .and_then(Value::as_str)
                .map(String::from);
            let device = req
                .params
                .get("deviceId")
                .and_then(Value::as_str)
                .map(String::from);
            if ctx.is_empty() {
                return Response::err(req.id, -32602, "contextUri required");
            }
            let target = match device {
                Some(d) => Some(d),
                None => ensure_target_device(&music).await,
            };
            match api_call(&music, move |a| {
                let off = off.clone();
                let target = target.clone();
                let ctx = ctx.clone();
                async move {
                    a.play_context(&ctx, off.as_deref(), target.as_deref())
                        .await
                        .map(|_| Value::Null)
                }
            })
            .await
            {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "transfer" => {
            let did = req
                .params
                .get("deviceId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let play_now = req
                .params
                .get("play")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if did.is_empty() {
                return Response::err(req.id, -32602, "deviceId required");
            }
            match api_call(&music, move |a| {
                let did = did.clone();
                async move {
                    a.transfer_playback(&did, play_now)
                        .await
                        .map(|_| Value::Null)
                }
            })
            .await
            {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "search" => {
            let q = req
                .params
                .get("q")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let types: Vec<String> = req
                .params
                .get("types")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_else(|| vec!["track".into()]);
            let limit = req
                .params
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(10) as u32;
            match api_call(&music, move |a| {
                let q = q.clone();
                let types = types.clone();
                async move {
                    let t: Vec<&str> = types.iter().map(String::as_str).collect();
                    a.search(&q, &t, limit).await
                }
            })
            .await
            {
                Ok(v) => Response::ok(req.id, v),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "playlists" => match api_call(&music, |a| async move { a.user_playlists().await }).await {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },
        "playlist.tracks" => {
            let id = req
                .params
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if id.is_empty() {
                return Response::err(req.id, -32602, "id required");
            }
            match api_call(&music, move |a| {
                let id = id.clone();
                async move { a.playlist_tracks(&id).await }
            })
            .await
            {
                Ok(v) => Response::ok(req.id, v),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "liked" => match api_call(&music, |a| async move { a.liked_songs().await }).await {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },

        "queue" => match api_call(&music, |a| async move { a.queue().await }).await {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
        },

        "artist.top_tracks" => {
            let id = req
                .params
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let market = req
                .params
                .get("market")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            if id.is_empty() {
                return Response::err(req.id, -32602, "id required");
            }
            match api_call(&music, move |a| {
                let id = id.clone();
                let market = market.clone();
                async move { a.artist_top_tracks(&id, market.as_deref()).await }
            })
            .await
            {
                Ok(v) => Response::ok(req.id, v),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "queue.add" => {
            let uri = req
                .params
                .get("uri")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let device = req
                .params
                .get("deviceId")
                .and_then(Value::as_str)
                .map(String::from);
            if uri.is_empty() {
                return Response::err(req.id, -32602, "uri required");
            }
            match api_call(&music, move |a| {
                let uri = uri.clone();
                let device = device.clone();
                async move {
                    a.add_to_queue(&uri, device.as_deref())
                        .await
                        .map(|_| Value::Null)
                }
            })
            .await
            {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        "like.check" => {
            let ids: Vec<String> = req
                .params
                .get("ids")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            if ids.is_empty() {
                return Response::ok(req.id, json!([]));
            }
            match api_call(&music, move |a| {
                let ids = ids.clone();
                async move { a.check_liked(&ids).await }
            })
            .await
            {
                Ok(v) => Response::ok(req.id, v),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }
        "like.set" => {
            let ids: Vec<String> = req
                .params
                .get("ids")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let liked = req
                .params
                .get("liked")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            if ids.is_empty() {
                return Response::err(req.id, -32602, "ids required");
            }
            let res = if liked {
                api_call(&music, move |a| {
                    let ids = ids.clone();
                    async move { a.like(&ids).await.map(|_| Value::Null) }
                })
                .await
            } else {
                api_call(&music, move |a| {
                    let ids = ids.clone();
                    async move { a.unlike(&ids).await.map(|_| Value::Null) }
                })
                .await
            };
            match res {
                Ok(_) => Response::ok(req.id, json!({"ok": true})),
                Err(e) => Response::err(req.id, -32000, format!("{e:#}")),
            }
        }

        _ => Response::err(
            req.id,
            -32601,
            format!("Unknown music method: {}", method),
        ),
    }
}

async fn api_call<F, Fut>(music: &MusicState, op: F) -> Result<Value>
where
    F: FnOnce(api::ApiClient) -> Fut + Send,
    Fut: std::future::Future<Output = Result<Value>> + Send,
{
    let cid = music.current_client_id().await;
    let tokens = music
        .auth
        .ensure_fresh(&cid)
        .await?
        .ok_or_else(|| anyhow::anyhow!("not authenticated"))?;
    let client = api::ApiClient::new(tokens.access_token);
    op(client).await
}

async fn ensure_target_device(music: &MusicState) -> Option<String> {
    let cid = music.current_client_id().await;
    let tokens = music.auth.ensure_fresh(&cid).await.ok().flatten()?;
    let client = api::ApiClient::new(tokens.access_token);
    let devices = client.devices().await.ok()?;
    let wanted = music.current_device().await.to_lowercase();
    let arr = devices.get("devices").and_then(Value::as_array)?;
    for d in arr {
        let name = d.get("name").and_then(Value::as_str).unwrap_or("");
        if name.to_lowercase() == wanted {
            let id = d.get("id").and_then(Value::as_str)?.to_string();
            let active = d.get("is_active").and_then(Value::as_bool).unwrap_or(false);
            if !active {
                client.transfer_playback(&id, false).await.ok();
            }
            return Some(id);
        }
    }
    None
}

async fn start_player(music: std::sync::Arc<MusicState>) -> Result<()> {
    let session = music.session.ensure().await?;
    let device = music.current_device().await;
    let cid = music.current_client_id().await;

    let creds = if let Some(c) = session.cache().and_then(|c| c.credentials()) {
        tracing::info!("music: start_player using cached credentials auth_type={:?}", c.auth_type);
        c
    } else {
        tracing::info!("music: no cached credentials, launching discovery for pairing");
        let _ = music.event_tx.send(serde_json::json!({
            "event": "skwd.music.discovery.waiting",
            "data": {"deviceName": device}
        }).to_string());
        let cid = if cid.is_empty() {
            "65b708073fc0480ea92a077233ca87bd".to_string()
        } else {
            cid
        };
        let creds = discovery::await_credentials(&device, &cid).await?;
        let _ = music.event_tx.send(serde_json::json!({
            "event": "skwd.music.discovery.paired",
            "data": {}
        }).to_string());
        creds
    };

    music.player.ensure(session, creds, &device, music.event_tx.clone(), music.mpris_state.clone()).await?;

    let resume_music = music.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let _ = ensure_target_device(&resume_music).await;
    });

    Ok(())
}
