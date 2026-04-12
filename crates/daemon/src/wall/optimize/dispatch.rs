use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::warn;

use super::{image, video};
use crate::server::SharedState;

pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    match req.method.as_str() {
        "optimize.start" => {
            let preset = req.str_param("preset", "balanced");
            let resolution = req.str_param("resolution", "4k");
            let config = state.config.read().await.clone();
            let optimize_state = state.optimize_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            let p = preset.to_string();
            let r = resolution.to_string();
            tokio::spawn(async move {
                if let Err(e) = image::start(&config, db, tx, optimize_state, &p, &r).await {
                    warn!("optimize start failed: {e}");
                }
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "optimize.cancel" => {
            image::cancel(&state.optimize_state).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "optimize.status" => {
            let s = state.optimize_state.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "running": s.running,
                    "progress": s.progress,
                    "total": s.total,
                    "currentFile": s.current_file,
                    "optimized": s.succeeded,
                    "skipped": s.skipped,
                    "failed": s.failed,
                }),
            )
        }

        "optimize.presets" => Response::ok(
            req.id,
            serde_json::json!({
                "presets": image::presets(),
                "resolutions": image::resolutions(),
            }),
        ),

        "video_convert.start" => {
            let preset = req.str_param("preset", "balanced");
            let resolution = req.str_param("resolution", "4k");
            let config = state.config.read().await.clone();
            let convert_state = state.convert_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            let p = preset.to_string();
            let r = resolution.to_string();
            tokio::spawn(async move {
                if let Err(e) = video::start(&config, db, tx, convert_state, &p, &r).await {
                    warn!("video_convert start failed: {e}");
                }
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "video_convert.cancel" => {
            video::cancel(&state.convert_state).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "video_convert.status" => {
            let s = state.convert_state.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "running": s.running,
                    "progress": s.progress,
                    "total": s.total,
                    "currentFile": s.current_file,
                    "converted": s.succeeded,
                    "skipped": s.skipped,
                    "failed": s.failed,
                }),
            )
        }

        "video_convert.presets" => Response::ok(
            req.id,
            serde_json::json!({
                "presets": video::presets(),
                "resolutions": video::resolutions(),
            }),
        ),

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}
