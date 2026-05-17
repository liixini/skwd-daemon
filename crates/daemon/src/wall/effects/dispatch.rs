use std::path::{Path, PathBuf};

use serde_json::json;
use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::warn;

use crate::server::SharedState;

use super::native;

pub async fn dispatch(req: &Request, _event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    match req.method.as_str() {
        "effects.list" => Response::ok(req.id, json!({ "effects": native::list() })),

        "effects.preview" => {
            let Some(input_str) = req.params.get("input").and_then(|v| v.as_str()) else {
                return Response::err(req.id, -32602, "missing 'input' parameter");
            };
            let input = PathBuf::from(input_str);
            let effect = req.params.get("effect").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let params = req.params.get("params").cloned().unwrap_or(json!({}));

            let cache_dir = state.config.read().await.cache_dir();
            let suffix = native::suffix(&effect, &params);
            let output = match native::preview_path(&cache_dir, &input, &suffix) {
                Ok(p) => p,
                Err(e) => return Response::err(req.id, -32603, format!("path resolution failed: {e}")),
            };

            match native::render(&effect, &input, &params, &output).await {
                Ok(()) => Response::ok(req.id, json!({ "output": output.to_string_lossy() })),
                Err(e) => {
                    warn!("effects.preview {effect}: {e}");
                    Response::err(req.id, -32603, format!("{effect} failed: {e}"))
                }
            }
        }

        "effects.commit" => {
            let Some(preview_str) = req.params.get("preview").and_then(|v| v.as_str()) else {
                return Response::err(req.id, -32602, "missing 'preview' parameter");
            };
            let Some(input_str) = req.params.get("input").and_then(|v| v.as_str()) else {
                return Response::err(req.id, -32602, "missing 'input' parameter");
            };
            let preview = PathBuf::from(preview_str);
            let input = PathBuf::from(input_str);
            let effect = req.params.get("effect").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let params = req.params.get("params").cloned().unwrap_or(json!({}));

            let suffix = native::suffix(&effect, &params);
            let final_path = match native::library_path(&input, &suffix) {
                Ok(p) => p,
                Err(e) => return Response::err(req.id, -32603, format!("path resolution failed: {e}")),
            };

            match commit_preview(&preview, &final_path).await {
                Ok(()) => Response::ok(req.id, json!({ "output": final_path.to_string_lossy() })),
                Err(e) => {
                    warn!("effects.commit: {e}");
                    Response::err(req.id, -32603, format!("commit failed: {e}"))
                }
            }
        }

        "effects.discard" => {
            let Some(preview_str) = req.params.get("preview").and_then(|v| v.as_str()) else {
                return Response::err(req.id, -32602, "missing 'preview' parameter");
            };
            let preview = PathBuf::from(preview_str);
            let _ = tokio::fs::remove_file(&preview).await;
            Response::ok(req.id, json!({ "ok": true }))
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

async fn commit_preview(preview: &Path, final_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if tokio::fs::rename(preview, final_path).await.is_ok() {
        return Ok(());
    }

    let mut staging = final_path.as_os_str().to_owned();
    staging.push(".staging");
    let staging = PathBuf::from(staging);

    tokio::fs::copy(preview, &staging).await?;
    tokio::fs::rename(&staging, final_path).await?;
    let _ = tokio::fs::remove_file(preview).await;
    Ok(())
}
