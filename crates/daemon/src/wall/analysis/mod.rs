use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{Mutex, broadcast};
use tracing::{info, warn};

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::{SharedState, make_event};
use crate::util::BatchJobState;
use crate::wall::thumb;

use skwd_proto::{Request, Response};


pub async fn dispatch(req: &Request, event_tx: &broadcast::Sender<String>, state: &SharedState) -> Response {
    let method = req.method.strip_prefix("analysis.").unwrap_or(&req.method);
    match method {
        "start" => {
            let config = state.config.read().await.clone();
            let analysis_state = state.analysis_state.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = start(&config, db, tx, analysis_state).await {
                    warn!("analysis start failed: {e}");
                }
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
        }

        "stop" => {
            stop(&state.analysis_state).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "status" => {
            let s = state.analysis_state.lock().await;
            Response::ok(
                req.id,
                serde_json::json!({
                    "running": s.batch.running,
                    "progress": s.batch.progress,
                    "total": s.batch.total,
                    "taggedCount": s.tagged_count,
                    "coloredCount": s.colored_count,
                    "totalThumbs": s.total_thumbs,
                    "failedCount": s.failed_count,
                    "lastLog": s.last_log,
                    "eta": s.eta,
                }),
            )
        }

        "regenerate" => {
            regenerate(&state.db_shared).await;
            Response::ok(req.id, serde_json::json!({"ok": true}))
        }

        "default_prompt" => {
            Response::ok(req.id, serde_json::json!({"prompt": OLLAMA_PROMPT}))
        }

        "retag_one" => {
            let key = req.str_param("key", "").to_string();
            if key.is_empty() {
                return Response::err(req.id, 1, "missing 'key' parameter");
            }
            let config = state.config.read().await.clone();
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            let key_for_task = key.clone();
            tokio::spawn(async move {
                if let Err(e) = retag_one(&config, db, tx, &key_for_task).await {
                    warn!("retag_one failed for {key_for_task}: {e}");
                }
            });
            Response::ok(req.id, serde_json::json!({"started": true, "key": key}))
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}


#[derive(Default)]
pub struct AnalysisState {
    pub batch: BatchJobState,
    pub tagged_count: usize,
    pub colored_count: usize,
    pub total_thumbs: usize,
    pub failed_count: usize,
    pub last_log: String,
    pub eta: String,
}


const OLLAMA_PROMPT: &str = "\
You are tagging an image for visual search. Look at the image, then output exactly two lines.

LINE 1 - tags, lowercase, comma-separated. Cover these four categories, in this order. Skip a category if nothing in the image fits it.

1. SUBJECTS - every distinct living thing or notable foreground object you can see. Each subject gets its own tag. If two creatures are present, tag both separately.
2. THEMES - the larger setting, environment, or location depicted (one to a few tags).
3. MOOD - the overall feeling the image conveys (one tag, sometimes two).
4. ART STYLE - the visual technique used to make the image (one tag).

Strict rules:
  - Only tag what is clearly visible. Never invent. If you do not see a person or creature, do not tag one. Missing a tag is always better than inventing one.
  - Lowercase, one word per tag (hyphen only for unavoidable compounds).
  - No duplicates. No color names. No generic filler (image, wallpaper, background, scene, view, aesthetic, beautiful).
  - Prefer specific over vague.
  - Tag count scales to image complexity. A dense scene may have 10-15 tags; a simple abstract may have 3-4. Do not pad to hit a number.

Quality flags - include each independently only if it obviously applies:
  - minimalist (sparse composition, lots of negative space)
  - colourful (five or more distinct, well-distributed colors)
  - vibrant (saturated, high-energy colors)

LINE 2 - weather fit, comma-separated subset of: clear, sunny, cloudy, rainy, snowy, stormy, foggy, windy. If nothing fits, output 'clear'.";

const DEFAULT_OLLAMA_URL: &str = "http://localhost:11434";
const DEFAULT_OLLAMA_MODEL: &str = "llava:latest";

pub async fn start(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    state: Arc<Mutex<AnalysisState>>,
) -> anyhow::Result<()> {
    {
        let s = state.lock().await;
        if s.batch.running {
            bail!("already running");
        }
    }

    let ollama_url = if config.ollama.url.is_empty() {
        DEFAULT_OLLAMA_URL.to_string()
    } else {
        config.ollama.url.clone()
    };
    let ollama_model = if config.ollama.model.is_empty() {
        DEFAULT_OLLAMA_MODEL.to_string()
    } else {
        config.ollama.model.clone()
    };
    let ollama_prompt = if config.ollama.prompt.trim().is_empty() {
        OLLAMA_PROMPT.to_string()
    } else {
        config.ollama.prompt.clone()
    };
    {
        let mut s = state.lock().await;
        s.batch.running = true;
        s.last_log = "Connecting to Ollama...".into();
    }
    broadcast_progress(&event_tx, &state).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;

    let resp = client.get(format!("{ollama_url}/api/tags")).send().await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            let msg = format!("Ollama unavailable (HTTP {})", r.status());
            let mut s = state.lock().await;
            s.batch.running = false;
            s.last_log = msg.clone();
            drop(s);
            broadcast_progress(&event_tx, &state).await;
            return Err(anyhow::anyhow!(msg));
        }
        Err(e) => {
            let msg = format!("Ollama error: {e}");
            let mut s = state.lock().await;
            s.batch.running = false;
            s.last_log = msg.clone();
            drop(s);
            broadcast_progress(&event_tx, &state).await;
            return Err(anyhow::anyhow!(msg));
        }
    }

    let cache_dir = config.cache_dir().join("wallpaper");
    let thumbs_dirs = vec![
        cache_dir.join("thumbs"),
        cache_dir.join("we-thumbs"),
        cache_dir.join("video-thumbs"),
    ];

    let (existing_tags, existing_colors, failed_keys) = {
        let conn = db.lock().await;
        load_existing(&conn, &ollama_model)
    };

    let mut thumbs = Vec::new();
    for dir in &thumbs_dirs {
        collect_thumbs(dir, &mut thumbs).await;
    }
    thumbs.sort();

    let total_thumbs = thumbs.len();

    
    let mut queue: Vec<(String, String, bool)> = Vec::new();
    for path in &thumbs {
        let key = thumb_to_key(path);
        let has_tags = existing_tags.contains(&key);
        let has_colors = existing_colors.contains(&key);
        let was_failed = failed_keys.contains(&key);
        if !has_tags || !has_colors || was_failed {
            queue.push((path.clone(), key, was_failed));
        }
    }
    let retry_count = queue.iter().filter(|(_, _, f)| *f).count();
    if retry_count > 0 {
        info!("retrying {retry_count} previously-failed items");
    }

    {
        let mut s = state.lock().await;
        s.batch.running = true;
        s.batch.cancel = false;
        s.batch.progress = 0;
        s.batch.total = queue.len();
        s.tagged_count = existing_tags.len();
        s.colored_count = existing_colors.len();
        s.total_thumbs = total_thumbs;
        s.failed_count = failed_keys.len();
        s.last_log = if queue.is_empty() {
            format!("All {total_thumbs} items already analyzed")
        } else {
            format!("Analyzing {} items...", queue.len())
        };
        s.eta.clear();
    }

    broadcast_progress(&event_tx, &state).await;

    let start_time = std::time::Instant::now();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    for (i, (path, key, was_failed)) in queue.iter().enumerate() {
        {
            let s = state.lock().await;
            if s.batch.cancel {
                break;
            }
        }

        {
            let mut s = state.lock().await;
            s.last_log = format!(
                "Analyzing {}",
                Path::new(path).file_name().unwrap_or_default().to_string_lossy()
            );
        }

        match analyze_one(path, &ollama_url, &ollama_model, &ollama_prompt, &client).await {
            Ok((hue, sat, tags, colors_json, weather)) => {
                let tags_json = serde_json::to_string(&tags).unwrap_or_else(|_| "[]".into());
                let weather_json = serde_json::to_string(&weather).unwrap_or_else(|_| "[]".into());
                let conn = db.lock().await;
                let _ = db::update_analysis(
                    &conn,
                    key,
                    Some(&tags_json),
                    Some(&colors_json),
                    Some(&ollama_model),
                    Some(hue),
                    Some(sat),
                    Some(&weather_json),
                );
                
                
                let _ = conn.execute(
                    "UPDATE meta SET analysis_error = NULL WHERE key = ?1",
                    rusqlite::params![key],
                );
                drop(conn);

                let _ = event_tx.send(make_event(
                    "skwd.wall.analysis.item",
                    serde_json::json!({
                        "key": key, "tags": tags, "hue": hue, "sat": sat, "weather": weather,
                    }),
                ));

                let mut s = state.lock().await;
                s.tagged_count += 1;
                s.colored_count += 1;
                if *was_failed && s.failed_count > 0 {
                    s.failed_count -= 1;
                }
            }
            Err(e) => {
                warn!("analysis failed for {}: {e}", key);
                let conn = db.lock().await;
                let err_msg: String = e.to_string().chars().take(200).collect();
                let _ = conn.execute(
                    "UPDATE meta SET analysis_error = ?1, analyzed_by = COALESCE(analyzed_by, ?2) WHERE key = ?3",
                    rusqlite::params![err_msg, &ollama_model, key],
                );
                let mut s = state.lock().await;
                if !*was_failed {
                    s.failed_count += 1;
                }
            }
        }

        {
            let mut s = state.lock().await;
            s.batch.progress = i + 1;
            let elapsed = start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 && s.batch.progress > 0 {
                let per_item = elapsed / s.batch.progress as f64;
                let remaining = (s.batch.total - s.batch.progress) as f64 * per_item;
                s.eta = format_eta(remaining);
            }
        }

        broadcast_progress(&event_tx, &state).await;
    }

    {
        let mut s = state.lock().await;
        s.batch.running = false;
        s.last_log.clear();
        s.eta.clear();
    }

    let _ = event_tx.send(make_event("skwd.wall.analysis.complete", serde_json::json!({})));

    Ok(())
}

pub async fn stop(state: &Arc<Mutex<AnalysisState>>) {
    let mut s = state.lock().await;
    s.batch.cancel = true;
}

pub async fn regenerate(db: &Arc<Mutex<Connection>>) {
    let conn = db.lock().await;
    let _ = conn.execute(
        "UPDATE meta SET tags=NULL, colors=NULL, analyzed_by=NULL, analysis_error=NULL, weather=NULL",
        [],
    );
}


pub async fn retag_one(
    config: &Config,
    db: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<String>,
    key: &str,
) -> anyhow::Result<()> {
    let ollama_url = if config.ollama.url.is_empty() {
        DEFAULT_OLLAMA_URL.to_string()
    } else {
        config.ollama.url.clone()
    };
    let ollama_model = if config.ollama.model.is_empty() {
        DEFAULT_OLLAMA_MODEL.to_string()
    } else {
        config.ollama.model.clone()
    };
    let ollama_prompt = if config.ollama.prompt.trim().is_empty() {
        OLLAMA_PROMPT.to_string()
    } else {
        config.ollama.prompt.clone()
    };

    let thumb_path: Option<String> = {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT thumb FROM meta WHERE key = ?1",
            rusqlite::params![key],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
    };
    let thumb_path = thumb_path
        .ok_or_else(|| anyhow::anyhow!("no thumb path for key {key}"))?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    info!("retagging {key} from {thumb_path}");

    match analyze_one(&thumb_path, &ollama_url, &ollama_model, &ollama_prompt, &client).await {
        Ok((hue, sat, tags, colors_json, weather)) => {
            let tags_json = serde_json::to_string(&tags).unwrap_or_else(|_| "[]".into());
            let weather_json = serde_json::to_string(&weather).unwrap_or_else(|_| "[]".into());
            let conn = db.lock().await;
            let _ = db::update_analysis(
                &conn,
                key,
                Some(&tags_json),
                Some(&colors_json),
                Some(&ollama_model),
                Some(hue),
                Some(sat),
                Some(&weather_json),
            );
            let _ = conn.execute(
                "UPDATE meta SET analysis_error = NULL, tags_raw = ?1 WHERE key = ?2",
                rusqlite::params![tags_json, key],
            );
            drop(conn);

            let _ = event_tx.send(make_event(
                "skwd.wall.analysis.item",
                serde_json::json!({
                    "key": key, "tags": tags, "hue": hue, "sat": sat, "weather": weather,
                }),
            ));
            Ok(())
        }
        Err(e) => {
            let conn = db.lock().await;
            let err_msg: String = e.to_string().chars().take(200).collect();
            let _ = conn.execute(
                "UPDATE meta SET analysis_error = ?1 WHERE key = ?2",
                rusqlite::params![err_msg, key],
            );
            drop(conn);

            let _ = event_tx.send(make_event(
                "skwd.wall.analysis.item_failed",
                serde_json::json!({ "key": key, "error": err_msg }),
            ));
            Err(e)
        }
    }
}


async fn analyze_one(
    thumb_path: &str,
    ollama_url: &str,
    model: &str,
    prompt: &str,
    client: &reqwest::Client,
) -> anyhow::Result<(i64, i64, Vec<String>, String, Vec<String>)> {
    let image_bytes = tokio::fs::read(thumb_path)
        .await
        .map_err(|e| anyhow::anyhow!("read thumb: {e}"))?;
    use base64::Engine as _;
    let image_b64 = base64::engine::general_purpose::STANDARD.encode(&image_bytes);
    if image_b64.is_empty() {
        bail!("empty base64");
    }

    let body = serde_json::json!({
        "model": model,
        "prompt": prompt,
        "images": [image_b64],
        "stream": false,
    });

    let resp = client
        .post(format!("{ollama_url}/api/generate"))
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("ollama request: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        bail!("ollama failed (HTTP {status}): {text}");
    }

    let resp_json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parse ollama response: {e}"))?;

    if let Some(err_msg) = resp_json.get("error").and_then(|v| v.as_str()) {
        bail!("ollama error: {err_msg}");
    }

    let response_text = resp_json
        .get("response")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if response_text.is_empty() {
        bail!("empty ollama response");
    }

    parse_ollama_response(&response_text, thumb_path).await
}

async fn parse_ollama_response(
    text: &str,
    thumb_path: &str,
) -> anyhow::Result<(i64, i64, Vec<String>, String, Vec<String>)> {
    let (tags, weather) = parse_tags_and_weather(text);

    let (mut hue, mut sat) = (99i64, 0i64);
    if let Ok((extracted_hue, extracted_sat)) = extract_hue_from_thumb(thumb_path).await {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            hue = i64::from(thumb::hue_bucket(extracted_hue as u16, extracted_sat as u16));
            sat = extracted_sat as i64;
        }
    }

    let colors_json = serde_json::json!({"hue": hue, "saturation": sat}).to_string();

    Ok((hue, sat, tags, colors_json, weather))
}

fn parse_tags_and_weather(text: &str) -> (Vec<String>, Vec<String>) {
    let mut tag_line = None;
    let mut weather_line = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if !trimmed.contains(',') {
            continue;
        }
        if tag_line.is_none() {
            tag_line = Some(trimmed);
        } else if weather_line.is_none() {
            weather_line = Some(trimmed);
        }
    }

    let mut tags = Vec::new();
    if let Some(tl) = tag_line {
        let mut seen = HashSet::new();
        for raw in tl.split(',') {
            let tag = raw.trim().to_lowercase().trim_end_matches('.').to_string();
            if tag.is_empty() || tag.len() > 24 || tag.starts_with('-') {
                continue;
            }
            let tag = tag.replace(' ', "-");
            if seen.insert(tag.clone()) {
                tags.push(tag);
            }
            if tags.len() >= 30 {
                break;
            }
        }
    }

    let mut weather = Vec::new();
    if let Some(wl) = weather_line {
        for raw in wl.split(',') {
            let w = raw.trim().to_lowercase().trim_end_matches('.').to_string();
            if !w.is_empty() && w.len() <= 24 && !weather.contains(&w) {
                weather.push(w);
            }
        }
    }

    (tags, weather)
}

async fn extract_hue_from_thumb(path: &str) -> anyhow::Result<(f32, f32)> {
    let output = tokio::process::Command::new("magick")
        .args([
            path,
            "-resize",
            "1x1!",
            "-format",
            "%[fx:hue] %[fx:saturation]",
            "info:",
        ])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("magick hue: {e}"))?;

    let text = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = text.split_whitespace().collect();
    let hue = parts.first().and_then(|s| s.parse::<f32>().ok()).unwrap_or(0.0) * 360.0;
    let sat = parts.get(1).and_then(|s| s.parse::<f32>().ok()).unwrap_or(0.0) * 100.0;
    Ok((hue, sat))
}


fn format_eta(seconds: f64) -> String {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let s = seconds as u64;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m {}s", s / 60, s % 60)
    } else {
        format!("{}h {}m", s / 3600, (s % 3600) / 60)
    }
}

fn thumb_to_key(path: &str) -> String {
    let fname = path.rsplit('/').next().unwrap_or(path);
    fname.rsplit_once('.').map_or(fname, |(s, _)| s).to_string()
}

fn load_existing(conn: &Connection, model: &str) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
    let mut tags_set = HashSet::new();
    let mut colors_set = HashSet::new();
    let mut failed_set = HashSet::new();

    if let Ok(mut stmt) = conn.prepare("SELECT key, tags, colors, analysis_error FROM meta WHERE analyzed_by = ?1") {
        let _ = stmt
            .query_map(rusqlite::params![model], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map(|rows| {
                for r in rows.flatten() {
                    let (key, tags, colors, error) = r;
                    if tags.is_some() { tags_set.insert(key.clone()); }
                    if colors.is_some() { colors_set.insert(key.clone()); }
                    if error.is_some() { failed_set.insert(key); }
                }
            });
    }

    if let Ok(mut stmt) = conn.prepare("SELECT key FROM meta WHERE analysis_error IS NOT NULL") {
        let _ = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map(|rows| {
                for key in rows.flatten() {
                    failed_set.insert(key);
                }
            });
    }

    (tags_set, colors_set, failed_set)
}

async fn collect_thumbs(dir: &Path, result: &mut Vec<String>) {
    let thumb_exts: &[&str] = &["webp", "jpg", "jpeg", "png"];
    for path_str in crate::util::scan_dir_by_ext(dir, thumb_exts).await {
        result.push(path_str);
    }
}

async fn broadcast_progress(tx: &broadcast::Sender<String>, state: &Arc<Mutex<AnalysisState>>) {
    let s = state.lock().await;
    let _ = tx.send(make_event(
        "skwd.wall.analysis.progress",
        serde_json::json!({
            "running": s.batch.running,
            "progress": s.batch.progress,
            "total": s.batch.total,
            "taggedCount": s.tagged_count,
            "coloredCount": s.colored_count,
            "totalThumbs": s.total_thumbs,
            "failedCount": s.failed_count,
            "lastLog": s.last_log,
            "eta": s.eta,
        }),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tags_uses_first_comma_line_normalizes_and_dedups() {
        let text = "Here is the description\nMountain, Sunset , forest, Mountain, sky.\nsunny, clear";
        let (tags, weather) = parse_tags_and_weather(text);
        assert_eq!(tags, vec!["mountain", "sunset", "forest", "sky"]);
        assert_eq!(weather, vec!["sunny", "clear"]);
    }

    #[test]
    fn parse_tags_replaces_spaces_and_drops_too_long_or_dashes() {
        let (tags, _) = parse_tags_and_weather("city lights, -bad, this-tag-is-way-too-long-to-keep-here, ok");
        assert!(tags.contains(&"city-lights".to_string()));
        assert!(tags.contains(&"ok".to_string()));
        assert!(!tags.iter().any(|t| t.starts_with('-')));
        assert!(!tags.iter().any(|t| t.len() > 24));
    }

    #[test]
    fn parse_tags_empty_when_no_comma_lines() {
        let (tags, weather) = parse_tags_and_weather("just one word\nanother line");
        assert!(tags.is_empty());
        assert!(weather.is_empty());
    }

    // Contract: skwd-wall's WallpaperAnalysisService.qml reads these exact (camelCase) fields
    // from both analysis.status and the skwd.wall.analysis.progress event.
    #[tokio::test]
    async fn analysis_status_field_contract() {
        let h = crate::server::test_state();
        let r = h.dispatch("analysis.status", serde_json::json!({})).await.result.unwrap();
        for field in [
            "running", "progress", "total", "taggedCount", "coloredCount", "totalThumbs", "lastLog", "eta",
        ] {
            assert!(r.get(field).is_some(), "analysis.status missing field: {field}");
        }
    }

    #[tokio::test]
    async fn analysis_default_prompt_returns_prompt_string() {
        let h = crate::server::test_state();
        let r = h.dispatch("analysis.default_prompt", serde_json::json!({})).await.result.unwrap();
        assert!(r["prompt"].as_str().is_some_and(|s| !s.is_empty()));
    }

    #[tokio::test]
    async fn analyze_one_posts_image_and_parses_tags_over_http() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let dir = tempfile::tempdir().unwrap();
        let thumb = dir.path().join("thumb.png");
        tokio::fs::write(&thumb, b"not-a-real-image").await.unwrap();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/generate"))
            .and(body_string_contains("bm90LWEtcmVhbC1pbWFnZQ=="))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "response": "forest, mountain, sunset\nsunny, clear"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let (_hue, _sat, tags, colors_json, weather) =
            analyze_one(thumb.to_str().unwrap(), &server.uri(), "llava", "describe", &client)
                .await
                .unwrap();

        assert_eq!(tags, vec!["forest", "mountain", "sunset"]);
        assert_eq!(weather, vec!["sunny", "clear"]);
        assert!(colors_json.contains("\"hue\""));
        assert!(colors_json.contains("\"saturation\""));
    }

    #[tokio::test]
    async fn analyze_one_bails_on_ollama_error_field() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let dir = tempfile::tempdir().unwrap();
        let thumb = dir.path().join("thumb.png");
        tokio::fs::write(&thumb, b"bytes").await.unwrap();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/generate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "error": "model not found"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = analyze_one(thumb.to_str().unwrap(), &server.uri(), "llava", "p", &client)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("model not found"), "got: {err}");
    }

    #[tokio::test]
    async fn analyze_one_bails_on_http_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let dir = tempfile::tempdir().unwrap();
        let thumb = dir.path().join("thumb.png");
        tokio::fs::write(&thumb, b"bytes").await.unwrap();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/generate"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = analyze_one(thumb.to_str().unwrap(), &server.uri(), "llava", "p", &client)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ollama failed"), "got: {err}");
    }
}
