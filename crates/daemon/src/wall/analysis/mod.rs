use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{Mutex, broadcast};
use tracing::{info, warn};

use anyhow::bail;

use crate::config::Config;
use crate::db;
use crate::server::{SharedState, broadcast_event, make_event};
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

        "consolidate" => {
            let config = state.config.read().await.clone();
            if !config.ollama.consolidate_enabled {
                return Response::err(req.id, -32000, "tag consolidation is disabled".to_string());
            }
            let db = state.db_shared.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                let ollama_url = &config.ollama.url;
                let model = if config.ollama.consolidation_model.is_empty() {
                    &config.ollama.model
                } else {
                    &config.ollama.consolidation_model
                };
                let _ = broadcast_event(
                    &tx,
                    "skwd.wall.analysis.progress",
                    serde_json::json!({ "running": true, "lastLog": "Consolidating tags..." }),
                );
                match consolidate_tags(ollama_url, model, &db).await {
                    Ok(()) => info!("tag consolidation complete"),
                    Err(e) => warn!("tag consolidation failed: {e}"),
                }
                let _ = broadcast_event(&tx, "skwd.wall.analysis.complete", serde_json::json!({}));
            });
            Response::ok(req.id, serde_json::json!({"started": true}))
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
First line: dominant color from [red, orange, yellow, lime, green, teal, cyan, sky blue, blue, indigo, violet, pink, neutral] and saturation 0-100. Format: COLOR|NUMBER
Color tips: dark blue/navy=indigo, brown/sepia/earth=orange, purple=violet, light blue=sky blue. Use neutral ONLY for pure grayscale.
Second line: 8-12 comma-separated single-word tags for what you see.
Third line: what weather would this wallpaper be a good fit for? Pick from: clear, sunny, cloudy, rainy, snowy, stormy, foggy, windy. Comma-separated, all that apply.
Three lines only, nothing else.";

const DEFAULT_OLLAMA_URL: &str = "http://localhost:11434";
const DEFAULT_OLLAMA_MODEL: &str = "llava:latest";

const COLOR_ALIASES: &[(&str, i64)] = &[
    ("red", 0),
    ("orange", 1),
    ("yellow", 2),
    ("lime", 3),
    ("green", 4),
    ("teal", 5),
    ("cyan", 6),
    ("sky", 7),
    ("blue", 8),
    ("indigo", 9),
    ("violet", 10),
    ("purple", 10),
    ("pink", 11),
];


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
    let consolidation_model = if config.ollama.consolidation_model.is_empty() {
        ollama_model.clone()
    } else {
        config.ollama.consolidation_model.clone()
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

    let mut queue = Vec::new();
    for path in &thumbs {
        let key = thumb_to_key(path);
        if failed_keys.contains(&key) {
            continue;
        }
        let has_tags = existing_tags.contains(&key);
        let has_colors = existing_colors.contains(&key);
        if !has_tags || !has_colors {
            queue.push((path.clone(), key));
        }
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

    for (i, (path, key)) in queue.iter().enumerate() {
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

        match analyze_one(path, &ollama_url, &ollama_model, &client).await {
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
                s.failed_count += 1;
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

    if config.ollama.consolidate_enabled {
        {
            let mut s = state.lock().await;
            s.last_log = "Consolidating tags...".into();
            s.eta.clear();
        }
        broadcast_progress(&event_tx, &state).await;

        if let Err(e) = consolidate_tags(&ollama_url, &consolidation_model, &db).await {
            warn!("tag consolidation failed: {e}");
        }
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


pub async fn consolidate_tags(ollama_url: &str, model: &str, db: &Arc<Mutex<Connection>>) -> anyhow::Result<()> {
    let all_tags: Vec<String> = {
        let conn = db.lock().await;
        let mut stmt = conn
            .prepare("SELECT tags FROM meta WHERE tags IS NOT NULL")
            .map_err(|e| anyhow::anyhow!(e))?;
        let tag_set: HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .flat_map(|raw| serde_json::from_str::<Vec<String>>(&raw).unwrap_or_default())
            .collect();
        let mut v: Vec<String> = tag_set.into_iter().collect();
        v.sort();
        v
    };

    if all_tags.len() <= 1 {
        return Ok(());
    }

    const CHUNK_SIZE: usize = 200;
    let mut remap: HashMap<String, String> = HashMap::new();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    for (ci, chunk) in all_tags.chunks(CHUNK_SIZE).enumerate() {
        info!(
            "consolidation chunk {}/{}: {} tags",
            ci + 1,
            all_tags.len().div_ceil(CHUNK_SIZE),
            chunk.len()
        );

        let groups = match consolidate_chunk(ollama_url, model, chunk, &client).await {
            Ok(g) => g,
            Err(e) => {
                warn!("consolidation chunk {} failed, skipping: {e}", ci + 1);
                continue;
            }
        };

        for (canonical, members) in &groups {
            let canon = canonical.to_lowercase();
            for member in members {
                let m = member.to_lowercase();
                if m != canon {
                    remap.insert(m, canon.clone());
                }
            }
        }
    }

    if remap.is_empty() {
        return Ok(());
    }

    info!("consolidating {} synonym mappings", remap.len());

    let conn = db.lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT key, tags FROM meta \
             WHERE tags IS NOT NULL \
             AND (tags_raw IS NULL OR tags = tags_raw)",
        )
        .map_err(|e| anyhow::anyhow!(e))?;

    let rows: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
        .map_err(|e| anyhow::anyhow!(e))?
        .filter_map(std::result::Result::ok)
        .collect();
    drop(stmt);

    let mut updated = 0usize;
    for (key, raw_tags) in &rows {
        if let Ok(tags) = serde_json::from_str::<Vec<String>>(raw_tags) {
            let mut seen = HashSet::new();
            let mut new_tags = Vec::new();
            let mut changed = false;
            for tag in &tags {
                let mapped = remap.get(tag.as_str()).unwrap_or(tag).clone();
                if mapped != *tag {
                    changed = true;
                }
                if seen.insert(mapped.clone()) {
                    new_tags.push(mapped);
                }
            }
            if new_tags.len() != tags.len() {
                changed = true;
            }

            if changed {
                let new_json = serde_json::to_string(&new_tags).unwrap_or_else(|_| raw_tags.clone());
                let _ = conn.execute(
                    "UPDATE meta SET tags = ?1, tags_raw = COALESCE(tags_raw, ?2) WHERE key = ?3",
                    rusqlite::params![new_json, raw_tags, key],
                );
                updated += 1;
            }
        }
    }

    info!(
        "tag consolidation: updated {updated} unconsolidated entries ({} skipped)",
        rows.len().saturating_sub(updated)
    );
    Ok(())
}

async fn consolidate_chunk(
    ollama_url: &str,
    model: &str,
    tags: &[String],
    client: &reqwest::Client,
) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let tag_list = tags.join(", ");
    let prompt = format!(
        "Here is a list of tags used to describe wallpapers:\n{tag_list}\n\n\
         Group tags that mean the same thing or are very similar (synonyms, plurals, \
         spelling variants). For each group pick the single best canonical tag.\n\
         Return ONLY a JSON object where each key is the canonical tag and the value \
         is an array of all tags in that group (including the canonical one). \
         Tags that have no synonyms should be omitted. Example:\n\
         {{\"mountain\":[\"mountain\",\"mountains\",\"peak\"],\"peaceful\":[\"peaceful\",\"serene\",\"calm\"]}}\n\
         JSON only, nothing else."
    );

    let body = serde_json::json!({
        "model": model,
        "prompt": prompt,
        "stream": false,
        "options": { "num_ctx": 8192 },
    });

    let resp = client
        .post(format!("{ollama_url}/api/generate"))
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("ollama consolidate request: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        bail!("ollama consolidate failed (HTTP {status}): {text}");
    }

    let resp_json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parse consolidation response: {e}"))?;

    if let Some(err_msg) = resp_json.get("error").and_then(|v| v.as_str()) {
        bail!("ollama consolidation error: {err_msg}");
    }

    let response_text = resp_json.get("response").and_then(|v| v.as_str()).unwrap_or("").trim();

    let json_str = extract_json_block(response_text);

    let groups: HashMap<String, Vec<String>> = serde_json::from_str(&json_str)
        .map_err(|e| anyhow::anyhow!("parse consolidation JSON: {e} — raw: {json_str}"))?;

    Ok(groups)
}


async fn analyze_one(
    thumb_path: &str,
    ollama_url: &str,
    model: &str,
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
        "prompt": OLLAMA_PROMPT,
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
    let lines: Vec<&str> = text.lines().collect();

    let mut color_line = None;
    let mut tag_line = None;
    let mut weather_line = None;

    for line in &lines {
        let trimmed = line.trim();
        if trimmed.contains('|') && color_line.is_none() {
            color_line = Some(trimmed);
        } else if trimmed.contains(',') && tag_line.is_none() {
            tag_line = Some(trimmed);
        } else if trimmed.contains(',') && weather_line.is_none() {
            weather_line = Some(trimmed);
        }
    }

    let (mut hue, mut sat) = (99i64, 0i64);
    if let Some(cl) = color_line {
        let parts: Vec<&str> = cl.split('|').collect();
        if parts.len() >= 2 {
            let color_name = parts[0].trim();
            hue = color_to_hue(color_name);
            sat = parts[1].trim().parse::<i64>().unwrap_or(0).clamp(0, 100);
        }
    }

    if hue == 99
        && let Ok((extracted_hue, extracted_sat)) = extract_hue_from_thumb(thumb_path).await
    {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            hue = i64::from(thumb::hue_bucket(extracted_hue as u16, extracted_sat as u16));
            sat = extracted_sat as i64;
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
            if tags.len() >= 20 {
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

    let colors_json = serde_json::json!({"hue": hue, "saturation": sat}).to_string();

    Ok((hue, sat, tags, colors_json, weather))
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


fn color_to_hue(name: &str) -> i64 {
    let n = name.to_lowercase();
    match n.as_str() {
        "red" | "crimson" | "scarlet" | "maroon" | "burgundy" | "wine" => 0,
        "orange" | "amber" | "coral" | "peach" | "brown" | "rust" | "copper" | "sepia" | "tan" => 1,
        "yellow" | "gold" | "golden" | "beige" | "cream" => 2,
        "lime" | "chartreuse" | "yellow-green" => 3,
        "green" | "emerald" | "olive" | "mint" | "forest" | "dark green" | "neon" => 4,
        "teal" | "sea green" | "aqua" => 5,
        "cyan" | "turquoise" => 6,
        "sky blue" | "sky" | "light blue" => 7,
        "blue" | "cobalt" => 8,
        "navy" | "dark blue" | "indigo" | "dark purple" => 9,
        "violet" | "purple" | "magenta" | "lavender" | "lilac" | "plum" => 10,
        "pink" | "rose" | "fuchsia" | "hot pink" | "salmon" => 11,
        "neutral" | "gray" | "grey" | "black" | "white" | "grayscale" | "monochrome" => 99,
        _ => COLOR_ALIASES
            .iter()
            .find(|(alias, _)| n.contains(alias) || alias.contains(n.as_str()))
            .map_or(99, |(_, hue)| *hue),
    }
}

fn extract_json_block(text: &str) -> String {
    if let Some(start) = text.find('{')
        && let Some(end) = text.rfind('}')
    {
        return text[start..=end].to_string();
    }
    text.to_string()
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
    let stem = fname.rsplit_once('.').map_or(fname, |(s, _)| s);
    if path.contains("/we-thumbs/") {
        format!("we:{stem}")
    } else if path.contains("/video-thumbs/") {
        format!("video:{stem}")
    } else {
        format!("static:{stem}")
    }
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
