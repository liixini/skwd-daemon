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
You are tagging an image for a wallpaper browser where the user filters a large collection by what they want to look at right now.

OUTPUT EXACTLY THREE LINES, NOTHING ELSE.

LINE 1 — dominant color and saturation
Format: COLOR|NUMBER (e.g. teal|62)
Color from this list: red, orange, yellow, lime, green, teal, cyan, sky blue, blue, indigo, violet, pink, neutral
Mapping hints: dark blue / navy → indigo, brown / sepia / earth tones → orange, purple → violet, light blue → sky blue. Use 'neutral' ONLY for pure grayscale.
Saturation: 0 (grayscale) to 100 (very vivid).

LINE 2 — 8 to 12 lowercase comma-separated tags
Cover the dimensions below WHEN APPLICABLE. Skip a dimension if it doesn't fit.
  - subject: the main thing depicted (forest, mountain, city, woman, dragon, spaceship, cat, building, road, tree, flower, ship, robot, road, person, character)
  - style: visual treatment (anime, illustration, photo, painting, render, 3d, pixelart, sketch, abstract, minimalist, cyberpunk, vaporwave, fantasy, scifi, horror, retro, realistic, surreal)
  - mood: emotional feel (peaceful, melancholy, ominous, epic, dreamy, vibrant, lonely, intimate, energetic, mysterious, cozy, somber, hopeful, eerie)
  - lighting / time: when applicable (sunset, sunrise, night, twilight, golden, neon, moonlit, overcast, dramatic)
  - setting: when applicable (indoor, outdoor, underwater, space, urban, rural, forest, desert, beach, mountain, futuristic, medieval, ancient, post-apocalyptic, abandoned)
  - notable details: when present (fog, snow, rain, fire, water, reflection, silhouette, closeup, panorama)
Tag rules:
  - lowercase only, single English word per tag (use a hyphen for unavoidable two-word concepts: post-apocalyptic, sci-fi)
  - no duplicates, no color words (line 1 covers color), no generic filler ('image', 'wallpaper', 'background', 'scene', 'view')
  - if the image is in anime, manga, or Japanese-cartoon visual style, ALWAYS include 'anime'
  - prefer concrete and searchable over generic ('cyberpunk' over 'futuristic-stuff', 'sunset' over 'orange-light')

Quality tags — these augment the metric-based sort modes so users can filter for them.
ALWAYS evaluate each independently and include the matching tag(s):
  - 'minimalist' — sparse composition, lots of negative space, single subject on a flat/simple background, very few visual elements. NOT just low color count; also requires compositional simplicity.
  - 'colourful' — five or more distinct, well-distributed colors throughout the image (not just one accent color in a sea of black). Multi-hue artwork, rainbow palettes, busy illustrations qualify.
  - 'vibrant' — saturated, high-energy, eye-catching colors regardless of how many. Neon scenes, fully-saturated cartoons, vivid sunsets qualify; pastel and muted images do NOT.
A wallpaper can have multiple of these (a vivid abstract can be both colourful and vibrant); pick whichever genuinely apply.

Examples:
  forest at sunset, photo → trees, forest, sunset, golden, peaceful, outdoor, photo, nature, vibrant
  anime girl in a neon city at night → anime, character, city, neon, night, cyberpunk, illustration, vibrant, colourful
  abstract gradient → abstract, gradient, minimalist, dreamy, smooth, render
  solid black wallpaper with one red sphere → minimalist, sphere, render, dramatic

LINE 3 — weather fit
Which weather conditions would this wallpaper match? Comma-separated subset of: clear, sunny, cloudy, rainy, snowy, stormy, foggy, windy.";

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

    match analyze_one(&thumb_path, &ollama_url, &ollama_model, &client).await {
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
            Err(e)
        }
    }
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
