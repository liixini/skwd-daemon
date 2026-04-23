use std::sync::Mutex as StdMutex;
use std::time::Instant;

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::server::SharedState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LyricWord {
    pub word: String,
    pub start: i64,
    pub end: i64,
    #[serde(rename = "charStart")]
    pub char_start: usize,
    #[serde(rename = "charEnd")]
    pub char_end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LyricLine {
    pub text: String,
    pub start: i64,
    pub end: i64,
    pub words: Vec<LyricWord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LyricsData {
    enhanced: bool,
    lines: Vec<LyricLine>,
}

static MX_TOKEN: std::sync::LazyLock<StdMutex<Option<(String, Instant)>>> =
    std::sync::LazyLock::new(|| StdMutex::new(None));

const MX_TOKEN_TTL_SECS: u64 = 600;

pub fn get_lyrics(conn: &Connection, artist: &str, title: &str) -> Option<LyricsData> {
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT data, enhanced FROM lyrics WHERE artist=?1 AND title=?2",
            params![artist, title],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();

    let (data_json, enhanced_int) = row?;
    let lines: Vec<LyricLine> = serde_json::from_str(&data_json).ok()?;
    Some(LyricsData {
        enhanced: enhanced_int != 0,
        lines,
    })
}

pub fn upsert_lyrics(
    conn: &Connection,
    artist: &str,
    title: &str,
    enhanced: bool,
    lines: &[LyricLine],
) -> rusqlite::Result<()> {
    let data = serde_json::to_string(lines).unwrap_or_default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    conn.execute(
        "INSERT INTO lyrics(artist, title, enhanced, data, fetched_at)
         VALUES(?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(artist, title) DO UPDATE SET
           enhanced=excluded.enhanced, data=excluded.data, fetched_at=excluded.fetched_at",
        params![artist, title, i64::from(enhanced), data, now],
    )?;
    Ok(())
}

fn build_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("skwd-lyrics/1.0")
        .build()
}

async fn mx_ensure_token(client: &reqwest::Client) -> Option<String> {
    {
        let guard = MX_TOKEN.lock().unwrap();
        if let Some((ref token, ref fetched)) = *guard {
            if fetched.elapsed().as_secs() < MX_TOKEN_TTL_SECS {
                return Some(token.clone());
            }
        }
    }

    let resp: serde_json::Value = client
        .get("https://apic-desktop.musixmatch.com/ws/1.1/token.get")
        .query(&[
            ("app_id", "web-desktop-app-v1.0"),
            ("user_language", "en"),
        ])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let status = resp
        .pointer("/message/header/status_code")
        .and_then(|v| v.as_i64())?;
    if status == 401 {
        return None;
    }

    let token = resp
        .pointer("/message/body/user_token")
        .and_then(|v| v.as_str())?
        .to_string();

    {
        let mut guard = MX_TOKEN.lock().unwrap();
        *guard = Some((token.clone(), Instant::now()));
    }

    Some(token)
}

async fn mx_api_call(
    client: &reqwest::Client,
    action: &str,
    params: &[(&str, String)],
) -> Option<serde_json::Value> {
    let token = mx_ensure_token(client).await?;
    let mut query: Vec<(&str, String)> = params.to_vec();
    query.push(("app_id", "web-desktop-app-v1.0".into()));
    query.push(("usertoken", token));
    query.push(("t", chrono_or_now()));

    let url = format!(
        "https://apic-desktop.musixmatch.com/ws/1.1/{}",
        action
    );

    let resp: serde_json::Value = client
        .get(&url)
        .query(&query)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let status = resp
        .pointer("/message/header/status_code")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if status != 200 {
        return None;
    }

    resp.pointer("/message/body").cloned()
}

fn chrono_or_now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

fn normalize_token(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn tokenize(s: &str) -> Vec<String> {
    const STOP: &[&str] = &[
        "the", "a", "an", "and", "or", "of", "to", "in", "on", "at", "by",
        "for", "with", "from", "is", "it", "i", "im", "you", "me", "my",
        "feat", "ft", "featuring", "with", "vs", "remix", "edit", "version",
        "remastered", "remaster", "mix", "official", "video", "audio",
    ];
    s.split_whitespace()
        .map(normalize_token)
        .filter(|t| t.len() >= 3 && !STOP.contains(&t.as_str()))
        .collect()
}

fn token_overlap(target: &[String], candidate: &[String]) -> usize {
    target.iter().filter(|t| candidate.contains(t)).count()
}

fn mx_best_match(
    track_list: &[serde_json::Value],
    artist: &str,
    title: &str,
) -> Option<i64> {
    let target_artist_tokens = tokenize(artist);
    let target_title_tokens = tokenize(title);
    let need_artist = !target_artist_tokens.is_empty();
    let need_title = !target_title_tokens.is_empty();

    let mut best_id: Option<i64> = None;
    let mut best_score: usize = 0;

    for item in track_list {
        let Some(track) = item.get("track") else { continue };
        let name = track.get("track_name").and_then(|v| v.as_str()).unwrap_or("");
        let art = track.get("artist_name").and_then(|v| v.as_str()).unwrap_or("");

        let cand_artist_tokens = tokenize(art);
        let cand_title_tokens = tokenize(name);

        let artist_overlap = token_overlap(&target_artist_tokens, &cand_artist_tokens);
        let title_overlap = token_overlap(&target_title_tokens, &cand_title_tokens);

        if need_artist && artist_overlap == 0 {
            continue;
        }
        if need_title && title_overlap == 0 {
            continue;
        }

        let score = artist_overlap * 2 + title_overlap;
        if score > best_score {
            best_score = score;
            best_id = track.get("track_id").and_then(|v| v.as_i64());
        }
    }

    best_id
}

async fn fetch_musixmatch(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let search_term = format!("{} {}", artist, title);

    let body = mx_api_call(
        client,
        "track.search",
        &[
            ("q", search_term),
            ("page_size", "5".into()),
            ("page", "1".into()),
        ],
    )
    .await?;

    let track_list = body
        .get("track_list")
        .and_then(|v| v.as_array())?;

    if track_list.is_empty() {
        return None;
    }

    let track_id = mx_best_match(track_list, artist, title)?;

    if let Some(data) = mx_get_richsync(client, track_id).await {
        return Some(data);
    }

    mx_get_subtitle(client, track_id).await
}

async fn mx_get_richsync(client: &reqwest::Client, track_id: i64) -> Option<LyricsData> {
    let body = mx_api_call(
        client,
        "track.richsync.get",
        &[("track_id", track_id.to_string())],
    )
    .await?;

    let richsync_body = body
        .pointer("/richsync/richsync_body")
        .and_then(|v| v.as_str())?;

    let rich_data: Vec<serde_json::Value> = serde_json::from_str(richsync_body).ok()?;
    let lines = parse_richsync(&rich_data);

    if lines.is_empty() {
        return None;
    }

    Some(LyricsData {
        enhanced: true,
        lines,
    })
}

async fn mx_get_subtitle(client: &reqwest::Client, track_id: i64) -> Option<LyricsData> {
    let body = mx_api_call(
        client,
        "track.subtitle.get",
        &[
            ("track_id", track_id.to_string()),
            ("subtitle_format", "lrc".into()),
        ],
    )
    .await?;

    let lrc = body
        .pointer("/subtitle/subtitle_body")
        .and_then(|v| v.as_str())?;

    let (lines, enhanced) = parse_lrc(lrc);
    if lines.is_empty() {
        return None;
    }

    Some(LyricsData { enhanced, lines })
}

async fn fetch_lrclib(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    if let Some(data) = lrclib_get(client, artist, title).await {
        return Some(data);
    }
    lrclib_search(client, artist, title).await
}

async fn lrclib_get(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let resp: serde_json::Value = client
        .get("https://lrclib.net/api/get")
        .query(&[("artist_name", artist), ("track_name", title)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let synced = resp.get("syncedLyrics").and_then(|v| v.as_str())?;
    let (lines, enhanced) = parse_lrc(synced);
    if lines.is_empty() {
        return None;
    }
    Some(LyricsData { enhanced, lines })
}

async fn lrclib_search(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let query = format!("{} {}", artist, title);
    let results: Vec<serde_json::Value> = client
        .get("https://lrclib.net/api/search")
        .query(&[("q", &query)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let mut best_synced: Option<&str> = None;
    let mut best_enhanced: Option<&str> = None;

    for item in &results {
        if let Some(synced) = item.get("syncedLyrics").and_then(|v| v.as_str()) {
            if best_synced.is_none() {
                best_synced = Some(synced);
            }
            if best_enhanced.is_none() && is_enhanced_lrc(synced) {
                best_enhanced = Some(synced);
            }
        }
    }

    let lrc = best_enhanced.or(best_synced)?;
    let (lines, enhanced) = parse_lrc(lrc);
    if lines.is_empty() {
        return None;
    }
    Some(LyricsData { enhanced, lines })
}

async fn fetch_netease(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let search_term = format!("{} {}", artist, title);

    let resp: serde_json::Value = client
        .get("https://music.163.com/api/search/pc")
        .query(&[
            ("limit", "5"),
            ("type", "1"),
            ("offset", "0"),
            ("s", &search_term),
        ])
        .header("Referer", "https://music.163.com/")
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let songs = resp
        .pointer("/result/songs")
        .and_then(|v| v.as_array())?;

    let track_id = songs.first()?.get("id").and_then(|v| v.as_i64())?;

    let lyric_resp: serde_json::Value = client
        .get("https://music.163.com/api/song/lyric")
        .query(&[("id", &track_id.to_string()), ("lv", &"1".to_string())])
        .header("Referer", "https://music.163.com/")
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let lrc = lyric_resp
        .pointer("/lrc/lyric")
        .and_then(|v| v.as_str())?;

    let (lines, _) = parse_lrc(lrc);
    if lines.is_empty() {
        return None;
    }

    Some(LyricsData {
        enhanced: false,
        lines,
    })
}

fn is_enhanced_lrc(text: &str) -> bool {
    text.contains('<') && has_word_timestamp(text)
}

fn has_word_timestamp(text: &str) -> bool {
    let mut rest = text;
    while let Some(pos) = rest.find('<') {
        rest = &rest[pos + 1..];
        if let Some(end) = rest.find('>') {
            let inner = &rest[..end];
            if parse_timestamp(inner).is_some() {
                return true;
            }
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    false
}

fn parse_timestamp(s: &str) -> Option<i64> {
    let (min_s, sec_s) = s.trim().split_once(':')?;
    if !sec_s.contains('.') {
        return None;
    }
    let _: i64 = min_s.parse().ok()?;
    let _: f64 = sec_s.parse().ok()?;
    Some(ts_to_ms(s))
}

fn ts_to_ms(ts: &str) -> i64 {
    let parts: Vec<&str> = ts.trim().splitn(2, ':').collect();
    if parts.len() == 2 {
        let minutes: i64 = parts[0].parse().unwrap_or(0);
        let seconds: f64 = parts[1].parse().unwrap_or(0.0);
        ((minutes as f64 * 60.0 + seconds) * 1000.0).round() as i64
    } else {
        0
    }
}

fn parse_lrc(text: &str) -> (Vec<LyricLine>, bool) {
    if is_enhanced_lrc(text) {
        return (parse_enhanced_lrc(text), true);
    }
    parse_regular_lrc(text)
}

fn parse_regular_lrc(text: &str) -> (Vec<LyricLine>, bool) {
    let mut result = Vec::new();

    for raw in text.lines() {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let Some((ts, rest)) = parse_lrc_line_start(raw) else {
            continue;
        };
        let start = ts_to_ms(ts);
        let line_text = rest.trim();
        if line_text.is_empty() {
            continue;
        }
        result.push(LyricLine {
            text: line_text.to_string(),
            start,
            end: start + 3000,
            words: vec![],
        });
    }

    for i in 0..result.len().saturating_sub(1) {
        result[i].end = result[i + 1].start;
    }

    (result, false)
}

fn parse_lrc_line_start(line: &str) -> Option<(&str, &str)> {
    let line = line.trim();
    if !line.starts_with('[') {
        return None;
    }
    let close = line.find(']')?;
    let ts = &line[1..close];
    parse_timestamp(ts)?;
    let rest = &line[close + 1..];
    Some((ts, rest))
}

fn parse_enhanced_lrc(text: &str) -> Vec<LyricLine> {
    let mut result = Vec::new();

    for raw in text.lines() {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let Some((ts, rest)) = parse_lrc_line_start(raw) else {
            continue;
        };
        let line_start = ts_to_ms(ts);
        let rest = rest.trim();
        if rest.is_empty() {
            continue;
        }

        let word_matches = extract_word_timestamps(rest);

        if !word_matches.is_empty() {
            let full_text = strip_word_timestamps(rest);
            let mut parsed_words = Vec::new();
            let mut search_from = 0;

            for (i, (w_ts, w)) in word_matches.iter().enumerate() {
                let w_start = ts_to_ms(w_ts);
                let w_end = if i + 1 < word_matches.len() {
                    ts_to_ms(&word_matches[i + 1].0)
                } else {
                    w_start + std::cmp::max(200, (w.len() as i64) * 80)
                };

                let (char_start, char_end) = if let Some(pos) = full_text[search_from..].find(w.as_str()) {
                    let cs = search_from + pos;
                    let ce = cs + w.len();
                    search_from = ce;
                    (cs, ce)
                } else {
                    let cs = search_from;
                    let ce = search_from + w.len();
                    search_from = ce;
                    (cs, ce)
                };

                parsed_words.push(LyricWord {
                    word: w.clone(),
                    start: w_start,
                    end: w_end,
                    char_start,
                    char_end,
                });
            }

            let line_end = parsed_words
                .last()
                .map(|w| w.end)
                .unwrap_or(line_start + 3000);

            result.push(LyricLine {
                text: full_text,
                start: line_start,
                end: line_end,
                words: parsed_words,
            });
        } else {
            result.push(LyricLine {
                text: rest.to_string(),
                start: line_start,
                end: line_start + 3000,
                words: vec![],
            });
        }
    }

    for i in 0..result.len().saturating_sub(1) {
        let next_start = result[i + 1].start;
        if let Some(last_word) = result[i].words.last_mut() {
            last_word.end = next_start;
        }
        result[i].end = next_start;
    }

    result
}

fn extract_word_timestamps(text: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut rest = text;

    while let Some(open) = rest.find('<') {
        rest = &rest[open + 1..];
        let Some(close) = rest.find('>') else { break };
        let ts = &rest[..close];
        if parse_timestamp(ts).is_none() {
            rest = &rest[close + 1..];
            continue;
        }
        rest = &rest[close + 1..];

        let word_end = rest.find('<').unwrap_or(rest.len());
        let word = rest[..word_end].trim();
        if !word.is_empty() {
            result.push((ts.to_string(), word.to_string()));
        }
        rest = &rest[word_end..];
    }

    result
}

fn strip_word_timestamps(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(open) = rest.find('<') {
        result.push_str(&rest[..open]);
        rest = &rest[open + 1..];
        if let Some(close) = rest.find('>') {
            let inner = &rest[..close];
            if parse_timestamp(inner).is_none() {
                result.push('<');
                result.push_str(inner);
                result.push('>');
            }
            rest = &rest[close + 1..];
        } else {
            result.push('<');
        }
    }
    result.push_str(rest);
    result.trim().to_string()
}

fn parse_richsync(rich_data: &[serde_json::Value]) -> Vec<LyricLine> {
    let mut result = Vec::new();

    for line in rich_data {
        let line_start = (line.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0) * 1000.0).round() as i64;
        let line_end = (line.get("te").and_then(|v| v.as_f64()).unwrap_or(0.0) * 1000.0).round() as i64;

        let fragments = match line.get("l").and_then(|v| v.as_array()) {
            Some(f) => f,
            None => continue,
        };

        let mut full_text = String::new();
        let mut words = Vec::new();

        let ts_base = line.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0);

        for frag in fragments {
            let c = frag
                .get("c")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let offset = frag
                .get("o")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let w_start = ((ts_base + offset) * 1000.0).round() as i64;

            if c.trim().is_empty() {
                full_text.push_str(c);
            } else {
                let char_start = full_text.len();
                full_text.push_str(c);
                let char_end = full_text.len();
                words.push(LyricWord {
                    word: c.to_string(),
                    start: w_start,
                    end: w_start,
                    char_start,
                    char_end,
                });
            }
        }

        for i in 0..words.len().saturating_sub(1) {
            words[i].end = words[i + 1].start;
        }
        if let Some(last) = words.last_mut() {
            last.end = line_end;
        }

        let display = line
            .get("x")
            .and_then(|v| v.as_str())
            .unwrap_or(&full_text)
            .trim()
            .to_string();

        if !display.is_empty() {
            result.push(LyricLine {
                text: display,
                start: line_start,
                end: line_end,
                words,
            });
        }
    }

    result
}

pub async fn dispatch(
    req: &Request,
    _event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("lyrics.").unwrap_or(&req.method);

    match method {
        "get" => {
            let artist = req.str_param("artist", "").to_string();
            let title = req.str_param("title", "").to_string();
            if artist.is_empty() || title.is_empty() {
                return Response::err(req.id, -32602, "missing artist or title".to_string());
            }

            {
                let db = state.db.lock().await;
                if let Some(data) = get_lyrics(&db, &artist, &title) {
                    debug!("lyrics cache hit for {artist} - {title}");
                    return Response::ok(
                        req.id,
                        serde_json::json!({
                            "lines": data.lines,
                            "enhanced": data.enhanced,
                            "cached": true,
                        }),
                    );
                }
            }

            info!("fetching lyrics for {artist} - {title}");

            let client = match build_client() {
                Ok(c) => c,
                Err(e) => {
                    return Response::err(req.id, -32000, format!("http client error: {e}"));
                }
            };

            let (mx, lrc, ne) = tokio::join!(
                fetch_musixmatch(&client, &artist, &title),
                fetch_lrclib(&client, &artist, &title),
                fetch_netease(&client, &artist, &title),
            );

            let candidates: Vec<LyricsData> =
                [mx, lrc, ne].into_iter().flatten().collect();

            let best = candidates.into_iter().reduce(|a, b| {
                if b.enhanced && !a.enhanced {
                    b
                } else if a.enhanced && !b.enhanced {
                    a
                } else if b.lines.len() > a.lines.len() {
                    b
                } else {
                    a
                }
            });

            match best {
                Some(data) => {
                    let db = state.db.lock().await;
                    if let Err(e) = upsert_lyrics(&db, &artist, &title, data.enhanced, &data.lines)
                    {
                        warn!("failed to cache lyrics: {e}");
                    }

                    Response::ok(
                        req.id,
                        serde_json::json!({
                            "lines": data.lines,
                            "enhanced": data.enhanced,
                            "cached": false,
                        }),
                    )
                }
                None => Response::ok(
                    req.id,
                    serde_json::json!({
                        "lines": [],
                        "enhanced": false,
                        "notFound": true,
                    }),
                ),
            }
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}
