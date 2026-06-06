use std::sync::Mutex as StdMutex;
use std::time::Instant;


use super::*;

static MX_TOKEN: std::sync::LazyLock<StdMutex<Option<(String, Instant)>>> =
    std::sync::LazyLock::new(|| StdMutex::new(None));

const MX_TOKEN_TTL_SECS: u64 = 600;

const MX_BASE: &str = "https://apic-desktop.musixmatch.com/ws/1.1";

pub(super) fn build_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("skwd-lyrics/1.0")
        .build()
}

pub(super) async fn mx_ensure_token_from(client: &reqwest::Client, base: &str) -> Option<String> {
    {
        let guard = MX_TOKEN.lock().unwrap();
        if let Some((ref token, ref fetched)) = *guard
            && fetched.elapsed().as_secs() < MX_TOKEN_TTL_SECS {
                return Some(token.clone());
            }
    }

    let resp: serde_json::Value = client
        .get(format!("{base}/token.get"))
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
        .and_then(serde_json::Value::as_i64)?;
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

pub(super) async fn mx_api_call_from(
    client: &reqwest::Client,
    base: &str,
    action: &str,
    params: &[(&str, String)],
) -> Option<serde_json::Value> {
    let token = mx_ensure_token_from(client, base).await?;
    let mut query: Vec<(&str, String)> = params.to_vec();
    query.push(("app_id", "web-desktop-app-v1.0".into()));
    query.push(("usertoken", token));
    query.push(("t", chrono_or_now()));

    let url = format!("{base}/{action}");

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
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if status != 200 {
        return None;
    }

    resp.pointer("/message/body").cloned()
}

pub(super) fn chrono_or_now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

pub(super) fn normalize_token(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn tokenize(s: &str) -> Vec<String> {
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

pub(super) fn token_overlap(target: &[String], candidate: &[String]) -> usize {
    target.iter().filter(|t| candidate.contains(t)).count()
}

pub(super) fn mx_best_match(
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
            best_id = track.get("track_id").and_then(serde_json::Value::as_i64);
        }
    }

    best_id
}

pub(super) async fn fetch_musixmatch(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    fetch_musixmatch_from(client, MX_BASE, artist, title).await
}

pub(super) async fn fetch_musixmatch_from(
    client: &reqwest::Client,
    base: &str,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let search_term = format!("{artist} {title}");

    let body = mx_api_call_from(
        client,
        base,
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

    if let Some(data) = mx_get_richsync_from(client, base, track_id).await {
        return Some(data);
    }

    mx_get_subtitle_from(client, base, track_id).await
}

pub(super) async fn mx_get_richsync_from(
    client: &reqwest::Client,
    base: &str,
    track_id: i64,
) -> Option<LyricsData> {
    let body = mx_api_call_from(
        client,
        base,
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

pub(super) async fn mx_get_subtitle_from(
    client: &reqwest::Client,
    base: &str,
    track_id: i64,
) -> Option<LyricsData> {
    let body = mx_api_call_from(
        client,
        base,
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

pub(super) async fn fetch_lrclib(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    if let Some(data) = lrclib_get(client, artist, title).await {
        return Some(data);
    }
    lrclib_search(client, artist, title).await
}

const LRCLIB_BASE: &str = "https://lrclib.net";

pub(super) async fn lrclib_get(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    lrclib_get_from(client, LRCLIB_BASE, artist, title).await
}

pub(super) async fn lrclib_get_from(
    client: &reqwest::Client,
    base: &str,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let resp: serde_json::Value = client
        .get(format!("{base}/api/get"))
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

pub(super) async fn lrclib_search(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    lrclib_search_from(client, LRCLIB_BASE, artist, title).await
}

pub(super) async fn lrclib_search_from(
    client: &reqwest::Client,
    base: &str,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let query = format!("{artist} {title}");
    let results: Vec<serde_json::Value> = client
        .get(format!("{base}/api/search"))
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

const NETEASE_BASE: &str = "https://music.163.com";

pub(super) async fn fetch_netease(
    client: &reqwest::Client,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    fetch_netease_from(client, NETEASE_BASE, artist, title).await
}

pub(super) async fn fetch_netease_from(
    client: &reqwest::Client,
    base: &str,
    artist: &str,
    title: &str,
) -> Option<LyricsData> {
    let search_term = format!("{artist} {title}");

    let resp: serde_json::Value = client
        .get(format!("{base}/api/search/pc"))
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

    let track_id = songs.first()?.get("id").and_then(serde_json::Value::as_i64)?;

    let lyric_resp: serde_json::Value = client
        .get(format!("{base}/api/song/lyric"))
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
