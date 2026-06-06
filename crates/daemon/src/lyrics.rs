
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use skwd_proto::{Request, Response};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::server::SharedState;

mod parse;
mod providers;
use parse::*;
use providers::*;

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


pub enum CachedLyrics {
    Hit(LyricsData),
    KnownMiss,
}

pub fn lookup_lyrics(conn: &Connection, artist: &str, title: &str) -> Option<CachedLyrics> {
    let row: Option<(String, i64, i64)> = conn
        .query_row(
            "SELECT data, enhanced, not_found FROM lyrics WHERE artist=?1 AND title=?2",
            params![artist, title],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok();

    let (data_json, enhanced_int, not_found_int) = row?;
    if not_found_int != 0 {
        return Some(CachedLyrics::KnownMiss);
    }
    let lines: Vec<LyricLine> = serde_json::from_str(&data_json).ok()?;
    Some(CachedLyrics::Hit(LyricsData {
        enhanced: enhanced_int != 0,
        lines,
    }))
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
        "INSERT INTO lyrics(artist, title, enhanced, data, fetched_at, not_found)
         VALUES(?1, ?2, ?3, ?4, ?5, 0)
         ON CONFLICT(artist, title) DO UPDATE SET
           enhanced=excluded.enhanced, data=excluded.data, fetched_at=excluded.fetched_at, not_found=0",
        params![artist, title, i64::from(enhanced), data, now],
    )?;
    Ok(())
}

pub fn mark_not_found(conn: &Connection, artist: &str, title: &str) -> rusqlite::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    conn.execute(
        "INSERT INTO lyrics(artist, title, enhanced, data, fetched_at, not_found)
         VALUES(?1, ?2, 0, '[]', ?3, 1)
         ON CONFLICT(artist, title) DO UPDATE SET
           data='[]', enhanced=0, fetched_at=excluded.fetched_at, not_found=1",
        params![artist, title, now],
    )?;
    Ok(())
}


pub async fn dispatch(
    req: &Request,
    _event_tx: &broadcast::Sender<String>,
    state: &SharedState,
) -> Response {
    let method = req.method.strip_prefix("lyrics.").unwrap_or(&req.method);

    match method {
        "peek" => {
            let artist = req.str_param("artist", "").to_string();
            let title = req.str_param("title", "").to_string();
            if artist.is_empty() || title.is_empty() {
                return Response::err(req.id, -32602, "missing artist or title".to_string());
            }
            let db = state.db.lock().await;
            match lookup_lyrics(&db, &artist, &title) {
                Some(CachedLyrics::Hit(data)) => Response::ok(
                    req.id,
                    serde_json::json!({
                        "lines": data.lines,
                        "enhanced": data.enhanced,
                        "cached": true,
                    }),
                ),
                Some(CachedLyrics::KnownMiss) => Response::ok(
                    req.id,
                    serde_json::json!({ "cached": true, "notFound": true }),
                ),
                None => Response::ok(req.id, serde_json::json!({ "cacheMiss": true })),
            }
        }

        "get" | "fetch" => {
            let artist = req.str_param("artist", "").to_string();
            let title = req.str_param("title", "").to_string();
            if artist.is_empty() || title.is_empty() {
                return Response::err(req.id, -32602, "missing artist or title".to_string());
            }

            {
                let db = state.db.lock().await;
                match lookup_lyrics(&db, &artist, &title) {
                    Some(CachedLyrics::Hit(data)) => {
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
                    Some(CachedLyrics::KnownMiss) => {
                        debug!("lyrics known-miss for {artist} - {title}");
                        return Response::ok(
                            req.id,
                            serde_json::json!({
                                "lines": [],
                                "enhanced": false,
                                "cached": true,
                                "notFound": true,
                            }),
                        );
                    }
                    None => {}
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
                None => {
                    let db = state.db.lock().await;
                    if let Err(e) = mark_not_found(&db, &artist, &title) {
                        warn!("failed to record lyrics not-found: {e}");
                    }
                    Response::ok(
                        req.id,
                        serde_json::json!({
                            "lines": [],
                            "enhanced": false,
                            "cached": false,
                            "notFound": true,
                        }),
                    )
                }
            }
        }

        _ => Response::err(req.id, -32601, format!("unknown method: {}", req.method)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn toks(words: &[&str]) -> Vec<String> {
        words.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn normalize_token_strips_non_alnum_and_lowercases() {
        assert_eq!(normalize_token("Hello!"), "hello");
        assert_eq!(normalize_token("R.E.M."), "rem");
        assert_eq!(normalize_token("café"), "café");
        assert_eq!(normalize_token("123-abc"), "123abc");
    }

    #[test]
    fn tokenize_drops_short_and_stopwords() {
        assert_eq!(tokenize("The Official Video"), Vec::<String>::new());
        assert_eq!(tokenize("Bohemian Rhapsody"), toks(&["bohemian", "rhapsody"]));
        assert_eq!(tokenize("a I im of"), Vec::<String>::new());
    }

    #[test]
    fn token_overlap_counts_shared_tokens() {
        let a = toks(&["bohemian", "rhapsody"]);
        let b = toks(&["rhapsody", "live"]);
        assert_eq!(token_overlap(&a, &b), 1);
        assert_eq!(token_overlap(&a, &toks(&["nothing"])), 0);
    }

    #[test]
    fn parse_timestamp_requires_colon_and_fraction() {
        assert_eq!(parse_timestamp("01:23.45"), Some(83450));
        assert_eq!(parse_timestamp("00:00.00"), Some(0));
        assert_eq!(parse_timestamp("01:23"), None);
        assert_eq!(parse_timestamp("9999"), None);
        assert_eq!(parse_timestamp("ab:cd.ef"), None);
    }

    #[test]
    fn ts_to_ms_converts_minutes_and_seconds() {
        assert_eq!(ts_to_ms("01:30.5"), 90500);
        assert_eq!(ts_to_ms("00:01.000"), 1000);
        assert_eq!(ts_to_ms("garbage"), 0);
    }

    #[test]
    fn enhanced_detection_needs_word_timestamps() {
        assert!(is_enhanced_lrc("[00:00.00]<00:00.40>Hi"));
        assert!(!is_enhanced_lrc("[00:00.00]Hi"));
        assert!(!is_enhanced_lrc("plain <tag> no timestamp"));
        assert!(has_word_timestamp("<00:01.50>word"));
        assert!(!has_word_timestamp("<notatime>word"));
    }

    #[test]
    fn parse_lrc_line_start_splits_timestamp_and_text() {
        assert_eq!(
            parse_lrc_line_start("[00:12.34]Hello world"),
            Some(("00:12.34", "Hello world"))
        );
        assert_eq!(parse_lrc_line_start("no bracket"), None);
        assert_eq!(parse_lrc_line_start("[xx]text"), None);
    }

    #[test]
    fn parse_regular_lrc_chains_line_end_to_next_start() {
        let (lines, enhanced) = parse_regular_lrc("[00:00.00]First\n[00:03.00]Second\n");
        assert!(!enhanced);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].text, "First");
        assert_eq!(lines[0].start, 0);
        assert_eq!(lines[0].end, 3000);
        assert_eq!(lines[1].start, 3000);
        assert!(lines[1].words.is_empty());
    }

    #[test]
    fn extract_and_strip_word_timestamps_are_consistent() {
        let line = "<00:00.00>Hello <00:00.50>world";
        let matches = extract_word_timestamps(line);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0], ("00:00.00".to_string(), "Hello".to_string()));
        assert_eq!(matches[1], ("00:00.50".to_string(), "world".to_string()));
        assert_eq!(strip_word_timestamps(line), "Hello world");
    }

    #[test]
    fn parse_enhanced_lrc_builds_word_offsets() {
        let lines = parse_enhanced_lrc("[00:00.00]<00:00.00>Hello <00:00.50>world");
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].text, "Hello world");
        assert_eq!(lines[0].words.len(), 2);
        assert_eq!(lines[0].words[0].word, "Hello");
        assert_eq!(lines[0].words[0].start, 0);
        assert_eq!((lines[0].words[0].char_start, lines[0].words[0].char_end), (0, 5));
        assert_eq!(lines[0].words[1].start, 500);
        assert_eq!(lines[0].words[1].char_start, 6);
    }

    #[test]
    fn parse_richsync_maps_fragments_to_words() {
        let data = json!([
            {
                "ts": 1.0,
                "te": 2.0,
                "l": [
                    { "c": "Hi", "o": 0.0 },
                    { "c": " ", "o": 0.2 },
                    { "c": "there", "o": 0.3 }
                ]
            }
        ]);
        let lines = parse_richsync(data.as_array().unwrap());
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].text, "Hi there");
        assert_eq!(lines[0].start, 1000);
        assert_eq!(lines[0].end, 2000);
        assert_eq!(lines[0].words.len(), 2);
        assert_eq!(lines[0].words[0].word, "Hi");
        assert_eq!(lines[0].words[0].start, 1000);
        assert_eq!(lines[0].words[0].end, 1300);
        assert_eq!(lines[0].words[1].word, "there");
        assert_eq!(lines[0].words[1].start, 1300);
        assert_eq!(lines[0].words[1].end, 2000);
        assert_eq!((lines[0].words[1].char_start, lines[0].words[1].char_end), (3, 8));
    }

    #[test]
    fn mx_best_match_scores_artist_double() {
        let list = json!([
            { "track": { "track_name": "Bohemian Rhapsody", "artist_name": "Queen", "track_id": 111 } },
            { "track": { "track_name": "Something Else", "artist_name": "Other Band", "track_id": 222 } }
        ]);
        let id = mx_best_match(list.as_array().unwrap(), "Queen", "Bohemian Rhapsody");
        assert_eq!(id, Some(111));
    }

    #[test]
    fn mx_best_match_rejects_when_no_overlap() {
        let list = json!([
            { "track": { "track_name": "Totally Unrelated", "artist_name": "Nobody", "track_id": 5 } }
        ]);
        assert_eq!(mx_best_match(list.as_array().unwrap(), "Queen", "Bohemian Rhapsody"), None);
    }

    #[tokio::test]
    async fn lrclib_get_from_parses_synced_lyrics_over_http() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/get"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "syncedLyrics": "[00:01.00]hello\n[00:03.00]world"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let data = lrclib_get_from(&client, &server.uri(), "artist", "title").await.unwrap();
        assert!(!data.enhanced);
        assert_eq!(data.lines.len(), 2);
        assert_eq!(data.lines[0].text, "hello");
        assert_eq!(data.lines[0].start, 1000);
    }

    #[tokio::test]
    async fn lrclib_get_from_returns_none_on_error_status() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/get"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        assert!(lrclib_get_from(&client, &server.uri(), "a", "t").await.is_none());
    }

    #[tokio::test]
    async fn lrclib_search_from_prefers_enhanced_lrc() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                { "syncedLyrics": "[00:01.00]plain line" },
                { "syncedLyrics": "[00:02.00]<00:02.00>word <00:02.50>timed" }
            ])))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let data = lrclib_search_from(&client, &server.uri(), "a", "t").await.unwrap();
        assert!(data.enhanced);
    }

    #[tokio::test]
    async fn fetch_musixmatch_from_search_then_subtitle_over_http() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token.get"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "message": { "header": { "status_code": 200 }, "body": { "user_token": "tok" } }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/track.search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "message": { "header": { "status_code": 200 }, "body": { "track_list": [
                    { "track": { "track_id": 42, "track_name": "Bohemian Rhapsody", "artist_name": "Queen" } }
                ] } }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/track.richsync.get"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "message": { "header": { "status_code": 200 }, "body": {} }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/track.subtitle.get"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "message": { "header": { "status_code": 200 }, "body": {
                    "subtitle": { "subtitle_body": "[00:01.00]hello\n[00:03.00]world" }
                } }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let data = fetch_musixmatch_from(&client, &server.uri(), "Queen", "Bohemian Rhapsody")
            .await
            .unwrap();
        assert_eq!(data.lines.len(), 2);
        assert_eq!(data.lines[0].text, "hello");
        assert_eq!(data.lines[0].start, 1000);
    }

    #[tokio::test]
    async fn fetch_netease_from_search_then_lyric_over_http() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/pc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": { "songs": [ { "id": 99 } ] }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/song/lyric"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "lrc": { "lyric": "[00:01.00]hi\n[00:02.00]there" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let data = fetch_netease_from(&client, &server.uri(), "artist", "some title")
            .await
            .unwrap();
        assert_eq!(data.lines.len(), 2);
        assert!(!data.enhanced);
        assert_eq!(data.lines[1].text, "there");
    }

    #[tokio::test]
    async fn fetch_netease_from_returns_none_when_no_songs() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/pc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": { "songs": [] }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        assert!(fetch_netease_from(&client, &server.uri(), "a", "t").await.is_none());
    }

    // Contract: skwd-wall's LyricsService.qml reads lines/enhanced/notFound off lyrics.peek,
    // and relies on the cached/cacheMiss distinction to decide whether to fetch.
    #[tokio::test]
    async fn lyrics_peek_response_contract() {
        let h = crate::server::test_state();

        assert_eq!(
            h.dispatch("lyrics.peek", serde_json::json!({})).await.error.unwrap().code,
            -32602
        );

        let miss = h
            .dispatch("lyrics.peek", serde_json::json!({ "artist": "a", "title": "t" }))
            .await
            .result
            .unwrap();
        assert_eq!(miss["cacheMiss"], true);

        {
            let db = h.state.db.try_lock().unwrap();
            let lines = vec![LyricLine { text: "hello".into(), start: 0, end: 1000, words: vec![] }];
            upsert_lyrics(&db, "a", "t", false, &lines).unwrap();
            mark_not_found(&db, "b", "u").unwrap();
        }

        let hit = h
            .dispatch("lyrics.peek", serde_json::json!({ "artist": "a", "title": "t" }))
            .await
            .result
            .unwrap();
        assert_eq!(hit["enhanced"], false);
        assert_eq!(hit["cached"], true);
        assert_eq!(hit["lines"][0]["text"], "hello");

        let known = h
            .dispatch("lyrics.peek", serde_json::json!({ "artist": "b", "title": "u" }))
            .await
            .result
            .unwrap();
        assert_eq!(known["notFound"], true);
        assert_eq!(known["cached"], true);
    }
}
