#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use super::*;

pub(super) fn is_enhanced_lrc(text: &str) -> bool {
    text.contains('<') && has_word_timestamp(text)
}

pub(super) fn has_word_timestamp(text: &str) -> bool {
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

pub(super) fn parse_timestamp(s: &str) -> Option<i64> {
    let (min_s, sec_s) = s.trim().split_once(':')?;
    if !sec_s.contains('.') {
        return None;
    }
    let _: i64 = min_s.parse().ok()?;
    let _: f64 = sec_s.parse().ok()?;
    Some(ts_to_ms(s))
}

pub(super) fn ts_to_ms(ts: &str) -> i64 {
    let parts: Vec<&str> = ts.trim().splitn(2, ':').collect();
    if parts.len() == 2 {
        let minutes: i64 = parts[0].parse().unwrap_or(0);
        let seconds: f64 = parts[1].parse().unwrap_or(0.0);
        ((minutes as f64 * 60.0 + seconds) * 1000.0).round() as i64
    } else {
        0
    }
}

pub(super) fn parse_lrc(text: &str) -> (Vec<LyricLine>, bool) {
    if is_enhanced_lrc(text) {
        return (parse_enhanced_lrc(text), true);
    }
    parse_regular_lrc(text)
}

pub(super) fn parse_regular_lrc(text: &str) -> (Vec<LyricLine>, bool) {
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

pub(super) fn parse_lrc_line_start(line: &str) -> Option<(&str, &str)> {
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

pub(super) fn parse_enhanced_lrc(text: &str) -> Vec<LyricLine> {
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
                .map_or(line_start + 3000, |w| w.end);

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

pub(super) fn extract_word_timestamps(text: &str) -> Vec<(String, String)> {
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

pub(super) fn strip_word_timestamps(text: &str) -> String {
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

pub(super) fn parse_richsync(rich_data: &[serde_json::Value]) -> Vec<LyricLine> {
    let mut result = Vec::new();

    for line in rich_data {
        let line_start = (line.get("ts").and_then(serde_json::Value::as_f64).unwrap_or(0.0) * 1000.0).round() as i64;
        let line_end = (line.get("te").and_then(serde_json::Value::as_f64).unwrap_or(0.0) * 1000.0).round() as i64;

        let Some(fragments) = line.get("l").and_then(|v| v.as_array()) else {
            continue;
        };

        let mut full_text = String::new();
        let mut words = Vec::new();

        let ts_base = line.get("ts").and_then(serde_json::Value::as_f64).unwrap_or(0.0);

        for frag in fragments {
            let c = frag
                .get("c")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let offset = frag
                .get("o")
                .and_then(serde_json::Value::as_f64)
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
