use std::collections::{HashMap, HashSet};

use tokio::io::AsyncWriteExt;

use crate::config::Config;

use super::*;

pub async fn set_audio_for(
    config: &Config,
    mute: Option<bool>,
    volume: Option<u32>,
    outputs: Option<Vec<String>>,
) {
    let cache_dir = config.cache_dir();
    let mut payload = serde_json::Map::new();
    payload.insert("to".into(), serde_json::json!(""));
    if let Some(m) = mute {
        payload.insert("mute".into(), serde_json::json!(m));
    }
    if let Some(v) = volume {
        payload.insert("volume".into(), serde_json::json!(v));
    }
    let line = format!("{}\n", serde_json::Value::Object(payload));

    let filter: Option<HashSet<String>> = outputs.map(|v| v.into_iter().collect());

    {
        let mut map = persist_papers().lock().await;
        let keys: Vec<String> = map.keys().cloned().collect();
        for out in keys {
            if let Some(ref f) = filter
                && !f.contains(&out)
            {
                continue;
            }
            if let Some(p) = map.get_mut(&out)
                && p.stdin.write_all(line.as_bytes()).await.is_err()
            {
                tracing::warn!(output = %out, "set_audio: persist write failed");
            }
            if let Some(p) = map.get_mut(&out) {
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map = video_persist_papers().lock().await;
        let keys: Vec<String> = map.keys().cloned().collect();
        for out in keys {
            if let Some(ref f) = filter
                && !f.contains(&out)
            {
                continue;
            }
            if let Some(p) = map.get_mut(&out)
                && p.stdin.write_all(line.as_bytes()).await.is_err()
            {
                tracing::warn!(output = %out, "set_audio: video persist write failed");
            }
            if let Some(p) = map.get_mut(&out) {
                let _ = p.stdin.flush().await;
            }
        }
    }

    if mute.is_some() || volume.is_some() {
        update_outputs_state_audio(&cache_dir, &filter, mute, volume).await;

        let we_affected = we_outputs_in_filter(&cache_dir, &filter).await;
        if we_affected {
            let global_volume = volume.unwrap_or_else(|| config.volume());
            let _ = rebuild_scene_we(
                config,
                &[],
                &std::collections::BTreeMap::new(),
                global_volume,
            )
            .await;
        }
    }

    if is_kde() {
        // Translate the (mute, volume, filter) into per-output maps the same
        // way the non-KDE stdin path implicitly does, then push them into
        // plasma containments.
        let state = read_outputs_state(&cache_dir).await;
        let mut mute_map: HashMap<String, bool> = HashMap::new();
        let mut volume_map: HashMap<String, u32> = HashMap::new();
        if let Some(obj) = state.as_object() {
            for out in obj.keys() {
                if let Some(ref f) = filter
                    && !f.contains(out) {
                        continue;
                    }
                if let Some(m) = mute {
                    mute_map.insert(out.clone(), m);
                }
                if let Some(v) = volume {
                    volume_map.insert(out.clone(), v);
                }
            }
        }
        kde_apply_audio(config, &mute_map, &volume_map).await;
    }

    enforce_audio_dedup(config).await;
}

pub(super) async fn preserve_group_audio(config: &Config, prev_state: &serde_json::Value) {
    let mut prev_audible: HashSet<(String, String, String)> = HashSet::new();
    if let Some(obj) = prev_state.as_object() {
        for (_, entry) in obj {
            let muted = entry
                .get("mute")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            if muted {
                continue;
            }
            let t = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if t != "video" && t != "we" {
                continue;
            }
            let path = entry
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let we_id = entry
                .get("we_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            prev_audible.insert((t, path, we_id));
        }
    }

    if prev_audible.is_empty() {
        return;
    }

    let cache_dir = config.cache_dir();
    let current = read_outputs_state(&cache_dir).await;
    let map = match current.as_object() {
        Some(m) => m.clone(),
        None => return,
    };

    let mut current_groups: HashMap<(String, String, String), Vec<(String, bool)>> = HashMap::new();
    for (out, entry) in &map {
        let t = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if t != "video" && t != "we" {
            continue;
        }
        let path = entry
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let we_id = entry
            .get("we_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let muted = entry
            .get("mute")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        current_groups
            .entry((t, path, we_id))
            .or_default()
            .push((out.clone(), muted));
    }

    let mut to_unmute: Vec<String> = Vec::new();
    for (key, mut outputs) in current_groups {
        if !prev_audible.contains(&key) {
            continue;
        }
        if outputs.iter().any(|(_, m)| !m) {
            continue;
        }
        outputs.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some((out, _)) = outputs.first() {
            to_unmute.push(out.clone());
        }
    }

    if to_unmute.is_empty() {
        return;
    }

    let payload = serde_json::json!({"to": "", "mute": false});
    let line = format!("{payload}\n");
    let unmute_set: HashSet<String> = to_unmute.iter().cloned().collect();

    {
        let mut map_p = persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !unmute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map_p = video_persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !unmute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }

    let filter = Some(unmute_set.clone());
    update_outputs_state_audio(&cache_dir, &filter, Some(false), None).await;

    let any_we = to_unmute.iter().any(|out| {
        map.get(out)
            .and_then(|e| e.get("type"))
            .and_then(|v| v.as_str())
            == Some("we")
    });
    if any_we {
        let _ = rebuild_scene_we(
            config,
            &[],
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await;
    }
}

pub async fn enforce_audio_dedup(config: &Config) {
    let cache_dir = config.cache_dir();
    let state = read_outputs_state(&cache_dir).await;
    let map = match state.as_object() {
        Some(m) => m.clone(),
        None => return,
    };

    let mut groups: HashMap<(String, String, String), Vec<String>> = HashMap::new();
    for (out, entry) in &map {
        let t = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if t != "video" && t != "we" {
            continue;
        }
        let path = entry
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let we_id = entry
            .get("we_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        groups
            .entry((t, path, we_id))
            .or_default()
            .push(out.clone());
    }

    let mut to_mute: Vec<String> = Vec::new();
    for (_, mut outputs) in groups {
        outputs.sort();
        let mut found_primary = false;
        for out in &outputs {
            let muted = map
                .get(out)
                .and_then(|e| e.get("mute"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            if !muted {
                if !found_primary {
                    found_primary = true;
                } else {
                    to_mute.push(out.clone());
                }
            }
        }
    }

    if to_mute.is_empty() {
        return;
    }

    let payload = serde_json::json!({"to": "", "mute": true});
    let line = format!("{payload}\n");
    let mute_set: HashSet<String> = to_mute.iter().cloned().collect();

    {
        let mut map_p = persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !mute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map_p = video_persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !mute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }

    let filter = Some(mute_set.clone());
    update_outputs_state_audio(&cache_dir, &filter, Some(true), None).await;

    if is_kde() {
        let mute_map: HashMap<String, bool> =
            mute_set.iter().map(|o| (o.clone(), true)).collect();
        kde_apply_audio(config, &mute_map, &HashMap::new()).await;
    }

    let any_we = to_mute.iter().any(|out| {
        map.get(out)
            .and_then(|e| e.get("type"))
            .and_then(|v| v.as_str())
            == Some("we")
    });
    if any_we {
        let _ = rebuild_scene_we(
            config,
            &[],
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await;
    }
}

pub(super) async fn mute_wildcard_if_present(config: &Config) {
    let state = read_outputs_state(&config.cache_dir()).await;
    let has_star = state
        .as_object()
        .is_some_and(|m| m.contains_key("*"));
    if !has_star {
        return;
    }
    set_audio_for(config, Some(true), None, Some(vec!["*".to_string()])).await;
}
