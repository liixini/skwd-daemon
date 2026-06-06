use std::collections::{HashMap, HashSet};

use tokio::process::Command;
use tracing::{info, warn};

use crate::config::{self, Config};
use crate::util::CommandExt;

use super::{is_kde, read_outputs_state};

pub(super) async fn run_plasma_evaluate_script(script: &str) -> anyhow::Result<()> {
    let status = Command::new("qdbus6")
        .arg("org.kde.plasmashell")
        .arg("/PlasmaShell")
        .arg("org.kde.PlasmaShell.evaluateScript")
        .arg(script)
        .silent()
        .status()
        .await?;
    if !status.success() {
        warn!("plasmashell evaluateScript failed ({})", status);
        anyhow::bail!("plasmashell evaluateScript failed ({status})");
    }
    Ok(())
}

pub(super) async fn query_kde_screen_map() -> HashMap<String, u32> {
    let out = match Command::new("kscreen-doctor").arg("-j").output().await {
        Ok(o) if o.status.success() => o.stdout,
        _ => return HashMap::new(),
    };
    let text = String::from_utf8_lossy(&out);
    parse_kscreen_json(&text)
}

fn parse_kscreen_json(text: &str) -> HashMap<String, u32> {
    let json: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };
    let Some(outputs) = json.get("outputs").and_then(|v| v.as_array()) else {
        return HashMap::new();
    };
    let mut map = HashMap::new();
    for output in outputs {
        let connected = output.get("connected").and_then(serde_json::Value::as_bool).unwrap_or(false);
        if !connected {
            continue;
        }
        let name = match output.get("name").and_then(|v| v.as_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        let priority = match output.get("priority").and_then(serde_json::Value::as_u64) {
            Some(p) if p >= 1 => u32::try_from(p - 1).unwrap_or(0),
            _ => continue,
        };
        map.insert(name, priority);
    }
    map
}

pub(super) fn kde_target_indices(outputs: &[String], map: &HashMap<String, u32>) -> Vec<u32> {
    if outputs.is_empty() {
        if map.is_empty() {
            Vec::new()
        } else {
            map.values().copied().collect()
        }
    } else {
        outputs.iter().filter_map(|o| map.get(o).copied()).collect()
    }
}

pub(super) fn kde_fill_mode_value(fm: config::FillMode) -> u32 {
    match fm {
        config::FillMode::Fill => 2,
        config::FillMode::Fit => 0,
        config::FillMode::Stretch => 1,
        config::FillMode::Center => 6,
        config::FillMode::Tile => 3,
    }
}

pub(super) async fn apply_kde_static(path: &str, outputs: &[String], config: &Config) -> anyhow::Result<()> {
    let map = query_kde_screen_map().await;
    let targets = kde_target_indices(outputs, &map);
    let indices_js = targets.iter().map(std::string::ToString::to_string).collect::<Vec<_>>().join(",");
    let fill_mode = kde_fill_mode_value(config.display.fill_mode);
    let file_url = format!("file://{path}");
    info!(
        path = %path,
        outputs = ?outputs,
        targets = ?targets,
        "apply_kde_static: setting wallpaper via plasmashell evaluateScript"
    );
    let script = format!(
        "var targets = [{indices_js}]; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (targets.length > 0 && targets.indexOf(ds[i].screen) === -1) continue; \
           var d = ds[i]; \
           d.wallpaperPlugin = 'org.kde.image'; \
           d.currentConfigGroup = ['Wallpaper', 'org.kde.image', 'General']; \
           d.writeConfig('Image', '{file_url}'); \
           d.writeConfig('FillMode', '{fill_mode}'); \
         }}"
    );
    run_plasma_evaluate_script(&script).await
}

pub(super) async fn kde_unload_video_plugin(outputs: &[String]) {
    if !is_kde() {
        return;
    }
    let map = query_kde_screen_map().await;
    if map.is_empty() {
        return;
    }
    let target_indices: Vec<u32> = if outputs.is_empty() || outputs.iter().any(|o| o == "*") {
        map.values().copied().collect()
    } else {
        outputs.iter().filter_map(|o| map.get(o).copied()).collect()
    };
    if target_indices.is_empty() {
        return;
    }
    let indices_js = target_indices
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let script = format!(
        "var idx = [{indices_js}]; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (idx.indexOf(ds[i].screen) === -1) continue; \
           ds[i].wallpaperPlugin = 'org.kde.image'; \
         }}"
    );
    info!(outputs = ?outputs, "kde_unload_video_plugin");
    if let Err(e) = run_plasma_evaluate_script(&script).await {
        warn!(error = %e, "kde_unload_video_plugin failed");
    }
}

pub(super) async fn kde_apply_audio(
    config: &Config,
    mute_per_output: &HashMap<String, bool>,
    volume_per_output: &HashMap<String, u32>,
) {
    if !is_kde() {
        return;
    }
    if mute_per_output.is_empty() && volume_per_output.is_empty() {
        return;
    }
    let kde_map = query_kde_screen_map().await;
    if kde_map.is_empty() {
        return;
    }
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";

    // Filter to outputs currently playing video/we in outputs.json.
    let state = read_outputs_state(&config.cache_dir()).await;
    let mut video_outputs: HashSet<String> = HashSet::new();
    if let Some(obj) = state.as_object() {
        for (out, entry) in obj {
            let t = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if t == "video" || t == "we" {
                video_outputs.insert(out.clone());
            }
        }
    }

    let mut configs: Vec<serde_json::Value> = Vec::new();
    let touched: HashSet<&String> = mute_per_output
        .keys()
        .chain(volume_per_output.keys())
        .collect();
    for out in touched {
        if !video_outputs.contains(out) {
            continue;
        }
        let Some(&idx) = kde_map.get(out) else {
            continue;
        };
        let mut entry = serde_json::Map::new();
        entry.insert("screen".into(), serde_json::json!(idx));
        if let Some(&m) = mute_per_output.get(out) {
            // MuteMode: 5 = always mute, 4 = never mute (always play)
            entry.insert(
                "muteMode".into(),
                serde_json::json!(if m { "5" } else { "4" }),
            );
        }
        if let Some(&v) = volume_per_output.get(out) {
            // Volume: plugin schema is Double in 0.0..1.0 - convert from 0..100 percent.
            let vol = (v as f32 / 100.0).clamp(0.0, 1.0);
            entry.insert("volume".into(), serde_json::json!(format!("{vol}")));
        }
        configs.push(serde_json::Value::Object(entry));
    }

    if configs.is_empty() {
        return;
    }

    let configs_js =
        serde_json::to_string(&configs).unwrap_or_else(|_| "[]".to_string());
    let script = format!(
        "var cfgs = {configs_js}; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           var cfg = null; \
           for (var j = 0; j < cfgs.length; j++) {{ \
             if (cfgs[j].screen === ds[i].screen) {{ cfg = cfgs[j]; break; }} \
           }} \
           if (cfg === null) continue; \
           var d = ds[i]; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           if (cfg.muteMode !== undefined) {{ d.writeConfig('MuteMode', cfg.muteMode); }} \
           if (cfg.volume !== undefined) {{ d.writeConfig('Volume', cfg.volume); }} \
         }}"
    );
    info!(configs = %configs_js, "kde_apply_audio");
    if let Err(e) = run_plasma_evaluate_script(&script).await {
        warn!(error = %e, "kde_apply_audio script failed");
    }
}

pub(super) async fn apply_kde_video(
    path: &str,
    outputs: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
    let map = query_kde_screen_map().await;
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";
    let file_url = format!("file://{path}");
    let global_mute = config.is_muted();
    let global_volume = config.volume();
    let global_mute_mode = if global_mute { "5" } else { "4" };
    let global_volume_f = (global_volume as f32 / 100.0).clamp(0.0, 1.0);

    let resolve = |name: &str| -> (String, f32) {
        let mute = outputs_audio.get(name).copied().unwrap_or(global_mute);
        let volume = outputs_volume.get(name).copied().unwrap_or(global_volume);
        let mute_mode = if mute { "5" } else { "4" };
        let vol = (volume as f32 / 100.0).clamp(0.0, 1.0);
        (mute_mode.to_string(), vol)
    };

    let (per_screen, fallback_to_all): (Vec<serde_json::Value>, bool) = if map.is_empty() {
        (Vec::new(), true)
    } else if outputs.is_empty() {
        let mut v = Vec::new();
        for (name, idx) in &map {
            let (mute_mode, volume) = resolve(name);
            v.push(serde_json::json!({
                "screen": idx,
                "muteMode": mute_mode,
                "volume": format!("{volume}"),
            }));
        }
        (v, false)
    } else {
        let mut v = Vec::new();
        for name in outputs {
            if let Some(idx) = map.get(name) {
                let (mute_mode, volume) = resolve(name);
                v.push(serde_json::json!({
                    "screen": idx,
                    "muteMode": mute_mode,
                    "volume": format!("{volume}"),
                }));
            }
        }
        (v, false)
    };

    let configs_js =
        serde_json::to_string(&per_screen).unwrap_or_else(|_| "[]".to_string());

    info!(
        path = %path,
        outputs = ?outputs,
        configs = %configs_js,
        fallback_to_all = fallback_to_all,
        "apply_kde_video: setting video wallpaper via plasmashell evaluateScript"
    );

    let script = format!(
        "var configs = {configs_js}; \
         var fallbackToAll = {fallback_to_all}; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           var match = null; \
           for (var j = 0; j < configs.length; j++) {{ \
             if (configs[j].screen === ds[i].screen) {{ match = configs[j]; break; }} \
           }} \
           if (!fallbackToAll && match === null) continue; \
           var muteMode = match ? match.muteMode : '{global_mute_mode}'; \
           var volume = match ? match.volume : '{global_volume_f}'; \
           var d = ds[i]; \
           d.wallpaperPlugin = '{plugin}'; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           d.writeConfig('VideoUrls', '[{{\"filename\":\"{file_url}\",\"enabled\":true}}]'); \
           d.writeConfig('MuteMode', muteMode); \
           d.writeConfig('Volume', volume); \
         }}"
    );
    run_plasma_evaluate_script(&script).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kscreen_json_maps_connected_outputs_priority_minus_one() {
        let json = r#"{"outputs":[
            {"name":"DP-1","connected":true,"priority":1},
            {"name":"DP-2","connected":true,"priority":2},
            {"name":"HDMI-1","connected":false,"priority":3},
            {"name":"zero","connected":true,"priority":0}
        ]}"#;
        let map = parse_kscreen_json(json);
        assert_eq!(map.get("DP-1"), Some(&0));
        assert_eq!(map.get("DP-2"), Some(&1));
        assert_eq!(map.get("HDMI-1"), None);
        assert_eq!(map.get("zero"), None);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn parse_kscreen_json_handles_garbage_and_missing() {
        assert!(parse_kscreen_json("not json").is_empty());
        assert!(parse_kscreen_json(r#"{"other":1}"#).is_empty());
    }

    #[test]
    fn kde_target_indices_filters_and_defaults() {
        let mut map = HashMap::new();
        map.insert("DP-1".to_string(), 0u32);
        map.insert("DP-2".to_string(), 1u32);

        assert_eq!(kde_target_indices(&["DP-2".to_string()], &map), vec![1]);

        let mut all = kde_target_indices(&[], &map);
        all.sort_unstable();
        assert_eq!(all, vec![0, 1]);

        assert!(kde_target_indices(&[], &HashMap::new()).is_empty());
        assert!(kde_target_indices(&["unknown".to_string()], &map).is_empty());
    }

    #[test]
    fn kde_fill_mode_value_covers_all_variants() {
        assert_eq!(kde_fill_mode_value(config::FillMode::Fill), 2);
        assert_eq!(kde_fill_mode_value(config::FillMode::Fit), 0);
        assert_eq!(kde_fill_mode_value(config::FillMode::Stretch), 1);
        assert_eq!(kde_fill_mode_value(config::FillMode::Center), 6);
        assert_eq!(kde_fill_mode_value(config::FillMode::Tile), 3);
    }
}
