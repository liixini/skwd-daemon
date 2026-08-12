use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::ErrorKind;

use tokio::process::Command;
use tracing::{info, warn};

use crate::config::{self, Config};

use super::{is_kde, read_outputs_state};

const QDBUS_CANDIDATES: &[&str] = &[
    "qdbus6",
    "qdbus-qt6",
    "qdbus",
    "/usr/lib64/qt6/bin/qdbus",
    "/usr/lib/qt6/bin/qdbus",
];

pub(super) async fn run_plasma_evaluate_script(script: &str) -> anyhow::Result<String> {
    let candidates: Vec<&OsStr> = QDBUS_CANDIDATES.iter().map(OsStr::new).collect();
    run_plasma_evaluate_script_with(script, &candidates).await
}

async fn run_plasma_evaluate_script_with(script: &str, candidates: &[&OsStr]) -> anyhow::Result<String> {
    let mut missing = Vec::new();
    for candidate in candidates {
        let out = match Command::new(candidate)
            .arg("org.kde.plasmashell")
            .arg("/PlasmaShell")
            .arg("org.kde.PlasmaShell.evaluateScript")
            .arg(script)
            .output()
            .await
        {
            Ok(out) => out,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(candidate.to_string_lossy().into_owned());
                continue;
            }
            Err(error) => {
                anyhow::bail!("failed to launch {}: {error}", candidate.to_string_lossy());
            }
        };

        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            warn!(
                command = %candidate.to_string_lossy(),
                status = %out.status,
                stderr = %stderr,
                "plasmashell evaluateScript failed"
            );
            anyhow::bail!(
                "plasmashell evaluateScript via {} failed ({}): {}",
                candidate.to_string_lossy(),
                out.status,
                stderr
            );
        }

        return Ok(String::from_utf8_lossy(&out.stdout).trim().to_string());
    }

    anyhow::bail!("no Qt 6 qdbus executable found (tried: {})", missing.join(", "))
}

pub(super) async fn query_kde_screen_map() -> HashMap<String, (i64, i64)> {
    let out = match Command::new("kscreen-doctor").arg("-j").output().await {
        Ok(o) if o.status.success() => o.stdout,
        _ => return HashMap::new(),
    };
    let text = String::from_utf8_lossy(&out);
    parse_kscreen_json(&text)
}

fn parse_kscreen_json(text: &str) -> HashMap<String, (i64, i64)> {
    let json: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };
    let Some(outputs) = json.get("outputs").and_then(|v| v.as_array()) else {
        return HashMap::new();
    };
    let mut map = HashMap::new();
    for output in outputs {
        let connected = output
            .get("connected")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !connected {
            continue;
        }
        if output.get("enabled").and_then(serde_json::Value::as_bool) == Some(false) {
            continue;
        }
        let Some(name) = output.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let pos = output.get("pos");
        let x = pos.and_then(|p| p.get("x")).and_then(serde_json::Value::as_i64);
        let y = pos.and_then(|p| p.get("y")).and_then(serde_json::Value::as_i64);
        if let (Some(x), Some(y)) = (x, y) {
            map.insert(name.to_string(), (x, y));
        }
    }
    map
}

fn kde_targets(outputs: &[String], map: &HashMap<String, (i64, i64)>) -> (Vec<(i64, i64)>, bool) {
    if outputs.is_empty() || outputs.iter().any(|o| o == "*") || map.is_empty() {
        return (Vec::new(), true);
    }
    (outputs.iter().filter_map(|o| map.get(o).copied()).collect(), false)
}

fn kde_match_js(targets: &[(i64, i64)], apply_all: bool) -> String {
    if apply_all {
        return "function __kmatch(s){ return true; }".to_string();
    }
    let arr = targets
        .iter()
        .map(|(x, y)| format!("[{x},{y}]"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "var __kt=[{arr}]; \
         function __kmatch(s){{ if(s===-1) return false; var g=screenGeometry(s); \
           for(var k=0;k<__kt.length;k++){{ if(Math.abs(__kt[k][0]-g.x)<=1 && Math.abs(__kt[k][1]-g.y)<=1) return true; }} \
           return false; }}"
    )
}

fn kde_static_script(matcher: &str, file_url: &str, fill_mode: u32) -> String {
    format!(
        "{matcher} \
         var ds = desktops(); var __n = 0; \
         for (var i = 0; i < ds.length; i++) {{ \
           if (ds[i].screen === -1) continue; \
           if (!__kmatch(ds[i].screen)) continue; \
           var d = ds[i]; \
           d.wallpaperPlugin = 'org.kde.image'; \
           d.currentConfigGroup = ['Wallpaper', 'org.kde.image', 'General']; \
           d.writeConfig('Image', '{file_url}'); \
           d.writeConfig('FillMode', '{fill_mode}'); \
           __n++; \
         }} \
         __n;"
    )
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
    let (targets, apply_all) = kde_targets(outputs, &map);
    let matcher = kde_match_js(&targets, apply_all);
    let fill_mode = kde_fill_mode_value(config.display.fill_mode);
    let file_url = format!("file://{path}");
    info!(
        path = %path,
        outputs = ?outputs,
        targets = ?targets,
        apply_all,
        "apply_kde_static: setting wallpaper via plasmashell evaluateScript"
    );
    let script = kde_static_script(&matcher, &file_url, fill_mode);
    let matched: i64 = run_plasma_evaluate_script(&script).await?.parse().unwrap_or(-1);
    if !apply_all && !targets.is_empty() && matched == 0 {
        warn!(
            outputs = ?outputs,
            targets = ?targets,
            "apply_kde_static: requested outputs matched no Plasma desktop \
             (geometry/scaling mismatch?) - wallpaper not applied"
        );
    } else {
        info!(matched, "apply_kde_static: applied to {matched} desktop(s)");
    }
    Ok(())
}

pub(super) async fn kde_unload_video_plugin(outputs: &[String]) {
    if !is_kde() {
        return;
    }
    let map = query_kde_screen_map().await;
    if map.is_empty() {
        return;
    }
    let (targets, apply_all) = kde_targets(outputs, &map);
    if !apply_all && targets.is_empty() {
        return;
    }
    let matcher = kde_match_js(&targets, apply_all);
    let script = format!(
        "{matcher} \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (ds[i].screen === -1) continue; \
           if (!__kmatch(ds[i].screen)) continue; \
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
    let touched: HashSet<&String> = mute_per_output.keys().chain(volume_per_output.keys()).collect();
    for out in touched {
        if !video_outputs.contains(out) {
            continue;
        }
        let Some(&(x, y)) = kde_map.get(out) else {
            continue;
        };
        let mut entry = serde_json::Map::new();
        entry.insert("x".into(), serde_json::json!(x));
        entry.insert("y".into(), serde_json::json!(y));
        if let Some(&m) = mute_per_output.get(out) {
            // MuteMode: 5 = always mute, 4 = never mute (always play)
            entry.insert("muteMode".into(), serde_json::json!(if m { "5" } else { "4" }));
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

    let configs_js = serde_json::to_string(&configs).unwrap_or_else(|_| "[]".to_string());
    let script = format!(
        "var cfgs = {configs_js}; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (ds[i].screen === -1) continue; \
           var g = screenGeometry(ds[i].screen); \
           var cfg = null; \
           for (var j = 0; j < cfgs.length; j++) {{ \
             if (Math.abs(cfgs[j].x - g.x) <= 1 && Math.abs(cfgs[j].y - g.y) <= 1) {{ cfg = cfgs[j]; break; }} \
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

    let entry_for = |name: &str, &(x, y): &(i64, i64)| {
        let (mute_mode, volume) = resolve(name);
        serde_json::json!({
            "x": x,
            "y": y,
            "muteMode": mute_mode,
            "volume": format!("{volume}"),
        })
    };

    let (per_screen, fallback_to_all): (Vec<serde_json::Value>, bool) = if map.is_empty() {
        (Vec::new(), true)
    } else if outputs.is_empty() || outputs.iter().any(|o| o == "*") {
        (map.iter().map(|(name, pos)| entry_for(name, pos)).collect(), false)
    } else {
        let v = outputs
            .iter()
            .filter_map(|name| map.get(name).map(|pos| entry_for(name, pos)))
            .collect();
        (v, false)
    };

    let configs_js = serde_json::to_string(&per_screen).unwrap_or_else(|_| "[]".to_string());

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
           if (ds[i].screen === -1) continue; \
           var g = screenGeometry(ds[i].screen); \
           var match = null; \
           for (var j = 0; j < configs.length; j++) {{ \
             if (Math.abs(configs[j].x - g.x) <= 1 && Math.abs(configs[j].y - g.y) <= 1) {{ match = configs[j]; break; }} \
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
    run_plasma_evaluate_script(&script).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn executable(path: &std::path::Path, body: &str) {
        std::fs::write(path, body).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[tokio::test]
    async fn qdbus_resolution_skips_missing_candidate() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("qdbus6");
        let fallback = temp.path().join("qdbus-qt6");
        executable(&fallback, "#!/bin/sh\nprintf '2\\n'\n");
        let candidates = [missing.as_os_str(), fallback.as_os_str()];

        let result = run_plasma_evaluate_script_with("1 + 1", &candidates).await.unwrap();

        assert_eq!(result, "2");
    }

    #[tokio::test]
    async fn qdbus_resolution_reports_all_missing_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("qdbus6");
        let second = temp.path().join("qdbus-qt6");
        let candidates = [first.as_os_str(), second.as_os_str()];

        let error = run_plasma_evaluate_script_with("1 + 1", &candidates)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("no Qt 6 qdbus executable found"));
        assert!(error.contains("qdbus6"));
        assert!(error.contains("qdbus-qt6"));
    }

    #[tokio::test]
    async fn qdbus_resolution_preserves_command_stderr() {
        let temp = tempfile::tempdir().unwrap();
        let command = temp.path().join("qdbus-qt6");
        executable(&command, "#!/bin/sh\necho 'dbus unavailable' >&2\nexit 3\n");
        let candidates = [command.as_os_str()];

        let error = run_plasma_evaluate_script_with("1 + 1", &candidates)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("dbus unavailable"));
        assert!(error.contains("qdbus-qt6"));
    }

    #[test]
    fn parse_kscreen_json_maps_connected_outputs_to_position() {
        let json = r#"{"outputs":[
            {"name":"DP-1","connected":true,"enabled":true,"pos":{"x":0,"y":0}},
            {"name":"DP-2","connected":true,"enabled":true,"pos":{"x":1920,"y":0}},
            {"name":"DP-3","connected":true,"enabled":true,"pos":{"x":3840,"y":0}},
            {"name":"PORTRAIT","connected":true,"enabled":true,"pos":{"x":-1080,"y":0}},
            {"name":"HDMI-1","connected":false,"pos":{"x":0,"y":0}},
            {"name":"OFF","connected":true,"enabled":false,"pos":{"x":0,"y":0}}
        ]}"#;
        let map = parse_kscreen_json(json);
        assert_eq!(map.get("DP-1"), Some(&(0, 0)));
        assert_eq!(map.get("DP-2"), Some(&(1920, 0)));
        assert_eq!(map.get("DP-3"), Some(&(3840, 0)));
        assert_eq!(map.get("PORTRAIT"), Some(&(-1080, 0)));
        assert_eq!(map.get("HDMI-1"), None, "disconnected dropped");
        assert_eq!(map.get("OFF"), None, "disabled dropped");
        assert_eq!(map.len(), 4);
    }

    #[test]
    fn parse_kscreen_json_handles_garbage_and_missing() {
        assert!(parse_kscreen_json("not json").is_empty());
        assert!(parse_kscreen_json(r#"{"other":1}"#).is_empty());
    }

    #[test]
    fn kde_targets_specific_match_and_all() {
        let mut map = HashMap::new();
        map.insert("DP-1".to_string(), (0i64, 0i64));
        map.insert("PORTRAIT".to_string(), (-1080i64, 0i64));

        let (t, all) = kde_targets(&["PORTRAIT".to_string()], &map);
        assert!(!all);
        assert_eq!(t, vec![(-1080, 0)]);

        let (t, all) = kde_targets(&[], &map);
        assert!(all);
        assert!(t.is_empty());

        let (_t, all) = kde_targets(&["*".to_string()], &map);
        assert!(all);

        let (t, all) = kde_targets(&["UNKNOWN".to_string()], &map);
        assert!(!all, "unknown target must not fall back to all");
        assert!(t.is_empty());

        let (_t, all) = kde_targets(&["DP-1".to_string()], &HashMap::new());
        assert!(all);
    }

    #[test]
    fn kde_match_js_all_vs_specific() {
        assert!(kde_match_js(&[], true).contains("return true"));
        let js = kde_match_js(&[(1920, 0), (-1080, 0)], false);
        assert!(js.contains("[1920,0]"));
        assert!(js.contains("[-1080,0]"));
        assert!(js.contains("screenGeometry"));
        assert!(js.contains("s===-1"), "specific matcher must reject screen -1");
        assert!(
            js.contains("Math.abs"),
            "match must use a tolerance, not exact equality"
        );
    }

    #[test]
    fn kde_static_script_guards_minus_one_and_returns_count() {
        let matcher = kde_match_js(&[(0, 0)], false);
        let script = kde_static_script(&matcher, "file:///w.png", 2);
        assert!(
            script.contains("ds[i].screen === -1) continue"),
            "static script must skip desktops with no monitor"
        );
        assert!(script.contains("var __n = 0"), "must initialise a match counter");
        assert!(script.trim_end().ends_with("__n;"), "must return the match count");
        assert!(script.contains("file:///w.png"));
        assert!(script.contains("writeConfig('FillMode', '2')"));
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
