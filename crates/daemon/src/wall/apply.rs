#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;

use tokio::process::Command;
use tokio::sync::Mutex as AsyncMutex;

fn outputs_state_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

async fn write_outputs_state_atomic(state_path: &Path, contents: &str) {
    let tmp = state_path.with_extension("json.tmp");
    if tokio::fs::write(&tmp, contents).await.is_err() {
        return;
    }
    let _ = tokio::fs::rename(&tmp, state_path).await;
}
use tracing::{info, warn};

use crate::config::{self, Config};
use crate::util::CommandExt;

mod external;
use external::{
    basename, run_external_apply, run_post_processing, run_sh, run_sh_status, shell_quote,
};

mod matugen;
pub use matugen::{theme_full_palette, theme_preview};
use matugen::{run_matugen, run_matugen_with, run_reloads};

mod kde;
use kde::{apply_kde_static, apply_kde_video, kde_apply_audio, kde_unload_video_plugin};

mod we;
pub use we::{apply_we};
use we::*;
mod audio;
pub use audio::{set_audio_for, enforce_audio_dedup};
use audio::*;
mod awww;
use awww::*;

mod video;
pub use video::apply_video;
use video::*;

mod paper;
use paper::*;
pub use paper::{
    drop_persist_paper, drop_steady_image_paper, drop_video_persist_paper, on_wall_hide,
    on_wall_show, preheat, ready_registry, signal_paper_ready,
};

pub async fn apply_static(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    all_screens: &[String],
    config: &Config,
) -> anyhow::Result<()> {
    apply_static_inner(path, outputs, neighbors, all_screens, config, false).await
}

async fn apply_static_inner(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    all_screens: &[String],
    config: &Config,
    restoring: bool,
) -> anyhow::Result<()> {
    let apply_total = std::time::Instant::now();
    let lock_wait = std::time::Instant::now();
    let _apply_guard = apply_lock().lock().await;
    let lock_wait_ms = lock_wait.elapsed().as_millis() as u64;
    let is_kde = is_kde();
    let resolved_path = resolve_static_or_optimized(path, config);
    let path = resolved_path.as_str();
    let prev_image = read_prev_transition_image(&config.cache_dir()).await;
    let prev_was_we = linux_we_running().await;

    if !prev_was_we {
        kill_legacy_video_procs().await;
    }

    let matugen_handle = {
        let path = path.to_string();
        let config = config.clone();
        tokio::spawn(async move {
            run_matugen(&path, &config).await;
            run_reloads(&config).await;
        })
    };

    let prev_engine = swap_last_engine(config.paper.engine).await;
    if prev_engine == Some(config::PaperEngine::Awww)
        && config.paper.engine != config::PaperEngine::Awww
    {
        kill_awww_if_running().await;
    }

    if config.wants_external_render() {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        let _ = tokio::fs::remove_file(config.video_dir().join("lockscreen-video.mp4")).await;
        run_external_apply(config, "static", path, path).await;
    } else if is_kde {
        let kde_target_outs: Vec<String> = if outputs.is_empty() {
            vec!["*".to_string()]
        } else {
            outputs.to_vec()
        };
        let _ = rebuild_scene_we(
            config,
            &kde_target_outs,
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await;
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_kde_static(path, outputs, config).await?;
    } else if config.paper.engine == config::PaperEngine::Awww {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_transition_overlays_for(&["*".to_string()]).await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_awww(path, outputs, config).await?;
    } else {
        let still_bin = paper_still_bin();
        let bin = paper_bin();
        let target_outs: Vec<String> = if outputs.is_empty() {
            vec!["*".to_string()]
        } else {
            outputs.to_vec()
        };
        let target_is_wildcard = target_outs.iter().any(|o| o == "*");

        let stable_dir = config.cache_dir().join("wallpaper");
        let _ = tokio::fs::create_dir_all(&stable_dir).await;
        let render_cur = stable_dir.join("static-render.jpg");
        let render_prev = stable_dir.join("static-render-prev.jpg");
        if render_cur.exists() {
            let _ = tokio::fs::copy(&render_cur, &render_prev).await;
        }
        let render_src = resolve_static_or_optimized(path, config);
        let render_to = if tokio::fs::copy(&render_src, &render_cur).await.is_ok() {
            render_cur.display().to_string()
        } else {
            render_src
        };

        let pre_state = read_outputs_state(&config.cache_dir()).await;
        let prev_wildcard_static: Option<String> = if !target_is_wildcard {
            pre_state
                .get("*")
                .and_then(|v| v.as_object())
                .filter(|m| {
                    m.get("type").and_then(|t| t.as_str()) == Some("static")
                })
                .and_then(|m| m.get("path").and_then(|p| p.as_str()).map(String::from))
        } else {
            None
        };

        rebuild_scene_we(
            config,
            &target_outs,
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await
        .ok();
        drop_transition_overlays_for(&target_outs).await;

        if let Some(prev_path) = prev_wildcard_static.as_deref()
            && !all_screens.is_empty()
        {
            let target_set: std::collections::HashSet<&str> =
                target_outs.iter().map(String::as_str).collect();
            let carry_over: Vec<String> = all_screens
                .iter()
                .filter(|s| !target_set.contains(s.as_str()))
                .cloned()
                .collect();
            if !carry_over.is_empty() {
                info!(
                    carry_over = ?carry_over,
                    prev = %prev_path,
                    "static apply: carrying wildcard image to un-targeted monitors"
                );
                for out in &carry_over {
                    if let Err(e) = spawn_steady_image_paper(
                        &still_bin,
                        out,
                        prev_path,
                        config.display.fill_mode,
                    )
                    .await
                    {
                        warn!(error = %e, output = %out, "carry-over steady spawn failed");
                    }
                }
            }
        }

        let prev_for_transition = if restoring || !config.transition.enabled {
            None
        } else {
            prev_image
                .clone()
                .filter(|p| Path::new(p).exists())
                .or_else(|| render_prev.exists().then(|| render_prev.display().to_string()))
                .filter(|p| *p != render_to)
        };

        if let Some(prev) = prev_for_transition.as_deref() {
            let thumbs =
                pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, render_to.as_str()], 20).await;
            let shader = config.transition.shader.clone();
            let dur_ms = config.transition.duration_ms;
            for out in &target_outs {
                if let Err(e) = spawn_transition_overlay(
                    &bin,
                    out,
                    prev,
                    &render_to,
                    &shader,
                    dur_ms,
                    &thumbs,
                    config.display.fill_mode,
                )
                .await
                {
                    warn!(error = %e, output = %out, "transition overlay spawn failed");
                }
            }
        }

        ensure_steady_image_paper(&still_bin, &target_outs, &render_to, config.display.fill_mode).await;
        drop_video_persist_papers_for(&target_outs).await;
        drop_persist_papers_for(&target_outs).await;
        let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
        for mut c in prev_steady {
            let _ = c.start_kill();
        }

        if prev_was_we {
            kill_legacy_video_procs().await;
        }
        info!("apply_static: skwd-paper-still + bottom-layer transition");
    }

    let current_dir = config.cache_dir().join("wallpaper");
    let _ = tokio::fs::create_dir_all(&current_dir).await;
    let wall_jpg = current_dir.join("current.jpg");
    let _ = tokio::fs::copy(path, &wall_jpg).await;

    save_state(&config.cache_dir(), "static", path, "").await;
    let prev_outputs_state = read_outputs_state(&config.cache_dir()).await;
    save_outputs_state(&config.cache_dir(), outputs, "static", path, "", &HashMap::new()).await;
    if !outputs.is_empty() && !outputs.iter().any(|o| o == "*") {
        mute_wildcard_if_present(config).await;
    }
    preserve_group_audio(config, &prev_outputs_state).await;
    enforce_audio_dedup(config).await;

    let path = path.to_string();
    let config = config.clone();
    tokio::spawn(async move {
        let _ = matugen_handle.await;
        run_post_processing(&config, "static", &basename(&path), &path, &path, restoring).await;
        info!("post-apply tasks done for static: {path}");
    });

    info!(
        total_ms = apply_total.elapsed().as_millis() as u64,
        lock_wait_ms,
        "applied static wallpaper"
    );
    Ok(())
}

pub(crate) fn resolve_static_or_optimized(path: &str, config: &Config) -> String {
    if path.is_empty() || Path::new(path).exists() {
        return path.to_string();
    }
    if let Some(stem) = Path::new(path).file_stem().and_then(|s| s.to_str()) {
        let webp = config.wallpaper_dir().join(format!("{stem}.webp"));
        if webp.exists() {
            return webp.display().to_string();
        }
    }
    path.to_string()
}

pub async fn restore(config: &Config) -> anyhow::Result<String> {
    let outputs_state = read_outputs_state(&config.cache_dir()).await;
    let map = outputs_state
        .as_object()
        .cloned()
        .unwrap_or_default();

    if !map.is_empty() {
        let mut groups: std::collections::HashMap<(String, String, String), Vec<String>> =
            std::collections::HashMap::new();
        let mut audio_by_output: HashMap<String, bool> = HashMap::new();
        let mut volume_by_output: HashMap<String, u32> = HashMap::new();
        for (output, entry) in &map {
            let wp_type = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
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
            if let Some(m) = entry.get("mute").and_then(serde_json::Value::as_bool) {
                audio_by_output.insert(output.clone(), m);
            }
            if let Some(v) = entry.get("volume").and_then(serde_json::Value::as_u64) {
                volume_by_output.insert(output.clone(), v.min(100) as u32);
            }
            groups
                .entry((wp_type, path, we_id))
                .or_default()
                .push(output.clone());
        }

        let mut last_id = String::new();
        for ((wp_type, path, we_id), outs) in groups {
            let outputs_arg: Vec<String> = if outs.iter().any(|o| o == "*") {
                Vec::new()
            } else {
                outs.clone()
            };
            let audio_arg: HashMap<String, bool> = outs
                .iter()
                .filter_map(|o| audio_by_output.get(o).map(|&v| (o.clone(), v)))
                .collect();
            let volume_arg: HashMap<String, u32> = outs
                .iter()
                .filter_map(|o| volume_by_output.get(o).map(|&v| (o.clone(), v)))
                .collect();
            last_id = match wp_type.as_str() {
                "static" if !path.is_empty() => {
                    let path = resolve_static_or_optimized(&path, config);
                    apply_static_inner(&path, &outputs_arg, &[], &[], config, true).await?;
                    path
                }
                "video" if !path.is_empty() => {
                    apply_video_inner(&path, &outputs_arg, &[], &audio_arg, &volume_arg, config, true).await?;
                    path
                }
                "we" if !we_id.is_empty() => {
                    apply_we_inner(&we_id, &outputs_arg, &audio_arg, &volume_arg, config, true).await?;
                    we_id
                }
                _ => continue,
            };
        }
        if !last_id.is_empty() {
            return Ok(last_id);
        }
    }

    let state_path = config.cache_dir().join("last-wallpaper.json");
    if !state_path.exists() {
        anyhow::bail!("no saved state");
    }
    let text = tokio::fs::read_to_string(&state_path).await?;
    let state: serde_json::Value = serde_json::from_str(&text)?;

    let wp_type = state.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match wp_type {
        "static" => {
            let path = state.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                anyhow::bail!("no path in state");
            }
            let path = resolve_static_or_optimized(path, config);
            apply_static_inner(&path, &[], &[], &[], config, true).await?;
            Ok(path)
        }
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                anyhow::bail!("no path in state");
            }
            apply_video_inner(path, &[], &[], &HashMap::new(), &HashMap::new(), config, true).await?;
            Ok(path.to_string())
        }
        "we" => {
            let we_id = state.get("we_id").and_then(|v| v.as_str()).unwrap_or("");
            if we_id.is_empty() {
                anyhow::bail!("no we_id in state");
            }
            apply_we_inner(we_id, &[], &HashMap::new(), &HashMap::new(), config, true).await?;
            Ok(we_id.to_string())
        }
        _ => anyhow::bail!("unknown wallpaper type: {wp_type}"),
    }
}

pub async fn reapply_statics_for_engine_change(config: &Config) -> anyhow::Result<()> {
    let outputs_state = read_outputs_state(&config.cache_dir()).await;
    let map = outputs_state.as_object().cloned().unwrap_or_default();
    if map.is_empty() {
        return Ok(());
    }

    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for (output, entry) in &map {
        let wp_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let path = entry.get("path").and_then(|v| v.as_str()).unwrap_or("");
        if wp_type != "static" || path.is_empty() {
            continue;
        }
        groups.entry(path.to_string()).or_default().push(output.clone());
    }

    for (path, outs) in groups {
        let outputs_arg: Vec<String> = if outs.iter().any(|o| o == "*") {
            Vec::new()
        } else {
            outs
        };
        if let Err(e) = apply_static_inner(&path, &outputs_arg, &[], &[], config, true).await {
            warn!("engine-change re-apply failed for {path}: {e}");
        }
    }
    Ok(())
}

pub async fn retheme(
    config: &Config,
    scheme: Option<&str>,
    mode: Option<&str>,
    color_index: Option<u32>,
) -> anyhow::Result<()> {
    let current_jpg = config.cache_dir().join("wallpaper/current.jpg");
    if !current_jpg.exists() {
        anyhow::bail!("no current wallpaper image to retheme");
    }
    let image_path = current_jpg.display().to_string();
    run_matugen_with(&image_path, config, scheme, mode, color_index).await;
    run_reloads(config).await;
    info!("retheme completed for {image_path}");
    Ok(())
}

async fn find_we_preview(item_dir: &Path) -> Option<PathBuf> {
    let mut entries = tokio::fs::read_dir(item_dir).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_lowercase();
        if name.starts_with("preview.") {
            return Some(entry.path());
        }
    }
    None
}


async fn we_outputs_in_filter(cache_dir: &Path, filter: &Option<HashSet<String>>) -> bool {
    let existing = read_outputs_state(cache_dir).await;
    let Some(obj) = existing.as_object() else {
        return false;
    };
    for (out, entry) in obj {
        if let Some(f) = filter.as_ref()
            && !f.contains(out)
        {
            continue;
        }
        if entry.get("type").and_then(|v| v.as_str()) == Some("we") {
            return true;
        }
    }
    false
}

pub async fn we_properties_for(
    config: &crate::config::Config,
    we_id: &str,
) -> serde_json::Value {
    let we_dir = config.we_dir().join(we_id);
    let project = we_dir.join("project.json");
    let Ok(text) = tokio::fs::read_to_string(&project).await else {
        return serde_json::json!({"properties": [], "values": {}});
    };
    let raw: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return serde_json::json!({"properties": [], "values": {}}),
    };
    let props_obj = raw
        .get("general")
        .and_then(|g| g.get("properties"))
        .and_then(|p| p.as_object())
        .cloned()
        .unwrap_or_default();

    let mut entries: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
    for (name, val) in props_obj {
        if let Some(obj) = val.as_object() {
            entries.push((name, obj.clone()));
        }
    }
    entries.sort_by(|a, b| {
        let oa = a.1.get("order").and_then(serde_json::Value::as_i64).unwrap_or(i64::MAX);
        let ob = b.1.get("order").and_then(serde_json::Value::as_i64).unwrap_or(i64::MAX);
        oa.cmp(&ob).then_with(|| a.0.cmp(&b.0))
    });

    let mut properties: Vec<serde_json::Value> = Vec::new();
    for (name, obj) in entries {
        let mut prop = serde_json::Map::new();
        prop.insert("name".into(), serde_json::Value::String(name));
        for k in [
            "type", "text", "value", "min", "max", "step", "precision",
            "fraction", "options", "condition", "index", "order",
        ] {
            if let Some(v) = obj.get(k) {
                prop.insert(k.into(), v.clone());
            }
        }
        properties.push(serde_json::Value::Object(prop));
    }

    let values_path = we_props_path(config, we_id);
    let values: serde_json::Value = match tokio::fs::read_to_string(&values_path).await {
        Ok(t) => serde_json::from_str(&t).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };

    serde_json::json!({ "properties": properties, "values": values })
}

pub async fn set_we_screen_overrides(
    config: &crate::config::Config,
    outputs: Option<Vec<String>>,
    scaling: Option<String>,
    clamp: Option<String>,
) {
    let cache_dir = config.cache_dir();
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    let _ = tokio::fs::create_dir_all(&cache_dir).await;
    let existing: serde_json::Value = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    let mut map = match existing {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };

    let target_keys: Vec<String> = match outputs {
        Some(ref outs) if !outs.is_empty() => outs.clone(),
        _ => map.keys().cloned().collect(),
    };

    for k in target_keys {
        let entry = map.entry(k).or_insert_with(|| serde_json::json!({}));
        let Some(obj) = entry.as_object_mut() else {
            continue;
        };
        if let Some(ref s) = scaling {
            if s.is_empty() {
                obj.remove("we_scaling");
            } else if crate::config::WeScaling::from_str(s).is_some() {
                obj.insert("we_scaling".into(), serde_json::json!(s));
            }
        }
        if let Some(ref c) = clamp {
            if c.is_empty() {
                obj.remove("we_clamp");
            } else if crate::config::WeClamp::from_str(c).is_some() {
                obj.insert("we_clamp".into(), serde_json::json!(c));
            }
        }
    }

    let contents = serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
    write_outputs_state_atomic(&state_path, &contents).await;
}

pub async fn set_we_props(
    config: &crate::config::Config,
    we_id: &str,
    values: serde_json::Value,
) {
    let values_path = we_props_path(config, we_id);
    if let Some(parent) = values_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let contents = serde_json::to_string(&values).unwrap_or_else(|_| "{}".to_string());
    let _ = tokio::fs::write(&values_path, contents.as_bytes()).await;
}

pub async fn read_we_props(
    config: &crate::config::Config,
    we_id: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let values_path = we_props_path(config, we_id);
    let Ok(text) = tokio::fs::read_to_string(&values_path).await else {
        return serde_json::Map::new();
    };
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

fn we_props_path(config: &crate::config::Config, we_id: &str) -> std::path::PathBuf {
    config.cache_dir().join("wallpaper").join("we-props").join(format!("{we_id}.json"))
}

fn format_we_prop_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(b) => if *b { "1".to_string() } else { "0".to_string() },
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

async fn update_outputs_state_audio(
    cache_dir: &Path,
    filter: &Option<HashSet<String>>,
    mute: Option<bool>,
    volume: Option<u32>,
) {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    let existing: serde_json::Value = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    let mut map = match existing {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };

    let keys: Vec<String> = map.keys().cloned().collect();
    for k in keys {
        if let Some(f) = filter.as_ref()
            && !f.contains(&k)
        {
            continue;
        }
        if let Some(entry) = map.get_mut(&k) {
            if let Some(m) = mute {
                entry["mute"] = serde_json::json!(m);
            }
            if let Some(v) = volume {
                entry["volume"] = serde_json::json!(v);
            }
        }
    }

    let contents =
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
    write_outputs_state_atomic(&state_path, &contents).await;
}

async fn linux_we_running() -> bool {
    Command::new("pgrep")
        .arg("-x")
        .arg("linux-wallpaperengine")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn kill_legacy_video_procs() {
    let _ = run_sh(
        "pkill -9 mpvpaper 2>/dev/null; \
         true",
    )
    .await;
}

async fn kill_legacy_other_procs() {
    let _ = run_sh(
        "pkill -9 mpvpaper 2>/dev/null; \
         pkill awww 2>/dev/null; \
         pkill awww-daemon 2>/dev/null; \
         true",
    )
    .await;
}

async fn kill_linux_we_proc() {
    let _ = run_sh("pkill -9 -f '[l]inux-wallpaperengine' 2>/dev/null; true").await;
}

async fn rebuild_scene_we(
    config: &Config,
    excluded: &[String],
    additions: &std::collections::BTreeMap<String, (String, bool)>,
    global_volume: u32,
) -> anyhow::Result<()> {
    let we_dir = config.we_dir();
    let existing = read_outputs_state(&config.cache_dir()).await;
    let exclude_all = excluded.iter().any(|o| o == "*");
    let exclude_set: HashSet<&str> = excluded.iter().map(std::string::String::as_str).collect();

    let mut merged = additions.clone();
    if !exclude_all && let Some(obj) = existing.as_object() {
        for (out, entry) in obj {
            if out == "*" || exclude_set.contains(out.as_str()) || merged.contains_key(out) {
                continue;
            }
            let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if entry_type != "we" {
                continue;
            }
            let entry_we_id = entry
                .get("we_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if entry_we_id.is_empty() {
                continue;
            }
            let (entry_we_type, _) = read_we_project_type(&we_dir.join(&entry_we_id)).await;
            if entry_we_type != "scene" {
                continue;
            }
            let entry_mute = entry.get("mute").and_then(serde_json::Value::as_bool).unwrap_or(true);
            merged.insert(out.clone(), (entry_we_id, entry_mute));
        }
    }

    info!(merged = ?merged, excluded = ?excluded, "rebuild_scene_we: scene outputs to render");

    kill_linux_we_proc().await;

    if merged.is_empty() {
        info!("rebuild_scene_we: no scene outputs in merged set -> killed lwe, NOT spawning");
        return Ok(());
    }

    let any_unmuted = merged.values().any(|(_, mute)| !*mute);
    let audio_flag = if any_unmuted {
        let scaled = (global_volume as f32 * 1.28).round() as u32;
        format!("--volume {}", scaled.min(128))
    } else {
        "--silent".to_string()
    };

    let render = &config.we_render;
    let mut screen_args = String::new();
    for (out, (id, _)) in &merged {
        let entry = existing.get(out);
        let scaling = entry
            .and_then(|e| e.get("we_scaling"))
            .and_then(|v| v.as_str())
            .and_then(crate::config::WeScaling::from_str)
            .unwrap_or(render.scaling);
        screen_args.push_str(&format!(
            " --screen-root {} --scaling {}",
            shell_quote(out),
            scaling.as_arg(),
        ));
        let props = read_we_props(config, id).await;
        for (name, value) in props {
            let formatted = format_we_prop_value(&value);
            screen_args.push_str(&format!(
                " --set-property {}",
                shell_quote(&format!("{name}={formatted}")),
            ));
        }
    }

    let assets_arg = config
        .we_assets_dir()
        .map(|d| format!("--assets-dir {}", shell_quote(&d.display().to_string())))
        .unwrap_or_default();

    let mut flags: Vec<String> = Vec::new();
    flags.push(format!("--fps {}", render.fps.clamp(1, 240)));
    flags.push(format!("--clamp {}", render.clamp.as_arg()));
    if render.no_fullscreen_pause {
        flags.push("--no-fullscreen-pause".into());
    } else if render.fullscreen_pause_only_active {
        flags.push("--fullscreen-pause-only-active".into());
    }
    if render.noautomute {
        flags.push("--noautomute".into());
    }
    if render.no_audio_processing {
        flags.push("--no-audio-processing".into());
    }
    if render.disable_particles {
        flags.push("--disable-particles".into());
    }
    if render.disable_mouse {
        flags.push("--disable-mouse".into());
    }
    if render.disable_parallax {
        flags.push("--disable-parallax".into());
    }
    let flag_str = flags.join(" ");

    let bg = merged
        .values()
        .next()
        .map(|(id, _)| shell_quote(id))
        .unwrap_or_default();

    let lwe_log = config.cache_dir().join("lwe.log");
    let lwe_log_q = shell_quote(&lwe_log.display().to_string());
    let cmd = format!(
        "nohup setsid linux-wallpaperengine {audio_flag} {flag_str}{screen_args} {assets_arg} {bg} </dev/null >> {lwe_log_q} 2>&1 &"
    );
    info!(cmd = %cmd, lwe_log = %lwe_log.display(), "rebuild_scene_we: spawning linux-wallpaperengine (output -> lwe_log)");
    run_sh(&cmd).await?;
    Ok(())
}

async fn read_we_project_type(item_dir: &Path) -> (String, String) {
    let project_path = item_dir.join("project.json");
    if !project_path.exists() {
        return ("scene".to_string(), String::new());
    }
    let text = tokio::fs::read_to_string(&project_path).await.unwrap_or_default();
    let proj: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    let t = proj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("scene")
        .to_lowercase();
    let f = proj
        .get("file")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    (t, f)
}

pub async fn kill_orphan_paper_procs() {
    let _ = run_sh(
        "pkill -9 -x skwd-paper 2>/dev/null; pkill -9 -x skwd-paper-still 2>/dev/null; true",
    )
    .await;
}

fn paper_bin() -> String {
    std::env::var("SKWD_PAPER_BIN").unwrap_or_else(|_| {
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let local = dir.join("skwd-paper");
            if local.exists() {
                return local.display().to_string();
            }
        }
        "skwd-paper".to_string()
    })
}

fn paper_still_bin() -> String {
    std::env::var("SKWD_PAPER_STILL_BIN").unwrap_or_else(|_| {
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let local = dir.join("skwd-paper-still");
            if local.exists() {
                return local.display().to_string();
            }
        }
        "skwd-paper-still".to_string()
    })
}

async fn pick_thumbs(
    neighbors: &[String],
    wallpaper_dir: &Path,
    exclude: &[&str],
    n: usize,
) -> Vec<String> {
    if !neighbors.is_empty() {
        return neighbors
            .iter()
            .filter(|p| !exclude.contains(&p.as_str()))
            .filter(|p| Path::new(p).exists())
            .take(n)
            .cloned()
            .collect();
    }
    pick_random_thumbs(wallpaper_dir, exclude, n).await
}

async fn pick_random_thumbs(wallpaper_dir: &Path, exclude: &[&str], n: usize) -> Vec<String> {
    let Ok(mut entries) = tokio::fs::read_dir(wallpaper_dir).await else {
        return Vec::new();
    };
    let exts = [".jpg", ".jpeg", ".png", ".webp", ".bmp"];
    let mut all: Vec<String> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = path.to_string_lossy().to_lowercase();
        if !exts.iter().any(|e| name.ends_with(e)) {
            continue;
        }
        let s = path.to_string_lossy().to_string();
        if exclude.contains(&s.as_str()) {
            continue;
        }
        all.push(s);
    }
    if all.is_empty() {
        return Vec::new();
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0);
    let mut picks = Vec::new();
    for i in 0..n.min(all.len()) {
        let idx = (nanos.wrapping_mul(2654435761).wrapping_add(i.wrapping_mul(48271))) % all.len();
        picks.push(all[idx].clone());
    }
    picks
}

pub async fn read_prev_transition_image(cache_dir: &Path) -> Option<String> {
    let state_path = cache_dir.join("last-wallpaper.json");
    let text = tokio::fs::read_to_string(&state_path).await.ok()?;
    let state: serde_json::Value = serde_json::from_str(&text).ok()?;
    let wp_type = state.get("type").and_then(|v| v.as_str())?;
    match wp_type {
        "static" => state
            .get("path")
            .and_then(|v| v.as_str())
            .filter(|p| Path::new(p).exists())
            .map(std::string::ToString::to_string)
            .or_else(|| {
                let snapshot = cache_dir.join("wallpaper/static-render.jpg");
                snapshot.exists().then(|| snapshot.display().to_string())
            }),
        "we" => state
            .get("path")
            .and_then(|v| v.as_str())
            .filter(|p| !p.is_empty() && Path::new(p).exists())
            .map(std::string::ToString::to_string),
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str())?;
            let thumb = video_thumb_path(cache_dir, path);
            if thumb.exists() {
                Some(thumb.display().to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn video_thumb_path(cache_dir: &Path, video_path: &str) -> PathBuf {
    let stem = Path::new(video_path)
        .file_stem().map_or_else(|| "thumb".to_string(), |s| s.to_string_lossy().into_owned());
    cache_dir.join(format!("wallpaper/video-thumbs/{stem}.jpg"))
}

async fn ensure_video_thumb_blocking(video_path: &str, cache_dir: &Path) -> Option<PathBuf> {
    let thumb = video_thumb_path(cache_dir, video_path);
    if thumb.exists() {
        return Some(thumb);
    }
    let _ = tokio::fs::create_dir_all(thumb.parent()?).await;
    let status = Command::new("ffmpeg")
        .args(["-y", "-i", video_path, "-vframes", "1", "-q:v", "2"])
        .arg(thumb.to_str()?)
        .silent()
        .status()
        .await
        .ok()?;
    if !status.success() {
        warn!("ffmpeg failed extracting first frame from {video_path}");
        return None;
    }
    Some(thumb)
}

async fn save_state(cache_dir: &Path, wp_type: &str, path: &str, we_id: &str) {
    let state_path = cache_dir.join("last-wallpaper.json");
    let _ = tokio::fs::create_dir_all(cache_dir).await;
    let mut obj = serde_json::json!({"type": wp_type});
    if !path.is_empty() {
        obj["path"] = serde_json::json!(path);
    }
    if !we_id.is_empty() {
        obj["we_id"] = serde_json::json!(we_id);
    }
    let _ = tokio::fs::write(&state_path, serde_json::to_string(&obj).unwrap_or_default()).await;
}

async fn save_outputs_state(
    cache_dir: &Path,
    outputs: &[String],
    wp_type: &str,
    path: &str,
    we_id: &str,
    mute_map: &HashMap<String, bool>,
) {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    let _ = tokio::fs::create_dir_all(cache_dir).await;
    let existing: serde_json::Value = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    let mut map = match existing {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };

    let keys: Vec<String> = if outputs.is_empty() || outputs.iter().any(|o| o == "*") {
        vec!["*".to_string()]
    } else {
        outputs.to_vec()
    };

    if keys == ["*"] {
        map.clear();
    } else {
        map.remove("*");
    }
    for k in keys {
        let mut entry = serde_json::json!({"type": wp_type});
        if !path.is_empty() {
            entry["path"] = serde_json::json!(path);
        }
        if !we_id.is_empty() {
            entry["we_id"] = serde_json::json!(we_id);
        }
        let m = mute_map.get(&k).copied().unwrap_or(true);
        entry["mute"] = serde_json::json!(m);
        map.insert(k, entry);
    }

    let contents =
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
    write_outputs_state_atomic(&state_path, &contents).await;
}

async fn compute_audio_dedup(
    cache_dir: &Path,
    target_outs: &[String],
    outputs_audio: &HashMap<String, bool>,
    wp_type: &str,
    path: &str,
    we_id: &str,
    global_mute: bool,
) -> HashMap<String, bool> {
    let existing = read_outputs_state(cache_dir).await;
    compute_audio_dedup_from(&existing, target_outs, outputs_audio, wp_type, path, we_id, global_mute)
}

fn compute_audio_dedup_from(
    existing: &serde_json::Value,
    target_outs: &[String],
    outputs_audio: &HashMap<String, bool>,
    wp_type: &str,
    path: &str,
    we_id: &str,
    global_mute: bool,
) -> HashMap<String, bool> {
    let target_set: HashSet<&str> = target_outs.iter().map(std::string::String::as_str).collect();

    let mut external_unmuted = false;
    if let Some(obj) = existing.as_object() {
        for (out, entry) in obj {
            if out == "*" || target_set.contains(out.as_str()) {
                continue;
            }
            let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if entry_type != wp_type {
                continue;
            }
            let same_source = if wp_type == "we" {
                entry.get("we_id").and_then(|v| v.as_str()).unwrap_or("") == we_id
            } else {
                entry.get("path").and_then(|v| v.as_str()).unwrap_or("") == path
            };
            if !same_source {
                continue;
            }
            let m = entry.get("mute").and_then(serde_json::Value::as_bool).unwrap_or(true);
            if !m {
                external_unmuted = true;
                break;
            }
        }
    }

    let mut sorted = target_outs.to_vec();
    sorted.sort();

    let mut winner_chosen = external_unmuted;
    let mut result: HashMap<String, bool> = HashMap::new();
    for out in target_outs {
        let want_mute = outputs_audio.get(out).copied().unwrap_or(global_mute);
        if want_mute || winner_chosen {
            result.insert(out.clone(), true);
        } else {
            let is_first = sorted.iter().find(|o| !outputs_audio.get(*o).copied().unwrap_or(global_mute))
                .is_some_and(|first| first == out);
            if is_first {
                result.insert(out.clone(), false);
                winner_chosen = true;
            } else {
                result.insert(out.clone(), true);
            }
        }
    }
    result
}

pub async fn read_outputs_state(cache_dir: &Path) -> serde_json::Value {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    }
}

pub async fn repoint_optimized_wallpaper(
    cache_dir: &Path,
    old_name: &str,
    new_name: &str,
    new_path: &str,
) {
    if old_name == new_name {
        return;
    }
    let basename_is_old = |entry: &serde_json::Value| {
        entry
            .get("path")
            .and_then(serde_json::Value::as_str)
            .and_then(|p| Path::new(p).file_name())
            .and_then(|s| s.to_str())
            == Some(old_name)
    };

    {
        let _guard = outputs_state_lock().lock().await;
        let state_path = cache_dir.join("outputs.json");
        if let Ok(text) = tokio::fs::read_to_string(&state_path).await
            && let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&text)
        {
            let mut changed = false;
            if let Some(map) = v.as_object_mut() {
                for entry in map.values_mut() {
                    if basename_is_old(entry) {
                        entry["path"] = serde_json::json!(new_path);
                        changed = true;
                    }
                }
            }
            if changed {
                write_outputs_state_atomic(&state_path, &v.to_string()).await;
            }
        }
    }

    let last_path = cache_dir.join("last-wallpaper.json");
    if let Ok(text) = tokio::fs::read_to_string(&last_path).await
        && let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&text)
        && basename_is_old(&v)
    {
        v["path"] = serde_json::json!(new_path);
        if v.get("name").is_some() {
            v["name"] = serde_json::json!(new_name);
        }
        let _ = tokio::fs::write(&last_path, v.to_string()).await;
    }
}

fn is_kde() -> bool {
    std::env::var("XDG_CURRENT_DESKTOP")
        .map(|d| {
            let lower = d.to_lowercase();
            lower.contains("kde") || lower.contains("plasma")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_outputs_state_handles_missing_and_invalid_and_valid() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_outputs_state(dir.path()).await, serde_json::json!({}));

        std::fs::write(dir.path().join("outputs.json"), "{ not json").unwrap();
        assert_eq!(read_outputs_state(dir.path()).await, serde_json::json!({}));

        std::fs::write(dir.path().join("outputs.json"), r#"{"DP-1":"wall.png"}"#).unwrap();
        assert_eq!(
            read_outputs_state(dir.path()).await,
            serde_json::json!({ "DP-1": "wall.png" })
        );
    }

    #[tokio::test]
    async fn repoint_optimized_wallpaper_updates_outputs_and_last_state() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path();
        let wall = "/home/u/Pictures/Wallpapers";
        std::fs::write(
            cache.join("outputs.json"),
            format!(r#"{{"DP-1":{{"type":"static","path":"{wall}/x.jpg"}},"DP-2":{{"type":"static","path":"{wall}/other.png"}}}}"#),
        )
        .unwrap();
        std::fs::write(
            cache.join("last-wallpaper.json"),
            format!(r#"{{"type":"static","name":"x.jpg","path":"{wall}/x.jpg"}}"#),
        )
        .unwrap();

        repoint_optimized_wallpaper(cache, "x.jpg", "x.webp", &format!("{wall}/x.webp")).await;

        let outputs = read_outputs_state(cache).await;
        assert_eq!(outputs["DP-1"]["path"], format!("{wall}/x.webp"), "applied output repointed to webp");
        assert_eq!(outputs["DP-2"]["path"], format!("{wall}/other.png"), "untouched output unchanged");

        let last: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(cache.join("last-wallpaper.json")).unwrap()).unwrap();
        assert_eq!(last["path"], format!("{wall}/x.webp"));
        assert_eq!(last["name"], "x.webp");
    }

    #[test]
    fn resolve_static_or_optimized_falls_back_to_webp_when_original_trashed() {
        let dir = tempfile::tempdir().unwrap();
        let wall = dir.path().join("walls");
        std::fs::create_dir_all(&wall).unwrap();
        let mut cfg = Config::default();
        cfg.paths.wallpaper = Some(wall.display().to_string());

        let jpg = wall.join("x.jpg");
        let webp = wall.join("x.webp");

        std::fs::write(&jpg, b"j").unwrap();
        assert_eq!(
            resolve_static_or_optimized(&jpg.display().to_string(), &cfg),
            jpg.display().to_string(),
            "existing original is kept"
        );

        std::fs::remove_file(&jpg).unwrap();
        std::fs::write(&webp, b"w").unwrap();
        assert_eq!(
            resolve_static_or_optimized(&jpg.display().to_string(), &cfg),
            webp.display().to_string(),
            "trashed original falls back to same-stem webp"
        );

        std::fs::remove_file(&webp).unwrap();
        assert_eq!(
            resolve_static_or_optimized(&jpg.display().to_string(), &cfg),
            jpg.display().to_string(),
            "no webp -> returns original unchanged"
        );
    }

    #[tokio::test]
    async fn repoint_optimized_wallpaper_noop_when_name_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path();
        std::fs::write(cache.join("outputs.json"), r#"{"DP-1":{"type":"static","path":"/w/x.png"}}"#).unwrap();
        repoint_optimized_wallpaper(cache, "x.png", "x.png", "/w/x.png").await;
        let outputs = read_outputs_state(cache).await;
        assert_eq!(outputs["DP-1"]["path"], "/w/x.png");
    }

    #[test]
    fn audio_dedup_unmutes_only_first_output_when_globally_unmuted() {
        let existing = serde_json::json!({});
        let outs = vec!["DP-2".to_string(), "DP-1".to_string()];
        let res = compute_audio_dedup_from(&existing, &outs, &HashMap::new(), "static", "/w.png", "", false);
        assert_eq!(res.get("DP-1"), Some(&false));
        assert_eq!(res.get("DP-2"), Some(&true));
    }

    #[test]
    fn audio_dedup_mutes_all_when_globally_muted() {
        let existing = serde_json::json!({});
        let outs = vec!["DP-1".to_string(), "DP-2".to_string()];
        let res = compute_audio_dedup_from(&existing, &outs, &HashMap::new(), "static", "/w.png", "", true);
        assert!(res.values().all(|&m| m));
    }

    #[test]
    fn audio_dedup_defers_to_external_unmuted_same_source() {
        let existing = serde_json::json!({
            "DP-3": { "type": "static", "path": "/w.png", "mute": false }
        });
        let outs = vec!["DP-1".to_string()];
        let res = compute_audio_dedup_from(&existing, &outs, &HashMap::new(), "static", "/w.png", "", false);
        assert_eq!(res.get("DP-1"), Some(&true));
    }

    #[test]
    fn audio_dedup_honors_per_output_unmute_override() {
        let existing = serde_json::json!({});
        let outs = vec!["DP-1".to_string(), "DP-2".to_string()];
        let mut audio = HashMap::new();
        audio.insert("DP-2".to_string(), false);
        let res = compute_audio_dedup_from(&existing, &outs, &audio, "static", "/w.png", "", true);
        assert_eq!(res.get("DP-2"), Some(&false));
        assert_eq!(res.get("DP-1"), Some(&true));
    }

    // Contract: skwd-wall's DaemonClient.qml (wall.outputs) reads type/path/we_id/mute off each
    // outputs.json entry. save_outputs_state is what writes them.
    #[tokio::test]
    async fn save_outputs_state_entry_contract() {
        let dir = tempfile::tempdir().unwrap();
        let mut mute = HashMap::new();
        mute.insert("DP-1".to_string(), false);
        save_outputs_state(dir.path(), &["DP-1".to_string()], "static", "/wall.png", "", &mute).await;
        let state = read_outputs_state(dir.path()).await;
        assert_eq!(state["DP-1"]["type"], "static");
        assert_eq!(state["DP-1"]["path"], "/wall.png");
        assert_eq!(state["DP-1"]["mute"], false);

        save_outputs_state(dir.path(), &["DP-2".to_string()], "we", "", "12345", &HashMap::new()).await;
        let state2 = read_outputs_state(dir.path()).await;
        assert_eq!(state2["DP-2"]["type"], "we");
        assert_eq!(state2["DP-2"]["we_id"], "12345");
        assert_eq!(state2["DP-2"]["mute"], true);
    }
}
