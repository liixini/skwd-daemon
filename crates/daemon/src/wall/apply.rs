use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;
use tracing::{info, warn};

use crate::config::{self, Config};
use crate::util::CommandExt;

pub async fn apply_static(path: &str, config: &Config) -> anyhow::Result<()> {
    let is_kde = is_kde();

    kill_wallpaper_procs().await;

    if is_kde {
        run_sh(&format!("plasma-apply-wallpaperimage {}", shell_quote(path))).await?;
    } else {
        run_sh(&format!(
            "if ! pgrep -x awww-daemon >/dev/null; then \
               setsid awww-daemon >/dev/null 2>&1 & disown; \
               for i in 1 2 3 4 5; do sleep 0.3; pgrep -x awww-daemon >/dev/null && break; done; \
             fi; \
             awww img {} --transition-type wipe --transition-angle 45 --transition-duration 0.5",
            shell_quote(path)
        ))
        .await?;
    }

    let current_dir = config.cache_dir().join("wallpaper");
    let _ = tokio::fs::create_dir_all(&current_dir).await;
    let wall_jpg = current_dir.join("current.jpg");
    let _ = tokio::fs::copy(path, &wall_jpg).await;

    save_state(&config.cache_dir(), "static", path, "").await;

    run_matugen(path, config).await;
    run_reloads(config).await;

    info!("applied static wallpaper: {path}");
    Ok(())
}

pub async fn apply_video(path: &str, config: &Config) -> anyhow::Result<()> {
    let is_kde = is_kde();
    let mute = config.is_muted();

    kill_wallpaper_procs().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    if is_kde {
        apply_kde_video(path, mute).await?;
    } else {
        let mute_flag = if mute { "loop --mute=yes" } else { "loop" };
        let cmd = format!(
            "pkill -9 mpvpaper 2>/dev/null; while pgrep -x mpvpaper >/dev/null; do sleep 0.1; done; nohup setsid mpvpaper -o '{}' '*' {} </dev/null >/dev/null 2>&1 &",
            mute_flag,
            shell_quote(path)
        );
        info!("apply_video cmd: {cmd}");
        run_sh(&cmd).await?;
    }

    let thumb_path: Option<PathBuf> = extract_video_thumb(path, config).await;
    if let Some(ref thumb) = thumb_path {
        let _ = tokio::fs::copy(thumb, config.cache_dir().join("wallpaper/current.jpg")).await;
        run_matugen(thumb.to_str().unwrap_or(""), config).await;
        run_reloads(config).await;
    }

    save_state(&config.cache_dir(), "video", path, "").await;
    info!("applied video wallpaper: {path}");
    Ok(())
}

pub async fn apply_we(we_id: &str, screens: &[String], config: &Config) -> anyhow::Result<()> {
    if !config.features.steam {
        anyhow::bail!("Steam feature is disabled");
    }
    let we_dir = config.we_dir();
    let item_dir = we_dir.join(we_id);

    if !item_dir.exists() {
        anyhow::bail!("WE item not found: {}", item_dir.display());
    }

    kill_wallpaper_procs().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let project_path = item_dir.join("project.json");
    let (we_type, we_file) = if project_path.exists() {
        let text = tokio::fs::read_to_string(&project_path).await.unwrap_or_default();
        let proj: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        let t = proj
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("scene")
            .to_lowercase();
        let f = proj.get("file").and_then(|v| v.as_str()).unwrap_or("").to_string();
        (t, f)
    } else {
        ("scene".to_string(), String::new())
    };

    if we_type == "video" && !we_file.is_empty() {
        let video_path = item_dir.join(&we_file);
        let video_str = video_path.display().to_string();

        if is_kde() {
            apply_kde_video(&video_str, config.is_muted()).await?;
        } else {
            let mute_flag = if config.is_muted() { "loop --mute=yes" } else { "loop" };
            run_sh(&format!(
                "nohup setsid mpvpaper -o '{}' '*' {} </dev/null >/dev/null 2>&1 &",
                mute_flag,
                shell_quote(&video_str)
            ))
            .await?;
        }
    } else {
        let audio_flag = if config.is_muted() { "--silent" } else { "" };
        let assets_arg = config
            .we_assets_dir()
            .map(|d| format!("--assets-dir {}", shell_quote(&d.display().to_string())))
            .unwrap_or_default();

        let screen_args: String = if screens.is_empty() {
            get_screen_args().await
        } else {
            screens
                .iter()
                .map(|n| format!(" --screen-root {n} --scaling fill"))
                .collect()
        };

        run_sh(&format!(
            "nohup setsid linux-wallpaperengine {} --no-fullscreen-pause --noautomute{} \
             --clamp border {} {} </dev/null >/dev/null 2>&1 &",
            audio_flag,
            screen_args,
            assets_arg,
            shell_quote(we_id)
        ))
        .await?;
    }

    save_state(&config.cache_dir(), "we", "", we_id).await;

    if let Some(preview) = find_we_preview(&item_dir).await {
        let wd_cache = config.cache_dir().join("wallpaper");
        let _ = tokio::fs::create_dir_all(&wd_cache).await;
        let _ = tokio::fs::copy(&preview, wd_cache.join("current.jpg")).await;
        let preview_str = preview.display().to_string();
        run_matugen(&preview_str, config).await;
        run_reloads(config).await;
    }

    info!("applied WE wallpaper: {we_id}");
    Ok(())
}

pub async fn restore(config: &Config) -> anyhow::Result<String> {
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
            apply_static(path, config).await?;
            Ok(path.to_string())
        }
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                anyhow::bail!("no path in state");
            }
            apply_video(path, config).await?;
            Ok(path.to_string())
        }
        "we" => {
            let we_id = state.get("we_id").and_then(|v| v.as_str()).unwrap_or("");
            if we_id.is_empty() {
                anyhow::bail!("no we_id in state");
            }
            apply_we(we_id, &[], config).await?;
            Ok(we_id.to_string())
        }
        _ => anyhow::bail!("unknown wallpaper type: {wp_type}"),
    }
}

pub async fn retheme(config: &Config, scheme: Option<&str>, mode: Option<&str>) -> anyhow::Result<()> {
    let current_jpg = config.cache_dir().join("wallpaper/current.jpg");
    if !current_jpg.exists() {
        anyhow::bail!("no current wallpaper image to retheme");
    }
    let image_path = current_jpg.display().to_string();
    run_matugen_with(&image_path, config, scheme, mode).await;
    run_reloads(config).await;
    info!("retheme completed for {image_path}");
    Ok(())
}


async fn generate_matugen_config(config: &Config) -> PathBuf {
    let config_path = config.matugen_config_path();
    let template_dir = config.template_dir();
    let cache_dir = config.cache_dir();

    let mut lines = vec!["[config]".to_string(), "reload_apps = false".to_string(), String::new()];

    for (i, integ) in config.integrations.iter().enumerate() {
        let template = match &integ.template {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };
        let output = match &integ.output {
            Some(o) if !o.is_empty() => o,
            _ => continue,
        };

        let input_path = if template.contains('/') {
            config::resolve_tilde(template)
        } else {
            template_dir.join(template)
        };

        let output_path = if output.contains('/') {
            config::resolve_tilde(output)
        } else {
            cache_dir.join(output)
        };

        let safe_name = integ
            .name
            .as_deref()
            .unwrap_or(&format!("integration_{i}"))
            .replace(|c: char| !c.is_alphanumeric() && c != '_' && c != '-', "_");

        lines.push(format!("[templates.{safe_name}]"));
        lines.push(format!("input_path = \"{}\"", input_path.display()));
        lines.push(format!("output_path = \"{}\"", output_path.display()));
        lines.push(String::new());
    }

    let _ = tokio::fs::create_dir_all(config_path.parent().unwrap_or_else(|| Path::new("/tmp"))).await;
    let _ = tokio::fs::write(&config_path, lines.join("\n")).await;
    info!(
        "generated matugen config with {} integrations",
        config.integrations.len()
    );
    config_path
}

async fn run_matugen(image_path: &str, config: &Config) {
    if !config.features.matugen {
        return;
    }

    if Command::new("command")
        .arg("-v")
        .arg("matugen")
        .silent()
        .status()
        .await
        .map(|s| !s.success())
        .unwrap_or(true)
        && Command::new("which")
            .arg("matugen")
            .silent()
            .status()
            .await
            .map(|s| !s.success())
            .unwrap_or(true)
    {
        warn!("matugen not found in PATH, skipping");
        return;
    }

    let config_path = generate_matugen_config(config).await;
    let scheme = config.matugen_scheme();
    let mode = config.matugen_mode();
    run_matugen_inner(image_path, config, &config_path, scheme, mode).await;
}

async fn run_matugen_with(image_path: &str, config: &Config, scheme: Option<&str>, mode: Option<&str>) {
    if !config.features.matugen {
        return;
    }
    let config_path = generate_matugen_config(config).await;
    let scheme = scheme.unwrap_or_else(|| config.matugen_scheme());
    let mode = mode.unwrap_or_else(|| config.matugen_mode());
    run_matugen_inner(image_path, config, &config_path, scheme, mode).await;
}

async fn run_matugen_inner(image_path: &str, config: &Config, config_path: &Path, scheme: &str, mode: &str) {
    let status = Command::new("matugen")
        .arg("-c")
        .arg(&config_path)
        .arg("image")
        .arg("-t")
        .arg(scheme)
        .arg("-m")
        .arg(mode)
        .arg("--source-color-index")
        .arg("0")
        .arg(image_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await;

    match status {
        Ok(s) if s.success() => info!("matugen completed for {image_path}"),
        Ok(s) => warn!("matugen exited with {s} for {image_path}"),
        Err(e) => warn!("failed to run matugen: {e}"),
    }

    if let Some(default_cfg) = config.default_matugen_config_path()
        && default_cfg.exists()
    {
        let status = Command::new("matugen")
            .arg("-c")
            .arg(&default_cfg)
            .arg("image")
            .arg("-t")
            .arg(scheme)
            .arg("-m")
            .arg(mode)
            .arg("--source-color-index")
            .arg("0")
            .arg(image_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .await;

        if let Err(e) = status {
            warn!("failed to run matugen with default config: {e}");
        }
    }
}

async fn run_reloads(config: &Config) {
    for integ in &config.integrations {
        let reload = match &integ.reload {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };

        let resolved = config::resolve_tilde(reload);
        let cmd = if resolved.to_str().is_some_and(|s| s.contains('/') && !s.contains(' ')) {
            format!("sh {}", shell_quote(&resolved.display().to_string()))
        } else {
            reload.clone()
        };

        info!("running reload: {cmd}");
        let _ = run_sh(&cmd).await;
    }

    let _ = run_sh("command -v notify-send >/dev/null && notify-send 'Wallpaper Changed' || true").await;
}

async fn extract_video_thumb(video_path: &str, config: &Config) -> Option<PathBuf> {
    let name = Path::new(video_path)
        .file_stem()
        .map_or_else(|| "thumb.jpg".to_string(), |s| format!("{}.jpg", s.to_string_lossy()));

    let thumb_dir = config.cache_dir().join("wallpaper/video-thumbs");
    let _ = tokio::fs::create_dir_all(&thumb_dir).await;
    let thumb_path = thumb_dir.join(&name);

    if !thumb_path.exists() {
        let status = Command::new("ffmpeg")
            .args(["-y", "-i", video_path, "-vframes", "1", "-q:v", "2"])
            .arg(thumb_path.to_str()?)
            .silent()
            .status()
            .await;

        match status {
            Ok(s) if s.success() => {}
            _ => {
                warn!("failed to extract video thumbnail from {video_path}");
                return None;
            }
        }
    }

    Some(thumb_path)
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


async fn kill_wallpaper_procs() {
    let _ = run_sh(
        "pkill -9 -f '[l]inux-wallpaperengine' 2>/dev/null; \
         pkill -9 mpvpaper 2>/dev/null; \
         pkill awww 2>/dev/null; \
         pkill awww-daemon 2>/dev/null; \
         true",
    )
    .await;
}

async fn apply_kde_video(path: &str, mute: bool) -> anyhow::Result<()> {
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";
    let mute_mode = if mute { "4" } else { "0" };
    let file_url = format!("file://{path}");
    let script = format!(
        "var allDesktops = desktops(); \
         for (var i = 0; i < allDesktops.length; i++) {{ \
           var d = allDesktops[i]; \
           d.wallpaperPlugin = '{plugin}'; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           d.writeConfig('VideoUrls', '[{{\"filename\":\"{file_url}\",\"enabled\":true}}]'); \
           d.writeConfig('MuteMode', '{mute_mode}'); \
         }}"
    );
    run_sh(&format!(
        "qdbus6 org.kde.plasmashell /PlasmaShell org.kde.PlasmaShell.evaluateScript {}",
        shell_quote(&script)
    ))
    .await
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

fn is_kde() -> bool {
    std::env::var("XDG_CURRENT_DESKTOP")
        .map(|d| {
            let lower = d.to_lowercase();
            lower.contains("kde") || lower.contains("plasma")
        })
        .unwrap_or(false)
}

async fn run_sh(cmd: &str) -> anyhow::Result<()> {
    let status = Command::new("sh").arg("-c").arg(cmd).silent().status().await?;
    if !status.success() {
        warn!("command failed ({}): {cmd}", status);
    }
    Ok(())
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

async fn get_screen_args() -> String {
    if let Ok(output) = Command::new("wlr-randr")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let names: Vec<&str> = text
            .lines()
            .filter(|l| !l.starts_with(' ') && !l.is_empty())
            .filter_map(|l| l.split_whitespace().next())
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {n} --scaling fill"))
                .collect::<String>();
        }
    }

    if let Ok(output) = Command::new("hyprctl")
        .arg("monitors")
        .arg("-j")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
        && let Ok(monitors) = serde_json::from_slice::<Vec<serde_json::Value>>(&output.stdout)
    {
        let names: Vec<&str> = monitors
            .iter()
            .filter_map(|m| m.get("name").and_then(|v| v.as_str()))
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {n} --scaling fill"))
                .collect::<String>();
        }
    }

    if let Ok(output) = Command::new("niri")
        .arg("msg")
        .arg("outputs")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let names: Vec<&str> = text
            .lines()
            .filter_map(|l| {
                let trimmed = l.trim();
                if trimmed.starts_with("Output") {
                    trimmed.split_whitespace().nth(1).map(|s| s.trim_end_matches(':'))
                } else {
                    None
                }
            })
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {n} --scaling fill"))
                .collect::<String>();
        }
    }

    String::new()
}
