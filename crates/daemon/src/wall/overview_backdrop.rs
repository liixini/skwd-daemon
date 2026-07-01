use std::path::{Path, PathBuf};
use std::process::Stdio;

use image::ImageReader;
use tokio::process::Command;

use crate::config::Config;

const NAMESPACE: &str = "skwd-paper-backdrop";

pub fn backdrop_path(config: &Config) -> PathBuf {
    config.cache_dir().join("wallpaper").join("overview-backdrop.jpg")
}

pub async fn resolve_source(config: &Config) -> Option<String> {
    if let Some(b) = config.niri.backdrop_source() {
        return Some(b.to_string());
    }
    let cache_dir = config.cache_dir();
    let state_path = cache_dir.join("last-wallpaper.json");
    let text = tokio::fs::read_to_string(&state_path).await.ok()?;
    let state: serde_json::Value = serde_json::from_str(&text).ok()?;
    let wp_type = state.get("type").and_then(|v| v.as_str())?;

    match wp_type {
        "static" => state
            .get("path")
            .and_then(|v| v.as_str())
            .filter(|p| Path::new(p).exists())
            .map(std::string::ToString::to_string),
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str())?;
            let stem = Path::new(path).file_stem()?.to_string_lossy().into_owned();
            let thumb = cache_dir.join(format!("wallpaper/video-thumbs/{stem}.jpg"));
            if thumb.exists() { Some(thumb.display().to_string()) } else { None }
        }
        "we" => {
            let we_id = state.get("we_id").and_then(|v| v.as_str()).unwrap_or("");
            if !we_id.is_empty() {
                let thumb = cache_dir.join(format!("wallpaper/we-thumbs/{we_id}.webp"));
                if thumb.exists() {
                    return Some(thumb.display().to_string());
                }
            }
            state
                .get("path")
                .and_then(|v| v.as_str())
                .filter(|p| !p.is_empty() && Path::new(p).exists())
                .map(std::string::ToString::to_string)
        }
        _ => None,
    }
}

pub async fn refresh_for(config: &Config) {
    if let Some(src) = resolve_source(config).await {
        refresh(&src, config).await;
    }
}

fn is_niri() -> bool {
    std::env::var("XDG_CURRENT_DESKTOP")
        .map(|d| d.to_lowercase().contains("niri"))
        .unwrap_or(false)
}

pub async fn refresh(source: &str, config: &Config) {
    if !is_niri() {
        let _ = kill_existing().await;
        return;
    }
    if !config.niri.overview_backdrop {
        let _ = kill_existing().await;
        return;
    }

    let blur = config.niri.overview_backdrop_blur.max(1) as f32;
    let blur_enabled = config.niri.overview_backdrop_blur_enabled;
    let theme = config.niri.backdrop_theme_name().map(str::to_string);
    let dim = config.niri.backdrop_dim.min(100);
    let src = source.to_string();
    let dst = backdrop_path(config);

    let blur_result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut img = ImageReader::open(&src)?.with_guessed_format()?.decode()?;
        if let Some(t) = theme.as_deref() {
            match crate::wall::effects::native::theme_image(img.clone(), t) {
                Ok(themed) => img = themed,
                Err(e) => tracing::warn!("overview-backdrop: theme '{t}' failed: {e}"),
            }
        }
        let mut out = if blur_enabled {
            image::imageops::blur(&img.to_rgb8(), blur)
        } else {
            img.to_rgb8()
        };
        if dim > 0 {
            let keep = 100 - dim;
            for px in out.pixels_mut() {
                px[0] = u8::try_from(u32::from(px[0]) * keep / 100).unwrap_or(255);
                px[1] = u8::try_from(u32::from(px[1]) * keep / 100).unwrap_or(255);
                px[2] = u8::try_from(u32::from(px[2]) * keep / 100).unwrap_or(255);
            }
        }
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        out.save(&dst)?;
        Ok(())
    })
    .await;

    match blur_result {
        Ok(Ok(())) => {
            tracing::info!("overview-backdrop: regenerated blurred copy");
        }
        Ok(Err(e)) => {
            tracing::warn!("overview-backdrop: blur failed: {e}");
            return;
        }
        Err(e) => {
            tracing::warn!("overview-backdrop: blur task joined with error: {e}");
            return;
        }
    }

    if let Err(e) = respawn(&backdrop_path(config)).await {
        tracing::warn!("overview-backdrop: respawn failed: {e}");
    }
}

async fn kill_existing() -> anyhow::Result<()> {
    let _ = Command::new("pkill")
        .args(["-f", &format!("skwd-paper-still .*--namespace {NAMESPACE}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    Ok(())
}

async fn respawn(blurred: &Path) -> anyhow::Result<()> {
    let _ = kill_existing().await;

    let bin = which_paper_still();
    Command::new(bin)
        .args([
            "*",
            blurred.to_string_lossy().as_ref(),
            "--namespace",
            NAMESPACE,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(())
}

fn which_paper_still() -> String {
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent() {
            let local = dir.join("skwd-paper-still");
            if local.exists() {
                return local.to_string_lossy().to_string();
            }
        }
    "skwd-paper-still".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn config_with_cache(cache: &Path) -> Config {
        let mut config = Config::default();
        config.paths.cache = Some(cache.display().to_string());
        config
    }

    #[tokio::test]
    async fn resolve_source_follows_last_wallpaper_json() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path();
        let img = cache.join("wp.png");
        std::fs::write(&img, b"x").unwrap();
        std::fs::write(
            cache.join("last-wallpaper.json"),
            format!(r#"{{"type":"static","name":"wp.png","path":"{}"}}"#, img.display()),
        )
        .unwrap();

        assert_eq!(
            resolve_source(&config_with_cache(cache)).await,
            Some(img.display().to_string()),
            "backdrop must follow last-wallpaper.json - random rotation persists it, so refresh_for \
             (called from both the manual apply and the rotation loop) picks up the rotated wallpaper"
        );
    }

    #[tokio::test]
    async fn resolve_source_prefers_explicit_backdrop_over_wallpaper() {
        let dir = tempfile::tempdir().unwrap();
        let bd = dir.path().join("fixed.png");
        std::fs::write(&bd, b"x").unwrap();
        std::fs::write(
            dir.path().join("last-wallpaper.json"),
            r#"{"type":"static","name":"other.png","path":"/does/not/matter.png"}"#,
        )
        .unwrap();

        let mut config = config_with_cache(dir.path());
        config.niri.backdrop = bd.display().to_string();
        assert_eq!(
            resolve_source(&config).await,
            Some(bd.display().to_string()),
            "an explicit niri.backdrop path overrides the follow-wallpaper source"
        );
    }
}
