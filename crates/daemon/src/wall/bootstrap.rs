use std::path::Path;

use tracing::info;

use crate::config::Config;

pub async fn run(config: &Config) {
    let config_dir = crate::config::config_dir();
    let marker = config_dir.join(".bootstrapped");

    if marker.exists() {
        info!("bootstrap: already done, skipping");
        return;
    }

    info!("bootstrap: first run, setting up directories and defaults");

    let cache_dir = config.cache_dir().join("wallpaper");
    let wp_dir = config.wallpaper_dir();
    let scripts_dir = config.scripts_dir();
    let template_dir = config.template_dir();

    for dir in [&config_dir, &cache_dir, &wp_dir, &scripts_dir, &template_dir] {
        tokio::fs::create_dir_all(dir).await.ok();
    }

    let data_dir = Config::data_dir();

    let config_json = config_dir.join("config.json");
    if !config_json.exists() {
        let example = data_dir.join("config.json.example");
        if example.exists() {
            if let Err(e) = tokio::fs::copy(&example, &config_json).await {
                tracing::warn!("bootstrap: failed to seed config.json: {e}");
            } else {
                info!("bootstrap: created default config.json");
            }
        }
    }

    seed_dir(&data_dir.join("scripts"), &scripts_dir, true).await;

    seed_dir(&data_dir.join("matugen/templates"), &template_dir, false).await;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string();
    tokio::fs::write(&marker, timestamp.as_bytes()).await.ok();
    info!("bootstrap: setup complete");
}

async fn seed_dir(src_dir: &Path, dst_dir: &Path, executable: bool) {
    let Ok(entries) = std::fs::read_dir(src_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name() {
            Some(n) => n.to_owned(),
            None => continue,
        };
        let dst = dst_dir.join(&name);
        if dst.exists() {
            continue;
        }
        if let Err(e) = tokio::fs::copy(&path, &dst).await {
            tracing::warn!("bootstrap: failed to copy {}: {e}", name.to_string_lossy());
            continue;
        }
        if executable {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o755);
                std::fs::set_permissions(&dst, perms).ok();
            }
        }
        info!("bootstrap: seeded {}", name.to_string_lossy());
    }
}
