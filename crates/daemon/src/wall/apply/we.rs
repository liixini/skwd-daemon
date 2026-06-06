use std::collections::HashMap;

use tracing::info;

use crate::config::Config;

use super::*;

pub async fn apply_we(
    we_id: &str,
    screens: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
    apply_we_inner(we_id, screens, outputs_audio, outputs_volume, config, false).await
}

pub(super) async fn apply_we_inner(
    we_id: &str,
    screens: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
    restoring: bool,
) -> anyhow::Result<()> {
    let _apply_guard = apply_lock().lock().await;
    info!(we_id, ?screens, restoring, "apply_we_inner: start");
    if !config.features.steam {
        anyhow::bail!("Steam feature is disabled");
    }
    let we_dir = config.we_dir();
    let item_dir = we_dir.join(we_id);

    if !item_dir.exists() {
        anyhow::bail!("WE item not found: {}", item_dir.display());
    }

    kill_legacy_other_procs().await;

    if config.wants_external_render() {
        info!(we_id, "apply_we_inner: EXTERNAL render path (pickOnlyMode/externalWallpaperCommand)");
        let preview = find_we_preview(&item_dir).await;
        let preview_str = preview
            .as_ref()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string();
        save_state(&config.cache_dir(), "we", &preview_str, we_id).await;
        save_outputs_state(&config.cache_dir(), screens, "we", &preview_str, we_id, &HashMap::new()).await;

        run_external_apply(
            config,
            "we",
            &item_dir.display().to_string(),
            &preview_str,
        )
        .await;

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if screens.is_empty() || screens.iter().any(|o| o == "*") {
            drop_persist_paper().await;
            drop_video_persist_paper().await;
            drop_steady_image_paper().await;
            fleet().lock().await.replace_steady(Vec::new());
        } else {
            drop_persist_papers_for(screens).await;
            drop_video_persist_papers_for(screens).await;
            drop_steady_image_papers_for(screens).await;
        }

        let config_clone = config.clone();
        let item_dir_clone = item_dir.clone();
        let we_id_str = we_id.to_string();
        let preview_clone = preview.clone();
        let preview_str_clone = preview_str.clone();
        tokio::spawn(async move {
            if let Some(ref preview_path) = preview_clone {
                let wd_cache = config_clone.cache_dir().join("wallpaper");
                let _ = tokio::fs::create_dir_all(&wd_cache).await;
                let _ = tokio::fs::copy(preview_path, wd_cache.join("current.jpg")).await;
                run_matugen(&preview_str_clone, &config_clone).await;
                run_reloads(&config_clone).await;
            }
            run_post_processing(
                &config_clone,
                "we",
                &we_id_str,
                &item_dir_clone.display().to_string(),
                &preview_str_clone,
                restoring,
            )
            .await;
            info!("post-apply tasks done for WE: {we_id_str}");
        });
        info!("applied WE wallpaper (external mode)");
        return Ok(());
    }

    let (we_type, we_file) = read_we_project_type(&item_dir).await;
    info!(we_id, %we_type, %we_file, "apply_we_inner: INTERNAL render path, project type resolved");

    let preview = find_we_preview(&item_dir).await;
    let preview_str = preview
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();

    let matugen_handle = preview.as_ref().map(|preview_path| {
        let preview_path = preview_path.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            let wd_cache = cfg.cache_dir().join("wallpaper");
            let _ = tokio::fs::create_dir_all(&wd_cache).await;
            let _ = tokio::fs::copy(&preview_path, wd_cache.join("current.jpg")).await;
            run_matugen(&preview_path.display().to_string(), &cfg).await;
            run_reloads(&cfg).await;
        })
    });

    let global_mute = config.is_muted();
    let dedup_mute = compute_audio_dedup(
        &config.cache_dir(),
        screens,
        outputs_audio,
        "we",
        "",
        we_id,
        global_mute,
    )
    .await;
    let volume_for = |out: &str| -> u32 {
        outputs_volume.get(out).copied().unwrap_or_else(|| config.volume())
    };
    let scene_winner_volume: u32 = screens
        .iter()
        .find(|s| !dedup_mute.get(*s).copied().unwrap_or(global_mute)).map_or_else(|| config.volume(), |s| volume_for(s));

    let mut additions: std::collections::BTreeMap<String, (String, bool)> =
        std::collections::BTreeMap::new();
    if we_type == "scene" {
        for out in screens {
            let m = dedup_mute.get(out).copied().unwrap_or(global_mute);
            additions.insert(out.clone(), (we_id.to_string(), m));
        }
    }
    rebuild_scene_we(config, screens, &additions, scene_winner_volume).await?;

    if we_type == "scene" && is_kde() {
        kde_unload_video_plugin(screens).await;
    }

    if we_type == "video" && !we_file.is_empty() {
        let video_path = item_dir.join(&we_file);
        let video_str = video_path.display().to_string();

        if is_kde() {
            apply_kde_video(&video_str, screens, &dedup_mute, outputs_volume, config).await?;
        } else if !screens.is_empty() {
            for out in screens {
                let m = dedup_mute.get(out).copied().unwrap_or(global_mute);
                let mpv_opts = if m {
                    String::from("mute=yes")
                } else {
                    format!("mute=no;volume={}", volume_for(out))
                };
                run_sh(&format!(
                    "nohup setsid {} {} {} -o '{}' </dev/null >/dev/null 2>&1 &",
                    paper_bin(),
                    shell_quote(out),
                    shell_quote(&video_str),
                    mpv_opts
                ))
                .await?;
            }
        } else {
            let mpv_opts = if global_mute {
                String::from("mute=yes")
            } else {
                format!("mute=no;volume={}", config.volume())
            };
            run_sh(&format!(
                "nohup setsid {} '*' {} -o '{}' </dev/null >/dev/null 2>&1 &",
                paper_bin(),
                shell_quote(&video_str),
                mpv_opts
            ))
            .await?;
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    if screens.is_empty() || screens.iter().any(|o| o == "*") {
        drop_persist_paper().await;
        drop_video_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
    } else {
        drop_persist_papers_for(screens).await;
        drop_video_persist_papers_for(screens).await;
        drop_steady_image_papers_for(screens).await;
    }

    save_state(&config.cache_dir(), "we", &preview_str, we_id).await;
    let prev_outputs_state = read_outputs_state(&config.cache_dir()).await;
    save_outputs_state(&config.cache_dir(), screens, "we", &preview_str, we_id, &dedup_mute).await;
    if !screens.is_empty() && !screens.iter().any(|o| o == "*") {
        mute_wildcard_if_present(config).await;
    }
    preserve_group_audio(config, &prev_outputs_state).await;
    enforce_audio_dedup(config).await;

    let config = config.clone();
    let item_dir = item_dir.clone();
    let we_id = we_id.to_string();
    tokio::spawn(async move {
        if let Some(handle) = matugen_handle {
            let _ = handle.await;
        }
        run_post_processing(
            &config,
            "we",
            &we_id,
            &item_dir.display().to_string(),
            &preview_str,
            restoring,
        )
        .await;
        info!("post-apply tasks done for WE: {we_id}");
    });

    info!("applied WE wallpaper");
    Ok(())
}
