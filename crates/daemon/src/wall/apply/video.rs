use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::process::Child;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::Config;

use super::*;

pub async fn apply_video(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    all_screens: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
    let _ = all_screens;
    apply_video_inner(path, outputs, neighbors, outputs_audio, outputs_volume, config, false).await
}

pub(super) async fn apply_video_inner(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
    restoring: bool,
) -> anyhow::Result<()> {
    let apply_total = std::time::Instant::now();
    let lock_wait = std::time::Instant::now();
    let _apply_guard = apply_lock().lock().await;
    let lock_wait_ms = lock_wait.elapsed().as_millis() as u64;
    let is_kde = is_kde();
    let mute = config.is_muted();
    let prev_image = read_prev_transition_image(&config.cache_dir()).await;
    let prev_was_we = linux_we_running().await;

    let dedup_target_outs: Vec<String> = if outputs.is_empty() {
        vec!["*".to_string()]
    } else {
        outputs.to_vec()
    };
    let dedup_mute = compute_audio_dedup(
        &config.cache_dir(),
        &dedup_target_outs,
        outputs_audio,
        "video",
        path,
        "",
        mute,
    )
    .await;

    if !prev_was_we {
        kill_legacy_video_procs().await;
    }

    let thumb_path: Option<PathBuf> = ensure_video_thumb_blocking(path, &config.cache_dir()).await;
    let thumb_str = thumb_path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();

    let matugen_handle = thumb_path.as_ref().map(|thumb| {
        let thumb = thumb.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            let wd_cache = cfg.cache_dir().join("wallpaper");
            let _ = tokio::fs::create_dir_all(&wd_cache).await;
            let _ = tokio::fs::copy(&thumb, wd_cache.join("current.jpg")).await;
            run_matugen(thumb.to_str().unwrap_or(""), &cfg).await;
            run_reloads(&cfg).await;
        })
    });

    if config.wants_external_render() {
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        let _ = tokio::fs::remove_file(config.video_dir().join("lockscreen-video.mp4")).await;
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
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_kde_video(path, outputs, &dedup_mute, outputs_volume, config).await?;
    } else {
        let global_volume = config.volume();
        let auto_scale = config.features.video_auto_scale;
        let volume_for = |out: &str| -> u32 {
            outputs_volume.get(out).copied().unwrap_or(global_volume)
        };
        let build_mpv_opts = |out_mute: bool, vol: u32| -> String {
            let mut parts: Vec<String> = Vec::new();
            if out_mute {
                parts.push("mute=yes".to_string());
            } else {
                parts.push("mute=no".to_string());
                parts.push(format!("volume={vol}"));
            }
            if auto_scale {
                parts.push("keepaspect=yes".to_string());
                parts.push("panscan=1.0".to_string());
                parts.push("video-unscaled=no".to_string());
            }
            parts.join(";")
        };
        let bin = paper_bin();
        let target_outs: Vec<String> = dedup_target_outs.clone();
        let mute_for = |out: &str| -> bool {
            dedup_mute.get(out).copied().unwrap_or(mute)
        };
        rebuild_scene_we(
            config,
            &target_outs,
            &std::collections::BTreeMap::new(),
            global_volume,
        )
        .await
        .ok();

        let video_alive = video_persist_alive_outputs(&target_outs).await;
        let image_alive = persist_alive_outputs(&target_outs).await;
        let all_image_alive = !target_outs.is_empty()
            && target_outs.iter().all(|o| image_alive.contains(o));

        let transition_handles = config.transition.enabled
            && !restoring
            && prev_image.as_deref().filter(|p| Path::new(p).exists()).is_some();

        let restore_via_transition = restoring && !thumb_str.is_empty();
        let mut video_persist_ok = !restoring;
        let mut cold_video_pids: Vec<u32> = Vec::new();
        if transition_handles {
            drop_video_persist_papers_for(&target_outs).await;
        } else if !restore_via_transition {
            for out in &target_outs {
                if !video_alive.contains(out) {
                    let mpv_opts = build_mpv_opts(mute_for(out), volume_for(out));
                    match spawn_video_persist_paper(&bin, out, path, &mpv_opts, config.display.fill_mode).await {
                        Ok(Some(pid)) => cold_video_pids.push(pid),
                        Ok(None) => {}
                        Err(e) => {
                            warn!(error = %e, output = %out, "video persist: spawn failed");
                            video_persist_ok = false;
                            break;
                        }
                    }
                }
            }
            if !video_persist_ok {
                drop_video_persist_paper().await;
            }
        }

        let to_image = if (config.transition.enabled && !restoring) || restore_via_transition {
            Some(path.to_string())
        } else {
            None
        };
        let prev_for_transition = if restore_via_transition {
            Some(thumb_str.clone())
        } else if !config.transition.enabled || restoring {
            None
        } else {
            prev_image
                .as_deref()
                .filter(|p| Path::new(p).exists())
                .map(std::string::ToString::to_string)
        };

        let mut transitions: Vec<Child> = Vec::new();
        let mut transition_ready_at: Option<std::time::Instant> = None;
        let transition_dur = config.transition.duration_ms;
        let mut image_persist_handled_transition = false;

        if video_persist_ok
            && all_image_alive
            && let (Some(prev), Some(new_img)) =
                (prev_for_transition.as_deref(), to_image.as_deref())
        {
            let thumbs = pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, new_img], 20).await;
            let shader = config.transition.shader.clone();
            let mut all_sent = true;
            for out in &target_outs {
                let m = mute_for(out);
                let v = if m { None } else { Some(volume_for(out)) };
                if !try_send_persist(out, new_img, &shader, transition_dur, &thumbs, Some(m), v).await {
                    all_sent = false;
                    break;
                }
            }
            if all_sent {
                transition_ready_at = Some(std::time::Instant::now());
                image_persist_handled_transition = true;
            }
        }

        if !image_persist_handled_transition && video_persist_ok {
            drop_persist_papers_for(&target_outs).await;
        }

        let mut cold_persist_spawned = false;
        if !image_persist_handled_transition
            && let (Some(prev), Some(new_img)) = (prev_for_transition.as_deref(), to_image.as_deref()) {
            let thumbs = pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, new_img], 20).await;
            let shader = config.transition.shader.clone();
            let dur_ms = if restore_via_transition { 1 } else { config.transition.duration_ms };
            for out in &target_outs {
                let m = mute_for(out);
                match spawn_persist_paper(
                    &bin, out, prev, new_img, &shader, dur_ms, &thumbs, config.display.fill_mode, m, volume_for(out),
                )
                .await
                {
                    Ok(()) => cold_persist_spawned = true,
                    Err(e) => warn!(error = %e, output = %out, "cold persist spawn failed"),
                }
            }
            if cold_persist_spawned {
                transition_ready_at = Some(std::time::Instant::now());
            }
        }

        if video_persist_ok || cold_persist_spawned {
            drop_steady_image_papers_for(&target_outs).await;
        }

        let have_transitions =
            !transitions.is_empty() || image_persist_handled_transition || cold_persist_spawned;
        let scheduled_transitions: Vec<Child> = std::mem::take(&mut transitions);

        if video_persist_ok || restore_via_transition {
            let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
            for mut c in prev_steady {
                let _ = c.start_kill();
            }

            let alive_video_outs: Vec<String> = video_alive.iter().cloned().collect();
            let mut alive_pids: Vec<u32> = Vec::new();
            for out in &alive_video_outs {
                if let Some(pid) = video_persist_pid_for(out).await {
                    alive_pids.push(pid);
                }
            }

            let mut alive_notifies: Vec<(u32, Arc<Notify>)> = Vec::new();
            {
                let mut reg = ready_registry().lock().await;
                for &pid in &alive_pids {
                    let notify = Arc::new(Notify::new());
                    reg.insert(pid, notify.clone());
                    alive_notifies.push((pid, notify));
                }
            }
            for out in &alive_video_outs {
                let _ = try_send_video_persist(out, path, mute_for(out)).await;
            }

            if have_transitions {
                let elapsed = transition_ready_at.map(|t| t.elapsed()).unwrap_or_default();
                let visual_remaining = std::time::Duration::from_millis(transition_dur)
                    .saturating_sub(elapsed);
                let kill_image_persist = false;
                let cold_pids = cold_video_pids.clone();
                let kill_targets = target_outs.clone();

                tokio::spawn(async move {
                    let visual_done = tokio::time::sleep(visual_remaining);

                    let alive_acks = async move {
                        let mut tasks: JoinSet<()> = JoinSet::new();
                        for (pid, notify) in alive_notifies {
                            tasks.spawn(async move {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_millis(3000),
                                    notify.notified(),
                                )
                                .await;
                                ready_registry().lock().await.remove(&pid);
                            });
                        }
                        while tasks.join_next().await.is_some() {}
                    };

                    tokio::join!(visual_done, alive_acks);

                    let mut cold_notifies: Vec<(u32, Arc<Notify>)> = Vec::new();
                    {
                        let mut reg = ready_registry().lock().await;
                        for &pid in &cold_pids {
                            let notify = Arc::new(Notify::new());
                            reg.insert(pid, notify.clone());
                            cold_notifies.push((pid, notify));
                        }
                    }
                    for &pid in &cold_pids {
                        let _ = nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(pid as i32),
                            nix::sys::signal::Signal::SIGUSR1,
                        );
                    }
                    let mut cold_tasks: JoinSet<()> = JoinSet::new();
                    for (pid, notify) in cold_notifies {
                        cold_tasks.spawn(async move {
                            let _ = tokio::time::timeout(
                                std::time::Duration::from_millis(3000),
                                notify.notified(),
                            )
                            .await;
                            ready_registry().lock().await.remove(&pid);
                        });
                    }
                    while cold_tasks.join_next().await.is_some() {}

                    for mut c in scheduled_transitions {
                        let _ = c.start_kill();
                    }
                    if kill_image_persist {
                        drop_persist_papers_for(&kill_targets).await;
                    }
                });
            } else {
                for pid in &cold_video_pids {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(*pid as i32),
                        nix::sys::signal::Signal::SIGUSR1,
                    );
                }
            }
            info!(
                video_reused = video_alive.len(),
                via_image_persist = image_persist_handled_transition,
                outputs = target_outs.len(),
                "apply_video persist"
            );
        } else {
            let mut steady: Vec<Child> = Vec::new();
            let mut steady_set: JoinSet<(String, std::io::Result<Child>)> = JoinSet::new();
            let fill_mode_arg = config.display.fill_mode.as_arg().to_string();
            for out in &target_outs {
                let bin = bin.clone();
                let path = path.to_string();
                let mpv_opts = build_mpv_opts(mute_for(out), volume_for(out));
                let out = out.clone();
                let fill = fill_mode_arg.clone();
                steady_set.spawn(async move {
                    let args = vec![
                        "--fill-mode".to_string(),
                        fill,
                        out.clone(),
                        path,
                        "-o".to_string(),
                        mpv_opts,
                    ];
                    (out, spawn_paper_await_ready(&bin, &args, 10000).await)
                });
            }
            while let Some(res) = steady_set.join_next().await {
                match res {
                    Ok((_, Ok(c))) => steady.push(c),
                    Ok((out, Err(e))) => warn!(error = %e, output = %out, "spawn video paper failed"),
                    Err(e) => warn!(error = %e, "video steady spawn task panicked"),
                }
            }

            let video_pids: Vec<u32> = steady.iter().filter_map(tokio::process::Child::id).collect();

            let mut f = fleet().lock().await;
            let prev_steady = std::mem::take(&mut f.steady);
            f.steady = steady;
            let s_len = f.steady.len();
            drop(f);
            for mut c in prev_steady {
                let _ = c.start_kill();
            }

            if have_transitions {
                let elapsed = transition_ready_at.map(|t| t.elapsed()).unwrap_or_default();
                let remaining = std::time::Duration::from_millis(transition_dur)
                    .saturating_sub(elapsed);
                tokio::spawn(async move {
                    if !remaining.is_zero() {
                        tokio::time::sleep(remaining).await;
                    }
                    for pid in &video_pids {
                        let _ = nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(*pid as i32),
                            nix::sys::signal::Signal::SIGUSR1,
                        );
                    }
                    for mut c in scheduled_transitions {
                        let _ = c.start_kill();
                    }
                });
            } else {
                for pid in &video_pids {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(*pid as i32),
                        nix::sys::signal::Signal::SIGUSR1,
                    );
                }
            }
            info!("apply_video spawned {s_len} steady, {} transition", if have_transitions { 1 } else { 0 });
        }
    }

    if prev_was_we {
        kill_legacy_video_procs().await;
    }

    save_state(&config.cache_dir(), "video", path, "").await;
    let prev_outputs_state = read_outputs_state(&config.cache_dir()).await;
    save_outputs_state(&config.cache_dir(), outputs, "video", path, "", &dedup_mute).await;
    if !outputs.is_empty() && !outputs.iter().any(|o| o == "*") {
        mute_wildcard_if_present(config).await;
    }
    preserve_group_audio(config, &prev_outputs_state).await;
    enforce_audio_dedup(config).await;

    let path = path.to_string();
    let config = config.clone();
    tokio::spawn(async move {
        if let Some(handle) = matugen_handle {
            let _ = handle.await;
        }
        if config.wants_external_render() {
            run_external_apply(&config, "video", &path, &thumb_str).await;
        }
        run_post_processing(&config, "video", &basename(&path), &path, &thumb_str, restoring).await;
        info!("post-apply tasks done for video: {path}");
    });

    info!(
        total_ms = apply_total.elapsed().as_millis() as u64,
        lock_wait_ms,
        "applied video wallpaper"
    );
    Ok(())
}
