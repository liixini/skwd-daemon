
use tracing::info;

use crate::config::{self, Config};

use super::*;

pub(super) async fn swap_last_engine(new: config::PaperEngine) -> Option<config::PaperEngine> {
    static LAST: OnceLock<AsyncMutex<Option<config::PaperEngine>>> = OnceLock::new();
    let cell = LAST.get_or_init(|| AsyncMutex::new(None));
    let mut guard = cell.lock().await;
    let prev = *guard;
    *guard = Some(new);
    prev
}

pub(super) async fn wait_for_awww_ready() -> bool {
    let safety_deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if run_sh_status("awww query >/dev/null 2>&1").await {
            return true;
        }
        if std::time::Instant::now() >= safety_deadline {
            return false;
        }
        if !run_sh_status("pgrep -x awww-daemon >/dev/null 2>&1").await {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub(super) async fn kill_awww_if_running() {
    if run_sh_status("awww query >/dev/null 2>&1").await {
        info!("apply_static: shutting down awww-daemon for non-awww engine");
        let _ = run_sh("awww kill >/dev/null 2>&1; pkill -x awww-daemon 2>/dev/null; true").await;
    }
}

pub(super) async fn apply_awww(path: &str, outputs: &[String], config: &Config) -> anyhow::Result<()> {
    if !run_sh_status("awww query >/dev/null 2>&1").await {
        info!("apply_awww: awww-daemon not running, spawning it");
        let _ = run_sh("awww-daemon >/dev/null 2>&1 &").await;
        if !wait_for_awww_ready().await {
            anyhow::bail!("awww-daemon did not become ready (process missing or 15s safety cap)");
        }
        info!("apply_awww: awww-daemon ready");
    }

    let s = &config.paper.awww;
    let resize = match config.display.fill_mode {
        config::FillMode::Fill | config::FillMode::Tile => "crop",
        config::FillMode::Fit => "fit",
        config::FillMode::Stretch => "stretch",
        config::FillMode::Center => "no",
    };

    let duration_s = (s.transition_duration_ms as f32) / 1000.0;
    let mut args = format!(
        "--transition-type {} --transition-duration {} --transition-fps {} --transition-step {} --resize {} --filter {} --fill-color {}",
        shell_quote(&s.transition_type),
        duration_s,
        s.transition_fps,
        s.transition_step,
        resize,
        shell_quote(&s.filter),
        shell_quote(&s.fill_color),
    );

    match s.transition_type.as_str() {
        "wipe" | "wave" => {
            args.push_str(&format!(" --transition-angle {}", s.transition_angle));
        }
        _ => {}
    }
    if s.transition_type == "wave" {
        args.push_str(&format!(
            " --transition-wave {},{}",
            s.transition_wave_width, s.transition_wave_height
        ));
    }
    if matches!(s.transition_type.as_str(), "grow" | "outer") {
        args.push_str(&format!(" --transition-pos {}", shell_quote(&s.transition_pos)));
        if s.invert_y {
            args.push_str(" --invert-y");
        }
    }
    if s.transition_type == "fade" {
        args.push_str(&format!(" --transition-bezier {}", shell_quote(&s.transition_bezier)));
    }

    if outputs.is_empty() {
        let cmd = format!("awww img {} {}", args, shell_quote(path));
        info!(cmd = %cmd, "apply_static: awww img (all outputs)");
        run_sh(&cmd).await?;
    } else {
        for out in outputs {
            let cmd = format!(
                "awww img --outputs {} {} {}",
                shell_quote(out),
                args,
                shell_quote(path),
            );
            info!(cmd = %cmd, output = %out, "apply_static: awww img");
            run_sh(&cmd).await?;
        }
    }

    Ok(())
}
