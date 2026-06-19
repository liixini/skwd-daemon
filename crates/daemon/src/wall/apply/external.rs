use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;
use tracing::{info, warn};

use crate::config::Config;
use crate::util::CommandExt;

pub(super) async fn run_sh(cmd: &str) -> anyhow::Result<()> {
    let out = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        let mut detail = String::new();
        if !stderr.trim().is_empty() {
            detail.push_str(&format!("\n  stderr: {}", stderr.trim()));
        }
        if !stdout.trim().is_empty() {
            detail.push_str(&format!("\n  stdout: {}", stdout.trim()));
        }
        warn!("command failed ({}): {cmd}{detail}", out.status);
        anyhow::bail!("shell command failed ({}): {cmd}", out.status);
    }
    Ok(())
}

pub(super) async fn run_sh_status(cmd: &str) -> bool {
    match Command::new("sh").arg("-c").arg(cmd).silent().status().await {
        Ok(s) => s.success(),
        Err(_) => false,
    }
}

pub(super) fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

pub(super) fn basename(path: &str) -> String {
    Path::new(path)
        .file_name().map_or_else(|| path.to_string(), |s| s.to_string_lossy().into_owned())
}

pub(super) fn substitute_placeholders(template: &str, wp_type: &str, name: &str, path: &str, thumb: &str) -> String {
    template
        .replace("%type%", wp_type)
        .replace("%name%", name)
        .replace("%path%", path)
        .replace("%thumb%", thumb)
}

pub(super) async fn run_detached(cmd: &str) {
    let wrapped = format!(
        "nohup setsid sh -c {} </dev/null >/dev/null 2>&1 &",
        shell_quote(cmd)
    );
    if let Err(e) = run_sh(&wrapped).await {
        warn!("failed to spawn detached command: {e}");
    }
}

pub(super) async fn run_external_apply(config: &Config, wp_type: &str, path: &str, thumb: &str) {
    let cmd = match config.external_wallpaper_command.as_deref() {
        Some(s) if !s.is_empty() => s,
        _ => return,
    };
    let name = basename(path);
    let resolved = substitute_placeholders(cmd, wp_type, &name, path, thumb);
    info!("running external wallpaper command: {resolved}");
    run_detached(&resolved).await;
}

pub(super) async fn run_post_processing(
    config: &Config,
    wp_type: &str,
    name: &str,
    path: &str,
    thumb: &str,
    restoring: bool,
) {
    if restoring && !config.post_process_on_restore {
        return;
    }
    if config.post_processing.is_empty() {
        return;
    }
    for entry in &config.post_processing {
        if !entry.matches(wp_type) {
            continue;
        }
        let cmd = entry.command();
        if cmd.is_empty() {
            continue;
        }
        let resolved = substitute_placeholders(cmd, wp_type, name, path, thumb);
        info!("running post-processing ({wp_type}): {resolved}");
        run_detached(&resolved).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_wraps_and_escapes_single_quotes() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn basename_extracts_final_component() {
        assert_eq!(basename("/a/b/wall.png"), "wall.png");
        assert_eq!(basename("wall.png"), "wall.png");
        assert_eq!(basename("/a/b/"), "b");
    }

    #[test]
    fn substitute_placeholders_replaces_all_tokens() {
        let out = substitute_placeholders(
            "t=%type% n=%name% p=%path% th=%thumb%",
            "video",
            "clip",
            "/w/clip.mp4",
            "/c/clip.jpg",
        );
        assert_eq!(out, "t=video n=clip p=/w/clip.mp4 th=/c/clip.jpg");
    }

    #[test]
    fn substitute_placeholders_leaves_unknown_tokens() {
        let out = substitute_placeholders("%unknown% %name%", "static", "a", "p", "t");
        assert_eq!(out, "%unknown% a");
    }

    #[tokio::test]
    async fn run_sh_ok_on_success_err_on_failure() {
        assert!(run_sh("true").await.is_ok());
        let err = run_sh("echo boom >&2; exit 3").await.unwrap_err().to_string();
        assert!(err.contains("exit status: 3"), "error carries the exit status: {err}");
    }
}
