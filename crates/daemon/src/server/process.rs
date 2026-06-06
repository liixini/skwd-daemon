use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::config::Config;

use super::*;

/// When SKWD_HEADLESS is set, the daemon serves its socket but spawns no quickshell/UI/music
/// processes and skips the steamworks browser init. Used for end-to-end tests of the real binary.
pub(super) fn is_headless() -> bool {
    std::env::var_os("SKWD_HEADLESS").is_some()
}

pub struct ManagedProcess {
    child: Option<tokio::process::Child>,
    shell_qml: PathBuf,
    label: &'static str,
    env_key: &'static str,
    dry_run: bool,
}

impl ManagedProcess {
    pub(super) fn new(label: &'static str, env_key: &'static str, shell_qml: PathBuf) -> Self {
        Self {
            child: None,
            shell_qml,
            label,
            env_key,
            dry_run: false,
        }
    }

    #[cfg(test)]
    pub(super) fn new_dry(label: &'static str, env_key: &'static str, shell_qml: PathBuf) -> Self {
        Self {
            child: None,
            shell_qml,
            label,
            env_key,
            dry_run: true,
        }
    }

    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => {
                    self.child = None;
                    false
                }
                Ok(None) => true,
            }
        } else {
            false
        }
    }

    pub fn launch(&mut self) {
        self.launch_with_env(&[]);
    }

    pub fn launch_with_env(&mut self, extra_env: &[(&str, String)]) {
        if self.dry_run || is_headless() {
            return;
        }
        if self.is_running() {
            return;
        }
        let _ = std::process::Command::new("pkill")
            .arg("-f")
            .arg(format!("quickshell .*{}", self.shell_qml.display()))
            .status();
        info!("launching {}: quickshell -p {}", self.label, self.shell_qml.display());
        let install_dir = self.shell_qml.parent().unwrap_or_else(|| Path::new("/usr/share/skwd-wall"));
        let mut cmd = tokio::process::Command::new("quickshell");
        cmd.arg("-p").arg(&self.shell_qml).env(self.env_key, install_dir);
        cmd.kill_on_drop(true);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        cmd.stdin(std::process::Stdio::null());
        match open_managed_log(self.label) {
            Some(log_path) => match std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(&log_path) {
                Ok(stdout_file) => match stdout_file.try_clone() {
                    Ok(stderr_file) => {
                        info!("{}: spawn stdout/stderr -> {}", self.label, log_path.display());
                        cmd.stdout(std::process::Stdio::from(stdout_file));
                        cmd.stderr(std::process::Stdio::from(stderr_file));
                    }
                    Err(e) => {
                        warn!("{}: failed to clone log fd ({e}); spawn output discarded", self.label);
                        cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
                    }
                },
                Err(e) => {
                    warn!("{}: failed to open {} ({e}); spawn output discarded", self.label, log_path.display());
                    cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
                }
            },
            None => {
                cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
            }
        }
        match cmd.spawn() {
            Ok(child) => {
                self.child = Some(child);
            }
            Err(e) => {
                warn!("failed to launch {}: {e}", self.label);
            }
        }
    }

    pub fn kill(&mut self) {
        if let Some(ref mut child) = self.child {
            info!("killing {} process", self.label);
            let _ = child.start_kill();
            self.child = None;
        }
    }

    pub fn toggle(&mut self) {
        self.toggle_with_env(&[]);
    }

    pub fn toggle_with_env(&mut self, extra_env: &[(&str, String)]) {
        if self.is_running() {
            self.kill();
        } else {
            self.launch_with_env(extra_env);
        }
    }
}

pub(super) fn open_managed_log(label: &str) -> Option<PathBuf> {
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| std::env::var("HOME").ok().map(|h| PathBuf::from(h).join(".cache")))?
        .join("skwd");
    std::fs::create_dir_all(&base).ok()?;
    Some(base.join(format!("{label}.log")))
}

pub(super) fn resolve_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SHELL_QML") {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("shell.qml");
    if local.exists() {
        return std::fs::canonicalize(&local).unwrap_or(local);
    }
    let sibling = PathBuf::from("../skwd-wall/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    PathBuf::from("/usr/share/skwd-wall/shell.qml")
}

pub(super) fn resolve_bar_qml() -> PathBuf {
    resolve_dev_or_system("skwd-bar", "SKWD_BAR_QML")
}


pub(super) fn resolve_dev_or_system(name: &str, env_var: &str) -> PathBuf {
    if let Ok(p) = std::env::var(env_var) {
        return PathBuf::from(p);
    }
    if let Ok(install) = std::env::var("SKWD_INSTALL") {
        let p = PathBuf::from(install).join(name).join("shell.qml");
        if p.exists() {
            return p;
        }
    }
    let umbrella = PathBuf::from(format!("../skwd-shell/{name}/shell.qml"));
    if umbrella.exists() {
        return std::fs::canonicalize(&umbrella).unwrap_or(umbrella);
    }
    let sibling = PathBuf::from(format!("../{name}/shell.qml"));
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    let suite_path = PathBuf::from(format!("/usr/share/skwd/{name}/shell.qml"));
    if suite_path.exists() {
        return suite_path;
    }
    PathBuf::from(format!("/usr/share/{name}/shell.qml"))
}

pub(super) fn resolve_launch_qml() -> PathBuf {
    resolve_dev_or_system("skwd-launch", "SKWD_LAUNCH_QML")
}

pub(super) fn resolve_launch_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_LAUNCH_SHELL_QML") {
        return PathBuf::from(p);
    }
    let launch_shell_qml = resolve_launch_qml();
    let candidate = launch_shell_qml
        .parent()
        .map(|p| p.join("qml/launcher/LauncherShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-launch/qml/launcher/LauncherShell.qml")
}

pub(super) fn resolve_bar_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_BAR_SHELL_QML") {
        return PathBuf::from(p);
    }
    let bar_qml = resolve_bar_qml();
    let candidate = bar_qml.parent().map(|p| p.join("qml/bar/BarShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-bar/qml/bar/BarShell.qml")
}

pub(super) fn resolve_switch_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SWITCH_SHELL_QML") {
        return PathBuf::from(p);
    }
    let switch_qml = resolve_switch_qml();
    let candidate = switch_qml
        .parent()
        .map(|p| p.join("qml/switcher/SwitchShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-switch/qml/switcher/SwitchShell.qml")
}

pub(super) fn install_dir_of(shell_qml: &Path) -> String {
    shell_qml
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

pub(super) async fn build_host_env(config: &Config) -> Vec<(&'static str, String)> {
    let launch_shell   = resolve_launch_shell_qml();
    let bar_shell      = resolve_bar_shell_qml();
    let switch_shell   = resolve_switch_shell_qml();
    let settings_shell = resolve_settings_shell_qml();
    let power_shell    = resolve_power_shell_qml();

    let launch_install   = install_dir_of(&resolve_launch_qml());
    let bar_install      = install_dir_of(&resolve_bar_qml());
    let switch_install   = install_dir_of(&resolve_switch_qml());
    let settings_install = install_dir_of(&resolve_settings_qml());
    let power_install    = install_dir_of(&resolve_power_qml());

    let mut env: Vec<(&'static str, String)> = vec![
        ("SKWD_LAUNCH_SHELL",     launch_shell.display().to_string()),
        ("SKWD_BAR_SHELL",        bar_shell.display().to_string()),
        ("SKWD_SWITCH_SHELL",     switch_shell.display().to_string()),
        ("SKWD_SETTINGS_SHELL",   settings_shell.display().to_string()),
        ("SKWD_POWER_SHELL",      power_shell.display().to_string()),
        ("SKWD_LAUNCH_INSTALL",   launch_install),
        ("SKWD_BAR_INSTALL",      bar_install),
        ("SKWD_SWITCH_INSTALL",   switch_install),
        ("SKWD_SETTINGS_INSTALL", settings_install),
        ("SKWD_POWER_INSTALL",    power_install),
    ];
    if should_launch_notification(config, &DbusNameOwnership).await {
        let notification_shell = resolve_notification_shell_qml();
        let notification_install = install_dir_of(&resolve_notification_qml());
        env.push(("SKWD_NOTIFICATION_SHELL", notification_shell.display().to_string()));
        env.push(("SKWD_NOTIFICATION_INSTALL", notification_install));
    }
    env
}

pub(super) fn resolve_notification_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_NOTIFICATION_SHELL_QML") {
        return PathBuf::from(p);
    }
    let notification_qml = resolve_notification_qml();
    let candidate = notification_qml
        .parent()
        .map(|p| p.join("qml/NotificationShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-notification/qml/NotificationShell.qml")
}

pub(super) fn resolve_power_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_POWER_SHELL_QML") {
        return PathBuf::from(p);
    }
    let power_qml = resolve_power_qml();
    let candidate = power_qml
        .parent()
        .map(|p| p.join("qml/power/PowerShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-power/qml/power/PowerShell.qml")
}

pub(super) fn resolve_settings_shell_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_SETTINGS_SHELL_QML") {
        return PathBuf::from(p);
    }
    let settings_qml = resolve_settings_qml();
    let candidate = settings_qml
        .parent()
        .map(|p| p.join("qml/SettingsShell.qml"));
    if let Some(p) = candidate
        && p.exists() {
            return std::fs::canonicalize(&p).unwrap_or(p);
        }
    PathBuf::from("/usr/share/skwd/skwd-settings/qml/SettingsShell.qml")
}

pub(super) fn resolve_host_qml() -> PathBuf {
    if let Ok(p) = std::env::var("SKWD_HOST_QML") {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("data/host/shell.qml");
    if local.exists() {
        return std::fs::canonicalize(&local).unwrap_or(local);
    }
    let sibling = PathBuf::from("../skwd-daemon/data/host/shell.qml");
    if sibling.exists() {
        return std::fs::canonicalize(&sibling).unwrap_or(sibling);
    }
    if let Ok(install) = std::env::var("SKWD_INSTALL") {
        let p = PathBuf::from(install).join("skwd-daemon/host/shell.qml");
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("/usr/share/skwd/skwd-daemon/host/shell.qml")
}

pub(super) fn resolve_switch_qml() -> PathBuf {
    resolve_dev_or_system("skwd-switch", "SKWD_SWITCH_QML")
}

pub(super) fn resolve_notification_qml() -> PathBuf {
    resolve_dev_or_system("skwd-notification", "SKWD_NOTIFICATION_QML")
}

pub(super) fn resolve_music_qml() -> PathBuf {
    resolve_dev_or_system("skwd-music", "SKWD_MUSIC_QML")
}

pub(super) fn resolve_power_qml() -> PathBuf {
    resolve_dev_or_system("skwd-power", "SKWD_POWER_QML")
}

pub(super) fn resolve_settings_qml() -> PathBuf {
    resolve_dev_or_system("skwd-settings", "SKWD_SETTINGS_QML")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_dir_of_returns_parent_dir() {
        assert_eq!(install_dir_of(Path::new("/a/b/shell.qml")), "/a/b");
        assert_eq!(install_dir_of(Path::new("shell.qml")), "");
        assert_eq!(install_dir_of(Path::new("/")), "");
    }
}
