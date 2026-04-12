use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

pub const CMD_TIMEOUT: Duration = Duration::from_secs(60);

pub trait CommandExt {
    fn silent(&mut self) -> &mut Self;
}

impl CommandExt for Command {
    fn silent(&mut self) -> &mut Self {
        self.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
    }
}

pub async fn timed_output(cmd: &mut Command, timeout: Duration) -> anyhow::Result<std::process::Output> {
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(result) => result.map_err(|e| anyhow::anyhow!("command failed: {e}")),
        Err(_) => anyhow::bail!("command timed out after {}s", timeout.as_secs()),
    }
}

pub async fn timed_status(cmd: &mut Command, timeout: Duration) -> anyhow::Result<std::process::ExitStatus> {
    match tokio::time::timeout(timeout, cmd.status()).await {
        Ok(result) => result.map_err(|e| anyhow::anyhow!("command failed: {e}")),
        Err(_) => anyhow::bail!("command timed out after {}s", timeout.as_secs()),
    }
}

pub struct Resolution {
    pub max_w: u32,
    pub max_h: u32,
}

#[must_use]
pub fn get_resolution(key: &str) -> Option<Resolution> {
    match key {
        "1080p" => Some(Resolution {
            max_w: 1920,
            max_h: 1080,
        }),
        "2k" => Some(Resolution {
            max_w: 2560,
            max_h: 1440,
        }),
        "4k" => Some(Resolution {
            max_w: 3840,
            max_h: 2160,
        }),
        _ => None,
    }
}

pub async fn scan_dir_by_ext(dir: &Path, exts: &[&str]) -> Vec<String> {
    let mut result = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return result;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_lowercase)
            .unwrap_or_default();
        if exts.contains(&ext.as_str()) {
            result.push(path.to_string_lossy().to_string());
        }
    }
    result.sort();
    result
}

#[must_use]
pub fn hash_prefix(s: &str) -> String {
    let h = s.bytes().fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
    format!("{:08x}", h & 0xFFFFFFFF)
}

#[must_use]
pub fn resolutions_json() -> serde_json::Value {
    serde_json::json!({
        "1080p": { "maxW": 1920, "maxH": 1080 },
        "2k":    { "maxW": 2560, "maxH": 1440 },
        "4k":    { "maxW": 3840, "maxH": 2160 },
    })
}

#[derive(Default)]
pub struct BatchJobState {
    pub running: bool,
    pub progress: usize,
    pub total: usize,
    pub current_file: String,
    pub succeeded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub cancel: bool,
}
