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

#[derive(Debug, Clone, Default)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub stdin: Option<Vec<u8>>,
    pub timeout: Option<Duration>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            stdin: None,
            timeout: None,
        }
    }

    #[must_use]
    pub fn args<I, S>(mut self, it: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(it.into_iter().map(Into::into));
        self
    }
}

#[async_trait::async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(&self, spec: CommandSpec) -> std::io::Result<std::process::Output>;
}

pub struct RealRunner;

#[async_trait::async_trait]
impl CommandRunner for RealRunner {
    async fn run(&self, spec: CommandSpec) -> std::io::Result<std::process::Output> {
        let timeout = spec.timeout.unwrap_or(CMD_TIMEOUT);
        let mut cmd = Command::new(&spec.program);
        cmd.args(&spec.args);
        cmd.stdin(if spec.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let fut = async {
            let mut child = cmd.spawn()?;
            if let Some(data) = &spec.stdin
                && let Some(mut si) = child.stdin.take()
            {
                use tokio::io::AsyncWriteExt;
                si.write_all(data).await?;
                drop(si);
            }
            child.wait_with_output().await
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("command timed out after {}s", timeout.as_secs()),
            )),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub enum OutSpec {
    LastArg,
    #[allow(dead_code)]
    Arg(usize),
}

#[cfg(test)]
struct FakeResponse {
    program: String,
    arg_contains: Vec<String>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    code: i32,
    creates: Option<(OutSpec, Vec<u8>)>,
}

#[cfg(test)]
#[derive(Default)]
pub struct FakeRunner {
    responses: std::sync::Mutex<Vec<FakeResponse>>,
    pub calls: std::sync::Mutex<Vec<CommandSpec>>,
}

#[cfg(test)]
impl FakeRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on(&self, program: &str, arg_contains: &[&str], stdout: &[u8], code: i32) -> &Self {
        self.responses.lock().unwrap().push(FakeResponse {
            program: program.to_string(),
            arg_contains: arg_contains.iter().map(|s| (*s).to_string()).collect(),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
            code,
            creates: None,
        });
        self
    }

    /// Like `on`, but on match also writes `bytes` to the output file the command targets
    /// (so the caller's later metadata()/read()/rename() succeed) before returning success.
    pub fn on_creating(
        &self,
        program: &str,
        arg_contains: &[&str],
        out: OutSpec,
        bytes: &[u8],
    ) -> &Self {
        self.responses.lock().unwrap().push(FakeResponse {
            program: program.to_string(),
            arg_contains: arg_contains.iter().map(|s| (*s).to_string()).collect(),
            stdout: Vec::new(),
            stderr: Vec::new(),
            code: 0,
            creates: Some((out, bytes.to_vec())),
        });
        self
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

#[cfg(test)]
impl FakeRunner {
    fn perform_side_effect(spec: &CommandSpec, out: OutSpec, bytes: &[u8]) {
        let path = match out {
            OutSpec::LastArg => spec.args.last().cloned(),
            OutSpec::Arg(i) => spec.args.get(i).cloned(),
        };
        if let Some(p) = path {
            if let Some(parent) = std::path::Path::new(&p).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&p, bytes);
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl CommandRunner for FakeRunner {
    async fn run(&self, spec: CommandSpec) -> std::io::Result<std::process::Output> {
        self.calls.lock().unwrap().push(spec.clone());
        let matched = {
            let responses = self.responses.lock().unwrap();
            responses
                .iter()
                .find(|r| {
                    r.program == spec.program
                        && r.arg_contains
                            .iter()
                            .all(|needle| spec.args.iter().any(|a| a.contains(needle)))
                })
                .map(|r| (r.stdout.clone(), r.stderr.clone(), r.code, r.creates.clone()))
        };
        let (stdout, stderr, code, creates) = matched.unwrap_or_default();
        if let Some((out, bytes)) = creates {
            Self::perform_side_effect(&spec, out, &bytes);
        }
        use std::os::unix::process::ExitStatusExt;
        Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw((code & 0xff) << 8),
            stdout,
            stderr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_prefix_is_deterministic_and_eight_hex_chars() {
        let a = hash_prefix("wallpaper.jpg");
        assert_eq!(a, hash_prefix("wallpaper.jpg"));
        assert_eq!(a.len(), 8);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(hash_prefix("a"), hash_prefix("b"));
        assert_eq!(hash_prefix(""), "00000000");
    }

    #[test]
    fn get_resolution_known_keys() {
        let r = get_resolution("2k").unwrap();
        assert_eq!((r.max_w, r.max_h), (2560, 1440));
        let r = get_resolution("1080p").unwrap();
        assert_eq!((r.max_w, r.max_h), (1920, 1080));
        let r = get_resolution("4k").unwrap();
        assert_eq!((r.max_w, r.max_h), (3840, 2160));
        assert!(get_resolution("8k").is_none());
    }

    #[test]
    fn resolutions_json_matches_resolution_table() {
        let j = resolutions_json();
        for key in ["1080p", "2k", "4k"] {
            let r = get_resolution(key).unwrap();
            assert_eq!(j[key]["maxW"], r.max_w);
            assert_eq!(j[key]["maxH"], r.max_h);
        }
    }

    #[tokio::test]
    async fn scan_dir_by_ext_filters_sorts_and_lowercases() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["b.JPG", "a.png", "note.txt", "c.mp4"] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        std::fs::create_dir(dir.path().join("sub.png")).unwrap();

        let found = scan_dir_by_ext(dir.path(), &["jpg", "png"]).await;
        let names: Vec<String> = found
            .iter()
            .map(|p| Path::new(p).file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["a.png".to_string(), "b.JPG".to_string()]);
    }

    #[tokio::test]
    async fn scan_dir_by_ext_missing_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(scan_dir_by_ext(&missing, &["png"]).await.is_empty());
    }
}
