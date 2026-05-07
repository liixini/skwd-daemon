use std::env;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

fn socket_path() -> PathBuf {
    let runtime = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(runtime).join("skwd").join("daemon.sock")
}

pub fn signal_ready() {
    let path = socket_path();
    let pid = std::process::id();
    let mut stream = match UnixStream::connect(&path) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, path = %path.display(), "ipc: connect failed");
            return;
        }
    };
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    let msg = format!("{{\"method\":\"paper.ready\",\"params\":{{\"pid\":{pid}}},\"id\":0}}\n");
    if let Err(e) = stream.write_all(msg.as_bytes()) {
        tracing::debug!(error = %e, "ipc: write failed");
    }
}
