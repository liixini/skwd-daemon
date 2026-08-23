#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

use std::collections::{HashMap, HashSet};
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{self, Config};

use super::{paper_bin, read_outputs_state, read_prev_transition_image};

const PAPER_READY_TIMEOUT_MS: u64 = 2500;

fn paper_stderr() -> Stdio {
    Stdio::inherit()
}

#[derive(Default)]
pub(super) struct PaperFleet {
    pub(super) steady: Vec<Child>,
    pub(super) transitions: Vec<Child>,
}

impl PaperFleet {
    pub(super) fn replace_steady(&mut self, new_steady: Vec<Child>) {
        for mut c in self.steady.drain(..) {
            let _ = c.start_kill();
        }
        for mut c in self.transitions.drain(..) {
            let _ = c.start_kill();
        }
        self.steady = new_steady;
    }

}

pub(super) struct PersistPaper {
    pub(super) child: Child,
    pub(super) stdin: ChildStdin,
}

pub(super) struct TransitionOverlayHandle {
    pid: u32,
    kill_tx: tokio::sync::oneshot::Sender<()>,
}

#[derive(Default)]
pub(super) struct PaperGroup {
    map: AsyncMutex<HashMap<String, PersistPaper>>,
}

impl PaperGroup {
    async fn insert_killing_prev(&self, output: String, paper: PersistPaper) {
        if let Some(mut prev) = self.map.lock().await.insert(output, paper) {
            let _ = prev.child.start_kill();
        }
    }

    async fn drop_all(&self) {
        let mut map = self.map.lock().await;
        for (_, mut p) in map.drain() {
            let _ = p.child.start_kill();
        }
    }

    async fn drop_for(&self, targets: &[String]) {
        let dead: Vec<Child> = {
            let mut map = self.map.lock().await;
            let mut dead: Vec<Child> = Vec::new();
            if targets.iter().any(|o| o == "*") {
                for (_, mut p) in map.drain() {
                    let _ = p.child.start_kill();
                    dead.push(p.child);
                }
            } else {
                if let Some(mut p) = map.remove("*") {
                    let _ = p.child.start_kill();
                    dead.push(p.child);
                }
                for out in targets {
                    if let Some(mut p) = map.remove(out) {
                        let _ = p.child.start_kill();
                        dead.push(p.child);
                    }
                }
            }
            dead
        };
        await_dead_papers(dead).await;
    }

    async fn alive_outputs(&self, target_outs: &[String]) -> HashSet<String> {
        let mut map = self.map.lock().await;
        let mut alive = HashSet::new();
        let mut to_remove = Vec::new();
        for out in target_outs {
            if let Some(p) = map.get_mut(out) {
                match p.child.try_wait() {
                    Ok(None) => {
                        alive.insert(out.clone());
                    }
                    _ => to_remove.push(out.clone()),
                }
            }
        }
        for out in to_remove {
            map.remove(&out);
        }
        alive
    }

    async fn pid_for(&self, output: &str) -> Option<u32> {
        let mut map = self.map.lock().await;
        let p = map.get_mut(output)?;
        if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
            map.remove(output);
            return None;
        }
        p.child.id()
    }

    async fn try_send_json(&self, output: &str, payload: &serde_json::Value) -> bool {
        let mut map = self.map.lock().await;
        let Some(p) = map.get_mut(output) else { return false };
        if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
            map.remove(output);
            return false;
        }
        let line = format!("{payload}\n");
        if let Err(e) = p.stdin.write_all(line.as_bytes()).await {
            warn!(output = %output, error = %e, "paper: stdin write failed, dropping paper");
            if let Some(mut paper) = map.remove(output) {
                let _ = paper.child.start_kill();
            }
            return false;
        }
        if let Err(e) = p.stdin.flush().await {
            warn!(output = %output, error = %e, "paper: stdin flush failed, dropping paper");
            if let Some(mut paper) = map.remove(output) {
                let _ = paper.child.start_kill();
            }
            return false;
        }
        true
    }
}

#[derive(Default)]
pub(super) struct PaperManager {
    persist: PaperGroup,
    video: PaperGroup,
    steady: PaperGroup,
    overlays: AsyncMutex<HashMap<String, TransitionOverlayHandle>>,
    ready: AsyncMutex<HashMap<u32, Arc<Notify>>>,
    fleet: AsyncMutex<PaperFleet>,
    apply_lock: AsyncMutex<()>,
    preheat: StdMutex<HashSet<String>>,
}

pub(super) fn manager() -> &'static PaperManager {
    static M: OnceLock<PaperManager> = OnceLock::new();
    M.get_or_init(PaperManager::default)
}

pub(super) fn fleet() -> &'static AsyncMutex<PaperFleet> {
    &manager().fleet
}

pub(super) fn apply_lock() -> &'static AsyncMutex<()> {
    &manager().apply_lock
}

pub fn ready_registry() -> &'static AsyncMutex<HashMap<u32, Arc<Notify>>> {
    &manager().ready
}

pub(super) fn persist_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    &manager().persist.map
}

pub(super) fn video_persist_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    &manager().video.map
}

pub(super) fn steady_image_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    &manager().steady.map
}

pub(super) fn transition_overlays() -> &'static AsyncMutex<HashMap<String, TransitionOverlayHandle>> {
    &manager().overlays
}

pub(super) async fn drop_transition_overlays_for(targets: &[String]) {
    let star_mode = targets.iter().any(|o| o == "*");
    let mut map = transition_overlays().lock().await;
    let handles: Vec<TransitionOverlayHandle> = if star_mode {
        map.drain().map(|(_, v)| v).collect()
    } else {
        targets.iter().filter_map(|o| map.remove(o)).collect()
    };
    drop(map);
    for h in handles {
        let _ = h.kill_tx.send(());
    }
}

pub async fn drop_steady_image_paper() {
    manager().steady.drop_all().await;
}

pub(super) async fn broadcast_warmup(warmup: bool) {
    let line = format!(
        "{}\n",
        serde_json::json!({"to": "", "warmup": warmup})
    );
    let mut map = persist_papers().lock().await;
    let outputs: Vec<String> = map.keys().cloned().collect();
    for out in outputs {
        if let Some(p) = map.get_mut(&out) {
            let _ = p.stdin.write_all(line.as_bytes()).await;
            let _ = p.stdin.flush().await;
        }
    }
}

async fn outputs_divergent_static(cache_dir: &Path) -> bool {
    let state = read_outputs_state(cache_dir).await;
    let Some(map) = state.as_object() else {
        return false;
    };
    if map.is_empty() {
        return false;
    }
    let mut paths = HashSet::new();
    for entry in map.values() {
        if entry.get("type").and_then(|v| v.as_str()).unwrap_or("static") != "static" {
            return true;
        }
        paths.insert(entry.get("path").and_then(|v| v.as_str()).unwrap_or("").to_string());
    }
    paths.len() > 1
}

pub async fn on_wall_show(config: &Config) {
    if super::is_kde() {
        return;
    }
    if config.paper.engine != config::PaperEngine::SkwdPaper {
        return;
    }
    let prev = match read_prev_transition_image(&config.cache_dir()).await {
        Some(p) if Path::new(&p).exists() => p,
        _ => return,
    };
    let state_path = config.cache_dir().join("last-wallpaper.json");
    let is_static = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(String::from))
            .is_some_and(|t| t == "static"),
        Err(_) => false,
    };
    if !is_static {
        return;
    }
    {
        let map = persist_papers().lock().await;
        if !map.is_empty() {
            return;
        }
    }

    if outputs_divergent_static(&config.cache_dir()).await {
        return;
    }
    let bin = paper_bin();
    let shader = config.transition.shader.clone();
    let args: Vec<String> = vec![
        "--transition-from".to_string(),
        prev.clone(),
        "--shader".to_string(),
        shader,
        "--duration-ms".to_string(),
        "1".to_string(),
        "--persist".to_string(),
        "--fill-mode".to_string(),
        config.display.fill_mode.as_arg().to_string(),
        "*".to_string(),
        prev.clone(),
    ];
    let mut cmd = Command::new(&bin);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "wall.show: failed to prewarm GL persist");
            return;
        }
    };
    let Some(stdin) = child.stdin.take() else {
        let _ = child.start_kill();
        return;
    };
    let pid = child.id();
    let mut map = persist_papers().lock().await;
    map.insert("*".to_string(), PersistPaper { child, stdin });
    info!(pid = ?pid, "wall.show: GL persist prewarmed");
    drop(map);
    broadcast_warmup(true).await;
}

pub async fn on_wall_hide() {
    broadcast_warmup(false).await;
}

pub(super) async fn try_send_steady_image(output: &str, path: &str) -> bool {
    let payload = serde_json::json!({"path": path});
    manager().steady.try_send_json(output, &payload).await
}

pub(super) async fn spawn_steady_image_paper(
    bin: &str,
    output: &str,
    path: &str,
    fill_mode: crate::config::FillMode,
) -> std::io::Result<()> {
    let args: Vec<String> = vec![
        "--persist".to_string(),
        "--fill-mode".to_string(),
        fill_mode.as_arg().to_string(),
        output.to_string(),
        path.to_string(),
    ];
    let mut cmd = Command::new(bin);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = cmd.spawn()?;
    let Some(stdin) = child.stdin.take() else {
        let _ = child.start_kill();
        return Err(std::io::Error::other("steady image paper missing stdin"));
    };
    let pid = child.id();
    let notify = if let Some(p) = pid {
        let n = Arc::new(Notify::new());
        ready_registry().lock().await.insert(p, n.clone());
        Some((p, n))
    } else {
        None
    };
    if let Some((p, n)) = notify {
        match await_paper_ready_or_exit(&mut child, p, n, PAPER_READY_TIMEOUT_MS).await {
            Ok(true) => {}
            Ok(false) => warn!(pid = p, output = %output, "steady image paper readiness timed out"),
            Err(error) => return Err(error),
        }
    }
    manager()
        .steady
        .insert_killing_prev(output.to_string(), PersistPaper { child, stdin })
        .await;
    info!(pid = ?pid, output = %output, "steady image: spawned");
    Ok(())
}

pub(super) async fn spawn_transition_overlay(
    bin: &str,
    output: &str,
    from_path: &str,
    to_path: &str,
    shader: &str,
    duration_ms: u64,
    thumbs: &[String],
    fill_mode: crate::config::FillMode,
) -> std::io::Result<()> {
    let from_path = crate::wall::optimized::optimized_or(from_path);
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let mut args: Vec<String> = vec![
        "--transition-from".to_string(),
        from_path,
        "--shader".to_string(),
        shader.to_string(),
        "--duration-ms".to_string(),
        duration_ms.to_string(),
        "--fill-mode".to_string(),
        fill_mode.as_arg().to_string(),
        "--layer".to_string(),
        "bottom".to_string(),
    ];
    if !thumbs.is_empty() {
        args.push("--thumbs".to_string());
        args.push(thumbs.join(","));
    }
    args.push(output.to_string());
    args.push(to_path);

    let mut cmd = Command::new(bin);
    cmd.args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = cmd.spawn()?;
    let Some(pid) = child.id() else {
        return Err(std::io::Error::other("transition overlay: missing pid"));
    };
    let notify = Arc::new(Notify::new());
    ready_registry().lock().await.insert(pid, notify.clone());
    match await_paper_ready_or_exit(&mut child, pid, notify, PAPER_READY_TIMEOUT_MS).await {
        Ok(true) => {}
        Ok(false) => warn!(pid, output = %output, "transition overlay readiness timed out"),
        Err(error) => return Err(error),
    }
    let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
    let output_owned = output.to_string();
    tokio::spawn(async move {
        tokio::select! {
            _ = child.wait() => {}
            _ = kill_rx => {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
        let mut map = transition_overlays().lock().await;
        if let Some(h) = map.get(&output_owned)
            && h.pid == pid
        {
            map.remove(&output_owned);
        }
    });
    if let Some(prev) = transition_overlays()
        .lock()
        .await
        .insert(output.to_string(), TransitionOverlayHandle { pid, kill_tx })
    {
        let _ = prev.kill_tx.send(());
    }
    info!(pid, output = %output, "transition overlay: spawned");
    Ok(())
}

pub(super) async fn ensure_steady_image_paper(
    bin: &str,
    outputs: &[String],
    path: &str,
    fill_mode: crate::config::FillMode,
) {
    prune_steady_image_papers(outputs).await;
    for out in outputs {
        if try_send_steady_image(out, path).await {
            continue;
        }
        if let Err(e) = spawn_steady_image_paper(bin, out, path, fill_mode).await {
            warn!(error = %e, output = %out, "steady image: spawn failed");
        }
    }
}

pub async fn drop_video_persist_paper() {
    manager().video.drop_all().await;
}

pub(super) async fn video_persist_alive_outputs(target_outs: &[String]) -> HashSet<String> {
    manager().video.alive_outputs(target_outs).await
}

pub(super) async fn persist_alive_outputs(target_outs: &[String]) -> HashSet<String> {
    manager().persist.alive_outputs(target_outs).await
}

pub(super) async fn video_persist_pid_for(output: &str) -> Option<u32> {
    manager().video.pid_for(output).await
}

pub(super) async fn try_send_video_persist(output: &str, to_path: &str, mute: bool) -> bool {
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let payload = serde_json::json!({"path": to_path, "mute": mute});
    manager().video.try_send_json(output, &payload).await
}

pub(super) async fn spawn_video_persist_paper(
    bin: &str,
    output: &str,
    file_path: &str,
    mpv_opts: &str,
    fill_mode: crate::config::FillMode,
) -> std::io::Result<Option<u32>> {
    let file_path = crate::wall::optimized::optimized_or(file_path);
    let args: Vec<String> = vec![
        "--persist".to_string(),
        "--fill-mode".to_string(),
        fill_mode.as_arg().to_string(),
        output.to_string(),
        file_path,
        "-o".to_string(),
        mpv_opts.to_string(),
    ];
    let mut cmd = Command::new(bin);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = cmd.spawn()?;
    let Some(stdin) = child.stdin.take() else {
        let _ = child.start_kill();
        return Err(std::io::Error::other("video persist paper missing stdin"));
    };
    let pid = child.id();
    let notify = if let Some(p) = pid {
        let n = Arc::new(Notify::new());
        ready_registry().lock().await.insert(p, n.clone());
        Some((p, n))
    } else {
        None
    };
    if let Some((p, n)) = notify {
        match await_paper_ready_or_exit(&mut child, p, n, PAPER_READY_TIMEOUT_MS).await {
            Ok(true) => {}
            Ok(false) => warn!(pid = p, output = %output, "video persist paper readiness timed out"),
            Err(error) => return Err(error),
        }
    }
    manager()
        .video
        .insert_killing_prev(output.to_string(), PersistPaper { child, stdin })
        .await;
    info!(pid = ?pid, output = %output, "video persist: spawned");
    Ok(pid)
}

pub async fn drop_persist_paper() {
    manager().persist.drop_all().await;
}

pub(super) async fn await_dead_papers(dead: Vec<Child>) {
    if dead.is_empty() {
        return;
    }
    let mut set = JoinSet::new();
    for mut c in dead {
        set.spawn(async move {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), c.wait()).await;
        });
    }
    while set.join_next().await.is_some() {}
}

pub(super) async fn drop_persist_papers_for(targets: &[String]) {
    manager().persist.drop_for(targets).await;
}

pub(super) async fn drop_video_persist_papers_for(targets: &[String]) {
    manager().video.drop_for(targets).await;
}

pub(super) async fn drop_steady_image_papers_for(targets: &[String]) {
    manager().steady.drop_for(targets).await;
}

pub(super) async fn prune_steady_image_papers(targets: &[String]) {
    let star_mode = targets.iter().any(|o| o == "*");
    let mut map = steady_image_papers().lock().await;
    if star_mode {
        let to_drop: Vec<String> = map.keys().filter(|k| *k != "*").cloned().collect();
        for k in to_drop {
            if let Some(mut p) = map.remove(&k) {
                let _ = p.child.start_kill();
            }
        }
    } else if let Some(mut p) = map.remove("*") {
        let _ = p.child.start_kill();
    }
}

pub(super) async fn try_send_persist(
    output: &str,
    to_path: &str,
    shader: &str,
    duration_ms: u64,
    thumbs: &[String],
    mute: Option<bool>,
    volume: Option<u32>,
) -> bool {
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let mut payload = serde_json::json!({
        "to": to_path,
        "shader": shader,
        "duration_ms": duration_ms,
        "thumbs": thumbs,
    });
    if let (Some(obj), Some(m)) = (payload.as_object_mut(), mute) {
        obj.insert("mute".into(), serde_json::json!(m));
    }
    if let (Some(obj), Some(v)) = (payload.as_object_mut(), volume) {
        obj.insert("volume".into(), serde_json::json!(v));
    }
    manager().persist.try_send_json(output, &payload).await
}

pub(super) async fn spawn_persist_paper(
    bin: &str,
    output: &str,
    from_path: &str,
    to_path: &str,
    shader: &str,
    duration_ms: u64,
    thumbs: &[String],
    fill_mode: crate::config::FillMode,
    mute: bool,
    volume: u32,
) -> std::io::Result<()> {
    let from_path = crate::wall::optimized::optimized_or(from_path);
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let mut args: Vec<String> = vec![
        "--transition-from".to_string(),
        from_path,
        "--shader".to_string(),
        shader.to_string(),
        "--duration-ms".to_string(),
        duration_ms.to_string(),
        "--persist".to_string(),
        "--fill-mode".to_string(),
        fill_mode.as_arg().to_string(),
        "--mute".to_string(),
        if mute { "true".to_string() } else { "false".to_string() },
        "--volume".to_string(),
        volume.to_string(),
    ];
    if !thumbs.is_empty() {
        args.push("--thumbs".to_string());
        args.push(thumbs.join(","));
    }
    args.push(output.to_string());
    args.push(to_path);

    let mut cmd = Command::new(bin);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = cmd.spawn()?;
    let Some(stdin) = child.stdin.take() else {
        let _ = child.start_kill();
        return Err(std::io::Error::other("persist paper missing stdin"));
    };
    let pid = child.id();
    let notify = if let Some(p) = pid {
        let n = Arc::new(Notify::new());
        ready_registry().lock().await.insert(p, n.clone());
        Some((p, n))
    } else {
        None
    };
    if let Some((p, n)) = notify {
        match await_paper_ready_or_exit(&mut child, p, n, PAPER_READY_TIMEOUT_MS).await {
            Ok(true) => {}
            Ok(false) => warn!(pid = p, output = %output, "persist paper readiness timed out"),
            Err(error) => return Err(error),
        }
    }
    manager()
        .persist
        .insert_killing_prev(output.to_string(), PersistPaper { child, stdin })
        .await;
    info!(pid = ?pid, output = %output, "persist: spawned");
    Ok(())
}

pub(super) fn preheat_inflight() -> &'static StdMutex<HashSet<String>> {
    &manager().preheat
}

pub fn preheat(path: &str) {
    if path.is_empty() {
        return;
    }
    let key = path.to_string();
    {
        let mut set = preheat_inflight().lock().unwrap();
        if !set.insert(key.clone()) {
            return;
        }
    }
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let bytes = tokio::fs::read(&key).await.map(|b| b.len()).unwrap_or(0);
        let dur_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(path = %key, bytes, dur_ms, "wall.preheat done");
        preheat_inflight().lock().unwrap().remove(&key);
    });
}

pub async fn signal_paper_ready(pid: u32) {
    let entry = {
        let mut reg = ready_registry().lock().await;
        reg.remove(&pid)
    };
    if let Some(notify) = entry {
        notify.notify_one();
    }
}

async fn await_paper_ready_or_exit(
    child: &mut Child,
    pid: u32,
    notify: Arc<Notify>,
    timeout_ms: u64,
) -> std::io::Result<bool> {
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), async {
        tokio::select! {
            _ = notify.notified() => Ok(()),
            status = child.wait() => {
                let status = status?;
                Err(std::io::Error::other(format!(
                    "paper process {pid} exited before readiness: {status}"
                )))
            }
        }
    })
    .await;
    ready_registry().lock().await.remove(&pid);

    match outcome {
        Ok(result) => result.map(|()| true),
        Err(_) => match child.try_wait()? {
            Some(status) => Err(std::io::Error::other(format!(
                "paper process {pid} exited before readiness: {status}"
            ))),
            None => Ok(false),
        },
    }
}

pub(super) async fn spawn_paper_await_ready(
    bin: &str,
    args: &[String],
    timeout_ms: u64,
) -> std::io::Result<Child> {
    let spawn_start = std::time::Instant::now();
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(paper_stderr());
    cmd.as_std_mut().process_group(0);
    cmd.kill_on_drop(false);
    let mut child = cmd.spawn()?;
    let Some(pid) = child.id() else {
        return Ok(child);
    };
    let notify = Arc::new(Notify::new());
    {
        let mut reg = ready_registry().lock().await;
        reg.insert(pid, notify.clone());
    }
    let ready = await_paper_ready_or_exit(&mut child, pid, notify, timeout_ms).await?;
    let dur_ms = spawn_start.elapsed().as_millis() as u64;
    if ready {
        info!(pid, dur_ms, "paper ready");
    } else {
        warn!(pid, dur_ms, timeout_ms, "paper readiness timed out");
    }
    Ok(child)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_outputs(dir: &Path, json: &str) {
        std::fs::write(dir.join("outputs.json"), json).unwrap();
    }

    #[tokio::test]
    async fn paper_exit_before_readiness_is_an_error() {
        let args = vec!["-c".to_string(), "exit 42".to_string()];
        let error = spawn_paper_await_ready("/bin/sh", &args, 1000)
            .await
            .expect_err("exited paper was accepted as ready");
        assert!(
            error.to_string().contains("42"),
            "exit status was missing from error: {error}"
        );
    }

    #[tokio::test]
    async fn prewarm_skips_divergent_static_desktop() {
        let dir = tempfile::tempdir().unwrap();
        let c = dir.path();

        write_outputs(c, r#"{"*":{"type":"static","path":"/a.png"}}"#);
        assert!(!outputs_divergent_static(c).await, "single * -> uniform");

        write_outputs(
            c,
            r#"{"DP-1":{"type":"static","path":"/a.png"},"DP-2":{"type":"static","path":"/a.png"}}"#,
        );
        assert!(!outputs_divergent_static(c).await, "same wallpaper per output -> uniform");

        write_outputs(
            c,
            r#"{"DP-1":{"type":"static","path":"/a.png"},"DP-2":{"type":"static","path":"/b.png"}}"#,
        );
        assert!(outputs_divergent_static(c).await, "different wallpapers -> divergent, skip prewarm");

        write_outputs(
            c,
            r#"{"DP-1":{"type":"static","path":"/a.png"},"DP-2":{"type":"video","path":"/v.mp4"}}"#,
        );
        assert!(outputs_divergent_static(c).await, "any non-static output -> skip prewarm");
    }

    #[tokio::test]
    async fn prewarm_allowed_when_outputs_state_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            !outputs_divergent_static(dir.path()).await,
            "no outputs.json -> not divergent (prewarm allowed, preserves single/uniform warmup)"
        );
    }
}
