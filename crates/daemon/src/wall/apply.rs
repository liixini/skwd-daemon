use std::collections::{HashMap, HashSet};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;

fn paper_stderr() -> Stdio {
    Stdio::inherit()
}
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex as AsyncMutex, Notify};

fn outputs_state_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

async fn write_outputs_state_atomic(state_path: &Path, contents: &str) {
    let tmp = state_path.with_extension("json.tmp");
    if tokio::fs::write(&tmp, contents).await.is_err() {
        return;
    }
    let _ = tokio::fs::rename(&tmp, state_path).await;
}
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{self, Config};
use crate::util::CommandExt;

#[derive(Default)]
struct PaperFleet {
    steady: Vec<Child>,
    transitions: Vec<Child>,
}

impl PaperFleet {
    fn replace_steady(&mut self, new_steady: Vec<Child>) {
        for mut c in self.steady.drain(..) {
            let _ = c.start_kill();
        }
        for mut c in self.transitions.drain(..) {
            let _ = c.start_kill();
        }
        self.steady = new_steady;
    }

}

fn fleet() -> &'static AsyncMutex<PaperFleet> {
    static FLEET: OnceLock<AsyncMutex<PaperFleet>> = OnceLock::new();
    FLEET.get_or_init(|| AsyncMutex::new(PaperFleet::default()))
}

fn apply_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

pub fn ready_registry() -> &'static AsyncMutex<HashMap<u32, Arc<Notify>>> {
    static REG: OnceLock<AsyncMutex<HashMap<u32, Arc<Notify>>>> = OnceLock::new();
    REG.get_or_init(|| AsyncMutex::new(HashMap::new()))
}

struct PersistPaper {
    child: Child,
    stdin: ChildStdin,
}

fn persist_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    static PERSIST: OnceLock<AsyncMutex<HashMap<String, PersistPaper>>> = OnceLock::new();
    PERSIST.get_or_init(|| AsyncMutex::new(HashMap::new()))
}

fn video_persist_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    static V: OnceLock<AsyncMutex<HashMap<String, PersistPaper>>> = OnceLock::new();
    V.get_or_init(|| AsyncMutex::new(HashMap::new()))
}

fn steady_image_papers() -> &'static AsyncMutex<HashMap<String, PersistPaper>> {
    static S: OnceLock<AsyncMutex<HashMap<String, PersistPaper>>> = OnceLock::new();
    S.get_or_init(|| AsyncMutex::new(HashMap::new()))
}

struct TransitionOverlayHandle {
    pid: u32,
    kill_tx: tokio::sync::oneshot::Sender<()>,
}

fn transition_overlays() -> &'static AsyncMutex<HashMap<String, TransitionOverlayHandle>> {
    static T: OnceLock<AsyncMutex<HashMap<String, TransitionOverlayHandle>>> = OnceLock::new();
    T.get_or_init(|| AsyncMutex::new(HashMap::new()))
}

async fn drop_transition_overlays_for(targets: &[String]) {
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
    let mut map = steady_image_papers().lock().await;
    for (_, mut p) in map.drain() {
        let _ = p.child.start_kill();
    }
}

async fn broadcast_warmup(warmup: bool) {
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

pub async fn on_wall_show(config: &Config) {
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
            .map(|t| t == "static")
            .unwrap_or(false),
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
    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            let _ = child.start_kill();
            return;
        }
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

async fn try_send_steady_image(output: &str, path: &str) -> bool {
    let mut map = steady_image_papers().lock().await;
    let Some(p) = map.get_mut(output) else { return false };
    if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
        map.remove(output);
        return false;
    }
    let cmd = serde_json::json!({"path": path});
    let line = format!("{}\n", cmd);
    if p.stdin.write_all(line.as_bytes()).await.is_err() {
        if let Some(mut paper) = map.remove(output) {
            let _ = paper.child.start_kill();
        }
        return false;
    }
    let _ = p.stdin.flush().await;
    true
}

async fn spawn_steady_image_paper(
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
    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            let _ = child.start_kill();
            return Err(std::io::Error::other("steady image paper missing stdin"));
        }
    };
    let pid = child.id();
    let notify = if let Some(p) = pid {
        let n = Arc::new(Notify::new());
        ready_registry().lock().await.insert(p, n.clone());
        Some((p, n))
    } else {
        None
    };
    {
        let mut map = steady_image_papers().lock().await;
        if let Some(mut prev) = map.insert(output.to_string(), PersistPaper { child, stdin }) {
            let _ = prev.child.start_kill();
        }
    }
    if let Some((p, n)) = notify {
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(2500),
            n.notified(),
        )
        .await;
        ready_registry().lock().await.remove(&p);
    }
    info!(pid = ?pid, output = %output, "steady image: spawned");
    Ok(())
}

async fn spawn_transition_overlay(
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
    let pid = match child.id() {
        Some(p) => p,
        None => return Err(std::io::Error::other("transition overlay: missing pid")),
    };
    let notify = Arc::new(Notify::new());
    ready_registry().lock().await.insert(pid, notify.clone());
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
    let _ = tokio::time::timeout(std::time::Duration::from_millis(2500), notify.notified()).await;
    ready_registry().lock().await.remove(&pid);
    info!(pid, output = %output, "transition overlay: spawned");
    Ok(())
}

async fn ensure_steady_image_paper(
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
    let mut map = video_persist_papers().lock().await;
    for (_, mut p) in map.drain() {
        let _ = p.child.start_kill();
    }
}

async fn video_persist_alive_outputs(target_outs: &[String]) -> HashSet<String> {
    let mut map = video_persist_papers().lock().await;
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

async fn persist_alive_outputs(target_outs: &[String]) -> HashSet<String> {
    let mut map = persist_papers().lock().await;
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

async fn video_persist_pid_for(output: &str) -> Option<u32> {
    let mut map = video_persist_papers().lock().await;
    let p = map.get_mut(output)?;
    if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
        map.remove(output);
        return None;
    }
    p.child.id()
}

async fn try_send_video_persist(output: &str, to_path: &str, mute: bool) -> bool {
    let mut map = video_persist_papers().lock().await;
    let Some(p) = map.get_mut(output) else { return false };
    if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
        map.remove(output);
        return false;
    }
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let cmd = serde_json::json!({"path": to_path, "mute": mute});
    let line = format!("{}\n", cmd);
    if let Err(e) = p.stdin.write_all(line.as_bytes()).await {
        warn!(output = %output, error = %e, "video persist: stdin write failed");
        if let Some(mut paper) = map.remove(output) {
            let _ = paper.child.start_kill();
        }
        return false;
    }
    if let Err(e) = p.stdin.flush().await {
        warn!(output = %output, error = %e, "video persist: stdin flush failed");
        if let Some(mut paper) = map.remove(output) {
            let _ = paper.child.start_kill();
        }
        return false;
    }
    true
}

async fn spawn_video_persist_paper(
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
    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            let _ = child.start_kill();
            return Err(std::io::Error::other("video persist paper missing stdin"));
        }
    };
    let pid = child.id();
    let mut map = video_persist_papers().lock().await;
    if let Some(mut prev) = map.insert(output.to_string(), PersistPaper { child, stdin }) {
        let _ = prev.child.start_kill();
    }
    info!(pid = ?pid, output = %output, "video persist: spawned");
    Ok(pid)
}

pub async fn drop_persist_paper() {
    let mut map = persist_papers().lock().await;
    for (_, mut p) in map.drain() {
        let _ = p.child.start_kill();
    }
}

async fn await_dead_papers(dead: Vec<Child>) {
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

async fn drop_persist_papers_for(targets: &[String]) {
    let dead: Vec<Child> = {
        let mut map = persist_papers().lock().await;
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

async fn drop_video_persist_papers_for(targets: &[String]) {
    let dead: Vec<Child> = {
        let mut map = video_persist_papers().lock().await;
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

async fn drop_steady_image_papers_for(targets: &[String]) {
    let dead: Vec<Child> = {
        let mut map = steady_image_papers().lock().await;
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

async fn prune_steady_image_papers(targets: &[String]) {
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

async fn try_send_persist(
    output: &str,
    to_path: &str,
    shader: &str,
    duration_ms: u64,
    thumbs: &[String],
    mute: Option<bool>,
    volume: Option<u32>,
) -> bool {
    let mut map = persist_papers().lock().await;
    let Some(p) = map.get_mut(output) else {
        info!(output = %output, to = %to_path, "try_send_persist: no paper in map");
        return false;
    };
    if matches!(p.child.try_wait(), Ok(Some(_)) | Err(_)) {
        info!(output = %output, to = %to_path, "try_send_persist: paper already exited");
        map.remove(output);
        return false;
    }
    let to_path = crate::wall::optimized::optimized_or(to_path);
    let mut cmd = serde_json::json!({
        "to": to_path,
        "shader": shader,
        "duration_ms": duration_ms,
        "thumbs": thumbs,
    });
    if let (Some(obj), Some(m)) = (cmd.as_object_mut(), mute) {
        obj.insert("mute".into(), serde_json::json!(m));
    }
    if let (Some(obj), Some(v)) = (cmd.as_object_mut(), volume) {
        obj.insert("volume".into(), serde_json::json!(v));
    }
    let line = format!("{}\n", cmd);
    info!(output = %output, to = %to_path, "try_send_persist: writing");
    if let Err(e) = p.stdin.write_all(line.as_bytes()).await {
        warn!(output = %output, error = %e, "persist: stdin write failed, dropping paper");
        if let Some(mut paper) = map.remove(output) {
            let _ = paper.child.start_kill();
        }
        return false;
    }
    if let Err(e) = p.stdin.flush().await {
        warn!(output = %output, error = %e, "persist: stdin flush failed, dropping paper");
        if let Some(mut paper) = map.remove(output) {
            let _ = paper.child.start_kill();
        }
        return false;
    }
    true
}

async fn spawn_persist_paper(
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
    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => {
            let _ = child.start_kill();
            return Err(std::io::Error::other("persist paper missing stdin"));
        }
    };
    let pid = child.id();
    let notify = if let Some(p) = pid {
        let n = Arc::new(Notify::new());
        ready_registry().lock().await.insert(p, n.clone());
        Some((p, n))
    } else {
        None
    };
    {
        let mut map = persist_papers().lock().await;
        if let Some(mut prev) = map.insert(output.to_string(), PersistPaper { child, stdin }) {
            let _ = prev.child.start_kill();
        }
    }
    if let Some((p, n)) = notify {
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(2500),
            n.notified(),
        )
        .await;
        ready_registry().lock().await.remove(&p);
    }
    info!(pid = ?pid, output = %output, "persist: spawned");
    Ok(())
}

fn preheat_inflight() -> &'static StdMutex<HashSet<String>> {
    static SET: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();
    SET.get_or_init(|| StdMutex::new(HashSet::new()))
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

async fn spawn_paper_await_ready(
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
    let child = cmd.spawn()?;
    let pid = match child.id() {
        Some(p) => p,
        None => return Ok(child),
    };
    let notify = Arc::new(Notify::new());
    {
        let mut reg = ready_registry().lock().await;
        reg.insert(pid, notify.clone());
    }
    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        notify.notified(),
    )
    .await
    .is_err();
    let mut reg = ready_registry().lock().await;
    reg.remove(&pid);
    let dur_ms = spawn_start.elapsed().as_millis() as u64;
    if timed_out {
        warn!(pid, dur_ms, timeout_ms, "paper readiness timed out");
    } else {
        info!(pid, dur_ms, "paper ready");
    }
    Ok(child)
}

pub async fn apply_static(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    all_screens: &[String],
    config: &Config,
) -> anyhow::Result<()> {
    apply_static_inner(path, outputs, neighbors, all_screens, config, false).await
}

async fn apply_static_inner(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
    all_screens: &[String],
    config: &Config,
    restoring: bool,
) -> anyhow::Result<()> {
    let apply_total = std::time::Instant::now();
    let lock_wait = std::time::Instant::now();
    let _apply_guard = apply_lock().lock().await;
    let lock_wait_ms = lock_wait.elapsed().as_millis() as u64;
    let is_kde = is_kde();
    let prev_image = read_prev_transition_image(&config.cache_dir()).await;
    let prev_was_we = linux_we_running().await;

    if !prev_was_we {
        kill_legacy_video_procs().await;
    }

    let matugen_handle = {
        let path = path.to_string();
        let config = config.clone();
        tokio::spawn(async move {
            run_matugen(&path, &config).await;
            run_reloads(&config).await;
        })
    };

    let prev_engine = swap_last_engine(config.paper.engine).await;
    if prev_engine == Some(config::PaperEngine::Awww)
        && config.paper.engine != config::PaperEngine::Awww
    {
        kill_awww_if_running().await;
    }

    if config.wants_external_render() {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        let _ = tokio::fs::remove_file(config.video_dir().join("lockscreen-video.mp4")).await;
        run_external_apply(config, "static", path, path).await;
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
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_kde_static(path, outputs, config).await?;
    } else if config.paper.engine == config::PaperEngine::Awww {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_transition_overlays_for(&["*".to_string()]).await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_awww(path, outputs, config).await?;
    } else {
        let still_bin = paper_still_bin();
        let bin = paper_bin();
        let target_outs: Vec<String> = if outputs.is_empty() {
            vec!["*".to_string()]
        } else {
            outputs.to_vec()
        };
        let target_is_wildcard = target_outs.iter().any(|o| o == "*");

        let pre_state = read_outputs_state(&config.cache_dir()).await;
        let prev_wildcard_static: Option<String> = if !target_is_wildcard {
            pre_state
                .get("*")
                .and_then(|v| v.as_object())
                .filter(|m| {
                    m.get("type").and_then(|t| t.as_str()) == Some("static")
                })
                .and_then(|m| m.get("path").and_then(|p| p.as_str()).map(String::from))
        } else {
            None
        };

        rebuild_scene_we(
            config,
            &target_outs,
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await
        .ok();
        drop_transition_overlays_for(&target_outs).await;

        if let Some(prev_path) = prev_wildcard_static.as_deref()
            && !all_screens.is_empty()
        {
            let target_set: std::collections::HashSet<&str> =
                target_outs.iter().map(String::as_str).collect();
            let carry_over: Vec<String> = all_screens
                .iter()
                .filter(|s| !target_set.contains(s.as_str()))
                .cloned()
                .collect();
            if !carry_over.is_empty() {
                info!(
                    carry_over = ?carry_over,
                    prev = %prev_path,
                    "static apply: carrying wildcard image to un-targeted monitors"
                );
                for out in &carry_over {
                    if let Err(e) = spawn_steady_image_paper(
                        &still_bin,
                        out,
                        prev_path,
                        config.display.fill_mode,
                    )
                    .await
                    {
                        warn!(error = %e, output = %out, "carry-over steady spawn failed");
                    }
                }
            }
        }

        let prev_for_transition = if restoring || !config.transition.enabled {
            None
        } else {
            prev_image
                .as_deref()
                .filter(|p| *p != path && Path::new(p).exists())
                .map(|p| p.to_string())
        };

        if let Some(prev) = prev_for_transition.as_deref() {
            let thumbs =
                pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, path], 20).await;
            let shader = config.transition.shader.clone();
            let dur_ms = config.transition.duration_ms;
            for out in &target_outs {
                if let Err(e) = spawn_transition_overlay(
                    &bin,
                    out,
                    prev,
                    path,
                    &shader,
                    dur_ms,
                    &thumbs,
                    config.display.fill_mode,
                )
                .await
                {
                    warn!(error = %e, output = %out, "transition overlay spawn failed");
                }
            }
        }

        ensure_steady_image_paper(&still_bin, &target_outs, path, config.display.fill_mode).await;
        drop_video_persist_papers_for(&target_outs).await;
        drop_persist_papers_for(&target_outs).await;
        let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
        for mut c in prev_steady {
            let _ = c.start_kill();
        }

        if prev_was_we {
            kill_legacy_video_procs().await;
        }
        info!("apply_static: skwd-paper-still + bottom-layer transition");
    }

    let current_dir = config.cache_dir().join("wallpaper");
    let _ = tokio::fs::create_dir_all(&current_dir).await;
    let wall_jpg = current_dir.join("current.jpg");
    let _ = tokio::fs::copy(path, &wall_jpg).await;

    save_state(&config.cache_dir(), "static", path, "").await;
    let prev_outputs_state = read_outputs_state(&config.cache_dir()).await;
    save_outputs_state(&config.cache_dir(), outputs, "static", path, "", &HashMap::new()).await;
    if !outputs.is_empty() && !outputs.iter().any(|o| o == "*") {
        mute_wildcard_if_present(config).await;
    }
    preserve_group_audio(config, &prev_outputs_state).await;
    enforce_audio_dedup(config).await;

    let path = path.to_string();
    let config = config.clone();
    tokio::spawn(async move {
        let _ = matugen_handle.await;
        run_post_processing(&config, "static", &basename(&path), &path, &path, restoring).await;
        info!("post-apply tasks done for static: {path}");
    });

    info!(
        total_ms = apply_total.elapsed().as_millis() as u64,
        lock_wait_ms,
        "applied static wallpaper"
    );
    Ok(())
}

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

async fn apply_video_inner(
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
                parts.push(format!("volume={}", vol));
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
                .map(|p| p.to_string())
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

            let video_pids: Vec<u32> = steady.iter().filter_map(|c| c.id()).collect();

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

pub async fn apply_we(
    we_id: &str,
    screens: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
    apply_we_inner(we_id, screens, outputs_audio, outputs_volume, config, false).await
}

async fn apply_we_inner(
    we_id: &str,
    screens: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
    restoring: bool,
) -> anyhow::Result<()> {
    let _apply_guard = apply_lock().lock().await;
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
        .find(|s| !dedup_mute.get(*s).copied().unwrap_or(global_mute))
        .map(|s| volume_for(s))
        .unwrap_or_else(|| config.volume());

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

pub async fn restore(config: &Config) -> anyhow::Result<String> {
    let outputs_state = read_outputs_state(&config.cache_dir()).await;
    let map = outputs_state
        .as_object()
        .cloned()
        .unwrap_or_default();

    if !map.is_empty() {
        let mut groups: std::collections::HashMap<(String, String, String), Vec<String>> =
            std::collections::HashMap::new();
        let mut audio_by_output: HashMap<String, bool> = HashMap::new();
        let mut volume_by_output: HashMap<String, u32> = HashMap::new();
        for (output, entry) in &map {
            let wp_type = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let path = entry
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let we_id = entry
                .get("we_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(m) = entry.get("mute").and_then(|v| v.as_bool()) {
                audio_by_output.insert(output.clone(), m);
            }
            if let Some(v) = entry.get("volume").and_then(|v| v.as_u64()) {
                volume_by_output.insert(output.clone(), v.min(100) as u32);
            }
            groups
                .entry((wp_type, path, we_id))
                .or_default()
                .push(output.clone());
        }

        let mut last_id = String::new();
        for ((wp_type, path, we_id), outs) in groups {
            let outputs_arg: Vec<String> = if outs.iter().any(|o| o == "*") {
                Vec::new()
            } else {
                outs.clone()
            };
            let audio_arg: HashMap<String, bool> = outs
                .iter()
                .filter_map(|o| audio_by_output.get(o).map(|&v| (o.clone(), v)))
                .collect();
            let volume_arg: HashMap<String, u32> = outs
                .iter()
                .filter_map(|o| volume_by_output.get(o).map(|&v| (o.clone(), v)))
                .collect();
            last_id = match wp_type.as_str() {
                "static" if !path.is_empty() => {
                    apply_static_inner(&path, &outputs_arg, &[], &[], config, true).await?;
                    path
                }
                "video" if !path.is_empty() => {
                    apply_video_inner(&path, &outputs_arg, &[], &audio_arg, &volume_arg, config, true).await?;
                    path
                }
                "we" if !we_id.is_empty() => {
                    apply_we_inner(&we_id, &outputs_arg, &audio_arg, &volume_arg, config, true).await?;
                    we_id
                }
                _ => continue,
            };
        }
        if !last_id.is_empty() {
            return Ok(last_id);
        }
    }

    let state_path = config.cache_dir().join("last-wallpaper.json");
    if !state_path.exists() {
        anyhow::bail!("no saved state");
    }
    let text = tokio::fs::read_to_string(&state_path).await?;
    let state: serde_json::Value = serde_json::from_str(&text)?;

    let wp_type = state.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match wp_type {
        "static" => {
            let path = state.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                anyhow::bail!("no path in state");
            }
            apply_static_inner(path, &[], &[], &[], config, true).await?;
            Ok(path.to_string())
        }
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                anyhow::bail!("no path in state");
            }
            apply_video_inner(path, &[], &[], &HashMap::new(), &HashMap::new(), config, true).await?;
            Ok(path.to_string())
        }
        "we" => {
            let we_id = state.get("we_id").and_then(|v| v.as_str()).unwrap_or("");
            if we_id.is_empty() {
                anyhow::bail!("no we_id in state");
            }
            apply_we_inner(we_id, &[], &HashMap::new(), &HashMap::new(), config, true).await?;
            Ok(we_id.to_string())
        }
        _ => anyhow::bail!("unknown wallpaper type: {wp_type}"),
    }
}

pub async fn reapply_statics_for_engine_change(config: &Config) -> anyhow::Result<()> {
    let outputs_state = read_outputs_state(&config.cache_dir()).await;
    let map = outputs_state.as_object().cloned().unwrap_or_default();
    if map.is_empty() {
        return Ok(());
    }

    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for (output, entry) in &map {
        let wp_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let path = entry.get("path").and_then(|v| v.as_str()).unwrap_or("");
        if wp_type != "static" || path.is_empty() {
            continue;
        }
        groups.entry(path.to_string()).or_default().push(output.clone());
    }

    for (path, outs) in groups {
        let outputs_arg: Vec<String> = if outs.iter().any(|o| o == "*") {
            Vec::new()
        } else {
            outs
        };
        if let Err(e) = apply_static_inner(&path, &outputs_arg, &[], &[], config, true).await {
            warn!("engine-change re-apply failed for {path}: {e}");
        }
    }
    Ok(())
}

pub async fn retheme(
    config: &Config,
    scheme: Option<&str>,
    mode: Option<&str>,
    color_index: Option<u32>,
) -> anyhow::Result<()> {
    let current_jpg = config.cache_dir().join("wallpaper/current.jpg");
    if !current_jpg.exists() {
        anyhow::bail!("no current wallpaper image to retheme");
    }
    let image_path = current_jpg.display().to_string();
    run_matugen_with(&image_path, config, scheme, mode, color_index).await;
    run_reloads(config).await;
    info!("retheme completed for {image_path}");
    Ok(())
}

pub async fn theme_preview(
    config: &Config,
    scheme: &str,
    mode: &str,
    color_index: u32,
) -> anyhow::Result<serde_json::Value> {
    let current_jpg = config.cache_dir().join("wallpaper/current.jpg");
    if !current_jpg.exists() {
        anyhow::bail!("no current wallpaper image for preview");
    }
    let out = Command::new("matugen")
        .arg("--dry-run")
        .arg("-j")
        .arg("hex")
        .arg("image")
        .arg("-t")
        .arg(scheme)
        .arg("-m")
        .arg(mode)
        .arg("--source-color-index")
        .arg(color_index.to_string())
        .arg(current_jpg.display().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !out.status.success() {
        let err_msg = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!(
            "matugen preview failed: status {} stderr={}",
            out.status,
            err_msg.trim()
        );
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    let pick = |key: &str| -> String {
        json.get("colors")
            .and_then(|c| c.get(key))
            .and_then(|e| e.get("default"))
            .and_then(|d| d.get("color"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    Ok(serde_json::json!({
        "scheme": scheme,
        "mode": mode,
        "color_index": color_index,
        "primary": pick("primary"),
        "secondary": pick("secondary"),
        "tertiary": pick("tertiary"),
        "surface": pick("surface"),
        "background": pick("background"),
        "on_surface": pick("on_surface"),
    }))
}


async fn generate_matugen_config(config: &Config) -> PathBuf {
    let config_path = config.matugen_config_path();
    let template_dir = config.template_dir();
    let cache_dir = config.cache_dir();

    let mut lines = vec!["[config]".to_string(), "reload_apps = false".to_string(), String::new()];
    let mut emitted = 0usize;

    for (i, integ) in config.integrations.iter().enumerate() {
        let template = match &integ.template {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };
        let output = match &integ.output {
            Some(o) if !o.is_empty() => o,
            _ => continue,
        };

        let input_path = if template.contains('/') {
            config::resolve_tilde(template)
        } else {
            template_dir.join(template)
        };

        if !input_path.exists() {
            warn!(
                "matugen integration '{}' template not found at {}, skipping",
                integ.name.as_deref().unwrap_or("(unnamed)"),
                input_path.display()
            );
            continue;
        }

        let output_path = if output.contains('/') {
            config::resolve_tilde(output)
        } else {
            cache_dir.join(output)
        };

        let safe_name = integ
            .name
            .as_deref()
            .unwrap_or(&format!("integration_{i}"))
            .replace(|c: char| !c.is_alphanumeric() && c != '_' && c != '-', "_");

        lines.push(format!("[templates.{safe_name}]"));
        lines.push(format!("input_path = \"{}\"", input_path.display()));
        lines.push(format!("output_path = \"{}\"", output_path.display()));
        lines.push(String::new());
        emitted += 1;
    }

    let _ = tokio::fs::create_dir_all(config_path.parent().unwrap_or_else(|| Path::new("/tmp"))).await;
    let _ = tokio::fs::write(&config_path, lines.join("\n")).await;
    info!("generated matugen config with {emitted} integrations");
    config_path
}

async fn run_matugen(image_path: &str, config: &Config) {
    if !config.features.matugen {
        return;
    }

    if Command::new("command")
        .arg("-v")
        .arg("matugen")
        .silent()
        .status()
        .await
        .map(|s| !s.success())
        .unwrap_or(true)
        && Command::new("which")
            .arg("matugen")
            .silent()
            .status()
            .await
            .map(|s| !s.success())
            .unwrap_or(true)
    {
        warn!("matugen not found in PATH, skipping");
        return;
    }

    let config_path = generate_matugen_config(config).await;
    let scheme = config.matugen_scheme();
    let mode = config.matugen_mode();
    let color_index = config.matugen_color_index();
    run_matugen_inner(image_path, config, &config_path, scheme, mode, color_index).await;
}

async fn run_matugen_with(
    image_path: &str,
    config: &Config,
    scheme: Option<&str>,
    mode: Option<&str>,
    color_index: Option<u32>,
) {
    if !config.features.matugen {
        return;
    }
    let config_path = generate_matugen_config(config).await;
    let scheme = scheme.unwrap_or_else(|| config.matugen_scheme());
    let mode = mode.unwrap_or_else(|| config.matugen_mode());
    let color_index = color_index.unwrap_or_else(|| config.matugen_color_index());
    run_matugen_inner(image_path, config, &config_path, scheme, mode, color_index).await;
}

async fn run_matugen_inner(
    image_path: &str,
    config: &Config,
    config_path: &Path,
    scheme: &str,
    mode: &str,
    color_index: u32,
) {
    let status = Command::new("matugen")
        .arg("-c")
        .arg(&config_path)
        .arg("image")
        .arg("-t")
        .arg(scheme)
        .arg("-m")
        .arg(mode)
        .arg("--source-color-index")
        .arg(color_index.to_string())
        .arg(image_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await;

    match status {
        Ok(s) if s.success() => info!("matugen completed for {image_path}"),
        Ok(s) => warn!("matugen exited with {s} for {image_path}"),
        Err(e) => warn!("failed to run matugen: {e}"),
    }

    if let Some(default_cfg) = config.default_matugen_config_path()
        && default_cfg.exists()
    {
        let default_cmd = "matugen -c %config% image %path%".to_string();
        let cmd_template = config.external_matugen_command.as_deref().unwrap_or(&default_cmd);
        let cmd = cmd_template
            .replace("%config%", &shell_quote(&default_cfg.display().to_string()))
            .replace("%path%", &shell_quote(image_path));
        info!("running external matugen: {cmd}");
        if let Err(e) = run_sh(&cmd).await {
            warn!("failed to run external matugen: {e}");
        }
    }
}

async fn run_reloads(config: &Config) {
    for integ in &config.integrations {
        let reload = match &integ.reload {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };

        let resolved = config::resolve_tilde(reload);
        let cmd = if resolved.to_str().is_some_and(|s| s.contains('/') && !s.contains(' ')) {
            format!("sh {}", shell_quote(&resolved.display().to_string()))
        } else {
            reload.clone()
        };

        info!("running reload: {cmd}");
        let _ = run_sh(&cmd).await;
    }

    if config.general.notify_on_wallpaper_change {
        let _ = run_sh("command -v notify-send >/dev/null && notify-send 'Wallpaper Changed' || true").await;
    }
}

async fn find_we_preview(item_dir: &Path) -> Option<PathBuf> {
    let mut entries = tokio::fs::read_dir(item_dir).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_lowercase();
        if name.starts_with("preview.") {
            return Some(entry.path());
        }
    }
    None
}


pub async fn set_audio_for(
    config: &Config,
    mute: Option<bool>,
    volume: Option<u32>,
    outputs: Option<Vec<String>>,
) {
    let cache_dir = config.cache_dir();
    let mut payload = serde_json::Map::new();
    payload.insert("to".into(), serde_json::json!(""));
    if let Some(m) = mute {
        payload.insert("mute".into(), serde_json::json!(m));
    }
    if let Some(v) = volume {
        payload.insert("volume".into(), serde_json::json!(v));
    }
    let line = format!("{}\n", serde_json::Value::Object(payload));

    let filter: Option<HashSet<String>> = outputs.map(|v| v.into_iter().collect());

    {
        let mut map = persist_papers().lock().await;
        let keys: Vec<String> = map.keys().cloned().collect();
        for out in keys {
            if let Some(ref f) = filter
                && !f.contains(&out)
            {
                continue;
            }
            if let Some(p) = map.get_mut(&out)
                && p.stdin.write_all(line.as_bytes()).await.is_err()
            {
                tracing::warn!(output = %out, "set_audio: persist write failed");
            }
            if let Some(p) = map.get_mut(&out) {
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map = video_persist_papers().lock().await;
        let keys: Vec<String> = map.keys().cloned().collect();
        for out in keys {
            if let Some(ref f) = filter
                && !f.contains(&out)
            {
                continue;
            }
            if let Some(p) = map.get_mut(&out)
                && p.stdin.write_all(line.as_bytes()).await.is_err()
            {
                tracing::warn!(output = %out, "set_audio: video persist write failed");
            }
            if let Some(p) = map.get_mut(&out) {
                let _ = p.stdin.flush().await;
            }
        }
    }

    if mute.is_some() || volume.is_some() {
        update_outputs_state_audio(&cache_dir, &filter, mute, volume).await;

        let we_affected = we_outputs_in_filter(&cache_dir, &filter).await;
        if we_affected {
            let global_volume = volume.unwrap_or_else(|| config.volume());
            let _ = rebuild_scene_we(
                config,
                &[],
                &std::collections::BTreeMap::new(),
                global_volume,
            )
            .await;
        }
    }

    if is_kde() {
        // Translate the (mute, volume, filter) into per-output maps the same
        // way the non-KDE stdin path implicitly does, then push them into
        // plasma containments.
        let state = read_outputs_state(&cache_dir).await;
        let mut mute_map: HashMap<String, bool> = HashMap::new();
        let mut volume_map: HashMap<String, u32> = HashMap::new();
        if let Some(obj) = state.as_object() {
            for out in obj.keys() {
                if let Some(ref f) = filter {
                    if !f.contains(out) {
                        continue;
                    }
                }
                if let Some(m) = mute {
                    mute_map.insert(out.clone(), m);
                }
                if let Some(v) = volume {
                    volume_map.insert(out.clone(), v);
                }
            }
        }
        kde_apply_audio(config, &mute_map, &volume_map).await;
    }

    enforce_audio_dedup(config).await;
}

async fn preserve_group_audio(config: &Config, prev_state: &serde_json::Value) {
    let mut prev_audible: HashSet<(String, String, String)> = HashSet::new();
    if let Some(obj) = prev_state.as_object() {
        for (_, entry) in obj {
            let muted = entry
                .get("mute")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if muted {
                continue;
            }
            let t = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if t != "video" && t != "we" {
                continue;
            }
            let path = entry
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let we_id = entry
                .get("we_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            prev_audible.insert((t, path, we_id));
        }
    }

    if prev_audible.is_empty() {
        return;
    }

    let cache_dir = config.cache_dir();
    let current = read_outputs_state(&cache_dir).await;
    let map = match current.as_object() {
        Some(m) => m.clone(),
        None => return,
    };

    let mut current_groups: HashMap<(String, String, String), Vec<(String, bool)>> = HashMap::new();
    for (out, entry) in &map {
        let t = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if t != "video" && t != "we" {
            continue;
        }
        let path = entry
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let we_id = entry
            .get("we_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let muted = entry
            .get("mute")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        current_groups
            .entry((t, path, we_id))
            .or_default()
            .push((out.clone(), muted));
    }

    let mut to_unmute: Vec<String> = Vec::new();
    for (key, mut outputs) in current_groups {
        if !prev_audible.contains(&key) {
            continue;
        }
        if outputs.iter().any(|(_, m)| !m) {
            continue;
        }
        outputs.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some((out, _)) = outputs.first() {
            to_unmute.push(out.clone());
        }
    }

    if to_unmute.is_empty() {
        return;
    }

    let payload = serde_json::json!({"to": "", "mute": false});
    let line = format!("{}\n", payload);
    let unmute_set: HashSet<String> = to_unmute.iter().cloned().collect();

    {
        let mut map_p = persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !unmute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map_p = video_persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !unmute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }

    let filter = Some(unmute_set.clone());
    update_outputs_state_audio(&cache_dir, &filter, Some(false), None).await;

    let any_we = to_unmute.iter().any(|out| {
        map.get(out)
            .and_then(|e| e.get("type"))
            .and_then(|v| v.as_str())
            == Some("we")
    });
    if any_we {
        let _ = rebuild_scene_we(
            config,
            &[],
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await;
    }
}

pub async fn enforce_audio_dedup(config: &Config) {
    let cache_dir = config.cache_dir();
    let state = read_outputs_state(&cache_dir).await;
    let map = match state.as_object() {
        Some(m) => m.clone(),
        None => return,
    };

    let mut groups: HashMap<(String, String, String), Vec<String>> = HashMap::new();
    for (out, entry) in &map {
        let t = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if t != "video" && t != "we" {
            continue;
        }
        let path = entry
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let we_id = entry
            .get("we_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        groups
            .entry((t, path, we_id))
            .or_default()
            .push(out.clone());
    }

    let mut to_mute: Vec<String> = Vec::new();
    for (_, mut outputs) in groups {
        outputs.sort();
        let mut found_primary = false;
        for out in &outputs {
            let muted = map
                .get(out)
                .and_then(|e| e.get("mute"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if !muted {
                if !found_primary {
                    found_primary = true;
                } else {
                    to_mute.push(out.clone());
                }
            }
        }
    }

    if to_mute.is_empty() {
        return;
    }

    let payload = serde_json::json!({"to": "", "mute": true});
    let line = format!("{}\n", payload);
    let mute_set: HashSet<String> = to_mute.iter().cloned().collect();

    {
        let mut map_p = persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !mute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }
    {
        let mut map_p = video_persist_papers().lock().await;
        let keys: Vec<String> = map_p.keys().cloned().collect();
        for out in keys {
            if !mute_set.contains(&out) {
                continue;
            }
            if let Some(p) = map_p.get_mut(&out) {
                let _ = p.stdin.write_all(line.as_bytes()).await;
                let _ = p.stdin.flush().await;
            }
        }
    }

    let filter = Some(mute_set.clone());
    update_outputs_state_audio(&cache_dir, &filter, Some(true), None).await;

    if is_kde() {
        let mute_map: HashMap<String, bool> =
            mute_set.iter().map(|o| (o.clone(), true)).collect();
        kde_apply_audio(config, &mute_map, &HashMap::new()).await;
    }

    let any_we = to_mute.iter().any(|out| {
        map.get(out)
            .and_then(|e| e.get("type"))
            .and_then(|v| v.as_str())
            == Some("we")
    });
    if any_we {
        let _ = rebuild_scene_we(
            config,
            &[],
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await;
    }
}

async fn mute_wildcard_if_present(config: &Config) {
    let state = read_outputs_state(&config.cache_dir()).await;
    let has_star = state
        .as_object()
        .map(|m| m.contains_key("*"))
        .unwrap_or(false);
    if !has_star {
        return;
    }
    set_audio_for(config, Some(true), None, Some(vec!["*".to_string()])).await;
}

async fn we_outputs_in_filter(cache_dir: &Path, filter: &Option<HashSet<String>>) -> bool {
    let existing = read_outputs_state(cache_dir).await;
    let Some(obj) = existing.as_object() else {
        return false;
    };
    for (out, entry) in obj {
        if let Some(f) = filter.as_ref()
            && !f.contains(out)
        {
            continue;
        }
        if entry.get("type").and_then(|v| v.as_str()) == Some("we") {
            return true;
        }
    }
    false
}

async fn update_outputs_state_audio(
    cache_dir: &Path,
    filter: &Option<HashSet<String>>,
    mute: Option<bool>,
    volume: Option<u32>,
) {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    let existing: serde_json::Value = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    let mut map = match existing {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };

    let keys: Vec<String> = map.keys().cloned().collect();
    for k in keys {
        if let Some(f) = filter.as_ref()
            && !f.contains(&k)
        {
            continue;
        }
        if let Some(entry) = map.get_mut(&k) {
            if let Some(m) = mute {
                entry["mute"] = serde_json::json!(m);
            }
            if let Some(v) = volume {
                entry["volume"] = serde_json::json!(v);
            }
        }
    }

    let contents =
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
    write_outputs_state_atomic(&state_path, &contents).await;
}

async fn linux_we_running() -> bool {
    Command::new("pgrep")
        .arg("-x")
        .arg("linux-wallpaperengine")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn kill_legacy_video_procs() {
    let _ = run_sh(
        "pkill -9 mpvpaper 2>/dev/null; \
         true",
    )
    .await;
}

async fn kill_legacy_other_procs() {
    let _ = run_sh(
        "pkill -9 mpvpaper 2>/dev/null; \
         pkill awww 2>/dev/null; \
         pkill awww-daemon 2>/dev/null; \
         true",
    )
    .await;
}

async fn kill_linux_we_proc() {
    let _ = run_sh("pkill -9 -f '[l]inux-wallpaperengine' 2>/dev/null; true").await;
}

async fn rebuild_scene_we(
    config: &Config,
    excluded: &[String],
    additions: &std::collections::BTreeMap<String, (String, bool)>,
    global_volume: u32,
) -> anyhow::Result<()> {
    let we_dir = config.we_dir();
    let existing = read_outputs_state(&config.cache_dir()).await;
    let exclude_all = excluded.iter().any(|o| o == "*");
    let exclude_set: HashSet<&str> = excluded.iter().map(|s| s.as_str()).collect();

    let mut merged = additions.clone();
    if !exclude_all && let Some(obj) = existing.as_object() {
        for (out, entry) in obj {
            if out == "*" || exclude_set.contains(out.as_str()) || merged.contains_key(out) {
                continue;
            }
            let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if entry_type != "we" {
                continue;
            }
            let entry_we_id = entry
                .get("we_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if entry_we_id.is_empty() {
                continue;
            }
            let (entry_we_type, _) = read_we_project_type(&we_dir.join(&entry_we_id)).await;
            if entry_we_type != "scene" {
                continue;
            }
            let entry_mute = entry.get("mute").and_then(|v| v.as_bool()).unwrap_or(true);
            merged.insert(out.clone(), (entry_we_id, entry_mute));
        }
    }

    kill_linux_we_proc().await;

    if merged.is_empty() {
        return Ok(());
    }

    let any_unmuted = merged.values().any(|(_, mute)| !*mute);
    let audio_flag = if any_unmuted {
        let scaled = (global_volume as f32 * 1.28).round() as u32;
        format!("--volume {}", scaled.min(128))
    } else {
        "--silent".to_string()
    };

    let mut screen_args = String::new();
    for (out, (id, _)) in &merged {
        screen_args.push_str(&format!(
            " --screen-root {} --bg {} --scaling fill",
            shell_quote(out),
            shell_quote(id),
        ));
    }

    let assets_arg = config
        .we_assets_dir()
        .map(|d| format!("--assets-dir {}", shell_quote(&d.display().to_string())))
        .unwrap_or_default();

    run_sh(&format!(
        "nohup setsid linux-wallpaperengine {} --no-fullscreen-pause --noautomute{} \
         --clamp border {} </dev/null >/dev/null 2>&1 &",
        audio_flag, screen_args, assets_arg
    ))
    .await?;
    Ok(())
}

async fn read_we_project_type(item_dir: &Path) -> (String, String) {
    let project_path = item_dir.join("project.json");
    if !project_path.exists() {
        return ("scene".to_string(), String::new());
    }
    let text = tokio::fs::read_to_string(&project_path).await.unwrap_or_default();
    let proj: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    let t = proj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("scene")
        .to_lowercase();
    let f = proj
        .get("file")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    (t, f)
}

pub async fn kill_orphan_paper_procs() {
    let _ = run_sh(
        "pkill -9 -x skwd-paper 2>/dev/null; pkill -9 -x skwd-paper-still 2>/dev/null; true",
    )
    .await;
}

fn paper_bin() -> String {
    std::env::var("SKWD_PAPER_BIN").unwrap_or_else(|_| {
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let local = dir.join("skwd-paper");
            if local.exists() {
                return local.display().to_string();
            }
        }
        "skwd-paper".to_string()
    })
}

fn paper_still_bin() -> String {
    std::env::var("SKWD_PAPER_STILL_BIN").unwrap_or_else(|_| {
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let local = dir.join("skwd-paper-still");
            if local.exists() {
                return local.display().to_string();
            }
        }
        "skwd-paper-still".to_string()
    })
}

async fn swap_last_engine(new: config::PaperEngine) -> Option<config::PaperEngine> {
    static LAST: OnceLock<AsyncMutex<Option<config::PaperEngine>>> = OnceLock::new();
    let cell = LAST.get_or_init(|| AsyncMutex::new(None));
    let mut guard = cell.lock().await;
    let prev = *guard;
    *guard = Some(new);
    prev
}

async fn wait_for_awww_ready() -> bool {
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

async fn kill_awww_if_running() {
    if run_sh_status("awww query >/dev/null 2>&1").await {
        info!("apply_static: shutting down awww-daemon for non-awww engine");
        let _ = run_sh("awww kill >/dev/null 2>&1; pkill -x awww-daemon 2>/dev/null; true").await;
    }
}

async fn apply_awww(path: &str, outputs: &[String], config: &Config) -> anyhow::Result<()> {
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

async fn run_plasma_evaluate_script(script: &str) -> anyhow::Result<()> {
    let status = Command::new("qdbus6")
        .arg("org.kde.plasmashell")
        .arg("/PlasmaShell")
        .arg("org.kde.PlasmaShell.evaluateScript")
        .arg(script)
        .silent()
        .status()
        .await?;
    if !status.success() {
        warn!("plasmashell evaluateScript failed ({})", status);
        anyhow::bail!("plasmashell evaluateScript failed ({})", status);
    }
    Ok(())
}

async fn query_kde_screen_map() -> HashMap<String, u32> {
    let out = match Command::new("kscreen-doctor").arg("-j").output().await {
        Ok(o) if o.status.success() => o.stdout,
        _ => return HashMap::new(),
    };
    let text = String::from_utf8_lossy(&out);
    let json: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };
    let outputs = match json.get("outputs").and_then(|v| v.as_array()) {
        Some(o) => o,
        None => return HashMap::new(),
    };
    let mut map = HashMap::new();
    for output in outputs {
        let connected = output.get("connected").and_then(|v| v.as_bool()).unwrap_or(false);
        if !connected {
            continue;
        }
        let name = match output.get("name").and_then(|v| v.as_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        let priority = match output.get("priority").and_then(|v| v.as_u64()) {
            Some(p) if p >= 1 => (p - 1) as u32,
            _ => continue,
        };
        map.insert(name, priority);
    }
    map
}

fn kde_target_indices(outputs: &[String], map: &HashMap<String, u32>) -> Vec<u32> {
    if outputs.is_empty() {
        if map.is_empty() {
            Vec::new()
        } else {
            map.values().cloned().collect()
        }
    } else {
        outputs.iter().filter_map(|o| map.get(o).cloned()).collect()
    }
}

fn kde_fill_mode_value(fm: config::FillMode) -> u32 {
    match fm {
        config::FillMode::Fill => 2,
        config::FillMode::Fit => 0,
        config::FillMode::Stretch => 1,
        config::FillMode::Center => 6,
        config::FillMode::Tile => 3,
    }
}

async fn apply_kde_static(path: &str, outputs: &[String], config: &Config) -> anyhow::Result<()> {
    let map = query_kde_screen_map().await;
    let targets = kde_target_indices(outputs, &map);
    let indices_js = targets.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",");
    let fill_mode = kde_fill_mode_value(config.display.fill_mode);
    let file_url = format!("file://{path}");
    info!(
        path = %path,
        outputs = ?outputs,
        targets = ?targets,
        "apply_kde_static: setting wallpaper via plasmashell evaluateScript"
    );
    let script = format!(
        "var targets = [{indices_js}]; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (targets.length > 0 && targets.indexOf(ds[i].screen) === -1) continue; \
           var d = ds[i]; \
           d.wallpaperPlugin = 'org.kde.image'; \
           d.currentConfigGroup = ['Wallpaper', 'org.kde.image', 'General']; \
           d.writeConfig('Image', '{file_url}'); \
           d.writeConfig('FillMode', '{fill_mode}'); \
         }}"
    );
    run_plasma_evaluate_script(&script).await
}

async fn kde_unload_video_plugin(outputs: &[String]) {
    if !is_kde() {
        return;
    }
    let map = query_kde_screen_map().await;
    if map.is_empty() {
        return;
    }
    let target_indices: Vec<u32> = if outputs.is_empty() || outputs.iter().any(|o| o == "*") {
        map.values().copied().collect()
    } else {
        outputs.iter().filter_map(|o| map.get(o).copied()).collect()
    };
    if target_indices.is_empty() {
        return;
    }
    let indices_js = target_indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let script = format!(
        "var idx = [{indices_js}]; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           if (idx.indexOf(ds[i].screen) === -1) continue; \
           ds[i].wallpaperPlugin = 'org.kde.image'; \
         }}"
    );
    info!(outputs = ?outputs, "kde_unload_video_plugin");
    if let Err(e) = run_plasma_evaluate_script(&script).await {
        warn!(error = %e, "kde_unload_video_plugin failed");
    }
}

async fn kde_apply_audio(
    config: &Config,
    mute_per_output: &HashMap<String, bool>,
    volume_per_output: &HashMap<String, u32>,
) {
    if !is_kde() {
        return;
    }
    if mute_per_output.is_empty() && volume_per_output.is_empty() {
        return;
    }
    let kde_map = query_kde_screen_map().await;
    if kde_map.is_empty() {
        return;
    }
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";

    // Filter to outputs currently playing video/we in outputs.json.
    let state = read_outputs_state(&config.cache_dir()).await;
    let mut video_outputs: HashSet<String> = HashSet::new();
    if let Some(obj) = state.as_object() {
        for (out, entry) in obj {
            let t = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if t == "video" || t == "we" {
                video_outputs.insert(out.clone());
            }
        }
    }

    let mut configs: Vec<serde_json::Value> = Vec::new();
    let touched: HashSet<&String> = mute_per_output
        .keys()
        .chain(volume_per_output.keys())
        .collect();
    for out in touched {
        if !video_outputs.contains(out) {
            continue;
        }
        let Some(&idx) = kde_map.get(out) else {
            continue;
        };
        let mut entry = serde_json::Map::new();
        entry.insert("screen".into(), serde_json::json!(idx));
        if let Some(&m) = mute_per_output.get(out) {
            // MuteMode: 5 = always mute, 4 = never mute (always play)
            entry.insert(
                "muteMode".into(),
                serde_json::json!(if m { "5" } else { "4" }),
            );
        }
        if let Some(&v) = volume_per_output.get(out) {
            // Volume: plugin schema is Double in 0.0..1.0 - convert from 0..100 percent.
            let vol = (v as f32 / 100.0).clamp(0.0, 1.0);
            entry.insert("volume".into(), serde_json::json!(format!("{vol}")));
        }
        configs.push(serde_json::Value::Object(entry));
    }

    if configs.is_empty() {
        return;
    }

    let configs_js =
        serde_json::to_string(&configs).unwrap_or_else(|_| "[]".to_string());
    let script = format!(
        "var cfgs = {configs_js}; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           var cfg = null; \
           for (var j = 0; j < cfgs.length; j++) {{ \
             if (cfgs[j].screen === ds[i].screen) {{ cfg = cfgs[j]; break; }} \
           }} \
           if (cfg === null) continue; \
           var d = ds[i]; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           if (cfg.muteMode !== undefined) {{ d.writeConfig('MuteMode', cfg.muteMode); }} \
           if (cfg.volume !== undefined) {{ d.writeConfig('Volume', cfg.volume); }} \
         }}"
    );
    info!(configs = %configs_js, "kde_apply_audio");
    if let Err(e) = run_plasma_evaluate_script(&script).await {
        warn!(error = %e, "kde_apply_audio script failed");
    }
}

async fn apply_kde_video(
    path: &str,
    outputs: &[String],
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
    let map = query_kde_screen_map().await;
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";
    let file_url = format!("file://{path}");
    let global_mute = config.is_muted();
    let global_volume = config.volume();
    let global_mute_mode = if global_mute { "5" } else { "4" };
    let global_volume_f = (global_volume as f32 / 100.0).clamp(0.0, 1.0);

    let resolve = |name: &str| -> (String, f32) {
        let mute = outputs_audio.get(name).copied().unwrap_or(global_mute);
        let volume = outputs_volume.get(name).copied().unwrap_or(global_volume);
        let mute_mode = if mute { "5" } else { "4" };
        let vol = (volume as f32 / 100.0).clamp(0.0, 1.0);
        (mute_mode.to_string(), vol)
    };

    let (per_screen, fallback_to_all): (Vec<serde_json::Value>, bool) = if map.is_empty() {
        (Vec::new(), true)
    } else if outputs.is_empty() {
        let mut v = Vec::new();
        for (name, idx) in &map {
            let (mute_mode, volume) = resolve(name);
            v.push(serde_json::json!({
                "screen": idx,
                "muteMode": mute_mode,
                "volume": format!("{volume}"),
            }));
        }
        (v, false)
    } else {
        let mut v = Vec::new();
        for name in outputs {
            if let Some(idx) = map.get(name) {
                let (mute_mode, volume) = resolve(name);
                v.push(serde_json::json!({
                    "screen": idx,
                    "muteMode": mute_mode,
                    "volume": format!("{volume}"),
                }));
            }
        }
        (v, false)
    };

    let configs_js =
        serde_json::to_string(&per_screen).unwrap_or_else(|_| "[]".to_string());

    info!(
        path = %path,
        outputs = ?outputs,
        configs = %configs_js,
        fallback_to_all = fallback_to_all,
        "apply_kde_video: setting video wallpaper via plasmashell evaluateScript"
    );

    let script = format!(
        "var configs = {configs_js}; \
         var fallbackToAll = {fallback_to_all}; \
         var ds = desktops(); \
         for (var i = 0; i < ds.length; i++) {{ \
           var match = null; \
           for (var j = 0; j < configs.length; j++) {{ \
             if (configs[j].screen === ds[i].screen) {{ match = configs[j]; break; }} \
           }} \
           if (!fallbackToAll && match === null) continue; \
           var muteMode = match ? match.muteMode : '{global_mute_mode}'; \
           var volume = match ? match.volume : '{global_volume_f}'; \
           var d = ds[i]; \
           d.wallpaperPlugin = '{plugin}'; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           d.writeConfig('VideoUrls', '[{{\"filename\":\"{file_url}\",\"enabled\":true}}]'); \
           d.writeConfig('MuteMode', muteMode); \
           d.writeConfig('Volume', volume); \
         }}"
    );
    run_plasma_evaluate_script(&script).await
}

async fn pick_thumbs(
    neighbors: &[String],
    wallpaper_dir: &Path,
    exclude: &[&str],
    n: usize,
) -> Vec<String> {
    if !neighbors.is_empty() {
        return neighbors
            .iter()
            .filter(|p| !exclude.iter().any(|e| *e == p.as_str()))
            .filter(|p| Path::new(p).exists())
            .take(n)
            .cloned()
            .collect();
    }
    pick_random_thumbs(wallpaper_dir, exclude, n).await
}

async fn pick_random_thumbs(wallpaper_dir: &Path, exclude: &[&str], n: usize) -> Vec<String> {
    let mut entries = match tokio::fs::read_dir(wallpaper_dir).await {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let exts = [".jpg", ".jpeg", ".png", ".webp", ".bmp"];
    let mut all: Vec<String> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = path.to_string_lossy().to_lowercase();
        if !exts.iter().any(|e| name.ends_with(e)) {
            continue;
        }
        let s = path.to_string_lossy().to_string();
        if exclude.iter().any(|e| *e == s.as_str()) {
            continue;
        }
        all.push(s);
    }
    if all.is_empty() {
        return Vec::new();
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0);
    let mut picks = Vec::new();
    for i in 0..n.min(all.len()) {
        let idx = (nanos.wrapping_mul(2654435761).wrapping_add(i.wrapping_mul(48271))) % all.len();
        picks.push(all[idx].clone());
    }
    picks
}

async fn read_prev_transition_image(cache_dir: &Path) -> Option<String> {
    let state_path = cache_dir.join("last-wallpaper.json");
    let text = tokio::fs::read_to_string(&state_path).await.ok()?;
    let state: serde_json::Value = serde_json::from_str(&text).ok()?;
    let wp_type = state.get("type").and_then(|v| v.as_str())?;
    match wp_type {
        "static" => state
            .get("path")
            .and_then(|v| v.as_str())
            .filter(|p| Path::new(p).exists())
            .map(|s| s.to_string()),
        "we" => state
            .get("path")
            .and_then(|v| v.as_str())
            .filter(|p| !p.is_empty() && Path::new(p).exists())
            .map(|s| s.to_string()),
        "video" => {
            let path = state.get("path").and_then(|v| v.as_str())?;
            let thumb = video_thumb_path(cache_dir, path);
            if thumb.exists() {
                Some(thumb.display().to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn video_thumb_path(cache_dir: &Path, video_path: &str) -> PathBuf {
    let stem = Path::new(video_path)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "thumb".to_string());
    cache_dir.join(format!("wallpaper/video-thumbs/{stem}.jpg"))
}

async fn ensure_video_thumb_blocking(video_path: &str, cache_dir: &Path) -> Option<PathBuf> {
    let thumb = video_thumb_path(cache_dir, video_path);
    if thumb.exists() {
        return Some(thumb);
    }
    let _ = tokio::fs::create_dir_all(thumb.parent()?).await;
    let status = Command::new("ffmpeg")
        .args(["-y", "-i", video_path, "-vframes", "1", "-q:v", "2"])
        .arg(thumb.to_str()?)
        .silent()
        .status()
        .await
        .ok()?;
    if !status.success() {
        warn!("ffmpeg failed extracting first frame from {video_path}");
        return None;
    }
    Some(thumb)
}

async fn save_state(cache_dir: &Path, wp_type: &str, path: &str, we_id: &str) {
    let state_path = cache_dir.join("last-wallpaper.json");
    let _ = tokio::fs::create_dir_all(cache_dir).await;
    let mut obj = serde_json::json!({"type": wp_type});
    if !path.is_empty() {
        obj["path"] = serde_json::json!(path);
    }
    if !we_id.is_empty() {
        obj["we_id"] = serde_json::json!(we_id);
    }
    let _ = tokio::fs::write(&state_path, serde_json::to_string(&obj).unwrap_or_default()).await;
}

async fn save_outputs_state(
    cache_dir: &Path,
    outputs: &[String],
    wp_type: &str,
    path: &str,
    we_id: &str,
    mute_map: &HashMap<String, bool>,
) {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    let _ = tokio::fs::create_dir_all(cache_dir).await;
    let existing: serde_json::Value = match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    let mut map = match existing {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };

    let keys: Vec<String> = if outputs.is_empty() || outputs.iter().any(|o| o == "*") {
        vec!["*".to_string()]
    } else {
        outputs.to_vec()
    };

    if keys == ["*"] {
        map.clear();
    } else {
        map.remove("*");
    }
    for k in keys {
        let mut entry = serde_json::json!({"type": wp_type});
        if !path.is_empty() {
            entry["path"] = serde_json::json!(path);
        }
        if !we_id.is_empty() {
            entry["we_id"] = serde_json::json!(we_id);
        }
        let m = mute_map.get(&k).copied().unwrap_or(true);
        entry["mute"] = serde_json::json!(m);
        map.insert(k, entry);
    }

    let contents =
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
    write_outputs_state_atomic(&state_path, &contents).await;
}

async fn compute_audio_dedup(
    cache_dir: &Path,
    target_outs: &[String],
    outputs_audio: &HashMap<String, bool>,
    wp_type: &str,
    path: &str,
    we_id: &str,
    global_mute: bool,
) -> HashMap<String, bool> {
    let existing = read_outputs_state(cache_dir).await;
    let target_set: HashSet<&str> = target_outs.iter().map(|s| s.as_str()).collect();

    let mut external_unmuted = false;
    if let Some(obj) = existing.as_object() {
        for (out, entry) in obj {
            if out == "*" || target_set.contains(out.as_str()) {
                continue;
            }
            let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if entry_type != wp_type {
                continue;
            }
            let same_source = if wp_type == "we" {
                entry.get("we_id").and_then(|v| v.as_str()).unwrap_or("") == we_id
            } else {
                entry.get("path").and_then(|v| v.as_str()).unwrap_or("") == path
            };
            if !same_source {
                continue;
            }
            let m = entry.get("mute").and_then(|v| v.as_bool()).unwrap_or(true);
            if !m {
                external_unmuted = true;
                break;
            }
        }
    }

    let mut sorted = target_outs.to_vec();
    sorted.sort();

    let mut winner_chosen = external_unmuted;
    let mut result: HashMap<String, bool> = HashMap::new();
    for out in target_outs {
        let want_mute = outputs_audio.get(out).copied().unwrap_or(global_mute);
        if want_mute {
            result.insert(out.clone(), true);
        } else if winner_chosen {
            result.insert(out.clone(), true);
        } else {
            let is_first = sorted.iter().find(|o| !outputs_audio.get(*o).copied().unwrap_or(global_mute))
                .map(|first| first == out)
                .unwrap_or(false);
            if is_first {
                result.insert(out.clone(), false);
                winner_chosen = true;
            } else {
                result.insert(out.clone(), true);
            }
        }
    }
    result
}

pub async fn read_outputs_state(cache_dir: &Path) -> serde_json::Value {
    let _guard = outputs_state_lock().lock().await;
    let state_path = cache_dir.join("outputs.json");
    match tokio::fs::read_to_string(&state_path).await {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    }
}

fn is_kde() -> bool {
    std::env::var("XDG_CURRENT_DESKTOP")
        .map(|d| {
            let lower = d.to_lowercase();
            lower.contains("kde") || lower.contains("plasma")
        })
        .unwrap_or(false)
}

async fn run_sh(cmd: &str) -> anyhow::Result<()> {
    let status = Command::new("sh").arg("-c").arg(cmd).silent().status().await?;
    if !status.success() {
        warn!("command failed ({}): {cmd}", status);
        anyhow::bail!("shell command failed ({}): {cmd}", status);
    }
    Ok(())
}

async fn run_sh_status(cmd: &str) -> bool {
    match Command::new("sh").arg("-c").arg(cmd).silent().status().await {
        Ok(s) => s.success(),
        Err(_) => false,
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

fn substitute_placeholders(template: &str, wp_type: &str, name: &str, path: &str, thumb: &str) -> String {
    template
        .replace("%type%", wp_type)
        .replace("%name%", name)
        .replace("%path%", path)
        .replace("%thumb%", thumb)
}

async fn run_detached(cmd: &str) {
    let wrapped = format!(
        "nohup setsid sh -c {} </dev/null >/dev/null 2>&1 &",
        shell_quote(cmd)
    );
    if let Err(e) = run_sh(&wrapped).await {
        warn!("failed to spawn detached command: {e}");
    }
}

async fn run_external_apply(config: &Config, wp_type: &str, path: &str, thumb: &str) {
    let cmd = match config.external_wallpaper_command.as_deref() {
        Some(s) if !s.is_empty() => s,
        _ => return,
    };
    let name = basename(path);
    let resolved = substitute_placeholders(cmd, wp_type, &name, path, thumb);
    info!("running external wallpaper command: {resolved}");
    run_detached(&resolved).await;
}

async fn run_post_processing(
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
    use crate::config::{Config, Integration};

    #[tokio::test]
    async fn generate_matugen_config_skips_missing_template_files() {
        let tmp = std::env::temp_dir().join(format!("skwd-test-matugen-{}", std::process::id()));
        let template_dir = tmp.join("templates");
        let cache_dir = tmp.join("cache");
        std::fs::create_dir_all(&template_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();

        std::fs::write(template_dir.join("good.conf"), "value = {{colors.primary}}").unwrap();

        let mut config = Config::default();
        config.features.matugen = true;
        config.paths.templates = Some(template_dir.to_string_lossy().to_string());
        config.paths.cache = Some(cache_dir.to_string_lossy().to_string());
        config.integrations = vec![
            Integration {
                name: Some("good".into()),
                template: Some("good.conf".into()),
                output: Some("good-output.conf".into()),
                reload: None,
            },
            Integration {
                name: Some("missing-relative".into()),
                template: Some("does-not-exist.conf".into()),
                output: Some("never-written.conf".into()),
                reload: None,
            },
            Integration {
                name: Some("missing-absolute".into()),
                template: Some("/var/empty/__skwd_missing__.conf".into()),
                output: Some("also-never.conf".into()),
                reload: None,
            },
            Integration {
                name: Some("missing-tilde".into()),
                template: Some("~/.config/__skwd_missing_tilde__.conf".into()),
                output: Some("tilde-never.conf".into()),
                reload: None,
            },
        ];

        let cfg_path = generate_matugen_config(&config).await;
        let content = std::fs::read_to_string(&cfg_path).unwrap();

        assert!(content.contains("[templates.good]"), "good integration should be emitted:\n{content}");
        assert!(content.contains("good.conf"), "good template path should be present:\n{content}");

        assert!(!content.contains("[templates.missing-relative]"), "missing-relative should be skipped:\n{content}");
        assert!(!content.contains("does-not-exist.conf"), "missing relative template path should not be emitted:\n{content}");

        assert!(!content.contains("[templates.missing-absolute]"), "missing-absolute should be skipped:\n{content}");
        assert!(!content.contains("__skwd_missing__.conf"), "missing absolute template path should not be emitted:\n{content}");

        assert!(!content.contains("[templates.missing-tilde]"), "missing-tilde should be skipped:\n{content}");
        assert!(!content.contains("__skwd_missing_tilde__.conf"), "missing tilde template path should not be emitted:\n{content}");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test]
    async fn generate_matugen_config_emits_only_when_both_template_and_output_set() {
        let tmp = std::env::temp_dir().join(format!("skwd-test-matugen-pair-{}", std::process::id()));
        let template_dir = tmp.join("templates");
        let cache_dir = tmp.join("cache");
        std::fs::create_dir_all(&template_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();

        std::fs::write(template_dir.join("a.conf"), "x").unwrap();
        std::fs::write(template_dir.join("b.conf"), "x").unwrap();

        let mut config = Config::default();
        config.features.matugen = true;
        config.paths.templates = Some(template_dir.to_string_lossy().to_string());
        config.paths.cache = Some(cache_dir.to_string_lossy().to_string());
        config.integrations = vec![
            Integration {
                name: Some("no-output".into()),
                template: Some("a.conf".into()),
                output: None,
                reload: None,
            },
            Integration {
                name: Some("no-template".into()),
                template: None,
                output: Some("orphan.conf".into()),
                reload: None,
            },
            Integration {
                name: Some("complete".into()),
                template: Some("b.conf".into()),
                output: Some("complete.conf".into()),
                reload: None,
            },
        ];

        let cfg_path = generate_matugen_config(&config).await;
        let content = std::fs::read_to_string(&cfg_path).unwrap();

        assert!(content.contains("[templates.complete]"));
        assert!(!content.contains("[templates.no-output]"));
        assert!(!content.contains("[templates.no-template]"));

        std::fs::remove_dir_all(&tmp).ok();
    }
}
