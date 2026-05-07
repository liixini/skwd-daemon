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

    fn add_transitions(&mut self, mut new: Vec<Child>) {
        self.transitions.retain_mut(|c| matches!(c.try_wait(), Ok(None)));
        self.transitions.append(&mut new);
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

async fn await_ready_for_pid(pid: u32, timeout_ms: u64) -> bool {
    let notify = Arc::new(Notify::new());
    {
        let mut reg = ready_registry().lock().await;
        reg.insert(pid, notify.clone());
    }
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        notify.notified(),
    )
    .await
    .is_ok();
    let mut reg = ready_registry().lock().await;
    reg.remove(&pid);
    result
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
    let args: Vec<String> = vec![
        "--persist".to_string(),
        "--fill-mode".to_string(),
        fill_mode.as_arg().to_string(),
        output.to_string(),
        file_path.to_string(),
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

async fn drop_persist_papers_for(targets: &[String]) {
    let mut map = persist_papers().lock().await;
    if targets.iter().any(|o| o == "*") {
        for (_, mut p) in map.drain() {
            let _ = p.child.start_kill();
        }
        return;
    }
    for out in targets {
        if let Some(mut p) = map.remove(out) {
            let _ = p.child.start_kill();
        }
    }
}

async fn drop_video_persist_papers_for(targets: &[String]) {
    let mut map = video_persist_papers().lock().await;
    if targets.iter().any(|o| o == "*") {
        for (_, mut p) in map.drain() {
            let _ = p.child.start_kill();
        }
        return;
    }
    for out in targets {
        if let Some(mut p) = map.remove(out) {
            let _ = p.child.start_kill();
        }
    }
}

async fn drop_steady_image_papers_for(targets: &[String]) {
    let mut map = steady_image_papers().lock().await;
    if targets.iter().any(|o| o == "*") {
        for (_, mut p) in map.drain() {
            let _ = p.child.start_kill();
        }
        return;
    }
    for out in targets {
        if let Some(mut p) = map.remove(out) {
            let _ = p.child.start_kill();
        }
    }
}

async fn prune_persist_papers(targets: &[String]) {
    let star_mode = targets.iter().any(|o| o == "*");
    let mut map = persist_papers().lock().await;
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
    let mut args: Vec<String> = vec![
        "--transition-from".to_string(),
        from_path.to_string(),
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
    args.push(to_path.to_string());

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
    config: &Config,
) -> anyhow::Result<()> {
    apply_static_inner(path, outputs, neighbors, config, false).await
}

async fn apply_static_inner(
    path: &str,
    outputs: &[String],
    neighbors: &[String],
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

    if config.wants_external_render() {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        let _ = tokio::fs::remove_file(config.video_dir().join("lockscreen-video.mp4")).await;
        run_external_apply(config, "static", path, path).await;
    } else if is_kde {
        drop_video_persist_paper().await;
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        run_sh(&format!("plasma-apply-wallpaperimage {}", shell_quote(path))).await?;
    } else {
        let bin = paper_bin();
        let target_outs: Vec<String> = if outputs.is_empty() {
            vec!["*".to_string()]
        } else {
            outputs.to_vec()
        };
        rebuild_scene_we(
            config,
            &target_outs,
            &std::collections::BTreeMap::new(),
            config.volume(),
        )
        .await
        .ok();
        drop_video_persist_papers_for(&target_outs).await;

        let prev_for_transition = if restoring || !config.transition.enabled {
            None
        } else {
            prev_image
                .as_deref()
                .filter(|p| *p != path && Path::new(p).exists())
                .map(|p| p.to_string())
        };

        let persist_eligible = !restoring
            && config.transition.enabled
            && prev_for_transition.is_some();

        if persist_eligible {
            let prev = prev_for_transition.as_deref().unwrap();
            let thumbs = pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, path], 20).await;
            let shader = config.transition.shader.clone();
            let duration_ms = config.transition.duration_ms;

            prune_persist_papers(&target_outs).await;

            let mut all_ok = true;
            let mut any_reused = false;
            let mut any_spawned = false;
            for out in &target_outs {
                if try_send_persist(out, path, &shader, duration_ms, &thumbs, None, None).await {
                    any_reused = true;
                    continue;
                }
                match spawn_persist_paper(&bin, out, prev, path, &shader, duration_ms, &thumbs, config.display.fill_mode, true, 0)
                    .await
                {
                    Ok(()) => any_spawned = true,
                    Err(e) => {
                        warn!(error = %e, output = %out, "persist: spawn failed");
                        all_ok = false;
                        break;
                    }
                }
            }

            if all_ok {
                let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
                for mut c in prev_steady {
                    let _ = c.start_kill();
                }
                drop_steady_image_papers_for(&target_outs).await;
                if prev_was_we {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    kill_legacy_video_procs().await;
                }
                let current_dir = config.cache_dir().join("wallpaper");
                let _ = tokio::fs::create_dir_all(&current_dir).await;
                let wall_jpg = current_dir.join("current.jpg");
                let _ = tokio::fs::copy(path, &wall_jpg).await;
                save_state(&config.cache_dir(), "static", path, "").await;
                save_outputs_state(&config.cache_dir(), &target_outs, "static", path, "", &HashMap::new()).await;
                let path_owned = path.to_string();
                let config_clone = config.clone();
                tokio::spawn(async move {
                    let _ = matugen_handle.await;
                    run_post_processing(&config_clone, "static", &basename(&path_owned), &path_owned, &path_owned, restoring).await;
                    info!("post-apply tasks done for static (persist): {path_owned}");
                });
                info!(
                    total_ms = apply_total.elapsed().as_millis() as u64,
                    lock_wait_ms,
                    reused = any_reused,
                    spawned = any_spawned,
                    outputs = target_outs.len(),
                    "applied static wallpaper (persist)"
                );
                return Ok(());
            } else {
                drop_persist_paper().await;
                warn!("persist: partial failure, falling back to legacy");
            }
        }

        let mut cold_persist_spawned = false;
        if let Some(prev) = prev_for_transition.as_deref() {
            let thumbs = pick_thumbs(neighbors, &config.wallpaper_dir(), &[prev, path], 20).await;
            let shader = config.transition.shader.clone();
            let dur_ms = config.transition.duration_ms;
            for out in &target_outs {
                match spawn_persist_paper(
                    &bin, out, prev, path, &shader, dur_ms, &thumbs, config.display.fill_mode, true, 0,
                )
                .await
                {
                    Ok(()) => cold_persist_spawned = true,
                    Err(e) => warn!(error = %e, output = %out, "static cold persist spawn failed"),
                }
            }
        }

        if cold_persist_spawned {
            drop_steady_image_papers_for(&target_outs).await;
            let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
            for mut c in prev_steady {
                let _ = c.start_kill();
            }
            info!("apply_static legacy: cold-spawned persist transition_paper");
        } else {
            ensure_steady_image_paper(&bin, &target_outs, path, config.display.fill_mode).await;
            let prev_steady = std::mem::take(&mut fleet().lock().await.steady);
            for mut c in prev_steady {
                let _ = c.start_kill();
            }
            info!("apply_static legacy: steady_image_paper only (no transition)");
        }
        if prev_was_we {
            kill_legacy_video_procs().await;
        }
    }

    let current_dir = config.cache_dir().join("wallpaper");
    let _ = tokio::fs::create_dir_all(&current_dir).await;
    let wall_jpg = current_dir.join("current.jpg");
    let _ = tokio::fs::copy(path, &wall_jpg).await;

    save_state(&config.cache_dir(), "static", path, "").await;
    save_outputs_state(&config.cache_dir(), outputs, "static", path, "", &HashMap::new()).await;

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
    outputs_audio: &HashMap<String, bool>,
    outputs_volume: &HashMap<String, u32>,
    config: &Config,
) -> anyhow::Result<()> {
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
        drop_persist_paper().await;
        drop_steady_image_paper().await;
        fleet().lock().await.replace_steady(Vec::new());
        apply_kde_video(path, mute).await?;
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
        drop_steady_image_papers_for(&target_outs).await;

        let video_alive = video_persist_alive_outputs(&target_outs).await;
        let image_alive = persist_alive_outputs(&target_outs).await;
        let all_image_alive = !target_outs.is_empty()
            && target_outs.iter().all(|o| image_alive.contains(o));

        let transition_handles = config.transition.enabled
            && !restoring
            && prev_image.as_deref().filter(|p| Path::new(p).exists()).is_some();

        let mut video_persist_ok = !restoring;
        let mut cold_video_pids: Vec<u32> = Vec::new();
        if transition_handles {
            drop_video_persist_papers_for(&target_outs).await;
        } else {
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

        let to_image = if !restoring && config.transition.enabled {
            Some(path.to_string())
        } else {
            None
        };
        let prev_for_transition = if restoring || !config.transition.enabled {
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
            let dur_ms = config.transition.duration_ms;
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

        let have_transitions =
            !transitions.is_empty() || image_persist_handled_transition || cold_persist_spawned;
        let scheduled_transitions: Vec<Child> = std::mem::take(&mut transitions);

        if video_persist_ok {
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
    save_outputs_state(&config.cache_dir(), outputs, "video", path, "", &dedup_mute).await;

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

    if we_type == "video" && !we_file.is_empty() {
        let video_path = item_dir.join(&we_file);
        let video_str = video_path.display().to_string();

        if is_kde() {
            apply_kde_video(&video_str, global_mute).await?;
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
    save_outputs_state(&config.cache_dir(), screens, "we", &preview_str, we_id, &dedup_mute).await;

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
                outs
            };
            last_id = match wp_type.as_str() {
                "static" if !path.is_empty() => {
                    apply_static_inner(&path, &outputs_arg, &[], config, true).await?;
                    path
                }
                "video" if !path.is_empty() => {
                    apply_video_inner(&path, &outputs_arg, &[], &HashMap::new(), &HashMap::new(), config, true).await?;
                    path
                }
                "we" if !we_id.is_empty() => {
                    apply_we_inner(&we_id, &outputs_arg, &HashMap::new(), &HashMap::new(), config, true).await?;
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
            apply_static_inner(path, &[], &[], config, true).await?;
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

pub async fn retheme(config: &Config, scheme: Option<&str>, mode: Option<&str>) -> anyhow::Result<()> {
    let current_jpg = config.cache_dir().join("wallpaper/current.jpg");
    if !current_jpg.exists() {
        anyhow::bail!("no current wallpaper image to retheme");
    }
    let image_path = current_jpg.display().to_string();
    run_matugen_with(&image_path, config, scheme, mode).await;
    run_reloads(config).await;
    info!("retheme completed for {image_path}");
    Ok(())
}


async fn generate_matugen_config(config: &Config) -> PathBuf {
    let config_path = config.matugen_config_path();
    let template_dir = config.template_dir();
    let cache_dir = config.cache_dir();

    let mut lines = vec!["[config]".to_string(), "reload_apps = false".to_string(), String::new()];

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
    }

    let _ = tokio::fs::create_dir_all(config_path.parent().unwrap_or_else(|| Path::new("/tmp"))).await;
    let _ = tokio::fs::write(&config_path, lines.join("\n")).await;
    info!(
        "generated matugen config with {} integrations",
        config.integrations.len()
    );
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
    run_matugen_inner(image_path, config, &config_path, scheme, mode).await;
}

async fn run_matugen_with(image_path: &str, config: &Config, scheme: Option<&str>, mode: Option<&str>) {
    if !config.features.matugen {
        return;
    }
    let config_path = generate_matugen_config(config).await;
    let scheme = scheme.unwrap_or_else(|| config.matugen_scheme());
    let mode = mode.unwrap_or_else(|| config.matugen_mode());
    run_matugen_inner(image_path, config, &config_path, scheme, mode).await;
}

async fn run_matugen_inner(image_path: &str, config: &Config, config_path: &Path, scheme: &str, mode: &str) {
    let status = Command::new("matugen")
        .arg("-c")
        .arg(&config_path)
        .arg("image")
        .arg("-t")
        .arg(scheme)
        .arg("-m")
        .arg(mode)
        .arg("--source-color-index")
        .arg("0")
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


pub async fn set_audio(mute: Option<bool>, volume: Option<u32>) {
    let mut payload = serde_json::Map::new();
    payload.insert("to".into(), serde_json::json!(""));
    if let Some(m) = mute {
        payload.insert("mute".into(), serde_json::json!(m));
    }
    if let Some(v) = volume {
        payload.insert("volume".into(), serde_json::json!(v));
    }
    let line = format!("{}\n", serde_json::Value::Object(payload));

    {
        let mut map = persist_papers().lock().await;
        let outputs: Vec<String> = map.keys().cloned().collect();
        for out in outputs {
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
        let outputs: Vec<String> = map.keys().cloned().collect();
        for out in outputs {
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
    let _ = run_sh("pkill -9 -x skwd-paper 2>/dev/null; true").await;
}

fn paper_bin() -> String {
    std::env::var("SKWD_PAPER_BIN").unwrap_or_else(|_| {
        // Try sibling binary in the same directory as this daemon
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

async fn apply_kde_video(path: &str, mute: bool) -> anyhow::Result<()> {
    let plugin = "luisbocanegra.smart.video.wallpaper.reborn";
    let mute_mode = if mute { "4" } else { "0" };
    let file_url = format!("file://{path}");
    let script = format!(
        "var allDesktops = desktops(); \
         for (var i = 0; i < allDesktops.length; i++) {{ \
           var d = allDesktops[i]; \
           d.wallpaperPlugin = '{plugin}'; \
           d.currentConfigGroup = ['Wallpaper', '{plugin}', 'General']; \
           d.writeConfig('VideoUrls', '[{{\"filename\":\"{file_url}\",\"enabled\":true}}]'); \
           d.writeConfig('MuteMode', '{mute_mode}'); \
         }}"
    );
    run_sh(&format!(
        "qdbus6 org.kde.plasmashell /PlasmaShell org.kde.PlasmaShell.evaluateScript {}",
        shell_quote(&script)
    ))
    .await
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

    let _ = tokio::fs::write(
        &state_path,
        serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default(),
    )
    .await;
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
    }
    Ok(())
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

async fn get_screen_args() -> String {
    if let Ok(output) = Command::new("wlr-randr")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let names: Vec<&str> = text
            .lines()
            .filter(|l| !l.starts_with(' ') && !l.is_empty())
            .filter_map(|l| l.split_whitespace().next())
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {} --scaling fill", shell_quote(n)))
                .collect::<String>();
        }
    }

    if let Ok(output) = Command::new("hyprctl")
        .arg("monitors")
        .arg("-j")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
        && let Ok(monitors) = serde_json::from_slice::<Vec<serde_json::Value>>(&output.stdout)
    {
        let names: Vec<&str> = monitors
            .iter()
            .filter_map(|m| m.get("name").and_then(|v| v.as_str()))
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {} --scaling fill", shell_quote(n)))
                .collect::<String>();
        }
    }

    if let Ok(output) = Command::new("niri")
        .arg("msg")
        .arg("outputs")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let names: Vec<String> = text
            .lines()
            .filter_map(|l| {
                let trimmed = l.trim();
                if !trimmed.starts_with("Output") {
                    return None;
                }
                if let (Some(open), Some(close)) = (trimmed.rfind('('), trimmed.rfind(')'))
                    && open < close
                {
                    return Some(trimmed[open + 1..close].to_string());
                }
                trimmed
                    .split_whitespace()
                    .nth(1)
                    .map(|s| s.trim_matches(|c: char| c == '"' || c == ':').to_string())
            })
            .collect();
        if !names.is_empty() {
            return names
                .iter()
                .map(|n| format!(" --screen-root {} --scaling fill", shell_quote(n)))
                .collect::<String>();
        }
    }

    String::new()
}
