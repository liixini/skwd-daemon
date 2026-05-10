use anyhow::Result;
use librespot::core::{cache::Cache, config::SessionConfig, session::Session};
use std::path::PathBuf;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct SessionStore {
    inner: Mutex<Option<Session>>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn current(&self) -> Option<Session> {
        self.inner.lock().await.clone()
    }

    pub async fn ensure(&self) -> Result<Session> {
        let mut guard = self.inner.lock().await;
        if let Some(s) = guard.as_ref() {
            if !s.is_invalid() {
                return Ok(s.clone());
            }
        }
        let cache = build_cache().ok();
        let cfg = SessionConfig::default();
        let session = Session::new(cfg, cache);
        *guard = Some(session.clone());
        Ok(session)
    }

    pub async fn disconnect(&self) {
        let mut guard = self.inner.lock().await;
        if let Some(s) = guard.take() {
            s.shutdown();
        }
    }
}

fn build_cache() -> Result<Cache> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .ok_or_else(|| anyhow::anyhow!("no cache dir"))?
        .join("skwd")
        .join("music");
    std::fs::create_dir_all(&base).ok();
    Ok(Cache::new(
        Some(base.join("system")),
        Some(base.join("volume")),
        Some(base.join("audio")),
        None,
    )?)
}
