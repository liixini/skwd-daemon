use crate::music::{auth::AuthStore, mpris, player::PlayerStore, session::SessionStore};
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc};

#[derive(Clone)]
pub struct PendingOAuth {
    pub verifier: String,
    pub state: String,
    pub client_id: String,
}

pub struct MusicState {
    pub auth: Arc<AuthStore>,
    pub session: Arc<SessionStore>,
    pub player: Arc<PlayerStore>,
    pub client_id: Arc<Mutex<String>>,
    pub device_name: Arc<Mutex<String>>,
    pub pending_oauth: Arc<Mutex<Option<PendingOAuth>>>,
    pub event_tx: broadcast::Sender<String>,
    pub callback_running: Arc<Mutex<bool>>,
    pub mpris_state: Arc<Mutex<mpris::MprisState>>,
    pub mpris_cmd_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<mpris::MprisCommand>>>>,
    pub mpris_cmd_tx: mpsc::UnboundedSender<mpris::MprisCommand>,
    pub mpris_server: Arc<Mutex<Option<Arc<mpris_server::Server<mpris::MprisAdapter>>>>>,
}

impl MusicState {
    pub fn new(event_tx: broadcast::Sender<String>) -> Self {
        let (mpris_tx, mpris_rx) = mpsc::unbounded_channel();
        Self {
            auth: Arc::new(AuthStore::new()),
            session: Arc::new(SessionStore::new()),
            player: Arc::new(PlayerStore::new()),
            client_id: Arc::new(Mutex::new(String::new())),
            device_name: Arc::new(Mutex::new("skwd-music".to_string())),
            pending_oauth: Arc::new(Mutex::new(None)),
            event_tx,
            callback_running: Arc::new(Mutex::new(false)),
            mpris_state: Arc::new(Mutex::new(mpris::MprisState::default())),
            mpris_cmd_rx: Arc::new(Mutex::new(Some(mpris_rx))),
            mpris_cmd_tx: mpris_tx,
            mpris_server: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn current_client_id(&self) -> String {
        self.client_id.lock().await.clone()
    }

    pub async fn set_client_id(&self, cid: String) {
        let mut g = self.client_id.lock().await;
        *g = cid;
    }

    pub async fn current_device(&self) -> String {
        self.device_name.lock().await.clone()
    }

    pub async fn set_device(&self, name: String) {
        let mut g = self.device_name.lock().await;
        *g = name;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_id_and_device_roundtrip() {
        let (tx, _rx) = broadcast::channel(8);
        let ms = MusicState::new(tx);
        assert_eq!(ms.current_device().await, "skwd-music");
        assert_eq!(ms.current_client_id().await, "");
        ms.set_client_id("cid-123".into()).await;
        assert_eq!(ms.current_client_id().await, "cid-123");
        ms.set_device("living-room".into()).await;
        assert_eq!(ms.current_device().await, "living-room");
    }
}
