use anyhow::Result;
use librespot::{
    core::{authentication::Credentials, session::Session},
    playback::{
        audio_backend,
        config::{AudioFormat, Bitrate, PlayerConfig},
        mixer::{Mixer, MixerConfig, softmixer::SoftMixer},
        player::{Player, PlayerEvent},
    },
};
use librespot::connect::{ConnectConfig, Spirc};
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};

#[derive(Default)]
pub struct PlayerStore {
    inner: Mutex<Option<RunningPlayer>>,
}

pub struct RunningPlayer {
    spirc: Spirc,
}

impl PlayerStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn ensure(
        &self,
        session: Session,
        credentials: Credentials,
        device_name: &str,
        event_tx: broadcast::Sender<String>,
        mpris_state: Arc<tokio::sync::Mutex<crate::music::mpris::MprisState>>,
    ) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let player_config = PlayerConfig {
            bitrate: Bitrate::Bitrate320,
            ..PlayerConfig::default()
        };
        let audio_format = AudioFormat::default();
        let backend = audio_backend::find(None)
            .ok_or_else(|| anyhow::anyhow!("no audio backend"))?;
        let mixer: Arc<dyn Mixer> = Arc::new(SoftMixer::open(MixerConfig::default())?);
        let mixer_clone = mixer.clone();
        let player = Player::new(
            player_config,
            session.clone(),
            mixer_clone.get_soft_volume(),
            move || backend(None, audio_format),
        );
        let player_events = player.get_player_event_channel();
        tokio::spawn(forward_player_events(player_events, event_tx, mpris_state));
        let connect_config = ConnectConfig {
            name: device_name.to_string(),
            ..Default::default()
        };
        tracing::info!("music: Spirc::new starting");
        let (spirc, spirc_task) =
            match Spirc::new(connect_config, session, credentials, player, mixer).await {
                Ok(v) => { tracing::info!("music: Spirc::new succeeded"); v }
                Err(e) => { tracing::error!("music: Spirc::new failed: {e:#}"); return Err(e.into()); }
            };
        tokio::spawn(spirc_task);
        match spirc.activate() {
            Ok(_) => tracing::info!("music: spirc.activate() ok"),
            Err(e) => tracing::warn!("music: spirc.activate() failed: {e:#}"),
        }
        *guard = Some(RunningPlayer { spirc });
        Ok(())
    }

    pub async fn set_volume(&self, volume: u16) -> Result<()> {
        let guard = self.inner.lock().await;
        let running = guard.as_ref().ok_or_else(|| anyhow::anyhow!("no player"))?;
        running.spirc.set_volume(volume)?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        let mut guard = self.inner.lock().await;
        if let Some(running) = guard.take() {
            running.spirc.shutdown().ok();
        }
    }

    pub async fn play(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let running = guard.as_ref().ok_or_else(|| anyhow::anyhow!("no player"))?;
        running.spirc.play()?;
        Ok(())
    }

    pub async fn pause(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let running = guard.as_ref().ok_or_else(|| anyhow::anyhow!("no player"))?;
        running.spirc.pause()?;
        Ok(())
    }

    pub async fn next(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let running = guard.as_ref().ok_or_else(|| anyhow::anyhow!("no player"))?;
        running.spirc.next()?;
        Ok(())
    }

    pub async fn previous(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let running = guard.as_ref().ok_or_else(|| anyhow::anyhow!("no player"))?;
        running.spirc.prev()?;
        Ok(())
    }
}

async fn forward_player_events(
    mut rx: librespot::playback::player::PlayerEventChannel,
    tx: broadcast::Sender<String>,
    mpris_state: Arc<tokio::sync::Mutex<crate::music::mpris::MprisState>>,
) {
    while let Some(event) = rx.recv().await {
        let payload = match event {
            PlayerEvent::TrackChanged { audio_item } => {
                use librespot::metadata::audio::UniqueFields;
                let (artist, album) = match &audio_item.unique_fields {
                    UniqueFields::Track { artists, album, .. } => {
                        let names: Vec<String> = artists.0.iter().map(|a| a.name.clone()).collect();
                        (names.join(", "), album.clone())
                    }
                    _ => (String::new(), String::new()),
                };
                let art_url = audio_item.covers.first().map(|c| c.url.clone()).unwrap_or_default();
                {
                    let mut s = mpris_state.lock().await;
                    s.track.uri = audio_item.uri.clone();
                    s.track.title = audio_item.name.clone();
                    s.track.artist = artist.clone();
                    s.track.album = album.clone();
                    s.track.art_url = art_url.clone();
                    s.track.duration_ms = audio_item.duration_ms;
                    s.position_ms = 0;
                }
                Some(serde_json::json!({
                    "event": "skwd.music.track.changed",
                    "data": {
                        "uri": audio_item.uri,
                        "name": audio_item.name,
                        "artist": artist,
                        "album": album,
                        "duration_ms": audio_item.duration_ms,
                        "covers": audio_item.covers.iter().map(|c| c.url.clone()).collect::<Vec<_>>(),
                    }
                }))
            },
            PlayerEvent::Playing { track_id, position_ms, .. } => {
                {
                    let mut s = mpris_state.lock().await;
                    s.playing = true;
                    s.position_ms = position_ms.into();
                }
                Some(serde_json::json!({
                    "event": "skwd.music.playing",
                    "data": { "uri": track_id.to_string(), "position_ms": position_ms }
                }))
            },
            PlayerEvent::Paused { track_id, position_ms, .. } => {
                {
                    let mut s = mpris_state.lock().await;
                    s.playing = false;
                    s.position_ms = position_ms.into();
                }
                Some(serde_json::json!({
                    "event": "skwd.music.paused",
                    "data": { "uri": track_id.to_string(), "position_ms": position_ms }
                }))
            },
            PlayerEvent::Stopped { track_id, .. } => {
                mpris_state.lock().await.playing = false;
                Some(serde_json::json!({
                    "event": "skwd.music.stopped",
                    "data": { "uri": track_id.to_string() }
                }))
            },
            PlayerEvent::PositionCorrection { position_ms, .. } => Some(serde_json::json!({
                "event": "skwd.music.position",
                "data": { "position_ms": position_ms }
            })),
            PlayerEvent::Seeked { position_ms, .. } => Some(serde_json::json!({
                "event": "skwd.music.seeked",
                "data": { "position_ms": position_ms }
            })),
            PlayerEvent::VolumeChanged { volume } => {
                mpris_state.lock().await.volume = (volume as f64) / 65535.0;
                Some(serde_json::json!({
                    "event": "skwd.music.volume",
                    "data": { "volume": volume }
                }))
            },
            PlayerEvent::ShuffleChanged { shuffle } => Some(serde_json::json!({
                "event": "skwd.music.shuffle",
                "data": { "shuffle": shuffle }
            })),
            PlayerEvent::EndOfTrack { track_id, .. } => Some(serde_json::json!({
                "event": "skwd.music.end_of_track",
                "data": { "uri": track_id.to_string() }
            })),
            _ => None,
        };
        if let Some(p) = payload {
            let _ = tx.send(p.to_string());
        }
    }
}
