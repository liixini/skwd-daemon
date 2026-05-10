use anyhow::Result;
use mpris_server::{
    LoopStatus, Metadata, PlaybackRate, PlaybackStatus, PlayerInterface, Property, RootInterface,
    Server, Signal, Time, TrackId, Volume,
    zbus::{self, fdo},
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default, Clone)]
pub struct MprisTrack {
    pub uri: String,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub art_url: String,
    pub duration_ms: u32,
}

pub struct MprisState {
    pub track: MprisTrack,
    pub playing: bool,
    pub position_ms: i64,
    pub volume: f64,
}

impl Default for MprisState {
    fn default() -> Self {
        Self {
            track: MprisTrack::default(),
            playing: false,
            position_ms: 0,
            volume: 1.0,
        }
    }
}

pub struct MprisAdapter {
    state: Arc<Mutex<MprisState>>,
    cmd: tokio::sync::mpsc::UnboundedSender<MprisCommand>,
}

pub enum MprisCommand {
    Play,
    Pause,
    PlayPause,
    Next,
    Previous,
    SetVolume(f64),
}

impl RootInterface for MprisAdapter {
    async fn raise(&self) -> fdo::Result<()> { Ok(()) }
    async fn quit(&self) -> fdo::Result<()> { Ok(()) }
    async fn can_quit(&self) -> fdo::Result<bool> { Ok(false) }
    async fn fullscreen(&self) -> fdo::Result<bool> { Ok(false) }
    async fn set_fullscreen(&self, _: bool) -> zbus::Result<()> { Ok(()) }
    async fn can_set_fullscreen(&self) -> fdo::Result<bool> { Ok(false) }
    async fn can_raise(&self) -> fdo::Result<bool> { Ok(false) }
    async fn has_track_list(&self) -> fdo::Result<bool> { Ok(false) }
    async fn identity(&self) -> fdo::Result<String> { Ok("skwd-music".into()) }
    async fn desktop_entry(&self) -> fdo::Result<String> { Ok("skwd-music".into()) }
    async fn supported_uri_schemes(&self) -> fdo::Result<Vec<String>> { Ok(vec!["spotify".into()]) }
    async fn supported_mime_types(&self) -> fdo::Result<Vec<String>> { Ok(vec![]) }
}

impl PlayerInterface for MprisAdapter {
    async fn next(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::Next);
        Ok(())
    }
    async fn previous(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::Previous);
        Ok(())
    }
    async fn pause(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::Pause);
        Ok(())
    }
    async fn play_pause(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::PlayPause);
        Ok(())
    }
    async fn stop(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::Pause);
        Ok(())
    }
    async fn play(&self) -> fdo::Result<()> {
        let _ = self.cmd.send(MprisCommand::Play);
        Ok(())
    }
    async fn seek(&self, _: Time) -> fdo::Result<()> { Ok(()) }
    async fn set_position(&self, _: TrackId, _: Time) -> fdo::Result<()> { Ok(()) }
    async fn open_uri(&self, _: String) -> fdo::Result<()> { Ok(()) }

    async fn playback_status(&self) -> fdo::Result<PlaybackStatus> {
        let s = self.state.lock().await;
        Ok(if s.playing { PlaybackStatus::Playing } else { PlaybackStatus::Paused })
    }
    async fn loop_status(&self) -> fdo::Result<LoopStatus> { Ok(LoopStatus::None) }
    async fn set_loop_status(&self, _: LoopStatus) -> zbus::Result<()> { Ok(()) }
    async fn rate(&self) -> fdo::Result<PlaybackRate> { Ok(1.0) }
    async fn set_rate(&self, _: PlaybackRate) -> zbus::Result<()> { Ok(()) }
    async fn shuffle(&self) -> fdo::Result<bool> { Ok(false) }
    async fn set_shuffle(&self, _: bool) -> zbus::Result<()> { Ok(()) }

    async fn metadata(&self) -> fdo::Result<Metadata> {
        let s = self.state.lock().await;
        let mut md = Metadata::builder();
        if !s.track.uri.is_empty() {
            md = md.trackid(TrackId::try_from(format!("/skwd/music/{}", sanitize(&s.track.uri))).unwrap_or_else(|_| TrackId::NO_TRACK.clone()));
        }
        if !s.track.title.is_empty() { md = md.title(s.track.title.clone()); }
        if !s.track.artist.is_empty() {
            md = md.artist(vec![s.track.artist.clone()]);
        }
        if !s.track.album.is_empty() { md = md.album(s.track.album.clone()); }
        if !s.track.art_url.is_empty() { md = md.art_url(s.track.art_url.clone()); }
        if s.track.duration_ms > 0 {
            md = md.length(Time::from_millis(s.track.duration_ms.into()));
        }
        Ok(md.build())
    }

    async fn volume(&self) -> fdo::Result<Volume> {
        Ok(self.state.lock().await.volume)
    }
    async fn set_volume(&self, v: Volume) -> zbus::Result<()> {
        let _ = self.cmd.send(MprisCommand::SetVolume(v.clamp(0.0, 1.0)));
        Ok(())
    }
    async fn position(&self) -> fdo::Result<Time> {
        Ok(Time::from_millis(self.state.lock().await.position_ms))
    }
    async fn minimum_rate(&self) -> fdo::Result<PlaybackRate> { Ok(1.0) }
    async fn maximum_rate(&self) -> fdo::Result<PlaybackRate> { Ok(1.0) }
    async fn can_go_next(&self) -> fdo::Result<bool> { Ok(true) }
    async fn can_go_previous(&self) -> fdo::Result<bool> { Ok(true) }
    async fn can_play(&self) -> fdo::Result<bool> { Ok(true) }
    async fn can_pause(&self) -> fdo::Result<bool> { Ok(true) }
    async fn can_seek(&self) -> fdo::Result<bool> { Ok(false) }
    async fn can_control(&self) -> fdo::Result<bool> { Ok(true) }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

pub async fn launch(
    state: Arc<Mutex<MprisState>>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<MprisCommand>,
) -> Result<Arc<Server<MprisAdapter>>> {
    let adapter = MprisAdapter { state, cmd: cmd_tx };
    let server = Server::new("skwd-music", adapter).await?;
    Ok(Arc::new(server))
}

pub async fn emit_state_update(server: &Server<MprisAdapter>) -> Result<()> {
    let imp = server.imp();
    let s = imp.state.lock().await;
    let status = if s.playing { PlaybackStatus::Playing } else { PlaybackStatus::Paused };
    drop(s);

    let md = imp.metadata().await.unwrap_or_else(|_| Metadata::builder().build());
    server
        .properties_changed([
            Property::PlaybackStatus(status),
            Property::Metadata(md),
            Property::Volume(imp.state.lock().await.volume),
        ])
        .await?;
    Ok(())
}
