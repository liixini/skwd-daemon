use crate::fill_mode::FillMode;
use anyhow::{Context, Result};
use serde::Deserialize;
use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState},
    delegate_compositor, delegate_output, delegate_registry,
    output::{OutputHandler, OutputState},
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
};
use wayland_client::{
    Connection, EventQueue, Proxy, QueueHandle,
    globals::{GlobalList, registry_queue_init},
    protocol::{wl_output::WlOutput, wl_surface::WlSurface},
};
use wayland_protocols_wlr::layer_shell::v1::client::{
    zwlr_layer_shell_v1::{Layer, ZwlrLayerShellV1},
    zwlr_layer_surface_v1::{Anchor, KeyboardInteractivity, ZwlrLayerSurfaceV1},
};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::render::{OutputBlitter, SharedRenderer, wayland_display_ptr};

static UNPAUSE_REQUESTED: AtomicBool = AtomicBool::new(false);
static ARMED: AtomicBool = AtomicBool::new(true);

extern "C" fn sigusr1_handler(_: libc::c_int) {
    UNPAUSE_REQUESTED.store(true, Ordering::Release);
    ARMED.store(true, Ordering::Release);
}

#[derive(Debug, Deserialize)]
struct VideoPersistCommand {
    #[serde(default, alias = "path")]
    to: String,
    #[serde(default)]
    mute: Option<bool>,
    #[serde(default)]
    volume: Option<u32>,
}

pub enum OutputTarget {
    All,
    Named(String),
}

struct App {
    registry_state: RegistryState,
    output_state: OutputState,
    compositor_state: CompositorState,
    layer_shell: ZwlrLayerShellV1,
    qh: QueueHandle<App>,
    target: OutputTarget,
    file_path: String,
    mpv_opts: Vec<(String, String)>,
    surfaces: Vec<SurfaceState>,
    renderer: Option<SharedRenderer>,
    ready_signaled: bool,
    ready_armed: bool,
    post_load_blit_count: u32,
    persist: bool,
    pending_cmd: Arc<Mutex<Option<VideoPersistCommand>>>,
}

struct SurfaceState {
    output: WlOutput,
    output_name: String,
    surface: WlSurface,
    layer: ZwlrLayerSurfaceV1,
    width: u32,
    height: u32,
    configured: bool,
    blitter: Option<OutputBlitter>,
    frame_pending: bool,
}

fn fill_mode_mpv_opts(mode: FillMode) -> Vec<(String, String)> {
    match mode {
        FillMode::Fill => vec![
            ("keepaspect".into(), "yes".into()),
            ("panscan".into(), "1.0".into()),
            ("video-unscaled".into(), "no".into()),
        ],
        FillMode::Fit => vec![
            ("keepaspect".into(), "yes".into()),
            ("panscan".into(), "0".into()),
            ("video-unscaled".into(), "no".into()),
        ],
        FillMode::Stretch => vec![
            ("keepaspect".into(), "no".into()),
            ("panscan".into(), "0".into()),
            ("video-unscaled".into(), "no".into()),
        ],
        FillMode::Center => vec![
            ("keepaspect".into(), "yes".into()),
            ("panscan".into(), "0".into()),
            ("video-unscaled".into(), "yes".into()),
        ],
        FillMode::Tile => vec![
            ("keepaspect".into(), "yes".into()),
            ("panscan".into(), "1.0".into()),
            ("video-unscaled".into(), "no".into()),
        ],
    }
}

pub fn run(
    target: OutputTarget,
    file_path: &str,
    mpv_opts: &[(String, String)],
    persist: bool,
    fill_mode: FillMode,
) -> Result<()> {
    let conn = Connection::connect_to_env().context("connecting to wayland")?;
    let (globals, mut event_queue): (GlobalList, EventQueue<App>) =
        registry_queue_init(&conn).context("registry_queue_init")?;
    let qh = event_queue.handle();

    let registry_state = RegistryState::new(&globals);
    let output_state = OutputState::new(&globals, &qh);
    let compositor_state =
        CompositorState::bind(&globals, &qh).context("compositor not available")?;
    let layer_shell: ZwlrLayerShellV1 = globals
        .bind(&qh, 1..=4, ())
        .context("zwlr_layer_shell_v1 not available")?;

    let pending_cmd: Arc<Mutex<Option<VideoPersistCommand>>> = Arc::new(Mutex::new(None));
    let mut combined_opts: Vec<(String, String)> = fill_mode_mpv_opts(fill_mode);
    for (k, v) in mpv_opts {
        if combined_opts.iter().any(|(ek, _)| ek == k) {
            if let Some(slot) = combined_opts.iter_mut().find(|(ek, _)| ek == k) {
                slot.1 = v.clone();
            }
        } else {
            combined_opts.push((k.clone(), v.clone()));
        }
    }
    let mut app = App {
        registry_state,
        output_state,
        compositor_state,
        layer_shell,
        qh: qh.clone(),
        target,
        file_path: file_path.to_string(),
        mpv_opts: combined_opts,
        surfaces: Vec::new(),
        renderer: None,
        ready_signaled: false,
        ready_armed: false,
        post_load_blit_count: 0,
        persist,
        pending_cmd: pending_cmd.clone(),
    };

    unsafe {
        libc::signal(libc::SIGUSR1, sigusr1_handler as *const () as libc::sighandler_t);
    }

    if persist {
        ARMED.store(false, Ordering::Release);
        spawn_video_stdin_reader(pending_cmd);
    }

    event_queue.roundtrip(&mut app)?;
    app.spawn_initial_surfaces();

    loop {
        event_queue.blocking_dispatch(&mut app)?;
        if UNPAUSE_REQUESTED.swap(false, Ordering::AcqRel) {
            if let Some(r) = app.renderer.as_mut() {
                r.unpause_mpv();
            }
            if persist {
                app.render_all(true);
                for i in 0..app.surfaces.len() {
                    app.schedule_frame(i);
                }
            }
        }
        app.try_consume_pending_cmd();
    }
}

fn spawn_video_stdin_reader(pending: Arc<Mutex<Option<VideoPersistCommand>>>) {
    std::thread::spawn(move || {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            match handle.read_line(&mut line) {
                Ok(0) => {
                    tracing::info!("video persist: stdin closed, exiting");
                    std::process::exit(0);
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<VideoPersistCommand>(trimmed) {
                        Ok(cmd) => {
                            tracing::info!(path = %cmd.to, "video persist: command received");
                            *pending.lock().unwrap() = Some(cmd);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, line = %trimmed, "video persist: bad command");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "video persist: stdin read error");
                    std::process::exit(0);
                }
            }
        }
    });
}

impl App {
    fn spawn_initial_surfaces(&mut self) {
        let outputs: Vec<WlOutput> = self.output_state.outputs().collect();
        for output in outputs {
            self.maybe_create_surface(output);
        }
    }

    fn maybe_create_surface(&mut self, output: WlOutput) {
        let info = match self.output_state.info(&output) {
            Some(i) => i,
            None => return,
        };
        let name = info.name.clone().unwrap_or_default();

        match &self.target {
            OutputTarget::All => {}
            OutputTarget::Named(target) => {
                if &name != target {
                    return;
                }
            }
        }

        if self.surfaces.iter().any(|s| s.output_name == name) {
            return;
        }

        let surface = self.compositor_state.create_surface(&self.qh);
        let layer = self.layer_shell.get_layer_surface(
            &surface,
            Some(&output),
            Layer::Background,
            "skwd-paper".to_string(),
            &self.qh,
            (),
        );
        layer.set_anchor(Anchor::Top | Anchor::Bottom | Anchor::Left | Anchor::Right);
        layer.set_exclusive_zone(-1);
        layer.set_keyboard_interactivity(KeyboardInteractivity::None);
        layer.set_size(0, 0);
        surface.commit();

        tracing::info!(output = %name, "created layer surface");

        self.surfaces.push(SurfaceState {
            output,
            output_name: name,
            surface,
            layer,
            width: 0,
            height: 0,
            configured: false,
            blitter: None,
            frame_pending: false,
        });
    }

    fn ensure_renderer(&mut self) -> Result<()> {
        if self.renderer.is_some() {
            return Ok(());
        }
        let max_w = self
            .surfaces
            .iter()
            .map(|s| s.width)
            .max()
            .unwrap_or(0)
            .max(1);
        let max_h = self
            .surfaces
            .iter()
            .map(|s| s.height)
            .max()
            .unwrap_or(0)
            .max(1);
        let surface_for_display = self
            .surfaces
            .first()
            .map(|s| &s.surface)
            .ok_or_else(|| anyhow::anyhow!("no surface yet"))?;
        let display_ptr = wayland_display_ptr(surface_for_display)?;
        let renderer = SharedRenderer::new(
            display_ptr,
            max_w,
            max_h,
            &self.file_path,
            &self.mpv_opts,
        )?;
        self.renderer = Some(renderer);
        tracing::info!(fbo_w = max_w, fbo_h = max_h, "shared renderer initialized");
        Ok(())
    }

    fn ensure_blitter(&mut self, idx: usize) {
        if self.surfaces[idx].blitter.is_some() {
            let s = &mut self.surfaces[idx];
            if let Some(b) = s.blitter.as_mut() {
                b.resize(s.width, s.height);
            }
            return;
        }
        let Some(renderer) = self.renderer.as_ref() else {
            return;
        };
        let s = &mut self.surfaces[idx];
        match renderer.make_blitter(&s.surface, s.width, s.height) {
            Ok(b) => {
                s.blitter = Some(b);
                tracing::info!(output = %s.output_name, w = s.width, h = s.height, "blitter created");
            }
            Err(e) => tracing::error!(output = %s.output_name, error = %e, "blitter create failed"),
        }
    }

    fn try_consume_pending_cmd(&mut self) {
        if !self.persist {
            return;
        }
        let cmd = match self.pending_cmd.lock().unwrap().take() {
            Some(c) => c,
            None => return,
        };
        let Some(renderer) = self.renderer.as_mut() else {
            tracing::warn!("video persist: command received before renderer init");
            return;
        };
        if cmd.to.is_empty() {
            tracing::info!(mute = ?cmd.mute, volume = ?cmd.volume, "legacy persist: audio update");
            if let Some(m) = cmd.mute {
                renderer.set_mute(m);
            }
            if let Some(v) = cmd.volume {
                renderer.set_volume(v);
            }
            return;
        }
        let swap_start = std::time::Instant::now();
        let next_mute = cmd.mute.unwrap_or(true);
        let next_volume = cmd.volume.unwrap_or(80);
        match renderer.load_path(&cmd.to, next_mute, next_volume) {
            Ok(()) => {
                self.file_path = cmd.to.clone();
                self.ready_signaled = false;
                self.ready_armed = false;
                self.post_load_blit_count = 0;
                tracing::info!(path = %cmd.to, swap_ms = swap_start.elapsed().as_millis() as u64, "video persist: loaded");
            }
            Err(e) => {
                tracing::error!(error = %e, path = %cmd.to, "video persist: load_path failed");
            }
        }
    }

    fn schedule_frame(&mut self, idx: usize) {
        let s = &mut self.surfaces[idx];
        if s.frame_pending {
            return;
        }
        s.frame_pending = true;
        s.surface.frame(&self.qh, s.surface.clone());
        s.surface.commit();
    }

    fn render_all(&mut self, force_blit: bool) {
        if !ARMED.load(Ordering::Acquire) {
            return;
        }
        let Some(renderer) = self.renderer.as_mut() else {
            return;
        };
        let new_frame = renderer.render_mpv_to_fbo();
        if std::env::var("SKWD_PAPER_TRACE").is_ok() {
            tracing::info!(new_frame, force_blit, surfaces = self.surfaces.len(), "render_all tick");
        }
        if !new_frame && !force_blit {
            return;
        }
        let mut blitted = false;
        for s in &self.surfaces {
            if let Some(b) = s.blitter.as_ref() {
                renderer.blit_to(b);
                blitted = true;
            }
        }
        if blitted && new_frame && !self.ready_signaled {
            self.post_load_blit_count = self.post_load_blit_count.saturating_add(1);
            if self.post_load_blit_count >= 3 && !self.ready_armed {
                self.ready_armed = true;
            }
        }
    }
}

impl CompositorHandler for App {
    fn scale_factor_changed(
        &mut self,
        _: &Connection,
        _: &QueueHandle<Self>,
        _: &WlSurface,
        _: i32,
    ) {
    }
    fn transform_changed(
        &mut self,
        _: &Connection,
        _: &QueueHandle<Self>,
        _: &WlSurface,
        _: wayland_client::protocol::wl_output::Transform,
    ) {
    }
    fn frame(&mut self, _: &Connection, _: &QueueHandle<Self>, surface: &WlSurface, _: u32) {
        if self.ready_armed && !self.ready_signaled {
            crate::ipc::signal_ready();
            self.ready_signaled = true;
            self.ready_armed = false;
        }
        let idx = match self.surfaces.iter().position(|s| &s.surface == surface) {
            Some(i) => i,
            None => return,
        };
        self.surfaces[idx].frame_pending = false;
        if std::env::var("SKWD_PAPER_NO_RENDER").is_err() {
            self.render_all(false);
        }
        // Reschedule frames for every output
        for i in 0..self.surfaces.len() {
            self.schedule_frame(i);
        }
    }
    fn surface_enter(
        &mut self,
        _: &Connection,
        _: &QueueHandle<Self>,
        _: &WlSurface,
        _: &WlOutput,
    ) {
    }
    fn surface_leave(
        &mut self,
        _: &Connection,
        _: &QueueHandle<Self>,
        _: &WlSurface,
        _: &WlOutput,
    ) {
    }
}

impl OutputHandler for App {
    fn output_state(&mut self) -> &mut OutputState {
        &mut self.output_state
    }
    fn new_output(&mut self, _: &Connection, _: &QueueHandle<Self>, output: WlOutput) {
        self.maybe_create_surface(output);
    }
    fn update_output(&mut self, _: &Connection, _: &QueueHandle<Self>, _: WlOutput) {}
    fn output_destroyed(&mut self, _: &Connection, _: &QueueHandle<Self>, output: WlOutput) {
        self.surfaces.retain(|s| s.output != output);
    }
}

impl ProvidesRegistryState for App {
    fn registry(&mut self) -> &mut RegistryState {
        &mut self.registry_state
    }
    registry_handlers![OutputState];
}

delegate_compositor!(App);
delegate_output!(App);
delegate_registry!(App);

impl wayland_client::Dispatch<ZwlrLayerSurfaceV1, ()> for App {
    fn event(
        state: &mut Self,
        layer: &ZwlrLayerSurfaceV1,
        event: <ZwlrLayerSurfaceV1 as Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        use wayland_protocols_wlr::layer_shell::v1::client::zwlr_layer_surface_v1::Event;
        match event {
            Event::Configure {
                serial,
                width,
                height,
            } => {
                layer.ack_configure(serial);
                let idx = match state.surfaces.iter().position(|s| &s.layer == layer) {
                    Some(i) => i,
                    None => return,
                };
                if width > 0 && height > 0 {
                    state.surfaces[idx].width = width;
                    state.surfaces[idx].height = height;
                }
                let was_unconfigured = !state.surfaces[idx].configured;
                state.surfaces[idx].configured = true;
                if state.surfaces[idx].width == 0 || state.surfaces[idx].height == 0 {
                    return;
                }
                if was_unconfigured && state.renderer.is_none()
                    && let Err(e) = state.ensure_renderer()
                {
                    tracing::error!(error = %e, "renderer init failed");
                    return;
                }
                state.ensure_blitter(idx);
                if was_unconfigured {
                    // Need an initial buffer attached to start the frame callback loop.
                    state.render_all(true);
                    state.schedule_frame(idx);
                }
            }
            Event::Closed => {
                state.surfaces.retain(|s| &s.layer != layer);
                if state.surfaces.is_empty() {
                    std::process::exit(0);
                }
            }
            _ => {}
        }
    }
}

impl wayland_client::Dispatch<ZwlrLayerShellV1, ()> for App {
    fn event(
        _: &mut Self,
        _: &ZwlrLayerShellV1,
        _: <ZwlrLayerShellV1 as Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}
