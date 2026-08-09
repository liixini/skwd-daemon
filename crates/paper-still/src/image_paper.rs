use crate::fill_mode::{FillMode, apply_fill_mode};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState},
    delegate_compositor, delegate_output, delegate_registry, delegate_shm,
    output::{OutputHandler, OutputState},
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
    shm::{Shm, ShmHandler, slot::SlotPool},
};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex};
use wayland_client::{
    Connection, EventQueue, Proxy, QueueHandle,
    globals::{GlobalList, registry_queue_init},
    protocol::{wl_output::WlOutput, wl_shm, wl_surface::WlSurface},
};
use wayland_protocols::wp::viewporter::client::{wp_viewport::WpViewport, wp_viewporter::WpViewporter};
use wayland_protocols_wlr::layer_shell::v1::client::{
    zwlr_layer_shell_v1::{Layer, ZwlrLayerShellV1},
    zwlr_layer_surface_v1::{Anchor, KeyboardInteractivity, ZwlrLayerSurfaceV1},
};

#[derive(Debug, Deserialize)]
struct ImagePersistCommand {
    path: String,
}

pub enum OutputTarget {
    All,
    Named(String),
}

fn decode_image(file_path: &str) -> Result<(u32, u32, Vec<u8>)> {
    let img = image::ImageReader::open(file_path)
        .with_context(|| format!("opening image: {file_path}"))?
        .with_guessed_format()
        .with_context(|| format!("sniffing image format: {file_path}"))?
        .decode()
        .with_context(|| format!("decoding image: {file_path}"))?;
    let rgba = img.to_rgba8();
    let (img_w, img_h) = rgba.dimensions();
    Ok((img_w, img_h, rgba.into_raw()))
}

fn copy_pixels_into_canvas(canvas: &mut [u8], pixels: &[u8]) -> Result<()> {
    let canvas_len = canvas.len();
    let destination = canvas.get_mut(..pixels.len()).ok_or_else(|| {
        anyhow!(
            "wl_shm canvas is too small: need {} bytes, got {}",
            pixels.len(),
            canvas_len
        )
    })?;
    destination.copy_from_slice(pixels);
    Ok(())
}

pub fn run(target: OutputTarget, file_path: &str, persist: bool, fill_mode: FillMode, namespace: &str) -> Result<()> {
    let (img_w, img_h, bytes) = decode_image(file_path)?;
    tracing::info!(w = img_w, h = img_h, ?fill_mode, "image decoded");

    let conn = Connection::connect_to_env().context("wayland connect")?;
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
    let viewporter: WpViewporter = globals
        .bind(&qh, 1..=1, ())
        .context("wp_viewporter not available")?;
    let shm = Shm::bind(&globals, &qh).context("wl_shm not available")?;

    let pending_cmd: Arc<Mutex<Option<ImagePersistCommand>>> = Arc::new(Mutex::new(None));
    let (wake_r, wake_w) = make_wake_pipe()?;
    let mut app = App {
        registry_state,
        output_state,
        compositor_state,
        shm,
        layer_shell,
        viewporter,
        qh: qh.clone(),
        target,
        raw_pixels: bytes,
        raw_w: img_w,
        raw_h: img_h,
        fill_mode,
        buffer: None,
        buffer_w: 0,
        buffer_h: 0,
        pool: None,
        _buffer_keepalive: None,
        surfaces: Vec::new(),
        ready_signaled: false,
        persist,
        pending_cmd: pending_cmd.clone(),
        namespace: namespace.to_string(),
    };

    if persist {
        spawn_image_stdin_reader(pending_cmd, wake_w);
    }

    event_queue.roundtrip(&mut app)?;
    app.spawn_initial_surfaces();

    run_event_loop(&mut app, &mut event_queue, &conn, wake_r)
}

fn make_wake_pipe() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    if rc != 0 {
        return Err(anyhow!("pipe2 failed: {}", std::io::Error::last_os_error()));
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn run_event_loop(
    app: &mut App,
    event_queue: &mut EventQueue<App>,
    conn: &Connection,
    wake_r: OwnedFd,
) -> Result<()> {
    let wl_fd = conn.as_fd().as_raw_fd();
    let wake_fd = wake_r.as_raw_fd();
    loop {
        event_queue.dispatch_pending(app)?;
        app.try_consume_pending_cmd();

        let read_guard = match event_queue.prepare_read() {
            Some(g) => g,
            None => continue,
        };
        let _ = conn.flush();

        let mut fds = [
            libc::pollfd {
                fd: wl_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake_fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                drop(read_guard);
                continue;
            }
            return Err(anyhow!("poll failed: {err}"));
        }

        if (fds[0].revents & libc::POLLIN) != 0 {
            read_guard.read()?;
        } else {
            drop(read_guard);
        }
        if (fds[1].revents & libc::POLLIN) != 0 {
            let mut buf = [0u8; 64];
            unsafe {
                let _ = libc::read(wake_fd, buf.as_mut_ptr().cast(), buf.len());
            }
        }
    }
}

fn spawn_image_stdin_reader(
    pending: Arc<Mutex<Option<ImagePersistCommand>>>,
    wake_w: OwnedFd,
) {
    std::thread::spawn(move || {
        use std::io::BufRead;
        let wake_fd = wake_w.as_raw_fd();
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            match handle.read_line(&mut line) {
                Ok(0) => {
                    tracing::info!("image persist: stdin closed, exiting");
                    std::process::exit(0);
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<ImagePersistCommand>(trimmed) {
                        Ok(cmd) => {
                            tracing::info!(path = %cmd.path, "image persist: command received");
                            *pending.lock().unwrap() = Some(cmd);
                            let byte: u8 = 1;
                            unsafe {
                                let _ = libc::write(
                                    wake_fd,
                                    (&byte as *const u8).cast(),
                                    1,
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, line = %trimmed, "image persist: bad command");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "image persist: stdin read error");
                    std::process::exit(0);
                }
            }
        }
    });
}

struct App {
    registry_state: RegistryState,
    output_state: OutputState,
    compositor_state: CompositorState,
    shm: Shm,
    layer_shell: ZwlrLayerShellV1,
    viewporter: WpViewporter,
    qh: QueueHandle<App>,
    target: OutputTarget,
    raw_pixels: Vec<u8>,
    raw_w: u32,
    raw_h: u32,
    fill_mode: FillMode,
    buffer: Option<wayland_client::protocol::wl_buffer::WlBuffer>,
    buffer_w: u32,
    buffer_h: u32,
    pool: Option<SlotPool>,
    _buffer_keepalive: Option<smithay_client_toolkit::shm::slot::Buffer>,
    surfaces: Vec<SurfaceState>,
    ready_signaled: bool,
    persist: bool,
    pending_cmd: Arc<Mutex<Option<ImagePersistCommand>>>,
    namespace: String,
}

struct SurfaceState {
    output: WlOutput,
    output_name: String,
    surface: WlSurface,
    layer: ZwlrLayerSurfaceV1,
    viewport: WpViewport,
    width: u32,
    height: u32,
    attached: bool,
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
            self.namespace.clone(),
            &self.qh,
            (),
        );
        layer.set_anchor(Anchor::Top | Anchor::Bottom | Anchor::Left | Anchor::Right);
        layer.set_exclusive_zone(-1);
        layer.set_keyboard_interactivity(KeyboardInteractivity::None);
        layer.set_size(0, 0);
        let viewport = self.viewporter.get_viewport(&surface, &self.qh, ());
        surface.commit();

        tracing::info!(output = %name, "created layer surface (image mode)");

        self.surfaces.push(SurfaceState {
            output,
            output_name: name,
            surface,
            layer,
            viewport,
            width: 0,
            height: 0,
            attached: false,
        });
    }

    fn ensure_buffer_for(&mut self, surf_w: u32, surf_h: u32) -> Result<()> {
        if surf_w == 0 || surf_h == 0 {
            return Ok(());
        }
        if self.buffer.is_some() && self.buffer_w == surf_w && self.buffer_h == surf_h {
            return Ok(());
        }
        let (bw, bh, pixels) = apply_fill_mode(
            self.raw_w,
            self.raw_h,
            self.raw_pixels.clone(),
            surf_w,
            surf_h,
            self.fill_mode,
        );
        let stride = (bw as i32) * 4;
        let pool_size = (stride as usize) * (bh as usize);
        let needs_new_pool = match &self.pool {
            None => true,
            Some(p) => p.len() < pool_size,
        };
        if needs_new_pool {
            self.pool = Some(
                SlotPool::new(pool_size, &self.shm).map_err(|e| anyhow!("SlotPool::new: {e}"))?,
            );
        }
        let pool = self.pool.as_mut().unwrap();
        let (buffer, canvas) = pool
            .create_buffer(bw as i32, bh as i32, stride, wl_shm::Format::Abgr8888)
            .map_err(|e| anyhow!("create_buffer: {e}"))?;
        copy_pixels_into_canvas(canvas, &pixels)?;
        self.buffer = Some(buffer.wl_buffer().clone());
        self._buffer_keepalive = Some(buffer);
        self.buffer_w = bw;
        self.buffer_h = bh;
        Ok(())
    }

    fn attach_to(&mut self, idx: usize) {
        if self.surfaces[idx].width == 0 || self.surfaces[idx].height == 0 {
            return;
        }
        let (sw, sh) = (self.surfaces[idx].width, self.surfaces[idx].height);
        if let Err(e) = self.ensure_buffer_for(sw, sh) {
            tracing::error!(error = %e, "ensure_buffer_for failed");
            return;
        }
        let buffer = match self.buffer.as_ref() {
            Some(b) => b.clone(),
            None => return,
        };
        let bw = self.buffer_w;
        let bh = self.buffer_h;
        let s = &mut self.surfaces[idx];
        s.viewport.set_source(0.0, 0.0, bw as f64, bh as f64);
        s.viewport.set_destination(s.width as i32, s.height as i32);
        s.surface.attach(Some(&buffer), 0, 0);
        s.surface.damage_buffer(0, 0, bw as i32, bh as i32);
        s.attached = true;
        s.surface.commit();
        if !self.ready_signaled {
            crate::ipc::signal_ready();
            self.ready_signaled = true;
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
        let (new_w, new_h, new_bytes) = match decode_image(&cmd.path) {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(error = %e, path = %cmd.path, "image persist: decode failed");
                return;
            }
        };
        self.raw_w = new_w;
        self.raw_h = new_h;
        self.raw_pixels = new_bytes;
        let (surf_w, surf_h) = match self.surfaces.iter().find(|s| s.width > 0 && s.height > 0) {
            Some(s) => (s.width, s.height),
            None => (self.buffer_w.max(1), self.buffer_h.max(1)),
        };
        self.buffer = None;
        if let Err(e) = self.ensure_buffer_for(surf_w, surf_h) {
            tracing::error!(error = %e, "image persist: rebuild buffer failed");
            return;
        }
        let buffer = match self.buffer.as_ref() {
            Some(b) => b.clone(),
            None => return,
        };
        let bw = self.buffer_w;
        let bh = self.buffer_h;
        for idx in 0..self.surfaces.len() {
            let s = &mut self.surfaces[idx];
            if s.width == 0 || s.height == 0 {
                continue;
            }
            s.viewport.set_source(0.0, 0.0, bw as f64, bh as f64);
            s.viewport.set_destination(s.width as i32, s.height as i32);
            s.surface.attach(Some(&buffer), 0, 0);
            s.surface.damage_buffer(0, 0, bw as i32, bh as i32);
            s.surface.commit();
        }
        unsafe { libc::malloc_trim(0) };
        tracing::info!(path = %cmd.path, w = new_w, h = new_h, "image persist: swapped");
    }
}

#[cfg(test)]
mod tests {
    use super::copy_pixels_into_canvas;

    #[test]
    fn copies_into_alignment_padded_canvas() {
        let pixels = [1, 2, 3, 4, 5];
        let mut canvas = [0xaa; 64];

        copy_pixels_into_canvas(&mut canvas, &pixels).unwrap();

        assert_eq!(&canvas[..pixels.len()], &pixels);
        assert!(canvas[pixels.len()..].iter().all(|byte| *byte == 0xaa));
    }

    #[test]
    fn rejects_canvas_smaller_than_pixels() {
        let mut canvas = [0; 3];
        let error = copy_pixels_into_canvas(&mut canvas, &[1, 2, 3, 4]).unwrap_err();

        assert!(error.to_string().contains("need 4 bytes, got 3"));
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
    fn frame(&mut self, _: &Connection, _: &QueueHandle<Self>, _: &WlSurface, _: u32) {}
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

impl ShmHandler for App {
    fn shm_state(&mut self) -> &mut Shm {
        &mut self.shm
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
delegate_shm!(App);
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
                state.attach_to(idx);
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

impl wayland_client::Dispatch<WpViewporter, ()> for App {
    fn event(
        _: &mut Self,
        _: &WpViewporter,
        _: <WpViewporter as Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

impl wayland_client::Dispatch<WpViewport, ()> for App {
    fn event(
        _: &mut Self,
        _: &WpViewport,
        _: <WpViewport as Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}
