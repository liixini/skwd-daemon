use crate::fill_mode::{FillMode, apply_fill_mode};
use crate::video_source::VideoSource;
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use smithay_client_toolkit::{
    compositor::{CompositorHandler, CompositorState},
    delegate_compositor, delegate_output, delegate_registry,
    output::{OutputHandler, OutputState},
    registry::{ProvidesRegistryState, RegistryState},
    registry_handlers,
};
use std::ffi::{CString, c_void};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use wayland_client::{
    Connection, EventQueue, Proxy, QueueHandle,
    globals::{GlobalList, registry_queue_init},
    protocol::{wl_output::WlOutput, wl_surface::WlSurface},
};
use wayland_egl::WlEglSurface;
use wayland_protocols_wlr::layer_shell::v1::client::{
    zwlr_layer_shell_v1::{Layer, ZwlrLayerShellV1},
    zwlr_layer_surface_v1::{Anchor, KeyboardInteractivity, ZwlrLayerSurfaceV1},
};

#[derive(Debug, Deserialize)]
pub struct PersistCommand {
    #[serde(default)]
    pub to: String,
    #[serde(default = "default_shader")]
    pub shader: String,
    #[serde(default = "default_duration_ms")]
    pub duration_ms: u64,
    #[serde(default)]
    pub thumbs: Vec<String>,
    #[serde(default)]
    pub mute: Option<bool>,
    #[serde(default)]
    pub volume: Option<u32>,
    #[serde(default)]
    pub warmup: Option<bool>,
}

fn default_shader() -> String {
    "random".to_string()
}

fn default_duration_ms() -> u64 {
    600
}

type EglInstance = khronos_egl::Instance<khronos_egl::Static>;
const EGL: EglInstance = khronos_egl::Instance::new(khronos_egl::Static);

pub enum OutputTarget {
    All,
    Named(String),
}

#[derive(Clone, Copy, Debug)]
pub enum SurfaceLayer {
    Background,
    Bottom,
}

pub const MAX_THUMBS: usize = 20;

pub fn run(
    target: OutputTarget,
    old_path: &str,
    new_path: &str,
    shader_name: &str,
    duration_ms: u64,
    thumb_paths: &[String],
    persist: bool,
    fill_mode: FillMode,
    initial_mute: bool,
    initial_volume: u32,
    layer: SurfaceLayer,
) -> Result<()> {
    let (chosen_name, chosen_src) = resolve_shader(shader_name);
    let thumb_slice: Vec<String> = if chosen_name == "mosaic-tumble" {
        thumb_paths.iter().take(MAX_THUMBS).cloned().collect()
    } else {
        Vec::new()
    };
    let from_is_video = is_video_path(old_path);
    let to_is_video = is_video_path(new_path);
    let (old_result, new_result, thumb_results) = std::thread::scope(|s| {
        let h_old = s.spawn(|| {
            if from_is_video {
                Ok(placeholder_pixels())
            } else {
                decode_path_rgba(old_path)
            }
        });
        let h_new = s.spawn(|| {
            if to_is_video {
                Ok(placeholder_pixels())
            } else {
                decode_path_rgba(new_path)
            }
        });
        let thumb_handles: Vec<_> = thumb_slice
            .iter()
            .map(|p| {
                let p = p.clone();
                s.spawn(move || decode_path_rgba(&p))
            })
            .collect();
        let thumbs: Vec<_> = thumb_handles.into_iter().map(|h| h.join().unwrap()).collect();
        (h_old.join().unwrap(), h_new.join().unwrap(), thumbs)
    });
    let (old_w, old_h, old_pixels) = old_result?;
    let (new_w, new_h, new_pixels) = new_result?;
    let thumbs: Vec<(u32, u32, Vec<u8>)> = thumb_results
        .into_iter()
        .filter_map(|r| match r {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(error = %e, "thumb decode failed");
                None
            }
        })
        .collect();
    tracing::info!(
        old_w, old_h, new_w, new_h,
        thumbs_count = thumbs.len(),
        shader = shader_name,
        duration_ms,
        "transition: images decoded"
    );

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

    let pending_cmd: Arc<Mutex<Option<PersistCommand>>> = Arc::new(Mutex::new(None));
    let mut app = App {
        registry_state,
        output_state,
        compositor_state,
        layer_shell,
        qh: qh.clone(),
        target,
        old_pixels,
        old_w,
        old_h,
        new_pixels,
        new_w,
        new_h,
        from_video_path: if from_is_video { Some(old_path.to_string()) } else { None },
        to_video_path: if to_is_video { Some(new_path.to_string()) } else { None },
        thumbs,
        shader_src: chosen_src.to_string(),
        shader_name: chosen_name.to_string(),
        duration_ms,
        start_time: None,
        gl_state: None,
        surfaces: Vec::new(),
        ready_signaled: false,
        persist,
        pending_cmd: pending_cmd.clone(),
        transition_active: true,
        fill_mode,
        initial_mute,
        initial_volume,
        layer,
        exit_scheduled: false,
    };

    if persist {
        spawn_stdin_reader(pending_cmd);
    }

    event_queue.roundtrip(&mut app)?;
    app.spawn_initial_surfaces();

    loop {
        event_queue.blocking_dispatch(&mut app)?;
    }
}

fn spawn_stdin_reader(pending: Arc<Mutex<Option<PersistCommand>>>) {
    std::thread::spawn(move || {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            match handle.read_line(&mut line) {
                Ok(0) => {
                    tracing::info!("persist: stdin closed, exiting");
                    std::process::exit(0);
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<PersistCommand>(trimmed) {
                        Ok(cmd) => {
                            tracing::info!(to = %cmd.to, "persist: command received (latest-wins)");
                            *pending.lock().unwrap() = Some(cmd);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, line = %trimmed, "persist: bad command");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "persist: stdin read error");
                    std::process::exit(0);
                }
            }
        }
    });
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
enum Pipeline {
    Single,
    Bloom { strength: f32, threshold: f32, radius: f32 },
}

struct GlState {
    egl_display: khronos_egl::Display,
    egl_config: khronos_egl::Config,
    primary_ctx: khronos_egl::Context,
    primary_pbuffer: khronos_egl::Surface,
    tex_old: u32,
    tex_new: u32,
    video_old: Option<VideoSource>,
    video_new: Option<VideoSource>,
    tex_old_fbo: Option<u32>,
    tex_new_fbo: Option<u32>,
    tex_thumbs: Vec<u32>,
    program: u32,
    quad_vbo: u32,
    loc_progress: i32,
    _loc_tex_old: i32,
    _loc_tex_new: i32,
    pipeline: Pipeline,
    bright_program: u32,
    loc_bright_threshold: i32,
    blur_program: u32,
    loc_blur_dir: i32,
    loc_blur_radius: i32,
    composite_program: u32,
    loc_composite_strength: i32,
    fit_program: u32,
    loc_fit_scale: i32,
    loc_fit_offset: i32,
    fit_vao: u32,
}

struct SurfaceBlitter {
    egl_context: khronos_egl::Context,
    egl_surface: khronos_egl::Surface,
    _wl_egl_surface: WlEglSurface,
    vao: u32,
    width: u32,
    height: u32,
    fbo_base: Option<u32>,
    tex_base: Option<u32>,
    fbo_a: Option<u32>,
    tex_a: Option<u32>,
    fbo_b: Option<u32>,
    tex_b: Option<u32>,
}

struct App {
    registry_state: RegistryState,
    output_state: OutputState,
    compositor_state: CompositorState,
    layer_shell: ZwlrLayerShellV1,
    qh: QueueHandle<App>,
    target: OutputTarget,
    old_pixels: Vec<u8>,
    old_w: u32,
    old_h: u32,
    new_pixels: Vec<u8>,
    new_w: u32,
    new_h: u32,
    from_video_path: Option<String>,
    to_video_path: Option<String>,
    thumbs: Vec<(u32, u32, Vec<u8>)>,
    shader_src: String,
    shader_name: String,
    duration_ms: u64,
    start_time: Option<Instant>,
    gl_state: Option<GlState>,
    surfaces: Vec<SurfaceState>,
    ready_signaled: bool,
    persist: bool,
    pending_cmd: Arc<Mutex<Option<PersistCommand>>>,
    transition_active: bool,
    fill_mode: FillMode,
    initial_mute: bool,
    initial_volume: u32,
    layer: SurfaceLayer,
    exit_scheduled: bool,
}

struct SurfaceState {
    output: WlOutput,
    output_name: String,
    surface: WlSurface,
    layer: ZwlrLayerSurfaceV1,
    width: u32,
    height: u32,
    blitter: Option<SurfaceBlitter>,
    frame_pending: bool,
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
        let wlr_layer = match self.layer {
            SurfaceLayer::Background => Layer::Background,
            SurfaceLayer::Bottom => Layer::Bottom,
        };
        let layer = self.layer_shell.get_layer_surface(
            &surface,
            Some(&output),
            wlr_layer,
            "skwd-paper-transition".to_string(),
            &self.qh,
            (),
        );
        layer.set_anchor(Anchor::Top | Anchor::Bottom | Anchor::Left | Anchor::Right);
        layer.set_exclusive_zone(-1);
        layer.set_keyboard_interactivity(KeyboardInteractivity::None);
        layer.set_size(0, 0);
        surface.commit();

        self.surfaces.push(SurfaceState {
            output,
            output_name: name,
            surface,
            layer,
            width: 0,
            height: 0,
            blitter: None,
            frame_pending: false,
        });
    }

    fn ensure_gl(&mut self) -> Result<()> {
        if self.gl_state.is_some() {
            return Ok(());
        }
        let surface_for_display = self
            .surfaces
            .first()
            .map(|s| &s.surface)
            .ok_or_else(|| anyhow!("no surface yet"))?;
        let display_ptr = wayland_display_ptr(surface_for_display)?;

        let egl_display = unsafe { EGL.get_display(display_ptr) }
            .ok_or_else(|| anyhow!("eglGetDisplay"))?;
        EGL.initialize(egl_display)
            .map_err(|e| anyhow!("eglInitialize: {e:?}"))?;
        EGL.bind_api(khronos_egl::OPENGL_API)
            .map_err(|e| anyhow!("eglBindAPI: {e:?}"))?;

        let attribs = [
            khronos_egl::SURFACE_TYPE,
            khronos_egl::WINDOW_BIT | khronos_egl::PBUFFER_BIT,
            khronos_egl::RENDERABLE_TYPE,
            khronos_egl::OPENGL_BIT,
            khronos_egl::RED_SIZE,
            8,
            khronos_egl::GREEN_SIZE,
            8,
            khronos_egl::BLUE_SIZE,
            8,
            khronos_egl::ALPHA_SIZE,
            0,
            khronos_egl::NONE,
        ];
        let egl_config = EGL
            .choose_first_config(egl_display, &attribs)
            .map_err(|e| anyhow!("eglChooseConfig: {e:?}"))?
            .ok_or_else(|| anyhow!("no EGL config"))?;

        let ctx_attribs = [
            khronos_egl::CONTEXT_MAJOR_VERSION,
            3,
            khronos_egl::CONTEXT_MINOR_VERSION,
            3,
            khronos_egl::NONE,
        ];
        let primary_ctx = EGL
            .create_context(egl_display, egl_config, None, &ctx_attribs)
            .map_err(|e| anyhow!("eglCreateContext: {e:?}"))?;

        let pb_attribs = [
            khronos_egl::WIDTH,
            1,
            khronos_egl::HEIGHT,
            1,
            khronos_egl::NONE,
        ];
        let primary_pbuffer = EGL
            .create_pbuffer_surface(egl_display, egl_config, &pb_attribs)
            .map_err(|e| anyhow!("eglCreatePbufferSurface: {e:?}"))?;

        EGL.make_current(
            egl_display,
            Some(primary_pbuffer),
            Some(primary_pbuffer),
            Some(primary_ctx),
        )
        .map_err(|e| anyhow!("eglMakeCurrent: {e:?}"))?;

        gl::load_with(|name| {
            let cname = CString::new(name).unwrap();
            EGL.get_proc_address(&cname.to_string_lossy())
                .map(|p| p as *const c_void)
                .unwrap_or(std::ptr::null())
        });

        let (target_w, target_h) = self
            .surfaces
            .iter()
            .find(|s| s.width > 0 && s.height > 0)
            .map(|s| (s.width, s.height))
            .unwrap_or((self.new_w.max(1), self.new_h.max(1)));

        let (tex_old, video_old) = if let Some(ref path) = self.from_video_path {
            let mut vs = VideoSource::new(path, target_w, target_h, self.initial_mute)
                .with_context(|| format!("VideoSource for from-side: {path}"))?;
            if !self.initial_mute {
                vs.set_volume(self.initial_volume);
            }
            vs.prime_first_frame(2000);
            (vs.fbo_texture, Some(vs))
        } else {
            let (old_w_f, old_h_f, old_px_f) = apply_fill_mode(
                self.old_w, self.old_h, self.old_pixels.clone(),
                target_w, target_h, self.fill_mode,
            );
            (upload_texture(old_w_f, old_h_f, &old_px_f), None)
        };

        let (tex_new, video_new) = if let Some(ref path) = self.to_video_path {
            let mut vs = VideoSource::new(path, target_w, target_h, self.initial_mute)
                .with_context(|| format!("VideoSource for to-side: {path}"))?;
            if !self.initial_mute {
                vs.set_volume(self.initial_volume);
            }
            vs.prime_first_frame(2000);
            (vs.fbo_texture, Some(vs))
        } else {
            let (new_w_f, new_h_f, new_px_f) = apply_fill_mode(
                self.new_w, self.new_h, self.new_pixels.clone(),
                target_w, target_h, self.fill_mode,
            );
            (upload_texture(new_w_f, new_h_f, &new_px_f), None)
        };
        let tex_thumbs: Vec<u32> = self
            .thumbs
            .iter()
            .map(|(w, h, pixels)| upload_texture(*w, *h, pixels))
            .collect();
        let program = compile_program(&self.shader_src)?;
        let quad_vbo = create_quad_vbo();

        unsafe { gl::UseProgram(program) };
        let loc_progress = unsafe {
            gl::GetUniformLocation(program, b"u_progress\0".as_ptr().cast())
        };
        let loc_tex_old =
            unsafe { gl::GetUniformLocation(program, b"u_tex_old\0".as_ptr().cast()) };
        let loc_tex_new =
            unsafe { gl::GetUniformLocation(program, b"u_tex_new\0".as_ptr().cast()) };
        unsafe {
            gl::Uniform1i(loc_tex_old, 0);
            gl::Uniform1i(loc_tex_new, 1);
            for i in 0..tex_thumbs.len() {
                let name = format!("u_tex_thumb_{i}\0");
                let loc = gl::GetUniformLocation(program, name.as_ptr().cast());
                if loc >= 0 {
                    gl::Uniform1i(loc, (2 + i) as i32);
                }
            }
            let loc_n = gl::GetUniformLocation(program, b"u_thumb_count\0".as_ptr().cast());
            if loc_n >= 0 {
                gl::Uniform1i(loc_n, tex_thumbs.len() as i32);
            }
        }

        let bright_program = compile_program(BRIGHT_EXTRACT_FRAG)?;
        let blur_program = compile_program(GAUSSIAN_BLUR_FRAG)?;
        let composite_program = compile_program(COMPOSITE_BLOOM_FRAG)?;
        let fit_program = compile_program(FIT_FRAG)?;
        let mut fit_vao: u32 = 0;
        unsafe {
            gl::UseProgram(fit_program);
            let loc_tex = gl::GetUniformLocation(fit_program, b"u_tex\0".as_ptr().cast());
            gl::Uniform1i(loc_tex, 0);
            gl::GenVertexArrays(1, &mut fit_vao);
            gl::BindVertexArray(fit_vao);
            gl::BindBuffer(gl::ARRAY_BUFFER, quad_vbo);
            gl::EnableVertexAttribArray(0);
            gl::VertexAttribPointer(0, 2, gl::FLOAT, gl::FALSE, 16, std::ptr::null());
            gl::EnableVertexAttribArray(1);
            gl::VertexAttribPointer(1, 2, gl::FLOAT, gl::FALSE, 16, 8 as *const _);
            gl::BindVertexArray(0);
        }
        let loc_fit_scale = unsafe {
            gl::GetUniformLocation(fit_program, b"u_scale\0".as_ptr().cast())
        };
        let loc_fit_offset = unsafe {
            gl::GetUniformLocation(fit_program, b"u_offset\0".as_ptr().cast())
        };

        unsafe {
            gl::UseProgram(bright_program);
            let loc = gl::GetUniformLocation(bright_program, b"u_tex\0".as_ptr().cast());
            gl::Uniform1i(loc, 0);
            gl::UseProgram(blur_program);
            let loc = gl::GetUniformLocation(blur_program, b"u_tex\0".as_ptr().cast());
            gl::Uniform1i(loc, 0);
            gl::UseProgram(composite_program);
            let loc_base = gl::GetUniformLocation(composite_program, b"u_tex_base\0".as_ptr().cast());
            gl::Uniform1i(loc_base, 0);
            let loc_bloom = gl::GetUniformLocation(composite_program, b"u_tex_bloom\0".as_ptr().cast());
            gl::Uniform1i(loc_bloom, 1);
        }
        let loc_bright_threshold = unsafe {
            gl::GetUniformLocation(bright_program, b"u_threshold\0".as_ptr().cast())
        };
        let loc_blur_dir = unsafe {
            gl::GetUniformLocation(blur_program, b"u_dir\0".as_ptr().cast())
        };
        let loc_blur_radius = unsafe {
            gl::GetUniformLocation(blur_program, b"u_radius\0".as_ptr().cast())
        };
        let loc_composite_strength = unsafe {
            gl::GetUniformLocation(composite_program, b"u_strength\0".as_ptr().cast())
        };

        self.gl_state = Some(GlState {
            egl_display,
            egl_config,
            primary_ctx,
            primary_pbuffer,
            tex_old,
            tex_new,
            video_old,
            video_new,
            tex_old_fbo: None,
            tex_new_fbo: None,
            tex_thumbs,
            program,
            quad_vbo,
            loc_progress,
            _loc_tex_old: loc_tex_old,
            _loc_tex_new: loc_tex_new,
            pipeline: pipeline_for(&self.shader_name),
            bright_program,
            loc_bright_threshold,
            blur_program,
            loc_blur_dir,
            loc_blur_radius,
            composite_program,
            loc_composite_strength,
            fit_program,
            loc_fit_scale,
            loc_fit_offset,
            fit_vao,
        });
        self.old_pixels = Vec::new();
        self.new_pixels = Vec::new();
        self.thumbs = Vec::new();
        unsafe { libc::malloc_trim(0) };
        self.start_time = Some(Instant::now());
        Ok(())
    }

    fn ensure_blitter(&mut self, idx: usize) {
        let Some(gl_state) = self.gl_state.as_ref() else {
            return;
        };
        if self.surfaces[idx].blitter.is_some() {
            return;
        }
        let s = &mut self.surfaces[idx];
        let ctx_attribs = [
            khronos_egl::CONTEXT_MAJOR_VERSION,
            3,
            khronos_egl::CONTEXT_MINOR_VERSION,
            3,
            khronos_egl::NONE,
        ];
        let egl_context = match EGL.create_context(
            gl_state.egl_display,
            gl_state.egl_config,
            Some(gl_state.primary_ctx),
            &ctx_attribs,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("blitter ctx: {e:?}");
                return;
            }
        };
        let wl_egl_surface = match WlEglSurface::new(s.surface.id(), s.width as i32, s.height as i32) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("WlEglSurface: {e}");
                return;
            }
        };
        let egl_surface = match unsafe {
            EGL.create_window_surface(
                gl_state.egl_display,
                gl_state.egl_config,
                wl_egl_surface.ptr() as khronos_egl::NativeWindowType,
                None,
            )
        } {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("eglCreateWindowSurface: {e:?}");
                return;
            }
        };
        if EGL
            .make_current(
                gl_state.egl_display,
                Some(egl_surface),
                Some(egl_surface),
                Some(egl_context),
            )
            .is_err()
        {
            return;
        }
        let mut vao: u32 = 0;
        unsafe {
            gl::GenVertexArrays(1, &mut vao);
            gl::BindVertexArray(vao);
            gl::BindBuffer(gl::ARRAY_BUFFER, gl_state.quad_vbo);
            gl::EnableVertexAttribArray(0);
            gl::EnableVertexAttribArray(1);
            gl::VertexAttribPointer(0, 2, gl::FLOAT, gl::FALSE, 16, std::ptr::null());
            gl::VertexAttribPointer(1, 2, gl::FLOAT, gl::FALSE, 16, 8 as *const _);
            gl::BindVertexArray(0);
            gl::BindBuffer(gl::ARRAY_BUFFER, 0);
        }
        s.blitter = Some(SurfaceBlitter {
            egl_context,
            egl_surface,
            _wl_egl_surface: wl_egl_surface,
            vao,
            width: s.width,
            height: s.height,
            fbo_base: None,
            tex_base: None,
            fbo_a: None,
            tex_a: None,
            fbo_b: None,
            tex_b: None,
        });
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

    fn try_consume_pending_cmd(&mut self) -> Result<()> {
        if !self.persist {
            return Ok(());
        }
        let cmd = match self.pending_cmd.lock().unwrap().take() {
            Some(c) => c,
            None => return Ok(()),
        };
        if cmd.to.is_empty() && (cmd.mute.is_some() || cmd.volume.is_some()) {
            if let Some(gl_state) = self.gl_state.as_mut()
                && let Some(vs) = gl_state.video_new.as_mut()
            {
                if let Some(m) = cmd.mute {
                    vs.set_mute(m);
                }
                if let Some(v) = cmd.volume {
                    vs.set_volume(v);
                }
            }
            tracing::info!(mute = ?cmd.mute, volume = ?cmd.volume, "persist: audio update");
            return Ok(());
        }
        if cmd.to.is_empty() && cmd.warmup.is_some() {
            return Ok(());
        }
        tracing::info!(to = %cmd.to, was_active = self.transition_active, "persist: applying (interrupt-allowed)");
        self.apply_swap(cmd)
    }

    fn apply_swap(&mut self, cmd: PersistCommand) -> Result<()> {
        let Some(gl_state) = self.gl_state.as_mut() else {
            tracing::warn!("persist: pending command but no GL state yet");
            return Ok(());
        };

        let swap_start = Instant::now();
        let (chosen_name, chosen_src) = resolve_shader(&cmd.shader);
        let to_is_video = is_video_path(&cmd.to);
        let needs_thumbs = chosen_name == "mosaic-tumble";

        EGL.make_current(
            gl_state.egl_display,
            Some(gl_state.primary_pbuffer),
            Some(gl_state.primary_pbuffer),
            Some(gl_state.primary_ctx),
        )
        .map_err(|e| anyhow!("eglMakeCurrent (primary): {e:?}"))?;

        let thumbs_t = Instant::now();
        let new_thumbs: Vec<(u32, u32, Vec<u8>)> = if needs_thumbs {
            let thumb_paths: Vec<String> =
                cmd.thumbs.iter().take(MAX_THUMBS).cloned().collect();
            std::thread::scope(|s| {
                let handles: Vec<_> = thumb_paths
                    .iter()
                    .map(|p| {
                        let p = p.clone();
                        s.spawn(move || decode_path_rgba(&p))
                    })
                    .collect();
                handles
                    .into_iter()
                    .filter_map(|h| h.join().ok().and_then(Result::ok))
                    .collect()
            })
        } else {
            Vec::new()
        };
        let thumbs_decode_ms = thumbs_t.elapsed().as_millis() as u64;

        unsafe {
            if let Some(vs) = gl_state.video_old.as_ref() {
                gl::DeleteFramebuffers(1, &vs.fbo);
                gl::DeleteTextures(1, &vs.fbo_texture);
            } else if let Some(fbo) = gl_state.tex_old_fbo {
                gl::DeleteFramebuffers(1, &fbo);
                gl::DeleteTextures(1, &gl_state.tex_old);
            } else {
                gl::DeleteTextures(1, &gl_state.tex_old);
            }
            for t in &gl_state.tex_thumbs {
                gl::DeleteTextures(1, t);
            }
        }
        let _ = gl_state.video_old.take();

        let (target_w, target_h) = self
            .surfaces
            .iter()
            .find(|s| s.width > 0 && s.height > 0)
            .map(|s| (s.width, s.height))
            .unwrap_or((1u32, 1u32));

        gl_state.tex_old = gl_state.tex_new;
        gl_state.tex_old_fbo = gl_state
            .video_new
            .take()
            .map(|vs| vs.fbo)
            .or_else(|| gl_state.tex_new_fbo.take());

        let mpv_t = Instant::now();
        let (mpv_init_ms, mpv_prime_ms) = if to_is_video {
            let mute = cmd.mute.unwrap_or(true);
            let mut vs = VideoSource::new(&cmd.to, target_w, target_h, mute)
                .with_context(|| format!("VideoSource for to-side: {}", cmd.to))?;
            if !mute && let Some(v) = cmd.volume {
                vs.set_volume(v);
            }
            let init_ms = mpv_t.elapsed().as_millis() as u64;
            let prime_t = Instant::now();
            vs.prime_first_frame(2000);
            let prime_ms = prime_t.elapsed().as_millis() as u64;
            gl_state.tex_new = vs.fbo_texture;
            gl_state.tex_new_fbo = None;
            gl_state.video_new = Some(vs);
            (init_ms, prime_ms)
        } else {
            let init_ms = mpv_t.elapsed().as_millis() as u64;
            let prime_t = Instant::now();
            let (img_w, img_h, img_px) = decode_rgba(&cmd.to)
                .with_context(|| format!("image decode: {}", cmd.to))?;
            let native_tex = upload_texture(img_w, img_h, &img_px);
            let (fbo, tex) = fit_texture_to_fbo(
                gl_state, native_tex, img_w, img_h,
                target_w, target_h, self.fill_mode,
            );
            unsafe { gl::DeleteTextures(1, &native_tex) };
            let prime_ms = prime_t.elapsed().as_millis() as u64;
            gl_state.tex_new = tex;
            gl_state.tex_new_fbo = Some(fbo);
            gl_state.video_new = None;
            (init_ms, prime_ms)
        };
        gl_state.tex_thumbs = new_thumbs
            .iter()
            .map(|(w, h, p)| upload_texture(*w, *h, p))
            .collect();

        let shader_changed = chosen_name != self.shader_name.as_str();
        let shader_t = Instant::now();
        if shader_changed {
            unsafe { gl::DeleteProgram(gl_state.program) };
            gl_state.program = compile_program(chosen_src)?;
            unsafe {
                gl::UseProgram(gl_state.program);
                let loc_tex_old = gl::GetUniformLocation(gl_state.program, b"u_tex_old\0".as_ptr().cast());
                let loc_tex_new = gl::GetUniformLocation(gl_state.program, b"u_tex_new\0".as_ptr().cast());
                gl::Uniform1i(loc_tex_old, 0);
                gl::Uniform1i(loc_tex_new, 1);
                for i in 0..gl_state.tex_thumbs.len() {
                    let name = format!("u_tex_thumb_{i}\0");
                    let loc = gl::GetUniformLocation(gl_state.program, name.as_ptr().cast());
                    if loc >= 0 {
                        gl::Uniform1i(loc, (2 + i) as i32);
                    }
                }
                let loc_n = gl::GetUniformLocation(gl_state.program, b"u_thumb_count\0".as_ptr().cast());
                if loc_n >= 0 {
                    gl::Uniform1i(loc_n, gl_state.tex_thumbs.len() as i32);
                }
                gl_state.loc_progress = gl::GetUniformLocation(gl_state.program, b"u_progress\0".as_ptr().cast());
            }
            gl_state.pipeline = pipeline_for(chosen_name);
            self.shader_src = chosen_src.to_string();
            self.shader_name = chosen_name.to_string();
        } else if !gl_state.tex_thumbs.is_empty() {
            unsafe {
                gl::UseProgram(gl_state.program);
                for i in 0..gl_state.tex_thumbs.len() {
                    let name = format!("u_tex_thumb_{i}\0");
                    let loc = gl::GetUniformLocation(gl_state.program, name.as_ptr().cast());
                    if loc >= 0 {
                        gl::Uniform1i(loc, (2 + i) as i32);
                    }
                }
                let loc_n = gl::GetUniformLocation(gl_state.program, b"u_thumb_count\0".as_ptr().cast());
                if loc_n >= 0 {
                    gl::Uniform1i(loc_n, gl_state.tex_thumbs.len() as i32);
                }
            }
        }

        let shader_compile_ms = if shader_changed { shader_t.elapsed().as_millis() as u64 } else { 0 };
        self.duration_ms = cmd.duration_ms.max(1);
        self.start_time = Some(Instant::now());
        self.transition_active = true;
        tracing::info!(
            shader = %self.shader_name,
            target_kind = if to_is_video { "video" } else { "image" },
            duration_ms = self.duration_ms,
            mpv_init_ms,
            mpv_prime_ms,
            thumbs_decode_ms,
            shader_compile_ms,
            swap_ms = swap_start.elapsed().as_millis() as u64,
            "persist: started new transition"
        );
        unsafe { libc::malloc_trim(0) };
        Ok(())
    }

    fn render_frame(&mut self, idx: usize) {
        let progress_now = self
            .start_time
            .map(|s| (s.elapsed().as_millis() as f32 / self.duration_ms.max(1) as f32).clamp(0.0, 1.0))
            .unwrap_or(0.0);
        let transition_completing = self.transition_active && progress_now >= 1.0;

        if let Some(gl_state) = self.gl_state.as_mut() {
            let needs_primary = transition_completing
                || gl_state.video_old.is_some()
                || gl_state.video_new.is_some();
            if needs_primary {
                let _ = EGL.make_current(
                    gl_state.egl_display,
                    Some(gl_state.primary_pbuffer),
                    Some(gl_state.primary_pbuffer),
                    Some(gl_state.primary_ctx),
                );
                if transition_completing
                    && let Some(vs) = gl_state.video_new.as_mut()
                {
                    vs.set_pause(false);
                }
                if let Some(vs) = gl_state.video_old.as_mut() {
                    vs.render_to_fbo();
                }
                if let Some(vs) = gl_state.video_new.as_mut() {
                    vs.render_to_fbo();
                }
            }
        }
        let is_bloom = matches!(
            self.gl_state.as_ref().map(|g| &g.pipeline),
            Some(Pipeline::Bloom { .. })
        );
        if is_bloom {
            let s = &mut self.surfaces[idx];
            if let Some(blitter) = s.blitter.as_mut()
                && blitter.fbo_base.is_none()
                && let Some(gl_state) = self.gl_state.as_ref()
                && EGL
                    .make_current(
                        gl_state.egl_display,
                        Some(blitter.egl_surface),
                        Some(blitter.egl_surface),
                        Some(blitter.egl_context),
                    )
                    .is_ok()
            {
                let (f, t) = create_color_fbo(blitter.width, blitter.height);
                blitter.fbo_base = Some(f);
                blitter.tex_base = Some(t);
                let (f, t) = create_color_fbo(blitter.width, blitter.height);
                blitter.fbo_a = Some(f);
                blitter.tex_a = Some(t);
                let (f, t) = create_color_fbo(blitter.width, blitter.height);
                blitter.fbo_b = Some(f);
                blitter.tex_b = Some(t);
            }
        }

        let Some(gl_state) = self.gl_state.as_ref() else {
            return;
        };
        let Some(start) = self.start_time else { return };
        let elapsed = start.elapsed().as_millis() as f32;
        let progress = (elapsed / self.duration_ms as f32).clamp(0.0, 1.0);
        let s = &self.surfaces[idx];
        let Some(blitter) = s.blitter.as_ref() else {
            return;
        };
        if EGL
            .make_current(
                gl_state.egl_display,
                Some(blitter.egl_surface),
                Some(blitter.egl_surface),
                Some(blitter.egl_context),
            )
            .is_err()
        {
            return;
        }
        match gl_state.pipeline {
            Pipeline::Single => {
                unsafe {
                    gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
                    gl::Viewport(0, 0, blitter.width as i32, blitter.height as i32);
                    gl::ClearColor(0.0, 0.0, 0.0, 1.0);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::UseProgram(gl_state.program);
                    gl::Uniform1f(gl_state.loc_progress, progress);
                    gl::ActiveTexture(gl::TEXTURE0);
                    gl::BindTexture(gl::TEXTURE_2D, gl_state.tex_old);
                    gl::ActiveTexture(gl::TEXTURE1);
                    gl::BindTexture(gl::TEXTURE_2D, gl_state.tex_new);
                    for (i, t) in gl_state.tex_thumbs.iter().enumerate() {
                        gl::ActiveTexture(gl::TEXTURE2 + i as u32);
                        gl::BindTexture(gl::TEXTURE_2D, *t);
                    }
                    gl::BindVertexArray(blitter.vao);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);
                    gl::BindVertexArray(0);
                }
            }
            Pipeline::Bloom { strength, threshold, radius } => {
                let (Some(fbo_base), Some(tex_base), Some(fbo_a), Some(tex_a), Some(fbo_b), Some(tex_b)) =
                    (blitter.fbo_base, blitter.tex_base, blitter.fbo_a, blitter.tex_a, blitter.fbo_b, blitter.tex_b)
                else {
                    return;
                };
                unsafe {
                    gl::Viewport(0, 0, blitter.width as i32, blitter.height as i32);

                    gl::BindFramebuffer(gl::FRAMEBUFFER, fbo_base);
                    gl::ClearColor(0.0, 0.0, 0.0, 1.0);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::UseProgram(gl_state.program);
                    gl::Uniform1f(gl_state.loc_progress, progress);
                    gl::ActiveTexture(gl::TEXTURE0);
                    gl::BindTexture(gl::TEXTURE_2D, gl_state.tex_old);
                    gl::ActiveTexture(gl::TEXTURE1);
                    gl::BindTexture(gl::TEXTURE_2D, gl_state.tex_new);
                    for (i, t) in gl_state.tex_thumbs.iter().enumerate() {
                        gl::ActiveTexture(gl::TEXTURE2 + i as u32);
                        gl::BindTexture(gl::TEXTURE_2D, *t);
                    }
                    gl::BindVertexArray(blitter.vao);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

                    gl::BindFramebuffer(gl::FRAMEBUFFER, fbo_a);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::UseProgram(gl_state.bright_program);
                    gl::Uniform1f(gl_state.loc_bright_threshold, threshold);
                    gl::ActiveTexture(gl::TEXTURE0);
                    gl::BindTexture(gl::TEXTURE_2D, tex_base);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

                    gl::UseProgram(gl_state.blur_program);
                    gl::Uniform1f(gl_state.loc_blur_radius, radius);

                    gl::BindFramebuffer(gl::FRAMEBUFFER, fbo_b);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::Uniform2f(
                        gl_state.loc_blur_dir,
                        1.0 / blitter.width as f32,
                        0.0,
                    );
                    gl::ActiveTexture(gl::TEXTURE0);
                    gl::BindTexture(gl::TEXTURE_2D, tex_a);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

                    gl::BindFramebuffer(gl::FRAMEBUFFER, fbo_a);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::Uniform2f(
                        gl_state.loc_blur_dir,
                        0.0,
                        1.0 / blitter.height as f32,
                    );
                    gl::BindTexture(gl::TEXTURE_2D, tex_b);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

                    gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
                    gl::Clear(gl::COLOR_BUFFER_BIT);
                    gl::UseProgram(gl_state.composite_program);
                    gl::Uniform1f(gl_state.loc_composite_strength, strength);
                    gl::ActiveTexture(gl::TEXTURE0);
                    gl::BindTexture(gl::TEXTURE_2D, tex_base);
                    gl::ActiveTexture(gl::TEXTURE1);
                    gl::BindTexture(gl::TEXTURE_2D, tex_a);
                    gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

                    gl::BindVertexArray(0);
                }
            }
        }
        let _ = EGL.swap_buffers(gl_state.egl_display, blitter.egl_surface);
        if !self.ready_signaled {
            crate::ipc::signal_ready();
            self.ready_signaled = true;
        }
        if progress >= 1.0 {
            self.transition_active = false;
            if !self.persist && !self.exit_scheduled {
                self.exit_scheduled = true;
                std::thread::spawn(|| {
                    std::thread::sleep(std::time::Duration::from_millis(150));
                    std::process::exit(0);
                });
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
        let idx = match self.surfaces.iter().position(|s| &s.surface == surface) {
            Some(i) => i,
            None => return,
        };
        self.surfaces[idx].frame_pending = false;
        if let Err(e) = self.try_consume_pending_cmd() {
            tracing::error!(error = %e, "persist: command apply failed");
        }
        self.render_frame(idx);
        if self.transition_active || self.persist {
            for i in 0..self.surfaces.len() {
                self.schedule_frame(i);
            }
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
                if state.surfaces[idx].width == 0 || state.surfaces[idx].height == 0 {
                    return;
                }
                if state.gl_state.is_none()
                    && let Err(e) = state.ensure_gl()
                {
                    tracing::error!(error = %e, "ensure_gl failed");
                    return;
                }
                state.ensure_blitter(idx);
                state.render_frame(idx);
                state.schedule_frame(idx);
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

fn decode_rgba(path: &str) -> Result<(u32, u32, Vec<u8>)> {
    let img = image::ImageReader::open(path)
        .with_context(|| format!("opening image: {path}"))?
        .with_guessed_format()?
        .decode()
        .with_context(|| format!("decoding image: {path}"))?
        .to_rgba8();
    let (w, h) = img.dimensions();
    Ok((w, h, img.into_raw()))
}

fn is_video_path(p: &str) -> bool {
    let lower = p.to_lowercase();
    [".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".flv", ".wmv"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}

fn decode_video_first_frame_rgba(path: &str) -> Result<(u32, u32, Vec<u8>)> {
    let output = std::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-hwaccel", "auto",
            "-loglevel", "error",
            "-an",
            "-i", path,
            "-vframes", "1",
            "-f", "image2pipe",
            "-c:v", "ppm",
            "pipe:1",
        ])
        .output()
        .with_context(|| format!("ffmpeg first-frame extract: {path}"))?;
    if !output.status.success() {
        return Err(anyhow!(
            "ffmpeg first-frame failed for {path}: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let img = image::load_from_memory(&output.stdout)
        .with_context(|| format!("decoding first-frame for {path}"))?
        .to_rgba8();
    let (w, h) = img.dimensions();
    Ok((w, h, img.into_raw()))
}

fn decode_path_rgba(path: &str) -> Result<(u32, u32, Vec<u8>)> {
    if is_video_path(path) {
        decode_video_first_frame_rgba(path)
    } else {
        decode_rgba(path)
    }
}

fn placeholder_pixels() -> (u32, u32, Vec<u8>) {
    (1, 1, vec![0, 0, 0, 255])
}

fn create_color_fbo(w: u32, h: u32) -> (u32, u32) {
    unsafe {
        let mut tex: u32 = 0;
        gl::GenTextures(1, &mut tex);
        gl::BindTexture(gl::TEXTURE_2D, tex);
        gl::TexImage2D(
            gl::TEXTURE_2D,
            0,
            gl::RGBA8 as i32,
            w as i32,
            h as i32,
            0,
            gl::RGBA,
            gl::UNSIGNED_BYTE,
            std::ptr::null(),
        );
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MIN_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MAG_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_S, gl::CLAMP_TO_EDGE as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_T, gl::CLAMP_TO_EDGE as i32);
        let mut fbo: u32 = 0;
        gl::GenFramebuffers(1, &mut fbo);
        gl::BindFramebuffer(gl::FRAMEBUFFER, fbo);
        gl::FramebufferTexture2D(gl::FRAMEBUFFER, gl::COLOR_ATTACHMENT0, gl::TEXTURE_2D, tex, 0);
        gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
        gl::BindTexture(gl::TEXTURE_2D, 0);
        (fbo, tex)
    }
}

const BRIGHT_EXTRACT_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex;
uniform float u_threshold;
void main() {
    vec3 c = texture(u_tex, v_uv).rgb;
    float lum = dot(c, vec3(0.299, 0.587, 0.114));
    float b = max(0.0, lum - u_threshold) / max(1.0 - u_threshold, 0.001);
    frag = vec4(c * b, 1.0);
}
";

const GAUSSIAN_BLUR_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex;
uniform vec2 u_dir;
uniform float u_radius;
void main() {
    vec3 acc = vec3(0.0);
    float w_sum = 0.0;
    float sigma = max(u_radius * 0.5, 0.001);
    int kr = int(min(u_radius * 2.0, 16.0));
    for (int i = -16; i <= 16; i++) {
        if (abs(i) > kr) continue;
        float fi = float(i);
        float w = exp(-(fi * fi) / (2.0 * sigma * sigma));
        acc += texture(u_tex, v_uv + u_dir * fi).rgb * w;
        w_sum += w;
    }
    frag = vec4(acc / max(w_sum, 0.001), 1.0);
}
";

const COMPOSITE_BLOOM_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_base;
uniform sampler2D u_tex_bloom;
uniform float u_strength;
void main() {
    vec3 base = texture(u_tex_base, v_uv).rgb;
    vec3 bloom = texture(u_tex_bloom, v_uv).rgb;
    vec3 result = base + bloom * u_strength;
    result = result / (result + vec3(0.5));
    result = pow(result, vec3(0.85));
    frag = vec4(result, 1.0);
}
";

const FIT_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex;
uniform vec2 u_scale;
uniform vec2 u_offset;
void main() {
    // Flip Y because rendering through an intermediate FBO inverts the texture
    // memory layout relative to a direct upload that downstream shaders expect.
    vec2 uv = vec2(v_uv.x, 1.0 - v_uv.y);
    vec2 src_uv = uv * u_scale + u_offset;
    if (src_uv.x < 0.0 || src_uv.x > 1.0 || src_uv.y < 0.0 || src_uv.y > 1.0) {
        frag = vec4(0.0, 0.0, 0.0, 1.0);
    } else {
        frag = texture(u_tex, src_uv);
    }
}
";

fn fit_uv_remap(
    src_w: u32,
    src_h: u32,
    target_w: u32,
    target_h: u32,
    fill_mode: FillMode,
) -> ([f32; 2], [f32; 2]) {
    let sw = src_w.max(1) as f32;
    let sh = src_h.max(1) as f32;
    let tw = target_w.max(1) as f32;
    let th = target_h.max(1) as f32;
    let s_aspect = sw / sh;
    let t_aspect = tw / th;
    match fill_mode {
        FillMode::Stretch => ([1.0, 1.0], [0.0, 0.0]),
        FillMode::Fill => {
            if s_aspect > t_aspect {
                let s = t_aspect / s_aspect;
                ([s, 1.0], [(1.0 - s) * 0.5, 0.0])
            } else {
                let s = s_aspect / t_aspect;
                ([1.0, s], [0.0, (1.0 - s) * 0.5])
            }
        }
        FillMode::Fit => {
            if s_aspect > t_aspect {
                let s = s_aspect / t_aspect;
                ([1.0, s], [0.0, (1.0 - s) * 0.5])
            } else {
                let s = t_aspect / s_aspect;
                ([s, 1.0], [(1.0 - s) * 0.5, 0.0])
            }
        }
        FillMode::Center => {
            let sx = tw / sw;
            let sy = th / sh;
            ([sx, sy], [(1.0 - sx) * 0.5, (1.0 - sy) * 0.5])
        }
        FillMode::Tile => ([tw / sw, th / sh], [0.0, 0.0]),
    }
}

fn fit_texture_to_fbo(
    gl_state: &GlState,
    src_tex: u32,
    src_w: u32,
    src_h: u32,
    target_w: u32,
    target_h: u32,
    fill_mode: FillMode,
) -> (u32, u32) {
    let (scale, offset) = fit_uv_remap(src_w, src_h, target_w, target_h, fill_mode);
    let (fbo, tex) = create_color_fbo(target_w, target_h);
    unsafe {
        let mut prev_fbo: i32 = 0;
        gl::GetIntegerv(gl::DRAW_FRAMEBUFFER_BINDING, &mut prev_fbo);
        let mut prev_vp: [i32; 4] = [0; 4];
        gl::GetIntegerv(gl::VIEWPORT, prev_vp.as_mut_ptr());
        let mut prev_vao: i32 = 0;
        gl::GetIntegerv(gl::VERTEX_ARRAY_BINDING, &mut prev_vao);

        gl::BindFramebuffer(gl::FRAMEBUFFER, fbo);
        gl::Viewport(0, 0, target_w as i32, target_h as i32);
        gl::ClearColor(0.0, 0.0, 0.0, 1.0);
        gl::Clear(gl::COLOR_BUFFER_BIT);
        gl::UseProgram(gl_state.fit_program);
        gl::Uniform2f(gl_state.loc_fit_scale, scale[0], scale[1]);
        gl::Uniform2f(gl_state.loc_fit_offset, offset[0], offset[1]);
        gl::ActiveTexture(gl::TEXTURE0);
        gl::BindTexture(gl::TEXTURE_2D, src_tex);
        if fill_mode == FillMode::Tile {
            gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_S, gl::REPEAT as i32);
            gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_T, gl::REPEAT as i32);
        }
        gl::BindVertexArray(gl_state.fit_vao);
        gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);
        gl::BindVertexArray(prev_vao as u32);

        gl::BindFramebuffer(gl::FRAMEBUFFER, prev_fbo as u32);
        gl::Viewport(prev_vp[0], prev_vp[1], prev_vp[2], prev_vp[3]);
    }
    (fbo, tex)
}

fn upload_texture(w: u32, h: u32, pixels: &[u8]) -> u32 {
    unsafe {
        let mut tex: u32 = 0;
        gl::GenTextures(1, &mut tex);
        gl::BindTexture(gl::TEXTURE_2D, tex);
        gl::TexImage2D(
            gl::TEXTURE_2D,
            0,
            gl::RGBA8 as i32,
            w as i32,
            h as i32,
            0,
            gl::RGBA,
            gl::UNSIGNED_BYTE,
            pixels.as_ptr().cast(),
        );
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MIN_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MAG_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_S, gl::CLAMP_TO_EDGE as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_T, gl::CLAMP_TO_EDGE as i32);
        gl::BindTexture(gl::TEXTURE_2D, 0);
        tex
    }
}

const VERT_SRC: &[u8] = b"#version 330 core\nlayout(location=0) in vec2 a_pos;\nlayout(location=1) in vec2 a_tex;\nout vec2 v_uv;\nvoid main() { gl_Position = vec4(a_pos, 0.0, 1.0); v_uv = a_tex; }\n\0";

fn compile_program(frag_src: &str) -> Result<u32> {
    let frag_c = CString::new(frag_src).map_err(|e| anyhow!("frag NUL: {e}"))?;
    unsafe {
        let v = compile_shader(gl::VERTEX_SHADER, VERT_SRC)?;
        let f = compile_shader(gl::FRAGMENT_SHADER, frag_c.as_bytes_with_nul())?;
        let p = gl::CreateProgram();
        gl::AttachShader(p, v);
        gl::AttachShader(p, f);
        gl::LinkProgram(p);
        let mut ok: i32 = 0;
        gl::GetProgramiv(p, gl::LINK_STATUS, &mut ok);
        gl::DeleteShader(v);
        gl::DeleteShader(f);
        if ok == 0 {
            return Err(anyhow!("program link failed"));
        }
        Ok(p)
    }
}

unsafe fn compile_shader(kind: u32, src: &[u8]) -> Result<u32> {
    unsafe {
        let s = gl::CreateShader(kind);
        let ptr = src.as_ptr().cast();
        let len = (src.len() - 1) as i32;
        gl::ShaderSource(s, 1, &ptr, &len);
        gl::CompileShader(s);
        let mut ok: i32 = 0;
        gl::GetShaderiv(s, gl::COMPILE_STATUS, &mut ok);
        if ok == 0 {
            let mut log = [0u8; 1024];
            let mut len: i32 = 0;
            gl::GetShaderInfoLog(s, log.len() as i32, &mut len, log.as_mut_ptr().cast());
            let msg = std::str::from_utf8(&log[..len as usize]).unwrap_or("?");
            tracing::error!("shader compile failed: {msg}");
            gl::DeleteShader(s);
            return Err(anyhow!("shader compile failed (kind={kind})"));
        }
        Ok(s)
    }
}

fn create_quad_vbo() -> u32 {
    let verts: [f32; 16] = [
        -1.0, -1.0, 0.0, 1.0,
         1.0, -1.0, 1.0, 1.0,
        -1.0,  1.0, 0.0, 0.0,
         1.0,  1.0, 1.0, 0.0,
    ];
    unsafe {
        let mut vbo: u32 = 0;
        gl::GenBuffers(1, &mut vbo);
        gl::BindBuffer(gl::ARRAY_BUFFER, vbo);
        gl::BufferData(
            gl::ARRAY_BUFFER,
            (verts.len() * std::mem::size_of::<f32>()) as isize,
            verts.as_ptr().cast(),
            gl::STATIC_DRAW,
        );
        gl::BindBuffer(gl::ARRAY_BUFFER, 0);
        vbo
    }
}

fn wayland_display_ptr(surface: &WlSurface) -> Result<*mut c_void> {
    let conn = surface
        .backend()
        .upgrade()
        .ok_or_else(|| anyhow!("wayland backend gone"))?;
    Ok(conn.display_ptr() as *mut c_void)
}

const PIXELATE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    float bump = 1.0 - abs(u_progress - 0.5) * 2.0;
    float blocks = mix(800.0, 12.0, bump);
    vec2 q = floor(v_uv * blocks) / blocks + 0.5 / blocks;
    vec4 a = texture(u_tex_old, q);
    vec4 b = texture(u_tex_new, q);
    frag = mix(a, b, smoothstep(0.0, 1.0, u_progress));
}
";

const IRIS_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    vec2 c = v_uv - vec2(0.5);
    c.x *= 1.7777;
    float d = length(c);
    float r = u_progress * 1.2;
    float feather = 0.05;
    float t = smoothstep(r + feather, r - feather, d);
    float edge = exp(-abs(d - r) * 100.0);
    vec3 chrom = vec3(
        texture(u_tex_new, v_uv + vec2(0.012, 0.0) * edge).r,
        texture(u_tex_new, v_uv).g,
        texture(u_tex_new, v_uv - vec2(0.012, 0.0) * edge).b
    );
    vec4 a = texture(u_tex_old, v_uv);
    frag = mix(a, vec4(chrom, 1.0), t);
}
";

const LIQUID_RIPPLE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float amplitude = 100.0;
const float speed = 50.0;
void main() {
    vec2 dir = v_uv - vec2(0.5);
    float dist = length(dir);
    vec2 offset = dir * (sin(u_progress * dist * amplitude - u_progress * speed) + 0.5) / 30.0 * u_progress;
    frag = mix(
        texture(u_tex_old, v_uv + offset),
        texture(u_tex_new, v_uv),
        smoothstep(0.2, 1.0, u_progress)
    );
}
";

const WAVE_WARP_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float smoothness = 0.5;
const vec2 direction = vec2(1.0, 0.0);
const vec2 center = vec2(0.5, 0.5);
void main() {
    vec2 v = normalize(direction);
    v /= abs(v.x) + abs(v.y);
    float d = v.x * center.x + v.y * center.y;
    float m = 1.0 - smoothstep(-smoothness, 0.0, v.x * v_uv.x + v.y * v_uv.y - (d - 0.5 + u_progress * (1.0 + smoothness)));
    frag = mix(
        texture(u_tex_old, (v_uv - 0.5) * (1.0 - m) + 0.5),
        texture(u_tex_new, (v_uv - 0.5) * m + 0.5),
        m
    );
}
";

const GLITCH_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    vec2 p = v_uv;
    vec2 block = floor(p.xy / vec2(16.0));
    vec2 uv_noise = block / vec2(64.0);
    uv_noise += floor(vec2(u_progress) * vec2(1200.0, 3500.0)) / vec2(64.0);
    vec2 dist = u_progress > 0.0 ? (fract(uv_noise) - 0.5) * 0.3 * (1.0 - u_progress) : vec2(0.0);
    vec2 red = p + dist * 0.2;
    vec2 green = p + dist * 0.3;
    vec2 blue = p + dist * 0.5;
    frag = vec4(
        mix(texture(u_tex_old, red), texture(u_tex_new, red), u_progress).r,
        mix(texture(u_tex_old, green), texture(u_tex_new, green), u_progress).g,
        mix(texture(u_tex_old, blue), texture(u_tex_new, blue), u_progress).b,
        1.0
    );
}
";

const VORONOI_SHATTER_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
vec2 hash2(vec2 p) {
    return fract(sin(vec2(dot(p, vec2(127.1, 311.7)),
                          dot(p, vec2(269.5, 183.3)))) * 43758.5453);
}
void main() {
    float scale = 14.0;
    vec2 p = v_uv * scale;
    vec2 g = floor(p);
    vec2 f = fract(p);
    float min_d = 100.0;
    vec2 cell = g;
    for (int y = -1; y <= 1; y++) {
        for (int x = -1; x <= 1; x++) {
            vec2 nb = vec2(float(x), float(y));
            vec2 q = nb + hash2(g + nb) - f;
            float d = dot(q, q);
            if (d < min_d) { min_d = d; cell = g + nb; }
        }
    }
    vec2 dir = normalize(hash2(cell) - 0.5 + vec2(0.0001));
    float seed = hash2(cell).x;
    float shard_p = smoothstep(seed * 0.5, seed * 0.5 + 0.5, u_progress);
    vec2 displaced = v_uv - dir * shard_p * 1.5;
    vec4 a = texture(u_tex_old, displaced);
    vec4 b = texture(u_tex_new, v_uv);
    frag = mix(a, b, smoothstep(0.0, 0.5, shard_p));
}
";

const HEAT_MELT_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const bool direction_v = true;
const float l_threshold = 0.65;
const bool above_v = false;
float rand(vec2 co) { return fract(sin(dot(co.xy, vec2(12.9898, 78.233))) * 43758.5453); }
vec3 mod289v3(vec3 x) { return x - floor(x * (1.0 / 289.0)) * 289.0; }
vec2 mod289v2(vec2 x) { return x - floor(x * (1.0 / 289.0)) * 289.0; }
vec3 permute(vec3 x) { return mod289v3(((x * 34.0) + 1.0) * x); }
float snoise(vec2 v) {
    const vec4 C = vec4(0.211324865405187, 0.366025403784439, -0.577350269189626, 0.024390243902439);
    vec2 i = floor(v + dot(v, C.yy));
    vec2 x0 = v - i + dot(i, C.xx);
    vec2 i1 = (x0.x > x0.y) ? vec2(1.0, 0.0) : vec2(0.0, 1.0);
    vec4 x12 = x0.xyxy + C.xxzz;
    x12.xy -= i1;
    i = mod289v2(i);
    vec3 p = permute(permute(i.y + vec3(0.0, i1.y, 1.0)) + i.x + vec3(0.0, i1.x, 1.0));
    vec3 m = max(0.5 - vec3(dot(x0, x0), dot(x12.xy, x12.xy), dot(x12.zw, x12.zw)), 0.0);
    m = m * m;
    m = m * m;
    vec3 x = 2.0 * fract(p * C.www) - 1.0;
    vec3 h = abs(x) - 0.5;
    vec3 ox = floor(x + 0.5);
    vec3 a0 = x - ox;
    m *= 1.79284291400159 - 0.85373472095314 * (a0 * a0 + h * h);
    vec3 g;
    g.x = a0.x * x0.x + h.x * x0.y;
    g.yz = a0.yz * x12.xz + h.yz * x12.yw;
    return 130.0 * dot(m, g);
}
float luminance(vec4 color) { return color.r * 0.299 + color.g * 0.587 + color.b * 0.114; }
void main() {
    vec2 center = vec2(1.0, direction_v ? 1.0 : 0.0);
    vec2 p = v_uv;
    if (u_progress == 0.0) { frag = texture(u_tex_old, p); return; }
    if (u_progress == 1.0) { frag = texture(u_tex_new, p); return; }
    float x = u_progress;
    float dist = distance(center, p) - u_progress * exp(snoise(vec2(p.x, 0.0)));
    float r = x - rand(vec2(p.x, 0.1));
    float m;
    if (above_v) {
        m = (dist <= r && luminance(texture(u_tex_old, p)) > l_threshold) ? 1.0 : (u_progress * u_progress * u_progress);
    } else {
        m = (dist <= r && luminance(texture(u_tex_old, p)) < l_threshold) ? 1.0 : (u_progress * u_progress * u_progress);
    }
    frag = mix(texture(u_tex_old, p), texture(u_tex_new, p), m);
}
";

const PLASMA_FLOW_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
float hash(vec2 p) {
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}
float noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    f = f * f * (3.0 - 2.0 * f);
    return mix(mix(hash(i), hash(i + vec2(1.0, 0.0)), f.x),
               mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), f.x), f.y);
}
void main() {
    float p = u_progress;
    vec2 flow = vec2(
        noise(v_uv * 5.0 + vec2(p * 2.0, 0.0)),
        noise(v_uv * 5.0 + vec2(0.0, p * 2.0))
    ) - 0.5;
    float intensity = sin(p * 3.14159) * 0.18;
    vec2 distorted = v_uv + flow * intensity;
    vec4 a = texture(u_tex_old, distorted);
    vec4 b = texture(u_tex_new, distorted);
    frag = mix(a, b, smoothstep(0.2, 0.8, p));
}
";

const INK_SPLASH_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
float hash(vec2 p) {
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}
float noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    f = f * f * (3.0 - 2.0 * f);
    return mix(mix(hash(i), hash(i + vec2(1.0, 0.0)), f.x),
               mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), f.x), f.y);
}
float fbm(vec2 p) {
    float v = 0.0;
    float amp = 0.5;
    for (int i = 0; i < 5; i++) {
        v += amp * noise(p);
        p *= 2.1;
        amp *= 0.5;
    }
    return v;
}
void main() {
    float p = u_progress;
    float blob = fbm(v_uv * 3.5);
    float fingers = fbm(v_uv * 14.0);
    float distortion = (blob - 0.5) * 0.5 + (fingers - 0.5) * 0.18;
    vec2 c = v_uv - vec2(0.5);
    c.x *= 1.7777;
    float d = length(c);
    float splash_d = d + distortion;
    float boundary = p * 1.7 - 0.15;
    float diff = splash_d - boundary;
    float reveal = smoothstep(0.04, -0.04, diff);
    float edge_outer = smoothstep(0.16, 0.02, diff);
    float edge_inner = smoothstep(0.02, -0.04, diff);
    float edge = edge_outer * (1.0 - edge_inner);
    vec4 a = texture(u_tex_old, v_uv);
    vec4 b = texture(u_tex_new, v_uv);
    vec4 mixed = mix(a, b, reveal);
    vec3 ink = vec3(0.03, 0.01, 0.06);
    mixed.rgb = mix(mixed.rgb, ink, edge * 0.95);
    float fingers_pre = smoothstep(0.25, 0.05, diff) * (1.0 - reveal);
    mixed.rgb = mix(mixed.rgb, ink, fingers_pre * fingers * 0.4);
    frag = mixed;
}
";

const SMOKE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
float hash(vec2 p) {
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}
float noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    f = f * f * (3.0 - 2.0 * f);
    return mix(mix(hash(i), hash(i + vec2(1.0, 0.0)), f.x),
               mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), f.x), f.y);
}
float fbm(vec2 p) {
    float v = 0.0;
    float amp = 0.5;
    for (int i = 0; i < 6; i++) {
        v += amp * noise(p);
        p *= 2.0;
        amp *= 0.5;
    }
    return v;
}
float warpedFbm(vec2 p, float t) {
    vec2 q = vec2(fbm(p), fbm(p + vec2(5.2, 1.3)));
    vec2 r = vec2(fbm(p + 6.0 * q + vec2(1.7, 9.2) + 0.25 * t),
                  fbm(p + 6.0 * q + vec2(8.3, 2.8) + 0.22 * t));
    vec2 s = vec2(fbm(p + 5.0 * r + vec2(3.1, 7.4) + 0.18 * t),
                  fbm(p + 5.0 * r + vec2(6.7, 0.9) + 0.20 * t));
    return fbm(p + 6.0 * s);
}
void main() {
    float p = u_progress;
    vec2 uv = v_uv;
    float t = p * 12.0;
    float fluid = warpedFbm(uv * 2.0, t);
    vec2 center = uv - 0.5;
    float dist = length(center * vec2(1.0, 0.7));
    float visibility = (1.0 - dist) * 1.2 + fluid * 0.7;
    float reveal_progress = p * 2.5 - 0.4;
    float reveal_mask = smoothstep(visibility - 0.4, visibility + 0.4, reveal_progress);
    float distort_strength = sin(p * 3.14159) * 0.35;
    vec2 wq = vec2(fbm(uv * 2.0 + vec2(0.0, t * 0.2)),
                   fbm(uv * 2.0 + vec2(5.2, t * 0.2)));
    vec2 wr = vec2(fbm(uv * 2.0 + 4.0 * wq + vec2(1.7, 9.2)),
                   fbm(uv * 2.0 + 4.0 * wq + vec2(8.3, 2.8)));
    vec2 warped_uv = uv + (wr - 0.5) * distort_strength;
    vec4 a = texture(u_tex_old, warped_uv);
    vec4 b = texture(u_tex_new, warped_uv);
    frag = mix(a, b, reveal_mask);
}
";

const CHROMATIC_BLOOM_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    float p = u_progress;
    float intensity = sin(p * 3.14159);
    vec2 c = v_uv - vec2(0.5);
    vec2 dir = c * intensity * 0.15;
    vec3 oldc = vec3(
        texture(u_tex_old, v_uv + dir).r,
        texture(u_tex_old, v_uv).g,
        texture(u_tex_old, v_uv - dir).b
    );
    vec3 newc = vec3(
        texture(u_tex_new, v_uv + dir).r,
        texture(u_tex_new, v_uv).g,
        texture(u_tex_new, v_uv - dir).b
    );
    vec3 mixed = mix(oldc, newc, smoothstep(0.4, 0.6, p));
    vec3 bloom = vec3(0.0);
    for (int i = 1; i <= 4; i++) {
        float r = float(i) * 0.01 * intensity;
        bloom += texture(u_tex_new, v_uv + vec2(r, 0.0)).rgb;
        bloom += texture(u_tex_new, v_uv - vec2(r, 0.0)).rgb;
        bloom += texture(u_tex_new, v_uv + vec2(0.0, r)).rgb;
        bloom += texture(u_tex_new, v_uv - vec2(0.0, r)).rgb;
    }
    bloom /= 16.0;
    mixed = mix(mixed, mixed + bloom * 0.5, intensity);
    frag = vec4(mixed, 1.0);
}
";

const INKWELL_DROP_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    float p = u_progress;
    vec2 impact = vec2(0.35, 0.4);
    vec2 c = v_uv - impact;
    c.x *= 1.7777;
    float d = length(c);
    float front = p * 1.5;
    float ring1 = sin((d - front) * 80.0) * exp(-abs(d - front) * 6.0);
    float ring2 = sin((d - front + 0.08) * 80.0) * exp(-abs(d - front + 0.08) * 8.0) * 0.6;
    float ring3 = sin((d - front + 0.16) * 80.0) * exp(-abs(d - front + 0.16) * 10.0) * 0.4;
    float ripple = (ring1 + ring2 + ring3) * 0.05 * (1.0 - p);
    vec2 dir = (d > 0.001) ? normalize(c) : vec2(0.0);
    vec2 distorted = v_uv + dir * ripple;
    vec4 a = texture(u_tex_old, distorted);
    vec4 b = texture(u_tex_new, distorted);
    float reveal = smoothstep(0.05, -0.02, d - front);
    vec4 mixed = mix(a, b, reveal);
    float crest = exp(-abs(d - front) * 25.0) * (1.0 - p);
    mixed.rgb += vec3(0.6, 0.75, 0.95) * crest * 0.5;
    frag = mixed;
}
";

const PIXELFADE_WAVE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    float p = u_progress;
    float wave_x = (v_uv.x + v_uv.y) * 0.5;
    float wave_p = smoothstep(0.0, 1.0, p * 1.6 - wave_x * 0.6);
    float bump = sin(wave_p * 3.14159);
    float blocks = mix(800.0, 8.0, bump);
    vec2 q = floor(v_uv * blocks) / blocks + 0.5 / blocks;
    vec4 a = texture(u_tex_old, q);
    vec4 b = texture(u_tex_new, q);
    frag = mix(a, b, smoothstep(0.0, 1.0, wave_p));
}
";

const SOFT_WARP_FADE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
float hash(vec2 p) {
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}
float noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    f = f * f * (3.0 - 2.0 * f);
    return mix(mix(hash(i), hash(i + vec2(1.0, 0.0)), f.x),
               mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), f.x), f.y);
}
void main() {
    float p = u_progress;
    float strength = sin(p * 3.14159) * 0.025;
    vec2 warp = vec2(
        noise(v_uv * 3.0 + vec2(0.0, p * 0.5)),
        noise(v_uv * 3.0 + vec2(p * 0.5, 0.0))
    ) - 0.5;
    vec2 uv_warped = v_uv + warp * strength;
    vec4 a = texture(u_tex_old, uv_warped);
    vec4 b = texture(u_tex_new, uv_warped);
    float t = smoothstep(0.05, 0.95, p);
    t = t * t * (3.0 - 2.0 * t);
    frag = mix(a, b, t);
}
";

const ZOOM_BLUR_PULL_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float PI = 3.14159265358979;
const float strength_v = 0.4;
float Linear_ease(float begin, float change, float duration, float time) {
    return change * time / duration + begin;
}
float Exponential_easeInOut(float begin, float change, float duration, float time) {
    if (time == 0.0) return begin;
    if (time == duration) return begin + change;
    time = time / (duration / 2.0);
    if (time < 1.0) return change / 2.0 * pow(2.0, 10.0 * (time - 1.0)) + begin;
    return change / 2.0 * (-pow(2.0, -10.0 * (time - 1.0)) + 2.0) + begin;
}
float Sinusoidal_easeInOut(float begin, float change, float duration, float time) {
    return -change / 2.0 * (cos(PI * time / duration) - 1.0) + begin;
}
float rand(vec2 co) {
    return fract(sin(dot(co.xy, vec2(12.9898, 78.233))) * 43758.5453);
}
vec4 crossFade(vec2 uv, float dissolve) {
    return mix(texture(u_tex_old, uv), texture(u_tex_new, uv), dissolve);
}
void main() {
    vec2 texCoord = v_uv;
    vec2 center = vec2(Linear_ease(0.25, 0.5, 1.0, u_progress), 0.5);
    float dissolve = Exponential_easeInOut(0.0, 1.0, 1.0, u_progress);
    float strength = Sinusoidal_easeInOut(0.0, strength_v, 0.5, u_progress);
    vec4 color = vec4(0.0);
    float total = 0.0;
    vec2 toCenter = center - texCoord;
    float offset = rand(v_uv);
    for (float t = 0.0; t <= 40.0; t++) {
        float percent = (t + offset) / 40.0;
        float weight = 4.0 * (percent - percent * percent);
        color += crossFade(texCoord + toCenter * percent * strength, dissolve) * weight;
        total += weight;
    }
    frag = color / total;
}
";

const FLYEYE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float size_v = 0.04;
const float zoom_v = 50.0;
const float colorSeparation = 0.3;
void main() {
    float inv = 1.0 - u_progress;
    vec2 disp = size_v * vec2(cos(zoom_v * v_uv.x), sin(zoom_v * v_uv.y));
    vec4 texTo = texture(u_tex_new, v_uv + inv * disp);
    vec4 texFrom = vec4(
        texture(u_tex_old, v_uv + u_progress * disp * (1.0 - colorSeparation)).r,
        texture(u_tex_old, v_uv + u_progress * disp).g,
        texture(u_tex_old, v_uv + u_progress * disp * (1.0 + colorSeparation)).b,
        1.0
    );
    frag = texTo * u_progress + texFrom * inv;
}
";

const MOSAIC_TUMBLE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform sampler2D u_tex_thumb_0;
uniform sampler2D u_tex_thumb_1;
uniform sampler2D u_tex_thumb_2;
uniform sampler2D u_tex_thumb_3;
uniform sampler2D u_tex_thumb_4;
uniform sampler2D u_tex_thumb_5;
uniform sampler2D u_tex_thumb_6;
uniform sampler2D u_tex_thumb_7;
uniform sampler2D u_tex_thumb_8;
uniform sampler2D u_tex_thumb_9;
uniform sampler2D u_tex_thumb_10;
uniform sampler2D u_tex_thumb_11;
uniform sampler2D u_tex_thumb_12;
uniform sampler2D u_tex_thumb_13;
uniform sampler2D u_tex_thumb_14;
uniform sampler2D u_tex_thumb_15;
uniform sampler2D u_tex_thumb_16;
uniform sampler2D u_tex_thumb_17;
uniform sampler2D u_tex_thumb_18;
uniform sampler2D u_tex_thumb_19;
uniform int u_thumb_count;
uniform float u_progress;
const float PI = 3.14159265358979323;
const int endx = 2;
const int endy = -1;
float Rand(vec2 v) { return fract(sin(dot(v.xy, vec2(12.9898, 78.233))) * 43758.5453); }
vec2 Rotate(vec2 v, float a) {
    mat2 rm = mat2(cos(a), -sin(a), sin(a), cos(a));
    return rm * v;
}
float CosInterpolation(float x) { return -cos(x * PI) / 2.0 + 0.5; }
vec4 sample_thumb(int idx, vec2 uv) {
    if (idx ==  0) return texture(u_tex_thumb_0,  uv);
    if (idx ==  1) return texture(u_tex_thumb_1,  uv);
    if (idx ==  2) return texture(u_tex_thumb_2,  uv);
    if (idx ==  3) return texture(u_tex_thumb_3,  uv);
    if (idx ==  4) return texture(u_tex_thumb_4,  uv);
    if (idx ==  5) return texture(u_tex_thumb_5,  uv);
    if (idx ==  6) return texture(u_tex_thumb_6,  uv);
    if (idx ==  7) return texture(u_tex_thumb_7,  uv);
    if (idx ==  8) return texture(u_tex_thumb_8,  uv);
    if (idx ==  9) return texture(u_tex_thumb_9,  uv);
    if (idx == 10) return texture(u_tex_thumb_10, uv);
    if (idx == 11) return texture(u_tex_thumb_11, uv);
    if (idx == 12) return texture(u_tex_thumb_12, uv);
    if (idx == 13) return texture(u_tex_thumb_13, uv);
    if (idx == 14) return texture(u_tex_thumb_14, uv);
    if (idx == 15) return texture(u_tex_thumb_15, uv);
    if (idx == 16) return texture(u_tex_thumb_16, uv);
    if (idx == 17) return texture(u_tex_thumb_17, uv);
    if (idx == 18) return texture(u_tex_thumb_18, uv);
    return texture(u_tex_thumb_19, uv);
}
void main() {
    vec2 p = v_uv - 0.5;
    vec2 rp = p;
    float rpr = (u_progress * 2.0 - 1.0);
    float z = -(rpr * rpr * 2.0) + 3.0;
    float az = abs(z);
    rp *= az;
    rp += mix(vec2(0.5, 0.5), vec2(float(endx) + 0.5, float(endy) + 0.5),
              CosInterpolation(u_progress) * CosInterpolation(u_progress));
    vec2 mrp = mod(rp, 1.0);
    vec2 crp = rp;
    int cx = int(floor(crp.x));
    int cy = int(floor(crp.y));
    bool onEnd = cx == endx && cy == endy;
    bool onStart = cx == 0 && cy == 0;
    if (onEnd) {
        frag = texture(u_tex_new, mrp);
    } else if (onStart) {
        frag = texture(u_tex_old, mrp);
    } else if (u_thumb_count > 0) {
        int idx = int(Rand(floor(crp) + vec2(7.3, 1.1)) * float(u_thumb_count));
        if (idx >= u_thumb_count) idx = u_thumb_count - 1;
        frag = sample_thumb(idx, mrp);
    } else {
        float ang = float(int(Rand(floor(crp)) * 4.0)) * 0.5 * PI;
        vec2 rotated = vec2(0.5) + Rotate(mrp - vec2(0.5), ang);
        if (Rand(floor(crp)) > 0.5) {
            frag = texture(u_tex_new, rotated);
        } else {
            frag = texture(u_tex_old, rotated);
        }
    }
}
";

const CROSSWARP_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
void main() {
    float x = u_progress;
    x = smoothstep(0.0, 1.0, (x * 2.0 + v_uv.x - 1.0));
    frag = mix(
        texture(u_tex_old, (v_uv - 0.5) * (1.0 - x) + 0.5),
        texture(u_tex_new, (v_uv - 0.5) * x + 0.5),
        x
    );
}
";

const MORPH_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float strength_v = 0.15;
void main() {
    vec4 ca = texture(u_tex_old, v_uv);
    vec4 cb = texture(u_tex_new, v_uv);
    vec2 oa = (((ca.rg + ca.b) * 0.5) * 2.0 - 1.0);
    vec2 ob = (((cb.rg + cb.b) * 0.5) * 2.0 - 1.0);
    vec2 oc = mix(oa, ob, 0.5) * strength_v;
    float w0 = u_progress;
    float w1 = 1.0 - w0;
    frag = mix(texture(u_tex_old, v_uv + oc * w0), texture(u_tex_new, v_uv - oc * w1), u_progress);
}
";

// === SHADER CONSTS ===
const BOUNCE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const vec4 shadow_colour = vec4(0.,0.,0.,.6);
const float shadow_height = 0.075;
const float bounces = 3.0;

const float PI = 3.14159265358;

vec4 transition (vec2 uv) {
  float time = u_progress;
  float stime = sin(time * PI / 2.);
  float phase = time * PI * bounces;
  float y = (abs(cos(phase))) * (1.0 - stime);
  float d = uv.y - y;
  return mix(
    mix(
      texture(u_tex_new, uv),
      shadow_colour,
      step(d, shadow_height) * (1. - mix(
        ((d / shadow_height) * shadow_colour.a) + (1.0 - shadow_colour.a),
        1.0,
        smoothstep(0.95, 1., u_progress) // fade-out the shadow at the end
      ))
    ),
    texture(u_tex_old, vec2(uv.x, uv.y + (1.0 - y))),
    step(d, 0.0)
  );
}
void main() {
    frag = transition(v_uv);
}
";

const CIRCLE_CROP_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float ratio = 1.7777;
const vec4 bgcolor = vec4(0.0, 0.0, 0.0, 1.0);

vec2 ratio2 = vec2(1.0, 1.0 / ratio);
float s = pow(2.0 * abs(u_progress - 0.5), 3.0);

vec4 transition(vec2 p) {
  float dist = length((vec2(p) - 0.5) * ratio2);
  return mix(
    u_progress < 0.5 ? texture(u_tex_old, p) : texture(u_tex_new, p), // branching is ok here as we statically depend on u_progress uniform (branching won't change over pixels)
    bgcolor,
    step(s, dist)
  );
}
void main() {
    frag = transition(v_uv);
}
";

const COLOUR_DISTANCE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float power = 5.0;

vec4 transition(vec2 p) {
  vec4 fTex = texture(u_tex_old, p);
  vec4 tTex = texture(u_tex_new, p);
  float m = step(distance(fTex, tTex), u_progress);
  return mix(
    mix(fTex, tTex, m),
    tTex,
    pow(u_progress, power)
  );
}
void main() {
    frag = transition(v_uv);
}
";

const CRAZY_PARAMETRIC_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float a = 4;
const float b = 1;
const float amplitude = 120;
const float smoothness = 0.1;

vec4 transition(vec2 uv) {
  vec2 p = uv.xy / vec2(1.0).xy;
  vec2 dir = p - vec2(.5);
  float dist = length(dir);
  float x = (a - b) * cos(u_progress) + b * cos(u_progress * ((a / b) - 1.) );
  float y = (a - b) * sin(u_progress) - b * sin(u_progress * ((a / b) - 1.));
  vec2 offset = dir * vec2(sin(u_progress  * dist * amplitude * x), sin(u_progress * dist * amplitude * y)) / smoothness;
  return mix(texture(u_tex_old, p + offset), texture(u_tex_new, p), smoothstep(0.2, 1.0, u_progress));
}
void main() {
    frag = transition(v_uv);
}
";

const DIRECTIONAL_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const vec2 direction = vec2(0.0, 1.0);

vec4 transition (vec2 uv) {
  vec2 p = uv + u_progress * sign(direction);
  vec2 f = fract(p);
  return mix(
    texture(u_tex_new, f),
    texture(u_tex_old, f),
    step(0.0, p.y) * step(p.y, 1.0) * step(0.0, p.x) * step(p.x, 1.0)
  );
}
void main() {
    frag = transition(v_uv);
}
";

const DIRECTIONAL_SCALED_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
#define PI acos(-1.0)

const vec2 direction = vec2(0.0, 1.0);
const float scale = .7;

float parabola(float x) {
  float y = pow(sin(x * PI), 1.);
  return y;
}

vec4 transition (vec2 uv) {
  float easedProgress = pow(sin(u_progress  * PI / 2.), 3.);
  vec2 p = uv + easedProgress * sign(direction);
  vec2 f = fract(p);
  
  float s = 1. - (1. - (1. / scale)) * parabola(u_progress);
  f = (f - 0.5) * s  + 0.5;
  
  float mixer = step(0.0, p.y) * step(p.y, 1.0) * step(0.0, p.x) * step(p.x, 1.0);
  vec4 col = mix(texture(u_tex_new, f), texture(u_tex_old, f), mixer);
  
  float border = step(0., f.x) * step(0., (1. - f.x)) * step(0., f.y) * step(0., 1. - f.y);
  col *= border;
  
  return col;
}
void main() {
    frag = transition(v_uv);
}
";

const EDGE_TRANSITION_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float edge_thickness = 0.001;
const float edge_brightness = 8.0;

vec4 detectEdgeColor(vec3[9] c) {
  
  vec3 dx = 2.0 * abs(c[7]-c[1]) + abs(c[2] - c[6]) + abs(c[8] - c[0]);
	vec3 dy = 2.0 * abs(c[3]-c[5]) + abs(c[6] - c[8]) + abs(c[0] - c[2]);
  float delta = length(0.25 * (dx + dy) * 0.5);
	return vec4(clamp(edge_brightness * delta, 0.0, 1.0) * c[4], 1.0);
}

vec4 getFromEdgeColor(vec2 uv) {
	vec3 c[9];
	for (int i=0; i < 3; ++i) for (int j=0; j < 3; ++j)
	{
	  vec4 color = texture(u_tex_old, uv + edge_thickness * vec2(i-1,j-1));
    c[3*i + j] = color.rgb;
	}
	return detectEdgeColor(c);
}

vec4 getToEdgeColor(vec2 uv) {
	vec3 c[9];
	for (int i=0; i < 3; ++i) for (int j=0; j < 3; ++j)
	{
	  vec4 color = texture(u_tex_new, uv + edge_thickness * vec2(i-1,j-1));
    c[3*i + j] = color.rgb;
	}
	return detectEdgeColor(c);
}

vec4 transition (vec2 uv) {
  vec4 start = mix(texture(u_tex_old, uv), getFromEdgeColor(uv), clamp(2.0 * u_progress, 0.0, 1.0));
  vec4 end = mix(getToEdgeColor(uv), texture(u_tex_new, uv), clamp(2.0 * (u_progress - 0.5), 0.0, 1.0));
  return mix(
    start,
    end,
    u_progress
  );
}
void main() {
    frag = transition(v_uv);
}
";

const GLITCH_DISPLACE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
float random(vec2 co)
{
    float a = 12.9898;
    float b = 78.233;
    float c = 43758.5453;
    float dt= dot(co.xy ,vec2(a,b));
    float sn= mod(dt,3.14);
    return fract(sin(sn) * c);
}
float voronoi( in vec2 x ) {
    vec2 p = floor( x );
    vec2 f = fract( x );
    float res = 8.0;
    for( float j=-1.; j<=1.; j++ )
    for( float i=-1.; i<=1.; i++ ) {
        vec2  b = vec2( i, j );
        vec2  r = b - f + random( p + b );
        float d = dot( r, r );
        res = min( res, d );
    }
    return sqrt( res );
}

vec2 displace(vec4 tex, vec2 texCoord, float dotDepth, float textureDepth, float strength) {
    float b = voronoi(.003 * texCoord + 2.0);
    float g = voronoi(0.2 * texCoord);
    float r = voronoi(texCoord - 1.0);
    vec4 dt = tex * 1.0;
    vec4 dis = dt * dotDepth + 1.0 - tex * textureDepth;

    dis.x = dis.x - 1.0 + textureDepth*dotDepth;
    dis.y = dis.y - 1.0 + textureDepth*dotDepth;
    dis.x *= strength;
    dis.y *= strength;
    vec2 res_uv = texCoord ;
    res_uv.x = res_uv.x + dis.x - 0.0;
    res_uv.y = res_uv.y + dis.y;
    return res_uv;
}

float ease1(float t) {
  return t == 0.0 || t == 1.0
    ? t
    : t < 0.5
      ? +0.5 * pow(2.0, (20.0 * t) - 10.0)
      : -0.5 * pow(2.0, 10.0 - (t * 20.0)) + 1.0;
}
float ease2(float t) {
  return t == 1.0 ? t : 1.0 - pow(2.0, -10.0 * t);
}



vec4 transition(vec2 uv) {
  vec2 p = uv.xy / vec2(1.0).xy;
  vec4 color1 = texture(u_tex_old, p);
  vec4 color2 = texture(u_tex_new, p);
  vec2 disp = displace(color1, p, 0.33, 0.7, 1.0-ease1(u_progress));
  vec2 disp2 = displace(color2, p, 0.33, 0.5, ease2(u_progress));
  vec4 dColor1 = texture(u_tex_new, disp);
  vec4 dColor2 = texture(u_tex_old, disp2);
  float val = ease1(u_progress);
  vec3 gray = vec3(dot(min(dColor2, dColor1).rgb, vec3(0.299, 0.587, 0.114)));
  dColor2 = vec4(gray, 1.0);
  dColor2 *= 2.0;
  color1 = mix(color1, dColor2, smoothstep(0.0, 0.5, u_progress));
  color2 = mix(color2, dColor1, smoothstep(1.0, 0.5, u_progress));
  return mix(color1, color2, val);
  //gl_FragColor = mix(gl_FragColor, dColor, smoothstep(0.0, 0.5, u_progress));

   //gl_FragColor = mix(texture(u_tex_old, p), texture(u_tex_new, p), u_progress);
}
void main() {
    frag = transition(v_uv);
}
";

const OVEREXPOSURE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float strength = 0.6;
const float PI = 3.141592653589793;

vec4 transition (vec2 uv) {
  vec4 from = texture(u_tex_old, uv);
  vec4 to = texture(u_tex_new, uv);

  // Multipliers
  float from_m = 1.0 - u_progress + sin(PI * u_progress) * strength;
  float to_m = u_progress + sin(PI * u_progress) * strength;
  
  return vec4(
    from.r * from.a * from_m + to.r * to.a * to_m,
    from.g * from.a * from_m + to.g * to.a * to_m,
    from.b * from.a * from_m + to.b * to.a * to_m,
    mix(from.a, to.a, u_progress)
  );
}
void main() {
    frag = transition(v_uv);
}
";

const POLKA_DOTS_CURTAIN_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float SQRT_2 = 1.414213562373;
const float dots = 20.0;
const vec2 center = vec2(0, 0);

vec4 transition(vec2 uv) {
  if (u_progress >= 1.0) return texture(u_tex_new, uv);
  bool nextImage = distance(fract(uv * dots), vec2(0.5, 0.5)) < ( u_progress / distance(uv, center));
  return nextImage ? texture(u_tex_new, uv) : texture(u_tex_old, uv);
}
void main() {
    frag = transition(v_uv);
}
";

const PUZZLE_RIGHT_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const ivec2 size = ivec2(4, 4);
const float pause = 0.1;
const float dividerWidth = 0.005;

float rand(vec2 co) {
  return fract(sin(dot(co, vec2(12.9898, 78.233))) * 43758.5453);
}

float getDelta(vec2 p) {
  vec2 rectangleSize = 1.0 / vec2(size);
  vec2 rectanglePos = floor(vec2(size) * p);
  float top = rectangleSize.y * (rectanglePos.y + 1.0);
  float bottom = rectangleSize.y * rectanglePos.y;
  float left = rectangleSize.x * rectanglePos.x;
  float right = rectangleSize.x * (rectanglePos.x + 1.0);
  float minX = min(abs(p.x - left), abs(p.x - right));
  float minY = min(abs(p.y - top), abs(p.y - bottom));
  return min(minX, minY);
}

vec4 transition(vec2 uv) {
  if (u_progress < pause) {
    float currentProg = u_progress / pause;
    float a = 1.0;
    if (getDelta(uv) < dividerWidth) { a = 1.0 - currentProg; }
    return mix(vec4(0.0, 0.0, 0.0, 1.0), texture(u_tex_old, uv), a);
  } else if (u_progress < 1.0 - pause) {
    if (getDelta(uv) < dividerWidth) {
      return vec4(0.0, 0.0, 0.0, 1.0);
    }
    float currentProg = (u_progress - pause) / (1.0 - pause * 2.0);
    vec2 rectanglePos = floor(vec2(size) * uv);
    float r = rand(rectanglePos) - 0.1;
    float cp = smoothstep(0.0, 1.0 - r, currentProg);
    float rectangleSize = 1.0 / float(size.x);
    float delta = rectanglePos.x * rectangleSize;
    float offset = rectangleSize / 2.0 + delta;
    vec2 p = uv;
    p.x = (p.x - offset) / abs(cp - 0.5) * 0.5 + offset;
    vec4 a = texture(u_tex_old, p);
    vec4 b = texture(u_tex_new, p);
    float s = step(abs(float(size.x) * (uv.x - delta) - 0.5), abs(cp - 0.5));
    return vec4(mix(b, a, step(cp, 0.5)).rgb * s, 1.0);
  } else {
    float currentProg = (u_progress - 1.0 + pause) / pause;
    float a = 1.0;
    if (getDelta(uv) < dividerWidth) { a = currentProg; }
    return mix(vec4(0.0, 0.0, 0.0, 1.0), texture(u_tex_new, uv), a);
  }
}
void main() {
    frag = transition(v_uv);
}
";

const STATIC_FADE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float n_noise_pixels = 200.0;
const float static_luminosity = 0.8;

float rnd (vec2 st) {
    return fract(sin(dot(st.xy,
                         vec2(10.5302340293,70.23492931)))*
        12345.5453123);
}

vec4 staticNoise (vec2 st, float offset, float luminosity) {
  float staticR = luminosity * rnd(st * vec2(offset * 2.0, offset * 3.0));
  float staticG = luminosity * rnd(st * vec2(offset * 3.0, offset * 5.0));
  float staticB = luminosity * rnd(st * vec2(offset * 5.0, offset * 7.0));
  return vec4(staticR, staticG, staticB, 1.0);
}

float staticIntensity(float t)
{
  float transitionProgress = abs(2.0*(t-0.5));
  float transformedThreshold =1.2*(1.0 - transitionProgress)-0.1;
  return min(1.0, transformedThreshold);
}
  
vec4 transition (vec2 uv) {

  float baseMix = step(0.5, u_progress);
  vec4 transitionMix = mix(
    texture(u_tex_old, uv),
    texture(u_tex_new, uv),
    baseMix
  );
  
  vec2 uvStatic = floor(uv * n_noise_pixels)/n_noise_pixels;
  
  vec4 staticColor = staticNoise(uvStatic, u_progress, static_luminosity);

  float staticThresh = staticIntensity(u_progress);
  float staticMix = step(rnd(uvStatic), staticThresh);

  return mix(transitionMix, staticColor, staticMix);
}
void main() {
    frag = transition(v_uv);
}
";

const CROSSHATCH_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const vec2 center = vec2(0.5);
const float threshold = 3.0;
const float fadeEdge = 0.1;

float rand(vec2 co) {
  return fract(sin(dot(co.xy ,vec2(12.9898,78.233))) * 43758.5453);
}
vec4 transition(vec2 p) {
  float dist = distance(center, p) / threshold;
  float r = u_progress - min(rand(vec2(p.y, 0.0)), rand(vec2(0.0, p.x)));
  return mix(texture(u_tex_old, p), texture(u_tex_new, p), mix(0.0, mix(step(dist, r), 1.0, smoothstep(1.0-fadeEdge, 1.0, u_progress)), smoothstep(0.0, fadeEdge, u_progress)));    
}
void main() {
    frag = transition(v_uv);
}
";

const DIRECTIONAL_WIPE_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const vec2 direction = vec2(1.0, -1.0);
const float smoothness = 0.5;
 
const vec2 center = vec2(0.5, 0.5);
 
vec4 transition (vec2 uv) {
  vec2 v = normalize(direction);
  v /= abs(v.x)+abs(v.y);
  float d = v.x * center.x + v.y * center.y;
  float m =
    (1.0-step(u_progress, 0.0)) * // there is something wrong with our formula that makes m not equals 0.0 with u_progress is 0.0
    (1.0 - smoothstep(-smoothness, 0.0, v.x * uv.x + v.y * uv.y - (d-0.5+u_progress*(1.+smoothness))));
  return mix(texture(u_tex_old, uv), texture(u_tex_new, uv), m);
}
void main() {
    frag = transition(v_uv);
}
";

const FADECOLOR_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const vec3 color = vec3(0.0);
const float colorPhase = 0.4;; // if 0.0, there is no black phase, if 0.9, the black phase is very important
vec4 transition (vec2 uv) {
  return mix(
    mix(vec4(color, 1.0), texture(u_tex_old, uv), smoothstep(1.0-colorPhase, 0.0, u_progress)),
    mix(vec4(color, 1.0), texture(u_tex_new, uv), smoothstep(    colorPhase, 1.0, u_progress)),
    u_progress);
}
void main() {
    frag = transition(v_uv);
}
";

const PARAMETRIC_GLITCH_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float ampx = 1.0;
const float ampy = 1.0;

vec4 transition (vec2 uv) {
  vec4 from = texture(u_tex_old, uv);
  vec4 to = texture(u_tex_new, uv);
  float r = from.r;
  float g = from.g;
  float b = from.b;
  float sphere = r*r + g*g + b*b - 1.0; //3 to 1
  float spiralX = cos(sphere - uv.x/(u_progress + .01));
  float spiralY = sin(sphere - uv.y/(u_progress+.01));
  vec2 st = uv;
  st.x = fract(ampx*st.x*spiralX); //1 to 2
  st.y = fract(ampy*st.y*spiralY);
  vec2 diff = uv - st;
  from = texture(u_tex_old, uv + u_progress*diff);
  return mix(from, to, u_progress);
}
void main() {
    frag = transition(v_uv);
}
";

const PERLIN_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const float scale = 4.0;
const float smoothness = 0.01;

const float seed = 12.9898;

// http://byteblacksmith.com/improvements-to-the-canonical-one-liner-glsl-rand-for-opengl-es-2-0/
float random(vec2 co)
{
    float a = seed;
    float b = 78.233;
    float c = 43758.5453;
    float dt= dot(co.xy ,vec2(a,b));
    float sn= mod(dt,3.14);
    return fract(sin(sn) * c);
}

// 2D Noise based on Morgan McGuire @morgan3d
// https://www.shadertoy.com/view/4dS3Wd
float noise (in vec2 st) {
    vec2 i = floor(st);
    vec2 f = fract(st);

    // Four corners in 2D of a tile
    float a = random(i);
    float b = random(i + vec2(1.0, 0.0));
    float c = random(i + vec2(0.0, 1.0));
    float d = random(i + vec2(1.0, 1.0));

    // Smooth Interpolation

    // Cubic Hermine Curve.  Same as SmoothStep()
    vec2 u = f*f*(3.0-2.0*f);
    // u = smoothstep(0.,1.,f);

    // Mix 4 coorners porcentages
    return mix(a, b, u.x) +
            (c - a)* u.y * (1.0 - u.x) +
            (d - b) * u.x * u.y;
}

vec4 transition (vec2 uv) {
  vec4 from = texture(u_tex_old, uv);
  vec4 to = texture(u_tex_new, uv);
  float n = noise(uv * scale);

  float p = mix(-smoothness, 1.0 + smoothness, u_progress);
  float lower = p - smoothness;
  float higher = p + smoothness;

  float q = smoothstep(lower, higher, n);

  return mix(
    from,
    to,
    1.0 - q
  );
}
void main() {
    frag = transition(v_uv);
}
";

const POLAR_FUNCTION_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
#define PI 3.14159265359

const int segments = 5;

vec4 transition (vec2 uv) {
  
  float angle = atan(uv.y - 0.5, uv.x - 0.5) - 0.5 * PI;
  float normalized = (angle + 1.5 * PI) * (2.0 * PI);
  
  float radius = (cos(float(segments) * angle) + 4.0) / 4.0;
  float difference = length(uv - vec2(0.5, 0.5));
  
  if (difference > radius * u_progress)
    return texture(u_tex_old, uv);
  else
    return texture(u_tex_new, uv);
}
void main() {
    frag = transition(v_uv);
}
";

const RANDOMSQUARES_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_tex_old;
uniform sampler2D u_tex_new;
uniform float u_progress;
const ivec2 size = ivec2(10, 10);
const float smoothness = 0.5;
 
float rand (vec2 co) {
  return fract(sin(dot(co.xy ,vec2(12.9898,78.233))) * 43758.5453);
}

vec4 transition(vec2 p) {
  float r = rand(floor(vec2(size) * p));
  float m = smoothstep(0.0, -smoothness, r - (u_progress * (1.0 + smoothness)));
  return mix(texture(u_tex_old, p), texture(u_tex_new, p), m);
}
void main() {
    frag = transition(v_uv);
}
";



const SHADER_CATALOG: &[(&str, &str)] = &[
    ("pixelate", PIXELATE_FRAG),
    ("iris", IRIS_FRAG),
    ("liquid-ripple", LIQUID_RIPPLE_FRAG),
    ("wave-warp", WAVE_WARP_FRAG),
    ("glitch", GLITCH_FRAG),
    ("voronoi-shatter", VORONOI_SHATTER_FRAG),
    ("heat-melt", HEAT_MELT_FRAG),
    ("plasma-flow", PLASMA_FLOW_FRAG),
    ("ink-splash", INK_SPLASH_FRAG),
    ("smoke", SMOKE_FRAG),
    ("chromatic-bloom", CHROMATIC_BLOOM_FRAG),
    ("inkwell-drop", INKWELL_DROP_FRAG),
    ("pixelfade-wave", PIXELFADE_WAVE_FRAG),
    ("soft-warp-fade", SOFT_WARP_FADE_FRAG),
    ("zoom-blur-pull", ZOOM_BLUR_PULL_FRAG),
    ("flyeye", FLYEYE_FRAG),
    ("mosaic-tumble", MOSAIC_TUMBLE_FRAG),
    ("crosswarp", CROSSWARP_FRAG),
    ("morph", MORPH_FRAG),
    ("bounce", BOUNCE_FRAG),
    ("circle-crop", CIRCLE_CROP_FRAG),
    ("colour-distance", COLOUR_DISTANCE_FRAG),
    ("crazy-parametric", CRAZY_PARAMETRIC_FRAG),
    ("directional", DIRECTIONAL_FRAG),
    ("directional-scaled", DIRECTIONAL_SCALED_FRAG),
    ("edge-transition", EDGE_TRANSITION_FRAG),
    ("glitch-displace", GLITCH_DISPLACE_FRAG),
    ("overexposure", OVEREXPOSURE_FRAG),
    ("polka-dots-curtain", POLKA_DOTS_CURTAIN_FRAG),
    ("puzzle-right", PUZZLE_RIGHT_FRAG),
    ("static-fade", STATIC_FADE_FRAG),
    ("crosshatch", CROSSHATCH_FRAG),
    ("directional-wipe", DIRECTIONAL_WIPE_FRAG),
    ("fadecolor", FADECOLOR_FRAG),
    ("parametric-glitch", PARAMETRIC_GLITCH_FRAG),
    ("perlin", PERLIN_FRAG),
    ("polar-function", POLAR_FUNCTION_FRAG),
    ("randomsquares", RANDOMSQUARES_FRAG),
];

fn resolve_shader(name: &str) -> (&'static str, &'static str) {
    if name == "random" {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0);
        let idx = nanos % SHADER_CATALOG.len();
        let entry = SHADER_CATALOG[idx];
        tracing::info!(picked = entry.0, "random transition shader chosen");
        return entry;
    }
    SHADER_CATALOG
        .iter()
        .find(|(n, _)| *n == name)
        .copied()
        .unwrap_or(("liquid-ripple", LIQUID_RIPPLE_FRAG))
}

fn pipeline_for(_name: &str) -> Pipeline {
    Pipeline::Single
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_video_path_matches_video_extensions() {
        for p in ["/a/b.mp4", "clip.MKV", "x.webm", "y.MOV", "z.avi"] {
            assert!(is_video_path(p), "{p} should be video");
        }
        for p in ["/a/b.png", "photo.jpg", "noext", "tricky.mp4.png"] {
            assert!(!is_video_path(p), "{p} should not be video");
        }
    }

    #[test]
    fn resolve_shader_known_name_returns_that_entry() {
        assert_eq!(resolve_shader("glitch").0, "glitch");
        assert_eq!(resolve_shader("pixelate").0, "pixelate");
    }

    #[test]
    fn resolve_shader_unknown_falls_back_to_liquid_ripple() {
        assert_eq!(resolve_shader("does-not-exist").0, "liquid-ripple");
    }

    #[test]
    fn resolve_shader_random_returns_catalog_entry() {
        let picked = resolve_shader("random").0;
        assert!(SHADER_CATALOG.iter().any(|(n, _)| *n == picked));
    }

    #[test]
    fn placeholder_pixels_is_one_black_pixel() {
        assert_eq!(placeholder_pixels(), (1, 1, vec![0, 0, 0, 255]));
    }
}
