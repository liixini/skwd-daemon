use anyhow::{Context, Result, anyhow};
use libmpv2::{
    Mpv,
    render::{OpenGLInitParams, RenderContext, RenderParam, RenderParamApiType},
};
use std::ffi::{CString, c_void};
use wayland_client::Proxy;
use wayland_client::protocol::wl_surface::WlSurface;
use wayland_egl::WlEglSurface;

type EglInstance = khronos_egl::Instance<khronos_egl::Static>;
const EGL: EglInstance = khronos_egl::Instance::new(khronos_egl::Static);

pub struct SharedRenderer {
    egl_display: khronos_egl::Display,
    egl_config: khronos_egl::Config,
    primary_ctx: khronos_egl::Context,
    primary_pbuffer: khronos_egl::Surface,
    fbo: u32,
    fbo_texture: u32,
    pub fbo_w: u32,
    pub fbo_h: u32,
    blit_program: u32,
    quad_vbo: u32,
    pub render_ctx: RenderContext,
    _mpv: Mpv,
}

pub struct OutputBlitter {
    egl_display: khronos_egl::Display,
    egl_context: khronos_egl::Context,
    egl_surface: khronos_egl::Surface,
    _wl_egl_surface: WlEglSurface,
    vao: u32,
    pub width: u32,
    pub height: u32,
}

impl SharedRenderer {
    pub fn new(
        wayland_display_ptr: *mut c_void,
        fbo_w: u32,
        fbo_h: u32,
        file_path: &str,
        mpv_opts: &[(String, String)],
    ) -> Result<Self> {
        let egl_display = unsafe { EGL.get_display(wayland_display_ptr) }
            .ok_or_else(|| anyhow!("eglGetDisplay failed"))?;
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
            .ok_or_else(|| anyhow!("no matching EGL config"))?;

        let ctx_attribs = [
            khronos_egl::CONTEXT_MAJOR_VERSION,
            3,
            khronos_egl::CONTEXT_MINOR_VERSION,
            3,
            khronos_egl::NONE,
        ];
        let primary_ctx = EGL
            .create_context(egl_display, egl_config, None, &ctx_attribs)
            .map_err(|e| anyhow!("eglCreateContext primary: {e:?}"))?;

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
        .map_err(|e| anyhow!("eglMakeCurrent primary: {e:?}"))?;

        gl::load_with(|name| {
            let cname = CString::new(name).unwrap();
            EGL.get_proc_address(&cname.to_string_lossy())
                .map(|p| p as *const c_void)
                .unwrap_or(std::ptr::null())
        });

        let (fbo, fbo_texture) = create_fbo(fbo_w, fbo_h)?;
        let blit_program = compile_blit_program()?;
        let quad_vbo = create_quad_vbo();

        let mut mpv = Mpv::with_initializer(|init| {
            init.set_property("vo", "libmpv")?;
            init.set_property("hwdec", "auto-safe")?;
            init.set_property("video-sync", "desync")?;
            init.set_property("profile", "fast")?;
            init.set_property("vd-lavc-fast", "yes")?;
            init.set_property("audio-display", "no")?;
            init.set_property("input-default-bindings", "no")?;
            init.set_property("input-vo-keyboard", "no")?;
            init.set_property("input-cursor", "no")?;
            init.set_property("loop-file", "inf")?;
            init.set_property("idle", "yes")?;
            init.set_property("pause", "no")?;
            init.set_property("keep-open", "always")?;
            init.set_property("osc", "no")?;
            init.set_property("osd-bar", "no")?;
            init.set_property("force-window", "no")?;
            for (k, v) in mpv_opts {
                let _ = init.set_property(k.as_str(), v.as_str());
            }
            Ok(())
        })
        .map_err(|e| anyhow!("mpv init: {e:?}"))?;

        let render_ctx = RenderContext::new(
            unsafe { mpv.ctx.as_mut() },
            vec![
                RenderParam::ApiType(RenderParamApiType::OpenGl),
                RenderParam::InitParams(OpenGLInitParams {
                    get_proc_address,
                    ctx: (),
                }),
            ],
        )
        .map_err(|e| anyhow!("mpv render_ctx: {e:?}"))?;

        load_file(&mpv, file_path)?;

        Ok(Self {
            egl_display,
            egl_config,
            primary_ctx,
            primary_pbuffer,
            fbo,
            fbo_texture,
            fbo_w,
            fbo_h,
            blit_program,
            quad_vbo,
            render_ctx,
            _mpv: mpv,
        })
    }

    pub fn render_mpv_to_fbo(&mut self) -> bool {
        let flags = self.render_ctx.update().map(|f| f as u64).unwrap_or(0);
        let frame_flag = libmpv2::render::mpv_render_update::Frame as u64;
        let force = std::env::var("SKWD_PAPER_FORCE_RENDER").is_ok();
        let test_color = std::env::var("SKWD_PAPER_TEST_COLOR").is_ok();
        if !force && !test_color && (flags & frame_flag) == 0 {
            return false;
        }
        if EGL
            .make_current(
                self.egl_display,
                Some(self.primary_pbuffer),
                Some(self.primary_pbuffer),
                Some(self.primary_ctx),
            )
            .is_err()
        {
            tracing::warn!("eglMakeCurrent primary failed");
            return false;
        }
        if test_color {
            unsafe {
                gl::BindFramebuffer(gl::FRAMEBUFFER, self.fbo);
                gl::Viewport(0, 0, self.fbo_w as i32, self.fbo_h as i32);
                gl::ClearColor(0.9, 0.2, 0.2, 1.0);
                gl::Clear(gl::COLOR_BUFFER_BIT);
                gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
            }
        } else {
            let res = self.render_ctx.render::<()>(
                self.fbo as i32,
                self.fbo_w as i32,
                self.fbo_h as i32,
                true,
            );
            if let Err(e) = &res {
                tracing::warn!("mpv render: {:?}", e);
            }
        }
        unsafe { gl::Finish() };
        true
    }

    pub fn blit_to(&self, blitter: &OutputBlitter) {
        if EGL
            .make_current(
                blitter.egl_display,
                Some(blitter.egl_surface),
                Some(blitter.egl_surface),
                Some(blitter.egl_context),
            )
            .is_err()
        {
            tracing::warn!("eglMakeCurrent blitter failed");
            return;
        }
        let direct_color = std::env::var("SKWD_PAPER_DIRECT_COLOR").is_ok();
        unsafe {
            gl::Viewport(0, 0, blitter.width as i32, blitter.height as i32);
            if direct_color {
                gl::ClearColor(0.1, 0.7, 0.2, 1.0); // green
                gl::Clear(gl::COLOR_BUFFER_BIT);
            } else {
                gl::ClearColor(0.0, 0.0, 0.0, 1.0);
                gl::Clear(gl::COLOR_BUFFER_BIT);
                gl::UseProgram(self.blit_program);
                gl::ActiveTexture(gl::TEXTURE0);
                gl::BindTexture(gl::TEXTURE_2D, self.fbo_texture);
                gl::BindVertexArray(blitter.vao);
                gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);
                gl::BindVertexArray(0);
                gl::BindTexture(gl::TEXTURE_2D, 0);
                let err = gl::GetError();
                if err != gl::NO_ERROR {
                    tracing::warn!(gl_error = err, "blit GL error");
                }
            }
        }
        if let Err(e) = EGL.swap_buffers(blitter.egl_display, blitter.egl_surface) {
            tracing::warn!(error = ?e, "swap_buffers failed");
        }
    }

    pub fn make_blitter(&self, surface: &WlSurface, w: u32, h: u32) -> Result<OutputBlitter> {
        let ctx_attribs = [
            khronos_egl::CONTEXT_MAJOR_VERSION,
            3,
            khronos_egl::CONTEXT_MINOR_VERSION,
            3,
            khronos_egl::NONE,
        ];
        let egl_context = EGL
            .create_context(
                self.egl_display,
                self.egl_config,
                Some(self.primary_ctx),
                &ctx_attribs,
            )
            .map_err(|e| anyhow!("eglCreateContext shared: {e:?}"))?;

        let wl_egl_surface = WlEglSurface::new(surface.id(), w as i32, h as i32)
            .context("WlEglSurface::new")?;
        let egl_surface = unsafe {
            EGL.create_window_surface(
                self.egl_display,
                self.egl_config,
                wl_egl_surface.ptr() as khronos_egl::NativeWindowType,
                None,
            )
        }
        .map_err(|e| anyhow!("eglCreateWindowSurface: {e:?}"))?;

        EGL.make_current(
            self.egl_display,
            Some(egl_surface),
            Some(egl_surface),
            Some(egl_context),
        )
        .map_err(|e| anyhow!("eglMakeCurrent for VAO setup: {e:?}"))?;
        let mut vao: u32 = 0;
        unsafe {
            gl::GenVertexArrays(1, &mut vao);
            gl::BindVertexArray(vao);
            gl::BindBuffer(gl::ARRAY_BUFFER, self.quad_vbo);
            gl::EnableVertexAttribArray(0);
            gl::EnableVertexAttribArray(1);
            gl::VertexAttribPointer(0, 2, gl::FLOAT, gl::FALSE, 16, std::ptr::null());
            gl::VertexAttribPointer(1, 2, gl::FLOAT, gl::FALSE, 16, 8 as *const _);
            gl::BindVertexArray(0);
            gl::BindBuffer(gl::ARRAY_BUFFER, 0);
        }

        Ok(OutputBlitter {
            egl_display: self.egl_display,
            egl_context,
            egl_surface,
            _wl_egl_surface: wl_egl_surface,
            vao,
            width: w,
            height: h,
        })
    }
}

impl OutputBlitter {
    pub fn resize(&mut self, w: u32, h: u32) {
        if w == self.width && h == self.height {
            return;
        }
        self.width = w;
        self.height = h;
        self._wl_egl_surface.resize(w as i32, h as i32, 0, 0);
    }
}

impl SharedRenderer {
    pub fn load_path(&mut self, path: &str) -> Result<()> {
        load_file(&self._mpv, path)?;
        let _ = self._mpv.set_property("pause", "no");
        Ok(())
    }

    pub fn set_mute(&mut self, mute: bool) {
        let _ = self._mpv.set_property("mute", if mute { "yes" } else { "no" });
    }

    pub fn unpause_mpv(&mut self) {
        if let Err(e) = self._mpv.set_property("pause", "no") {
            tracing::warn!("mpv unpause failed: {e:?}");
        } else {
            tracing::info!("mpv unpaused");
        }
    }
}

impl Drop for SharedRenderer {
    fn drop(&mut self) {
        let _ = EGL.make_current(self.egl_display, None, None, None);
        let _ = EGL.destroy_surface(self.egl_display, self.primary_pbuffer);
        let _ = EGL.destroy_context(self.egl_display, self.primary_ctx);
    }
}

impl Drop for OutputBlitter {
    fn drop(&mut self) {
        let _ = EGL.make_current(self.egl_display, None, None, None);
        let _ = EGL.destroy_surface(self.egl_display, self.egl_surface);
        let _ = EGL.destroy_context(self.egl_display, self.egl_context);
    }
}

fn create_fbo(w: u32, h: u32) -> Result<(u32, u32)> {
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
        gl::FramebufferTexture2D(
            gl::FRAMEBUFFER,
            gl::COLOR_ATTACHMENT0,
            gl::TEXTURE_2D,
            tex,
            0,
        );
        if gl::CheckFramebufferStatus(gl::FRAMEBUFFER) != gl::FRAMEBUFFER_COMPLETE {
            return Err(anyhow!("FBO incomplete"));
        }
        gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
        Ok((fbo, tex))
    }
}

const VERT_SRC: &[u8] = b"#version 330 core\nlayout(location=0) in vec2 a_pos;\nlayout(location=1) in vec2 a_tex;\nout vec2 v_tex;\nvoid main() { gl_Position = vec4(a_pos, 0.0, 1.0); v_tex = a_tex; }\n\0";
const FRAG_SRC: &[u8] = b"#version 330 core\nin vec2 v_tex;\nout vec4 frag;\nuniform sampler2D u_tex;\nvoid main() { frag = texture(u_tex, v_tex); }\n\0";

fn compile_blit_program() -> Result<u32> {
    unsafe {
        let v = compile_shader(gl::VERTEX_SHADER, VERT_SRC)?;
        let f = compile_shader(gl::FRAGMENT_SHADER, FRAG_SRC)?;
        let p = gl::CreateProgram();
        gl::AttachShader(p, v);
        gl::AttachShader(p, f);
        gl::LinkProgram(p);
        let mut ok: i32 = 0;
        gl::GetProgramiv(p, gl::LINK_STATUS, &mut ok);
        gl::DeleteShader(v);
        gl::DeleteShader(f);
        if ok == 0 {
            return Err(anyhow!("blit program link failed"));
        }
        gl::UseProgram(p);
        let loc = gl::GetUniformLocation(p, b"u_tex\0".as_ptr().cast());
        gl::Uniform1i(loc, 0);
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
            gl::DeleteShader(s);
            return Err(anyhow!("shader compile failed (kind={kind})"));
        }
        Ok(s)
    }
}

fn create_quad_vbo() -> u32 {
    let verts: [f32; 16] = [
        -1.0, -1.0, 0.0, 0.0,
         1.0, -1.0, 1.0, 0.0,
        -1.0,  1.0, 0.0, 1.0,
         1.0,  1.0, 1.0, 1.0,
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

fn load_file(mpv: &Mpv, path: &str) -> Result<()> {
    let cmd = CString::new("loadfile").unwrap();
    let arg = CString::new(path).map_err(|e| anyhow!("path NUL: {e}"))?;
    let ptrs: [*const std::os::raw::c_char; 3] = [cmd.as_ptr(), arg.as_ptr(), std::ptr::null()];
    let rc = unsafe { libmpv2_sys::mpv_command(mpv.ctx.as_ptr(), ptrs.as_ptr().cast_mut()) };
    if rc < 0 {
        return Err(anyhow!("mpv_command(loadfile) returned {}", rc));
    }
    Ok(())
}

fn get_proc_address(_ctx: &(), name: &str) -> *mut c_void {
    let cname = match CString::new(name) {
        Ok(c) => c,
        Err(_) => return std::ptr::null_mut(),
    };
    EGL.get_proc_address(&cname.to_string_lossy())
        .map(|p| p as *mut c_void)
        .unwrap_or(std::ptr::null_mut())
}

pub fn wayland_display_ptr(surface: &WlSurface) -> Result<*mut c_void> {
    let conn = surface
        .backend()
        .upgrade()
        .ok_or_else(|| anyhow!("wayland backend gone"))?;
    Ok(conn.display_ptr() as *mut c_void)
}
