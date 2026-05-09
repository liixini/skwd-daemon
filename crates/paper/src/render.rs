use anyhow::{Context, Result, anyhow};
use std::ffi::{CString, c_void};
use wayland_client::Proxy;
use wayland_client::protocol::wl_surface::WlSurface;
use wayland_egl::WlEglSurface;

use crate::video_source::VideoSource;

type EglInstance = khronos_egl::Instance<khronos_egl::Static>;
const EGL: EglInstance = khronos_egl::Instance::new(khronos_egl::Static);

pub struct SharedRenderer {
    egl_display: khronos_egl::Display,
    egl_config: khronos_egl::Config,
    primary_ctx: khronos_egl::Context,
    primary_pbuffer: khronos_egl::Surface,
    pub fbo_w: u32,
    pub fbo_h: u32,
    blit_program: u32,
    quad_vbo: u32,
    video: VideoSource,
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
        let initial_mute = mpv_opts
            .iter()
            .find(|(k, _)| k == "mute")
            .map(|(_, v)| v == "yes" || v == "true")
            .unwrap_or(true);
        let initial_volume: u32 = mpv_opts
            .iter()
            .find(|(k, _)| k == "volume")
            .and_then(|(_, v)| v.parse::<u32>().ok())
            .unwrap_or(80);
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

        let blit_program = compile_blit_program()?;
        let quad_vbo = create_quad_vbo();

        let mut video = VideoSource::new(file_path, fbo_w, fbo_h, initial_mute)
            .with_context(|| format!("VideoSource for {file_path}"))?;
        video.set_volume(initial_volume);
        video.prime_first_frame(2000);
        video.set_pause(false);

        Ok(Self {
            egl_display,
            egl_config,
            primary_ctx,
            primary_pbuffer,
            fbo_w,
            fbo_h,
            blit_program,
            quad_vbo,
            video,
        })
    }

    pub fn render_mpv_to_fbo(&mut self) -> bool {
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
        let new_frame = self.video.render_to_fbo();
        unsafe { gl::Finish() };
        new_frame
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
        unsafe {
            gl::Viewport(0, 0, blitter.width as i32, blitter.height as i32);
            gl::ClearColor(0.0, 0.0, 0.0, 1.0);
            gl::Clear(gl::COLOR_BUFFER_BIT);
            gl::UseProgram(self.blit_program);
            gl::ActiveTexture(gl::TEXTURE0);
            gl::BindTexture(gl::TEXTURE_2D, self.video.fbo_texture);
            gl::BindVertexArray(blitter.vao);
            gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);
            gl::BindVertexArray(0);
            gl::BindTexture(gl::TEXTURE_2D, 0);
            let err = gl::GetError();
            if err != gl::NO_ERROR {
                tracing::warn!(gl_error = err, "blit GL error");
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
    pub fn load_path(&mut self, path: &str, mute: bool, volume: u32) -> Result<()> {
        if EGL
            .make_current(
                self.egl_display,
                Some(self.primary_pbuffer),
                Some(self.primary_pbuffer),
                Some(self.primary_ctx),
            )
            .is_err()
        {
            return Err(anyhow!("eglMakeCurrent for load_path"));
        }
        let mut new_video = VideoSource::new(path, self.fbo_w, self.fbo_h, mute)
            .with_context(|| format!("VideoSource for {path}"))?;
        new_video.set_volume(volume);
        new_video.prime_first_frame(2000);
        new_video.set_pause(false);
        let old = std::mem::replace(&mut self.video, new_video);
        unsafe {
            gl::DeleteFramebuffers(1, &old.fbo);
            gl::DeleteTextures(1, &old.fbo_texture);
        }
        drop(old);
        Ok(())
    }

    pub fn set_mute(&mut self, mute: bool) {
        self.video.set_mute(mute);
    }

    pub fn set_volume(&mut self, vol: u32) {
        self.video.set_volume(vol);
    }

    pub fn unpause_mpv(&mut self) {
        self.video.set_pause(false);
    }
}

impl Drop for SharedRenderer {
    fn drop(&mut self) {
        unsafe {
            gl::DeleteFramebuffers(1, &self.video.fbo);
            gl::DeleteTextures(1, &self.video.fbo_texture);
        }
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

const VERT_SRC: &[u8] = b"#version 330 core\nlayout(location=0) in vec2 a_pos;\nlayout(location=1) in vec2 a_tex;\nout vec2 v_tex;\nvoid main() { gl_Position = vec4(a_pos, 0.0, 1.0); v_tex = a_tex; }\n\0";
const FRAG_SRC: &[u8] = b"#version 330 core\nin vec2 v_tex;\nout vec4 frag;\nuniform sampler2D u_tex;\nvoid main() { frag = texture(u_tex, vec2(v_tex.x, 1.0 - v_tex.y)); }\n\0";

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

pub fn wayland_display_ptr(surface: &WlSurface) -> Result<*mut c_void> {
    let conn = surface
        .backend()
        .upgrade()
        .ok_or_else(|| anyhow!("wayland backend gone"))?;
    Ok(conn.display_ptr() as *mut c_void)
}
