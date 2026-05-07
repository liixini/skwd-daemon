use anyhow::{Result, anyhow};
use libmpv2::{
    Mpv,
    render::{OpenGLInitParams, RenderContext, RenderParam, RenderParamApiType},
};
use std::ffi::{CString, c_void};

type EglInstance = khronos_egl::Instance<khronos_egl::Static>;
const EGL: EglInstance = khronos_egl::Instance::new(khronos_egl::Static);

pub struct VideoSource {
    pub fbo: u32,
    pub fbo_texture: u32,
    pub fbo_w: u32,
    pub fbo_h: u32,
    pub render_ctx: RenderContext,
    mpv: Mpv,
}

impl VideoSource {
    pub fn new(file_path: &str, fbo_w: u32, fbo_h: u32, mute: bool) -> Result<Self> {
        let (fbo, fbo_texture) = create_fbo(fbo_w, fbo_h)?;

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
            init.set_property("pause", "yes")?;
            init.set_property("keep-open", "always")?;
            init.set_property("osc", "no")?;
            init.set_property("osd-bar", "no")?;
            init.set_property("force-window", "no")?;
            init.set_property("mute", if mute { "yes" } else { "no" })?;
            Ok(())
        })
        .map_err(|e| anyhow!("VideoSource mpv init: {e:?}"))?;

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
        .map_err(|e| anyhow!("VideoSource render_ctx: {e:?}"))?;

        load_file(&mpv, file_path)?;

        Ok(Self {
            fbo,
            fbo_texture,
            fbo_w,
            fbo_h,
            render_ctx,
            mpv,
        })
    }

    pub fn set_pause(&mut self, paused: bool) {
        let _ = self.mpv.set_property("pause", if paused { "yes" } else { "no" });
    }

    pub fn set_mute(&mut self, mute: bool) {
        let _ = self.mpv.set_property("mute", if mute { "yes" } else { "no" });
    }

    pub fn set_volume(&mut self, vol: u32) {
        let _ = self.mpv.set_property("volume", vol.min(100) as i64);
    }

    pub fn prime_first_frame(&mut self, timeout_ms: u64) {
        let start = std::time::Instant::now();
        loop {
            if self.render_to_fbo() {
                return;
            }
            if start.elapsed().as_millis() as u64 > timeout_ms {
                tracing::warn!("VideoSource prime_first_frame timed out");
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    pub fn into_static_fbo(self) -> (u32, u32) {
        (self.fbo, self.fbo_texture)
    }

    pub fn render_to_fbo(&mut self) -> bool {
        let flags = self.render_ctx.update().map(|f| f as u64).unwrap_or(0);
        let frame_flag = libmpv2::render::mpv_render_update::Frame as u64;
        if (flags & frame_flag) == 0 {
            return false;
        }
        let res = self.render_ctx.render::<()>(
            self.fbo as i32,
            self.fbo_w as i32,
            self.fbo_h as i32,
            false,
        );
        if let Err(e) = &res {
            tracing::warn!("VideoSource mpv render: {:?}", e);
        }
        unsafe { gl::Finish() };
        true
    }
}

pub struct MpvImagePool {
    render_ctx: RenderContext,
    mpv: Mpv,
}

impl MpvImagePool {
    pub fn new() -> Result<Self> {
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
            init.set_property("idle", "yes")?;
            init.set_property("pause", "yes")?;
            init.set_property("force-window", "no")?;
            init.set_property("mute", "yes")?;
            init.set_property("osc", "no")?;
            init.set_property("osd-bar", "no")?;
            Ok(())
        })
        .map_err(|e| anyhow!("MpvImagePool mpv init: {e:?}"))?;

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
        .map_err(|e| anyhow!("MpvImagePool render_ctx: {e:?}"))?;

        Ok(Self {
            mpv,
            render_ctx,
        })
    }

    pub fn decode_to_fbo(
        &mut self,
        path: &str,
        w: u32,
        h: u32,
        timeout_ms: u64,
    ) -> Result<(u32, u32)> {
        let total_t = std::time::Instant::now();
        while self.mpv.event_context_mut().wait_event(0.0).is_some() {}
        load_file(&self.mpv, path)?;
        let (fbo, fbo_texture) = create_fbo(w, h)?;

        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms);

        let path_t = std::time::Instant::now();
        let mut path_matched = false;
        loop {
            while self.mpv.event_context_mut().wait_event(0.0).is_some() {}
            let current = self.mpv.get_property::<String>("path").unwrap_or_default();
            if current == path {
                path_matched = true;
                break;
            }
            if std::time::Instant::now() >= deadline {
                tracing::warn!(
                    requested = %path,
                    current = %current,
                    "MpvImagePool: path-property wait timed out"
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let path_ms = path_t.elapsed().as_millis() as u64;

        let frame_t = std::time::Instant::now();
        let mut renders = 0u32;
        loop {
            let flags = self.render_ctx.update().map(|f| f as u64).unwrap_or(0);
            let frame_flag = libmpv2::render::mpv_render_update::Frame as u64;
            if (flags & frame_flag) != 0 {
                let _ = self.render_ctx.render::<()>(
                    fbo as i32,
                    w as i32,
                    h as i32,
                    false,
                );
                unsafe { gl::Finish() };
                renders += 1;
                if renders >= 2 {
                    let frame_ms = frame_t.elapsed().as_millis() as u64;
                    let total_ms = total_t.elapsed().as_millis() as u64;
                    tracing::info!(
                        path_ms,
                        frame_ms,
                        total_ms,
                        path_matched,
                        renders,
                        "MpvImagePool decoded"
                    );
                    return Ok((fbo, fbo_texture));
                }
            }
            if std::time::Instant::now() >= deadline {
                if renders == 0 {
                    return Err(anyhow!("MpvImagePool decode timeout for {path}"));
                }
                let frame_ms = frame_t.elapsed().as_millis() as u64;
                let total_ms = total_t.elapsed().as_millis() as u64;
                tracing::info!(
                    path_ms,
                    frame_ms,
                    total_ms,
                    path_matched,
                    renders,
                    "MpvImagePool decoded (single render, deadline)"
                );
                return Ok((fbo, fbo_texture));
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
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
            return Err(anyhow!("VideoSource FBO incomplete"));
        }
        gl::BindFramebuffer(gl::FRAMEBUFFER, 0);
        Ok((fbo, tex))
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
