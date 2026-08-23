use anyhow::{Context, Result, anyhow};
use ffmpeg_the_third as ff;
use ff::format::Pixel;
use ff::media::Type as MediaType;
use ff::software::scaling::{Context as Scaler, Flags};
use std::ffi::CString;
use std::sync::Once;
use std::time::{Duration, Instant};

use crate::audio::AudioPlayer;

static FFMPEG_INIT: Once = Once::new();

fn sw_thread_count(width: u32, height: u32) -> usize {
    let cap = if u64::from(width) * u64::from(height) > 2_100_000 {
        8
    } else {
        4
    };
    std::thread::available_parallelism()
        .map_or(2, std::num::NonZeroUsize::get)
        .clamp(2, cap)
}

fn sw_threading_config(width: u32, height: u32) -> ff::codec::threading::Config {
    ff::codec::threading::Config {
        kind: ff::codec::threading::Type::Frame,
        count: sw_thread_count(width, height),
    }
}

fn init_ffmpeg() -> Result<()> {
    let mut err: Option<ff::Error> = None;
    FFMPEG_INIT.call_once(|| {
        if let Err(e) = ff::init() {
            err = Some(e);
        }
        ff::log::set_level(ff::log::Level::Error);
    });
    if let Some(e) = err {
        return Err(anyhow!("ffmpeg init: {e}"));
    }
    Ok(())
}

pub struct VideoSource {
    pub fbo: u32,
    pub fbo_texture: u32,
    pub fbo_w: u32,
    pub fbo_h: u32,

    src_w: u32,
    src_h: u32,

    input: ff::format::context::Input,
    stream_index: usize,
    decoder: ff::codec::decoder::Video,
    scaler: Scaler,

    decoded: ff::frame::Video,
    nv12: ff::frame::Video,

    tex_y: u32,
    tex_uv: u32,
    yuv_program: u32,
    yuv_vao: u32,
    yuv_vbo: u32,
    loc_y: i32,
    loc_uv: i32,

    time_base_secs: f64,

    paused: bool,
    start_time: Option<Instant>,
    pause_started: Option<Instant>,
    pause_offset: Duration,

    pending_frame_pts_secs: Option<f64>,

    audio: Option<AudioPlayer>,
}

impl VideoSource {
    pub fn new(file_path: &str, max_w: u32, max_h: u32, mute: bool) -> Result<Self> {
        init_ffmpeg()?;

        let mut input_opts = ff::Dictionary::new();
        input_opts.set("probesize", "65536");
        input_opts.set("analyzeduration", "500000");
        let input = ff::format::input_with_dictionary(file_path, input_opts)
            .with_context(|| format!("ffmpeg open input: {file_path}"))?;

        let (stream_index, time_base_secs, decoder) = {
            let stream = input
                .streams()
                .best(MediaType::Video)
                .ok_or_else(|| anyhow!("no video stream in {file_path}"))?;
            let stream_index = stream.index();
            let tb = stream.time_base();
            let time_base_secs =
                f64::from(tb.numerator()) / f64::from(tb.denominator()).max(1.0);
            let parameters = stream.parameters();
            let (width, height) = (parameters.width(), parameters.height());
            let mut ctx = ff::codec::context::Context::from_parameters(parameters)
                .with_context(|| "decoder context from parameters")?;
            ctx.set_threading(sw_threading_config(width, height));
            let decoder = ctx
                .decoder()
                .video()
                .with_context(|| "open video decoder")?;
            (stream_index, time_base_secs, decoder)
        };

        let src_w = decoder.width().max(2);
        let src_h = decoder.height().max(2);
        let fbo_w = src_w.min(max_w).max(2);
        let fbo_h = src_h.min(max_h).max(2);

        let scaler = Scaler::get(
            decoder.format(),
            src_w,
            src_h,
            Pixel::NV12,
            src_w,
            src_h,
            Flags::FAST_BILINEAR,
        )
        .map_err(|e| anyhow!("sws_scale init: {e}"))?;

        let nv12 = ff::frame::Video::new(Pixel::NV12, src_w, src_h);

        let (fbo, fbo_texture) = create_color_fbo(fbo_w, fbo_h)?;
        let tex_y = create_plane_texture(gl::R8, gl::RED, src_w, src_h);
        let tex_uv = create_plane_texture(gl::RG8, gl::RG, src_w / 2, src_h / 2);
        let yuv_program = compile_yuv_program()?;
        let (yuv_vao, yuv_vbo) = create_quad_vao();
        let (loc_y, loc_uv) = unsafe {
            (
                gl::GetUniformLocation(yuv_program, b"u_y\0".as_ptr().cast()),
                gl::GetUniformLocation(yuv_program, b"u_uv\0".as_ptr().cast()),
            )
        };

        let audio = AudioPlayer::new(file_path, mute, 80)
            .ok()
            .flatten();

        Ok(Self {
            fbo,
            fbo_texture,
            fbo_w,
            fbo_h,
            src_w,
            src_h,
            input,
            stream_index,
            decoder,
            scaler,
            decoded: ff::frame::Video::empty(),
            nv12,
            tex_y,
            tex_uv,
            yuv_program,
            yuv_vao,
            yuv_vbo,
            loc_y,
            loc_uv,
            time_base_secs,
            paused: true,
            start_time: None,
            pause_started: None,
            pause_offset: Duration::ZERO,
            pending_frame_pts_secs: None,
            audio,
        })
    }

    pub fn set_pause(&mut self, paused: bool) {
        if paused == self.paused {
            return;
        }
        if paused {
            self.pause_started = Some(Instant::now());
        } else {
            if let Some(st) = self.pause_started.take() {
                self.pause_offset += st.elapsed();
            }
            if self.start_time.is_none() {
                self.start_time = Some(Instant::now());
            }
        }
        self.paused = paused;
    }

    pub fn set_mute(&mut self, mute: bool) {
        if let Some(a) = self.audio.as_ref() {
            a.set_mute(mute);
        }
    }

    pub fn set_volume(&mut self, vol: u32) {
        if let Some(a) = self.audio.as_ref() {
            a.set_volume(vol);
        }
    }

    pub fn prime_first_frame(&mut self, timeout_ms: u64) {
        let start = Instant::now();
        while !self.decode_one_frame_swallow_err() {
            if start.elapsed().as_millis() as u64 > timeout_ms {
                tracing::warn!("VideoSource prime_first_frame: 1st frame timeout");
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        self.scale_and_render();
        if self.decode_one_frame_swallow_err() {
            self.pending_frame_pts_secs = Some(self.current_decoded_pts_secs());
        }
    }

    pub fn render_to_fbo(&mut self) -> bool {
        if self.paused {
            return false;
        }
        let Some(start) = self.start_time else {
            return false;
        };
        let elapsed = start.elapsed();
        let target_secs = elapsed
            .checked_sub(self.pause_offset)
            .unwrap_or(Duration::ZERO)
            .as_secs_f64();

        let mut uploaded = false;
        let mut iter_safety = 0;
        while iter_safety < 32 {
            iter_safety += 1;
            let Some(pts) = self.pending_frame_pts_secs else {
                break;
            };
            if pts > target_secs {
                break;
            }
            self.scale_and_render();
            uploaded = true;

            match self.decode_one_frame() {
                Ok(true) => {
                    self.pending_frame_pts_secs = Some(self.current_decoded_pts_secs());
                }
                Ok(false) => {
                    if let Err(e) = self.seek_to_start() {
                        tracing::warn!("VideoSource seek_to_start: {e:?}");
                        self.pending_frame_pts_secs = None;
                        break;
                    }
                    self.start_time = Some(Instant::now());
                    self.pause_offset = Duration::ZERO;
                    self.pause_started = None;
                    if self.decode_one_frame_swallow_err() {
                        self.pending_frame_pts_secs = Some(self.current_decoded_pts_secs());
                    } else {
                        self.pending_frame_pts_secs = None;
                    }
                    break;
                }
                Err(e) => {
                    tracing::warn!("VideoSource decode: {e:?}");
                    self.pending_frame_pts_secs = None;
                    break;
                }
            }
        }
        uploaded
    }

    fn current_decoded_pts_secs(&self) -> f64 {
        self.decoded.pts().unwrap_or(0) as f64 * self.time_base_secs
    }

    fn decode_one_frame_swallow_err(&mut self) -> bool {
        self.decode_one_frame().unwrap_or(false)
    }

    fn decode_one_frame(&mut self) -> Result<bool> {
        if self.decoder.receive_frame(&mut self.decoded).is_ok() {
            return Ok(true);
        }
        loop {
            let pkt = self.read_video_packet();
            match pkt {
                Some(packet) => {
                    self.decoder
                        .send_packet(&packet)
                        .map_err(|e| anyhow!("send_packet: {e}"))?;
                    if self.decoder.receive_frame(&mut self.decoded).is_ok() {
                        return Ok(true);
                    }
                }
                None => {
                    let _ = self.decoder.send_eof();
                    if self.decoder.receive_frame(&mut self.decoded).is_ok() {
                        return Ok(true);
                    }
                    return Ok(false);
                }
            }
        }
    }

    fn read_video_packet(&mut self) -> Option<ff::Packet> {
        for item in self.input.packets() {
            match item {
                Ok((stream, packet)) => {
                    if stream.index() == self.stream_index {
                        return Some(packet);
                    }
                }
                Err(e) => {
                    tracing::warn!("packet read: {e:?}");
                    return None;
                }
            }
        }
        None
    }

    fn seek_to_start(&mut self) -> Result<()> {
        self.input
            .seek(0, ..)
            .map_err(|e| anyhow!("av_seek_frame: {e}"))?;
        self.decoder.flush();
        Ok(())
    }

    fn scale_and_render(&mut self) {
        if let Err(e) = self.scaler.run(&self.decoded, &mut self.nv12) {
            tracing::warn!("sws scale: {e:?}");
            return;
        }

        let y_stride = self.nv12.stride(0);
        let uv_stride = self.nv12.stride(1);
        let y_data = self.nv12.data(0).as_ptr();
        let uv_data = self.nv12.data(1).as_ptr();

        unsafe {
            let mut prev_fbo: i32 = 0;
            gl::GetIntegerv(gl::DRAW_FRAMEBUFFER_BINDING, &mut prev_fbo);
            let mut prev_vp: [i32; 4] = [0; 4];
            gl::GetIntegerv(gl::VIEWPORT, prev_vp.as_mut_ptr());
            let mut prev_vao: i32 = 0;
            gl::GetIntegerv(gl::VERTEX_ARRAY_BINDING, &mut prev_vao);

            gl::BindTexture(gl::TEXTURE_2D, self.tex_y);
            gl::PixelStorei(gl::UNPACK_ROW_LENGTH, y_stride as i32);
            gl::TexSubImage2D(
                gl::TEXTURE_2D,
                0,
                0,
                0,
                self.src_w as i32,
                self.src_h as i32,
                gl::RED,
                gl::UNSIGNED_BYTE,
                y_data.cast(),
            );

            gl::BindTexture(gl::TEXTURE_2D, self.tex_uv);
            gl::PixelStorei(gl::UNPACK_ROW_LENGTH, (uv_stride / 2) as i32);
            gl::TexSubImage2D(
                gl::TEXTURE_2D,
                0,
                0,
                0,
                (self.src_w / 2) as i32,
                (self.src_h / 2) as i32,
                gl::RG,
                gl::UNSIGNED_BYTE,
                uv_data.cast(),
            );
            gl::PixelStorei(gl::UNPACK_ROW_LENGTH, 0);

            gl::BindFramebuffer(gl::FRAMEBUFFER, self.fbo);
            gl::Viewport(0, 0, self.fbo_w as i32, self.fbo_h as i32);
            gl::UseProgram(self.yuv_program);
            gl::ActiveTexture(gl::TEXTURE0);
            gl::BindTexture(gl::TEXTURE_2D, self.tex_y);
            gl::Uniform1i(self.loc_y, 0);
            gl::ActiveTexture(gl::TEXTURE1);
            gl::BindTexture(gl::TEXTURE_2D, self.tex_uv);
            gl::Uniform1i(self.loc_uv, 1);
            gl::BindVertexArray(self.yuv_vao);
            gl::DrawArrays(gl::TRIANGLE_STRIP, 0, 4);

            gl::BindVertexArray(prev_vao as u32);
            gl::BindFramebuffer(gl::FRAMEBUFFER, prev_fbo as u32);
            gl::Viewport(prev_vp[0], prev_vp[1], prev_vp[2], prev_vp[3]);
            gl::ActiveTexture(gl::TEXTURE0);
        }
    }
}

impl Drop for VideoSource {
    fn drop(&mut self) {
        unsafe {
            if self.tex_y != 0 {
                gl::DeleteTextures(1, &self.tex_y);
            }
            if self.tex_uv != 0 {
                gl::DeleteTextures(1, &self.tex_uv);
            }
            if self.yuv_program != 0 {
                gl::DeleteProgram(self.yuv_program);
            }
            if self.yuv_vao != 0 {
                gl::DeleteVertexArrays(1, &self.yuv_vao);
            }
            if self.yuv_vbo != 0 {
                gl::DeleteBuffers(1, &self.yuv_vbo);
            }
        }
    }
}

fn create_color_fbo(w: u32, h: u32) -> Result<(u32, u32)> {
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

fn create_plane_texture(internal_format: u32, format: u32, w: u32, h: u32) -> u32 {
    unsafe {
        let mut tex: u32 = 0;
        gl::GenTextures(1, &mut tex);
        gl::BindTexture(gl::TEXTURE_2D, tex);
        gl::TexImage2D(
            gl::TEXTURE_2D,
            0,
            internal_format as i32,
            w as i32,
            h as i32,
            0,
            format,
            gl::UNSIGNED_BYTE,
            std::ptr::null(),
        );
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MIN_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_MAG_FILTER, gl::LINEAR as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_S, gl::CLAMP_TO_EDGE as i32);
        gl::TexParameteri(gl::TEXTURE_2D, gl::TEXTURE_WRAP_T, gl::CLAMP_TO_EDGE as i32);
        gl::BindTexture(gl::TEXTURE_2D, 0);
        tex
    }
}

const YUV_VERT: &str = "#version 330 core
layout(location=0) in vec2 a_pos;
layout(location=1) in vec2 a_uv;
out vec2 v_uv;
void main() {
    gl_Position = vec4(a_pos, 0.0, 1.0);
    v_uv = a_uv;
}
";

const YUV_FRAG: &str = "#version 330 core
in vec2 v_uv;
out vec4 frag;
uniform sampler2D u_y;
uniform sampler2D u_uv;
void main() {
    vec2 uv = vec2(v_uv.x, 1.0 - v_uv.y);
    float y_n = texture(u_y, uv).r;
    vec2 cbcr_n = texture(u_uv, uv).rg;
    float y = (y_n * 255.0 - 16.0) / 219.0;
    float cb = (cbcr_n.x * 255.0 - 128.0) / 224.0;
    float cr = (cbcr_n.y * 255.0 - 128.0) / 224.0;
    vec3 rgb = vec3(
        y + 1.5748 * cr,
        y - 0.1873 * cb - 0.4681 * cr,
        y + 1.8556 * cb
    );
    frag = vec4(clamp(rgb, 0.0, 1.0), 1.0);
}
";

fn compile_yuv_program() -> Result<u32> {
    unsafe {
        let v = compile_shader(gl::VERTEX_SHADER, YUV_VERT)?;
        let f = compile_shader(gl::FRAGMENT_SHADER, YUV_FRAG)?;
        let p = gl::CreateProgram();
        gl::AttachShader(p, v);
        gl::AttachShader(p, f);
        gl::LinkProgram(p);
        let mut ok: i32 = 0;
        gl::GetProgramiv(p, gl::LINK_STATUS, &mut ok);
        gl::DeleteShader(v);
        gl::DeleteShader(f);
        if ok == 0 {
            return Err(anyhow!("yuv program link failed"));
        }
        Ok(p)
    }
}

unsafe fn compile_shader(kind: u32, src: &str) -> Result<u32> {
    let cstr = CString::new(src).map_err(|e| anyhow!("shader NUL: {e}"))?;
    unsafe {
        let s = gl::CreateShader(kind);
        let ptr: *const i8 = cstr.as_ptr();
        gl::ShaderSource(s, 1, &ptr, std::ptr::null());
        gl::CompileShader(s);
        let mut ok: i32 = 0;
        gl::GetShaderiv(s, gl::COMPILE_STATUS, &mut ok);
        if ok == 0 {
            let mut log = [0u8; 1024];
            let mut len: i32 = 0;
            gl::GetShaderInfoLog(s, log.len() as i32, &mut len, log.as_mut_ptr().cast());
            let msg = std::str::from_utf8(&log[..len as usize]).unwrap_or("?");
            tracing::error!("yuv shader compile failed: {msg}");
            gl::DeleteShader(s);
            return Err(anyhow!("yuv shader compile failed"));
        }
        Ok(s)
    }
}

fn create_quad_vao() -> (u32, u32) {
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

        let mut vao: u32 = 0;
        gl::GenVertexArrays(1, &mut vao);
        gl::BindVertexArray(vao);
        gl::EnableVertexAttribArray(0);
        gl::EnableVertexAttribArray(1);
        gl::VertexAttribPointer(0, 2, gl::FLOAT, gl::FALSE, 16, std::ptr::null());
        gl::VertexAttribPointer(1, 2, gl::FLOAT, gl::FALSE, 16, 8 as *const _);
        gl::BindVertexArray(0);
        gl::BindBuffer(gl::ARRAY_BUFFER, 0);
        (vao, vbo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_is_never_single_threaded() {
        for (w, h) in [
            (0, 0),
            (1280, 720),
            (1920, 1080),
            (3440, 1440),
            (3840, 2160),
            (7680, 4320),
        ] {
            assert!(sw_thread_count(w, h) >= 2, "{w}x{h} fell back to serial decode");
            assert_eq!(
                sw_threading_config(w, h).kind,
                ff::codec::threading::Type::Frame,
                "{w}x{h} did not enable frame threading"
            );
        }
    }

    #[test]
    fn threads_stay_bounded_and_scale_with_resolution() {
        assert!(sw_thread_count(1920, 1080) <= 4);
        assert!(sw_thread_count(3840, 2160) <= 8);
        assert!(sw_thread_count(7680, 4320) <= 8);
        assert!(sw_thread_count(3840, 2160) >= sw_thread_count(1920, 1080));
    }
}
