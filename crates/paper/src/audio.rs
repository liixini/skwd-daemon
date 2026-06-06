use anyhow::{Context, Result, anyhow};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ffmpeg_the_third as ff;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Producer, Split};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

const TARGET_SAMPLE_RATE: u32 = 48_000;
const RING_FRAMES: usize = TARGET_SAMPLE_RATE as usize / 5;

pub struct AudioPlayer {
    mute: Arc<AtomicBool>,
    volume_x100: Arc<AtomicU32>,
    stop: Arc<AtomicBool>,
    decoder_thread: Option<JoinHandle<()>>,
    _stream: cpal::Stream,
}

impl AudioPlayer {
    pub fn new(file_path: &str, mute: bool, volume: u32) -> Result<Option<Self>> {
        let probe = ff::format::input(file_path)
            .with_context(|| format!("audio probe: {file_path}"))?;
        let has_audio = probe.streams().best(ff::media::Type::Audio).is_some();
        drop(probe);
        if !has_audio {
            return Ok(None);
        }

        let mute_flag = Arc::new(AtomicBool::new(mute));
        let volume_flag = Arc::new(AtomicU32::new(volume.min(100)));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let rb = HeapRb::<f32>::new(RING_FRAMES * 2);
        let (producer, mut consumer) = rb.split();

        let host = cpal::default_host();
        let device = host
            .default_output_device()
            .ok_or_else(|| anyhow!("no default audio output device"))?;
        let config = cpal::StreamConfig {
            channels: 2,
            sample_rate: cpal::SampleRate(TARGET_SAMPLE_RATE),
            buffer_size: cpal::BufferSize::Default,
        };

        let cb_mute = mute_flag.clone();
        let cb_volume = volume_flag.clone();
        if let Ok(default_cfg) = device.default_output_config() {
            tracing::info!(
                "cpal device default: {:?}, requesting: {:?}",
                default_cfg, config,
            );
        }
        let stream = device
            .build_output_stream(
                &config,
                move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                    let muted = cb_mute.load(Ordering::Relaxed);
                    if muted {
                        let _ = consumer.skip(data.len());
                        for s in data.iter_mut() {
                            *s = 0.0;
                        }
                        return;
                    }
                    let v = (cb_volume.load(Ordering::Relaxed) as f32) / 100.0;
                    let popped = consumer.pop_slice(data);
                    for s in &mut data[..popped] {
                        *s *= v;
                    }
                    for s in &mut data[popped..] {
                        *s = 0.0;
                    }
                },
                |e| tracing::warn!("cpal stream: {e:?}"),
                None,
            )
            .map_err(|e| anyhow!("cpal build_output_stream: {e:?}"))?;
        stream
            .play()
            .map_err(|e| anyhow!("cpal play: {e:?}"))?;

        let path = file_path.to_string();
        let stop_th = stop_flag.clone();
        let thread = std::thread::Builder::new()
            .name("skwd-audio".into())
            .spawn(move || {
                if let Err(e) = decode_loop(&path, producer, stop_th) {
                    tracing::warn!("audio decode_loop exited: {e:?}");
                }
            })
            .map_err(|e| anyhow!("spawn audio thread: {e:?}"))?;

        Ok(Some(Self {
            mute: mute_flag,
            volume_x100: volume_flag,
            stop: stop_flag,
            decoder_thread: Some(thread),
            _stream: stream,
        }))
    }

    pub fn set_mute(&self, mute: bool) {
        self.mute.store(mute, Ordering::Relaxed);
    }

    pub fn set_volume(&self, vol: u32) {
        self.volume_x100.store(vol.min(100), Ordering::Relaxed);
    }
}

impl Drop for AudioPlayer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.decoder_thread.take() {
            let _ = t.join();
        }
    }
}

type RingProducer = ringbuf::HeapProd<f32>;

fn decode_loop(path: &str, mut producer: RingProducer, stop: Arc<AtomicBool>) -> Result<()> {
    let mut input_opts = ff::Dictionary::new();
    input_opts.set("probesize", "65536");
    input_opts.set("analyzeduration", "500000");
    let mut input = ff::format::input_with_dictionary(path, input_opts)
        .with_context(|| format!("audio open: {path}"))?;

    let (audio_idx, mut decoder, in_format, in_rate, in_channels): (
        usize,
        ff::codec::decoder::Audio,
        ff::format::Sample,
        u32,
        u32,
    ) = {
        let stream = input
            .streams()
            .best(ff::media::Type::Audio)
            .ok_or_else(|| anyhow!("no audio stream"))?;
        let idx = stream.index();
        let parameters = stream.parameters();
        let mut ctx = ff::codec::context::Context::from_parameters(parameters)
            .with_context(|| "audio decoder ctx")?;
        ctx.set_threading(ff::codec::threading::Config {
            kind: ff::codec::threading::Type::None,
            count: 1,
        });
        let dec = ctx
            .decoder()
            .audio()
            .with_context(|| "open audio decoder")?;
        let fmt = dec.format();
        let rate = dec.rate();
        let chans = dec.ch_layout().channels();
        (idx, dec, fmt, rate, chans)
    };

    let in_layout = ff::ChannelLayout::default_for_channels(in_channels);
    let target_layout = ff::ChannelLayout::default_for_channels(2);
    let target_format = ff::format::Sample::F32(ff::format::sample::Type::Planar);

    tracing::info!(
        "audio decode_loop: in_format={:?} in_rate={in_rate} in_channels={in_channels} target_format={:?} target_rate={TARGET_SAMPLE_RATE} target_channels=2",
        in_format,
        target_format,
    );

    let mut resampler = ff::software::resampling::Context::get2(
        in_format,
        in_layout,
        in_rate,
        target_format,
        target_layout,
        TARGET_SAMPLE_RATE,
    )
    .map_err(|e| anyhow!("swr init: {e}"))?;

    let mut decoded = ff::frame::Audio::empty();
    let mut resampled = ff::frame::Audio::empty();
    let mut interleave_buf: Vec<f32> = Vec::new();
    let mut traced_pushes: u32 = 0;

    while !stop.load(Ordering::Relaxed) {
        let pkt_opt = read_audio_packet(&mut input, audio_idx, &stop);
        match pkt_opt {
            Some(packet) => {
                if decoder.send_packet(&packet).is_err() {
                    continue;
                }
                while !stop.load(Ordering::Relaxed)
                    && decoder.receive_frame(&mut decoded).is_ok()
                {
                    if resampler.run(&decoded, &mut resampled).is_err() {
                        continue;
                    }
                    if traced_pushes < 3 {
                        tracing::info!(
                            "audio push #{}: format={:?} rate={} samples={} planes={} plane0_len={}",
                            traced_pushes,
                            resampled.format(),
                            resampled.rate(),
                            resampled.samples(),
                            resampled.planes(),
                            if resampled.planes() > 0 {
                                resampled.plane::<f32>(0).len()
                            } else {
                                0
                            },
                        );
                        traced_pushes += 1;
                    }
                    push_to_ring(&mut producer, &resampled, &mut interleave_buf, &stop);
                    while !stop.load(Ordering::Relaxed) && resampler.delay().is_some() {
                        if resampler.flush(&mut resampled).is_err() {
                            break;
                        }
                        if resampled.samples() == 0 {
                            break;
                        }
                        if traced_pushes < 3 {
                        tracing::info!(
                            "audio push #{}: format={:?} rate={} samples={} planes={} plane0_len={}",
                            traced_pushes,
                            resampled.format(),
                            resampled.rate(),
                            resampled.samples(),
                            resampled.planes(),
                            if resampled.planes() > 0 {
                                resampled.plane::<f32>(0).len()
                            } else {
                                0
                            },
                        );
                        traced_pushes += 1;
                    }
                    push_to_ring(&mut producer, &resampled, &mut interleave_buf, &stop);
                    }
                }
            }
            None => {
                if input.seek(0, ..).is_err() {
                    break;
                }
                decoder.flush();
            }
        }
    }
    Ok(())
}

fn read_audio_packet(
    input: &mut ff::format::context::Input,
    audio_idx: usize,
    stop: &AtomicBool,
) -> Option<ff::Packet> {
    for item in input.packets() {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        match item {
            Ok((stream, packet)) => {
                if stream.index() == audio_idx {
                    return Some(packet);
                }
            }
            Err(_) => return None,
        }
    }
    None
}

fn push_to_ring(
    producer: &mut RingProducer,
    frame: &ff::frame::Audio,
    interleave_buf: &mut Vec<f32>,
    stop: &AtomicBool,
) {
    let format = frame.format();
    if !matches!(format, ff::format::Sample::F32(_)) {
        return;
    }
    let n_samples = frame.samples();
    if n_samples == 0 {
        return;
    }
    let n_planes = frame.planes();
    if n_planes == 0 {
        return;
    }

    interleave_buf.clear();
    if format.is_packed() {
        let plane = frame.plane::<f32>(0);
        let per_frame_floats = (plane.len() / n_samples).max(1);
        let needed = n_samples * per_frame_floats;
        if plane.len() < needed {
            return;
        }
        interleave_buf.extend_from_slice(&plane[..needed]);
    } else {
        interleave_buf.reserve(n_samples * n_planes);
        let planes: Vec<&[f32]> = (0..n_planes).map(|c| frame.plane::<f32>(c)).collect();
        for s in 0..n_samples {
            for plane in &planes {
                if s < plane.len() {
                    interleave_buf.push(plane[s]);
                }
            }
        }
    }
    let buf = interleave_buf.as_slice();
    if buf.is_empty() {
        return;
    }

    let mut written = 0;
    while written < buf.len() {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let n = producer.push_slice(&buf[written..]);
        written += n;
        if n == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
