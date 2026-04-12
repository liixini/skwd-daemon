use std::path::{Path, PathBuf};

use image::DynamicImage;
use tokio::process::Command;

use crate::util::{self, CommandExt};

fn tmp_path(dest: &Path) -> PathBuf {
    let stem = dest.file_stem().unwrap_or_default();
    let ext = dest.extension().unwrap_or_default();
    let mut name = stem.to_owned();
    name.push(".tmp.");
    name.push(ext);
    dest.with_file_name(name)
}

pub const THUMB_W: u32 = 640;
pub const THUMB_H: u32 = 360;
pub const SMALL_W: u32 = 240;
pub const SMALL_H: u32 = 135;

pub struct ThumbResult {
    pub thumb_path: String,
    pub thumb_sm_path: String,
    pub hue: u16,
    pub sat: u16,
}

pub async fn generate_static(src: &Path, thumb_path: &Path, thumb_sm_path: &Path) -> anyhow::Result<ThumbResult> {
    if let Some(parent) = thumb_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    let src_arg = format!("{}[0]", src.display());
    let tmp_thumb = tmp_path(thumb_path);
    let mut cmd = Command::new("magick");
    cmd.args([
        src_arg.as_ref(),
        "-resize".as_ref(),
        format!("{THUMB_W}x{THUMB_H}^").as_ref(),
        "-gravity".as_ref(),
        "center".as_ref(),
        "-extent".as_ref(),
        format!("{THUMB_W}x{THUMB_H}").as_ref(),
        "-quality".as_ref(),
        "85".as_ref(),
        tmp_thumb.as_os_str(),
    ])
    .silent();
    let status = util::timed_status(&mut cmd, util::CMD_TIMEOUT).await?;

    if !status.success() {
        let _ = tokio::fs::remove_file(&tmp_thumb).await;
        anyhow::bail!("magick failed for {}", src.display());
    }
    tokio::fs::rename(&tmp_thumb, thumb_path).await?;

    generate_small_thumb(thumb_path, thumb_sm_path).await?;
    let (hue, sat) = extract_hue_sat_from_file(thumb_path).await;

    Ok(ThumbResult {
        thumb_path: thumb_path.display().to_string(),
        thumb_sm_path: thumb_sm_path.display().to_string(),
        hue,
        sat,
    })
}

pub async fn generate_video(
    src: &Path,
    thumb_path: &Path,
    thumb_sm_path: &Path,
    seek_sec: u32,
) -> anyhow::Result<ThumbResult> {
    if let Some(parent) = thumb_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    let tmp_thumb = tmp_path(thumb_path);
    let mut cmd = Command::new("ffmpeg");
    cmd.args([
        "-y",
        "-ss",
        &seek_sec.to_string(),
        "-i",
        &src.display().to_string(),
        "-vf",
        &format!("scale={THUMB_W}:{THUMB_H}:force_original_aspect_ratio=increase,crop={THUMB_W}:{THUMB_H}"),
        "-frames:v",
        "1",
        "-update",
        "1",
        &tmp_thumb.display().to_string(),
    ])
    .silent();
    let status = util::timed_status(&mut cmd, util::CMD_TIMEOUT).await?;

    if !status.success() {
        let _ = tokio::fs::remove_file(&tmp_thumb).await;
        anyhow::bail!("ffmpeg failed for {}", src.display());
    }
    tokio::fs::rename(&tmp_thumb, thumb_path).await?;

    generate_small_thumb(thumb_path, thumb_sm_path).await?;
    let (hue, sat) = extract_hue_sat_from_file(thumb_path).await;

    Ok(ThumbResult {
        thumb_path: thumb_path.display().to_string(),
        thumb_sm_path: thumb_sm_path.display().to_string(),
        hue,
        sat,
    })
}

pub async fn generate_small_and_colors(thumb_path: &Path, thumb_sm_path: &Path) -> anyhow::Result<(u16, u16)> {
    generate_small_thumb(thumb_path, thumb_sm_path).await?;
    Ok(extract_hue_sat_from_file(thumb_path).await)
}

async fn generate_small_thumb(thumb_path: &Path, thumb_sm_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = thumb_sm_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let tmp_sm = tmp_path(thumb_sm_path);
    let mut cmd = Command::new("magick");
    cmd.args([
        thumb_path.as_os_str(),
        "-resize".as_ref(),
        format!("{SMALL_W}x{SMALL_H}^").as_ref(),
        "-gravity".as_ref(),
        "center".as_ref(),
        "-extent".as_ref(),
        format!("{SMALL_W}x{SMALL_H}").as_ref(),
        "-quality".as_ref(),
        "85".as_ref(),
        tmp_sm.as_os_str(),
    ])
    .silent();
    let status = util::timed_status(&mut cmd, util::CMD_TIMEOUT).await?;

    if !status.success() {
        let _ = tokio::fs::remove_file(&tmp_sm).await;
        anyhow::bail!("magick small thumb failed for {}", thumb_path.display());
    }
    tokio::fs::rename(&tmp_sm, thumb_sm_path).await?;
    Ok(())
}

async fn extract_hue_sat_from_file(path: &Path) -> (u16, u16) {
    let path = path.to_path_buf();
    match tokio::task::spawn_blocking(move || -> anyhow::Result<(u16, u16)> {
        let img = image::open(&path)?;
        Ok(extract_hue_sat(&img))
    })
    .await
    {
        Ok(Ok(hs)) => hs,
        Ok(Err(_)) | Err(_) => (0, 0),
    }
}

#[must_use]
pub fn extract_hue_sat(img: &DynamicImage) -> (u16, u16) {
    let rgba = img.to_rgba8();
    let (tr, tg, tb, cnt) = rgba.pixels().fold((0u64, 0u64, 0u64, 0u64), |(r, g, b, c), px| {
        (r + u64::from(px[0]), g + u64::from(px[1]), b + u64::from(px[2]), c + 1)
    });
    if cnt == 0 {
        return (0, 0);
    }
    let r = (tr / cnt) as f64 / 255.0;
    let g = (tg / cnt) as f64 / 255.0;
    let b = (tb / cnt) as f64 / 255.0;

    let max = r.max(g).max(b);
    let min = r.min(g).min(b);
    let delta = max - min;

    let hue = if delta < 1e-6 {
        0.0
    } else if (max - r).abs() < 1e-6 {
        60.0 * (((g - b) / delta) % 6.0)
    } else if (max - g).abs() < 1e-6 {
        60.0 * (((b - r) / delta) + 2.0)
    } else {
        60.0 * (((r - g) / delta) + 4.0)
    };
    let hue = if hue < 0.0 { hue + 360.0 } else { hue };

    let lightness = (max + min) / 2.0;
    let sat = if delta < 1e-6 {
        0.0
    } else {
        delta / (1.0 - (2.0 * lightness - 1.0).abs())
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    (hue.round() as u16, (sat * 100.0).round() as u16)
}

#[must_use]
pub fn hue_bucket(hue: u16, sat: u16) -> u16 {
    if sat < 10 {
        return 99;
    }
    if !(25..340).contains(&hue) {
        return 0;
    }
    ((hue.wrapping_sub(25)) / 30) + 1
}

#[allow(dead_code)]
pub fn small_thumb_path(thumb_path: &str) -> String {
    thumb_path
        .replace("/thumbs/", "/thumbs-sm/")
        .replace("/we-thumbs/", "/thumbs-sm/we-")
        .replace("/video-thumbs/", "/thumbs-sm/vid-")
}

#[must_use]
pub fn cache_key(thumb_path: &str) -> String {
    let fname = thumb_path.rsplit('/').next().unwrap_or(thumb_path);
    fname.rsplit_once('.').map_or(fname, |(stem, _)| stem).to_string()
}
