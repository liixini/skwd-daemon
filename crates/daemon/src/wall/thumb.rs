#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]

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
    pub richness: u16,
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
    ]);
    cmd.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped());

    let output = match tokio::time::timeout(util::CMD_TIMEOUT, cmd.output()).await {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => anyhow::bail!("magick spawn failed for {}: {e}", src.display()),
        Err(_) => anyhow::bail!("magick timed out for {}", src.display()),
    };
    if !output.status.success() {
        let _ = tokio::fs::remove_file(&tmp_thumb).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("magick failed for {}: {}", src.display(), stderr.trim());
    }
    tokio::fs::rename(&tmp_thumb, thumb_path).await?;

    generate_small_thumb(thumb_path, thumb_sm_path).await?;
    let (hue, sat, richness) = extract_hue_sat_from_file(thumb_path).await;

    Ok(ThumbResult {
        thumb_path: thumb_path.display().to_string(),
        thumb_sm_path: thumb_sm_path.display().to_string(),
        hue,
        sat,
        richness,
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
    let (hue, sat, richness) = extract_hue_sat_from_file(thumb_path).await;

    Ok(ThumbResult {
        thumb_path: thumb_path.display().to_string(),
        thumb_sm_path: thumb_sm_path.display().to_string(),
        hue,
        sat,
        richness,
    })
}

pub async fn generate_small_and_colors(thumb_path: &Path, thumb_sm_path: &Path) -> anyhow::Result<(u16, u16, u16)> {
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

async fn extract_hue_sat_from_file(path: &Path) -> (u16, u16, u16) {
    let path = path.to_path_buf();
    match tokio::task::spawn_blocking(move || -> anyhow::Result<(u16, u16, u16)> {
        let img = image::open(&path)?;
        Ok(extract_hue_sat(&img))
    })
    .await
    {
        Ok(Ok(hs)) => hs,
        Ok(Err(_)) | Err(_) => (0, 0, 0),
    }
}


#[must_use]
pub fn extract_hue_sat(img: &DynamicImage) -> (u16, u16, u16) {
    let rgba = img.to_rgba8();
    
    
    let mut counts = [0u64; 13];
    let mut meaningful = 0u64;

    for px in rgba.pixels() {
        let r = f64::from(px[0]) / 255.0;
        let g = f64::from(px[1]) / 255.0;
        let b = f64::from(px[2]) / 255.0;
        let max = r.max(g).max(b);
        let min = r.min(g).min(b);
        let delta = max - min;
        let lightness = (max + min) / 2.0;

        
        if !(0.06..=0.94).contains(&lightness) {
            continue;
        }

        let sat = if delta < 1e-6 {
            0.0
        } else {
            delta / (1.0 - (2.0f64).mul_add(lightness, -1.0).abs())
        };

        
        if sat < 0.18 {
            counts[12] += 1;
            meaningful += 1;
            continue;
        }

        let hue = if (max - r).abs() < 1e-6 {
            60.0 * (((g - b) / delta) % 6.0)
        } else if (max - g).abs() < 1e-6 {
            60.0f64.mul_add((b - r) / delta, 120.0)
        } else {
            60.0f64.mul_add((r - g) / delta, 240.0)
        };
        let hue = if hue < 0.0 { hue + 360.0 } else { hue };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let hue_u = (hue.round() as u16) % 360;

        
        let bucket = hue_to_bucket_idx(hue_u);
        counts[bucket] += 1;
        meaningful += 1;
    }

    if meaningful == 0 {
        return (0, 0, 0);
    }

    
    let (mut best_idx, mut best_count) = (0usize, 0u64);
    for (i, &c) in counts.iter().enumerate().take(12) {
        if c > best_count {
            best_count = c;
            best_idx = i;
        }
    }

    
    let chromatic_mass: u64 = counts[..12].iter().sum();
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::cast_sign_loss)]
    let richness: u16 = if chromatic_mass == 0 {
        0
    } else {
        let total = chromatic_mass as f64;
        let mut sumsq = 0.0_f64;
        for &c in &counts[..12] {
            if c == 0 { continue; }
            let p = c as f64 / total;
            sumsq += p * p;
        }
        if sumsq > 0.0 {
            ((1.0 / sumsq) * 100.0).round().clamp(0.0, 1500.0) as u16
        } else {
            0
        }
    };

    
    if chromatic_mass * 100 < meaningful * 5 {
        return (0, 0, richness);
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::cast_sign_loss)]
    let coverage = ((best_count as f64 / meaningful as f64) * 100.0).round() as u16;

    
    let hue_for_bucket: u16 = match best_idx {
        0 => 10,
        10 => 307,
        11 => 337,
        n => 25 + (n as u16 - 1) * 30 + 15,
    };
    
    (hue_for_bucket, coverage.clamp(10, 100), richness)
}

#[must_use]
pub fn hue_bucket(hue: u16, sat: u16) -> u16 {
    if sat < 10 {
        return 99;
    }
    hue_to_bucket_idx(hue) as u16
}


fn hue_to_bucket_idx(hue: u16) -> usize {
    if !(25..355).contains(&hue) {
        return 0;
    }
    if hue >= 320 {
        return 11;
    }
    if hue >= 295 {
        return 10;
    }
    ((hue - 25) / 30 + 1) as usize
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tmp_path_inserts_tmp_segment_before_extension() {
        assert_eq!(tmp_path(Path::new("/c/wall.webp")), PathBuf::from("/c/wall.tmp.webp"));
        assert_eq!(tmp_path(Path::new("thumb.png")), PathBuf::from("thumb.tmp.png"));
    }

    #[test]
    fn cache_key_strips_dir_and_extension() {
        assert_eq!(cache_key("/cache/thumbs/wall.webp"), "wall");
        assert_eq!(cache_key("wall.png"), "wall");
        assert_eq!(cache_key("noext"), "noext");
        assert_eq!(cache_key("/d/archive.tar.gz"), "archive.tar");
    }

    #[test]
    fn hue_bucket_is_greyscale_when_low_saturation() {
        assert_eq!(hue_bucket(200, 9), 99);
        assert_eq!(hue_bucket(0, 0), 99);
    }

    #[test]
    fn hue_bucket_maps_colored_hues() {
        assert_eq!(hue_bucket(0, 50), 0);
        assert_eq!(hue_bucket(10, 50), 0);
        assert_eq!(hue_bucket(355, 50), 0);
        assert_eq!(hue_bucket(330, 50), 11);
        assert_eq!(hue_bucket(300, 50), 10);
        assert_eq!(hue_bucket(25, 50), 1);
        assert_eq!(hue_bucket(120, 50), 4);
    }
}
