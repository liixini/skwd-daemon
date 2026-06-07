#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap)]
#![allow(clippy::needless_range_loop, clippy::needless_pass_by_value)]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use image::{DynamicImage, ImageReader, Rgba, RgbaImage};
use rayon::prelude::*;
use serde_json::{json, Value};

use super::themes;

pub fn preview_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("effects-preview")
}

pub fn preview_path(cache_dir: &Path, input: &Path, suffix: &str) -> anyhow::Result<PathBuf> {
    let stem = input
        .file_stem()
        .ok_or_else(|| anyhow::anyhow!("input has no stem: {}", input.display()))?
        .to_string_lossy()
        .into_owned();
    let ext = input
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("png")
        .to_owned();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    Ok(preview_dir(cache_dir).join(format!("{ts}-{stem}-{suffix}.{ext}")))
}

pub fn library_path(input: &Path, suffix: &str) -> anyhow::Result<PathBuf> {
    let parent = input
        .parent()
        .ok_or_else(|| anyhow::anyhow!("input has no parent: {}", input.display()))?;
    let stem = input
        .file_stem()
        .ok_or_else(|| anyhow::anyhow!("input has no stem: {}", input.display()))?
        .to_string_lossy()
        .into_owned();
    let ext = input
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("png")
        .to_owned();
    Ok(parent.join("effects").join(format!("{stem}-{suffix}.{ext}")))
}

pub fn suffix(effect: &str, params: &Value) -> String {
    if let Some(def) = super::registry::find(effect) {
        return def.suffix.map_or_else(|| effect.to_string(), |f| f(params));
    }
    match effect {
        "theme" => {
            let theme = params
                .get("theme")
                .and_then(|v| v.as_str())
                .unwrap_or("Catppuccin");
            format!(
                "theme-{}",
                theme.to_lowercase().replace(' ', "-")
            )
        }
        other => other.to_string(),
    }
}

fn with_cat(mut schema: Value, category: &str) -> Value {
    schema["category"] = Value::String(category.to_string());
    schema
}

pub fn list() -> Value {
    let mut effects = vec![
        with_cat(theme_schema(), "Colour"),
        with_cat(simple_schema("invert",    "Invert",    "Invert every colour channel."), "Adjust"),
        with_cat(simple_schema("flip",      "Flip",      "Flip the image vertically."), "Transform"),
        with_cat(simple_schema("mirror",    "Mirror",    "Mirror the image horizontally."), "Transform"),
        with_cat(simple_schema("grayscale", "Grayscale", "Drop colour, keep luminance."), "Adjust"),
        with_cat(brightness_schema(), "Adjust"),
        with_cat(contrast_schema(), "Adjust"),
        with_cat(saturation_schema(), "Adjust"),
        with_cat(gamma_schema(), "Adjust"),
        with_cat(pixelate_schema(), "Stylize"),
        with_cat(border_schema(), "Transform"),
        with_cat(round_schema(), "Transform"),
    ];
    effects.extend(super::registry::schemas());
    Value::Array(effects)
}

pub async fn render(effect: &str, input: &Path, params: &Value, output: &Path) -> anyhow::Result<()> {
    let effect = effect.to_string();
    let input = input.to_owned();
    let output = output.to_owned();
    let params = params.clone();

    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let img = ImageReader::open(&input)?
            .with_guessed_format()?
            .decode()?;
        let out = match super::registry::find(&effect) {
            Some(def) => (def.render)(img, &params)?,
            None => render_sync(&effect, img, &params)?,
        };
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        out.save(&output)?;
        Ok(())
    })
    .await??;

    Ok(())
}

fn render_sync(effect: &str, img: DynamicImage, params: &Value) -> anyhow::Result<DynamicImage> {
    match effect {
        "theme"      => apply_theme(img, params),
        "invert"     => Ok(apply_invert(img)),
        "flip"       => Ok(image::imageops::flip_vertical(&img).into()),
        "mirror"     => Ok(image::imageops::flip_horizontal(&img).into()),
        "grayscale"  => Ok(img.grayscale()),
        "brightness" => Ok(apply_brightness(img, params)),
        "contrast"   => Ok(apply_contrast(img, params)),
        "saturation" => Ok(apply_saturation(img, params)),
        "gamma"      => Ok(apply_gamma(img, params)),
        "pixelate"   => Ok(apply_pixelate(img, params)),
        "border"     => Ok(apply_border(img, params)),
        "round"      => Ok(apply_round(img, params)),
        other        => anyhow::bail!("unknown effect: {other}"),
    }
}

fn simple_schema(id: &str, label: &str, description: &str) -> Value {
    json!({ "id": id, "label": label, "description": description, "params": [] })
}

fn theme_schema() -> Value {
    let options: Vec<Value> = themes::names()
        .into_iter()
        .map(|n| json!({ "mode": n, "label": n }))
        .collect();
    let default = themes::names().first().copied().unwrap_or("Catppuccin");
    json!({
        "id": "theme",
        "label": "Theme recolor",
        "description": "Snap every pixel to its nearest colour in a built-in palette.",
        "params": [
            { "id": "theme", "label": "Theme", "type": "dropdown",
              "default": default, "options": options }
        ]
    })
}

fn brightness_schema() -> Value {
    json!({
        "id": "brightness",
        "label": "Brightness",
        "description": "Multiply pixel luminance.",
        "params": [
            { "id": "factor", "label": "Factor", "type": "number",
              "min": 0.1, "max": 10.0, "step": 0.05, "decimals": 2, "default": 1.1 }
        ]
    })
}

fn contrast_schema() -> Value {
    json!({
        "id": "contrast",
        "label": "Contrast",
        "description": "Stretch or compress the tonal range.",
        "params": [
            { "id": "mode", "label": "Mode", "type": "dropdown", "default": "normal",
              "options": [
                  { "mode": "normal",  "label": "Normal" },
                  { "mode": "sigmoid", "label": "Sigmoid" }
              ] },
            { "id": "factor", "label": "Factor", "type": "number",
              "min": -100.0, "max": 100.0, "step": 1.0, "decimals": 1, "default": 25.0 }
        ]
    })
}

fn saturation_schema() -> Value {
    json!({
        "id": "saturation",
        "label": "Saturation",
        "description": "Boost or mute colour intensity.",
        "params": [
            { "id": "percentage", "label": "Percentage", "type": "integer",
              "min": -100, "max": 100, "step": 1, "default": 25 }
        ]
    })
}

fn gamma_schema() -> Value {
    json!({
        "id": "gamma",
        "label": "Gamma",
        "description": "Adjust the gamma curve.",
        "params": [
            { "id": "gamma", "label": "Gamma", "type": "number",
              "min": 0.1, "max": 5.0, "step": 0.05, "decimals": 2, "default": 1.0 }
        ]
    })
}

fn pixelate_schema() -> Value {
    json!({
        "id": "pixelate",
        "label": "Pixelate",
        "description": "Reduce the image to large blocky pixels.",
        "params": [
            { "id": "scale", "label": "Scale", "type": "integer",
              "min": 2, "max": 100, "step": 1, "default": 15 }
        ]
    })
}

fn border_schema() -> Value {
    json!({
        "id": "border",
        "label": "Border",
        "description": "Draw a coloured frame around the image.",
        "params": [
            { "id": "color",     "label": "Colour",    "type": "color",   "default": "#1a1a1a" },
            { "id": "thickness", "label": "Thickness", "type": "integer",
              "min": 0, "max": 500, "step": 1, "default": 30 },
            { "id": "radius",    "label": "Radius",    "type": "integer",
              "min": 0, "max": 500, "step": 1, "default": 0 }
        ]
    })
}

fn round_schema() -> Value {
    json!({
        "id": "round",
        "label": "Round corners",
        "description": "Round off the image corners.",
        "params": [
            { "id": "radius", "label": "Radius", "type": "integer",
              "min": 1, "max": 1000, "step": 1, "default": 60 }
        ]
    })
}

fn apply_invert(mut img: DynamicImage) -> DynamicImage {
    img.invert();
    img
}

fn apply_brightness(img: DynamicImage, params: &Value) -> DynamicImage {
    let factor = params.get("factor").and_then(serde_json::Value::as_f64).unwrap_or(1.1) as f32;
    let mut rgba = img.into_rgba8();
    for px in rgba.pixels_mut() {
        for i in 0..3 {
            px[i] = (f32::from(px[i]) * factor).clamp(0.0, 255.0) as u8;
        }
    }
    DynamicImage::ImageRgba8(rgba)
}

fn apply_gamma(img: DynamicImage, params: &Value) -> DynamicImage {
    let gamma = params.get("gamma").and_then(serde_json::Value::as_f64).unwrap_or(1.0).max(0.001) as f32;
    let inv = 1.0 / gamma;
    let mut lut = [0u8; 256];
    for v in 0..256 {
        let n = (v as f32 / 255.0).powf(inv);
        lut[v] = (n * 255.0).clamp(0.0, 255.0) as u8;
    }
    let mut rgba = img.into_rgba8();
    for px in rgba.pixels_mut() {
        px[0] = lut[px[0] as usize];
        px[1] = lut[px[1] as usize];
        px[2] = lut[px[2] as usize];
    }
    DynamicImage::ImageRgba8(rgba)
}

fn apply_contrast(img: DynamicImage, params: &Value) -> DynamicImage {
    let mode = params.get("mode").and_then(|v| v.as_str()).unwrap_or("normal");
    let factor = params.get("factor").and_then(serde_json::Value::as_f64).unwrap_or(25.0) as f32;
    let mut lut = [0u8; 256];

    if mode == "sigmoid" {
        let k = (factor / 25.0).clamp(-8.0, 8.0);
        let denom_lo = 1.0 + (-k * (-0.5) * 2.0_f32).exp();
        let denom_hi = 1.0 + (-k * (0.5)  * 2.0_f32).exp();
        let s_lo = 1.0 / denom_lo;
        let s_hi = 1.0 / denom_hi;
        let span = (s_hi - s_lo).abs().max(1e-6);
        for v in 0..256 {
            let n = v as f32 / 255.0;
            let s = 1.0 / (1.0 + (-k * (n - 0.5) * 2.0_f32).exp());
            let normed = (s - s_lo) / span;
            lut[v] = (normed * 255.0).clamp(0.0, 255.0) as u8;
        }
    } else {
        let f = ((factor + 100.0) / 100.0).max(0.0);
        for v in 0..256 {
            let out = (v as f32 - 127.5) * f + 127.5;
            lut[v] = out.clamp(0.0, 255.0) as u8;
        }
    }

    let mut rgba = img.into_rgba8();
    for px in rgba.pixels_mut() {
        px[0] = lut[px[0] as usize];
        px[1] = lut[px[1] as usize];
        px[2] = lut[px[2] as usize];
    }
    DynamicImage::ImageRgba8(rgba)
}

fn apply_saturation(img: DynamicImage, params: &Value) -> DynamicImage {
    let pct = params.get("percentage").and_then(serde_json::Value::as_i64).unwrap_or(25) as f32;
    let factor = 1.0 + pct / 100.0;
    let mut rgba = img.into_rgba8();
    for px in rgba.pixels_mut() {
        let r = f32::from(px[0]);
        let g = f32::from(px[1]);
        let b = f32::from(px[2]);
        let luma = 0.299 * r + 0.587 * g + 0.114 * b;
        px[0] = (luma + (r - luma) * factor).clamp(0.0, 255.0) as u8;
        px[1] = (luma + (g - luma) * factor).clamp(0.0, 255.0) as u8;
        px[2] = (luma + (b - luma) * factor).clamp(0.0, 255.0) as u8;
    }
    DynamicImage::ImageRgba8(rgba)
}

fn apply_pixelate(img: DynamicImage, params: &Value) -> DynamicImage {
    let scale = params
        .get("scale")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(15)
        .max(2) as u32;
    let (w, h) = (img.width(), img.height());
    let small_w = (w / scale).max(1);
    let small_h = (h / scale).max(1);
    let small = image::imageops::resize(&img, small_w, small_h, image::imageops::FilterType::Triangle);
    let big = image::imageops::resize(&small, w, h, image::imageops::FilterType::Nearest);
    DynamicImage::ImageRgba8(big)
}

fn parse_hex(s: &str) -> Option<(u8, u8, u8)> {
    let h = s.trim_start_matches('#');
    if h.len() != 6 {
        return None;
    }
    let r = u8::from_str_radix(&h[0..2], 16).ok()?;
    let g = u8::from_str_radix(&h[2..4], 16).ok()?;
    let b = u8::from_str_radix(&h[4..6], 16).ok()?;
    Some((r, g, b))
}

fn apply_border(img: DynamicImage, params: &Value) -> DynamicImage {
    let color = params.get("color").and_then(|v| v.as_str()).unwrap_or("#1a1a1a");
    let thickness = params
        .get("thickness")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(30)
        .max(0) as u32;
    let radius = params
        .get("radius")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        .max(0) as u32;

    let (r, g, b) = parse_hex(color).unwrap_or((26, 26, 26));
    let (w, h) = (img.width(), img.height());
    let new_w = w + thickness * 2;
    let new_h = h + thickness * 2;

    let rgba = img.into_rgba8();
    let mut out = RgbaImage::from_pixel(new_w, new_h, Rgba([r, g, b, 255]));
    image::imageops::overlay(&mut out, &rgba, i64::from(thickness), i64::from(thickness));

    if radius > 0 {
        apply_corner_mask(&mut out, radius);
    }

    DynamicImage::ImageRgba8(out)
}

fn apply_round(img: DynamicImage, params: &Value) -> DynamicImage {
    let radius = params
        .get("radius")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(60)
        .max(1) as u32;
    let mut rgba = img.into_rgba8();
    apply_corner_mask(&mut rgba, radius);
    DynamicImage::ImageRgba8(rgba)
}

fn apply_corner_mask(img: &mut RgbaImage, radius: u32) {
    let r = radius as f32;
    let (w, h) = (img.width(), img.height());
    if radius >= w / 2 || radius >= h / 2 {
        return;
    }

    let corners: [(u32, u32, f32, f32); 4] = [
        (0,            0,            r,                  r                 ),
        (w - radius,   0,            (w - radius) as f32, r                ),
        (0,            h - radius,   r,                  (h - radius) as f32),
        (w - radius,   h - radius,   (w - radius) as f32, (h - radius) as f32),
    ];

    for &(x0, y0, cx, cy) in corners.iter() {
        for dy in 0..radius {
            for dx in 0..radius {
                let px = x0 + dx;
                let py = y0 + dy;
                let fx = px as f32 + 0.5;
                let fy = py as f32 + 0.5;
                let dist = ((fx - cx).powi(2) + (fy - cy).powi(2)).sqrt();
                let alpha = if dist <= r - 0.5 {
                    1.0
                } else if dist >= r + 0.5 {
                    0.0
                } else {
                    (r + 0.5) - dist
                };
                if alpha < 1.0 {
                    let p = img.get_pixel_mut(px, py);
                    p[3] = (f32::from(p[3]) * alpha) as u8;
                }
            }
        }
    }
}

pub fn theme_image(img: DynamicImage, theme: &str) -> anyhow::Result<DynamicImage> {
    apply_theme(img, &json!({ "theme": theme }))
}

fn apply_theme(img: DynamicImage, params: &Value) -> anyhow::Result<DynamicImage> {
    let name = params
        .get("theme")
        .and_then(|v| v.as_str())
        .unwrap_or("Catppuccin");
    let palette = themes::lookup(name)
        .ok_or_else(|| anyhow::anyhow!("unknown theme: {name}"))?;
    if palette.is_empty() {
        anyhow::bail!("theme {name} has no colours");
    }

    let lut = build_palette_lut(palette, 50.0);

    let rgba = img.into_rgba8();
    let (w, h) = (rgba.width(), rgba.height());
    let mut raw = rgba.into_raw();

    raw.par_chunks_exact_mut(4).for_each(|chunk| {
        let i = (chunk[0] >> 3) as usize;
        let j = (chunk[1] >> 3) as usize;
        let k = (chunk[2] >> 3) as usize;
        let entry = lut[(i << 10) | (j << 5) | k];
        chunk[0] = entry[0];
        chunk[1] = entry[1];
        chunk[2] = entry[2];
    });

    let out = RgbaImage::from_raw(w, h, raw)
        .ok_or_else(|| anyhow::anyhow!("failed to rebuild image buffer"))?;
    Ok(DynamicImage::ImageRgba8(out))
}

fn build_palette_lut(palette: &[(u8, u8, u8)], sigma: f32) -> Vec<[u8; 3]> {
    const N: usize = 32;
    let two_sigma_sq = 2.0 * sigma * sigma;
    let palette: Vec<(f32, f32, f32)> = palette
        .iter()
        .map(|&(r, g, b)| (f32::from(r), f32::from(g), f32::from(b)))
        .collect();

    (0..N * N * N)
        .into_par_iter()
        .map(|idx| {
            let i = idx >> 10;
            let j = (idx >> 5) & 0x1f;
            let k = idx & 0x1f;
            let tr = (i * 8 + 4) as f32;
            let tg = (j * 8 + 4) as f32;
            let tb = (k * 8 + 4) as f32;

            let mut num_r = 0.0f64;
            let mut num_g = 0.0f64;
            let mut num_b = 0.0f64;
            let mut den = 0.0f64;

            for &(pr, pg, pb) in &palette {
                let dr = tr - pr;
                let dg = tg - pg;
                let db = tb - pb;
                let d2 = dr * dr + dg * dg + db * db;
                let w = f64::from((-d2 / two_sigma_sq).exp());
                num_r += f64::from(pr) * w;
                num_g += f64::from(pg) * w;
                num_b += f64::from(pb) * w;
                den += w;
            }

            let inv = 1.0 / den.max(1e-30);
            [
                (num_r * inv).clamp(0.0, 255.0) as u8,
                (num_g * inv).clamp(0.0, 255.0) as u8,
                (num_b * inv).clamp(0.0, 255.0) as u8,
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn suffix_themes_and_passthrough() {
        assert_eq!(suffix("theme", &json!({ "theme": "Tokyo Night" })), "theme-tokyo-night");
        assert_eq!(suffix("theme", &json!({})), "theme-catppuccin");
        assert_eq!(suffix("invert", &json!({})), "invert");
        assert_eq!(suffix("pixelate", &json!({ "n": 8 })), "pixelate");
    }

    #[test]
    fn library_path_places_in_effects_dir() {
        let p = library_path(Path::new("/wall/pic.png"), "invert").unwrap();
        assert_eq!(p, PathBuf::from("/wall/effects/pic-invert.png"));
        let p2 = library_path(Path::new("/wall/noext"), "flip").unwrap();
        assert_eq!(p2, PathBuf::from("/wall/effects/noext-flip.png"));
        assert!(library_path(Path::new("/"), "x").is_err());
    }

    #[test]
    fn preview_path_uses_cache_preview_dir() {
        let p = preview_path(Path::new("/c"), Path::new("/wall/pic.jpg"), "grayscale").unwrap();
        let s = p.to_string_lossy();
        assert!(s.ends_with("-pic-grayscale.jpg"), "{s}");
        assert!(s.contains("/c/"), "{s}");
        assert!(preview_path(Path::new("/c"), Path::new("/"), "x").is_err());
    }

    // Contract: skwd-wall's EffectsPanel.qml reads id/label/description/params off each
    // effects.list item, and id/label/type off each param.
    #[test]
    fn list_item_and_param_contract() {
        let v = list();
        let arr = v.as_array().unwrap();
        assert!(arr.len() >= 10);
        let names: Vec<&str> = arr.iter().filter_map(|e| e.get("id").and_then(|i| i.as_str())).collect();
        assert!(names.contains(&"invert"));
        assert!(names.contains(&"grayscale"));

        let invert = arr.iter().find(|e| e["id"] == "invert").unwrap();
        assert!(invert["label"].is_string());
        assert!(invert["description"].is_string());
        assert!(invert["params"].is_array());

        for effect in arr {
            for p in effect["params"].as_array().unwrap() {
                assert!(p["id"].is_string(), "param missing id in {}", effect["id"]);
                assert!(p["label"].is_string(), "param missing label in {}", effect["id"]);
                assert!(p["type"].is_string(), "param missing type in {}", effect["id"]);
            }
        }
    }

    #[test]
    fn every_effect_has_nonempty_category() {
        let v = list();
        for effect in v.as_array().unwrap() {
            let cat = effect["category"].as_str().unwrap_or("");
            assert!(!cat.is_empty(), "effect {} missing category", effect["id"]);
        }
        let theme = v.as_array().unwrap().iter().find(|e| e["id"] == "theme").unwrap();
        assert_eq!(theme["category"], "Colour");
    }
}
