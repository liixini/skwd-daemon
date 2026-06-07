#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss, clippy::cast_possible_wrap)]

use image::{DynamicImage, RgbaImage};
use rayon::prelude::*;
use serde_json::{json, Value};

use super::imgutil::{clampu8, f64_param, hash01, i64_param, luma, str_param};
use super::registry::EffectDef;
use super::themes;

pub fn effects() -> Vec<EffectDef> {
    vec![
        EffectDef { id: "dither", schema: dither_schema, render: apply_dither, suffix: Some(dither_suffix) },
        EffectDef { id: "scanlines", schema: scanlines_schema, render: |i, p| Ok(apply_scanlines(i, p)), suffix: None },
        EffectDef { id: "grain", schema: grain_schema, render: |i, p| Ok(apply_grain(i, p)), suffix: None },
        EffectDef { id: "sharpen", schema: sharpen_schema, render: |i, p| Ok(apply_convolution(i, p, Kernel::Sharpen)), suffix: None },
        EffectDef { id: "emboss", schema: emboss_schema, render: |i, _| Ok(apply_convolution(i, &Value::Null, Kernel::Emboss)), suffix: None },
        EffectDef { id: "edge", schema: edge_schema, render: |i, p| Ok(apply_edge(i, p)), suffix: None },
        EffectDef { id: "halftone", schema: halftone_schema, render: |i, p| Ok(apply_halftone(i, p)), suffix: None },
    ]
}

fn theme_options() -> Vec<Value> {
    themes::names().into_iter().map(|n| json!({ "mode": n, "label": n })).collect()
}

fn dither_suffix(params: &Value) -> String {
    let t = str_param(params, "theme", "Catppuccin");
    format!("dither-{}", t.to_lowercase().replace(' ', "-"))
}

fn dither_schema() -> Value {
    json!({
        "id": "dither", "label": "Dither to theme",
        "description": "Floyd-Steinberg dither down to a theme palette.",
        "params": [
            { "id": "theme", "label": "Theme", "type": "dropdown",
              "default": themes::names().first().copied().unwrap_or("Catppuccin"),
              "options": theme_options() }
        ]
    })
}

fn nearest(palette: &[(i32, i32, i32)], r: i32, g: i32, b: i32) -> (i32, i32, i32) {
    let mut best = palette[0];
    let mut bd = i32::MAX;
    for &(pr, pg, pb) in palette {
        let d = (r - pr).pow(2) + (g - pg).pow(2) + (b - pb).pow(2);
        if d < bd {
            bd = d;
            best = (pr, pg, pb);
        }
    }
    best
}

fn apply_dither(img: DynamicImage, params: &Value) -> anyhow::Result<DynamicImage> {
    let name = str_param(params, "theme", "Catppuccin");
    let palette: Vec<(i32, i32, i32)> = themes::lookup(name)
        .ok_or_else(|| anyhow::anyhow!("unknown theme: {name}"))?
        .iter()
        .map(|&(r, g, b)| (i32::from(r), i32::from(g), i32::from(b)))
        .collect();
    if palette.is_empty() {
        anyhow::bail!("theme {name} has no colours");
    }
    let rgba = img.into_rgba8();
    let (w, h) = (rgba.width(), rgba.height());
    let mut buf: Vec<[i32; 3]> = rgba
        .pixels()
        .map(|p| [i32::from(p[0]), i32::from(p[1]), i32::from(p[2])])
        .collect();
    let alpha: Vec<u8> = rgba.pixels().map(|p| p[3]).collect();
    let idx = |x: u32, y: u32| (y * w + x) as usize;

    for y in 0..h {
        for x in 0..w {
            let i = idx(x, y);
            let old = buf[i];
            let new = nearest(&palette, old[0], old[1], old[2]);
            buf[i] = [new.0, new.1, new.2];
            let err = [old[0] - new.0, old[1] - new.1, old[2] - new.2];
            let mut spread = |nx: i64, ny: i64, num: i32| {
                if nx < 0 || ny < 0 || nx >= i64::from(w) || ny >= i64::from(h) {
                    return;
                }
                let j = idx(nx as u32, ny as u32);
                for k in 0..3 {
                    buf[j][k] += err[k] * num / 16;
                }
            };
            let (xi, yi) = (i64::from(x), i64::from(y));
            spread(xi + 1, yi, 7);
            spread(xi - 1, yi + 1, 3);
            spread(xi, yi + 1, 5);
            spread(xi + 1, yi + 1, 1);
        }
    }

    let mut out = RgbaImage::new(w, h);
    for (i, px) in out.pixels_mut().enumerate() {
        *px = image::Rgba([
            clampu8(buf[i][0] as f32),
            clampu8(buf[i][1] as f32),
            clampu8(buf[i][2] as f32),
            alpha[i],
        ]);
    }
    Ok(DynamicImage::ImageRgba8(out))
}

fn scanlines_schema() -> Value {
    json!({
        "id": "scanlines", "label": "Scanlines",
        "description": "Darken alternating rows for a CRT look.",
        "params": [
            { "id": "spacing", "label": "Spacing", "type": "integer",
              "min": 2, "max": 20, "step": 1, "default": 3 },
            { "id": "intensity", "label": "Intensity", "type": "number",
              "min": 0.0, "max": 1.0, "step": 0.05, "decimals": 2, "default": 0.4 }
        ]
    })
}

fn apply_scanlines(img: DynamicImage, params: &Value) -> DynamicImage {
    let spacing = i64_param(params, "spacing", 3).max(2) as u32;
    let intensity = f64_param(params, "intensity", 0.4).clamp(0.0, 1.0) as f32;
    let rgba = img.into_rgba8();
    let (w, h) = (rgba.width(), rgba.height());
    let mut raw = rgba.into_raw();
    raw.par_chunks_exact_mut(4).enumerate().for_each(|(idx, c)| {
        let y = idx as u32 / w;
        if y.is_multiple_of(spacing) {
            let dark = 1.0 - intensity;
            c[0] = clampu8(f32::from(c[0]) * dark);
            c[1] = clampu8(f32::from(c[1]) * dark);
            c[2] = clampu8(f32::from(c[2]) * dark);
        }
    });
    DynamicImage::ImageRgba8(RgbaImage::from_raw(w, h, raw).unwrap())
}

fn grain_schema() -> Value {
    json!({
        "id": "grain", "label": "Film grain",
        "description": "Add monochrome noise.",
        "params": [
            { "id": "amount", "label": "Amount", "type": "integer",
              "min": 1, "max": 100, "step": 1, "default": 25 }
        ]
    })
}

fn apply_grain(img: DynamicImage, params: &Value) -> DynamicImage {
    let amount = i64_param(params, "amount", 25).clamp(0, 255) as f32;
    let rgba = img.into_rgba8();
    let w = rgba.width();
    let h = rgba.height();
    let mut raw = rgba.into_raw();
    raw.par_chunks_exact_mut(4).enumerate().for_each(|(idx, c)| {
        let n = (hash01(idx as u32) - 0.5) * 2.0 * amount;
        c[0] = clampu8(f32::from(c[0]) + n);
        c[1] = clampu8(f32::from(c[1]) + n);
        c[2] = clampu8(f32::from(c[2]) + n);
    });
    DynamicImage::ImageRgba8(RgbaImage::from_raw(w, h, raw).unwrap())
}

#[derive(Clone, Copy)]
enum Kernel {
    Sharpen,
    Emboss,
}

fn sharpen_schema() -> Value {
    json!({
        "id": "sharpen", "label": "Sharpen",
        "description": "Crisp up edges (unsharp).",
        "params": [
            { "id": "amount", "label": "Amount", "type": "number",
              "min": 0.0, "max": 3.0, "step": 0.1, "decimals": 1, "default": 1.0 }
        ]
    })
}

fn emboss_schema() -> Value {
    json!({ "id": "emboss", "label": "Emboss", "description": "Grey relief from edges.", "params": [] })
}

fn apply_convolution(img: DynamicImage, params: &Value, kernel: Kernel) -> DynamicImage {
    let amount = f64_param(params, "amount", 1.0).clamp(0.0, 5.0) as f32;
    let k: [f32; 9] = match kernel {
        Kernel::Sharpen => {
            let c = 1.0 + 4.0 * amount;
            let e = -amount;
            [0.0, e, 0.0, e, c, e, 0.0, e, 0.0]
        }
        Kernel::Emboss => [-2.0, -1.0, 0.0, -1.0, 1.0, 1.0, 0.0, 1.0, 2.0],
    };
    let bias = matches!(kernel, Kernel::Emboss);
    let src = img.into_rgba8();
    let (w, h) = (src.width(), src.height());
    let mut out = RgbaImage::new(w, h);
    let getp = |x: i64, y: i64, ch: usize| -> f32 {
        let xx = x.clamp(0, i64::from(w) - 1) as u32;
        let yy = y.clamp(0, i64::from(h) - 1) as u32;
        f32::from(src.get_pixel(xx, yy)[ch])
    };
    for y in 0..h {
        for x in 0..w {
            let p = out.get_pixel_mut(x, y);
            for ch in 0..3 {
                let mut acc = 0.0;
                let mut ki = 0;
                for dy in -1i64..=1 {
                    for dx in -1i64..=1 {
                        acc += getp(i64::from(x) + dx, i64::from(y) + dy, ch) * k[ki];
                        ki += 1;
                    }
                }
                if bias {
                    acc += 128.0;
                }
                p[ch] = clampu8(acc);
            }
            p[3] = src.get_pixel(x, y)[3];
        }
    }
    DynamicImage::ImageRgba8(out)
}

fn edge_schema() -> Value {
    json!({
        "id": "edge", "label": "Edge detect",
        "description": "Sobel outline on a dark background.",
        "params": [
            { "id": "threshold", "label": "Threshold", "type": "integer",
              "min": 0, "max": 255, "step": 1, "default": 60 }
        ]
    })
}

fn apply_edge(img: DynamicImage, params: &Value) -> DynamicImage {
    let thr = i64_param(params, "threshold", 60).clamp(0, 255) as f32;
    let src = img.into_rgba8();
    let (w, h) = (src.width(), src.height());
    let lum = |x: i64, y: i64| -> f32 {
        let xx = x.clamp(0, i64::from(w) - 1) as u32;
        let yy = y.clamp(0, i64::from(h) - 1) as u32;
        let p = src.get_pixel(xx, yy);
        luma(p[0], p[1], p[2])
    };
    let mut out = RgbaImage::new(w, h);
    for y in 0..h {
        for x in 0..w {
            let (xi, yi) = (i64::from(x), i64::from(y));
            let gx = -lum(xi - 1, yi - 1) - 2.0 * lum(xi - 1, yi) - lum(xi - 1, yi + 1)
                + lum(xi + 1, yi - 1) + 2.0 * lum(xi + 1, yi) + lum(xi + 1, yi + 1);
            let gy = -lum(xi - 1, yi - 1) - 2.0 * lum(xi, yi - 1) - lum(xi + 1, yi - 1)
                + lum(xi - 1, yi + 1) + 2.0 * lum(xi, yi + 1) + lum(xi + 1, yi + 1);
            let mag = (gx * gx + gy * gy).sqrt();
            let v = if mag >= thr { clampu8(mag) } else { 0 };
            out.put_pixel(x, y, image::Rgba([v, v, v, src.get_pixel(x, y)[3]]));
        }
    }
    DynamicImage::ImageRgba8(out)
}

fn halftone_schema() -> Value {
    json!({
        "id": "halftone", "label": "Halftone",
        "description": "Newspaper-style dot pattern.",
        "params": [
            { "id": "size", "label": "Dot size", "type": "integer",
              "min": 3, "max": 40, "step": 1, "default": 8 }
        ]
    })
}

fn apply_halftone(img: DynamicImage, params: &Value) -> DynamicImage {
    let cell = i64_param(params, "size", 8).max(2) as u32;
    let src = img.into_rgba8();
    let (w, h) = (src.width(), src.height());
    let mut out = RgbaImage::from_pixel(w, h, image::Rgba([255, 255, 255, 255]));
    let mut cy = 0;
    while cy < h {
        let mut cx = 0;
        while cx < w {
            let mut sum = 0.0f32;
            let mut n = 0.0f32;
            for y in cy..(cy + cell).min(h) {
                for x in cx..(cx + cell).min(w) {
                    let p = src.get_pixel(x, y);
                    sum += luma(p[0], p[1], p[2]);
                    n += 1.0;
                }
            }
            let avg = if n > 0.0 { sum / n } else { 255.0 };
            let darkness = 1.0 - avg / 255.0;
            let max_r = cell as f32 / 2.0;
            let r = darkness.sqrt() * max_r;
            let ccx = cx as f32 + cell as f32 / 2.0;
            let ccy = cy as f32 + cell as f32 / 2.0;
            for y in cy..(cy + cell).min(h) {
                for x in cx..(cx + cell).min(w) {
                    let d = ((x as f32 - ccx).powi(2) + (y as f32 - ccy).powi(2)).sqrt();
                    if d <= r {
                        out.put_pixel(x, y, image::Rgba([20, 20, 20, 255]));
                    }
                }
            }
            cx += cell;
        }
        cy += cell;
    }
    DynamicImage::ImageRgba8(out)
}
