#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss, clippy::cast_possible_wrap)]

use image::{DynamicImage, RgbaImage};
use rayon::prelude::*;
use serde_json::{json, Value};

use super::imgutil::{
    clampu8, color_param, f64_param, gradient_sample, hsl_to_rgb, i64_param, lerp, luma, rgb_to_hsl,
    str_param,
};
use super::registry::EffectDef;
use super::themes;

pub fn effects() -> Vec<EffectDef> {
    vec![
        EffectDef { id: "duotone", schema: duotone_schema, render: |i, p| Ok(apply_duotone(i, p)), suffix: None },
        EffectDef { id: "gradientmap", schema: gradientmap_schema, render: apply_gradientmap, suffix: Some(theme_suffix) },
        EffectDef { id: "sepia", schema: sepia_schema, render: |i, p| Ok(apply_sepia(i, p)), suffix: None },
        EffectDef { id: "hue", schema: hue_schema, render: |i, p| Ok(apply_hue(i, p)), suffix: None },
        EffectDef { id: "temperature", schema: temperature_schema, render: |i, p| Ok(apply_temperature(i, p)), suffix: None },
        EffectDef { id: "tint", schema: tint_schema, render: |i, p| Ok(apply_tint(i, p)), suffix: None },
        EffectDef { id: "chromatic", schema: chromatic_schema, render: |i, p| Ok(apply_chromatic(i, p)), suffix: None },
    ]
}

fn theme_suffix(params: &Value) -> String {
    let theme = str_param(params, "theme", "Catppuccin");
    format!("gradientmap-{}", theme.to_lowercase().replace(' ', "-"))
}

fn theme_options() -> Vec<Value> {
    themes::names().into_iter().map(|n| json!({ "mode": n, "label": n })).collect()
}

fn duotone_schema() -> Value {
    json!({
        "id": "duotone", "label": "Duotone",
        "description": "Map shadows and highlights to two colours.",
        "params": [
            { "id": "shadow",    "label": "Shadow",    "type": "color", "default": "#1e1e2e" },
            { "id": "highlight", "label": "Highlight", "type": "color", "default": "#cdd6f4" }
        ]
    })
}

fn apply_duotone(img: DynamicImage, params: &Value) -> DynamicImage {
    let lo = color_param(params, "shadow", "#1e1e2e");
    let hi = color_param(params, "highlight", "#cdd6f4");
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        let t = luma(c[0], c[1], c[2]) / 255.0;
        c[0] = lerp(lo.0, hi.0, t);
        c[1] = lerp(lo.1, hi.1, t);
        c[2] = lerp(lo.2, hi.2, t);
    });
    DynamicImage::ImageRgba8(rgba)
}

fn gradientmap_schema() -> Value {
    json!({
        "id": "gradientmap", "label": "Gradient map",
        "description": "Map brightness across a theme's palette as a gradient.",
        "params": [
            { "id": "theme", "label": "Theme", "type": "dropdown",
              "default": themes::names().first().copied().unwrap_or("Catppuccin"),
              "options": theme_options() }
        ]
    })
}

fn apply_gradientmap(img: DynamicImage, params: &Value) -> anyhow::Result<DynamicImage> {
    let name = str_param(params, "theme", "Catppuccin");
    let mut palette = themes::lookup(name)
        .ok_or_else(|| anyhow::anyhow!("unknown theme: {name}"))?
        .to_vec();
    if palette.is_empty() {
        anyhow::bail!("theme {name} has no colours");
    }
    palette.sort_by(|a, b| luma(a.0, a.1, a.2).partial_cmp(&luma(b.0, b.1, b.2)).unwrap());
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        let t = luma(c[0], c[1], c[2]) / 255.0;
        let (r, g, b) = gradient_sample(&palette, t);
        c[0] = r;
        c[1] = g;
        c[2] = b;
    });
    Ok(DynamicImage::ImageRgba8(rgba))
}

fn sepia_schema() -> Value {
    json!({
        "id": "sepia", "label": "Sepia",
        "description": "Warm vintage monochrome.",
        "params": [
            { "id": "intensity", "label": "Intensity", "type": "number",
              "min": 0.0, "max": 1.0, "step": 0.05, "decimals": 2, "default": 1.0 }
        ]
    })
}

fn apply_sepia(img: DynamicImage, params: &Value) -> DynamicImage {
    let amt = f64_param(params, "intensity", 1.0).clamp(0.0, 1.0) as f32;
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        let (r, g, b) = (f32::from(c[0]), f32::from(c[1]), f32::from(c[2]));
        let sr = 0.393 * r + 0.769 * g + 0.189 * b;
        let sg = 0.349 * r + 0.686 * g + 0.168 * b;
        let sb = 0.272 * r + 0.534 * g + 0.131 * b;
        c[0] = clampu8(r + (sr - r) * amt);
        c[1] = clampu8(g + (sg - g) * amt);
        c[2] = clampu8(b + (sb - b) * amt);
    });
    DynamicImage::ImageRgba8(rgba)
}

fn hue_schema() -> Value {
    json!({
        "id": "hue", "label": "Hue rotate",
        "description": "Rotate every colour around the wheel.",
        "params": [
            { "id": "degrees", "label": "Degrees", "type": "integer",
              "min": 0, "max": 360, "step": 1, "default": 180 }
        ]
    })
}

fn apply_hue(img: DynamicImage, params: &Value) -> DynamicImage {
    let shift = (i64_param(params, "degrees", 180) as f32) / 360.0;
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        let (h, s, l) = rgb_to_hsl(c[0], c[1], c[2]);
        let (r, g, b) = hsl_to_rgb((h + shift).rem_euclid(1.0), s, l);
        c[0] = r;
        c[1] = g;
        c[2] = b;
    });
    DynamicImage::ImageRgba8(rgba)
}

fn temperature_schema() -> Value {
    json!({
        "id": "temperature", "label": "Colour temperature",
        "description": "Shift towards warm (+) or cool (-).",
        "params": [
            { "id": "amount", "label": "Amount", "type": "integer",
              "min": -100, "max": 100, "step": 1, "default": 30 }
        ]
    })
}

fn apply_temperature(img: DynamicImage, params: &Value) -> DynamicImage {
    let amt = (i64_param(params, "amount", 30) as f32) / 100.0;
    let rd = amt * 50.0;
    let bd = -amt * 50.0;
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        c[0] = clampu8(f32::from(c[0]) + rd);
        c[2] = clampu8(f32::from(c[2]) + bd);
    });
    DynamicImage::ImageRgba8(rgba)
}

fn tint_schema() -> Value {
    json!({
        "id": "tint", "label": "Tint",
        "description": "Blend a flat colour over the image.",
        "params": [
            { "id": "color",   "label": "Colour",  "type": "color", "default": "#7aa2f7" },
            { "id": "opacity", "label": "Opacity", "type": "number",
              "min": 0.0, "max": 1.0, "step": 0.05, "decimals": 2, "default": 0.35 }
        ]
    })
}

fn apply_tint(img: DynamicImage, params: &Value) -> DynamicImage {
    let (tr, tg, tb) = color_param(params, "color", "#7aa2f7");
    let a = f64_param(params, "opacity", 0.35).clamp(0.0, 1.0) as f32;
    let mut rgba = img.into_rgba8();
    rgba.par_chunks_exact_mut(4).for_each(|c| {
        c[0] = lerp(c[0], tr, a);
        c[1] = lerp(c[1], tg, a);
        c[2] = lerp(c[2], tb, a);
    });
    DynamicImage::ImageRgba8(rgba)
}

fn chromatic_schema() -> Value {
    json!({
        "id": "chromatic", "label": "Chromatic aberration",
        "description": "Offset the red and blue channels for a glitch fringe.",
        "params": [
            { "id": "offset", "label": "Offset px", "type": "integer",
              "min": 1, "max": 80, "step": 1, "default": 8 }
        ]
    })
}

fn apply_chromatic(img: DynamicImage, params: &Value) -> DynamicImage {
    let off = i64_param(params, "offset", 8).max(0);
    let src = img.into_rgba8();
    let (w, h) = (src.width(), src.height());
    let mut out = RgbaImage::new(w, h);
    let sample = |x: i64, y: i64, ch: usize| -> u8 {
        let xx = x.clamp(0, i64::from(w) - 1) as u32;
        let yy = y.clamp(0, i64::from(h) - 1) as u32;
        src.get_pixel(xx, yy)[ch]
    };
    for y in 0..h {
        for x in 0..w {
            let (xi, yi) = (i64::from(x), i64::from(y));
            let p = out.get_pixel_mut(x, y);
            p[0] = sample(xi + off, yi, 0);
            p[1] = src.get_pixel(x, y)[1];
            p[2] = sample(xi - off, yi, 2);
            p[3] = src.get_pixel(x, y)[3];
        }
    }
    DynamicImage::ImageRgba8(out)
}
