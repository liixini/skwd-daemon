#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]

use serde_json::Value;

pub fn parse_hex(s: &str) -> (u8, u8, u8) {
    let h = s.trim().trim_start_matches('#');
    if h.len() == 6
        && let (Ok(r), Ok(g), Ok(b)) = (
            u8::from_str_radix(&h[0..2], 16),
            u8::from_str_radix(&h[2..4], 16),
            u8::from_str_radix(&h[4..6], 16),
        )
    {
        return (r, g, b);
    }
    (0, 0, 0)
}

pub fn color_param(params: &Value, id: &str, default: &str) -> (u8, u8, u8) {
    parse_hex(params.get(id).and_then(|v| v.as_str()).unwrap_or(default))
}

pub fn f64_param(params: &Value, id: &str, default: f64) -> f64 {
    params.get(id).and_then(serde_json::Value::as_f64).unwrap_or(default)
}

pub fn i64_param(params: &Value, id: &str, default: i64) -> i64 {
    params.get(id).and_then(serde_json::Value::as_i64).unwrap_or(default)
}

pub fn str_param<'a>(params: &'a Value, id: &str, default: &'a str) -> &'a str {
    params.get(id).and_then(|v| v.as_str()).unwrap_or(default)
}

#[must_use]
pub fn luma(r: u8, g: u8, b: u8) -> f32 {
    0.299 * f32::from(r) + 0.587 * f32::from(g) + 0.114 * f32::from(b)
}

#[must_use]
pub fn clampu8(v: f32) -> u8 {
    v.clamp(0.0, 255.0) as u8
}

#[must_use]
pub fn lerp(a: u8, b: u8, t: f32) -> u8 {
    clampu8(f32::from(a) + (f32::from(b) - f32::from(a)) * t)
}

#[must_use]
pub fn hash01(mut x: u32) -> f32 {
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^= x >> 16;
    x as f32 / u32::MAX as f32
}

#[must_use]
pub fn rgb_to_hsl(r: u8, g: u8, b: u8) -> (f32, f32, f32) {
    let (r, g, b) = (f32::from(r) / 255.0, f32::from(g) / 255.0, f32::from(b) / 255.0);
    let max = r.max(g).max(b);
    let min = r.min(g).min(b);
    let l = (max + min) / 2.0;
    let d = max - min;
    if d.abs() < 1e-6 {
        return (0.0, 0.0, l);
    }
    let s = if l > 0.5 { d / (2.0 - max - min) } else { d / (max + min) };
    let h = if (max - r).abs() < 1e-6 {
        (g - b) / d + if g < b { 6.0 } else { 0.0 }
    } else if (max - g).abs() < 1e-6 {
        (b - r) / d + 2.0
    } else {
        (r - g) / d + 4.0
    };
    (h / 6.0, s, l)
}

#[must_use]
pub fn hsl_to_rgb(h: f32, s: f32, l: f32) -> (u8, u8, u8) {
    if s.abs() < 1e-6 {
        let v = clampu8(l * 255.0);
        return (v, v, v);
    }
    let q = if l < 0.5 { l * (1.0 + s) } else { l + s - l * s };
    let p = 2.0 * l - q;
    let hue = |mut t: f32| -> f32 {
        if t < 0.0 {
            t += 1.0;
        }
        if t > 1.0 {
            t -= 1.0;
        }
        if t < 1.0 / 6.0 {
            p + (q - p) * 6.0 * t
        } else if t < 0.5 {
            q
        } else if t < 2.0 / 3.0 {
            p + (q - p) * (2.0 / 3.0 - t) * 6.0
        } else {
            p
        }
    };
    (
        clampu8(hue(h + 1.0 / 3.0) * 255.0),
        clampu8(hue(h) * 255.0),
        clampu8(hue(h - 1.0 / 3.0) * 255.0),
    )
}

#[must_use]
pub fn gradient_sample(stops: &[(u8, u8, u8)], t: f32) -> (u8, u8, u8) {
    if stops.is_empty() {
        return (0, 0, 0);
    }
    if stops.len() == 1 {
        return stops[0];
    }
    let t = t.clamp(0.0, 1.0);
    let scaled = t * (stops.len() - 1) as f32;
    let i = (scaled.floor() as usize).min(stops.len() - 2);
    let frac = scaled - i as f32;
    let a = stops[i];
    let b = stops[i + 1];
    (lerp(a.0, b.0, frac), lerp(a.1, b.1, frac), lerp(a.2, b.2, frac))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_handles_valid_and_invalid() {
        assert_eq!(parse_hex("#ff8800"), (255, 136, 0));
        assert_eq!(parse_hex("00ff00"), (0, 255, 0));
        assert_eq!(parse_hex("nope"), (0, 0, 0));
    }

    #[test]
    fn hsl_roundtrip_is_close() {
        for &(r, g, b) in &[(200u8, 50u8, 100u8), (10, 220, 30), (128, 128, 128)] {
            let (h, s, l) = rgb_to_hsl(r, g, b);
            let (r2, g2, b2) = hsl_to_rgb(h, s, l);
            assert!((i32::from(r) - i32::from(r2)).abs() <= 2);
            assert!((i32::from(g) - i32::from(g2)).abs() <= 2);
            assert!((i32::from(b) - i32::from(b2)).abs() <= 2);
        }
    }

    #[test]
    fn gradient_sample_endpoints_and_mid() {
        let s = [(0, 0, 0), (255, 255, 255)];
        assert_eq!(gradient_sample(&s, 0.0), (0, 0, 0));
        assert_eq!(gradient_sample(&s, 1.0), (255, 255, 255));
        let m = gradient_sample(&s, 0.5);
        assert!(m.0 > 120 && m.0 < 135);
    }
}
