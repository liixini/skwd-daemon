use clap::ValueEnum;
use image::imageops::{self, FilterType};
use image::RgbaImage;

#[derive(Debug, Clone, Copy, ValueEnum, Default, PartialEq, Eq)]
#[value(rename_all = "lowercase")]
pub enum FillMode {
    #[default]
    Fill,
    Fit,
    Stretch,
    Center,
    Tile,
}

pub fn apply_fill_mode(
    img_w: u32,
    img_h: u32,
    pixels: Vec<u8>,
    surf_w: u32,
    surf_h: u32,
    mode: FillMode,
) -> (u32, u32, Vec<u8>) {
    if surf_w == 0 || surf_h == 0 {
        return (img_w, img_h, pixels);
    }
    if img_w == 0 || img_h == 0 {
        return (surf_w, surf_h, opaque_black(surf_w, surf_h));
    }

    let src = match RgbaImage::from_raw(img_w, img_h, pixels) {
        Some(img) => img,
        None => return (surf_w, surf_h, opaque_black(surf_w, surf_h)),
    };

    let out: RgbaImage = match mode {
        FillMode::Stretch => imageops::resize(&src, surf_w, surf_h, FilterType::Triangle),
        FillMode::Fill => {
            let img_aspect = img_w as f32 / img_h as f32;
            let surf_aspect = surf_w as f32 / surf_h as f32;
            let (cx, cy, cw, ch) = if img_aspect > surf_aspect {
                let cw = ((img_h as f32) * surf_aspect).round().max(1.0) as u32;
                let cw = cw.min(img_w);
                ((img_w - cw) / 2, 0, cw, img_h)
            } else {
                let ch = ((img_w as f32) / surf_aspect).round().max(1.0) as u32;
                let ch = ch.min(img_h);
                (0, (img_h - ch) / 2, img_w, ch)
            };
            let cropped = imageops::crop_imm(&src, cx, cy, cw, ch).to_image();
            imageops::resize(&cropped, surf_w, surf_h, FilterType::Triangle)
        }
        FillMode::Fit => {
            let img_aspect = img_w as f32 / img_h as f32;
            let surf_aspect = surf_w as f32 / surf_h as f32;
            let (sw, sh) = if img_aspect > surf_aspect {
                (surf_w, ((surf_w as f32) / img_aspect).round().max(1.0) as u32)
            } else {
                (((surf_h as f32) * img_aspect).round().max(1.0) as u32, surf_h)
            };
            let scaled = imageops::resize(&src, sw, sh, FilterType::Triangle);
            let mut canvas = opaque_black_image(surf_w, surf_h);
            let off_x = ((surf_w - sw) / 2) as i64;
            let off_y = ((surf_h - sh) / 2) as i64;
            imageops::overlay(&mut canvas, &scaled, off_x, off_y);
            canvas
        }
        FillMode::Center => {
            let mut canvas = opaque_black_image(surf_w, surf_h);
            if img_w >= surf_w && img_h >= surf_h {
                let cx = (img_w - surf_w) / 2;
                let cy = (img_h - surf_h) / 2;
                let cropped = imageops::crop_imm(&src, cx, cy, surf_w, surf_h).to_image();
                canvas = cropped;
            } else if img_w >= surf_w {
                let cx = (img_w - surf_w) / 2;
                let cropped = imageops::crop_imm(&src, cx, 0, surf_w, img_h).to_image();
                let off_y = ((surf_h - img_h) / 2) as i64;
                imageops::overlay(&mut canvas, &cropped, 0, off_y);
            } else if img_h >= surf_h {
                let cy = (img_h - surf_h) / 2;
                let cropped = imageops::crop_imm(&src, 0, cy, img_w, surf_h).to_image();
                let off_x = ((surf_w - img_w) / 2) as i64;
                imageops::overlay(&mut canvas, &cropped, off_x, 0);
            } else {
                let off_x = ((surf_w - img_w) / 2) as i64;
                let off_y = ((surf_h - img_h) / 2) as i64;
                imageops::overlay(&mut canvas, &src, off_x, off_y);
            }
            canvas
        }
        FillMode::Tile => {
            let mut canvas = opaque_black_image(surf_w, surf_h);
            let mut y: i64 = 0;
            while y < surf_h as i64 {
                let mut x: i64 = 0;
                while x < surf_w as i64 {
                    imageops::overlay(&mut canvas, &src, x, y);
                    x += img_w as i64;
                }
                y += img_h as i64;
            }
            canvas
        }
    };

    (out.width(), out.height(), out.into_raw())
}

fn opaque_black(w: u32, h: u32) -> Vec<u8> {
    let mut v = vec![0u8; (w as usize) * (h as usize) * 4];
    for px in v.chunks_exact_mut(4) {
        px[3] = 255;
    }
    v
}

fn opaque_black_image(w: u32, h: u32) -> RgbaImage {
    RgbaImage::from_raw(w, h, opaque_black(w, h)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RED: [u8; 4] = [255, 0, 0, 255];
    const BLACK: [u8; 4] = [0, 0, 0, 255];

    fn solid(w: u32, h: u32, color: [u8; 4]) -> Vec<u8> {
        let mut v = Vec::with_capacity((w * h * 4) as usize);
        for _ in 0..(w * h) {
            v.extend_from_slice(&color);
        }
        v
    }

    fn px(pixels: &[u8], w: u32, x: u32, y: u32) -> [u8; 4] {
        let i = ((y * w + x) * 4) as usize;
        [pixels[i], pixels[i + 1], pixels[i + 2], pixels[i + 3]]
    }

    #[test]
    fn zero_surface_returns_input_unchanged() {
        let pixels = solid(2, 2, RED);
        let (w, h, out) = apply_fill_mode(2, 2, pixels.clone(), 0, 4, FillMode::Fill);
        assert_eq!((w, h), (2, 2));
        assert_eq!(out, pixels);
    }

    #[test]
    fn zero_image_returns_opaque_black_surface() {
        let (w, h, out) = apply_fill_mode(0, 0, Vec::new(), 3, 2, FillMode::Fill);
        assert_eq!((w, h), (3, 2));
        assert_eq!(out.len(), 3 * 2 * 4);
        for p in out.chunks_exact(4) {
            assert_eq!(p, BLACK);
        }
    }

    #[test]
    fn stretch_matches_surface_dimensions() {
        let (w, h, out) = apply_fill_mode(4, 4, solid(4, 4, RED), 8, 6, FillMode::Stretch);
        assert_eq!((w, h), (8, 6));
        assert_eq!(px(&out, 8, 0, 0), RED);
        assert_eq!(px(&out, 8, 7, 5), RED);
    }

    #[test]
    fn fill_covers_whole_surface() {
        let (w, h, out) = apply_fill_mode(4, 2, solid(4, 2, RED), 8, 8, FillMode::Fill);
        assert_eq!((w, h), (8, 8));
        for p in out.chunks_exact(4) {
            assert_eq!(p, RED);
        }
    }

    #[test]
    fn fit_letterboxes_with_black_bars() {
        let (w, h, out) = apply_fill_mode(4, 2, solid(4, 2, RED), 4, 4, FillMode::Fit);
        assert_eq!((w, h), (4, 4));
        assert_eq!(px(&out, 4, 0, 0), BLACK);
        assert_eq!(px(&out, 4, 3, 0), BLACK);
        assert_eq!(px(&out, 4, 0, 1), RED);
        assert_eq!(px(&out, 4, 2, 2), RED);
        assert_eq!(px(&out, 4, 0, 3), BLACK);
    }

    #[test]
    fn center_smaller_image_is_padded() {
        let (w, h, out) = apply_fill_mode(2, 2, solid(2, 2, RED), 4, 4, FillMode::Center);
        assert_eq!((w, h), (4, 4));
        assert_eq!(px(&out, 4, 0, 0), BLACK);
        assert_eq!(px(&out, 4, 1, 1), RED);
        assert_eq!(px(&out, 4, 2, 2), RED);
        assert_eq!(px(&out, 4, 3, 3), BLACK);
    }

    #[test]
    fn center_larger_image_is_cropped() {
        let (w, h, out) = apply_fill_mode(8, 8, solid(8, 8, RED), 4, 4, FillMode::Center);
        assert_eq!((w, h), (4, 4));
        for p in out.chunks_exact(4) {
            assert_eq!(p, RED);
        }
    }

    #[test]
    fn tile_fills_surface_with_repeats() {
        let (w, h, out) = apply_fill_mode(2, 2, solid(2, 2, RED), 5, 5, FillMode::Tile);
        assert_eq!((w, h), (5, 5));
        for p in out.chunks_exact(4) {
            assert_eq!(p, RED);
        }
    }

    #[test]
    fn opaque_black_is_fully_opaque() {
        let v = opaque_black(2, 3);
        assert_eq!(v.len(), 2 * 3 * 4);
        for p in v.chunks_exact(4) {
            assert_eq!(p, BLACK);
        }
    }
}
