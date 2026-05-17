#![allow(dead_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::util::{os, run_magick, output_path};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Params {
    pub bg_mode: String,
    pub bg_color: String,
    pub blur: f32,
    pub shrink_pct: u32,
    pub border_px: u32,
    pub border_color: String,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            bg_mode: "blurred".to_string(),
            bg_color: "#000000".to_string(),
            blur: 30.0,
            shrink_pct: 70,
            border_px: 15,
            border_color: "#1a1a1a".to_string(),
        }
    }
}

pub fn schema() -> serde_json::Value {
    serde_json::json!({
        "id": "blur_frame",
        "label": "Blur frame",
        "description": "Shrinks the wallpaper and centres it on either a blurred copy of itself or a solid colour background.",
        "params": [
            { "id": "bg_mode",      "label": "Background",       "type": "dropdown", "default": "blurred",
              "options": [
                  { "mode": "blurred", "label": "Blurred copy" },
                  { "mode": "solid",   "label": "Solid colour" }
              ]
            },
            { "id": "bg_color",     "label": "Background colour","type": "color",   "default": "#000000" },
            { "id": "blur",         "label": "Blur radius",      "type": "integer", "min": 0,  "max": 100, "step": 1, "default": 30 },
            { "id": "shrink_pct",   "label": "Shrink %",         "type": "integer", "min": 10, "max": 99,  "step": 1, "default": 70 },
            { "id": "border_px",    "label": "Border (px)",      "type": "integer", "min": 0,  "max": 200, "step": 1, "default": 15 },
            { "id": "border_color", "label": "Border colour",    "type": "color",   "default": "#1a1a1a" }
        ]
    })
}

pub async fn apply(input: &Path, p: &Params) -> anyhow::Result<PathBuf> {
    let output = output_path(input, "blur-frame", None)?;

    let bg_group: Vec<OsString> = if p.bg_mode == "solid" {
        vec![
            os("("), os("-clone"), os("0"),
            os("-fill"), os(&p.bg_color),
            os("-colorize"), os("100"),
            os(")"),
        ]
    } else {
        vec![
            os("("), os("-clone"), os("0"),
            os("-blur"), os(format!("0x{}", p.blur)),
            os(")"),
        ]
    };

    let fg_group: Vec<OsString> = vec![
        os("("), os("-clone"), os("0"),
        os("-resize"), os(format!("{}%", p.shrink_pct)),
        os("-bordercolor"), os(&p.border_color),
        os("-border"), os(p.border_px.to_string()),
        os(")"),
    ];

    let mut args: Vec<OsString> = Vec::with_capacity(bg_group.len() + fg_group.len() + 6);
    args.extend(bg_group);
    args.extend(fg_group);
    args.extend([
        os("-delete"), os("0"),
        os("-gravity"), os("center"),
        os("-composite"),
    ]);

    run_magick(input, &output, args).await?;
    Ok(output)
}
