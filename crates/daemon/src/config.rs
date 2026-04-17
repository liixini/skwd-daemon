use std::path::PathBuf;

use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)] // Fields deserialized from JSON config, read by QML clients
pub struct Config {
    #[serde(default)]
    pub compositor: String,
    #[serde(default)]
    pub monitor: String,
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub features: FeaturesConfig,
    #[serde(default, rename = "colorSource")]
    pub color_source: String,
    #[serde(default)]
    pub ollama: OllamaConfig,
    #[serde(default)]
    pub matugen: MatugenConfig,
    #[serde(default)]
    pub steam: SteamConfig,
    #[serde(default)]
    pub integrations: Vec<Integration>,
    #[serde(default, rename = "wallpaperMute")]
    pub wallpaper_mute: Option<bool>,
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default, rename = "defaultMatugenConfig")]
    pub default_matugen_config: Option<String>,
    #[serde(default, rename = "externalMatugenCommand")]
    pub external_matugen_command: Option<String>,
    #[serde(default, rename = "postProcessing")]
    pub post_processing: Vec<String>,
    #[serde(default, rename = "postProcessOnRestore")]
    pub post_process_on_restore: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GeneralConfig {
    #[serde(default)]
    pub locale: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PathsConfig {
    pub wallpaper: Option<String>,
    #[serde(rename = "videoWallpaper")]
    pub video_wallpaper: Option<String>,
    pub cache: Option<String>,
    pub templates: Option<String>,
    pub scripts: Option<String>,
    pub steam: Option<String>,
    #[serde(rename = "steamWorkshop")]
    pub steam_workshop: Option<String>,
    #[serde(rename = "steamWeAssets")]
    pub steam_we_assets: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct FeaturesConfig {
    #[serde(default = "default_true")]
    pub matugen: bool,
    #[serde(default)]
    pub ollama: bool,
    #[serde(default)]
    pub steam: bool,
    #[serde(default)]
    pub wallhaven: bool,
    #[serde(default)]
    pub videoAutoScale: bool,
}
impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            matugen: true,
            ollama: false,
            steam: false,
            wallhaven: false,
	    videoAutoScale: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OllamaConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub model: String,
    #[serde(default, rename = "consolidationModel")]
    pub consolidation_model: String,
    #[serde(default = "default_true", rename = "consolidateEnabled")]
    pub consolidate_enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MatugenConfig {
    #[serde(default, rename = "schemeType")]
    pub scheme_type: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
pub struct SteamConfig {
    #[serde(default)]
    pub username: String,
    #[serde(default, rename = "apiKey")]
    pub api_key: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Integration {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub reload: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
pub struct PerformanceConfig {
    #[serde(default, rename = "imageOptimizePreset")]
    pub image_optimize_preset: Option<String>,
    #[serde(default, rename = "imageOptimizeResolution")]
    pub image_optimize_resolution: Option<String>,
    #[serde(default, rename = "videoConvertPreset")]
    pub video_convert_preset: Option<String>,
    #[serde(default, rename = "autoOptimizeImages")]
    pub auto_optimize_images: bool,
    #[serde(default, rename = "autoConvertVideos")]
    pub auto_convert_videos: bool,
    #[serde(default, rename = "autoDeleteImageTrash")]
    pub auto_delete_image_trash: bool,
    #[serde(default = "default_trash_days", rename = "imageTrashDays")]
    pub image_trash_days: u32,
    #[serde(default, rename = "autoDeleteVideoTrash")]
    pub auto_delete_video_trash: bool,
    #[serde(default = "default_trash_days", rename = "videoTrashDays")]
    pub video_trash_days: u32,
    #[serde(default = "default_max_thumb_jobs", rename = "maxThumbJobs")]
    pub max_thumb_jobs: usize,
}

fn default_max_thumb_jobs() -> usize {
    16
}

fn default_trash_days() -> u32 {
    7
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn wallpaper_dir(&self) -> PathBuf {
        resolve_path(self.paths.wallpaper.as_deref()).unwrap_or_else(|| home().join("Pictures/Wallpapers"))
    }

    pub fn video_dir(&self) -> PathBuf {
        resolve_path(self.paths.video_wallpaper.as_deref()).unwrap_or_else(|| self.wallpaper_dir())
    }

    pub fn cache_dir(&self) -> PathBuf {
        resolve_path(self.paths.cache.as_deref()).unwrap_or_else(|| {
            std::env::var("XDG_CACHE_HOME")
                .map_or_else(|_| home().join(".cache"), PathBuf::from)
                .join("skwd-wall")
        })
    }

    pub fn we_dir(&self) -> PathBuf {
        resolve_path(self.paths.steam_workshop.as_deref()).unwrap_or_else(|| {
            let steam = resolve_path(self.paths.steam.as_deref()).unwrap_or_else(|| home().join(".local/share/Steam"));
            steam.join("steamapps/workshop/content/431960")
        })
    }

    pub fn we_assets_dir(&self) -> Option<PathBuf> {
        resolve_path(self.paths.steam_we_assets.as_deref())
    }

    pub fn template_dir(&self) -> PathBuf {
        resolve_path(self.paths.templates.as_deref()).unwrap_or_else(|| config_dir().join("data/matugen/templates"))
    }

    pub fn scripts_dir(&self) -> PathBuf {
        resolve_path(self.paths.scripts.as_deref()).unwrap_or_else(|| config_dir().join("scripts"))
    }

    pub fn matugen_scheme(&self) -> &str {
        self.matugen.scheme_type.as_deref().unwrap_or("scheme-fidelity")
    }

    pub fn matugen_mode(&self) -> &str {
        self.matugen.mode.as_deref().unwrap_or("dark")
    }

    pub fn is_muted(&self) -> bool {
        self.wallpaper_mute.unwrap_or(true)
    }

    pub fn matugen_config_path(&self) -> PathBuf {
        self.cache_dir().join("matugen-config.toml")
    }

    pub fn default_matugen_config_path(&self) -> Option<PathBuf> {
        resolve_path(self.default_matugen_config.as_deref())
    }

    pub fn steam_username(&self) -> &str {
        if self.steam.username.is_empty() {
            "anonymous"
        } else {
            &self.steam.username
        }
    }

    pub fn data_dir() -> PathBuf {
        if let Ok(p) = std::env::var("SKWD_DATA_DIR") {
            return PathBuf::from(p);
        }
        let local = PathBuf::from("data");
        if local.join("config.json.example").exists() {
            return std::fs::canonicalize(&local).unwrap_or(local);
        }
        PathBuf::from("/usr/share/skwd-wall/data")
    }
}

pub fn config_dir() -> PathBuf {
    std::env::var("SKWD_WALL_CONFIG").map_or_else(
        |_| {
            std::env::var("XDG_CONFIG_HOME")
                .map_or_else(|_| home().join(".config"), PathBuf::from)
                .join("skwd-wall")
        },
        PathBuf::from,
    )
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

pub fn load() -> anyhow::Result<Config> {
    let path = config_path();
    if !path.exists() {
        info!("no config at {}, using defaults", path.display());
        return Ok(Config::default());
    }
    let text = std::fs::read_to_string(&path)?;
    let cfg: Config = serde_json::from_str(&text)?;
    info!("config loaded from {}", path.display());
    Ok(cfg)
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/tmp".into()))
}

fn resolve_path(path: Option<&str>) -> Option<PathBuf> {
    let p = path?.trim();
    if p.is_empty() {
        return None;
    }
    Some(resolve_tilde(p))
}

#[must_use]
pub fn resolve_tilde(p: &str) -> PathBuf {
    if p.starts_with('~') {
        home().join(p.trim_start_matches("~/"))
    } else {
        PathBuf::from(p)
    }
}
