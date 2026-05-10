use std::path::PathBuf;

use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
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
    #[serde(default, rename = "wallpaperVolume")]
    pub wallpaper_volume: Option<u32>,
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default, rename = "defaultMatugenConfig")]
    pub default_matugen_config: Option<String>,
    #[serde(default, rename = "externalMatugenCommand")]
    pub external_matugen_command: Option<String>,
    #[serde(default, rename = "externalWallpaperCommand")]
    pub external_wallpaper_command: Option<String>,
    #[serde(default, rename = "pickOnlyMode")]
    pub pick_only_mode: bool,
    #[serde(default, rename = "postProcessing")]
    pub post_processing: Vec<PostProcessEntry>,
    #[serde(default, rename = "postProcessOnRestore")]
    pub post_process_on_restore: bool,
    #[serde(default = "default_true", rename = "restoreOnStartup")]
    pub restore_on_startup: bool,
    #[serde(default)]
    pub notifications: NotificationsConfig,
    #[serde(default)]
    pub transition: TransitionConfig,
    #[serde(default)]
    pub display: DisplayConfig,
    #[serde(default)]
    pub paper: PaperConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FillMode {
    #[default]
    Fill,
    Fit,
    Stretch,
    Center,
    Tile,
}

impl FillMode {
    pub fn as_arg(self) -> &'static str {
        match self {
            FillMode::Fill => "fill",
            FillMode::Fit => "fit",
            FillMode::Stretch => "stretch",
            FillMode::Center => "center",
            FillMode::Tile => "tile",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DisplayConfig {
    #[serde(default, rename = "fillMode")]
    pub fill_mode: FillMode,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransitionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_transition_shader")]
    pub shader: String,
    #[serde(default = "default_transition_duration_ms", rename = "durationMs")]
    pub duration_ms: u64,
}

impl Default for TransitionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            shader: default_transition_shader(),
            duration_ms: default_transition_duration_ms(),
        }
    }
}

fn default_transition_shader() -> String {
    "random".to_string()
}

fn default_transition_duration_ms() -> u64 {
    600
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PaperEngine {
    #[default]
    #[serde(alias = "skwd-paper", alias = "internal", alias = "skwd_paper")]
    SkwdPaper,
    Awww,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PaperConfig {
    #[serde(default)]
    pub engine: PaperEngine,
    #[serde(default)]
    pub awww: AwwwConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AwwwConfig {
    #[serde(default = "default_awww_transition_type", rename = "transitionType")]
    pub transition_type: String,
    #[serde(default = "default_awww_transition_duration_ms", rename = "transitionDurationMs")]
    pub transition_duration_ms: u32,
    #[serde(default = "default_awww_transition_fps", rename = "transitionFps")]
    pub transition_fps: u32,
    #[serde(default = "default_awww_transition_step", rename = "transitionStep")]
    pub transition_step: u32,

    #[serde(default = "default_awww_transition_angle", rename = "transitionAngle")]
    pub transition_angle: u32,
    #[serde(default = "default_awww_wave_dim", rename = "transitionWaveWidth")]
    pub transition_wave_width: u32,
    #[serde(default = "default_awww_wave_dim", rename = "transitionWaveHeight")]
    pub transition_wave_height: u32,
    #[serde(default = "default_awww_transition_pos", rename = "transitionPos")]
    pub transition_pos: String,
    #[serde(default = "default_awww_transition_bezier", rename = "transitionBezier")]
    pub transition_bezier: String,
    #[serde(default, rename = "invertY")]
    pub invert_y: bool,
    #[serde(default = "default_awww_filter")]
    pub filter: String,
    #[serde(default = "default_awww_fill_color", rename = "fillColor")]
    pub fill_color: String,
}

impl Default for AwwwConfig {
    fn default() -> Self {
        Self {
            transition_type: default_awww_transition_type(),
            transition_duration_ms: default_awww_transition_duration_ms(),
            transition_fps: default_awww_transition_fps(),
            transition_step: default_awww_transition_step(),
            transition_angle: default_awww_transition_angle(),
            transition_wave_width: default_awww_wave_dim(),
            transition_wave_height: default_awww_wave_dim(),
            transition_pos: default_awww_transition_pos(),
            transition_bezier: default_awww_transition_bezier(),
            invert_y: false,
            filter: default_awww_filter(),
            fill_color: default_awww_fill_color(),
        }
    }
}

fn default_awww_transition_type() -> String { "wipe".to_string() }
fn default_awww_transition_duration_ms() -> u32 { 1000 }
fn default_awww_transition_fps() -> u32 { 60 }
fn default_awww_transition_step() -> u32 { 90 }
fn default_awww_transition_angle() -> u32 { 45 }
fn default_awww_wave_dim() -> u32 { 20 }
fn default_awww_transition_pos() -> String { "center".to_string() }
fn default_awww_transition_bezier() -> String { ".54,0,.34,.99".to_string() }
fn default_awww_filter() -> String { "Lanczos3".to_string() }
fn default_awww_fill_color() -> String { "000000ff".to_string() }

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NotificationsBuiltIn {
    Auto,
    Always,
    #[default]
    Never,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NotificationsConfig {
    #[serde(default, rename = "builtIn")]
    pub built_in: NotificationsBuiltIn,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    #[serde(default)]
    pub locale: String,
    #[serde(default = "default_true", rename = "notifyOnWallpaperChange")]
    pub notify_on_wallpaper_change: bool,
}
impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            locale: String::new(),
            notify_on_wallpaper_change: true,
        }
    }
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
    #[serde(default, rename = "videoAutoScale")]
    pub video_auto_scale: bool,
    #[serde(default = "default_true")]
    pub lyrics: bool,
    #[serde(default = "default_true")]
    pub music: bool,
    #[serde(default = "default_true")]
    pub analysis: bool,
    #[serde(default = "default_true")]
    pub video: bool,
}
impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            matugen: true,
            ollama: false,
            steam: false,
            wallhaven: false,
            video_auto_scale: false,
            lyrics: true,
            music: true,
            analysis: true,
            video: true,
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
    #[allow(dead_code)]
    pub consolidation_model: String,
    #[serde(default = "default_true", rename = "consolidateEnabled")]
    #[allow(dead_code)]
    pub consolidate_enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MatugenConfig {
    #[serde(default, rename = "schemeType")]
    pub scheme_type: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default, rename = "colorIndex")]
    pub color_index: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
pub struct SteamConfig {
    #[serde(default)]
    pub username: String,
    #[serde(default, rename = "apiKey")]
    pub api_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PostProcessEntry {
    Plain(String),
    Detailed {
        #[serde(default)]
        command: String,
        #[serde(default = "default_post_process_type", rename = "type")]
        wp_type: String,
    },
}

fn default_post_process_type() -> String {
    "all".to_string()
}

impl PostProcessEntry {
    pub fn command(&self) -> &str {
        match self {
            PostProcessEntry::Plain(s) => s,
            PostProcessEntry::Detailed { command, .. } => command,
        }
    }

    pub fn wp_type(&self) -> &str {
        match self {
            PostProcessEntry::Plain(_) => "all",
            PostProcessEntry::Detailed { wp_type, .. } => wp_type,
        }
    }

    pub fn matches(&self, applied_type: &str) -> bool {
        let t = self.wp_type();
        t == "all" || t == applied_type
    }
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

    pub fn wants_external_render(&self) -> bool {
        self.pick_only_mode
            || self
                .external_wallpaper_command
                .as_deref()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
    }

    pub fn matugen_scheme(&self) -> &str {
        self.matugen.scheme_type.as_deref().unwrap_or("scheme-fidelity")
    }

    pub fn matugen_mode(&self) -> &str {
        self.matugen.mode.as_deref().unwrap_or("dark")
    }

    pub fn matugen_color_index(&self) -> u32 {
        self.matugen.color_index.unwrap_or(0).min(3)
    }

    pub fn is_muted(&self) -> bool {
        self.wallpaper_mute.unwrap_or(true)
    }

    pub fn volume(&self) -> u32 {
        self.wallpaper_volume.unwrap_or(100).min(100)
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

pub fn shell_config_path() -> PathBuf {
    std::env::var("SKWD_CONFIG").map_or_else(
        |_| {
            std::env::var("XDG_CONFIG_HOME")
                .map_or_else(|_| home().join(".config"), PathBuf::from)
                .join("skwd")
                .join("data")
        },
        PathBuf::from,
    )
    .join("config.json")
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ShellConfig {
    notifications: Option<NotificationsConfig>,
}

pub fn load() -> anyhow::Result<Config> {
    let path = config_path();
    let mut cfg = if path.exists() {
        let text = std::fs::read_to_string(&path)?;
        let parsed: Config = serde_json::from_str(&text)?;
        info!("config loaded from {}", path.display());
        parsed
    } else {
        info!("no config at {}, using defaults", path.display());
        Config::default()
    };

    let shell_path = shell_config_path();
    if shell_path.exists() {
        match std::fs::read_to_string(&shell_path)
            .ok()
            .and_then(|t| serde_json::from_str::<ShellConfig>(&t).ok())
        {
            Some(shell) => {
                if let Some(notifs) = shell.notifications {
                    cfg.notifications = notifs;
                    info!("notifications config taken from {}", shell_path.display());
                }
            }
            None => info!("shell config at {} unparseable, ignoring", shell_path.display()),
        }
    }

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
