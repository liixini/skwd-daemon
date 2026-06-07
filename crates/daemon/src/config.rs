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
    pub effects: EffectsConfig,
    #[serde(default)]
    pub integrations: Vec<Integration>,
    #[serde(default, rename = "wallpaperMute")]
    pub wallpaper_mute: Option<bool>,
    #[serde(default, rename = "wallpaperVolume")]
    pub wallpaper_volume: Option<u32>,
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub niri: NiriConfig,
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
    #[serde(default)]
    pub we_render: WeRenderConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum WeScaling {
    Default,
    #[serde(alias = "fill")]
    #[default]
    Fill,
    Fit,
    Stretch,
}


impl WeScaling {
    pub fn as_arg(self) -> &'static str {
        match self {
            WeScaling::Default => "default",
            WeScaling::Fill => "fill",
            WeScaling::Fit => "fit",
            WeScaling::Stretch => "stretch",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "default" => Some(WeScaling::Default),
            "fill" => Some(WeScaling::Fill),
            "fit" => Some(WeScaling::Fit),
            "stretch" => Some(WeScaling::Stretch),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum WeClamp {
    #[default]
    Border,
    Repeat,
    Mirror,
}


impl WeClamp {
    pub fn as_arg(self) -> &'static str {
        match self {
            WeClamp::Border => "border",
            WeClamp::Repeat => "repeat",
            WeClamp::Mirror => "mirror",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "border" => Some(WeClamp::Border),
            "repeat" => Some(WeClamp::Repeat),
            "mirror" => Some(WeClamp::Mirror),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WeRenderConfig {
    #[serde(default = "default_we_fps")]
    pub fps: u32,
    #[serde(default = "default_true", rename = "noFullscreenPause")]
    pub no_fullscreen_pause: bool,
    #[serde(default, rename = "fullscreenPauseOnlyActive")]
    pub fullscreen_pause_only_active: bool,
    #[serde(default = "default_true")]
    pub noautomute: bool,
    #[serde(default, rename = "noAudioProcessing")]
    pub no_audio_processing: bool,
    #[serde(default, rename = "disableParticles")]
    pub disable_particles: bool,
    #[serde(default, rename = "disableMouse")]
    pub disable_mouse: bool,
    #[serde(default, rename = "disableParallax")]
    pub disable_parallax: bool,
    #[serde(default)]
    pub scaling: WeScaling,
    #[serde(default)]
    pub clamp: WeClamp,
}

impl Default for WeRenderConfig {
    fn default() -> Self {
        Self {
            fps: default_we_fps(),
            no_fullscreen_pause: true,
            fullscreen_pause_only_active: false,
            noautomute: true,
            no_audio_processing: false,
            disable_particles: false,
            disable_mouse: false,
            disable_parallax: false,
            scaling: WeScaling::default(),
            clamp: WeClamp::default(),
        }
    }
}

fn default_we_fps() -> u32 {
    30
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
    #[serde(default)]
    pub prompt: String,

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
    #[serde(default)]
    pub contrast: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SteamBackend {
    #[default]
    #[serde(alias = "steam", alias = "steamworks", alias = "steam-client")]
    Steam,
    #[serde(alias = "steamcmd", alias = "depot-downloader", alias = "depotdownloader")]
    Steamcmd,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
pub struct SteamConfig {
    #[serde(default)]
    pub username: String,
    #[serde(default, rename = "apiKey")]
    pub api_key: String,
    #[serde(default)]
    pub backend: SteamBackend,
}

impl SteamConfig {
    #[must_use]
    pub fn uses_steam_client(&self) -> bool {
        self.backend == SteamBackend::Steam
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EffectsConfig {
    #[serde(default, rename = "autoRecolor")]
    pub auto_recolor: bool,
    #[serde(default, rename = "autoTheme")]
    pub auto_theme: String,
}

impl EffectsConfig {
    #[must_use]
    pub fn auto_recolor_theme(&self) -> &str {
        if self.auto_theme.trim().is_empty() {
            "Catppuccin"
        } else {
            self.auto_theme.trim()
        }
    }
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
pub struct NiriConfig {
    #[serde(default, rename = "overviewBackdrop")]
    pub overview_backdrop: bool,
    #[serde(default = "default_true", rename = "overviewBackdropBlurEnabled")]
    pub overview_backdrop_blur_enabled: bool,
    #[serde(default = "default_backdrop_blur", rename = "overviewBackdropBlur")]
    pub overview_backdrop_blur: u32,
    #[serde(default)]
    pub backdrop: String,
    #[serde(default, rename = "backdropFollowWallpaper")]
    pub backdrop_follow_wallpaper: bool,
    #[serde(default, rename = "backdropAutoTheme")]
    pub backdrop_auto_theme: bool,
    #[serde(default, rename = "backdropTheme")]
    pub backdrop_theme: String,
    #[serde(default, rename = "backdropDim")]
    pub backdrop_dim: u32,
}

impl NiriConfig {
    #[must_use]
    pub fn backdrop_source(&self) -> Option<&str> {
        if self.backdrop_follow_wallpaper {
            return None;
        }
        let p = self.backdrop.trim();
        if p.is_empty() || !std::path::Path::new(p).exists() {
            None
        } else {
            Some(p)
        }
    }

    #[must_use]
    pub fn backdrop_theme_name(&self) -> Option<&str> {
        if !self.backdrop_auto_theme {
            return None;
        }
        let t = self.backdrop_theme.trim();
        if t.is_empty() {
            Some("Catppuccin")
        } else {
            Some(t)
        }
    }
}

fn default_backdrop_blur() -> u32 { 30 }

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
        if let Some(p) = resolve_path(self.paths.steam_we_assets.as_deref()) {
            return Some(p);
        }
        let steam = resolve_path(self.paths.steam.as_deref()).unwrap_or_else(|| home().join(".local/share/Steam"));
        let default = steam.join("steamapps/common/wallpaper_engine/assets");
        default.exists().then_some(default)
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
                .is_some_and(|s| !s.is_empty())
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

    pub fn matugen_contrast(&self) -> Option<f64> {
        self.matugen
            .contrast
            .filter(|c| c.is_finite() && *c != 0.0)
            .map(|c| c.clamp(-1.0, 1.0))
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
        let configured = self.default_matugen_config.as_deref().filter(|s| !s.trim().is_empty());
        resolve_path(configured.or(Some("~/.config/matugen/config.toml")))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn we_scaling_as_arg_matches_variants() {
        assert_eq!(WeScaling::Default.as_arg(), "default");
        assert_eq!(WeScaling::Fill.as_arg(), "fill");
        assert_eq!(WeScaling::Fit.as_arg(), "fit");
        assert_eq!(WeScaling::Stretch.as_arg(), "stretch");
    }

    #[test]
    fn we_scaling_from_str_is_case_insensitive() {
        assert_eq!(WeScaling::from_str("FILL"), Some(WeScaling::Fill));
        assert_eq!(WeScaling::from_str("Fit"), Some(WeScaling::Fit));
        assert_eq!(WeScaling::from_str("stretch"), Some(WeScaling::Stretch));
        assert_eq!(WeScaling::from_str("default"), Some(WeScaling::Default));
        assert_eq!(WeScaling::from_str("nonsense"), None);
    }

    #[test]
    fn we_clamp_roundtrips_arg_and_parse() {
        for (c, s) in [
            (WeClamp::Border, "border"),
            (WeClamp::Repeat, "repeat"),
            (WeClamp::Mirror, "mirror"),
        ] {
            assert_eq!(c.as_arg(), s);
            assert_eq!(WeClamp::from_str(&s.to_uppercase()), Some(c));
        }
        assert_eq!(WeClamp::from_str("wrap"), None);
    }

    #[test]
    fn fill_mode_as_arg_covers_all_variants() {
        assert_eq!(FillMode::Fill.as_arg(), "fill");
        assert_eq!(FillMode::Fit.as_arg(), "fit");
        assert_eq!(FillMode::Stretch.as_arg(), "stretch");
        assert_eq!(FillMode::Center.as_arg(), "center");
        assert_eq!(FillMode::Tile.as_arg(), "tile");
        assert_eq!(FillMode::default(), FillMode::Fill);
    }

    #[test]
    fn post_process_plain_defaults_to_all() {
        let e: PostProcessEntry = serde_json::from_str(r#""echo hi""#).unwrap();
        assert_eq!(e.command(), "echo hi");
        assert_eq!(e.wp_type(), "all");
        assert!(e.matches("static"));
        assert!(e.matches("video"));
    }

    #[test]
    fn post_process_detailed_filters_by_type() {
        let e: PostProcessEntry =
            serde_json::from_str(r#"{"command":"run","type":"video"}"#).unwrap();
        assert_eq!(e.command(), "run");
        assert_eq!(e.wp_type(), "video");
        assert!(e.matches("video"));
        assert!(!e.matches("static"));
    }

    #[test]
    fn post_process_detailed_type_defaults_to_all() {
        let e: PostProcessEntry = serde_json::from_str(r#"{"command":"run"}"#).unwrap();
        assert_eq!(e.wp_type(), "all");
        assert!(e.matches("anything"));
    }

    #[test]
    fn matugen_getters_fall_back_to_defaults() {
        let c = Config::default();
        assert_eq!(c.matugen_scheme(), "scheme-fidelity");
        assert_eq!(c.matugen_mode(), "dark");
        assert_eq!(c.matugen_color_index(), 0);
    }

    #[test]
    fn matugen_color_index_clamps_to_three() {
        let c: Config = serde_json::from_str(r#"{"matugen":{"colorIndex":9}}"#).unwrap();
        assert_eq!(c.matugen_color_index(), 3);
        let c2: Config = serde_json::from_str(r#"{"matugen":{"colorIndex":2}}"#).unwrap();
        assert_eq!(c2.matugen_color_index(), 2);
    }

    #[test]
    fn mute_and_volume_defaults_and_clamp() {
        let c = Config::default();
        assert!(c.is_muted());
        assert_eq!(c.volume(), 100);
        let c2: Config =
            serde_json::from_str(r#"{"wallpaperMute":false,"wallpaperVolume":250}"#).unwrap();
        assert!(!c2.is_muted());
        assert_eq!(c2.volume(), 100);
        let c3: Config = serde_json::from_str(r#"{"wallpaperVolume":40}"#).unwrap();
        assert_eq!(c3.volume(), 40);
    }

    #[test]
    fn steam_username_falls_back_to_anonymous() {
        let c = Config::default();
        assert_eq!(c.steam_username(), "anonymous");
        let c2: Config = serde_json::from_str(r#"{"steam":{"username":"bob"}}"#).unwrap();
        assert_eq!(c2.steam_username(), "bob");
    }

    #[test]
    fn empty_json_yields_documented_defaults() {
        let c: Config = serde_json::from_str("{}").unwrap();
        assert!(c.restore_on_startup);
        assert!(c.features.matugen);
        assert!(c.features.lyrics);
        assert!(!c.features.steam);
        assert_eq!(c.we_render.fps, 30);
        assert!(c.we_render.no_fullscreen_pause);
        assert!(c.transition.enabled);
        assert_eq!(c.transition.shader, "random");
        assert_eq!(c.transition.duration_ms, 600);
        assert_eq!(c.display.fill_mode, FillMode::Fill);
        assert_eq!(c.we_render.scaling, WeScaling::Fill);
    }

    #[test]
    fn resolve_tilde_leaves_absolute_paths_untouched() {
        assert_eq!(resolve_tilde("/etc/passwd"), PathBuf::from("/etc/passwd"));
        assert_eq!(resolve_tilde("relative/dir"), PathBuf::from("relative/dir"));
    }

    #[test]
    fn resolve_tilde_expands_home_prefix() {
        let expanded = resolve_tilde("~/Pictures");
        assert_eq!(expanded, home().join("Pictures"));
        assert!(!expanded.to_string_lossy().starts_with('~'));
    }

    #[test]
    fn path_getters_honor_explicit_paths() {
        let mut c = Config::default();
        c.paths.wallpaper = Some("/custom/walls".into());
        c.paths.cache = Some("/custom/cache".into());
        assert_eq!(c.wallpaper_dir(), PathBuf::from("/custom/walls"));
        assert_eq!(c.cache_dir(), PathBuf::from("/custom/cache"));
        assert_eq!(c.matugen_config_path(), PathBuf::from("/custom/cache/matugen-config.toml"));
    }

    #[test]
    fn video_dir_falls_back_to_wallpaper_dir() {
        let mut c = Config::default();
        c.paths.wallpaper = Some("/walls".into());
        c.paths.video_wallpaper = None;
        assert_eq!(c.video_dir(), c.wallpaper_dir());
        c.paths.video_wallpaper = Some("/vids".into());
        assert_eq!(c.video_dir(), PathBuf::from("/vids"));
    }

    #[test]
    fn we_dir_uses_workshop_then_steam_then_default() {
        let mut c = Config::default();
        c.paths.steam_workshop = Some("/ws".into());
        assert_eq!(c.we_dir(), PathBuf::from("/ws"));
        c.paths.steam_workshop = None;
        c.paths.steam = Some("/steam".into());
        assert_eq!(c.we_dir(), PathBuf::from("/steam/steamapps/workshop/content/431960"));
    }

    #[test]
    fn wants_external_render_logic() {
        let mut c = Config::default();
        assert!(!c.wants_external_render());
        c.pick_only_mode = true;
        assert!(c.wants_external_render());
        c.pick_only_mode = false;
        c.external_wallpaper_command = Some("setwall %path%".into());
        assert!(c.wants_external_render());
        c.external_wallpaper_command = Some(String::new());
        assert!(!c.wants_external_render());
    }

    #[test]
    fn we_assets_dir_falls_back_to_steam_default_when_unset() {
        let dir = tempfile::tempdir().unwrap();
        let steam = dir.path();
        let assets = steam.join("steamapps/common/wallpaper_engine/assets");

        let mut c = Config::default();
        c.paths.steam = Some(steam.to_string_lossy().to_string());

        c.paths.steam_we_assets = None;
        assert_eq!(c.we_assets_dir(), None, "no fallback when default assets dir absent");

        std::fs::create_dir_all(&assets).unwrap();
        assert_eq!(c.we_assets_dir(), Some(assets.clone()), "falls back to existing default");

        c.paths.steam_we_assets = Some(String::new());
        assert_eq!(c.we_assets_dir(), Some(assets.clone()), "empty string also falls back");

        let custom = dir.path().join("custom-assets");
        c.paths.steam_we_assets = Some(custom.to_string_lossy().to_string());
        assert_eq!(c.we_assets_dir(), Some(custom), "explicit path wins over default");
    }

    #[test]
    fn default_matugen_config_path_treats_empty_as_unset() {
        let mut c = Config::default();

        assert!(
            c.default_matugen_config_path().unwrap().ends_with(".config/matugen/config.toml"),
            "unset -> falls back to default user config"
        );

        c.default_matugen_config = Some(String::new());
        assert!(
            c.default_matugen_config_path().unwrap().ends_with(".config/matugen/config.toml"),
            "empty string -> falls back (issue #68)"
        );

        c.default_matugen_config = Some("   ".into());
        assert!(
            c.default_matugen_config_path().unwrap().ends_with(".config/matugen/config.toml"),
            "whitespace -> falls back"
        );

        c.default_matugen_config = Some("/custom/matugen.toml".into());
        assert_eq!(
            c.default_matugen_config_path(),
            Some(std::path::PathBuf::from("/custom/matugen.toml")),
            "explicit path wins"
        );
    }

    #[test]
    fn matugen_contrast_clamps_and_omits_default() {
        let mut c = Config::default();
        assert_eq!(c.matugen_contrast(), None, "unset -> omit --contrast");
        c.matugen.contrast = Some(0.0);
        assert_eq!(c.matugen_contrast(), None, "0 == matugen default -> omit");
        c.matugen.contrast = Some(0.6);
        assert_eq!(c.matugen_contrast(), Some(0.6));
        c.matugen.contrast = Some(5.0);
        assert_eq!(c.matugen_contrast(), Some(1.0), "clamped to 1");
        c.matugen.contrast = Some(-9.0);
        assert_eq!(c.matugen_contrast(), Some(-1.0), "clamped to -1");
    }

    #[test]
    fn empty_path_strings_fall_back_to_defaults() {
        let mut c = Config::default();
        c.paths.wallpaper = Some("   ".into());
        assert_eq!(c.wallpaper_dir(), home().join("Pictures/Wallpapers"));
    }

    #[test]
    fn niri_backdrop_source_requires_existing_path() {
        let mut n = NiriConfig::default();
        assert_eq!(n.backdrop_source(), None, "unset -> auto");
        n.backdrop = "  ".into();
        assert_eq!(n.backdrop_source(), None, "blank -> auto");
        n.backdrop = "/no/such/file.png".into();
        assert_eq!(n.backdrop_source(), None, "missing path -> auto");
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("bd.png");
        std::fs::write(&f, b"x").unwrap();
        n.backdrop = f.display().to_string();
        assert_eq!(n.backdrop_source(), Some(f.display().to_string().as_str()));
    }

    #[test]
    fn effects_auto_recolor_theme_defaults_to_catppuccin() {
        let mut e = EffectsConfig::default();
        assert_eq!(e.auto_recolor_theme(), "Catppuccin", "unset -> default theme");
        e.auto_theme = "  ".into();
        assert_eq!(e.auto_recolor_theme(), "Catppuccin", "blank -> default theme");
        e.auto_theme = "Tokyo Night".into();
        assert_eq!(e.auto_recolor_theme(), "Tokyo Night", "explicit theme wins");
    }

    #[test]
    fn steam_client_only_selected_for_steam_backend() {
        let mut c = SteamConfig { backend: SteamBackend::Steam, ..Default::default() };
        assert!(c.uses_steam_client(), "default/steam backend connects the steamworks client");
        c.backend = SteamBackend::Steamcmd;
        assert!(
            !c.uses_steam_client(),
            "steamcmd backend must never init the client (no Steam appid lock)"
        );
    }
}
