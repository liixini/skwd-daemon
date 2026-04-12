pub mod apply;
pub mod bootstrap;
pub mod cache;
pub mod clean_trash;
pub mod thumb;
pub mod watcher;
pub mod weather;

pub mod analysis;
pub mod matugen;
pub mod optimize;
pub mod steam;

mod dispatch;
pub use dispatch::dispatch;

pub const IMAGE_EXTS: &[&str] = &["jpg", "jpeg", "png", "webp", "bmp", "gif", "tiff", "tif", "avif"];
pub const VIDEO_EXTS: &[&str] = &["mp4", "webm", "mkv", "avi", "mov"];
