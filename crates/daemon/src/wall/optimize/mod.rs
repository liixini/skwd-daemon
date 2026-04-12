pub mod image;
pub mod video;

mod dispatch;
pub use dispatch::dispatch;

pub use image::{OptimizeState, optimize_single_inline, should_optimize, start as start_optimize};
pub use video::ConvertState;
