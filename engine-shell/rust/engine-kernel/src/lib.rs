mod diff;
mod patch;

pub use diff::{diff_serializable, diff_serializable_checked, diff_value};
pub use patch::{apply_patches, PatchSegment, ViewModelPatch};
