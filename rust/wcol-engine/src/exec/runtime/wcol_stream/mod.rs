//! Colocated graph driver and WCS edge helpers (operator bodies live in `ops/`).

mod drive;
mod partial_batch;
mod pwcl_edge;

pub use drive::{drive_operator, row_batches};
pub use partial_batch::partial_state_to_row_batch;
pub use pwcl_edge::{partial_state_from_wcs_bytes, partial_state_to_wcs_bytes};
