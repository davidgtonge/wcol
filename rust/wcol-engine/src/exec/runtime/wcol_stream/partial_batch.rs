//! Attach typed partial-state columns to [`OwnedWcolBatch`] for colocated edges.

use crate::stream::partials::{partial_state_to_batch, PartialStateWcol};

use crate::stream::OwnedWcolBatch;

/// Build a row batch carrying partial-state columns and metadata (synthetic column ids).
pub fn partial_state_to_row_batch(state: &PartialStateWcol) -> OwnedWcolBatch {
    partial_state_to_batch(state.clone())
}
