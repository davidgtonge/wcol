//! WCS `FRAME_PARTIAL` body encoding (`PWCL`).

use crate::stream::partial_wcs::{partial_state_from_wire_payload, partial_state_to_wcs_payload};
use crate::stream::partials::PartialStateWcol;

/// WCS `FRAME_PARTIAL` payload from logical partial state (I-03).
pub fn partial_state_to_wcs_bytes(state: &PartialStateWcol) -> Result<Vec<u8>, String> {
    partial_state_to_wcs_payload(state)
}

pub fn partial_state_from_wcs_bytes(bytes: &[u8]) -> Result<PartialStateWcol, String> {
    partial_state_from_wire_payload(bytes)
}
