//! Canonical WCOL batches, dictionaries, partial state, and WCS1 wire framing.

pub mod batch;
pub mod core;
pub mod native_poll;
pub mod partial_wcs;
pub mod partials;
pub mod projection;
pub mod wcs;

pub use wcs::{
    encode_data_batch_projection, encode_frame, FRAME_DATA_BATCH, FRAME_DICTIONARY, FRAME_END,
    FRAME_PARTIAL,
};

pub use batch::{
    DictEntries, DictionaryValues, OwnedWcolBatch, SelectionMask, WcolBatch, WcolColumn,
    WcolValueKind,
};
pub use partial_wcs::{
    partial_first_sum_from_wire, partial_state_from_wire_payload, partial_state_to_wcs_payload,
    PWCL_MAGIC,
};
pub use partials::{partial_snapshot_from_batch, partial_snapshot_from_wire};
