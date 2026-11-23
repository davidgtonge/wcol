//! Stream runtime: WCS framing, colocated graph driver, Wasm FFI.

pub(crate) mod stream_io;
pub(crate) mod wcol_stream;

pub use crate::exec::wcol_session::open_wcol_plan;
pub use crate::formats::parquet::ParquetDecode;
pub use crate::ops::query::SelectionMask;
pub use crate::ops::{AggregateOp, FilterOp, FinalizeOp, ReduceOp};
pub use crate::stream::OwnedWcolBatch;
pub use stream_io::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
pub use wcol_stream::{
    drive_operator, partial_state_from_wcs_bytes, partial_state_to_row_batch,
    partial_state_to_wcs_bytes, row_batches,
};

pub mod ffi;
pub mod stream_pool_ffi;

pub use crate::stream::wcs::{
    encode_data_batch_projection, encode_frame, FRAME_DATA_BATCH, FRAME_END, FRAME_PARTIAL,
};
