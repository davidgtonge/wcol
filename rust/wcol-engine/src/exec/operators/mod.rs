//! Helpers for [`GraphHost`](crate::exec::graph_host::GraphHost) and projection wire formats.

mod ingest_query;
mod wcs_frame;
mod wcs_projection;
mod wcs_row_stream;

pub(crate) use ingest_query::EmitPartial;
pub use wcs_frame::WcsIngress;
pub(crate) use wcs_frame::{progress_after_io, CompleteFrame};
pub(crate) use wcs_projection::owned_batch_from_projection_body;
pub use wcs_projection::owned_batch_from_projection_body_with_dicts;

pub use wcs_projection::projection_body_from_owned_batch;
pub use wcs_row_stream::{
    demux_partition_stream, egress_peak_pending_bytes, egress_pending_bytes, note_egress_pending,
    RowBatchWcsEgress, RowBatchWcsIngress,
};
