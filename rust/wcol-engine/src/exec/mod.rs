//! Scheduling, colocated graph, and host session.

pub mod colocated_plan;
pub(crate) mod dict_traffic;
pub mod fetch_stage;
pub mod graph_host;
pub mod graph_scheduler;
pub(crate) mod mem_growth;
pub mod operator;
pub mod operators;
pub mod phase;
pub mod runtime;
pub mod stream_pool;
pub mod trace;
pub mod wcol_session;
pub(crate) mod wcs_port;

pub use crate::program::{
    decode_groups_from_ingest, dict_columns_from_ingest_groups, ingest_groups_from_plan,
    IngestGroup, IngestGroupChunk,
};
pub use fetch_stage::{FetchStage, FetchStageMetrics, HostFetchRead, FETCH_TICKET_BASE};
pub use graph_host::GraphHost;
pub use graph_scheduler::{GraphPump, LinearColocatedGraph, PollSinkOutcome, SinkPoll};
pub use runtime::{
    drive_operator, encode_data_batch_projection, encode_frame, partial_state_from_wcs_bytes,
    partial_state_to_row_batch, partial_state_to_wcs_bytes, row_batches, AggregateOp, FilterOp,
    FinalizeOp, Operator, OperatorIo, OwnedWcolBatch, ParquetDecode, ReduceOp, StreamProgress,
    WcolStreamItem, FRAME_DATA_BATCH, FRAME_END, FRAME_PARTIAL,
};
pub use wcol_session::{open_wcol_plan, open_wcol_plan_shared, WcolSession};
