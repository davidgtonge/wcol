//! Columnar execution engine.
//!
//! Plans, SQL text, and DataFusion stay in `wcol-plan`. This crate runs IR and the
//! fragment / PWCL / WCS wire formats.

#![deny(unused, dead_code)]

pub mod error;
pub mod exec;
pub mod formats;
pub mod ir;
pub mod ops;
pub mod program;
pub mod spill;
pub mod stream;

pub use error::{WcolError, WcolResult};
pub use exec::{
    decode_groups_from_ingest, dict_columns_from_ingest_groups, ingest_groups_from_plan, GraphHost,
    GraphPump, IngestGroup, IngestGroupChunk, LinearColocatedGraph, PollSinkOutcome, SinkPoll,
};
pub use ir::CatalogColumn;
pub use stream::OwnedWcolBatch;
