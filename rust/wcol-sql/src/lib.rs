#![cfg(feature = "datafusion")]

//! DataFusion SQL frontend for wcol.
//!
//! Parsing and lowering live in `wcol::plan::sql` so the engine can plan a query. This crate
//! re-exports that API and keeps the older ingest-spec lowering.

mod lower;
mod output_schema;

#[cfg(feature = "native")]
mod plan_graph;
#[cfg(feature = "native")]
pub mod native;

pub use wcol::plan::sql::{
    expr_summary, lower_logical, parquet_physical_to_arrow, plan_sql, required_source_columns,
    statement_relations, LowerError, PlanError, PlannedQuery, WcolCatalog, WcolColumnMeta,
};

#[cfg(feature = "native")]
pub use native::catalog_from_wcol_leaves;
pub use lower::{lower_to_wcol, WcolLoweredPlan};
pub use output_schema::{job_output_fields, output_field_names, scanned_tables};
#[cfg(feature = "native")]
pub use plan_graph::plan_df_execution_wire;
