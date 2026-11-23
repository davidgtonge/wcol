//! WCOL unified streaming engine.

#![deny(unused, dead_code)]

pub use wcol_engine::error;
pub use wcol_engine::exec;
pub use wcol_engine::formats;
pub use wcol_engine::ir;
pub use wcol_engine::ops;
pub use wcol_engine::program;
pub use wcol_engine::stream;
pub use wcol_plan::plan;

/// Keep engine and planner C exports in the cdylib. Copying the pointer tables
/// emits relocations so `--gc-sections` does not drop `no_mangle` symbols that
/// live in the rlib dependencies.
#[allow(dead_code)]
#[used]
static KEEP_ENGINE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol;
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI_LEN] =
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI;
#[allow(dead_code)]
#[used]
static KEEP_ITERATIVE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol; 6] =
    wcol_engine::exec::runtime::ffi::ITERATIVE_COORDINATOR_FFI;
#[allow(dead_code)]
#[used]
static KEEP_REDUCE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol; 5] =
    wcol_engine::exec::runtime::ffi::REDUCE_EXPORTED_FFI;
#[allow(dead_code)]
#[used]
static KEEP_PLAN_FFI: [wcol_plan::plan::host_ffi::FfiSymbol;
    wcol_plan::plan::host_ffi::EXPORTED_FFI_LEN] = wcol_plan::plan::host_ffi::EXPORTED_FFI;

pub use error::{WcolError, WcolResult};

// Pipeline: decode → WCOL stream → ops → encode (see docs/architecture.md).
pub use exec::runtime::{
    drive_operator, encode_data_batch_projection, encode_frame, open_wcol_plan,
    partial_state_from_wcs_bytes, partial_state_to_wcs_bytes, AggregateOp, FilterOp, FinalizeOp,
    Operator, ReduceOp, WcolStreamItem, FRAME_DATA_BATCH, FRAME_END,
};
pub use exec::{
    decode_groups_from_ingest, dict_columns_from_ingest_groups, ingest_groups_from_plan, GraphHost,
    GraphPump, IngestGroup, IngestGroupChunk, LinearColocatedGraph, PollSinkOutcome, SinkPoll,
};
#[cfg(not(target_arch = "wasm32"))]
pub use formats::parquet::{aggregate_column, open_plan};
pub use formats::parquet::{Agg, Extreme, Sum};
pub use formats::parquet::{Leaf, NumericScan, Plan, ScanLimits, ScanValueMode};
pub use formats::parquet::{ParquetDecode, ParquetEncode, ParquetEncodeConfig};
pub use ir::CatalogColumn;
pub use ops::query::{encoded_partials_equivalent, partial_first_sum_i, SelectionMask};
pub use plan::columns_for_sql;
pub use plan::{
    attempt_object_key, coordinator_reduce_finalize_rows, coordinator_reduce_partials,
    estimate_output_bytes, plan_page_compaction, verify_output_manifest,
    verify_page_compaction_manifest, verify_scan_task_coverage, workers_for_target_file,
    CompactionInputFile, IngestMemoryProfile, OutputObjectRecord, PageCompactionPartRecord,
    PageCompactionPlan, PageCompactionPolicy, ParquetOutputPolicy, ScanTaskInput, SourceVersion,
    TaskId,
};
pub use stream::OwnedWcolBatch;
pub use wcol_engine::spill;
pub use wcol_engine::stream::native_poll;
