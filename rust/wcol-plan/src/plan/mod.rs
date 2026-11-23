//! Query planning and placement (Rust-owned).

pub mod bind;
pub mod coordinator_fragment;
pub mod distributed;
pub mod execution_graph;
pub mod footer_split;
pub mod hash_group;
pub mod host_ffi;
pub mod job;
pub mod job_plan;
pub mod late_rows;
pub mod logical;
pub mod logical_from_parser;
pub mod logical_wire;
pub mod node_fragment;
pub mod output_plan;
pub mod page_compaction;
pub mod parquet_concat;
pub mod physical_plan;
pub mod query_dispatch;
pub mod range_sample;
pub mod range_sort;
pub mod result;
pub mod sql_parser;
pub mod to_ir;

pub use crate::program::ingest as physical;
pub use crate::program::stream_program;
pub use crate::program::{
    decode_groups_from_ingest, dict_columns_from_ingest_groups, ingest_groups_from_plan,
    IngestGroup, IngestGroupChunk, IngestMemoryProfile,
};
pub use bind::{columns_for_sql, output_columns_for_rows, row_order_columns};
pub use coordinator_fragment::{
    coordinator_steps, decode_coordinator_steps, encode_coordinator_program, CoordinatorStep,
    ExportColumn,
};
pub use distributed::{
    attempt_object_key, coordinator_reduce_finalize_rows, coordinator_reduce_partials,
    estimate_output_bytes, verify_output_manifest, verify_scan_task_coverage,
    workers_for_target_file, OutputObjectRecord, ParquetOutputPolicy, ScanTaskInput, SourceVersion,
    TaskId,
};
pub use execution_graph::{
    attach_output_plan, edge_transport, encode_execution_plan_wire, execution_plan_from_dispatch,
    worker_slot, EdgeTransport, ExecutionEdge, ExecutionPlan, ExecutionTask, RootOutputKind,
    SlotId, COORDINATOR_SLOT, WIRE_TAG_EXECUTION_GRAPH_V1, WIRE_TAG_EXECUTION_GRAPH_V3,
    WIRE_TAG_METADATA_PARTIAL,
};
pub use hash_group::{run_hash_aggregate, HashAggregateStats};
pub use job::{
    bind_partition_columns, manifest_object_key, parse_job_request, AttemptLog, AttemptReport,
    IdempotencyTable, JobRequest, NamedObject, ObjectLocation,
};
pub use logical_from_parser::{lower_query_plan, lower_sql_v0_to_logical, LowerQueryPlanError};
pub use logical_wire::{decode_logical_plan, encode_logical_plan, LogicalWireError};
pub use node_fragment::{
    decode_node_fragment, encode_node_fragment, encode_node_fragment_program, node_fragments,
    FragmentError, FragmentStep, FragmentTail, NodeFragment,
};
pub use output_plan::{
    concat_finalizer_for_graph_tasks, default_layout_for_parquet_job,
    default_parquet_export_layout, dispatch_is_unordered_parquet_parts, parse_layout_from_job,
    plan_has_global_sort_or_limit, OutputDestination, OutputFinalizer, OutputFormat, OutputLayout,
    OutputPlan,
};
pub use page_compaction::{
    catalog_from_leaves, encode_page_compact_wire, plan_page_compaction,
    plan_preserve_row_group_concat, validate_compaction_inputs, verify_page_compaction_manifest,
    CompactionInputFile, CopySegment, MergedColumnPlan, MergedRowGroupPlan, PageCompactionPartPlan,
    PageCompactionPartRecord, PageCompactionPlan, PageCompactionPolicy,
};
pub use parquet_concat::{
    concat_parquet_row_groups, CommittedParquetShard, ConcatPolicy,
    DEFAULT_CONCAT_FOOTER_BUDGET_BYTES,
};
pub use physical_plan::{
    estimated_distinct_groups, hash_reducer_count, lower_to_physical, AggExpr, AggKind, ByteSpan,
    ChunkMeta, CmpOp, Compare, FetchPiece, FetchRequest, Literal, LogicalOp, LogicalRead, NodeId,
    NodeMetrics, OpKind, ParquetMetadataCatalog, ParquetObject, PhysicalPlannerConfig,
    PlanError as PhysicalPlanError, PlannerMetrics, Predicate, SinkKind, SortKey, SortTarget,
    TableMeta, WcolLogicalPlan, WcolPhysicalPlan,
};
pub use query_dispatch::{
    catalog_from_footer, plan_query_dispatch, plan_query_dispatch_wire, plan_query_job,
    NodeProgram, QueryDispatch,
};
pub use stream_program::PlanCursor;
pub use wcol_engine::exec::stream_pool::{MAX_STREAM_INPUT_PORTS, MAX_STREAM_OUTPUT_PORTS};
