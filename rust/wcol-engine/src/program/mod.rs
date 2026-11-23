//! Wire contract for the node and the coordinator. No SQL text and no planner.

pub mod codec;
pub mod coordinator;
pub mod fragment;
pub mod ingest;
pub mod page_copy;
pub mod row_id;
pub mod stream_program;
pub mod to_ir;
pub mod types;

pub use coordinator::{
    decode_coordinator_steps, encode_coordinator_program, encode_finalize_program, CoordinatorStep,
    ExportColumn,
};
pub use fragment::{
    decode_node_fragment, encode_hash_reducer_program, encode_node_fragment,
    encode_node_fragment_program, FragmentError, FragmentStep, FragmentTail, NodeFragment,
    PartitionInput,
};
pub use ingest::{
    decode_groups_from_ingest, dict_columns_from_ingest_groups, ingest_groups_from_plan,
    IngestGroup, IngestGroupChunk, IngestMemoryProfile,
};
pub use page_copy::{CopySegment, MergedColumnPlan, MergedRowGroupPlan};
pub use stream_program::PlanCursor;
pub use types::{ClaimUnit, FetchRequest, LogicalRead, NodeId};
