mod canonical;
mod discard;
mod encode;
mod host;
mod memory;
mod memory_sorted_store;
mod r2;
mod sorted_run;
mod store;

pub use canonical::{
    cmp_spill_key_parts, decode_canonical_key, encode_canonical_key, CanonicalKeyError,
};
pub use discard::DiscardRunStore;
pub use encode::{decode_partial_group_payload, encode_partial_group_payload};
pub use host::{
    HostSpillBridge, HostSpillRequest, SpillProgress, SPILL_OP_RUN_COMMIT, SPILL_OP_RUN_PUT,
    SPILL_OP_RUN_READ,
};

/// Pull [`SpillProgress::Ready`] or return [`SpillProgress::AwaitHost`] from the enclosing function.
#[macro_export]
macro_rules! spill_pull {
    ($expr:expr) => {
        match $expr {
            Ok($crate::spill::SpillProgress::Ready(value)) => value,
            Ok($crate::spill::SpillProgress::AwaitHost) => {
                return Ok($crate::spill::SpillProgress::AwaitHost)
            }
            Err(error) => return Err(error),
        }
    };
}
pub use memory::{SpillConfig, SpillMemoryLedger, ESTIMATED_GROUP_BYTES};
pub use memory_sorted_store::MemorySortedRunStore;
pub use r2::{
    decode_run_put_open, decode_run_read_open, encode_run_put_open, encode_run_read_open,
    HostR2SortedRunStore, RunPutPlan, RunReadPlan, RUN_READ_CHUNK,
};
pub use sorted_run::{
    decode_run, encode_record, encode_run, planned_run_bytes, RunRecord, RunStreamDecoder,
    SpillRunDescriptor, RUN_HEADER_LEN, RUN_MAGIC, RUN_VERSION,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use store::FileAppendSpillStore;
pub use store::{KeyPayload, KeyedRow, SpillScanBatch, SpillScanResume, SpillStore};
