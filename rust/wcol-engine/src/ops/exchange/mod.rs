//! Shuffle / partition exchange operators and physical partitioning metadata.

mod hash_partition;
mod partitioning;
mod range_partition;
mod typed_hash;

pub use crate::exec::operators::{demux_partition_stream, RowBatchWcsEgress, RowBatchWcsIngress};
pub use crate::stream::OwnedWcolBatch;
pub use hash_partition::{HashPartitionIo, HashPartitionOp, HashPartitionStats};
pub use partitioning::{BatchPartitionPlan, ExchangePartitioning, HashPartitionPlan};
pub use range_partition::RangePartitionPlan;
pub use typed_hash::{
    exchange_hash_seed, hash_row_keys_at, join_key_fingerprint_at, join_key_is_null,
    join_keys_equal, partition_for_row, partition_index, CombinerRoute,
    WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
};

/// Split one batch across partition outputs (hash or range plan).
pub fn split_owned_batch(
    plan: &BatchPartitionPlan,
    batch: &OwnedWcolBatch,
) -> Result<Vec<OwnedWcolBatch>, String> {
    match plan {
        BatchPartitionPlan::Hash(hash) => hash_partition::split_owned_batch(hash, batch),
        BatchPartitionPlan::Range(range) => range_partition::split_owned_batch(range, batch),
    }
}
