//! Distributed global sort planning: sampled boundaries and in-memory capacity limits.

use crate::program::types::SortKey;
use wcol_engine::ir::Scalar;
use wcol_engine::ops::sort::DEFAULT_SORT_BYTE_BUDGET;

#[derive(Clone, Debug)]
pub struct SampledSortBoundaries {
    pub keys: Vec<SortKey>,
    /// `partitions - 1` interior split keys in sort order.
    pub splits: Vec<Vec<Scalar>>,
}

/// Refuse plans whose estimated sort working set exceeds aggregate partition memory.
pub fn refuse_if_exceeds_sort_capacity(
    estimated_sort_bytes: u64,
    partitions: u32,
    safety_margin: f64,
) -> Result<(), String> {
    if partitions == 0 {
        return Err("range sort requires at least one partition".into());
    }
    let budget = (partitions as u64).saturating_mul(DEFAULT_SORT_BYTE_BUDGET as u64);
    let allowed = ((budget as f64) * safety_margin) as u64;
    if estimated_sort_bytes > allowed {
        return Err(format!(
            "estimated sort working set {estimated_sort_bytes} bytes exceeds allowed {allowed} \
             (partitions={partitions}, per-partition budget={DEFAULT_SORT_BYTE_BUDGET})"
        ));
    }
    Ok(())
}

/// Partition count from estimated bytes and per-partition sort budget.
pub fn partition_count_for_sort(estimated_sort_bytes: u64, max_partitions: u32) -> u32 {
    let max_partitions = max_partitions.max(1);
    let needed = estimated_sort_bytes
        .max(1)
        .div_ceil(DEFAULT_SORT_BYTE_BUDGET as u64) as u32;
    needed.clamp(2, max_partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_when_estimate_exceeds_capacity() {
        let err = refuse_if_exceeds_sort_capacity(DEFAULT_SORT_BYTE_BUDGET as u64 * 200, 100, 0.9)
            .unwrap_err();
        assert!(err.contains("exceeds allowed"));
    }
}
