//! Hash, range and gather exchanges between fragments.

use super::*;

pub(super) fn is_exchange(kind: &OpKind) -> bool {
    matches!(
        kind,
        OpKind::Exchange | OpKind::HashExchange { .. } | OpKind::RangeExchange { .. }
    )
}

/// `(sort keys, partitions, split points)` of a range exchange.
pub(super) type RangeExchange = (Vec<SortKey>, u32, Vec<Vec<Scalar>>);

pub(super) fn range_exchange_for(
    shape: &Shape,
    node_count: usize,
    row_topk: &Option<(Vec<SortKey>, u64)>,
    coordinator_parquet: bool,
    estimated_sort_bytes: u64,
    metadata: &ParquetMetadataCatalog,
) -> Result<Option<RangeExchange>, PlanError> {
    if node_count < 2 {
        return Ok(None);
    }
    if shape.aggregate.is_some() {
        return Ok(None);
    }
    if row_topk.is_some() {
        return Ok(None);
    }
    if !coordinator_parquet {
        return Ok(None);
    }
    let mut sort_keys = None;
    for step in &shape.after {
        if let After::Sort(keys) = step {
            sort_keys = Some(keys.clone());
            break;
        }
    }
    let Some(keys) = sort_keys else {
        return Ok(None);
    };
    let partitions = crate::plan::range_sort::partition_count_for_sort(
        estimated_sort_bytes.max(1),
        wcol_engine::exec::stream_pool::MAX_STREAM_OUTPUT_PORTS as u32,
    );
    crate::plan::range_sort::refuse_if_exceeds_sort_capacity(estimated_sort_bytes, partitions, 0.9)
        .map_err(|_| PlanError::Unsupported("sort working set exceeds partition memory budget"))?;
    let boundaries =
        crate::plan::range_sample::boundaries_from_catalog(&keys, partitions, metadata)
            .map_err(PlanError::RangeSort)?;
    Ok(Some((keys, partitions, boundaries.splits)))
}

/// The one `COUNT(DISTINCT)` column, when every such aggregate counts that same column.
///
/// `Err` is more than one distinct column. The engine folds a single hidden key, so a plan
/// that pretended otherwise would trap instead of reporting the limit.
pub(super) fn single_distinct_column(aggregates: &[AggExpr]) -> Result<Option<u32>, ()> {
    let mut column = None;
    for aggregate in aggregates {
        if aggregate.kind != AggKind::CountDistinct {
            continue;
        }
        let Some(this) = aggregate.column else {
            return Err(());
        };
        if column.is_some_and(|seen| seen != this) {
            return Err(());
        }
        column = Some(this);
    }
    Ok(column)
}

/// Exchange keys of a hash aggregate. A `COUNT(DISTINCT)` also partitions on the value it
/// counts, including when `SUM` or `COUNT` sit beside it. One (group, value) pair reaches one
/// reducer; that reducer folds the value away and the finalizer sums the partial sums and the
/// partial distinct counts. Partitioning on the group alone would leave every distinct value of
/// a large group on one reducer.
pub fn exchange_keys(group_by: &[u32], aggregates: &[AggExpr]) -> Vec<u32> {
    let mut keys = group_by.to_vec();
    if let Ok(Some(column)) = single_distinct_column(aggregates) {
        if !keys.contains(&column) {
            keys.push(column);
        }
    }
    keys
}

/// Groups a reducer must own on average before a `COUNT(DISTINCT)` partitions on the group
/// alone. Below it (Q8/Q9 by RegionID) one large group's values would all reach one reducer,
/// so the counted value joins the keys. Above it (Q13 by SearchPhrase) each group is complete
/// on one reducer, its count is final there, and reducers can rank before the finalizer.
pub(super) const GROUP_ONLY_DISTINCT_MIN_GROUPS: u64 = 4096;

/// Dictionary pages below this mark a key with few values. A string key has no footer bound,
/// but its dictionary page size shows it: on full hits MobilePhoneModel's largest page is
/// 829 bytes and SearchPhrase's about 600 KB.
pub(super) const FEW_VALUES_DICTIONARY_BYTES: u64 = 64 * 1024;

/// Every chunk of `column` is dictionary-encoded with a small dictionary.
pub(super) fn few_valued_key(column: u32, metadata: &ParquetMetadataCatalog) -> bool {
    metadata
        .leaf_dictionary_bytes
        .get(column as usize)
        .copied()
        .flatten()
        .is_some_and(|bytes| bytes < FEW_VALUES_DICTIONARY_BYTES)
}

/// Planned state of one group: the fixed estimate, plus a dense sketch for each
/// approximate distinct. On the gather plan every scan holds every group's sketches.
pub(super) fn group_state_bytes(aggregates: &[AggExpr]) -> u64 {
    let sketches = aggregates
        .iter()
        .filter(|agg| agg.kind == AggKind::ApproxDistinct)
        .count() as u64;
    ESTIMATED_GROUP_BYTES + sketches * wcol_engine::ops::query::hll::HLL_DENSE_BYTES as u64
}

pub(super) fn hash_exchange_for(
    shape: &Shape,
    hash: &HashAggregateConfig,
    estimated_groups: u64,
    source_rows: u64,
    metadata: &ParquetMetadataCatalog,
) -> Result<Option<(Vec<u32>, u32)>, PlanError> {
    let leaf_physical = metadata.leaf_physical.as_slice();
    let Some(agg) = &shape.aggregate else {
        return Ok(None);
    };
    if single_distinct_column(&agg.aggregates).is_err() {
        return Err(PlanError::Unsupported(
            "COUNT(DISTINCT) on more than one column is not supported",
        ));
    }
    let keys = exchange_keys(&agg.group_by, &agg.aggregates);
    if keys.is_empty() || leaf_physical.is_empty() {
        return Ok(None);
    }
    // COUNT(DISTINCT) beside SUM/COUNT/AVG is summable once each (group, value) is on one
    // reducer. The estimate then includes the distinct column, so a value with no footer
    // bound cannot stay on the gather plan. Every aggregate partitions: an approximate
    // distinct's sketches merge per group, so routing by group sends each group's sketch
    // once instead of once per scan.
    for key in &keys {
        // Integer, float, and string keys all hash by logical value. BYTE_ARRAY and
        // FIXED_LEN_BYTE_ARRAY (6, 7) hash the UTF-8 bytes, never the row-group dictionary id.
        if leaf_physical.get(*key as usize).is_none() {
            return Ok(None);
        }
    }
    let reducers = match hash.reducers {
        Some(fixed) if fixed <= 1 => return Ok(None),
        // An explicit count is the caller's choice: only the stream limit bounds it, not
        // the ceiling the planner keeps for its own choice.
        Some(fixed) => {
            if !fixed.is_power_of_two() {
                return Err(PlanError::Unsupported(
                    "hash reducer override must be a power of two",
                ));
            }
            if fixed as usize > wcol_engine::exec::stream_pool::MAX_STREAM_OUTPUT_PORTS {
                return Err(PlanError::Unsupported(
                    "hash reducer count exceeds the per-node output stream limit",
                ));
            }
            fixed
        }
        None => auto_reducer_count(
            estimated_groups,
            source_rows,
            group_state_bytes(&agg.aggregates),
            hash,
        )?,
    };
    if reducers <= 1 {
        return Ok(None);
    }
    // Few-valued keys (Q10/Q11 by MobilePhoneModel) keep the value in the keys: grouping
    // alone would put each large model's users on one reducer.
    let group_only = keys.len() > agg.group_by.len()
        && !agg.group_by.is_empty()
        && !agg
            .group_by
            .iter()
            .all(|key| few_valued_key(*key, metadata))
        && estimated_distinct_groups(&agg.group_by, metadata, source_rows)
            >= u64::from(reducers).saturating_mul(GROUP_ONLY_DISTINCT_MIN_GROUPS);
    let keys = if group_only {
        agg.group_by.clone()
    } else {
        keys
    };
    Ok(Some((keys, reducers)))
}

pub(super) fn gather(operators: &mut Vec<Operator>, producers: &[u32]) -> Result<u32, PlanError> {
    if producers.is_empty() {
        return Err(PlanError::Inconsistent);
    }
    Ok(push(operators, None, OpKind::Exchange, producers.to_vec()))
}

pub(super) fn push(
    operators: &mut Vec<Operator>,
    node: Option<NodeId>,
    kind: OpKind,
    inputs: Vec<u32>,
) -> u32 {
    let id = operators.len() as u32;
    operators.push(Operator {
        id,
        node,
        kind,
        inputs,
    });
    id
}
