//! Planner configuration and the reducer and node-count heuristics.

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalPlannerConfig {
    /// Merge two ranges in the same object when the gap between them is at most this many bytes.
    pub max_gap_bytes: u64,
    /// Do not grow a coalesced request past this many bytes.
    pub max_request_bytes: u64,
    /// Max whole-fetch ReadAt requests in flight per node (matches runtime [`FetchStage`]).
    pub fetch_depth: usize,
    /// When true, cross-slot edges use HTTP fetch egress instead of Workers RPC.
    pub inter_do_fetch_wcs: bool,
    /// Estimated merged partial bytes above which a reduction tree may be inserted (phase 6+).
    pub reduction_tree_threshold_bytes: u64,
    /// When set, the plan ends in a Parquet sink with this many rows per row group.
    /// Placement follows the query: scan, filter, and projection sinks sit on the
    /// nodes; an aggregate, `ORDER BY`, or `LIMIT` sinks on the coordinator after
    /// that final step. Joins are rejected.
    pub parquet_row_group_rows: Option<u32>,
    /// Output ordinals for `partitionBy`, in request order. Empty means one file
    /// per node. Non-empty is allowed only when every node already emits final
    /// rows. Aggregates, global `ORDER BY`, and `LIMIT` are rejected.
    pub partition_columns: Vec<u32>,
    /// Floor for `clamp(ceil(planned_bytes / min_bytes_per_node), 1, max_nodes)`.
    /// `1` keeps a caller-supplied slot list fully used. The coordinator Wasm
    /// sets this above a Q1-sized scan (~25 KB) so that scan stays on one node.
    pub min_bytes_per_node: u64,
    /// Account or deployment cap. Not a requested worker count.
    pub max_nodes: u32,
    /// When a grouped aggregate's estimated state exceeds one reducer, partition it.
    pub hash_aggregate: HashAggregateConfig,
    /// When a row top-k reads its printed columns in a second pass.
    pub late_rows: crate::plan::late_rows::LateRowsConfig,
}

/// Memory budget for a hash-partitioned `GROUP BY`. The planner picks a power-of-two
/// reducer count from these numbers and does not change it at runtime.
///
/// Production default is **16 MiB flush / 32 MiB hard**.
/// After a pressure flush, empty-table retained capacity is reclaimed to an 8 MiB
/// low-water mark so it cannot sit above the flush lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashAggregateConfig {
    /// Start spilling when one reducer's accounted group table reaches this.
    /// Accounted bytes include keys, accumulators, retained hash capacity, and the interner.
    pub target_group_bytes: u64,
    /// Refuse the reducer once accounted group state exceeds this. Must be at least the target.
    pub hard_group_bytes: u64,
    /// The most reducers the planner picks on its own; it refuses the plan instead of going
    /// past this. An explicit `reducers` is bounded only by the stream limit. At least 1.
    pub max_reducers: u32,
    /// Bytes one scan may buffer across all partition outputs before it blocks.
    pub shuffle_buffer_bytes: u64,
    /// Force this power-of-two count. `None` uses the memory estimate. `Some(1)` stays on gather.
    pub reducers: Option<u32>,
    /// Spill overflow groups to R2 instead of hard-failing the group budget. On by
    /// default so a skewed reducer degrades to runs rather than failing the job.
    pub group_spill_enabled: bool,
}

impl Default for PhysicalPlannerConfig {
    fn default() -> Self {
        Self {
            max_gap_bytes: 128 * 1024,
            max_request_bytes: 2 * 1024 * 1024,
            // Scans are bound by R2 request latency, not bytes: eight 2 MiB fetches in
            // flight, and in flight plus resident at most sixteen, cap a node's read
            // buffering at 32 MiB (FetchStage::poll_host_fetch).
            fetch_depth: 8,
            inter_do_fetch_wcs: false,
            reduction_tree_threshold_bytes: u64::MAX,
            parquet_row_group_rows: None,
            partition_columns: Vec::new(),
            min_bytes_per_node: 1,
            // Each scan is one input stream on a reducer, so this is also the scan ceiling.
            max_nodes: wcol_engine::exec::stream_pool::MAX_STREAM_INPUT_PORTS as u32,
            hash_aggregate: HashAggregateConfig::default(),
            late_rows: crate::plan::late_rows::LateRowsConfig::default(),
        }
    }
}

impl Default for HashAggregateConfig {
    fn default() -> Self {
        Self {
            target_group_bytes: 16 * 1024 * 1024,
            hard_group_bytes: 32 * 1024 * 1024,
            // Up to the stream limit. The estimate picks more than 64 only for groups that do
            // not fit in 64: those spill whatever the count, and 128 reducers spread the spill
            // and its merge (Q32 at 32 x 128: 18-24 s against 37-44 s at 32 x 64).
            max_reducers: wcol_engine::exec::stream_pool::MAX_STREAM_OUTPUT_PORTS as u32,
            shuffle_buffer_bytes: 1024 * 1024,
            reducers: None,
            group_spill_enabled: true,
        }
    }
}

impl PhysicalPlannerConfig {
    /// Coordinator default: one node until the planned read exceeds 64 KB, then
    /// several, never more than `max_nodes`.
    pub fn for_coordinator(max_nodes: u32) -> Self {
        Self {
            min_bytes_per_node: 64 * 1024,
            max_nodes: max_nodes.max(1),
            ..Self::default()
        }
    }
}

impl PhysicalPlannerConfig {
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.max_request_bytes == 0 {
            return Err(PlanError::BadConfig);
        }
        if self.fetch_depth == 0 {
            return Err(PlanError::BadConfig);
        }
        if self.parquet_row_group_rows == Some(0) {
            return Err(PlanError::BadConfig);
        }
        if self.min_bytes_per_node == 0 || self.max_nodes == 0 {
            return Err(PlanError::BadConfig);
        }
        if self.hash_aggregate.max_reducers == 0
            || self.hash_aggregate.target_group_bytes == 0
            || self.hash_aggregate.hard_group_bytes < self.hash_aggregate.target_group_bytes
            || self.hash_aggregate.shuffle_buffer_bytes == 0
        {
            return Err(PlanError::BadConfig);
        }
        let host_cap = wcol_engine::exec::stream_pool::MAX_STREAM_INPUT_PORTS as u32;
        if self.max_nodes > host_cap {
            return Err(PlanError::Unsupported(
                "max_nodes exceeds per-node input stream limit",
            ));
        }
        if self.hash_aggregate.max_reducers > host_cap {
            return Err(PlanError::BadConfig);
        }
        Ok(())
    }
}

/// Bytes charged per group when no measured size is available.
pub const ESTIMATED_GROUP_BYTES: u64 = crate::ops::aggregate::ESTIMATED_GROUP_BYTES;

/// Power-of-two reducer count for `groups` distinct keys. `1` keeps the gather plan.
pub fn hash_reducer_count(
    groups: u64,
    bytes_per_group: u64,
    target_group_bytes: u64,
    max_reducers: u32,
) -> Result<u32, PlanError> {
    if groups == 0 || bytes_per_group == 0 || target_group_bytes == 0 {
        return Ok(1);
    }
    let memory = groups.saturating_mul(bytes_per_group);
    if memory <= target_group_bytes {
        return Ok(1);
    }
    let need = memory.div_ceil(target_group_bytes).max(2);
    let pow = need.next_power_of_two();
    let reducers = u32::try_from(pow)
        .map_err(|_| PlanError::Unsupported("hash aggregate exceeds max reducers"))?;
    if reducers > max_reducers {
        return Err(PlanError::Unsupported(
            "hash aggregate exceeds max reducers (host limit on input/output streams per node)",
        ));
    }
    Ok(reducers)
}

/// Estimated group state the gather plan may hold in one place. There every scan aggregates
/// its own rows and the coordinator merges the partials, so only partial rows cross the
/// network; a hash exchange ships every source row to reducers. Below this the merge is
/// cheaper even above one reducer's `target_group_bytes`: full-hits RegionID (a footer bound
/// of 131K groups) ran in 2.6–3.5s gathered against 4.0–4.4s through 16 reducers. The
/// estimate charges 384 bytes a group; the merge has no run-time budget to trip.
pub const GATHER_GROUP_BYTES: u64 = 64 * 1024 * 1024;

/// Rows one reducer ingests before another is worth starting. Measured on Cloudflare:
/// ~6M rows per reducer kept full-hits RegionID ingest to ~4s.
pub const ROWS_PER_REDUCER: u64 = 8 * 1024 * 1024;

/// Largest power of two the host can fan out to (reducers are a scan's output stream ports).
pub(super) fn largest_reducer_count(max_reducers: u32) -> u32 {
    let cap = max_reducers
        .min(wcol_engine::exec::stream_pool::MAX_STREAM_OUTPUT_PORTS as u32)
        .max(1);
    1 << (31 - cap.leading_zeros())
}

/// Reducer count when the request does not fix one.
///
/// Memory decides *whether* to hash-partition: groups within [`GATHER_GROUP_BYTES`] stay on
/// the gather plan, where scans pre-aggregate. Once partitioned, the count also scales with input rows so every
/// reducer ingests a bounded share in parallel. A memory need above the host limit is capped
/// instead of refused when spill can absorb the excess, or when the estimate is only the
/// row-count fallback (a key without a footer bound): that bound ignores filters and
/// duplicate keys, and a reducer that really overflows fails with the group-budget error.
pub fn auto_reducer_count(
    groups: u64,
    source_rows: u64,
    bytes_per_group: u64,
    hash: &HashAggregateConfig,
) -> Result<u32, PlanError> {
    // A row-count fallback (`groups >= source_rows`) bounds nothing: it stays partitioned.
    if groups < source_rows && groups.saturating_mul(bytes_per_group) <= GATHER_GROUP_BYTES {
        return Ok(1);
    }
    let largest = largest_reducer_count(hash.max_reducers);
    let memory = match hash_reducer_count(groups, bytes_per_group, hash.target_group_bytes, largest)
    {
        Ok(count) => count,
        Err(_) if hash.group_spill_enabled || groups >= source_rows => largest,
        Err(err) => return Err(err),
    };
    if memory <= 1 {
        return Ok(1);
    }
    let input = u32::try_from(
        source_rows
            .div_ceil(ROWS_PER_REDUCER)
            .max(1)
            .next_power_of_two(),
    )
    .unwrap_or(largest);
    Ok(memory.max(input).min(largest))
}

/// Distinct-group upper bound for `keys`. Falls back to `source_rows` when any
/// grouping column has no safe footer bound. The product of per-column bounds
/// is a joint-key upper bound; it is not a joint NDV estimate.
pub fn estimated_distinct_groups(
    keys: &[u32],
    metadata: &ParquetMetadataCatalog,
    source_rows: u64,
) -> u64 {
    if keys.is_empty() || source_rows == 0 {
        return 0;
    }
    let mut groups = 1u64;
    for key in keys {
        let Some(bound) = key_distinct_upper(*key, metadata) else {
            return source_rows;
        };
        groups = groups.saturating_mul(bound.max(1));
        if groups >= source_rows {
            return source_rows;
        }
    }
    groups.min(source_rows)
}

/// A hash scan keeps its partial aggregate (a combiner) and sends partial states instead of
/// raw rows, unless the footer proves its partial key near-unique. Edges between scans and
/// reducers are the slow part: a combiner sends each repeated key once per flush, and its
/// table is bounded, so it only loses when nearly every row is its own group. A key with no
/// footer bound (most text) combines. The partial key is the GROUP BY columns plus any
/// `COUNT(DISTINCT)` column.
pub(super) fn scan_combines(
    agg: &AggSpec,
    metadata: &ParquetMetadataCatalog,
    source_rows: u64,
) -> bool {
    let mut keys = agg.group_by.clone();
    if let Ok(Some(column)) = single_distinct_column(&agg.aggregates) {
        keys.push(column);
    }
    let mut groups = 1u64;
    for key in &keys {
        let Some(bound) = key_distinct_upper(*key, metadata) else {
            return true;
        };
        groups = groups.saturating_mul(bound.max(1));
    }
    groups < source_rows / 2
}

pub(super) fn key_distinct_upper(column: u32, metadata: &ParquetMetadataCatalog) -> Option<u64> {
    let index = column as usize;
    let physical = metadata.leaf_physical.get(index).copied();
    let mut bound: Option<u64> = None;
    if physical == Some(0) {
        bound = Some(2);
    }
    if let Some(Some(ndv)) = metadata.leaf_distinct_upper.get(index) {
        bound = Some(bound.map_or(*ndv, |known| known.min(*ndv)));
    }
    bound.filter(|count| *count > 0)
}

/// `clamp(ceil(planned_bytes / min_bytes_per_node), 1, max_nodes)`.
pub fn planned_node_slots(planned_bytes: u64, config: &PhysicalPlannerConfig) -> u32 {
    let min_bytes = config.min_bytes_per_node.max(1);
    let cap = config.max_nodes.max(1);
    let slots = planned_bytes.div_ceil(min_bytes).max(1);
    u32::try_from(slots).unwrap_or(u32::MAX).clamp(1, cap)
}
