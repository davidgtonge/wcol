//! Batch query engine: runs filter + aggregate plans column-at-a-time over [`WcolBatch`]es.
//!
//! Kernels come from the `.wcol` file engine (packed `u32` row masks, predicates evaluated
//! once per dictionary entry, dense dictionary-id slots for `GROUP BY`) but take in-memory
//! batch columns, so the engine knows nothing about the source format.
//!
//! Output is a [`PartialSnapshot`], so partial encoding, reduce and finalize stay on that type.

mod filter;
mod profile;
pub use profile::{
    clear_spill_boundaries, last_resident_group_bytes, last_resident_groups,
    last_spill_groups_flushed, last_spill_peak_interner_bytes, last_spill_peak_live_bytes,
    last_spill_runs, note_spill_merge_scan_begin, note_spill_merge_step, note_spill_serialize,
    phase_timers, reset_phase_timers, spill_boundary_wire, SpillBoundary,
};
use profile::{
    note_spill_boundary, phase, phase_on, publish_spill_stats, reset_flush_probe,
    spill_merge_counters, wasm_linear_bytes, FlushStart, FLUSH_PROBE, FLUSH_START, LAST_PAGE,
    PHASE_FILTER_NS, PHASE_FINISH_NS, PHASE_GROUPS_IN, PHASE_GROUPS_OUT, PHASE_HASH_NS,
    RESIDENT_GROUPS, RESIDENT_GROUP_BYTES,
};

mod chunked;
pub(crate) mod finish_stream;
mod group_spill;
mod mask;
mod pair_groups;
#[cfg(test)]
mod retain_capacity;
mod string_filter;
mod string_intern;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::float_key::{canonical_f64, float_key, key_to_f64, nan_key};
use super::hll::Hll;
use super::{
    count_stat, distinct_column, empty_stat, exact_f64, reject_unsupported_predicates, shell,
    stat_to_partial, validate_aggregate, AggRef, CatalogColumn, Partial, PartialAggSpec,
    PartialGroup, PartialKeyPart, PartialSnapshot, PartialStat, Stat, KIND_APPROX_DISTINCT,
    KIND_COUNT, KIND_COUNT_DISTINCT, KIND_COUNT_STAR, KIND_MAX, KIND_MIN,
};
use crate::ir::{AggExpr, ColumnKind, Predicate};
use crate::spill::{encode_canonical_key, encode_partial_group_payload, SpillProgress};
use crate::spill_pull;
use crate::stream::partials::{
    KeyPartRef, PartialColumnsBuilder, PartialRows, PartialStateSchemaV1, PartialStateWcol,
    PartialStringKeyColumn, PARTIAL_SCHEMA_VERSION,
};
use crate::stream::{SelectionMask, WcolBatch, WcolColumn};
use chunked::Chunked;
use filter::ColumnFilter;
use hashbrown::hash_table::{Entry as HashEntry, HashTable};
use pair_groups::{PairGroups, PairKey};
use rustc_hash::FxBuildHasher;
use std::hash::BuildHasher;
use string_filter::ByteDictKey;
use string_intern::{col_string_bytes, StringInterner};

/// `GROUP BY` keys fit a fixed-width hash key; the planner allows no more than this.
const MAX_KEYS: usize = crate::ir::MAX_GROUP_KEYS;

/// Empty grouping state kept after a flush. Above this, the table is recreated
/// so retained capacity cannot stay above the flush threshold.
const RETAINED_CAPACITY_TARGET: u64 = 8 * 1024 * 1024;
const NO_SLOT: u32 = u32::MAX;

/// One input column of a batch, by catalog id.
#[derive(Clone, Copy)]
pub(crate) enum ColRef<'a> {
    I64 {
        values: &'a [i64],
        nulls: &'a [u8],
    },
    Dict {
        ids: &'a [u32],
        nulls: &'a [u8],
        entries: &'a Arc<Vec<i64>>,
    },
    DictBytes {
        ids: &'a [u32],
        nulls: &'a [u8],
        offsets: &'a Arc<Vec<u32>>,
        bytes: &'a Arc<Vec<u8>>,
    },
    Utf8 {
        offsets: &'a [u32],
        bytes: &'a [u8],
        nulls: &'a [u8],
    },
    F64 {
        values: &'a [f64],
        nulls: &'a [u8],
    },
    DictF64 {
        ids: &'a [u32],
        nulls: &'a [u8],
        entries: &'a Arc<Vec<f64>>,
    },
}

impl ColRef<'_> {
    #[inline]
    fn nulls(&self) -> &[u8] {
        match self {
            ColRef::I64 { nulls, .. }
            | ColRef::Dict { nulls, .. }
            | ColRef::DictBytes { nulls, .. }
            | ColRef::Utf8 { nulls, .. }
            | ColRef::F64 { nulls, .. }
            | ColRef::DictF64 { nulls, .. } => nulls,
        }
    }

    #[inline]
    fn value(&self, row: usize) -> i64 {
        match self {
            ColRef::I64 { values, .. } => values[row],
            ColRef::Dict { ids, entries, .. } => entries[ids[row] as usize],
            ColRef::DictBytes { .. }
            | ColRef::Utf8 { .. }
            | ColRef::F64 { .. }
            | ColRef::DictF64 { .. } => 0,
        }
    }
}

pub(crate) type Cols<'a> = Vec<Option<ColRef<'a>>>;

fn cols_of<'a>(batch: &WcolBatch<'a>, width: usize) -> Result<Cols<'a>, String> {
    let rows = batch.row_count;
    let mut cols: Cols<'a> = vec![None; width];
    for col in batch.columns {
        let (column, col_ref, len, nulls_len) = match *col {
            WcolColumn::I64 {
                column,
                values,
                nulls,
            } => (
                column,
                ColRef::I64 { values, nulls },
                values.len(),
                nulls.len(),
            ),
            WcolColumn::DictId {
                column,
                ids,
                nulls,
                dictionary,
            } => {
                if let Some(entries) = dictionary.int_entries() {
                    if ids.iter().any(|id| *id as usize >= entries.len()) {
                        return Err("dictionary index out of range".to_string());
                    }
                    (
                        column,
                        ColRef::Dict {
                            ids,
                            nulls,
                            entries,
                        },
                        ids.len(),
                        nulls.len(),
                    )
                } else if let Some((offsets, bytes)) = dictionary.byte_entries() {
                    let n = dictionary.entry_count();
                    if ids.iter().any(|id| *id as usize >= n) {
                        return Err("dictionary index out of range".to_string());
                    }
                    (
                        column,
                        ColRef::DictBytes {
                            ids,
                            nulls,
                            offsets,
                            bytes,
                        },
                        ids.len(),
                        nulls.len(),
                    )
                } else if let Some(entries) = dictionary.float_entries() {
                    if ids.iter().any(|id| *id as usize >= entries.len()) {
                        return Err("dictionary index out of range".to_string());
                    }
                    (
                        column,
                        ColRef::DictF64 {
                            ids,
                            nulls,
                            entries,
                        },
                        ids.len(),
                        nulls.len(),
                    )
                } else {
                    return Err("dictionary column has no entries".to_string());
                }
            }
            WcolColumn::F64 {
                column,
                values,
                nulls,
            } => (
                column,
                ColRef::F64 { values, nulls },
                values.len(),
                nulls.len(),
            ),
            WcolColumn::Utf8 {
                column,
                offsets,
                bytes,
                nulls,
            } => {
                if offsets.len() != rows + 1 {
                    return Err("utf8 offsets length mismatch".to_string());
                }
                (
                    column,
                    ColRef::Utf8 {
                        offsets,
                        bytes,
                        nulls,
                    },
                    nulls.len(),
                    nulls.len(),
                )
            }
        };
        if len != rows || nulls_len != rows {
            return Err("decoded columns have different lengths".to_string());
        }
        if let Some(slot) = cols.get_mut(column as usize) {
            *slot = Some(col_ref);
        }
    }
    Ok(cols)
}

fn column<'a>(cols: &Cols<'a>, id: u32) -> Result<ColRef<'a>, String> {
    cols.get(id as usize)
        .copied()
        .flatten()
        .ok_or_else(|| "column was not decoded".to_string())
}

/// Mask words for a selection (a full mask when every row is selected).
fn selection_words(selection: &SelectionMask) -> std::borrow::Cow<'_, [u32]> {
    match selection.words() {
        Some(words) => std::borrow::Cow::Borrowed(words),
        None => std::borrow::Cow::Owned(mask::full_mask(selection.rows())),
    }
}

/// `WHERE` predicates as packed masks.
pub struct BatchFilter {
    filters: Vec<ColumnFilter>,
    width: usize,
}

impl BatchFilter {
    /// `Ok(None)` means the predicates are valid but use a shape these kernels do not run.
    pub fn from_predicates(
        predicates: &[Predicate],
        catalog: &[CatalogColumn],
    ) -> Result<Option<Self>, String> {
        reject_unsupported_predicates(predicates, catalog)?;
        Ok(filter::compile(predicates, catalog).map(|filters| Self {
            filters,
            width: catalog.len(),
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn selection(&mut self, batch: &WcolBatch<'_>) -> Result<SelectionMask, String> {
        let rows = batch.row_count;
        if self.filters.is_empty() {
            return Ok(SelectionMask::all(rows));
        }
        let _phase = phase(&PHASE_FILTER_NS);
        let cols = cols_of(batch, self.width)?;
        let mut mask = mask::full_mask(rows);
        filter::evaluate_conjunction(&mut self.filters, &cols, &mut mask)?;
        Ok(SelectionMask::from_words(rows, mask))
    }
}

mod acc;
#[cfg(test)]
mod conjunction_tests;
mod consume;
mod finish;
mod group_table;
mod groups;
mod kernels;
mod pairs;
mod spill;
mod spill_run;

use acc::*;
use group_table::*;
use kernels::*;

/// Filter-free aggregate engine over batches; see the module docs.
pub struct BatchQuery {
    shell: Partial,
    /// Count aggregate a final top-k orders by. Empty on a partial that another node will merge.
    final_rank: Option<u32>,
    /// This finish is the query's final aggregate: no later merge will see the partial.
    final_aggregate: bool,
    aggs: Vec<AggRef>,
    /// `k` in `SUM(col + k)` etc., per aggregate. Applied when stats are built:
    /// `Σ(v + k) = Σv + k·n`, and min / max shift by `k`.
    offsets: Vec<i128>,
    group_by: Vec<u32>,
    width: usize,
    metadata_only: bool,
    passed_rows: u64,
    /// Ungrouped accumulators and whether any row reached them.
    totals: Vec<Acc>,
    totals_f: Vec<FAcc>,
    /// Parallel to `aggs`: the aggregate column is FLOAT or DOUBLE.
    agg_float: Vec<bool>,
    totals_sketches: Vec<Option<Hll>>,
    totals_hit: bool,
    groups: GroupTable,
    /// Two integer group keys with integer accumulators. Absent for every other grouping.
    pairs: Option<PairGroups>,
    group_key_text: u32,
    group_key_float: u32,
    /// Global `COUNT(DISTINCT text)`: each value is stored as a 64-bit hash in the pair table.
    text_distinct: bool,
    interner: StringInterner,
    /// Per-batch scratch: selected row ids and their group slots, dictionary histogram.
    rows_scratch: Vec<u32>,
    slots_scratch: Vec<u32>,
    hist_scratch: Vec<u64>,
    spill: Option<group_spill::GroupSpill>,
    /// Resume index into the current selection after a host spill await.
    spill_consume_at: usize,
    /// Next sorted live slot to encode during an in-progress flush.
    spill_flush_at: usize,
    /// Slot order for the run being flushed. Indices only — not a copy of the table.
    spill_sort_scratch: Vec<u32>,
}

impl BatchQuery {
    /// Rejects aggregates the batch kernels cannot run.
    ///
    /// `predicates` are not applied here (the filter operator does that). They only decide
    /// whether a bare `COUNT(*)` may use the footer row count: a filter reads a column.
    pub fn from_ir(
        predicates: &[Predicate],
        group_by: &[u32],
        aggregates: &[AggExpr],
        limit: Option<u32>,
        offset: u32,
        order_groups_by_count: bool,
        catalog: &[CatalogColumn],
    ) -> Result<Self, String> {
        validate_aggregate(group_by, aggregates, catalog)?;
        // `COUNT(DISTINCT col)` runs as a two-level aggregate: `col` is a hidden last group
        // key, and `finalize_groups` folds it away after partials are merged. The partial
        // header stays ungrouped when that hidden key is the only one.
        let mut keys = group_by.to_vec();
        if let Some(column) = distinct_column(aggregates)? {
            keys.push(column);
        }
        if keys.len() > MAX_KEYS {
            return Err(format!(
                "GROUP BY plus COUNT(DISTINCT) uses more than {MAX_KEYS} key columns"
            ));
        }
        let mut group_key_text = 0u32;
        let mut group_key_float = 0u32;
        for (i, id) in keys.iter().enumerate() {
            if let Some(column) = catalog.get(*id as usize) {
                if column.kind == ColumnKind::Text {
                    group_key_text |= 1u32 << i;
                } else if matches!(column.physical, 4 | 5) {
                    group_key_float |= 1u32 << i;
                }
            }
        }
        // Only the count of distinct values is needed, and the hash key is folded away with the
        // rest of the distinct key. So a global text distinct stores 64-bit hashes, not strings.
        let text_distinct = group_by.is_empty()
            && !keys.is_empty()
            && aggregates
                .iter()
                .all(|agg| agg.kind == crate::ir::AggKind::CountDistinct)
            && catalog
                .get(keys[0] as usize)
                .is_some_and(|column| column.kind == ColumnKind::Text);
        if text_distinct {
            group_key_text = 0;
        }
        let partial = shell(group_by, aggregates, limit, offset, order_groups_by_count);
        let aggs = partial.aggs.clone();
        let agg_float: Vec<bool> = aggs
            .iter()
            .map(|agg| {
                catalog
                    .get(agg.column as usize)
                    .is_some_and(|column| matches!(column.physical, 4 | 5))
            })
            .collect();
        let metadata_only = predicates.is_empty()
            && group_by.is_empty()
            && aggregates.iter().all(|agg| agg.column.is_none());
        let pairs = PairGroups::for_keys(&keys, group_key_text, group_key_float, &agg_float, &aggs);
        Ok(Self {
            offsets: aggregates.iter().map(|a| i128::from(a.offset)).collect(),
            aggs: aggs.clone(),
            group_by: keys,
            group_key_text,
            text_distinct,
            group_key_float,
            agg_float: agg_float.clone(),
            interner: StringInterner::new(),
            width: catalog.len(),
            metadata_only,
            passed_rows: 0,
            totals: vec![Acc::EMPTY; aggs.len()],
            totals_f: std::iter::repeat_with(FAcc::empty)
                .take(aggs.len())
                .collect(),
            totals_sketches: vec![None; aggs.len()],
            totals_hit: false,
            groups: GroupTable::new(
                aggs.len(),
                aggs.iter().any(|agg| agg.kind == KIND_APPROX_DISTINCT),
            ),
            pairs,
            final_rank: None,
            final_aggregate: false,
            rows_scratch: Vec::new(),
            slots_scratch: Vec::new(),
            hist_scratch: Vec::new(),
            spill: None,
            spill_consume_at: 0,
            spill_flush_at: 0,
            spill_sort_scratch: Vec::new(),
            shell: partial,
        })
    }

    pub fn enable_spill(
        &mut self,
        config: crate::spill::SpillConfig,
        store: Box<dyn crate::spill::SpillStore>,
    ) -> Result<(), String> {
        self.spill = Some(group_spill::GroupSpill::open(config, store)?);
        publish_spill_stats(group_spill::FlushStats::default());
        Ok(())
    }

    /// Keep a heap of `offset + limit` groups ordered by this count aggregate.
    /// A partial that will be merged leaves this empty and emits every group.
    pub fn set_final_rank(&mut self, index: Option<u32>) {
        self.final_rank = index;
    }

    pub fn set_final_aggregate(&mut self, final_aggregate: bool) {
        self.final_aggregate = final_aggregate;
    }

    /// Groups resident in the hash table. This is the reducer's state size, not the rows scanned.
    pub fn resident_groups(&self) -> usize {
        self.pairs
            .as_ref()
            .map(PairGroups::len)
            .unwrap_or(self.groups.len())
    }

    /// Bytes of group-table state currently held, including hash-map capacity.
    pub fn resident_state_bytes(&self) -> u64 {
        let bytes = if let Some(pairs) = &self.pairs {
            pairs.resident_bytes() + self.interner.resident_bytes()
        } else {
            self.groups.resident_bytes() + self.interner.resident_bytes()
        };
        bytes as u64
    }

    pub(crate) fn spill_flush_stats(&self) -> Option<group_spill::FlushStats> {
        self.spill.as_ref().map(|spill| spill.stats())
    }

    pub(crate) fn publish_growth_gauges(&self) {
        let interner = self.interner.resident_bytes() as u64;
        let group = self.resident_state_bytes().saturating_sub(interner);
        let (runs, pending, merge) = self
            .spill
            .as_ref()
            .map(|spill| spill.growth_buffers())
            .unwrap_or((0, 0, 0));
        crate::exec::mem_growth::set_aggregate(group, interner, runs, pending, merge);
    }

    pub(crate) fn stamp_memory_phase(&self, phase: u8) {
        self.publish_growth_gauges();
        crate::exec::mem_growth::set_phase(phase);
        crate::exec::mem_growth::observe(false);
    }

    pub(crate) fn live_interner_bytes(&self) -> usize {
        self.interner.resident_bytes()
    }

    pub(crate) fn live_table_bytes(&self) -> usize {
        self.groups.resident_bytes()
    }

    pub(crate) fn note_groups(&self, groups_in: u64, groups_out: u64) {
        RESIDENT_GROUPS.with(|groups| groups.set(groups_in));
        RESIDENT_GROUP_BYTES.with(|bytes| bytes.set(self.resident_state_bytes()));
        if phase_on() {
            PHASE_GROUPS_IN.store(groups_in, Ordering::Relaxed);
            PHASE_GROUPS_OUT.store(groups_out, Ordering::Relaxed);
        }
    }

    pub fn metadata_only(&self) -> bool {
        self.metadata_only
    }

    pub fn passed_rows(&self) -> u64 {
        self.passed_rows
    }

    /// `COUNT(*)` with no filter and no group column. `rows` comes from the footer.
    /// [`Self::add_metadata`] for a query that reads no columns: its aggregates are all `COUNT(*)`.
    pub fn add_metadata_rows(&mut self, rows: u64) -> Result<(), String> {
        self.add_metadata(rows, &vec![None; self.aggs.len()])
    }

    /// Seed an ungrouped, unfiltered aggregate from footer metadata, as if it had read all
    /// `rows`: `COUNT(*)` counts them, and `MIN`/`MAX` take `bounds[i]`, the column's
    /// footer `(min, max)` over every chunk. The planner sends nothing else.
    pub fn add_metadata(&mut self, rows: u64, bounds: &[Option<(i64, i64)>]) -> Result<(), String> {
        if bounds.len() != self.aggs.len() || self.totals.len() != self.aggs.len() {
            return Err("metadata bounds do not match the aggregates".to_string());
        }
        for ((agg, bound), acc) in self.aggs.iter().zip(bounds).zip(&mut self.totals) {
            match (agg.kind, bound) {
                (KIND_COUNT_STAR, None) => acc.count += rows,
                (KIND_MIN | KIND_MAX, Some((min, max))) => {
                    // The bounds are of non-null values, so the result is not null.
                    acc.count += rows.max(1);
                    acc.min = acc.min.min(*min);
                    acc.max = acc.max.max(*max);
                }
                _ => return Err("metadata execution supports count(*), min and max".to_string()),
            }
        }
        self.passed_rows = self.passed_rows.saturating_add(rows);
        self.totals_hit = true;
        Ok(())
    }
}
