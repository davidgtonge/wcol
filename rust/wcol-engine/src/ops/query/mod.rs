//! WCOL query kernel: engine IR → batch aggregates and PWCL partial state.
//!
//! Production path: node-fragment plans + `LinearColocatedGraph` + the batch kernels.
//! SQL is planned outside this crate and arrives here as [`crate::ir`] values.

pub mod batch;
pub mod float_key;
pub mod hll;

use std::collections::BTreeMap;

use rustc_hash::FxHashMap;

pub use crate::ir::CatalogColumn;
use crate::ir::{AggExpr, AggKind, ColumnKind, Predicate};
pub use crate::stream::SelectionMask;

/// `LIKE` compares strings; on a numeric column it is an error when the filter is built.
pub fn reject_unsupported_predicates(
    predicates: &[Predicate],
    catalog: &[CatalogColumn],
) -> Result<(), String> {
    for predicate in predicates {
        if let Predicate::Like { column, .. } = predicate {
            if catalog.get(*column as usize).map(|c| c.kind) != Some(ColumnKind::Text) {
                return Err("LIKE needs a text column".to_string());
            }
        }
    }
    Ok(())
}

/// Public partial snapshot (row-oriented group state).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialAggSpec {
    pub kind: u8,
    pub column: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PartialKeyPart {
    Null,
    Int(i128),
    Float(u64),
    Bytes(Vec<u8>),
}

impl PartialOrd for PartialKeyPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartialKeyPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use PartialKeyPart::*;
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,
            (Int(a), Int(b)) => a.cmp(b),
            (Int(_), _) => Ordering::Less,
            (Float(_), Int(_)) => Ordering::Greater,
            (Float(a), Float(b)) => float_key::float_key(f64::from_bits(*a))
                .cmp(&float_key::float_key(f64::from_bits(*b))),
            (Float(_), Bytes(_)) => Ordering::Less,
            (Bytes(a), Bytes(b)) => a.cmp(b),
            (Bytes(_), _) => Ordering::Greater,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartialStat {
    pub count: u64,
    pub flags: u8,
    pub sum_i: i128,
    pub sum_f: f64,
    pub min_i: i128,
    pub max_i: i128,
    pub min_f: f64,
    pub max_f: f64,
    /// `approx_count_distinct` state; `None` for every other aggregate.
    pub sketch: Option<hll::Hll>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartialGroup {
    pub key: Vec<PartialKeyPart>,
    pub stats: Vec<PartialStat>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartialSnapshot {
    pub grouped: bool,
    pub order_groups_by_count: bool,
    pub limit: u32,
    pub offset: u32,
    pub passed_rows: u64,
    pub aggs: Vec<PartialAggSpec>,
    pub groups: Vec<PartialGroup>,
    /// Key arity when `groups` is empty. Populated snapshots still take arity from the first group.
    pub key_len: u8,
    pub key_float_mask: u32,
    pub key_string_mask: u32,
}

pub fn merge_partial_snapshots(
    left: &PartialSnapshot,
    right: &PartialSnapshot,
) -> Result<PartialSnapshot, String> {
    let mut merger = PartialSnapshotMerger::new(left.clone());
    merger.merge(right)?;
    Ok(merger.finish())
}

/// Folds many partial snapshots with a hashed group index (linear in total groups).
/// Groups keep first-seen order, matching a sequence of pairwise merges.
pub struct PartialSnapshotMerger {
    merged: PartialSnapshot,
    index: FxHashMap<Vec<PartialKeyPart>, usize>,
}

impl PartialSnapshotMerger {
    pub fn new(first: PartialSnapshot) -> Self {
        let index = first
            .groups
            .iter()
            .enumerate()
            .map(|(at, group)| (group.key.clone(), at))
            .collect();
        Self {
            merged: first,
            index,
        }
    }

    pub fn merge(&mut self, right: &PartialSnapshot) -> Result<(), String> {
        let left = &self.merged;
        if left.aggs.len() != right.aggs.len()
            || left.grouped != right.grouped
            || left.order_groups_by_count != right.order_groups_by_count
            || left.limit != right.limit
            || left.offset != right.offset
        {
            return Err("parquet partials are for different queries".to_string());
        }
        for (index, agg) in left.aggs.iter().enumerate() {
            if agg.kind != right.aggs[index].kind || agg.column != right.aggs[index].column {
                return Err("parquet partials are for different queries".to_string());
            }
        }
        self.merged.passed_rows += right.passed_rows;
        for group in &right.groups {
            if let Some(&at) = self.index.get(&group.key) {
                let stats = &mut self.merged.groups[at].stats;
                for (index, stat) in group.stats.iter().enumerate() {
                    merge_partial_stat(&mut stats[index], stat)?;
                }
            } else {
                self.index
                    .insert(group.key.clone(), self.merged.groups.len());
                self.merged.groups.push(group.clone());
            }
        }
        Ok(())
    }

    pub fn group_count(&self) -> usize {
        self.merged.groups.len()
    }

    pub fn finish(self) -> PartialSnapshot {
        self.merged
    }
}

/// Keep the `keep` groups with the highest `stats[agg].count`, then the smaller key.
/// This is the same order as a final count top-k. Groups already inside the window stay.
pub fn keep_top_count_groups(snap: &mut PartialSnapshot, agg: usize, keep: usize) {
    keep_top_counts(&mut snap.groups, agg, keep);
}

/// The `keep` groups with the largest `agg` count, ties by ascending key. A total order, so
/// trimming disjoint group sets separately and then together keeps the same groups.
fn keep_top_counts(groups: &mut Vec<PartialGroup>, agg: usize, keep: usize) {
    if keep == 0 || groups.len() <= keep {
        return;
    }
    groups.sort_by(|left, right| {
        let left_count = left.stats.get(agg).map(|stat| stat.count).unwrap_or(0);
        let right_count = right.stats.get(agg).map(|stat| stat.count).unwrap_or(0);
        right_count
            .cmp(&left_count)
            .then_with(|| left.key.cmp(&right.key))
    });
    groups.truncate(keep);
}

/// Top `keep` groups by `agg`'s count, from folded distinct counts of groups this input holds
/// completely (the exchange partitioned on the group alone).
///
/// A spilled reducer folds its merged runs chunk by chunk in key order, so one group's values
/// can span two consecutive chunks: the last group of a chunk stays open until a later key
/// arrives. Without this the coordinator merged every group of every reducer to rank ten.
pub struct FoldedTopCounts {
    agg: usize,
    keep: usize,
    shell: Option<PartialSnapshot>,
    open: Option<PartialGroup>,
    closed: Vec<PartialGroup>,
}

impl FoldedTopCounts {
    pub fn new(agg: usize, keep: usize) -> Self {
        Self {
            agg,
            keep,
            shell: None,
            open: None,
            closed: Vec::new(),
        }
    }

    pub fn push(&mut self, mut chunk: PartialSnapshot) -> Result<(), String> {
        let mut groups = std::mem::take(&mut chunk.groups);
        groups.sort_by(|left, right| crate::spill::cmp_spill_key_parts(&left.key, &right.key));
        match &mut self.shell {
            Some(shell) => shell.passed_rows += chunk.passed_rows,
            None => self.shell = Some(chunk),
        }
        for group in groups {
            let Some(mut open) = self.open.take() else {
                self.open = Some(group);
                continue;
            };
            match crate::spill::cmp_spill_key_parts(&group.key, &open.key) {
                std::cmp::Ordering::Equal => {
                    for (stat, more) in open.stats.iter_mut().zip(&group.stats) {
                        merge_partial_stat(stat, more)?;
                    }
                    self.open = Some(open);
                }
                std::cmp::Ordering::Greater => {
                    self.close(open);
                    self.open = Some(group);
                }
                std::cmp::Ordering::Less => {
                    return Err("distinct counts arrived out of key order".to_string());
                }
            }
        }
        Ok(())
    }

    fn close(&mut self, group: PartialGroup) {
        self.closed.push(group);
        // Trim in batches: memory stays near `keep` groups, sorting stays amortised.
        if self.closed.len() >= self.keep.saturating_mul(2).max(self.keep + 1024) {
            keep_top_counts(&mut self.closed, self.agg, self.keep);
        }
    }

    /// The ranked groups, or `None` when no chunk arrived.
    pub fn finish(mut self) -> Option<PartialSnapshot> {
        if let Some(open) = self.open.take() {
            self.close(open);
        }
        keep_top_counts(&mut self.closed, self.agg, self.keep);
        let mut shell = self.shell?;
        shell.groups = self.closed;
        Some(shell)
    }
}

/// Result groups for a merged partial, before ordering and `LIMIT`:
///
/// - With `COUNT(DISTINCT col)` the partial is keyed by (groups.., col). The last key part is
///   folded away: the distinct count is the number of non-null `col` values, and every other
///   aggregate merges across them.
/// - An ungrouped aggregate with no matching rows still yields one row (counts 0, other
///   aggregates empty), as SQL requires.
pub fn finalize_groups(snap: &PartialSnapshot) -> Result<Vec<PartialGroup>, String> {
    let distinct = snap.aggs.iter().any(|agg| agg.kind == KIND_COUNT_DISTINCT);
    let mut groups =
        if distinct && !snap.grouped && snap.aggs.iter().all(|agg| agg.kind == KIND_COUNT_DISTINCT)
        {
            // Already one cardinality, or a merged set of values. Count the set; do not sort it.
            let count = if snap.groups.len() == 1 && snap.groups[0].key.is_empty() {
                snap.groups[0]
                    .stats
                    .first()
                    .map(|stat| stat.count)
                    .unwrap_or(0)
            } else {
                snap.groups
                    .iter()
                    .filter(|group| group.key.last() != Some(&PartialKeyPart::Null))
                    .count() as u64
            };
            vec![PartialGroup {
                key: Vec::new(),
                stats: snap
                    .aggs
                    .iter()
                    .map(|_| stat_to_partial(&count_stat(count)))
                    .collect(),
            }]
        } else if distinct {
            let mut folded: BTreeMap<Vec<PartialKeyPart>, (Vec<PartialStat>, Vec<u64>)> =
                BTreeMap::new();
            for group in &snap.groups {
                let (last, prefix) = group
                    .key
                    .split_last()
                    .ok_or("COUNT(DISTINCT) partial has no distinct key")?;
                let (stats, distinct_counts) = folded.entry(prefix.to_vec()).or_insert_with(|| {
                    (
                        vec![stat_to_partial(&empty_stat(false)); snap.aggs.len()],
                        vec![0; snap.aggs.len()],
                    )
                });
                for (index, (agg, stat)) in snap.aggs.iter().zip(&group.stats).enumerate() {
                    if agg.kind == KIND_COUNT_DISTINCT {
                        if *last != PartialKeyPart::Null && stat.count > 0 {
                            distinct_counts[index] += 1;
                        }
                    } else {
                        merge_partial_stat(&mut stats[index], stat)?;
                    }
                }
            }
            folded
                .into_iter()
                .map(|(key, (mut stats, distinct_counts))| {
                    for (index, agg) in snap.aggs.iter().enumerate() {
                        if agg.kind == KIND_COUNT_DISTINCT {
                            stats[index] = stat_to_partial(&count_stat(distinct_counts[index]));
                        }
                    }
                    PartialGroup { key, stats }
                })
                .collect()
        } else {
            snap.groups.clone()
        };
    // A merged count stat keeps only a meaningful `count`; its min / max depend on how rows
    // were split across partials, so report the canonical form. An approx distinct becomes a
    // count of its sketch's estimate.
    for group in &mut groups {
        for (agg, stat) in snap.aggs.iter().zip(&mut group.stats) {
            if agg.kind == KIND_APPROX_DISTINCT {
                let estimate = stat.sketch.as_ref().map_or(0.0, |s| s.estimate()).round() as u64;
                *stat = stat_to_partial(&count_stat(estimate));
            } else if matches!(agg.kind, KIND_COUNT_STAR | KIND_COUNT) && stat.count > 0 {
                *stat = stat_to_partial(&count_stat(stat.count));
            }
        }
    }
    if !snap.grouped && groups.is_empty() {
        groups.push(PartialGroup {
            key: Vec::new(),
            stats: snap
                .aggs
                .iter()
                .map(|agg| {
                    let stat = if matches!(
                        agg.kind,
                        KIND_COUNT_STAR | KIND_COUNT | KIND_COUNT_DISTINCT | KIND_APPROX_DISTINCT
                    ) {
                        count_stat(0)
                    } else {
                        empty_stat(false)
                    };
                    stat_to_partial(&stat)
                })
                .collect(),
        });
    }
    Ok(groups)
}

/// A hash reducer's partial with each `COUNT(DISTINCT)` replaced by its count. The reducer owns
/// every (group, value) pair of the groups in its partition, so the count is final there. The
/// finalizer then sums counts across reducers, and never holds the value sets.
pub fn distinct_counts_snapshot(snap: &PartialSnapshot) -> Result<PartialSnapshot, String> {
    if !snap.aggs.iter().any(|agg| agg.kind == KIND_COUNT_DISTINCT) {
        return Ok(snap.clone());
    }
    let groups = finalize_groups(snap)?;
    // The distinct value is the hidden last key of a grouped partial; it is folded away.
    let key_len = if snap.grouped {
        snap.key_len.saturating_sub(1)
    } else {
        0
    };
    let keep = if key_len >= 32 {
        u32::MAX
    } else {
        (1u32 << key_len) - 1
    };
    Ok(PartialSnapshot {
        aggs: snap
            .aggs
            .iter()
            .map(|agg| PartialAggSpec {
                kind: if agg.kind == KIND_COUNT_DISTINCT {
                    KIND_COUNT
                } else {
                    agg.kind
                },
                column: agg.column,
            })
            .collect(),
        groups,
        key_len,
        key_float_mask: snap.key_float_mask & keep,
        key_string_mask: snap.key_string_mask & keep,
        grouped: snap.grouped,
        order_groups_by_count: snap.order_groups_by_count,
        limit: u32::MAX,
        offset: 0,
        passed_rows: snap.passed_rows,
    })
}

fn merge_partial_stat(left: &mut PartialStat, right: &PartialStat) -> Result<(), String> {
    let mut l = partial_to_stat(left);
    let r = partial_to_stat(right);
    merge_stat(&mut l, &r)?;
    *left = stat_to_partial(&l);
    Ok(())
}

pub(crate) fn merge_partial_groups(
    left: &mut PartialGroup,
    right: &PartialGroup,
) -> Result<(), String> {
    if left.stats.len() != right.stats.len() {
        return Err("partial group agg mismatch".to_string());
    }
    for (index, stat) in right.stats.iter().enumerate() {
        merge_partial_stat(&mut left.stats[index], stat)?;
    }
    Ok(())
}

fn stat_to_partial(stat: &Stat) -> PartialStat {
    PartialStat {
        count: stat.count,
        flags: stat.flags,
        sum_i: stat.sum_i,
        sum_f: stat.sum_f,
        min_i: stat.min_i,
        max_i: stat.max_i,
        min_f: stat.min_f,
        max_f: stat.max_f,
        sketch: stat.sketch.clone(),
    }
}

fn partial_to_stat(stat: &PartialStat) -> Stat {
    Stat {
        count: stat.count,
        flags: stat.flags,
        sum_i: stat.sum_i,
        sum_f: stat.sum_f,
        min_i: stat.min_i,
        max_i: stat.max_i,
        min_f: stat.min_f,
        max_f: stat.max_f,
        sketch: stat.sketch.clone(),
    }
}

/// The column of the plan's `COUNT(DISTINCT ..)` aggregates. All of them must share one
/// column: it becomes a hidden last group key (see [`finalize_groups`]).
pub(crate) fn distinct_column(aggregates: &[AggExpr]) -> Result<Option<u32>, String> {
    let mut column = None;
    for aggregate in aggregates {
        if aggregate.kind != AggKind::CountDistinct {
            continue;
        }
        let this = aggregate.column.ok_or("COUNT(DISTINCT) needs a column")?;
        if column.is_some_and(|c| c != this) {
            return Err("COUNT(DISTINCT) on more than one column is not supported".to_string());
        }
        column = Some(this);
    }
    Ok(column)
}

/// Reject an aggregate the batch kernels cannot run. Row projection is a planner concern.
pub fn validate_aggregate(
    group_by: &[u32],
    aggregates: &[AggExpr],
    catalog: &[CatalogColumn],
) -> Result<(), String> {
    if aggregates.is_empty() {
        return Err("query has no aggregates".to_string());
    }
    distinct_column(aggregates)?;
    for aggregate in aggregates {
        if let Some(column) = aggregate.column {
            // Counting needs only validity, so it works on any column; the rest need numbers.
            if !matches!(aggregate.kind, AggKind::Count | AggKind::CountDistinct) {
                require_number(catalog, column, agg_name(aggregate.kind))?;
            }
        }
    }
    for column in group_by {
        require_groupable(catalog, *column, "group by")?;
    }
    Ok(())
}

fn agg_name(kind: AggKind) -> &'static str {
    match kind {
        AggKind::CountStar | AggKind::Count => "COUNT",
        AggKind::Sum => "SUM",
        AggKind::Avg => "AVG",
        AggKind::Min => "MIN",
        AggKind::Max => "MAX",
        AggKind::ApproxDistinct => "approx_count_distinct",
        AggKind::CountDistinct => "COUNT(DISTINCT)",
    }
}

fn require_groupable(catalog: &[CatalogColumn], column: u32, what: &str) -> Result<(), String> {
    catalog
        .get(column as usize)
        .map(|column| column.kind)
        .ok_or_else(|| format!("{what} column is missing"))?;
    Ok(())
}

fn require_number(catalog: &[CatalogColumn], column: u32, what: &str) -> Result<(), String> {
    let kind = catalog
        .get(column as usize)
        .map(|column| column.kind)
        .ok_or_else(|| format!("{what} column is missing"))?;
    if kind != ColumnKind::Number {
        return Err(format!("{what} on text is not supported"));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct AggRef {
    kind: u8,
    column: u32,
}

#[derive(Clone)]
struct Stat {
    count: u64,
    sum_i: i128,
    sum_f: f64,
    min_i: i128,
    max_i: i128,
    min_f: f64,
    max_f: f64,
    flags: u8,
    sketch: Option<hll::Hll>,
}

struct Partial {
    grouped: bool,
    order_groups_by_count: bool,
    limit: u32,
    offset: u32,
    aggs: Vec<AggRef>,
}

fn empty_stat(integer: bool) -> Stat {
    Stat {
        count: 0,
        sum_i: 0,
        sum_f: 0.0,
        min_i: 0,
        max_i: 0,
        min_f: f64::NAN,
        max_f: f64::NAN,
        flags: if integer { 1 | 4 } else { 0 },
        sketch: None,
    }
}

const KIND_COUNT_STAR: u8 = 0;
const KIND_COUNT: u8 = 1;
const KIND_SUM: u8 = 2;
const KIND_AVG: u8 = 3;
const KIND_MIN: u8 = 4;
const KIND_MAX: u8 = 5;
/// `approx_count_distinct(col)`: count of non-null rows plus an HLL sketch of their values.
pub const KIND_APPROX_DISTINCT: u8 = 6;
/// `COUNT(DISTINCT col)`: in a partial, a count of non-null rows per (groups.., col) key.
pub const KIND_COUNT_DISTINCT: u8 = 7;

fn kind_tag(kind: AggKind) -> u8 {
    match kind {
        AggKind::CountStar => KIND_COUNT_STAR,
        AggKind::Count => KIND_COUNT,
        AggKind::Sum => KIND_SUM,
        AggKind::Avg => KIND_AVG,
        AggKind::Min => KIND_MIN,
        AggKind::Max => KIND_MAX,
        AggKind::ApproxDistinct => KIND_APPROX_DISTINCT,
        AggKind::CountDistinct => KIND_COUNT_DISTINCT,
    }
}

fn shell(
    group_by: &[u32],
    aggregates: &[AggExpr],
    limit: Option<u32>,
    offset: u32,
    order_groups_by_count: bool,
) -> Partial {
    Partial {
        grouped: !group_by.is_empty(),
        order_groups_by_count,
        limit: limit.unwrap_or(u32::MAX),
        offset,
        aggs: aggregates
            .iter()
            .map(|aggregate| AggRef {
                kind: kind_tag(aggregate.kind),
                column: aggregate.column.unwrap_or(u32::MAX),
            })
            .collect(),
    }
}

fn count_stat(count: u64) -> Stat {
    let value = i128::from(count);
    Stat {
        count,
        sum_i: value,
        sum_f: count as f64,
        min_i: value,
        max_i: value,
        min_f: count as f64,
        max_f: count as f64,
        flags: 1 | 4,
        sketch: None,
    }
}

pub(crate) fn exact_f64(value: i128) -> f64 {
    // Every integer within ±2^53 is exact in f64; convert those through i64, which is a
    // hardware instruction (i128 <-> f64 goes through software routines).
    const EXACT: i128 = 1 << 53;
    if (-EXACT..=EXACT).contains(&value) {
        return value as i64 as f64;
    }
    let out = value as f64;
    if out as i128 == value {
        out
    } else {
        f64::NAN
    }
}

fn merge_stat(left: &mut Stat, right: &Stat) -> Result<(), String> {
    if right.count == 0 {
        return Ok(());
    }
    if left.count == 0 {
        *left = right.clone();
        return Ok(());
    }
    if let Some(sketch) = &right.sketch {
        left.sketch.get_or_insert_with(hll::Hll::new).merge(sketch);
    }
    if (left.flags & 1) != (right.flags & 1) {
        return Err("cannot merge numeric and byte aggregates".to_string());
    }
    left.count += right.count;
    if left.flags & 1 != 0 {
        left.sum_i = left
            .sum_i
            .checked_add(right.sum_i)
            .ok_or("integer sum overflow")?;
        left.sum_f = exact_f64(left.sum_i);
        left.min_i = left.min_i.min(right.min_i);
        left.max_i = left.max_i.max(right.max_i);
        left.min_f = exact_f64(left.min_i);
        left.max_f = exact_f64(left.max_i);
    } else {
        left.sum_f += right.sum_f;
        left.min_f = float_key::key_to_f64(
            float_key::float_key(left.min_f).min(float_key::float_key(right.min_f)),
        );
        left.max_f = float_key::key_to_f64(
            float_key::float_key(left.max_f).max(float_key::float_key(right.max_f)),
        );
    }
    Ok(())
}

/// Compare two PWCL partials (group order may differ).
pub fn encoded_partials_equivalent(left: &[u8], right: &[u8]) -> Result<(), String> {
    let left = crate::stream::partials::partial_snapshot_from_wire(left)?;
    let right = crate::stream::partials::partial_snapshot_from_wire(right)?;
    if left.aggs != right.aggs
        || left.grouped != right.grouped
        || left.order_groups_by_count != right.order_groups_by_count
        || left.limit != right.limit
        || left.offset != right.offset
    {
        return Err("partial headers differ".to_string());
    }
    if left.groups.len() != right.groups.len() {
        return Err(format!(
            "group count {} vs {}",
            left.groups.len(),
            right.groups.len()
        ));
    }
    if !left.grouped && left.passed_rows != right.passed_rows {
        return Err(format!(
            "passed_rows {} vs {}",
            left.passed_rows, right.passed_rows
        ));
    }
    let right_groups: std::collections::HashMap<&Vec<PartialKeyPart>, &Vec<PartialStat>> = right
        .groups
        .iter()
        .map(|group| (&group.key, &group.stats))
        .collect();
    for group in &left.groups {
        let rstats = right_groups
            .get(&group.key)
            .ok_or_else(|| "group key missing on right".to_string())?;
        if group.stats.len() != rstats.len() {
            return Err("aggregate stat count differs".to_string());
        }
        for (left_stat, right_stat) in group.stats.iter().zip(rstats.iter()) {
            stat_equivalent(left_stat, right_stat)?;
        }
    }
    Ok(())
}

fn stat_equivalent(left: &PartialStat, right: &PartialStat) -> Result<(), String> {
    if left.count != right.count || left.flags != right.flags {
        return Err(format!(
            "stat count/flags {} vs {}",
            left.count, right.count
        ));
    }
    if left.sum_i != right.sum_i {
        return Err(format!("sum_i {} vs {}", left.sum_i, right.sum_i));
    }
    if (left.sum_f - right.sum_f).abs() > 1e-6 * (1.0 + left.sum_f.abs()) {
        return Err(format!("sum_f {} vs {}", left.sum_f, right.sum_f));
    }
    Ok(())
}

/// Rows that passed the filter, from a PWCL partial.
pub fn partial_passed_rows(bytes: &[u8]) -> Result<u64, String> {
    Ok(crate::stream::partial_wcs::partial_state_from_wire_payload(bytes)?.passed_rows)
}

pub fn partial_first_sum_i(bytes: &[u8]) -> Result<i128, String> {
    crate::stream::partial_wcs::partial_first_sum_from_wire(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::parquet::open_plan;

    fn counted(key: &[u8], count: u64) -> PartialGroup {
        PartialGroup {
            key: vec![PartialKeyPart::Bytes(key.to_vec())],
            stats: vec![stat_to_partial(&count_stat(count))],
        }
    }

    fn chunk(groups: Vec<PartialGroup>) -> PartialSnapshot {
        PartialSnapshot {
            grouped: true,
            order_groups_by_count: false,
            limit: 2,
            offset: 0,
            passed_rows: 0,
            aggs: vec![PartialAggSpec {
                kind: KIND_COUNT,
                column: 1,
            }],
            groups,
            key_len: 1,
            key_float_mask: 0,
            key_string_mask: 1,
        }
    }

    fn ranked(top: FoldedTopCounts) -> Vec<(Vec<u8>, u64)> {
        top.finish()
            .unwrap()
            .groups
            .into_iter()
            .map(|group| match &group.key[0] {
                PartialKeyPart::Bytes(key) => (key.clone(), group.stats[0].count),
                _ => unreachable!(),
            })
            .collect()
    }

    #[test]
    fn folded_top_counts_joins_a_group_split_across_chunks() {
        // "b" has 3 values in chunk one and 4 in chunk two: 7 in all, the largest.
        let mut top = FoldedTopCounts::new(0, 2);
        top.push(chunk(vec![counted(b"a", 5), counted(b"b", 3)]))
            .unwrap();
        top.push(chunk(vec![
            counted(b"b", 4),
            counted(b"c", 1),
            counted(b"d", 5),
        ]))
        .unwrap();
        assert_eq!(ranked(top), vec![(b"b".to_vec(), 7), (b"a".to_vec(), 5)]);
    }

    #[test]
    fn folded_top_counts_rejects_keys_out_of_order() {
        let mut top = FoldedTopCounts::new(0, 2);
        top.push(chunk(vec![counted(b"m", 1)])).unwrap();
        assert!(top.push(chunk(vec![counted(b"a", 1)])).is_err());
    }

    #[test]
    fn folded_top_counts_bounds_memory_while_ranking() {
        let mut top = FoldedTopCounts::new(0, 3);
        for start in (0..10_000u32).step_by(500) {
            let groups = (start..start + 500)
                .map(|i| counted(format!("{i:05}").as_bytes(), u64::from(i % 997)))
                .collect();
            top.push(chunk(groups)).unwrap();
            assert!(top.closed.len() < 3 + 1024 + 500);
        }
        let counts: Vec<u64> = ranked(top).into_iter().map(|(_, count)| count).collect();
        assert_eq!(counts, vec![996, 996, 996]);
    }
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    #[test]
    fn plans_and_filters_the_flat_fixture() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
        if !path.exists() {
            return;
        }
        let (mut file, plan) = open_plan(&path).unwrap();
        let catalog: Vec<CatalogColumn> = plan
            .leaves
            .iter()
            .map(|leaf| CatalogColumn {
                name: leaf.name.clone(),
                kind: if leaf.physical == 6 || leaf.physical == 7 {
                    ColumnKind::Text
                } else {
                    ColumnKind::Number
                },
                physical: leaf.physical,
                type_length: leaf.type_length,
                max_def: leaf.max_def,
                max_rep: leaf.max_rep,
            })
            .collect();
        let column = catalog
            .iter()
            .position(|c| c.name == "a")
            .expect("fixture column a") as u32;
        let leaf = plan.leaves[column as usize].clone();
        let mut count = 0u64;
        let mut sum = 0i128;
        for chunk in plan.chunks.iter().filter(|chunk| chunk.leaf == column) {
            let mut bytes = vec![0u8; chunk.compressed_size as usize];
            file.seek(SeekFrom::Start(chunk.start)).unwrap();
            file.read_exact(&mut bytes).unwrap();
            let limits = crate::formats::parquet::ScanLimits::for_batch_rows(64);
            let mut scan = crate::formats::parquet::NumericScan::begin_with_limits(
                leaf.clone(),
                chunk.codec,
                limits,
                chunk.compressed_size as u32,
                Some(chunk.rows as u64),
            )
            .unwrap();
            scan.push_compressed(&bytes).unwrap();
            while let Some((values, nulls)) = scan.next_owned(64).unwrap() {
                for (value, present) in values.iter().zip(nulls.iter()) {
                    if *present != 0 && *value > 5 {
                        count += 1;
                        sum += i128::from(*value);
                    }
                }
            }
        }
        assert_eq!(count, 2);
        assert_eq!(sum, 17);
    }
}
