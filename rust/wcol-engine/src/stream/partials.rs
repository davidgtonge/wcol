//! Versioned aggregate **partial state** as typed WCOL columns (logical stream Phase 2).
//!
//! Column ids are synthetic (`PARTIAL_*`); they are not Parquet leaf ids.
//! Merge/finalize semantics match internal `Partial` / `Stat` (AVG via sum+count).

use crate::ops::query::hll::Hll;
use crate::ops::query::{
    PartialAggSpec, PartialGroup, PartialKeyPart, PartialSnapshot, PartialSnapshotMerger,
    PartialStat, KIND_APPROX_DISTINCT,
};
use crate::stream::partial_wcs::{partial_state_from_wire_payload, partial_state_to_wcs_payload};
use crate::stream::OwnedWcolBatch;

pub const PARTIAL_SCHEMA_VERSION: u8 = 1;

pub const PARTIAL_KEY_COL_BASE: u32 = 10_000;
pub const PARTIAL_STAT_COL_BASE: u32 = 20_000;
pub const STAT_FIELDS_PER_AGG: u32 = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatField {
    Count = 0,
    Flags = 1,
    SumILo = 2,
    SumIHi = 3,
    MinILo = 4,
    MinIHi = 5,
    MaxILo = 6,
    MaxIHi = 7,
    SumFBits = 8,
    MinFBits = 9,
    MaxFBits = 10,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialStateSchemaV1 {
    pub version: u8,
    pub grouped: bool,
    pub order_groups_by_count: bool,
    pub limit: u32,
    pub offset: u32,
    pub group_key_len: u8,
    /// Bit `i` set → group key column `i` stores `f64` bits (not signed int).
    pub group_key_float_mask: u32,
    /// Bit `i` set → group key column `i` is length-prefixed bytes (not int/float lanes).
    pub group_key_string_mask: u32,
    pub aggs: Vec<PartialAggSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialColumn {
    pub id: u32,
    pub values: Vec<i64>,
    pub nulls: Vec<u8>,
}

/// Per-row `approx_count_distinct` sketches for one aggregate: row `r` is
/// `bytes[offsets[r]..offsets[r + 1]]` in [`Hll`](crate::ops::query::hll::Hll) encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialSketchColumn {
    pub agg: u32,
    pub offsets: Vec<u32>,
    pub bytes: Vec<u8>,
}

impl PartialSketchColumn {
    pub fn row(&self, row: usize) -> Result<&[u8], String> {
        let start = *self.offsets.get(row).ok_or("sketch row missing")? as usize;
        let end = *self.offsets.get(row + 1).ok_or("sketch row missing")? as usize;
        self.bytes
            .get(start..end)
            .ok_or_else(|| "sketch bytes out of range".to_string())
    }
}

/// One string group-key column (parallel to int key columns in PWCL v4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialStringKeyColumn {
    pub key_index: u8,
    pub offsets: Vec<u32>,
    pub bytes: Vec<u8>,
    pub nulls: Vec<u8>,
}

/// One partial-state batch on the logical WCOL stream (many rows = many group keys).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialStateWcol {
    pub schema: PartialStateSchemaV1,
    pub passed_rows: u64,
    pub row_count: usize,
    pub columns: Vec<PartialColumn>,
    /// One per `approx_count_distinct` aggregate, in aggregate order.
    pub sketches: Vec<PartialSketchColumn>,
    pub string_keys: Vec<PartialStringKeyColumn>,
}

/// Build typed partial-state WCOL directly from a query kernel snapshot.
pub fn partial_state_from_snapshot(snap: &PartialSnapshot) -> Result<PartialStateWcol, String> {
    partial_snapshot_to_wcol(snap)
}

/// Snapshot of a typed partial.
pub fn partial_snapshot_from_state(state: &PartialStateWcol) -> Result<PartialSnapshot, String> {
    partial_wcol_to_snapshot(state)
}

pub fn partial_state_merge(
    left: &PartialStateWcol,
    right: &PartialStateWcol,
) -> Result<PartialStateWcol, String> {
    let mut merger = PartialStateMerger::new(left.clone());
    merger.merge(right)?;
    merger.finish()
}

/// Append disjoint group rows from `right` onto `left` (same schema, no key merge).
pub fn partial_state_concat(
    mut left: PartialStateWcol,
    right: PartialStateWcol,
) -> Result<PartialStateWcol, String> {
    ensure_same_schema(&left.schema, &right.schema)?;
    if right.row_count == 0 {
        return Ok(left);
    }
    if left.row_count == 0 {
        return Ok(right);
    }
    let rows = left.row_count as usize;
    let add = right.row_count as usize;
    for col in &mut left.columns {
        let extra = find_column(&right, col.id)?;
        col.values.extend_from_slice(&extra.values);
        col.nulls.extend_from_slice(&extra.nulls);
    }
    for sketch in &mut left.sketches {
        let extra = right
            .sketches
            .iter()
            .find(|s| s.agg == sketch.agg)
            .ok_or("concat missing sketch column")?;
        let base = sketch.bytes.len();
        sketch.bytes.extend_from_slice(&extra.bytes);
        for i in 1..extra.offsets.len() {
            sketch
                .offsets
                .push(base as u32 + extra.offsets[i] - extra.offsets[0]);
        }
    }
    for extra in right.string_keys {
        if let Some(col) = left
            .string_keys
            .iter_mut()
            .find(|col| col.key_index == extra.key_index)
        {
            let base = col.bytes.len() as u32;
            let origin = extra.offsets.first().copied().unwrap_or(0);
            for offset in extra.offsets.iter().skip(1) {
                col.offsets.push(base + offset.saturating_sub(origin));
            }
            col.bytes.extend_from_slice(&extra.bytes);
            col.nulls.extend_from_slice(&extra.nulls);
        } else {
            left.string_keys.push(extra);
        }
    }
    left.row_count = rows + add;
    left.passed_rows = left.passed_rows.max(right.passed_rows);
    Ok(left)
}

/// Rows `[start, end)` as their own partial. `passed_rows` stays on the first slice so a
/// later merge does not add the same count twice.
pub fn partial_state_row_slice(
    state: &PartialStateWcol,
    start: usize,
    end: usize,
) -> Result<PartialStateWcol, String> {
    if start > end || end > state.row_count {
        return Err("partial row range out of bounds".into());
    }
    let rows: Vec<u32> = (start as u32..end as u32).collect();
    let passed_rows = if start == 0 { state.passed_rows } else { 0 };
    partial_state_rows(state, &rows, passed_rows)
}

/// The given rows, in that order, as their own partial carrying `passed_rows`.
fn partial_state_rows(
    state: &PartialStateWcol,
    rows: &[u32],
    passed_rows: u64,
) -> Result<PartialStateWcol, String> {
    let n = state.row_count;
    if rows.iter().any(|&row| row as usize >= n) {
        return Err("partial row out of bounds".into());
    }
    let gather = |values: &[i64]| rows.iter().map(|&row| values[row as usize]).collect();
    let columns = state
        .columns
        .iter()
        .map(|col| {
            if col.values.len() < n || col.nulls.len() < n {
                return Err("partial column shorter than row count".into());
            }
            Ok(PartialColumn {
                id: col.id,
                values: gather(&col.values),
                nulls: rows.iter().map(|&row| col.nulls[row as usize]).collect(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sketches = state
        .sketches
        .iter()
        .map(|sketch| {
            let (offsets, bytes) = gather_spans(&sketch.offsets, &sketch.bytes, rows, n)?;
            Ok(PartialSketchColumn {
                agg: sketch.agg,
                offsets,
                bytes,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let string_keys = state
        .string_keys
        .iter()
        .map(|column| {
            if column.nulls.len() < n {
                return Err("partial string key shorter than row count".to_string());
            }
            let (offsets, bytes) = gather_spans(&column.offsets, &column.bytes, rows, n)?;
            Ok(PartialStringKeyColumn {
                key_index: column.key_index,
                offsets,
                bytes,
                nulls: rows.iter().map(|&row| column.nulls[row as usize]).collect(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(PartialStateWcol {
        schema: state.schema.clone(),
        passed_rows,
        row_count: rows.len(),
        columns,
        sketches,
        string_keys,
    })
}

/// Byte spans `offsets[row]..offsets[row + 1]` of `rows`, packed with fresh offsets.
fn gather_spans(
    offsets: &[u32],
    bytes: &[u8],
    rows: &[u32],
    n: usize,
) -> Result<(Vec<u32>, Vec<u8>), String> {
    if offsets.len() < n + 1 {
        return Err("partial span column shorter than row count".into());
    }
    let mut out_offsets = Vec::with_capacity(rows.len() + 1);
    let mut out = Vec::new();
    out_offsets.push(0);
    for &row in rows {
        let (start, end) = (
            offsets[row as usize] as usize,
            offsets[row as usize + 1] as usize,
        );
        out.extend_from_slice(bytes.get(start..end).ok_or("partial span out of range")?);
        out_offsets.push(u32::try_from(out.len()).map_err(|_| "partial spans exceed 4 GiB")?);
    }
    Ok((out_offsets, out))
}

/// Independently decodable PWCL payloads, each at most `max_payload` bytes.
/// A single group that does not fit is an error. The reducer merges the pieces.
pub fn partial_state_payloads(
    state: &PartialStateWcol,
    max_payload: usize,
) -> Result<Vec<Vec<u8>>, String> {
    let encoded = partial_state_to_wcs_payload(state)?;
    if encoded.len() <= max_payload {
        return Ok(vec![encoded]);
    }
    if state.row_count <= 1 {
        return Err("WCS frame exceeds maximum size".into());
    }
    let mid = state.row_count / 2;
    let left = partial_state_row_slice(state, 0, mid)?;
    let right = partial_state_row_slice(state, mid, state.row_count)?;
    let mut frames = partial_state_payloads(&left, max_payload)?;
    frames.extend(partial_state_payloads(&right, max_payload)?);
    Ok(frames)
}

/// Reduce many partial states: each input is converted to a snapshot once and folded into one
/// hashed accumulator, so reducing N workers is linear in total groups (not N × groups²).
/// A single input is returned unchanged.
pub struct PartialStateMerger {
    first: Option<PartialStateWcol>,
    schema: PartialStateSchemaV1,
    snapshots: Option<PartialSnapshotMerger>,
}

impl PartialStateMerger {
    pub fn new(first: PartialStateWcol) -> Self {
        Self {
            schema: first.schema.clone(),
            first: Some(first),
            snapshots: None,
        }
    }

    pub fn merge(&mut self, next: &PartialStateWcol) -> Result<(), String> {
        ensure_same_schema(&self.schema, &next.schema)?;
        if let Some(first) = self.first.take() {
            self.snapshots = Some(PartialSnapshotMerger::new(partial_wcol_to_snapshot(
                &first,
            )?));
        }
        let snapshots = self
            .snapshots
            .as_mut()
            .expect("merger holds first input or snapshots");
        snapshots.merge(&partial_wcol_to_snapshot(next)?)
    }

    pub fn finish(self) -> Result<PartialStateWcol, String> {
        match (self.first, self.snapshots) {
            (Some(first), _) => Ok(first),
            (None, Some(snapshots)) => partial_snapshot_to_wcol(&snapshots.finish()),
            (None, None) => unreachable!("merger holds first input or snapshots"),
        }
    }
}

/// Partial aggregate state as a row batch plus typed metadata (logical stream carrier).
pub fn partial_state_to_batch(state: PartialStateWcol) -> OwnedWcolBatch {
    let PartialStateWcol {
        schema,
        passed_rows,
        row_count,
        columns,
        sketches,
        string_keys,
    } = state;
    let mut batch = OwnedWcolBatch::empty();
    for col in columns {
        batch.push_i64_column(col.id, col.values, col.nulls);
    }
    batch
        .set_row_count(row_count)
        .expect("partial state columns aligned");
    batch.set_partial_meta(schema, passed_rows);
    batch.set_partial_sketches(sketches);
    batch.set_partial_string_keys(string_keys);
    batch
}

/// Decode PWCL wire bytes to a kernel snapshot (for JSON / host tools).
pub fn partial_snapshot_from_wire(bytes: &[u8]) -> Result<PartialSnapshot, String> {
    partial_wcol_to_snapshot(&partial_state_from_wire_payload(bytes)?)
}

pub fn partial_snapshot_from_batch(batch: &OwnedWcolBatch) -> Result<PartialSnapshot, String> {
    partial_wcol_to_snapshot(&partial_state_from_batch(batch)?)
}

/// Like [`partial_state_from_batch`] but moves the columns out instead of copying them.
pub fn partial_state_from_owned_batch(batch: OwnedWcolBatch) -> Result<PartialStateWcol, String> {
    let (schema, passed_rows) = batch
        .partial_meta()
        .map(|(schema, passed)| (schema.clone(), passed))
        .ok_or_else(|| "batch is row data, not partial state".to_string())?;
    let row_count = batch.row_count();
    let string_keys = batch.partial_string_keys().to_vec();
    let (columns, sketches) = batch.into_partial_columns()?;
    let columns = columns
        .into_iter()
        .map(|(id, values, nulls)| PartialColumn { id, values, nulls })
        .collect();
    Ok(PartialStateWcol {
        schema,
        passed_rows,
        row_count,
        columns,
        sketches,
        string_keys,
    })
}

pub fn partial_state_from_batch(batch: &OwnedWcolBatch) -> Result<PartialStateWcol, String> {
    let (schema, passed_rows) = batch
        .partial_meta()
        .ok_or_else(|| "batch is row data, not partial state".to_string())?;
    let row_count = batch.row_count();
    let columns = batch
        .export_i64_columns()?
        .into_iter()
        .map(|(id, values, nulls)| PartialColumn { id, values, nulls })
        .collect();
    Ok(PartialStateWcol {
        schema: schema.clone(),
        passed_rows,
        row_count,
        columns,
        sketches: batch.partial_sketches().to_vec(),
        string_keys: batch.partial_string_keys().to_vec(),
    })
}

pub fn partial_state_first_sum_i(
    state: &PartialStateWcol,
    agg_index: usize,
) -> Result<i128, String> {
    if state.row_count == 0 {
        return Err("partial state has no rows".to_string());
    }
    let lo = column_i64_at(state, stat_column_id(agg_index, StatField::SumILo), 0)?;
    let hi = column_i64_at(state, stat_column_id(agg_index, StatField::SumIHi), 0)?;
    Ok(lo_hi_to_i128(lo, hi))
}

pub fn stat_column_id(agg_index: usize, field: StatField) -> u32 {
    PARTIAL_STAT_COL_BASE + agg_index as u32 * STAT_FIELDS_PER_AGG + field as u32
}

pub fn key_column_id(key_index: usize) -> u32 {
    PARTIAL_KEY_COL_BASE + key_index as u32
}

#[cfg(test)]
mod partial_key_tests {
    use super::*;
    use crate::ops::query::{
        PartialAggSpec, PartialGroup, PartialKeyPart, PartialSnapshot, PartialStat,
    };

    #[test]
    fn float_group_key_survives_wcol_roundtrip() {
        let snap = PartialSnapshot {
            grouped: true,
            order_groups_by_count: false,
            limit: 0,
            offset: 0,
            passed_rows: 1,
            aggs: vec![PartialAggSpec { kind: 0, column: 0 }],
            groups: vec![PartialGroup {
                key: vec![PartialKeyPart::Float(1.5f64.to_bits())],
                stats: vec![PartialStat {
                    count: 0,
                    flags: 0,
                    sum_i: 0,
                    sum_f: 0.0,
                    min_i: 0,
                    max_i: 0,
                    min_f: 0.0,
                    max_f: 0.0,
                    sketch: None,
                }],
            }],
            key_len: 1,
            key_float_mask: 1,
            key_string_mask: 0,
        };
        let state = partial_snapshot_to_wcol(&snap).unwrap();
        assert_eq!(state.schema.group_key_float_mask, 1);
        let back = partial_wcol_to_snapshot(&state).unwrap();
        assert!(matches!(back.groups[0].key[0], PartialKeyPart::Float(_)));
    }
}

fn ensure_same_schema(
    left: &PartialStateSchemaV1,
    right: &PartialStateSchemaV1,
) -> Result<(), String> {
    if left != right {
        return Err("partial state schemas differ".to_string());
    }
    Ok(())
}

pub fn partial_snapshot_to_wcol(snap: &PartialSnapshot) -> Result<PartialStateWcol, String> {
    let group_key_len = if snap.groups.is_empty() {
        snap.key_len
    } else {
        snap.groups[0].key.len() as u8
    };
    // A key column's type comes from its non-NULL parts. An all-NULL or empty column still
    // has to publish the planned string or float mask, or reducers will not merge.
    let mut group_key_float_mask = 0u32;
    let mut group_key_string_mask = 0u32;
    for key_idx in 0..group_key_len as usize {
        let mut kind = None;
        for group in &snap.groups {
            let this = match group.key.get(key_idx) {
                Some(PartialKeyPart::Null) | None => continue,
                Some(PartialKeyPart::Int(_)) => 0u8,
                Some(PartialKeyPart::Float(_)) => 1,
                Some(PartialKeyPart::Bytes(_)) => 2,
            };
            match kind {
                None => kind = Some(this),
                Some(seen) if seen != this => {
                    return Err(format!("group key column {key_idx} mixes value types"));
                }
                Some(_) => {}
            }
        }
        let bit = 1u32 << key_idx;
        match kind {
            Some(1) => group_key_float_mask |= bit,
            Some(2) => group_key_string_mask |= bit,
            None if snap.key_string_mask & bit != 0 => group_key_string_mask |= bit,
            None if snap.key_float_mask & bit != 0 => group_key_float_mask |= bit,
            _ => {}
        }
    }
    let schema = PartialStateSchemaV1 {
        version: PARTIAL_SCHEMA_VERSION,
        grouped: snap.grouped,
        order_groups_by_count: snap.order_groups_by_count,
        limit: snap.limit,
        offset: snap.offset,
        group_key_len,
        group_key_float_mask,
        group_key_string_mask,
        aggs: snap.aggs.clone(),
    };
    let mut out = PartialColumnsBuilder::new(group_key_len as usize, &snap.aggs, snap.groups.len());
    for group in &snap.groups {
        for key_idx in 0..group_key_len as usize {
            out.push_key(
                key_idx,
                match group.key.get(key_idx) {
                    Some(PartialKeyPart::Null) | None => None,
                    Some(PartialKeyPart::Int(v)) => Some(*v as i64),
                    Some(PartialKeyPart::Float(bits)) => Some(*bits as i64),
                    Some(PartialKeyPart::Bytes(_)) => Some(0),
                },
            );
        }
    }
    let mut stats = Vec::with_capacity(snap.groups.len());
    for agg_idx in 0..snap.aggs.len() {
        stats.clear();
        for group in &snap.groups {
            stats.push(
                group
                    .stats
                    .get(agg_idx)
                    .cloned()
                    .ok_or("partial group has fewer stats than aggregates")?,
            );
        }
        out.extend_stats(agg_idx, &stats);
    }
    let string_keys = build_string_key_columns(snap, group_key_string_mask)?;
    Ok(out.finish(schema, snap.passed_rows, string_keys))
}

fn build_string_key_columns(
    snap: &PartialSnapshot,
    string_mask: u32,
) -> Result<Vec<PartialStringKeyColumn>, String> {
    let key_len = snap.groups.first().map(|g| g.key.len()).unwrap_or(0);
    let mut out = Vec::new();
    for key_idx in 0..key_len {
        if string_mask & (1u32 << key_idx) == 0 {
            continue;
        }
        let mut col = PartialStringKeyColumn {
            key_index: key_idx as u8,
            offsets: vec![0],
            bytes: Vec::new(),
            nulls: Vec::with_capacity(snap.groups.len()),
        };
        for group in &snap.groups {
            match group.key.get(key_idx) {
                Some(PartialKeyPart::Bytes(b)) => {
                    col.nulls.push(1);
                    col.bytes.extend_from_slice(b);
                    col.offsets.push(col.bytes.len() as u32);
                }
                Some(PartialKeyPart::Null) | None => {
                    col.nulls.push(0);
                    col.offsets.push(col.offsets.last().copied().unwrap_or(0));
                }
                Some(_) => return Err("string key slot has non-string value".to_string()),
            }
        }
        out.push(col);
    }
    Ok(out)
}

/// Builds partial-state columns by position: group key columns, then
/// [`STAT_FIELDS_PER_AGG`] stat columns per aggregate (ascending column id).
pub(crate) struct PartialColumnsBuilder {
    key_len: usize,
    rows: usize,
    columns: Vec<PartialColumn>,
    sketches: Vec<PartialSketchColumn>,
}

impl PartialColumnsBuilder {
    pub(crate) fn new(key_len: usize, agg_specs: &[PartialAggSpec], rows: usize) -> Self {
        let aggs = agg_specs.len();
        let sketches = agg_specs
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.kind == KIND_APPROX_DISTINCT)
            .map(|(index, _)| PartialSketchColumn {
                agg: index as u32,
                offsets: vec![0],
                bytes: Vec::new(),
            })
            .collect();
        let mut columns = Vec::with_capacity(key_len + aggs * STAT_FIELDS_PER_AGG as usize);
        for key_idx in 0..key_len {
            columns.push(PartialColumn {
                id: key_column_id(key_idx),
                values: Vec::with_capacity(rows),
                nulls: Vec::with_capacity(rows),
            });
        }
        for agg_idx in 0..aggs {
            for field in 0..STAT_FIELDS_PER_AGG {
                columns.push(PartialColumn {
                    id: PARTIAL_STAT_COL_BASE + agg_idx as u32 * STAT_FIELDS_PER_AGG + field,
                    values: Vec::with_capacity(rows),
                    // Stat cells are never null; filled in `finish`.
                    nulls: Vec::new(),
                });
            }
        }
        Self {
            key_len,
            rows: 0,
            columns,
            sketches,
        }
    }

    /// Key part `key_idx` of the next group; `None` is a NULL key (Float keys pass their bits).
    #[inline]
    pub(crate) fn push_key(&mut self, key_idx: usize, value: Option<i64>) {
        let col = &mut self.columns[key_idx];
        col.values.push(value.unwrap_or(0));
        col.nulls.push(u8::from(value.is_some()));
    }

    /// Append one aggregate's stats for many groups at once, a column at a time.
    pub(crate) fn extend_stats(&mut self, agg_idx: usize, stats: &[PartialStat]) {
        let base = self.key_len + agg_idx * STAT_FIELDS_PER_AGG as usize;
        let columns = &mut self.columns[base..base + STAT_FIELDS_PER_AGG as usize];
        // One sequential pass per field; closures (not fn pointers) so each loop inlines.
        macro_rules! fill {
            ($field:expr, $read:expr) => {
                columns[$field as usize]
                    .values
                    .extend(stats.iter().map($read))
            };
        }
        fill!(StatField::Count, |s| s.count as i64);
        fill!(StatField::Flags, |s| s.flags as i64);
        fill!(StatField::SumILo, |s| i128_to_lo(s.sum_i));
        fill!(StatField::SumIHi, |s| i128_to_hi(s.sum_i));
        fill!(StatField::MinILo, |s| i128_to_lo(s.min_i));
        fill!(StatField::MinIHi, |s| i128_to_hi(s.min_i));
        fill!(StatField::MaxILo, |s| i128_to_lo(s.max_i));
        fill!(StatField::MaxIHi, |s| i128_to_hi(s.max_i));
        fill!(StatField::SumFBits, |s| s.sum_f.to_bits() as i64);
        fill!(StatField::MinFBits, |s| s.min_f.to_bits() as i64);
        fill!(StatField::MaxFBits, |s| s.max_f.to_bits() as i64);
        if let Some(column) = self.sketches.iter_mut().find(|c| c.agg as usize == agg_idx) {
            for stat in stats {
                match &stat.sketch {
                    Some(sketch) => sketch.encode(&mut column.bytes),
                    None => Hll::new().encode(&mut column.bytes),
                }
                column.offsets.push(column.bytes.len() as u32);
            }
        }
        if agg_idx == 0 {
            self.rows += stats.len();
        }
    }

    pub(crate) fn finish(
        mut self,
        schema: PartialStateSchemaV1,
        passed_rows: u64,
        string_keys: Vec<PartialStringKeyColumn>,
    ) -> PartialStateWcol {
        let rows = self
            .columns
            .first()
            .map(|c| c.values.len())
            .unwrap_or(self.rows);
        for col in &mut self.columns[self.key_len..] {
            col.nulls = vec![1u8; rows];
        }
        PartialStateWcol {
            schema,
            passed_rows,
            row_count: rows,
            columns: self.columns,
            sketches: self.sketches,
            string_keys,
        }
    }
}

fn partial_wcol_to_snapshot(state: &PartialStateWcol) -> Result<PartialSnapshot, String> {
    let rows = PartialRows::new(state)?;
    let key_n = state.schema.group_key_len as usize;
    let mut groups = Vec::with_capacity(state.row_count);
    for row in 0..state.row_count {
        let key = (0..key_n)
            .map(|k| rows.key(row, k).map(KeyPartRef::to_owned))
            .collect::<Result<Vec<PartialKeyPart>, String>>()?;
        let stats = (0..state.schema.aggs.len())
            .map(|agg| rows.stat(row, agg))
            .collect::<Result<Vec<PartialStat>, String>>()?;
        groups.push(PartialGroup { key, stats });
    }
    Ok(PartialSnapshot {
        grouped: state.schema.grouped,
        order_groups_by_count: state.schema.order_groups_by_count,
        limit: state.schema.limit,
        offset: state.schema.offset,
        passed_rows: state.passed_rows,
        aggs: state.schema.aggs.clone(),
        groups,
        key_len: state.schema.group_key_len,
        key_float_mask: state.schema.group_key_float_mask,
        key_string_mask: state.schema.group_key_string_mask,
    })
}

/// One group-key part of a partial row, borrowed from its columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyPartRef<'a> {
    Null,
    Int(i64),
    /// Canonical f64 bits.
    Float(u64),
    Bytes(&'a [u8]),
}

impl KeyPartRef<'_> {
    fn to_owned(self) -> PartialKeyPart {
        match self {
            KeyPartRef::Null => PartialKeyPart::Null,
            KeyPartRef::Int(v) => PartialKeyPart::Int(i128::from(v)),
            KeyPartRef::Float(bits) => PartialKeyPart::Float(bits),
            KeyPartRef::Bytes(bytes) => PartialKeyPart::Bytes(bytes.to_vec()),
        }
    }
}

/// Row-at-a-time access to a partial state's keys and stats, without building a group per
/// row: a reducer merging another node's partial reads each row once and keeps nothing.
pub(crate) struct PartialRows<'a> {
    state: &'a PartialStateWcol,
    keys: Vec<&'a PartialColumn>,
    /// Per key: its string column when the key is a string.
    strings: Vec<Option<&'a PartialStringKeyColumn>>,
    stats: Vec<[&'a [i64]; STAT_FIELDS_PER_AGG as usize]>,
    /// Per aggregate: its `approx_count_distinct` sketches.
    sketches: Vec<Option<&'a PartialSketchColumn>>,
}

impl<'a> PartialRows<'a> {
    pub(crate) fn new(state: &'a PartialStateWcol) -> Result<Self, String> {
        if state.schema.version != PARTIAL_SCHEMA_VERSION {
            return Err("unsupported partial schema version".to_string());
        }
        let rows = state.row_count;
        let column = |id: u32| -> Result<&'a PartialColumn, String> {
            let col = find_column(state, id)?;
            if col.values.len() < rows || col.nulls.len() < rows {
                return Err(format!("partial column {id} has fewer than {rows} rows"));
            }
            Ok(col)
        };
        let key_n = state.schema.group_key_len as usize;
        let keys = (0..key_n)
            .map(|k| column(key_column_id(k)))
            .collect::<Result<Vec<_>, _>>()?;
        let strings = (0..key_n)
            .map(|k| {
                if state.schema.group_key_string_mask & (1u32 << k) == 0 {
                    return Ok(None);
                }
                // An empty partial still publishes the string mask, with no column to go with it.
                let Some(sk) = state.string_keys.iter().find(|c| c.key_index as usize == k) else {
                    return match rows {
                        0 => Ok(None),
                        _ => Err("string key column missing".to_string()),
                    };
                };
                if sk.offsets.len() != rows + 1 || sk.nulls.len() != rows {
                    return Err("string key column row count mismatch".to_string());
                }
                Ok(Some(sk))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut stats = Vec::with_capacity(state.schema.aggs.len());
        for agg in 0..state.schema.aggs.len() {
            let mut fields: [&[i64]; STAT_FIELDS_PER_AGG as usize] =
                [&[]; STAT_FIELDS_PER_AGG as usize];
            for (field, slot) in fields.iter_mut().enumerate() {
                let id = PARTIAL_STAT_COL_BASE + agg as u32 * STAT_FIELDS_PER_AGG + field as u32;
                *slot = &column(id)?.values;
            }
            stats.push(fields);
        }
        let mut sketches = vec![None; state.schema.aggs.len()];
        for sketch in &state.sketches {
            *sketches
                .get_mut(sketch.agg as usize)
                .ok_or("sketch column for a missing aggregate")? = Some(sketch);
        }
        Ok(Self {
            state,
            keys,
            strings,
            stats,
            sketches,
        })
    }

    pub(crate) fn key(&self, row: usize, k: usize) -> Result<KeyPartRef<'a>, String> {
        let col = self.keys[k];
        if col.nulls[row] == 0 {
            return Ok(KeyPartRef::Null);
        }
        if let Some(sk) = self.strings[k] {
            if sk.nulls[row] == 0 {
                return Ok(KeyPartRef::Null);
            }
            let (start, end) = (sk.offsets[row] as usize, sk.offsets[row + 1] as usize);
            let bytes = sk.bytes.get(start..end).ok_or("string key out of range")?;
            return Ok(KeyPartRef::Bytes(bytes));
        }
        if self.state.schema.group_key_float_mask & (1u32 << k) != 0 {
            let value = f64::from_bits(col.values[row] as u64);
            return Ok(KeyPartRef::Float(
                crate::ops::query::float_key::canonical_f64(value).to_bits(),
            ));
        }
        Ok(KeyPartRef::Int(col.values[row]))
    }

    pub(crate) fn stat(&self, row: usize, agg: usize) -> Result<PartialStat, String> {
        let mut stat = read_stat_row(&self.stats[agg], row);
        if let Some(sketch) = self.sketches[agg] {
            stat.sketch = Some(Hll::decode(sketch.row(row)?)?.0);
        }
        Ok(stat)
    }
}

/// One aggregate's stats at `row`; `fields` are its stat columns in [`StatField`] order.
fn read_stat_row(fields: &[&[i64]; STAT_FIELDS_PER_AGG as usize], row: usize) -> PartialStat {
    let at = |field: StatField| fields[field as usize][row];
    PartialStat {
        count: at(StatField::Count) as u64,
        flags: at(StatField::Flags) as u8,
        sum_i: lo_hi_to_i128(at(StatField::SumILo), at(StatField::SumIHi)),
        sum_f: f64::from_bits(at(StatField::SumFBits) as u64),
        min_i: lo_hi_to_i128(at(StatField::MinILo), at(StatField::MinIHi)),
        max_i: lo_hi_to_i128(at(StatField::MaxILo), at(StatField::MaxIHi)),
        min_f: f64::from_bits(at(StatField::MinFBits) as u64),
        max_f: f64::from_bits(at(StatField::MaxFBits) as u64),
        sketch: None,
    }
}

fn find_column<'a>(state: &'a PartialStateWcol, id: u32) -> Result<&'a PartialColumn, String> {
    state
        .columns
        .iter()
        .find(|c| c.id == id)
        .ok_or_else(|| format!("partial column {id} missing"))
}

fn column_i64_at(state: &PartialStateWcol, id: u32, row: usize) -> Result<i64, String> {
    let col = find_column(state, id)?;
    col.values
        .get(row)
        .copied()
        .ok_or_else(|| format!("partial column {id} row {row} missing"))
}

fn i128_to_lo(v: i128) -> i64 {
    v as i64
}

fn i128_to_hi(v: i128) -> i64 {
    (v >> 64) as i64
}

fn lo_hi_to_i128(lo: i64, hi: i64) -> i128 {
    ((hi as i128) << 64) | ((lo as u64) as i128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ColumnKind;
    use crate::ops::query::{partial_first_sum_i, CatalogColumn, SelectionMask};
    use crate::stream::OwnedWcolBatch;

    fn catalog() -> Vec<CatalogColumn> {
        vec![CatalogColumn {
            name: "a".into(),
            kind: ColumnKind::Number,
            physical: 0,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        }]
    }

    fn sum_a(values: &[i64]) -> crate::ops::query::PartialSnapshot {
        let mut query = crate::ops::query::batch::BatchQuery::from_ir(
            &[],
            &[],
            &[crate::ir::AggExpr {
                column: Some(0),
                kind: crate::ir::AggKind::Sum,
                offset: 0,
            }],
            None,
            0,
            false,
            &catalog(),
        )
        .unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, values.to_vec(), vec![1; values.len()]);
        batch.set_row_count(values.len()).unwrap();
        let mut cols = Vec::new();
        let view = batch.view_with(&mut cols);
        query
            .consume(&view, &SelectionMask::all(values.len()))
            .unwrap();
        query.finish_snapshot()
    }

    #[test]
    fn partial_state_batch_roundtrip() {
        let wcol = partial_state_from_snapshot(&sum_a(&[5])).unwrap();
        let batch = partial_state_to_batch(wcol.clone());
        assert!(batch.is_partial_batch());
        assert_eq!(partial_state_from_batch(&batch).unwrap(), wcol);
    }

    #[test]
    fn partial_state_roundtrips_through_pwcl() {
        let state = partial_state_from_snapshot(&sum_a(&[5])).unwrap();
        let wire = crate::stream::partial_wcs::partial_state_to_wcs_payload(&state).unwrap();
        let back = crate::stream::partial_wcs::partial_state_from_wire_payload(&wire).unwrap();
        assert_eq!(state, back);
        assert_eq!(partial_state_first_sum_i(&state, 0).unwrap(), 5);
        assert_eq!(partial_first_sum_i(&wire).unwrap(), 5);
    }

    #[test]
    fn partial_state_merge_sums() {
        let a = partial_state_from_snapshot(&sum_a(&[5])).unwrap();
        let b = partial_state_from_snapshot(&sum_a(&[7])).unwrap();
        let merged = partial_state_merge(&a, &b).unwrap();
        assert_eq!(partial_state_first_sum_i(&merged, 0).unwrap(), 12);
    }

    #[test]
    fn oversized_partial_splits_on_group_rows() {
        let state = PartialStateWcol {
            schema: PartialStateSchemaV1 {
                version: PARTIAL_SCHEMA_VERSION,
                grouped: true,
                order_groups_by_count: false,
                limit: 0,
                offset: 0,
                group_key_len: 0,
                group_key_float_mask: 0,
                group_key_string_mask: 0,
                aggs: vec![],
            },
            passed_rows: 4,
            row_count: 2,
            columns: vec![PartialColumn {
                id: 1,
                values: vec![10, 20],
                nulls: vec![1, 1],
            }],
            sketches: vec![],
            string_keys: vec![],
        };
        let full = partial_state_to_wcs_payload(&state).unwrap();
        let frames = partial_state_payloads(&state, full.len() - 1).unwrap();
        assert!(frames.len() >= 2);
        assert!(frames.iter().all(|frame| frame.len() < full.len()));
        let passed: u64 = frames
            .iter()
            .map(|frame| partial_state_from_wire_payload(frame).unwrap().passed_rows)
            .sum();
        assert_eq!(passed, 4);
        let values: Vec<i64> = frames
            .iter()
            .flat_map(|frame| {
                partial_state_from_wire_payload(frame)
                    .unwrap()
                    .columns
                    .into_iter()
                    .flat_map(|column| column.values)
            })
            .collect();
        assert_eq!(values, vec![10, 20]);
    }
}
