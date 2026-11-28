//! Sort row batches by integer, float, string, or dictionary columns.
//!
//! Nulls sort last in both directions unless [`RowSortKey::nulls_first`]. Equal keys keep
//! input order. Floats use [`float_key`], so NaN sorts above `+inf` and `-0.0` ties with `0.0`.
//! Strings compare as bytes, which is DuckDB's default binary collation. Dictionary ids compare
//! as their resolved values, so ids from different scopes are not mixed.

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::ir::SortKey;
use crate::ops::query::float_key::float_key;
use crate::stream::{DictEntries, OwnedWcolBatch, SelectionMask, WcolColumn};

/// A full sort that exceeds this fails with [`SORT_TOO_LARGE`]. Spilling is out of scope.
pub const DEFAULT_SORT_BYTE_BUDGET: usize = 64 * 1024 * 1024;

pub const SORT_TOO_LARGE: &str = "SORT_TOO_LARGE";

/// Row sort key. Same value as [`SortKey`].
pub type RowSortKey = SortKey;

pub struct SortOp {
    keys: Vec<RowSortKey>,
    budget: usize,
    buffered_bytes: usize,
    buffered: Vec<OwnedWcolBatch>,
    done: bool,
}

impl SortOp {
    pub fn with_keys(keys: Vec<RowSortKey>) -> Self {
        Self::with_budget(keys, DEFAULT_SORT_BYTE_BUDGET)
    }

    pub fn with_budget(keys: Vec<RowSortKey>, budget: usize) -> Self {
        Self {
            keys,
            budget,
            buffered_bytes: 0,
            buffered: Vec::new(),
            done: false,
        }
    }
}

impl Operator for SortOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.done {
            return Ok(StreamProgress::Finished);
        }
        if let Some(slot) = io.input_slot.as_mut() {
            if let Some(item) = slot.take() {
                let batch = item.into_batch();
                let next = self
                    .buffered_bytes
                    .saturating_add(batch_payload_bytes(&batch));
                if next > self.budget {
                    return Err(format!(
                        "{SORT_TOO_LARGE}: buffered {next} bytes exceeds the {} byte sort budget",
                        self.budget
                    ));
                }
                self.buffered_bytes = next;
                self.buffered.push(batch);
                return Ok(StreamProgress::NeedInput);
            }
        } else if io.input_item().is_some() {
            return Err("sort requires OperatorIo::input_slot (zero-copy batch move)".to_string());
        }
        if !io.input_eof {
            return Ok(StreamProgress::NeedInput);
        }
        if self.buffered.is_empty() {
            self.done = true;
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        let sorted = sort_rows(&self.buffered, &self.keys)?;
        self.buffered.clear();
        self.done = true;
        *io.output = Some(WcolStreamItem::from_row_batch(sorted));
        Ok(StreamProgress::HaveOutput)
    }
}

enum ColData {
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bytes {
        offsets: Vec<u32>,
        bytes: Vec<u8>,
    },
    /// An integer column that outgrew `i64` somewhere (see [`column_kinds`]).
    Wide(Vec<i128>),
}

struct PlainCol {
    id: u32,
    data: ColData,
    nulls: Vec<u8>,
}

/// Order every row of `batches` by `keys`.
pub fn sort_row_batches(
    batches: &[OwnedWcolBatch],
    keys: &[SortKey],
) -> Result<OwnedWcolBatch, String> {
    sort_rows(batches, keys)
}

pub fn sort_rows(
    batches: &[OwnedWcolBatch],
    keys: &[RowSortKey],
) -> Result<OwnedWcolBatch, String> {
    if keys.is_empty() {
        return Err("sort needs a key".to_string());
    }
    let mut columns: Vec<PlainCol> = column_kinds(batches)?
        .into_iter()
        .map(|(id, kind)| plain_col(id, kind))
        .collect();
    let mut selected = Vec::new();
    let mut scope = 0u32;
    for batch in batches {
        scope = batch_scope(batch);
        let mut view_buf = Vec::new();
        let view = batch.view_with(&mut view_buf);
        if view.columns.len() != columns.len() {
            return Err("sort batches have different columns".to_string());
        }
        let mask = batch.effective_selection_mask();
        for row in 0..view.row_count {
            selected.push(mask.is_selected(row));
            for (slot, col) in view.columns.iter().enumerate() {
                if column_id(col) != columns[slot].id {
                    return Err("sort batches have different columns".to_string());
                }
                push_cell(&mut columns[slot], col, row)?;
            }
        }
    }
    let key_at: Vec<(usize, RowSortKey)> = keys
        .iter()
        .map(|key| {
            columns
                .iter()
                .position(|col| col.id == key.column)
                .map(|slot| (slot, *key))
                .ok_or_else(|| format!("sort column {} was not decoded", key.column))
        })
        .collect::<Result<_, _>>()?;
    let rows = selected.len();
    let mut order: Vec<usize> = (0..rows).collect();
    order.sort_by(|&a, &b| cmp_rows(&columns, &key_at, a, b));
    emit_ordered(&columns, &order, &selected, scope)
}

pub fn batch_payload_bytes(batch: &OwnedWcolBatch) -> usize {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    let mut total = 0usize;
    for col in view.columns {
        total = total.saturating_add(match col {
            WcolColumn::I64 { values, nulls, .. } => values.len().saturating_mul(8) + nulls.len(),
            WcolColumn::F64 { values, nulls, .. } => values.len().saturating_mul(8) + nulls.len(),
            WcolColumn::DictId {
                ids,
                nulls,
                dictionary,
                ..
            } => ids.len().saturating_mul(4) + nulls.len() + dict_bytes(dictionary),
            WcolColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => offsets.len().saturating_mul(4) + bytes.len() + nulls.len(),
        });
    }
    total
}

fn dict_bytes(dict: &crate::stream::DictionaryValues) -> usize {
    match &dict.entries {
        DictEntries::Int(values) => values.len().saturating_mul(8),
        DictEntries::Float(values) => values.len().saturating_mul(8),
        DictEntries::Bytes { offsets, bytes } => offsets.len().saturating_mul(4) + bytes.len(),
    }
}

fn batch_scope(batch: &OwnedWcolBatch) -> u32 {
    let mut buf = Vec::new();
    batch.view_with(&mut buf).dict_scope
}

fn plain_col(id: u32, kind: Resolved) -> PlainCol {
    let data = match kind {
        Resolved::I64 => ColData::I64(Vec::new()),
        Resolved::F64 => ColData::F64(Vec::new()),
        Resolved::Bytes => ColData::Bytes {
            offsets: vec![0],
            bytes: Vec::new(),
        },
        Resolved::Wide => ColData::Wide(Vec::new()),
    };
    PlainCol {
        id,
        data,
        nulls: Vec::new(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Resolved {
    I64,
    F64,
    Bytes,
    Wide,
}

/// What the batches say about one column so far.
struct ColumnEvidence {
    id: u32,
    kind: Resolved,
    saw_i64: bool,
    /// Every text value seen is a canonical integer.
    integer_text: bool,
    /// Some text value is an integer outside `i64`.
    past_i64: bool,
}

/// Each column's id and sort type across `batches`. A finalized integer `SUM` is `i64`, or
/// exact decimal text in a batch where it outgrew `i64` ([`push_wide_integer_column`]): a
/// column whose text is all canonical integers, and that is `i64` in another batch or holds
/// a value past `i64`, sorts as one wide integer column. Otherwise ORDER BY would compare
/// such sums as bytes ("9" after "10"), and a column `i64` in one reducer's rows and text
/// in another's could not be sorted at all.
///
/// [`push_wide_integer_column`]: crate::ops::finalize::push_wide_integer_column
fn column_kinds(batches: &[OwnedWcolBatch]) -> Result<Vec<(u32, Resolved)>, String> {
    let mut evidence: Vec<ColumnEvidence> = Vec::new();
    for batch in batches {
        let mut view_buf = Vec::new();
        let view = batch.view_with(&mut view_buf);
        if evidence.is_empty() {
            evidence = view
                .columns
                .iter()
                .map(|col| {
                    Ok(ColumnEvidence {
                        id: column_id(col),
                        kind: resolved_data(col)?,
                        saw_i64: false,
                        integer_text: true,
                        past_i64: false,
                    })
                })
                .collect::<Result<_, String>>()?;
        }
        if view.columns.len() != evidence.len() {
            return Err("sort batches have different columns".to_string());
        }
        for (seen, col) in evidence.iter_mut().zip(view.columns) {
            if column_id(col) != seen.id {
                return Err("sort batches have different columns".to_string());
            }
            match col {
                WcolColumn::I64 { .. } => seen.saw_i64 = true,
                WcolColumn::Utf8 {
                    offsets,
                    bytes,
                    nulls,
                    ..
                } if seen.integer_text => {
                    for row in 0..view.row_count {
                        if nulls[row] == 0 {
                            continue;
                        }
                        let text = &bytes[offsets[row] as usize..offsets[row + 1] as usize];
                        match canonical_integer(text) {
                            Some(value) => seen.past_i64 |= i64::try_from(value).is_err(),
                            None => {
                                seen.integer_text = false;
                                break;
                            }
                        }
                    }
                }
                WcolColumn::DictId { .. } => seen.integer_text = false,
                _ => {}
            }
            let kind = resolved_data(col)?;
            if kind != seen.kind && seen.kind != Resolved::Wide {
                let integer = matches!(
                    (seen.kind, kind),
                    (Resolved::I64, Resolved::Bytes) | (Resolved::Bytes, Resolved::I64)
                );
                if !integer {
                    return Err("sort column changed type".to_string());
                }
                seen.kind = Resolved::Wide;
            }
        }
    }
    evidence
        .into_iter()
        .map(|seen| {
            let kind = match seen.kind {
                Resolved::Wide if !seen.integer_text => {
                    return Err("sort column changed type".to_string());
                }
                Resolved::Bytes if seen.integer_text && seen.past_i64 => Resolved::Wide,
                kind => kind,
            };
            Ok((seen.id, kind))
        })
        .collect()
}

/// `text` as an integer if it is one in canonical form (no sign on zero, no leading zeros):
/// the form [`push_wide_integer_column`](crate::ops::finalize::push_wide_integer_column)
/// writes.
fn canonical_integer(text: &[u8]) -> Option<i128> {
    let digits = text.strip_prefix(b"-").unwrap_or(text);
    let canonical = match digits {
        [b'0'] => digits.len() == text.len(),
        [b'1'..=b'9', rest @ ..] => rest.iter().all(u8::is_ascii_digit),
        _ => false,
    };
    if !canonical {
        return None;
    }
    std::str::from_utf8(text).ok()?.parse().ok()
}

fn resolved_data(col: &WcolColumn<'_>) -> Result<Resolved, String> {
    match col {
        WcolColumn::I64 { .. } => Ok(Resolved::I64),
        WcolColumn::F64 { .. } => Ok(Resolved::F64),
        WcolColumn::Utf8 { .. } => Ok(Resolved::Bytes),
        WcolColumn::DictId { dictionary, .. } => match &dictionary.entries {
            DictEntries::Int(_) => Ok(Resolved::I64),
            DictEntries::Float(_) => Ok(Resolved::F64),
            DictEntries::Bytes { .. } => Ok(Resolved::Bytes),
        },
    }
}

fn push_cell(col: &mut PlainCol, src: &WcolColumn<'_>, row: usize) -> Result<(), String> {
    if let ColData::Wide(dest) = &mut col.data {
        let (value, present) = match src {
            WcolColumn::I64 { values, nulls, .. } => (i128::from(values[row]), nulls[row]),
            WcolColumn::Utf8 { nulls, .. } if nulls[row] == 0 => (0, 0),
            WcolColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => (
                canonical_integer(&bytes[offsets[row] as usize..offsets[row + 1] as usize])
                    .ok_or("sort column changed type")?,
                nulls[row],
            ),
            _ => return Err("sort column changed type".to_string()),
        };
        dest.push(value);
        col.nulls.push(present);
        return Ok(());
    }
    if !matches_resolved(&col.data, resolved_data(src)?) {
        return Err("sort column changed type".to_string());
    }
    match src {
        WcolColumn::I64 { values, nulls, .. } => {
            let ColData::I64(dest) = &mut col.data else {
                return Err("sort column changed type".to_string());
            };
            dest.push(values[row]);
            col.nulls.push(nulls[row]);
        }
        WcolColumn::F64 { values, nulls, .. } => {
            let ColData::F64(dest) = &mut col.data else {
                return Err("sort column changed type".to_string());
            };
            dest.push(values[row]);
            col.nulls.push(nulls[row]);
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => push_bytes(
            col,
            &bytes[offsets[row] as usize..offsets[row + 1] as usize],
            nulls[row],
        )?,
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            let present = nulls[row];
            if present == 0 {
                push_resolved_null(col)?;
                return Ok(());
            }
            let id = ids[row] as usize;
            match &dictionary.entries {
                DictEntries::Int(entries) => {
                    let value = *entries.get(id).ok_or("dictionary index out of range")?;
                    let ColData::I64(dest) = &mut col.data else {
                        return Err("sort column changed type".to_string());
                    };
                    dest.push(value);
                    col.nulls.push(1);
                }
                DictEntries::Float(entries) => {
                    let value = *entries.get(id).ok_or("dictionary index out of range")?;
                    let ColData::F64(dest) = &mut col.data else {
                        return Err("sort column changed type".to_string());
                    };
                    dest.push(value);
                    col.nulls.push(1);
                }
                DictEntries::Bytes { offsets, bytes } => {
                    if id + 1 >= offsets.len() {
                        return Err("dictionary index out of range".to_string());
                    }
                    let start = offsets[id] as usize;
                    let end = offsets[id + 1] as usize;
                    push_bytes(col, &bytes[start..end], 1)?;
                }
            }
        }
    }
    Ok(())
}

fn matches_resolved(data: &ColData, resolved: Resolved) -> bool {
    matches!(
        (data, resolved),
        (ColData::I64(_), Resolved::I64)
            | (ColData::F64(_), Resolved::F64)
            | (ColData::Bytes { .. }, Resolved::Bytes)
    )
}

fn push_resolved_null(col: &mut PlainCol) -> Result<(), String> {
    match &mut col.data {
        ColData::I64(values) => values.push(0),
        ColData::Wide(values) => values.push(0),
        ColData::F64(values) => values.push(0.0),
        ColData::Bytes { offsets, .. } => offsets.push(*offsets.last().unwrap_or(&0)),
    }
    col.nulls.push(0);
    Ok(())
}

fn push_bytes(col: &mut PlainCol, value: &[u8], present: u8) -> Result<(), String> {
    let ColData::Bytes { offsets, bytes } = &mut col.data else {
        return Err("sort column changed type".to_string());
    };
    bytes.extend_from_slice(value);
    offsets.push(u32::try_from(bytes.len()).map_err(|_| "sort string offset overflow")?);
    col.nulls.push(present);
    Ok(())
}

fn cmp_rows(
    columns: &[PlainCol],
    keys: &[(usize, RowSortKey)],
    a: usize,
    b: usize,
) -> std::cmp::Ordering {
    for &(slot, key) in keys {
        let ord = cmp_cell(&columns[slot], a, b, key.descending, key.nulls_first);
        if ord.is_ne() {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn cmp_cell(
    col: &PlainCol,
    a: usize,
    b: usize,
    desc: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    let pa = col.nulls[a] != 0;
    let pb = col.nulls[b] != 0;
    cmp_sort_presence(pa, pb, nulls_first, desc, || match &col.data {
        ColData::I64(values) => values[a].cmp(&values[b]),
        ColData::Wide(values) => values[a].cmp(&values[b]),
        ColData::F64(values) => float_key(values[a]).cmp(&float_key(values[b])),
        ColData::Bytes { offsets, bytes } => {
            let left = &bytes[offsets[a] as usize..offsets[a + 1] as usize];
            let right = &bytes[offsets[b] as usize..offsets[b + 1] as usize];
            left.cmp(right)
        }
    })
}

/// Null placement is independent of sort direction. `descending` reverses present values only.
pub(crate) fn cmp_sort_presence(
    left_present: bool,
    right_present: bool,
    nulls_first: bool,
    descending: bool,
    values: impl FnOnce() -> std::cmp::Ordering,
) -> std::cmp::Ordering {
    match (left_present, right_present) {
        (false, false) => std::cmp::Ordering::Equal,
        (false, true) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (true, false) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (true, true) => {
            let ord = values();
            if descending {
                ord.reverse()
            } else {
                ord
            }
        }
    }
}

fn emit_ordered(
    columns: &[PlainCol],
    order: &[usize],
    selected: &[bool],
    scope: u32,
) -> Result<OwnedWcolBatch, String> {
    let rows = order.len();
    let mut out = OwnedWcolBatch::empty();
    out.set_scope(scope);
    let mut mask = SelectionMask::with_rows(rows);
    for (at, &src) in order.iter().enumerate() {
        if selected[src] {
            mask.set_selected(at, true);
        }
    }
    for col in columns {
        let nulls: Vec<u8> = order.iter().map(|&row| col.nulls[row]).collect();
        match &col.data {
            ColData::I64(values) => {
                let values = order.iter().map(|&row| values[row]).collect();
                out.push_i64_column(col.id, values, nulls);
            }
            ColData::F64(values) => {
                let values = order.iter().map(|&row| values[row]).collect();
                out.push_f64_column(col.id, values, nulls);
            }
            ColData::Wide(values) => {
                let values: Vec<i128> = order.iter().map(|&row| values[row]).collect();
                crate::ops::finalize::push_wide_integer_column(&mut out, col.id, &values, nulls)?;
            }
            ColData::Bytes { offsets, bytes } => {
                let mut out_offsets = Vec::with_capacity(rows + 1);
                let mut out_bytes = Vec::new();
                out_offsets.push(0);
                for &row in order {
                    out_bytes.extend_from_slice(
                        &bytes[offsets[row] as usize..offsets[row + 1] as usize],
                    );
                    out_offsets.push(
                        u32::try_from(out_bytes.len())
                            .map_err(|_| "sort string offset overflow")?,
                    );
                }
                out.push_utf8_column(col.id, out_offsets, out_bytes, nulls);
            }
        }
    }
    out.set_row_count(rows).map_err(|err| err.to_string())?;
    out.set_selection_mask(mask);
    Ok(out)
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

#[cfg(test)]
mod tests;
