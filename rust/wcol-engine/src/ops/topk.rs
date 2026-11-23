//! Bounded top-k over the same row order as [`sort_rows`](super::sort::sort_rows).
//!
//! The operator keeps the best `n` rows. A bounded `n` does not use the full-sort byte budget.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::ops::query::float_key::float_key;
use crate::ops::sort::{sort_rows, RowSortKey};
use crate::stream::{DictEntries, OwnedWcolBatch, WcolColumn};

pub struct TopKOp {
    keys: Vec<RowSortKey>,
    n: usize,
    rows: Vec<OwnedWcolBatch>,
    template: Option<OwnedWcolBatch>,
    heap: BinaryHeap<HeapItem>,
    seq: u64,
    done: bool,
}

impl TopKOp {
    pub fn new(keys: Vec<RowSortKey>, n: usize) -> Self {
        Self {
            keys,
            n,
            rows: Vec::new(),
            template: None,
            heap: BinaryHeap::new(),
            seq: 0,
            done: false,
        }
    }

    /// The best `n` rows of `batches`, in sort order. `n == 0` is an empty batch with the
    /// input schema once any batch has been seen.
    pub fn take(
        keys: Vec<RowSortKey>,
        n: usize,
        batches: &[OwnedWcolBatch],
    ) -> Result<OwnedWcolBatch, String> {
        let mut op = Self::new(keys, n);
        for batch in batches {
            op.push_batch(batch)?;
        }
        op.finish()
    }
}

impl Operator for TopKOp {
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
                self.push_batch(&item.into_batch())?;
                return Ok(StreamProgress::NeedInput);
            }
        } else if io.input_item().is_some() {
            return Err("topk requires OperatorIo::input_slot (zero-copy batch move)".to_string());
        }
        if !io.input_eof {
            return Ok(StreamProgress::NeedInput);
        }
        if self.template.is_none() {
            self.done = true;
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        let batch = self.finish()?;
        self.done = true;
        *io.output = Some(WcolStreamItem::from_row_batch(batch));
        Ok(StreamProgress::HaveOutput)
    }
}

impl TopKOp {
    fn push_batch(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        if self.template.is_none() {
            self.template = Some(batch.clone());
        }
        let mut buf = Vec::new();
        let view = batch.view_with(&mut buf);
        let mask = batch.effective_selection_mask();
        for row in 0..view.row_count {
            if mask.is_selected(row) {
                self.offer(batch, &view.columns, row)?;
            }
        }
        Ok(())
    }

    fn offer(
        &mut self,
        batch: &OwnedWcolBatch,
        columns: &[WcolColumn<'_>],
        row: usize,
    ) -> Result<(), String> {
        if self.n == 0 {
            return Ok(());
        }
        let parts = key_parts(columns, row, &self.keys)?;
        let seq = self.seq;
        self.seq += 1;
        let candidate = HeapItem {
            parts,
            seq,
            slot: 0,
        };
        if self.rows.len() < self.n {
            let slot = self.rows.len();
            self.rows.push(one_row(batch, row)?);
            let mut item = candidate;
            item.slot = slot;
            self.heap.push(item);
            return Ok(());
        }
        let worst = self.heap.peek().expect("heap holds n rows");
        if candidate.cmp(worst) != Ordering::Less {
            return Ok(());
        }
        let HeapItem { slot, .. } = self.heap.pop().expect("heap holds n rows");
        self.rows[slot] = one_row(batch, row)?;
        let mut item = candidate;
        item.slot = slot;
        self.heap.push(item);
        Ok(())
    }

    fn finish(&mut self) -> Result<OwnedWcolBatch, String> {
        if self.rows.is_empty() {
            return empty_like(self.template.as_ref().expect("finish after a batch"));
        }
        let mut items: Vec<HeapItem> = self.heap.drain().collect();
        items.sort_by_key(|item| item.seq);
        let kept: Vec<OwnedWcolBatch> = items
            .iter()
            .map(|item| self.rows[item.slot].clone())
            .collect();
        sort_rows(&kept, &self.keys)
    }
}

fn one_row(src: &OwnedWcolBatch, row: usize) -> Result<OwnedWcolBatch, String> {
    let mut out = OwnedWcolBatch::empty();
    out.append_rows_from(src, &[row])?;
    Ok(out)
}

fn empty_like(src: &OwnedWcolBatch) -> Result<OwnedWcolBatch, String> {
    let mut buf = Vec::new();
    let view = src.view_with(&mut buf);
    let mut out = OwnedWcolBatch::empty();
    out.set_scope(view.dict_scope);
    for col in view.columns {
        match col {
            WcolColumn::I64 { column, .. } => out.push_i64_column(*column, Vec::new(), Vec::new()),
            WcolColumn::F64 { column, .. } => out.push_f64_column(*column, Vec::new(), Vec::new()),
            WcolColumn::Utf8 { column, .. } => {
                out.push_utf8_column(*column, vec![0], Vec::new(), Vec::new())
            }
            WcolColumn::DictId {
                column, dictionary, ..
            } => match &dictionary.entries {
                DictEntries::Int(_) => out.push_i64_column(*column, Vec::new(), Vec::new()),
                DictEntries::Float(_) => out.push_f64_column(*column, Vec::new(), Vec::new()),
                DictEntries::Bytes { .. } => {
                    out.push_utf8_column(*column, vec![0], Vec::new(), Vec::new())
                }
            },
        }
    }
    out.set_row_count(0).map_err(|err| err.to_string())?;
    Ok(out)
}

#[derive(Clone)]
enum Part {
    Null,
    Int(i64),
    Float(u64),
    Bytes(Vec<u8>),
}

struct Piece {
    part: Part,
    descending: bool,
    nulls_first: bool,
}

struct HeapItem {
    parts: Vec<Piece>,
    seq: u64,
    slot: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.parts.iter().zip(&other.parts) {
            let ord = cmp_piece(a, b);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.seq.cmp(&other.seq)
    }
}

fn cmp_piece(a: &Piece, b: &Piece) -> Ordering {
    let pa = !matches!(a.part, Part::Null);
    let pb = !matches!(b.part, Part::Null);
    match (pa, pb) {
        (false, false) => Ordering::Equal,
        (false, true) => {
            if a.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (true, false) => {
            if a.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (true, true) => {
            let ord = match (&a.part, &b.part) {
                (Part::Int(x), Part::Int(y)) => x.cmp(y),
                (Part::Float(x), Part::Float(y)) => x.cmp(y),
                (Part::Bytes(x), Part::Bytes(y)) => x.cmp(y),
                _ => Ordering::Equal,
            };
            if a.descending {
                ord.reverse()
            } else {
                ord
            }
        }
    }
}

fn key_parts(
    columns: &[WcolColumn<'_>],
    row: usize,
    keys: &[RowSortKey],
) -> Result<Vec<Piece>, String> {
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        let col = columns
            .iter()
            .find(|col| column_id(col) == key.column)
            .ok_or_else(|| format!("topk column {} was not decoded", key.column))?;
        parts.push(Piece {
            part: cell_part(col, row)?,
            descending: key.descending,
            nulls_first: key.nulls_first,
        });
    }
    Ok(parts)
}

fn cell_part(col: &WcolColumn<'_>, row: usize) -> Result<Part, String> {
    match col {
        WcolColumn::I64 { values, nulls, .. } => {
            if nulls[row] == 0 {
                Ok(Part::Null)
            } else {
                Ok(Part::Int(values[row]))
            }
        }
        WcolColumn::F64 { values, nulls, .. } => {
            if nulls[row] == 0 {
                Ok(Part::Null)
            } else {
                Ok(Part::Float(float_key(values[row])))
            }
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            if nulls[row] == 0 {
                Ok(Part::Null)
            } else {
                Ok(Part::Bytes(
                    bytes[offsets[row] as usize..offsets[row + 1] as usize].to_vec(),
                ))
            }
        }
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            if nulls[row] == 0 {
                return Ok(Part::Null);
            }
            let id = ids[row] as usize;
            match &dictionary.entries {
                DictEntries::Int(entries) => Ok(Part::Int(
                    *entries.get(id).ok_or("dictionary index out of range")?,
                )),
                DictEntries::Float(entries) => Ok(Part::Float(float_key(
                    *entries.get(id).ok_or("dictionary index out of range")?,
                ))),
                DictEntries::Bytes { offsets, bytes } => {
                    if id + 1 >= offsets.len() {
                        return Err("dictionary index out of range".to_string());
                    }
                    Ok(Part::Bytes(
                        bytes[offsets[id] as usize..offsets[id + 1] as usize].to_vec(),
                    ))
                }
            }
        }
    }
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}
