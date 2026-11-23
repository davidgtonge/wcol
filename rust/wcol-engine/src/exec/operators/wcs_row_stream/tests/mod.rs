use super::*;
use crate::stream::{DictEntries, DictionaryValues, WcolColumn};
use std::sync::Arc;

mod codec;
mod frames;
mod limits;

/// Decode one partition stream the way a reducer port does.
pub(super) fn decode_partition(wire: &[u8]) -> Vec<OwnedWcolBatch> {
    let mut ingress = RowBatchWcsIngress::new();
    ingress.feed(wire).unwrap();
    ingress.finish_feed().unwrap();
    ingress.take_batches()
}

/// Each row's dictionary value, `None` for a null row.
pub(super) fn dict_values(batch: &OwnedWcolBatch) -> Vec<Option<Vec<u8>>> {
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    let WcolColumn::DictId {
        ids,
        nulls,
        dictionary,
        ..
    } = &view.columns[0]
    else {
        panic!("expected a dictionary column");
    };
    let DictEntries::Bytes { offsets, bytes } = &dictionary.entries else {
        panic!("expected byte entries");
    };
    ids.iter()
        .zip(nulls.iter())
        .map(|(id, valid)| {
            (*valid != 0).then(|| {
                let at = *id as usize;
                bytes[offsets[at] as usize..offsets[at + 1] as usize].to_vec()
            })
        })
        .collect()
}

pub(super) fn round_trip(batches: &[OwnedWcolBatch]) -> Vec<OwnedWcolBatch> {
    let mut egress = RowBatchWcsEgress::new();
    for batch in batches {
        egress.push_batch(batch).unwrap();
    }
    egress.push_end().unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut ingress = RowBatchWcsIngress::new();
    let mut off = 0usize;
    while off < wire.len() {
        let n = ingress.feed(&wire[off..off + 1]).unwrap();
        assert_eq!(n, 1);
        off += n;
    }
    ingress.finish_feed().unwrap();
    ingress.take_batches()
}

pub(super) fn dict_int(scope: u32, column: u32, values: &[i64]) -> DictionaryValues {
    DictionaryValues {
        scope,
        column,
        entries: DictEntries::Int(Arc::from(values.to_vec())),
    }
}

pub(super) fn dict_bytes(scope: u32, column: u32, values: &[&[u8]]) -> DictionaryValues {
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend_from_slice(value);
        offsets.push(bytes.len() as u32);
    }
    DictionaryValues {
        scope,
        column,
        entries: DictEntries::Bytes {
            offsets: Arc::from(offsets),
            bytes: Arc::from(bytes),
        },
    }
}

/// Each column's cells as bytes (`None` for null): a dictionary column and its plain
/// values compare equal, since the egress may send either.
pub(super) fn cells(batch: &OwnedWcolBatch) -> Vec<(u32, Vec<Option<Vec<u8>>>)> {
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    let rows = view.row_count;
    view.columns
        .iter()
        .map(|col| {
            let cell = |row: usize| -> Option<Vec<u8>> {
                match col {
                    WcolColumn::I64 { values, nulls, .. } => {
                        (nulls[row] != 0).then(|| values[row].to_le_bytes().to_vec())
                    }
                    WcolColumn::F64 { values, nulls, .. } => {
                        (nulls[row] != 0).then(|| values[row].to_bits().to_le_bytes().to_vec())
                    }
                    WcolColumn::Utf8 {
                        offsets,
                        bytes,
                        nulls,
                        ..
                    } => (nulls[row] != 0)
                        .then(|| bytes[offsets[row] as usize..offsets[row + 1] as usize].to_vec()),
                    WcolColumn::DictId {
                        ids,
                        nulls,
                        dictionary,
                        ..
                    } => (nulls[row] != 0).then(|| {
                        let id = ids[row] as usize;
                        match &dictionary.entries {
                            DictEntries::Int(values) => values[id].to_le_bytes().to_vec(),
                            DictEntries::Float(values) => {
                                values[id].to_bits().to_le_bytes().to_vec()
                            }
                            DictEntries::Bytes { offsets, bytes } => {
                                bytes[offsets[id] as usize..offsets[id + 1] as usize].to_vec()
                            }
                        }
                    }),
                }
            };
            let column = match col {
                WcolColumn::I64 { column, .. }
                | WcolColumn::F64 { column, .. }
                | WcolColumn::Utf8 { column, .. }
                | WcolColumn::DictId { column, .. } => *column,
            };
            (column, (0..rows).map(cell).collect())
        })
        .collect()
}

pub(super) fn assert_same_batch(left: &OwnedWcolBatch, right: &OwnedWcolBatch) {
    assert_eq!(left.row_count(), right.row_count());
    assert_eq!(left.dict_scope(), right.dict_scope());
    assert_eq!(cells(left), cells(right));
}

pub(super) fn retained_dictionary_bytes(ingress: &RowBatchWcsIngress) -> usize {
    ingress
        .dicts
        .values()
        .map(|dict| match &dict.entries {
            DictEntries::Int(values) => values.len().saturating_mul(8),
            DictEntries::Float(values) => values.len().saturating_mul(8),
            DictEntries::Bytes { offsets, bytes } => offsets.len().saturating_mul(4) + bytes.len(),
        })
        .sum()
}
