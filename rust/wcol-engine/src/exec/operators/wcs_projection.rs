//! WCS projection payloads ↔ [`OwnedWcolBatch`].

use std::collections::HashMap;

use crate::error::{WcolError, WcolResult};
use crate::stream::projection::{
    decode_projection_columns, encode_projection_columns, DecodedProjectionColumn, ProjectionColumn,
};
use crate::stream::{DictionaryValues, OwnedWcolBatch};

pub fn owned_batch_from_projection_body(body: &[u8]) -> WcolResult<OwnedWcolBatch> {
    owned_batch_from_projection_body_with_dicts(body, &HashMap::new())
}

pub fn owned_batch_from_projection_body_with_dicts(
    body: &[u8],
    dicts: &HashMap<(u32, u32), DictionaryValues>,
) -> WcolResult<OwnedWcolBatch> {
    if body.len() < 13 {
        return Err(WcolError::from("truncated data batch"));
    }
    if body[0] != 2 {
        return Err(WcolError::from("expected projection batch kind"));
    }
    let scope = u32::from_le_bytes(body[1..5].try_into().unwrap());
    let body_len = u32::from_le_bytes(body[9..13].try_into().unwrap()) as usize;
    if body.len() - 13 < body_len {
        return Err(WcolError::from("truncated projection body"));
    }
    let columns = decode_projection_columns(&body[13..13 + body_len])?;
    let rows = columns.first().map(decoded_rows).unwrap_or(0);
    let mut batch = OwnedWcolBatch::empty();
    batch.set_scope(scope);
    for col in columns {
        match col {
            DecodedProjectionColumn::I64 { id, values, nulls } => {
                batch.push_i64_column(id, values, nulls);
            }
            DecodedProjectionColumn::F64 { id, values, nulls } => {
                batch.push_f64_column(id, values, nulls);
            }
            DecodedProjectionColumn::Utf8 {
                id,
                offsets,
                bytes,
                nulls,
            } => {
                batch.push_utf8_column(id, offsets, bytes, nulls);
            }
            DecodedProjectionColumn::DictId { id, ids, nulls } => {
                let dictionary = dicts.get(&(scope, id)).cloned().ok_or_else(|| {
                    WcolError::msg(format!(
                        "dictionary frame missing for column {id} in scope {scope}"
                    ))
                })?;
                batch.push_dict_id_column(id, ids, nulls, dictionary);
            }
        }
    }
    batch.set_row_count(rows)?;
    Ok(batch)
}

fn decoded_rows(col: &DecodedProjectionColumn) -> usize {
    match col {
        DecodedProjectionColumn::I64 { values, .. } => values.len(),
        DecodedProjectionColumn::F64 { values, .. } => values.len(),
        DecodedProjectionColumn::DictId { ids, .. } => ids.len(),
        DecodedProjectionColumn::Utf8 { nulls, .. } => nulls.len(),
    }
}

pub fn projection_body_from_owned_batch(batch: &OwnedWcolBatch) -> WcolResult<Vec<u8>> {
    let mut col_view = Vec::new();
    let view = batch.view_with(&mut col_view);
    let mut columns = Vec::with_capacity(view.columns.len());
    for col in view.columns {
        columns.push(match col {
            crate::stream::WcolColumn::I64 {
                column,
                values,
                nulls,
            } => ProjectionColumn::I64 {
                id: *column,
                values,
                nulls,
            },
            crate::stream::WcolColumn::F64 {
                column,
                values,
                nulls,
            } => ProjectionColumn::F64 {
                id: *column,
                values,
                nulls,
            },
            crate::stream::WcolColumn::DictId {
                column, ids, nulls, ..
            } => ProjectionColumn::DictId {
                id: *column,
                ids,
                nulls,
            },
            crate::stream::WcolColumn::Utf8 {
                column,
                offsets,
                bytes,
                nulls,
            } => ProjectionColumn::Utf8 {
                id: *column,
                offsets,
                bytes,
                nulls,
            },
        });
    }
    encode_projection_columns(&columns)
}
