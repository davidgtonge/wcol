use anyhow::{bail, Result};
use sav_to_cbor::parser::streaming_nom::{ColumnRows, ColumnarColumn};

use crate::constants::{ROWS_PER_CHUNK, TYPE_STRING};
use crate::types::{ChunkPages, ColumnBuffer, ColumnSpec, ColumnValues};
use crate::utils::{scaled_f64, set_bit};

use super::page::finalize_chunk;

pub(crate) fn encode_sav_chunks(
    columns: &[ColumnSpec],
    data: &[ColumnarColumn],
    total_rows: usize,
) -> Result<Vec<ChunkPages>> {
    let mut buffers: Vec<ColumnBuffer> = columns.iter().map(ColumnBuffer::new).collect();
    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < total_rows {
        let take = (total_rows - offset).min(ROWS_PER_CHUNK);
        for (idx, col) in data.iter().enumerate() {
            append_sav_column(&columns[idx], &mut buffers[idx], col, offset, take)?;
        }
        chunks.push(finalize_chunk(take, columns, &mut buffers)?);
        offset += take;
    }
    Ok(chunks)
}

fn append_sav_column(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    data: &ColumnarColumn,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    match (&data.rows, &mut buffer.values) {
        (ColumnRows::Numeric(rows), ColumnValues::Int(values)) => {
            for idx in 0..len {
                let row = start + idx;
                let value = rows
                    .get(row)
                    .ok_or_else(|| anyhow::anyhow!("Row index {} out of bounds", row))?;
                match value {
                    Some(v) => {
                        let mut value = *v;
                        if col.scale != 0 {
                            value = scaled_f64(value, col.scale);
                        }
                        values.push(value as i64);
                        set_bit(&mut buffer.nulls, base + idx);
                    }
                    None => {
                        values.push(0);
                        buffer.mark_null();
                    }
                }
            }
        }
        (ColumnRows::Numeric(rows), ColumnValues::Float(values)) => {
            for idx in 0..len {
                let row = start + idx;
                let value = rows
                    .get(row)
                    .ok_or_else(|| anyhow::anyhow!("Row index {} out of bounds", row))?;
                match value {
                    Some(v) => {
                        let mut value = *v;
                        if col.scale != 0 {
                            value = scaled_f64(value, col.scale);
                        }
                        values.push(value);
                        set_bit(&mut buffer.nulls, base + idx);
                    }
                    None => {
                        values.push(0.0);
                        buffer.mark_null();
                    }
                }
            }
        }
        (ColumnRows::Indexed(rows), ColumnValues::Dict(values)) => {
            let dict_len = col.dict_values.len();
            for idx in 0..len {
                let row = start + idx;
                let value = rows
                    .get(row)
                    .ok_or_else(|| anyhow::anyhow!("Row index {} out of bounds", row))?;
                match value {
                    Some(v) => {
                        if *v >= dict_len {
                            bail!(
                                "Dictionary index {} out of range for column {}",
                                v,
                                col.name
                            );
                        }
                        values.push(*v as u32);
                        set_bit(&mut buffer.nulls, base + idx);
                        if col.physical_type == TYPE_STRING {
                            if let Some(value) = col.dict_values.get(*v) {
                                if value.is_empty() {
                                    set_bit(&mut buffer.empties, base + idx);
                                    buffer.empty_count += 1;
                                }
                            }
                        }
                    }
                    None => {
                        values.push(0);
                        buffer.mark_null();
                    }
                }
            }
        }
        (ColumnRows::Indexed(rows), ColumnValues::String(values)) => {
            let dict_values = data
                .values
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Missing values for indexed column {}", col.name))?;
            for idx in 0..len {
                let row = start + idx;
                let value = rows
                    .get(row)
                    .ok_or_else(|| anyhow::anyhow!("Row index {} out of bounds", row))?;
                match value {
                    Some(v) => {
                        let value = dict_values.get(*v).ok_or_else(|| {
                            anyhow::anyhow!(
                                "Dictionary index {} out of range for column {}",
                                v,
                                col.name
                            )
                        })?;
                        values.push(value.clone());
                        set_bit(&mut buffer.nulls, base + idx);
                        if value.is_empty() {
                            set_bit(&mut buffer.empties, base + idx);
                            buffer.empty_count += 1;
                        }
                    }
                    None => {
                        values.push(String::new());
                        buffer.mark_null();
                    }
                }
            }
        }
        _ => {
            bail!("Column kind mismatch for {}", col.name);
        }
    }
    Ok(())
}
