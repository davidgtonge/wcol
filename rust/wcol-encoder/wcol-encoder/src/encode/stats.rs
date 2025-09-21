use crate::constants::PRESENCE_MAX_VALUES;
use crate::types::{ColumnBuffer, ColumnSpec, ColumnValues};
use crate::utils::is_valid;

pub(crate) fn compute_page_stats(
    col: &ColumnSpec,
    buffer: &ColumnBuffer,
    rows: usize,
) -> (f64, f64, u64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut presence = 0u64;

    match &buffer.values {
        ColumnValues::Int(values) => {
            for (i, &val) in values.iter().enumerate().take(rows) {
                if !is_valid(&buffer.nulls, i) {
                    continue;
                }
                let mut value = val as f64;
                if col.scale != 0 {
                    value /= col.scale as f64;
                }
                if !value.is_finite() {
                    continue;
                }
                if value < min {
                    min = value;
                }
                if value > max {
                    max = value;
                }
            }
        }
        ColumnValues::Float(values) => {
            for (i, &val) in values.iter().enumerate().take(rows) {
                if !is_valid(&buffer.nulls, i) {
                    continue;
                }
                let mut value = val;
                if col.scale != 0 {
                    value /= col.scale as f64;
                }
                if !value.is_finite() {
                    continue;
                }
                if value < min {
                    min = value;
                }
                if value > max {
                    max = value;
                }
            }
        }
        ColumnValues::Bool(values) => {
            for (i, &val) in values.iter().enumerate().take(rows) {
                if !is_valid(&buffer.nulls, i) {
                    continue;
                }
                let value = if val { 1.0 } else { 0.0 };
                if value < min {
                    min = value;
                }
                if value > max {
                    max = value;
                }
            }
        }
        ColumnValues::Dict(values) => {
            if col.dict_values.len() <= PRESENCE_MAX_VALUES {
                for (i, &val) in values.iter().enumerate().take(rows) {
                    if !is_valid(&buffer.nulls, i) {
                        continue;
                    }
                    let id = val as usize;
                    if id < PRESENCE_MAX_VALUES {
                        presence |= 1u64 << id;
                    }
                }
            }
        }
        ColumnValues::String(_) => {}
    }

    if !min.is_finite() || !max.is_finite() {
        min = 0.0;
        max = 0.0;
    }

    (min, max, presence)
}
