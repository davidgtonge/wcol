use crate::query::mask::is_valid;
use crate::query::scale::scale_int_value;
use crate::query::scan::{for_each_row, for_each_value};
use crate::types::{AggState, Column, ColumnData};

#[inline]
fn agg_int_column<T, F>(
    values: &[T],
    mask: &[u32],
    rows: usize,
    scale_factor: Option<f64>,
    cast: F,
    state: &mut AggState,
) where
    T: Copy,
    F: Fn(T) -> f64,
{
    for_each_value(values, mask, rows, |val| {
        let mut value = cast(val);
        if let Some(factor) = scale_factor {
            value *= factor;
        }
        update_agg(state, value);
    });
}

pub(crate) fn aggregate_column(
    col: &Column,
    data: &ColumnData,
    mask: &[u32],
    rows: usize,
) -> AggState {
    let mut state = AggState {
        sum: 0.0,
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
        count: 0,
    };
    let scale_factor = if col.scale != 0 {
        Some(scale_int_value(1.0, col.scale))
    } else {
        None
    };

    match data {
        ColumnData::U8(values) => agg_int_column(values, mask, rows, scale_factor, |v: u8| v as f64, &mut state),
        ColumnData::U16(values) => agg_int_column(values, mask, rows, scale_factor, |v: u16| v as f64, &mut state),
        ColumnData::I8(values) => agg_int_column(values, mask, rows, scale_factor, |v: i8| v as f64, &mut state),
        ColumnData::I16(values) => agg_int_column(values, mask, rows, scale_factor, |v: i16| v as f64, &mut state),
        ColumnData::I32(values) => agg_int_column(values, mask, rows, scale_factor, |v: i32| v as f64, &mut state),
        ColumnData::I64(values) => agg_int_column(values, mask, rows, scale_factor, |v: i64| v as f64, &mut state),
        ColumnData::U32(values) => agg_int_column(values, mask, rows, scale_factor, |v: u32| v as f64, &mut state),
        ColumnData::F64(values) => {
            for_each_value(values, mask, rows, |value| {
                if !value.is_nan() {
                    update_agg(&mut state, value);
                }
            });
        }
        ColumnData::Bool(values) => {
            for_each_row(mask, rows, |row| {
                let value = if is_valid(values, row) { 1.0 } else { 0.0 };
                update_agg(&mut state, value);
            });
        }
    }
    state
}

pub(crate) fn merge_agg(target: &mut AggState, partial: AggState) {
    if partial.count == 0 {
        return;
    }
    target.sum += partial.sum;
    target.count += partial.count;
    if partial.min < target.min {
        target.min = partial.min;
    }
    if partial.max > target.max {
        target.max = partial.max;
    }
}

/// Merge partial into target only for the fields relevant to agg_kind (MIN/MAX/SUM/AVG/COUNT_STAR).
pub(crate) fn merge_agg_by_kind(target: &mut AggState, partial: AggState, kind: u8) {
    if partial.count == 0 {
        return;
    }
    use crate::constants::{
        AGG_KIND_AVG, AGG_KIND_COUNT_STAR, AGG_KIND_MAX, AGG_KIND_MIN, AGG_KIND_SUM,
    };
    target.count += partial.count;
    match kind {
        AGG_KIND_COUNT_STAR => {}
        AGG_KIND_MIN => {
            if partial.min < target.min {
                target.min = partial.min;
            }
        }
        AGG_KIND_MAX => {
            if partial.max > target.max {
                target.max = partial.max;
            }
        }
        AGG_KIND_SUM | AGG_KIND_AVG | _ => {
            target.sum += partial.sum;
            if partial.min < target.min {
                target.min = partial.min;
            }
            if partial.max > target.max {
                target.max = partial.max;
            }
        }
    }
}

pub(crate) fn update_agg(state: &mut AggState, value: f64) {
    state.sum += value;
    state.count += 1;
    if value < state.min {
        state.min = value;
    }
    if value > state.max {
        state.max = value;
    }
}
