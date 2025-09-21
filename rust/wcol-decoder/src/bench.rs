use crate::query::{agg, filter, group, mask};
use crate::types::{AggState, Column, ColumnData};

pub fn rows_per_chunk() -> usize {
    crate::constants::ROWS_PER_CHUNK
}

pub fn mask_words() -> usize {
    crate::constants::MASK_WORDS
}

pub fn op_eq() -> u8 {
    crate::constants::OP_EQ
}

pub fn op_neq() -> u8 {
    crate::constants::OP_NEQ
}

pub fn op_lt() -> u8 {
    crate::constants::OP_LT
}

pub fn op_lte() -> u8 {
    crate::constants::OP_LTE
}

pub fn op_gt() -> u8 {
    crate::constants::OP_GT
}

pub fn op_gte() -> u8 {
    crate::constants::OP_GTE
}

pub fn op_between() -> u8 {
    crate::constants::OP_BETWEEN
}

fn make_column(scale: i32) -> Column {
    Column {
        id: 0,
        name: "bench".to_string(),
        logical_type: 0,
        physical_type: 0,
        flags: 0,
        encoding: 0,
        dict_id: 0,
        dict_index_width: 0,
        scale,
    }
}

pub struct BenchColumn {
    col: Column,
    data: ColumnData,
}

impl BenchColumn {
    pub fn i32(values: Vec<i32>, scale: i32) -> Self {
        Self {
            col: make_column(scale),
            data: ColumnData::I32(values),
        }
    }

    pub fn u32(values: Vec<u32>, scale: i32) -> Self {
        Self {
            col: make_column(scale),
            data: ColumnData::U32(values),
        }
    }

    pub fn f64(values: Vec<f64>, scale: i32) -> Self {
        Self {
            col: make_column(scale),
            data: ColumnData::F64(values),
        }
    }

    pub fn build_mask(&self, op: u8, value: f64, value2: f64, rows: usize) -> Vec<u32> {
        filter::build_single_mask(&self.col, &self.data, op, value, value2, rows)
    }

    pub fn aggregate_sum(&self, mask_bits: &[u32], rows: usize) -> f64 {
        let AggState { sum, .. } = agg::aggregate_column(&self.col, &self.data, mask_bits, rows);
        sum
    }

    pub fn read_value_f64(&self, row: usize) -> f64 {
        group::read_value_f64(&self.col, &self.data, row)
    }
}

pub fn build_group_key(
    a: &BenchColumn,
    b: Option<&BenchColumn>,
    row: usize,
) -> crate::types::GroupKey {
    match b {
        Some(other) => group::build_group_key(&[(&a.col, &a.data), (&other.col, &other.data)], row),
        None => group::build_group_key(&[(&a.col, &a.data)], row),
    }
}

pub fn mask_and(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask::mask_and(a, b)
}

pub fn mask_or(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask::mask_or(a, b)
}

pub fn mask_not(mask_bits: &[u32]) -> Vec<u32> {
    mask::mask_not(mask_bits)
}

pub fn mask_count(mask_bits: &[u32]) -> u32 {
    mask::mask_count(mask_bits)
}

pub fn mask_is_zero(mask_bits: &[u32]) -> bool {
    mask::mask_is_zero(mask_bits)
}

/// Count set bits by iterating (same traversal as sparse aggregate).
pub fn iter_mask_count(mask_bits: &[u32], rows: usize) -> usize {
    mask::iter_mask(mask_bits, rows).count()
}
