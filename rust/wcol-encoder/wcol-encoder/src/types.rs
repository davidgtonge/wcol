use std::collections::{HashMap, HashSet};

use crate::constants::{ROWS_PER_CHUNK, SCALE_CANDIDATE_LEN};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnKind {
    String,
    Boolean,
    Int,
    Float,
}

pub(crate) struct ColumnSpec {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) kind: ColumnKind,
    pub(crate) nullable: bool,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) f32_ok: bool,
    pub(crate) scale_candidates: u32,
    pub(crate) scaled_min: [i64; SCALE_CANDIDATE_LEN],
    pub(crate) scaled_max: [i64; SCALE_CANDIDATE_LEN],
    pub(crate) unsafe_int: bool,
    pub(crate) dict_map: Option<HashMap<String, u32>>,
    pub(crate) dict_values: Vec<String>,
    pub(crate) num_dict_values: Option<HashSet<i64>>,
    pub(crate) num_dict: bool,
    pub(crate) float_int_ok: bool,
    pub(crate) float_int_min: i64,
    pub(crate) float_int_max: i64,
    pub(crate) logical_type: u8,
    pub(crate) physical_type: u8,
    pub(crate) flags: u8,
    pub(crate) encoding: u8,
    pub(crate) dict_id: u32,
    pub(crate) dict_index_width: u8,
    pub(crate) scale: i32,
    /// When set, string values missing from `dict_map` encode to this id (cap overflow).
    pub(crate) other_dict_id: Option<u32>,
}

pub(crate) enum ColumnValues {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Bool(Vec<bool>),
    Dict(Vec<u32>),
    String(Vec<String>),
}

pub(crate) struct ColumnBuffer {
    pub(crate) values: ColumnValues,
    pub(crate) nulls: Vec<u8>,
    pub(crate) has_nulls: bool,
    pub(crate) null_count: u32,
    pub(crate) empties: Vec<u8>,
    pub(crate) empty_count: u32,
}

pub(crate) struct ColumnPage {
    pub(crate) data_raw_len: u32,
    pub(crate) data_comp: Vec<u8>,
    pub(crate) null_raw_len: u32,
    pub(crate) null_comp: Option<Vec<u8>>,
    pub(crate) empty_mode: u8,
    pub(crate) empty_count: u32,
    pub(crate) empty_raw_len: u32,
    pub(crate) empty_comp: Option<Vec<u8>>,
    pub(crate) data_off: u64,
    pub(crate) null_off: u64,
    pub(crate) empty_off: u64,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) presence: u64,
}

pub(crate) struct ChunkPages {
    pub(crate) columns: Vec<ColumnPage>,
}

#[derive(Clone, Copy)]
pub(crate) struct ColumnTotals {
    pub(crate) raw_data: u64,
    pub(crate) comp_data: u64,
    pub(crate) raw_null: u64,
    pub(crate) comp_null: u64,
}

pub(crate) struct IndexLayout {
    pub(crate) data_off: u64,
    pub(crate) toc: Vec<u64>,
    pub(crate) index_blocks: Vec<Vec<u8>>,
    pub(crate) index_len: u64,
}

impl ColumnBuffer {
    pub(crate) fn new(col: &ColumnSpec) -> Self {
        let values = match col.kind {
            ColumnKind::String => {
                if col.dict_map.is_none() {
                    ColumnValues::String(Vec::new())
                } else {
                    ColumnValues::Dict(Vec::new())
                }
            }
            ColumnKind::Boolean => ColumnValues::Bool(Vec::new()),
            ColumnKind::Int => ColumnValues::Int(Vec::new()),
            ColumnKind::Float => ColumnValues::Float(Vec::new()),
        };
        let nulls = vec![0u8; ROWS_PER_CHUNK.div_ceil(8)];
        let empties = vec![0u8; ROWS_PER_CHUNK.div_ceil(8)];
        Self {
            values,
            nulls,
            has_nulls: false,
            null_count: 0,
            empties,
            empty_count: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.values {
            ColumnValues::Int(values) => values.len(),
            ColumnValues::Float(values) => values.len(),
            ColumnValues::Bool(values) => values.len(),
            ColumnValues::Dict(values) => values.len(),
            ColumnValues::String(values) => values.len(),
        }
    }

    pub(crate) fn reset(&mut self) {
        match &mut self.values {
            ColumnValues::Int(values) => values.clear(),
            ColumnValues::Float(values) => values.clear(),
            ColumnValues::Bool(values) => values.clear(),
            ColumnValues::Dict(values) => values.clear(),
            ColumnValues::String(values) => values.clear(),
        }
        self.nulls.fill(0);
        self.has_nulls = false;
        self.null_count = 0;
        self.empties.fill(0);
        self.empty_count = 0;
    }
}

impl ColumnBuffer {
    pub(crate) fn mark_null(&mut self) {
        self.has_nulls = true;
        self.null_count += 1;
    }
}
