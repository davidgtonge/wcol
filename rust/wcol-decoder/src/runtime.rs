use crate::constants::*;
use crate::types::*;

pub(crate) type Possible = crate::query::filter::Possible;

#[allow(unused_imports)]
pub(crate) use crate::query::{
    agg::{aggregate_column, merge_agg, merge_agg_by_kind, update_agg},
    compare::{cmp_f64, cmp_i32, cmp_i64, cmp_u32},
    filter::{
        build_filter_mask, build_like_id_set, build_single_mask, eval_possible, filter_possible,
    },
    group::{
        build_group_key, build_group_key_materialized_with_runtime, build_group_key_with_runtime,
        read_distinct_key, read_key_value, read_value_f64, MaterializedGroupKey,
    },
    group_dict_hist::{hist_count_dict_column, plan_uses_group_dict_histogram},
    hll::{
        hll_aggregate_column, hll_aggregate_dict_ids, hll_error_estimate, hll_estimate, hll_merge,
        hll_new_default,
    },
    mask::{
        bitmap_is_all_valid, combine_masks, get_bit, is_valid, iter_mask, mask_and, mask_count,
        mask_from_bitmap, mask_is_full, mask_is_zero, mask_not, mask_or, set_bit,
    },
    plan::plan_required_columns,
    scale::{scale_f64_to_i64, scale_int_value, scaled_rhs_pair},
};

pub(crate) fn build_filter_mask_pipeline(
    col: &Column,
    data: &ColumnData,
    filter: &Filter,
    rows: usize,
    runtime: Option<&Runtime>,
    null_bitmap: Option<&[u8]>,
) -> Result<Vec<u32>, ()> {
    let mask = build_filter_mask(col, data, filter, rows, runtime);
    maybe_apply_validity_mask(mask, null_bitmap, rows)
}

pub(crate) fn build_empty_string_mask(
    entry: &IndexEntry,
    col: &Column,
    filter: &Filter,
    rows: usize,
    empty_bitmap: Option<&[u8]>,
    null_bitmap: Option<&[u8]>,
) -> Option<Vec<u32>> {
    if col.physical_type != TYPE_STRING {
        return None;
    }
    if filter.value_str.as_deref() != Some("") {
        return None;
    }
    if filter.op != OP_EQ && filter.op != OP_NEQ {
        return None;
    }

    match entry.empty_mode {
        EMPTY_MODE_ALL_ZERO => {
            if filter.op == OP_EQ {
                Some(vec![0u32; MASK_WORDS])
            } else if entry.null_raw_len > 0 {
                let nulls = null_bitmap?;
                mask_from_bitmap(nulls, rows)
            } else {
                Some(full_mask(rows))
            }
        }
        EMPTY_MODE_ALL_ONE => {
            if filter.op == OP_EQ {
                Some(full_mask(rows))
            } else {
                Some(vec![0u32; MASK_WORDS])
            }
        }
        EMPTY_MODE_MIXED => {
            let empty_raw = empty_bitmap?;
            let empty_mask = mask_from_bitmap(empty_raw, rows)?;
            if filter.op == OP_EQ {
                Some(empty_mask)
            } else if entry.null_raw_len > 0 {
                let nulls = null_bitmap?;
                let valid = mask_from_bitmap(nulls, rows)?;
                Some(mask_and(&valid, &mask_not(&empty_mask)))
            } else {
                Some(mask_and(&full_mask(rows), &mask_not(&empty_mask)))
            }
        }
        _ => None,
    }
}

pub(crate) fn rows_in_chunk(total_rows: u64, rows_per_chunk: usize, chunk_id: usize) -> usize {
    let start = chunk_id * rows_per_chunk;
    if start >= total_rows as usize {
        return 0;
    }
    let remaining = total_rows as usize - start;
    remaining.min(rows_per_chunk)
}

pub(crate) fn full_mask(rows: usize) -> Vec<u32> {
    let mut mask = vec![0xffff_ffff; MASK_WORDS];
    clear_tail(&mut mask, rows);
    mask
}

pub(crate) fn clear_tail(mask: &mut [u32], rows: usize) {
    let full_words = rows / 32;
    let tail_bits = rows % 32;
    if tail_bits == 0 {
        for word in mask.iter_mut().take(MASK_WORDS).skip(full_words) {
            *word = 0;
        }
        return;
    }
    if full_words < MASK_WORDS {
        let keep = (1u32 << tail_bits) - 1;
        mask[full_words] &= keep;
    }
    for word in mask.iter_mut().take(MASK_WORDS).skip(full_words + 1) {
        *word = 0;
    }
}

pub(crate) fn maybe_apply_validity_mask(
    mask: Vec<u32>,
    null_bitmap: Option<&[u8]>,
    rows: usize,
) -> Result<Vec<u32>, ()> {
    let Some(null_raw) = null_bitmap else {
        return Ok(mask);
    };
    if bitmap_is_all_valid(null_raw, rows) {
        return Ok(mask);
    }
    let valid = mask_from_bitmap(null_raw, rows).ok_or(())?;
    Ok(mask_and(&mask, &valid))
}

pub(crate) fn nullable_row_bitmap<'a>(
    null_bitmap: Option<&'a [u8]>,
    rows: usize,
) -> Option<&'a [u8]> {
    null_bitmap.filter(|bitmap| !bitmap_is_all_valid(bitmap, rows))
}

pub(crate) fn dict_value_bytes(dict: &Dictionary, value_id: usize) -> Option<&[u8]> {
    if !dict.offsets.is_empty() {
        let start = *dict.offsets.get(value_id)? as usize;
        let end = *dict.offsets.get(value_id + 1)? as usize;
        return dict.blob.get(start..end);
    }
    dict.values.get(value_id).map(|value| value.as_bytes())
}

/// Resolve a string literal to a dictionary id (lookup table, blob scan, or sorted values).
pub(crate) fn dict_string_id(dict: &Dictionary, s: &str) -> Option<u32> {
    if let Some(&id) = dict.lookup.get(s) {
        return Some(id);
    }
    if !dict.offsets.is_empty() {
        let needle = s.as_bytes();
        for idx in 0..dict.offsets.len().saturating_sub(1) {
            let start = dict.offsets[idx] as usize;
            let end = dict.offsets[idx + 1] as usize;
            if dict.blob.get(start..end) == Some(needle) {
                return Some(idx as u32);
            }
        }
        return None;
    }
    if dict.lookup.is_empty() {
        return dict
            .values
            .binary_search_by(|v| v.as_bytes().cmp(s.as_bytes()))
            .ok()
            .map(|pos| pos as u32);
    }
    None
}

// Aggregate key encoding (supports simple SUM(col +/- small_const) variants)
// bits: [ offset:i8 | col_id:u16 | kind:u8 ]
pub(crate) fn agg_key_make(col_id: u32, kind: u8, offset: i8) -> u32 {
    ((offset as u8 as u32) << 24) | (((col_id & 0xffff) as u32) << 8) | (kind as u32)
}

pub(crate) fn agg_key_col_id(key: u32) -> u32 {
    (key >> 8) & 0xffff
}

pub(crate) fn agg_key_kind(key: u32) -> u8 {
    (key & 0xff) as u8
}

pub(crate) fn agg_key_offset(key: u32) -> i8 {
    ((key >> 24) as u8) as i8
}
