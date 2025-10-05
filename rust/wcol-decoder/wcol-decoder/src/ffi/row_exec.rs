use rustc_hash::FxHashMap;

use crate::constants::{FLAG_DICT, FLAG_NULLABLE, TYPE_STRING};
use crate::decode::ensure_decoded;
use crate::ffi::{
    ensure_row_order_lex_ranks, push_candidate, read_order_key, NullBitmapCache, OrderCol,
};
use crate::runtime::iter_mask;
use crate::types::{Column, ColumnData, Plan, RowCandidate, Runtime};

pub(crate) fn execute_row_phase(
    plan: &mut Plan,
    runtime: &mut Runtime,
    schema: &[Column],
    data_pages: &FxHashMap<u32, Vec<u8>>,
    decoded: &mut FxHashMap<u32, ColumnData>,
    null_cache: &mut NullBitmapCache<'_>,
    combined: &[u32],
    combined_full: bool,
    rows_in_chunk: usize,
    chunk_id: u32,
    rows_per_chunk: u32,
) -> Result<(), i32> {
    if plan.limit == 0 {
        return Ok(());
    }
    let row_take = (plan.limit as usize).saturating_add(plan.offset as usize);
    if !plan.row_order_by.is_empty() {
        let mut order_col_ids: Vec<u32> = Vec::new();
        for &col_id in plan.row_order_by.iter().take(2) {
            if !order_col_ids.contains(&col_id) {
                order_col_ids.push(col_id);
            }
        }
        ensure_row_order_lex_ranks(plan, runtime, schema, &order_col_ids);
        for &col_id in &order_col_ids {
            let col = match schema.get(col_id as usize) {
                Some(c) => c,
                None => continue,
            };
            let data_raw = match data_pages.get(&col_id) {
                Some(p) => p,
                None => continue,
            };
            ensure_decoded(
                col,
                data_raw,
                decoded,
                rows_in_chunk,
                runtime,
                Some(&mut plan.timing),
            )?;
        }

        let runtime_ro: &Runtime = &*runtime;
        let mut order_cols: Vec<OrderCol<'_>> = Vec::with_capacity(plan.row_order_by.len());
        for &col_id in plan.row_order_by.iter().take(2) {
            let col = match schema.get(col_id as usize) {
                Some(c) => c,
                None => continue,
            };
            let data_page = match decoded.get(&col_id) {
                Some(d) => d,
                None => continue,
            };
            let nulls = if (col.flags & FLAG_NULLABLE) != 0 {
                null_cache.for_col(col_id)
            } else {
                None
            };
            let dict = if (col.flags & FLAG_DICT) != 0 || col.logical_type == TYPE_STRING {
                runtime_ro.dicts.get(&col.dict_id)
            } else {
                None
            };
            let lex_ranks = if col.logical_type == TYPE_STRING && (col.flags & FLAG_DICT) != 0 {
                plan.row_order_lex_ranks
                    .get(&col.dict_id)
                    .map(|v| v.as_slice())
            } else {
                None
            };
            order_cols.push(OrderCol {
                col,
                data: data_page,
                nulls,
                dict,
                lex_ranks,
            });
        }
        let mut consider_row = |row: usize| {
            let k1 = match order_cols.first() {
                Some(oc) => read_order_key(oc, row),
                None => return,
            };
            let k2 = order_cols.get(1).map(|oc| read_order_key(oc, row));

            let global = chunk_id as u64 * rows_per_chunk as u64 + row as u64;
            let candidate = RowCandidate {
                k1,
                k2,
                row_id: global,
            };
            push_candidate(&mut plan.row_heap, candidate, row_take);
        };

        if combined_full {
            for row in 0..rows_in_chunk {
                consider_row(row);
            }
        } else {
            for row in iter_mask(combined, rows_in_chunk) {
                consider_row(row);
            }
        }
    } else if plan.rows.len() < row_take {
        let remaining = row_take - plan.rows.len();
        if combined_full {
            for row in 0..rows_in_chunk {
                if plan.rows.len() >= row_take {
                    break;
                }
                let global = chunk_id as u64 * rows_per_chunk as u64 + row as u64;
                plan.rows.push(global);
            }
        } else {
            for row in iter_mask(combined, rows_in_chunk).take(remaining) {
                let global = chunk_id as u64 * rows_per_chunk as u64 + row as u64;
                plan.rows.push(global);
            }
        }
    }

    Ok(())
}
