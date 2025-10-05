//! Late materialization for SELECT projection (after row ids are finalized).

use rustc_hash::FxHashMap;

use crate::constants::{FLAG_DICT, FLAG_NULLABLE, TYPE_BOOL, TYPE_STRING};
use crate::decode::{dict_index_at, ensure_decoded};
use crate::exec::{decompress_pages, PageMaps};
use crate::ffi::{
    lock_plans_timed, lock_runtimes_timed, write_f64, write_u32, write_u8, NullBitmapCache, PLANS,
};
use crate::query::group::read_value_f64;
use crate::query::mask::is_valid;
use crate::runtime::rows_in_chunk;
use crate::constants::{NULL_SENTINEL, PAGE_KIND_DATA, PAGE_KIND_NULL, PAGE_REQ_WORDS};
use crate::parse::parse_chunk_index;
use crate::types::{
    Column, ColumnData, IndexEntry, PageDesc, Plan, ProjectionColumnBuf, RowProjectionBuf, Runtime,
    PROJ_KIND_BOOL, PROJ_KIND_DICT_ID, PROJ_KIND_F64, PROJ_MAGIC,
};

/// Returned when `runtime.index_cache` has no entry for the chunk (TS should read index bytes).
pub(crate) const MATERIALIZE_INDEX_CACHE_MISS: isize = -20;

pub(crate) fn projection_column_kind(col: &Column) -> u8 {
    if (col.flags & FLAG_DICT) != 0 || col.logical_type == TYPE_STRING {
        PROJ_KIND_DICT_ID
    } else if col.logical_type == TYPE_BOOL {
        PROJ_KIND_BOOL
    } else {
        PROJ_KIND_F64
    }
}

pub(crate) fn init_row_projection(plan: &mut Plan, schema: &[Column]) -> i32 {
    if plan.select_cols.is_empty() {
        plan.row_projection.clear();
        return 0;
    }
    let row_count = plan.rows.len();
    let mut buf = RowProjectionBuf {
        row_count,
        col_ids: Vec::with_capacity(plan.select_cols.len()),
        kinds: Vec::with_capacity(plan.select_cols.len()),
        columns: Vec::with_capacity(plan.select_cols.len()),
    };
    for &col_id in &plan.select_cols {
        let col = match schema.get(col_id as usize) {
            Some(c) => c,
            None => return -7,
        };
        let kind = projection_column_kind(col);
        buf.col_ids.push(col_id);
        buf.kinds.push(kind);
        let col_buf = match kind {
            PROJ_KIND_DICT_ID => ProjectionColumnBuf::DictId {
                values: vec![0; row_count],
                nulls: vec![0; row_count],
            },
            PROJ_KIND_BOOL => ProjectionColumnBuf::Bool {
                values: vec![0; row_count],
                nulls: vec![0; row_count],
            },
            _ => ProjectionColumnBuf::F64 {
                values: vec![0.0; row_count],
                nulls: vec![0; row_count],
            },
        };
        buf.columns.push(col_buf);
    }
    plan.row_projection = buf;
    0
}

fn row_present(nulls: Option<&[u8]>, row: usize) -> bool {
    match nulls {
        Some(bitmap) => is_valid(bitmap, row),
        None => true,
    }
}

fn gather_column(
    out: &mut ProjectionColumnBuf,
    kind: u8,
    col: &Column,
    data: &ColumnData,
    nulls: Option<&[u8]>,
    rows_in_chunk: usize,
    row_count: usize,
    local_rows: &[u32],
    dst_rows: &[u32],
) {
    match kind {
        PROJ_KIND_DICT_ID => {
            let ProjectionColumnBuf::DictId { values, nulls: out_nulls } = out else {
                return;
            };
            for (&local_row, &dst) in local_rows.iter().zip(dst_rows.iter()) {
                let local = local_row as usize;
                let dst = dst as usize;
                if dst >= row_count || local >= rows_in_chunk {
                    continue;
                }
                let present = row_present(nulls, local);
                out_nulls[dst] = if present { 1 } else { 0 };
                if present {
                    values[dst] = dict_index_at(data, local).unwrap_or(0) as u32;
                }
            }
        }
        PROJ_KIND_BOOL => {
            let ProjectionColumnBuf::Bool { values, nulls: out_nulls } = out else {
                return;
            };
            for (&local_row, &dst) in local_rows.iter().zip(dst_rows.iter()) {
                let local = local_row as usize;
                let dst = dst as usize;
                if dst >= row_count || local >= rows_in_chunk {
                    continue;
                }
                let present = row_present(nulls, local);
                out_nulls[dst] = if present { 1 } else { 0 };
                if present {
                    values[dst] = match data {
                        ColumnData::Bool(bits) => {
                            if is_valid(bits, local) {
                                1
                            } else {
                                0
                            }
                        }
                        _ => {
                            if read_value_f64(col, data, local) != 0.0 {
                                1
                            } else {
                                0
                            }
                        }
                    };
                }
            }
        }
        _ => {
            let ProjectionColumnBuf::F64 { values, nulls: out_nulls } = out else {
                return;
            };
            for (&local_row, &dst) in local_rows.iter().zip(dst_rows.iter()) {
                let local = local_row as usize;
                let dst = dst as usize;
                if dst >= row_count || local >= rows_in_chunk {
                    continue;
                }
                let present = row_present(nulls, local);
                out_nulls[dst] = if present { 1 } else { 0 };
                if present {
                    values[dst] = read_value_f64(col, data, local);
                }
            }
        }
    }
}

fn materialize_page_descs(
    runtime: &Runtime,
    entries: &[IndexEntry],
    select_cols: &[u32],
) -> Vec<PageDesc> {
    let mut pages = Vec::new();
    for &col_id in select_cols {
        let col_id = col_id as usize;
        if col_id >= runtime.schema.len() || col_id >= entries.len() {
            continue;
        }
        let col = &runtime.schema[col_id];
        let entry = &entries[col_id];
        pages.push(PageDesc {
            kind: PAGE_KIND_DATA,
            col_id: col_id as u32,
            offset: entry.data_off,
            comp_len: entry.data_comp_len,
            raw_len: entry.data_raw_len,
        });
        if (col.flags & FLAG_NULLABLE) != 0
            && entry.null_off != NULL_SENTINEL as u64
            && entry.null_comp_len > 0
        {
            pages.push(PageDesc {
                kind: PAGE_KIND_NULL,
                col_id: col_id as u32,
                offset: entry.null_off,
                comp_len: entry.null_comp_len,
                raw_len: entry.null_raw_len,
            });
        }
    }
    pages
}

unsafe fn write_page_requests(pages: &[PageDesc], out_ptr: *mut u32, out_len: usize) -> isize {
    let needed_words = pages.len() * PAGE_REQ_WORDS;
    let needed_bytes = needed_words * 4;
    if out_len < needed_bytes {
        return -(needed_bytes as isize);
    }
    let out = std::slice::from_raw_parts_mut(out_ptr, out_len / 4);
    let mut cursor = 0;
    for page in pages {
        out[cursor] = page.kind;
        out[cursor + 1] = page.col_id;
        out[cursor + 2] = (page.offset & 0xffff_ffff) as u32;
        out[cursor + 3] = (page.offset >> 32) as u32;
        out[cursor + 4] = page.comp_len;
        out[cursor + 5] = page.raw_len;
        cursor += PAGE_REQ_WORDS;
    }
    (cursor / PAGE_REQ_WORDS) as isize
}

pub(crate) unsafe fn plan_materialize_chunk_inner(
    runtime: &mut Runtime,
    plan: &mut Plan,
    chunk_id: u32,
    descs: &[u32],
    data: &[u8],
    local_rows: &[u32],
    dst_rows: &[u32],
) -> i32 {
    if plan.row_projection.columns.is_empty() {
        return 0;
    }
    if local_rows.len() != dst_rows.len() {
        return -4;
    }
    let header = match runtime.header {
        Some(h) => h,
        None => return -3,
    };
    let schema = runtime.schema.clone();
    let rows_in_chunk = rows_in_chunk(
        header.total_rows,
        header.rows_per_chunk as usize,
        chunk_id as usize,
    );
    if rows_in_chunk == 0 {
        return 0;
    }

    let PageMaps {
        data_pages,
        null_pages,
        empty_pages: _,
    } = match decompress_pages(descs, data, rows_in_chunk) {
        Ok(pm) => pm,
        Err(e) => return e,
    };
    let mut null_cache = NullBitmapCache::new(&null_pages, rows_in_chunk);
    let mut decoded: FxHashMap<u32, ColumnData> = FxHashMap::default();

    for &col_id in &plan.select_cols {
        let col = match schema.get(col_id as usize) {
            Some(c) => c,
            None => continue,
        };
        let data_raw = match data_pages.get(&col_id) {
            Some(p) => p,
            None => continue,
        };
        if ensure_decoded(col, data_raw, &mut decoded, rows_in_chunk, runtime, None).is_err() {
            return -17;
        }
    }

    let row_count = plan.row_projection.row_count;
    for (col_idx, &col_id) in plan.select_cols.iter().enumerate() {
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
        let kind = plan.row_projection.kinds[col_idx];
        let out_col = match plan.row_projection.columns.get_mut(col_idx) {
            Some(c) => c,
            None => continue,
        };
        gather_column(
            out_col,
            kind,
            col,
            data_page,
            nulls,
            rows_in_chunk,
            row_count,
            local_rows,
            dst_rows,
        );
    }
    0
}

pub(crate) fn copy_row_projection(plan: &Plan, out: &mut [u8]) -> usize {
    let buf = &plan.row_projection;
    if buf.columns.is_empty() {
        return 0;
    }
    let col_count = buf.columns.len();
    let header_size = 8 + 4 + 4 + col_count * 16;
    let mut col_data_lens = Vec::with_capacity(col_count);
    for col in &buf.columns {
        let cell_bytes = match col {
            ProjectionColumnBuf::F64 { values, nulls } => values.len() * 8 + nulls.len(),
            ProjectionColumnBuf::DictId { values, nulls } => values.len() * 4 + nulls.len(),
            ProjectionColumnBuf::Bool { values, nulls } => values.len() + nulls.len(),
        };
        col_data_lens.push(cell_bytes);
    }
    let total = header_size + col_data_lens.iter().sum::<usize>();
    if out.len() < total {
        return 0;
    }
    out[0..8].copy_from_slice(PROJ_MAGIC);
    write_u32(out, 8, buf.row_count as u32);
    write_u32(out, 12, col_count as u32);
    let mut cursor = 16;
    let mut data_cursor = header_size;
    for (col_idx, col) in buf.columns.iter().enumerate() {
        write_u32(out, cursor, buf.col_ids[col_idx]);
        cursor += 4;
        write_u8(out, cursor, buf.kinds[col_idx]);
        cursor += 1;
        cursor += 3;
        write_u32(out, cursor, data_cursor as u32);
        cursor += 4;
        let byte_len = col_data_lens[col_idx] as u32;
        write_u32(out, cursor, byte_len);
        cursor += 4;

        match col {
            ProjectionColumnBuf::F64 { values, nulls } => {
                for (i, &v) in values.iter().enumerate() {
                    write_f64(out, data_cursor + i * 8, v);
                }
                let off = data_cursor + values.len() * 8;
                out[off..off + nulls.len()].copy_from_slice(nulls);
            }
            ProjectionColumnBuf::DictId { values, nulls } => {
                for (i, &v) in values.iter().enumerate() {
                    write_u32(out, data_cursor + i * 4, v);
                }
                let off = data_cursor + values.len() * 4;
                out[off..off + nulls.len()].copy_from_slice(nulls);
            }
            ProjectionColumnBuf::Bool { values, nulls } => {
                let off = data_cursor;
                out[off..off + values.len()].copy_from_slice(values);
                out[off + values.len()..off + values.len() + nulls.len()].copy_from_slice(nulls);
            }
        }
        data_cursor += col_data_lens[col_idx];
    }
    total
}

pub(crate) fn row_projection_byte_len(plan: &Plan) -> usize {
    let buf = &plan.row_projection;
    if buf.columns.is_empty() {
        return 0;
    }
    let col_count = buf.columns.len();
    let header_size = 8 + 4 + 4 + col_count * 16;
    let data_size: usize = buf
        .columns
        .iter()
        .map(|col| match col {
            ProjectionColumnBuf::F64 { values, nulls } => values.len() * 8 + nulls.len(),
            ProjectionColumnBuf::DictId { values, nulls } => values.len() * 4 + nulls.len(),
            ProjectionColumnBuf::Bool { values, nulls } => values.len() + nulls.len(),
        })
        .sum();
    header_size + data_size
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_select(handle: u32, ptr: *const u32, len: usize) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        if ptr.is_null() && len > 0 {
            return -4;
        }
        let cols = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(ptr, len).to_vec()
        };
        plan.select_cols = cols;
        plan.row_projection.clear();
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_select_count(handle: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get(&handle) {
        return plan.select_cols.len() as i32;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_projection_begin(handle: u32) -> i32 {
    let mut plans = lock_plans_timed();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let runtime_handle = plan.runtime;
    drop(plans);

    let runtimes = lock_runtimes_timed();
    let runtime = match runtimes.get(&runtime_handle) {
        Some(r) => r,
        None => return -2,
    };
    let schema = runtime.schema.clone();
    drop(runtimes);

    let mut plans = lock_plans_timed();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    init_row_projection(plan, &schema)
}

#[no_mangle]
pub unsafe extern "C" fn plan_materialize_chunk(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    desc_ptr: *const u32,
    desc_len: usize,
    data_ptr: *const u8,
    data_len: usize,
    local_rows_ptr: *const u32,
    local_rows_len: usize,
    dst_rows_ptr: *const u32,
    dst_rows_len: usize,
) -> i32 {
    if local_rows_len != dst_rows_len {
        return -4;
    }
    let descs = if desc_len == 0 {
        &[]
    } else {
        if desc_ptr.is_null() {
            return -4;
        }
        std::slice::from_raw_parts(desc_ptr, desc_len)
    };
    let data = if data_len == 0 {
        &[]
    } else {
        if data_ptr.is_null() {
            return -4;
        }
        std::slice::from_raw_parts(data_ptr, data_len)
    };
    let local_rows = if local_rows_len == 0 {
        &[]
    } else {
        if local_rows_ptr.is_null() {
            return -4;
        }
        std::slice::from_raw_parts(local_rows_ptr, local_rows_len)
    };
    let dst_rows = if dst_rows_len == 0 {
        &[]
    } else {
        if dst_rows_ptr.is_null() {
            return -4;
        }
        std::slice::from_raw_parts(dst_rows_ptr, dst_rows_len)
    };

    let mut runtimes = lock_runtimes_timed();
    let runtime = match runtimes.get_mut(&runtime_handle) {
        Some(r) => r,
        None => return -2,
    };
    let mut plans = lock_plans_timed();
    let plan = match plans.get_mut(&plan_handle) {
        Some(p) => p,
        None => return -1,
    };
    if plan.runtime != runtime_handle {
        return -7;
    }
    plan_materialize_chunk_inner(runtime, plan, chunk_id, descs, data, local_rows, dst_rows)
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_row_projection(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let needed = row_projection_byte_len(plan);
    if needed == 0 {
        return 0;
    }
    if out_len < needed {
        return -(needed as isize);
    }
    if out_ptr.is_null() {
        return -4;
    }
    let out = std::slice::from_raw_parts_mut(out_ptr, out_len);
    let written = copy_row_projection(plan, out);
    written as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_materialize_required_pages_cached(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    out_ptr: *mut u32,
    out_len: usize,
) -> isize {
    let plans = lock_plans_timed();
    let plan = match plans.get(&plan_handle) {
        Some(p) => p,
        None => return -2,
    };
    if plan.runtime != runtime_handle {
        return -7;
    }
    let select_cols = plan.select_cols.clone();
    drop(plans);

    let mut runtimes = lock_runtimes_timed();
    let runtime = match runtimes.get_mut(&runtime_handle) {
        Some(r) => r,
        None => return -1,
    };
    if runtime.header.is_none() || runtime.toc.is_empty() {
        return -3;
    }
    if chunk_id as usize >= runtime.toc.len() {
        return -4;
    }
    let entries = match runtime.index_cache.get(&chunk_id) {
        Some(e) => e.as_slice(),
        None => return MATERIALIZE_INDEX_CACHE_MISS,
    };
    let pages = materialize_page_descs(runtime, entries, &select_cols);
    write_page_requests(&pages, out_ptr, out_len)
}

#[no_mangle]
pub unsafe extern "C" fn plan_materialize_required_pages(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    index_ptr: *const u8,
    index_len: usize,
    index_raw_len: usize,
    out_ptr: *mut u32,
    out_len: usize,
) -> isize {
    let plans = lock_plans_timed();
    let plan = match plans.get(&plan_handle) {
        Some(p) => p,
        None => return -2,
    };
    if plan.runtime != runtime_handle {
        return -7;
    }
    let select_cols = plan.select_cols.clone();
    drop(plans);

    let mut runtimes = lock_runtimes_timed();
    let runtime = match runtimes.get_mut(&runtime_handle) {
        Some(r) => r,
        None => return -1,
    };
    if runtime.header.is_none() || runtime.toc.is_empty() {
        return -3;
    }
    if chunk_id as usize >= runtime.toc.len() {
        return -4;
    }

    let index_bytes = std::slice::from_raw_parts(index_ptr, index_len);
    let decompressed = match lz4_flex::block::decompress(index_bytes, index_raw_len) {
        Ok(data) => data,
        Err(_) => return -5,
    };
    let entries = match parse_chunk_index(&decompressed, runtime.schema.len()) {
        Ok(e) => e,
        Err(_) => return -6,
    };
    runtime.index_cache.insert(chunk_id, entries.clone());

    let pages = materialize_page_descs(runtime, &entries, &select_cols);
    write_page_requests(&pages, out_ptr, out_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, FilterTiming, Plan, PlanTiming, RowProjectionBuf};
    use rustc_hash::FxHashMap;

    #[test]
    fn row_projection_blob_round_trip() {
        let col = Column {
            id: 0,
            name: "x".to_string(),
            logical_type: crate::constants::TYPE_F64,
            physical_type: crate::constants::TYPE_F64,
            flags: 0,
            encoding: 0,
            dict_id: 0,
            dict_index_width: 0,
            scale: 0,
        };
        let mut plan = Plan {
            runtime: 1,
            filters: vec![],
            combine: vec![],
            group_by: None,
            aggregates: vec![],
            limit: 2,
            offset: 0,
            rows: vec![0, 1],
            agg_state: FxHashMap::default(),
            group_state: FxHashMap::default(),
            group_keys: Vec::new(),
            group_key_repr: FxHashMap::default(),
            group_order_by_count: false,
            group_aggs: Vec::new(),
            row_order_by: Vec::new(),
            row_heap: std::collections::BinaryHeap::new(),
            row_order_lex_ranks: FxHashMap::default(),
            hll_state: FxHashMap::default(),
            group_emit_raw: false,
            group_rows_raw_with_keys: Vec::new(),
            group_dict_hist_dict_len: 0,
            group_dict_hist_counts: None,
            group_dict_hist_sums: None,
            select_cols: vec![0],
            row_projection: RowProjectionBuf::default(),
            timing: PlanTiming::default(),
            filter_timing: FilterTiming::default(),
        };
        assert_eq!(init_row_projection(&mut plan, std::slice::from_ref(&col)), 0);
        if let ProjectionColumnBuf::F64 { values, nulls } = &mut plan.row_projection.columns[0] {
            values[0] = 1.5;
            values[1] = 2.5;
            nulls[0] = 1;
            nulls[1] = 1;
        }
        let mut out = vec![0u8; 64];
        let n = copy_row_projection(&plan, &mut out);
        assert!(n > 0);
        assert_eq!(&out[0..8], PROJ_MAGIC);
    }
}
