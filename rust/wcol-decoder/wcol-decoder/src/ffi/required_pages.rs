use rustc_hash::FxHashSet;
use std::cell::Cell;

use crate::constants::{
    EMPTY_MODE_MIXED, NULL_SENTINEL, OP_EQ, OP_NEQ, PAGE_KIND_DATA, PAGE_KIND_EMPTY,
    PAGE_KIND_NULL, PAGE_REQ_WORDS,
};
use crate::ffi::{lock_plans_timed, lock_runtimes_timed};
use crate::parse::parse_chunk_index;
use crate::runtime::{eval_possible, filter_possible, plan_required_columns};
use crate::types::PageDesc;

#[derive(Clone, Copy)]
struct ReqPtrCache {
    plan_handle: u32,
    runtime_handle: u32,
    plan_ptr: *const crate::types::Plan,
    runtime_ptr: *mut crate::types::Runtime,
}

thread_local! {
    static REQ_PTR_CACHE: Cell<Option<ReqPtrCache>> = const { Cell::new(None) };
}

#[no_mangle]
pub unsafe extern "C" fn plan_required_pages(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    index_ptr: *const u8,
    index_len: usize,
    index_raw_len: usize,
    out_ptr: *mut u32,
    out_len: usize,
) -> isize {
    let (plan_ptr, runtime_ptr) = match required_cached_ptrs(plan_handle, runtime_handle) {
        Ok(ptrs) => ptrs,
        Err(code) => return code as isize,
    };
    let plan = unsafe { &*plan_ptr };
    let runtime = unsafe { &mut *runtime_ptr };
    let _header = match runtime.header {
        Some(h) => h,
        None => return -3,
    };
    if plan.runtime != runtime_handle {
        return -7;
    }

    if runtime.toc.is_empty() {
        return -3;
    }
    if chunk_id as usize >= runtime.toc.len() {
        return -4;
    }

    let index_bytes = unsafe { std::slice::from_raw_parts(index_ptr, index_len) };
    let decompressed = match lz4_flex::block::decompress(index_bytes, index_raw_len) {
        Ok(data) => data,
        Err(_) => return -5,
    };

    let entries = match parse_chunk_index(&decompressed, runtime.schema.len()) {
        Ok(e) => e,
        Err(_) => return -6,
    };
    runtime.index_cache.insert(chunk_id, entries.clone());

    if !plan.filters.is_empty() {
        let mut statuses = Vec::with_capacity(plan.filters.len());
        for filter in &plan.filters {
            let col = match runtime.schema.get(filter.col_id as usize) {
                Some(c) => c,
                None => return -7,
            };
            let entry = match entries.get(filter.col_id as usize) {
                Some(e) => e,
                None => return -6,
            };
            statuses.push(filter_possible(filter, col, entry, runtime));
        }
        let possible = if plan.combine.is_empty() {
            statuses.iter().all(|status| status.maybe_true)
        } else {
            match eval_possible(&plan.combine, &statuses) {
                Some(p) => p.maybe_true,
                None => true,
            }
        };
        if !possible {
            return 0;
        }
    }

    let required_cols = plan_required_columns(plan, runtime.schema.len());
    let mut pages = Vec::new();
    let mut empty_needed: FxHashSet<u32> = FxHashSet::default();
    for filter in &plan.filters {
        if filter.value_str.as_deref() == Some("") && (filter.op == OP_EQ || filter.op == OP_NEQ) {
            empty_needed.insert(filter.col_id);
        }
    }

    for col_id in required_cols {
        let col = match runtime.schema.get(col_id as usize) {
            Some(c) => c,
            None => continue,
        };
        let entry = entries.get(col_id as usize).unwrap();
        pages.push(PageDesc {
            kind: PAGE_KIND_DATA,
            col_id,
            offset: entry.data_off,
            comp_len: entry.data_comp_len,
            raw_len: entry.data_raw_len,
        });
        if (col.flags & crate::constants::FLAG_NULLABLE) != 0
            && entry.null_off != NULL_SENTINEL as u64
            && entry.null_comp_len > 0
        {
            pages.push(PageDesc {
                kind: PAGE_KIND_NULL,
                col_id,
                offset: entry.null_off,
                comp_len: entry.null_comp_len,
                raw_len: entry.null_raw_len,
            });
        }
        if empty_needed.contains(&col_id)
            && entry.empty_mode == EMPTY_MODE_MIXED
            && entry.empty_off != NULL_SENTINEL as u64
            && entry.empty_comp_len > 0
        {
            pages.push(PageDesc {
                kind: PAGE_KIND_EMPTY,
                col_id,
                offset: entry.empty_off,
                comp_len: entry.empty_comp_len,
                raw_len: entry.empty_raw_len,
            });
        }
    }

    let needed_words = pages.len() * PAGE_REQ_WORDS;
    let needed_bytes = needed_words * 4;
    if out_len < needed_bytes {
        return -(needed_bytes as isize);
    }

    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len / 4) };
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

fn required_cached_ptrs(
    plan_handle: u32,
    runtime_handle: u32,
) -> Result<(*const crate::types::Plan, *mut crate::types::Runtime), i32> {
    if let Some(hit) = REQ_PTR_CACHE.with(|cache| cache.get()) {
        if hit.plan_handle == plan_handle && hit.runtime_handle == runtime_handle {
            return Ok((hit.plan_ptr, hit.runtime_ptr));
        }
    }

    let (plan_ptr, runtime_ptr) = {
        let plans = lock_plans_timed();
        let plan = match plans.get(&plan_handle) {
            Some(p) => p,
            None => return Err(-2),
        };
        if plan.runtime != runtime_handle {
            return Err(-7);
        }
        let plan_ptr = (&**plan) as *const crate::types::Plan;
        drop(plans);

        let mut runtimes = lock_runtimes_timed();
        let runtime = match runtimes.get_mut(&runtime_handle) {
            Some(r) => r,
            None => return Err(-1),
        };
        let runtime_ptr = (&mut **runtime) as *mut crate::types::Runtime;
        (plan_ptr, runtime_ptr)
    };

    REQ_PTR_CACHE.with(|cache| {
        cache.set(Some(ReqPtrCache {
            plan_handle,
            runtime_handle,
            plan_ptr,
            runtime_ptr,
        }));
    });
    Ok((plan_ptr, runtime_ptr))
}
