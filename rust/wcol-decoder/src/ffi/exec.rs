//! plan_required_pages and plan_exec_chunk FFI + resolve_deferred_filter helper.

use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::constants::{
    ERR_UNSUPPORTED, FLAG_DICT, FLAG_NULLABLE, OP_LIKE, OP_NOT_LIKE, PAGE_EXEC_WORDS, TYPE_STRING,
};
use crate::decode::{decode_raw_string_like_mask, ensure_decoded};
use crate::exec::{decompress_pages, PageMaps};
use crate::ffi::{
    execute_row_phase, lock_plans_timed, lock_runtimes_timed, resolve_deferred_filter,
    row_is_valid_for_all, NullBitmapCache,
};
use crate::timing::Tic;
use crate::runtime::{
    agg_key_col_id, agg_key_kind, aggregate_column, build_empty_string_mask,
    build_filter_mask_pipeline, build_group_key_materialized_with_runtime,
    build_group_key_with_runtime, build_like_id_set, clear_tail, combine_masks, full_mask,
    hll_aggregate_column, hll_aggregate_dict_ids, hist_count_dict_column, is_valid, iter_mask,
    mask_and, mask_count, plan_uses_group_dict_histogram,
    mask_is_full, mask_is_zero, merge_agg_by_kind, read_distinct_key, read_value_f64,
    rows_in_chunk,
};
use crate::types::{AggState, Column, ColumnData, GroupAggState, GroupState};

#[derive(Clone, Copy)]
struct ExecPtrCache {
    plan_handle: u32,
    runtime_handle: u32,
    plan_ptr: *mut crate::types::Plan,
    runtime_ptr: *mut crate::types::Runtime,
}

thread_local! {
    static EXEC_PTR_CACHE: Cell<Option<ExecPtrCache>> = const { Cell::new(None) };
}

#[no_mangle]
pub unsafe extern "C" fn plan_exec_chunk(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    desc_ptr: *const u32,
    desc_len: usize,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    use crate::constants::{
        AGG_KIND_APPROX_DISTINCT, AGG_KIND_AVG, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR, AGG_KIND_MAX,
        AGG_KIND_MIN, AGG_KIND_SUM,
    };

    let (plan_ptr, runtime_ptr) = match exec_cached_ptrs(plan_handle, runtime_handle) {
        Ok(ptrs) => ptrs,
        Err(code) => return code,
    };
    let plan = unsafe { &mut *plan_ptr };
    let runtime = unsafe { &mut *runtime_ptr };
    let header = match runtime.header {
        Some(h) => h,
        None => return -3,
    };
    let schema = runtime.schema.clone();

    let descs = unsafe { std::slice::from_raw_parts(desc_ptr, desc_len) };
    let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
    if descs.len() % PAGE_EXEC_WORDS != 0 {
        return -4;
    }

    let rows_in_chunk = rows_in_chunk(
        header.total_rows,
        header.rows_per_chunk as usize,
        chunk_id as usize,
    );
    if rows_in_chunk == 0 {
        return 0;
    }

    let t_decode = Tic::start();
    let PageMaps {
        data_pages,
        null_pages,
        empty_pages,
    } = match decompress_pages(descs, data, rows_in_chunk) {
        Ok(pm) => pm,
        Err(e) => return e,
    };
    plan.timing.add_ms_decode(t_decode.elapsed());
    let mut null_cache = NullBitmapCache::new(&null_pages, rows_in_chunk);

    let mut decoded: FxHashMap<u32, ColumnData> = FxHashMap::default();
    let t_filters = Tic::start();
    plan.filter_timing.sync_filters(&plan.filters);
    let mut masks = Vec::new();
    for (idx, filter) in plan.filters.iter_mut().enumerate() {
        let col = match schema.get(filter.col_id as usize) {
            Some(c) => c,
            None => return -7,
        };
        if (filter.op == OP_LIKE || filter.op == OP_NOT_LIKE) && filter.like_ids.is_none() {
            if let Some(pattern) = filter.value_str.as_deref() {
                if (col.flags & FLAG_DICT) != 0 {
                    let ids = build_like_id_set(col, pattern, runtime);
                    if !ids.is_empty() {
                        filter.like_ids = Some(Arc::new(ids));
                    }
                }
            }
        }
        let mask = if let Some(entries) = runtime.index_cache.get(&chunk_id) {
            if let Some(entry) = entries.get(filter.col_id as usize) {
                build_empty_string_mask(
                    entry,
                    col,
                    filter,
                    rows_in_chunk,
                    empty_pages.get(&filter.col_id).map(|v| v.as_slice()),
                    null_pages.get(&filter.col_id).map(|v| v.as_slice()),
                )
            } else {
                None
            }
        } else {
            None
        };

        let mut mask = if let Some(mask) = mask {
            mask
        } else if (filter.op == OP_LIKE || filter.op == OP_NOT_LIKE)
            && col.logical_type == TYPE_STRING
            && (col.flags & FLAG_DICT) == 0
        {
            let data_raw = match data_pages.get(&filter.col_id) {
                Some(p) => p,
                None => return -8,
            };
            let pattern = match filter.value_str.as_deref() {
                Some(p) => p,
                None => return -8,
            };
            let t_f_build = Tic::start();
            let mut like_stats = crate::types::LikeMaskStats::default();
            let mask = match decode_raw_string_like_mask(
                data_raw,
                rows_in_chunk,
                pattern,
                filter.op == OP_NOT_LIKE,
                Some(&mut plan.timing),
                Some(&mut like_stats),
            ) {
                Ok(mask) => mask,
                Err(code) => return code,
            };
            plan.timing.add_ms_filters_build(t_f_build.elapsed());
            plan.filter_timing.add_ms_build(idx, t_f_build.elapsed());
            plan.filter_timing.merge_like_stats(idx, &like_stats);
            mask
        } else {
            let data_raw = match data_pages.get(&filter.col_id) {
                Some(p) => p,
                None => return -8,
            };
            let t_f_decode = Tic::start();
            if let Err(code) = ensure_decoded(
                col,
                data_raw,
                &mut decoded,
                rows_in_chunk,
                runtime,
                Some(&mut plan.timing),
            ) {
                return code;
            }
            plan.timing.add_ms_filters_decode(t_f_decode.elapsed());
            plan.filter_timing.add_ms_decode(idx, t_f_decode.elapsed());
            let data_page = match decoded.get(&filter.col_id) {
                Some(data) => data,
                None => return -8,
            };
            let resolved_filter = resolve_deferred_filter(filter, col, runtime);
            let t_f_build = Tic::start();
            let mask = match build_filter_mask_pipeline(
                col,
                data_page,
                &resolved_filter,
                rows_in_chunk,
                Some(runtime),
                if (col.flags & FLAG_NULLABLE) != 0 {
                    null_cache.for_col(filter.col_id)
                } else {
                    None
                },
            ) {
                Ok(m) => m,
                Err(_) => return -18,
            };
            plan.timing.add_ms_filters_build(t_f_build.elapsed());
            plan.filter_timing.add_ms_build(idx, t_f_build.elapsed());
            mask
        };

        if (col.flags & FLAG_NULLABLE) != 0 && (filter.op == OP_LIKE || filter.op == OP_NOT_LIKE) {
            if null_cache.for_col(filter.col_id).is_some() {
                let t_f_nulls = Tic::start();
                mask = match null_cache.apply_to_mask(mask, filter.col_id) {
                    Ok(m) => m,
                    Err(_) => return -18,
                };
                plan.timing.add_ms_filters_nulls(t_f_nulls.elapsed());
                plan.filter_timing.add_ms_nulls(idx, t_f_nulls.elapsed());
            }
        }
        masks.push(mask);
    }
    let t_f_combine = Tic::start();

    let t_aggs = Tic::start();
    let mut combined = if masks.is_empty() {
        full_mask(rows_in_chunk)
    } else if plan.combine.is_empty() {
        let mut out = masks[0].clone();
        for mask in masks.iter().skip(1) {
            out = mask_and(&out, mask);
        }
        out
    } else {
        match combine_masks(&plan.combine, &masks) {
            Ok(mask) => mask,
            Err(_) => return -9,
        }
    };
    plan.timing.add_ms_filters_combine(t_f_combine.elapsed());
    plan.timing.add_ms_filters(t_filters.elapsed());

    clear_tail(&mut combined, rows_in_chunk);
    if mask_is_zero(&combined) {
        return 0;
    }
    let combined_full = mask_is_full(&combined, rows_in_chunk);

    let mut cached_col_id: Option<u32> = None;
    let mut cached_partial: Option<AggState> = None;

    for agg_key in &plan.aggregates {
        let kind = agg_key_kind(*agg_key);
        let col_id = agg_key_col_id(*agg_key);

        if kind == AGG_KIND_COUNT_STAR {
            let n = mask_count(&combined);
            let state = plan.agg_state.get_mut(agg_key).unwrap();
            state.count += n;
            continue;
        }

        let col = match schema.get(col_id as usize) {
            Some(c) => c,
            None => return -10,
        };
        let data_raw = match data_pages.get(&col_id) {
            Some(p) => p,
            None => return -11,
        };
        if let Err(code) = ensure_decoded(
            col,
            data_raw,
            &mut decoded,
            rows_in_chunk,
            runtime,
            Some(&mut plan.timing),
        ) {
            return code;
        }
        let data_page = match decoded.get(&col_id) {
            Some(data) => data,
            None => return -11,
        };

        if kind == AGG_KIND_APPROX_DISTINCT {
            let hll_state = match plan.hll_state.get_mut(agg_key) {
                Some(s) => s,
                None => return -19,
            };
            let mut agg_mask = combined.clone();
            if (col.flags & FLAG_NULLABLE) != 0 {
                if null_cache.for_col(col_id).is_some() {
                    agg_mask = match null_cache.apply_to_mask(agg_mask, col_id) {
                        Ok(m) => m,
                        Err(_) => return -18,
                    };
                }
            }
            if (col.flags & FLAG_DICT) != 0 {
                hll_aggregate_dict_ids(data_page, &agg_mask, rows_in_chunk, hll_state);
            } else {
                hll_aggregate_column(col, data_page, &agg_mask, rows_in_chunk, hll_state);
            }
            continue;
        }

        if cached_col_id != Some(col_id) {
            let mut agg_mask = combined.clone();
            if (col.flags & FLAG_NULLABLE) != 0 {
                if null_cache.for_col(col_id).is_some() {
                    agg_mask = match null_cache.apply_to_mask(agg_mask, col_id) {
                        Ok(m) => m,
                        Err(_) => return -18,
                    };
                }
            }
            cached_partial = Some(aggregate_column(col, data_page, &agg_mask, rows_in_chunk));
            cached_col_id = Some(col_id);
        }

        let partial = cached_partial.as_ref().unwrap().clone();
        merge_agg_by_kind(plan.agg_state.get_mut(agg_key).unwrap(), partial, kind);
    }
    plan.timing.add_ms_aggs(t_aggs.elapsed());

    if let Some(group_by) = &plan.group_by {
        let t_group = Tic::start();
        let key_cols = &group_by.keys;
        let mut key_nulls = Vec::new();
        for key in key_cols {
            let col = match schema.get(*key as usize) {
                Some(c) => c,
                None => return -12,
            };
            let data_raw = match data_pages.get(key) {
                Some(p) => p,
                None => return -13,
            };
            if let Err(code) = ensure_decoded(
                col,
                data_raw,
                &mut decoded,
                rows_in_chunk,
                runtime,
                Some(&mut plan.timing),
            ) {
                return code;
            }
            if (col.flags & FLAG_NULLABLE) != 0 {
                key_nulls.push(null_cache.for_col(*key));
            } else {
                key_nulls.push(None);
            }
        }

        struct AggInput<'a> {
            kind: u8,
            col: Option<&'a Column>,
            data: Option<&'a ColumnData>,
            nulls: Option<&'a [u8]>,
        }

        let mut agg_cols: Vec<u32> = Vec::new();
        for agg in &plan.group_aggs {
            if agg.kind == AGG_KIND_COUNT_STAR {
                continue;
            }
            if !agg_cols.contains(&agg.col_id) {
                agg_cols.push(agg.col_id);
            }
        }
        for col_id in &agg_cols {
            let col = match schema.get(*col_id as usize) {
                Some(c) => c,
                None => return -14,
            };
            let data_raw = match data_pages.get(col_id) {
                Some(p) => p,
                None => return -15,
            };
            if let Err(code) = ensure_decoded(
                col,
                data_raw,
                &mut decoded,
                rows_in_chunk,
                runtime,
                Some(&mut plan.timing),
            ) {
                return code;
            }
        }

        let mut agg_inputs: Vec<AggInput<'_>> = Vec::with_capacity(plan.group_aggs.len());
        for agg in &plan.group_aggs {
            if agg.kind == AGG_KIND_COUNT_STAR {
                agg_inputs.push(AggInput {
                    kind: agg.kind,
                    col: None,
                    data: None,
                    nulls: None,
                });
                continue;
            }
            let col = match schema.get(agg.col_id as usize) {
                Some(c) => c,
                None => return -14,
            };
            let data_page = match decoded.get(&col.id) {
                Some(data) => data,
                None => return -15,
            };
            let nulls = if (col.flags & FLAG_NULLABLE) != 0 {
                null_cache.for_col(agg.col_id)
            } else {
                None
            };
            agg_inputs.push(AggInput {
                kind: agg.kind,
                col: Some(col),
                data: Some(data_page),
                nulls,
            });
        }

        let mut key_data = Vec::new();
        for key in key_cols {
            let col = match schema.get(*key as usize) {
                Some(c) => c,
                None => return -12,
            };
            let data_page = match decoded.get(&col.id) {
                Some(data) => data,
                None => return -13,
            };
            key_data.push((col, data_page));
        }

        if plan_uses_group_dict_histogram(plan) {
            let counts = match plan.group_dict_hist_counts.as_mut() {
                Some(c) => c,
                None => return -20,
            };
            let (col, data_page) = key_data[0];
            hist_count_dict_column(
                counts,
                col,
                data_page,
                &combined,
                rows_in_chunk,
                &key_nulls,
            );
            if let Some(sums) = plan.group_dict_hist_sums.as_mut() {
                use crate::constants::AGG_KIND_SUM;
                let sum_col_id = plan
                    .group_aggs
                    .iter()
                    .find(|a| a.kind == AGG_KIND_SUM)
                    .map(|a| a.col_id)
                    .unwrap_or(u32::MAX);
                let val_col = match schema.get(sum_col_id as usize) {
                    Some(c) => c,
                    None => return -21,
                };
                let val_data = match decoded.get(&sum_col_id) {
                    Some(d) => d,
                    None => return -22,
                };
                crate::query::group_dict_hist::hist_sum_f64_dict_key(
                    sums,
                    data_page,
                    val_data,
                    &combined,
                    rows_in_chunk,
                    &key_nulls,
                );
                let _ = val_col;
            }
            plan.timing.add_ms_group(t_group.elapsed());
            let t_rows = Tic::start();
            if let Err(code) = execute_row_phase(
                plan,
                runtime,
                &schema,
                &data_pages,
                &mut decoded,
                &mut null_cache,
                &combined,
                combined_full,
                rows_in_chunk,
                chunk_id,
                header.rows_per_chunk,
            ) {
                return code;
            }
            plan.timing.add_ms_rows(t_rows.elapsed());
            plan.timing.inc_chunks();
            return mask_count(&combined) as i32;
        }

        let heavy_string_keys = key_data.iter().any(|(col, _)| {
            col.logical_type == TYPE_STRING && (col.flags & FLAG_DICT) == 0
        });
        let group_aggs = plan.group_aggs.clone();
        let count_only = group_aggs.len() == 1 && group_aggs[0].kind == AGG_KIND_COUNT_STAR;
        if !plan.group_emit_raw {
            if plan.group_state.capacity() < rows_in_chunk {
                plan.group_state
                    .reserve(rows_in_chunk - plan.group_state.capacity());
            }
            if plan.group_keys.capacity() < rows_in_chunk {
                plan.group_keys
                    .reserve(rows_in_chunk - plan.group_keys.capacity());
            }
        }
        let new_group_state = || GroupState {
            aggs: group_aggs
                .iter()
                .map(|agg| match agg.kind {
                    AGG_KIND_APPROX_DISTINCT => GroupAggState::Distinct(FxHashSet::default()),
                    _ => GroupAggState::Numeric(AggState {
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                        count: 0,
                    }),
                })
                .collect(),
        };
        let debug_v2 = plan.group_emit_raw
            && std::env::var("WCOL_DEBUG_V2")
                .map(|v| v != "0")
                .unwrap_or(false);
        let mut processed_rows = 0usize;

        let mut process_row = |row: usize| -> i32 {
            if plan.group_emit_raw {
                let materialized = if heavy_string_keys {
                    Some(build_group_key_materialized_with_runtime(
                        &key_data, row, &*runtime,
                    ))
                } else {
                    None
                };
                let key = if let Some(materialized) = materialized.as_ref() {
                    materialized.key
                } else {
                    build_group_key_with_runtime(&key_data, row, Some(&*runtime))
                };
                let key_bytes = materialized
                    .as_ref()
                    .map(|m| m.repr.as_slice())
                    .unwrap_or(&[]);
                let out = &mut plan.group_rows_raw_with_keys;
                out.extend_from_slice(&key.a.to_le_bytes());
                out.extend_from_slice(&key.b.to_le_bytes());
                out.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
                out.extend_from_slice(key_bytes);
                for agg_input in &agg_inputs {
                    let (sum, min, max, count) = match agg_input.kind {
                        AGG_KIND_COUNT_STAR => (0.0, 0.0, 0.0, 1u32),
                        AGG_KIND_COUNT => {
                            if let Some(nulls) = agg_input.nulls {
                                if !is_valid(nulls, row) {
                                    (0.0, 0.0, 0.0, 0u32)
                                } else {
                                    (0.0, 0.0, 0.0, 1u32)
                                }
                            } else {
                                (0.0, 0.0, 0.0, 1u32)
                            }
                        }
                        AGG_KIND_SUM | AGG_KIND_MIN | AGG_KIND_MAX | AGG_KIND_AVG => {
                            if let Some(nulls) = agg_input.nulls {
                                if !is_valid(nulls, row) {
                                    (0.0, 0.0, 0.0, 0u32)
                                } else {
                                    let col = match agg_input.col {
                                        Some(c) => c,
                                        None => return -14,
                                    };
                                    let data = match agg_input.data {
                                        Some(d) => d,
                                        None => return -15,
                                    };
                                    let v = read_value_f64(col, data, row);
                                    (v, v, v, 1u32)
                                }
                            } else {
                                let col = match agg_input.col {
                                    Some(c) => c,
                                    None => return -14,
                                };
                                let data = match agg_input.data {
                                    Some(d) => d,
                                    None => return -15,
                                };
                                let v = read_value_f64(col, data, row);
                                (v, v, v, 1u32)
                            }
                        }
                        AGG_KIND_APPROX_DISTINCT => return ERR_UNSUPPORTED,
                        _ => (0.0, 0.0, 0.0, 0u32),
                    };
                    out.extend_from_slice(&sum.to_le_bytes());
                    out.extend_from_slice(&min.to_le_bytes());
                    out.extend_from_slice(&max.to_le_bytes());
                    out.extend_from_slice(&count.to_le_bytes());
                    out.extend_from_slice(&0u32.to_le_bytes());
                }
                return 0;
            }

            let materialized = if heavy_string_keys {
                Some(build_group_key_materialized_with_runtime(
                    &key_data, row, &*runtime,
                ))
            } else {
                None
            };
            let key = if let Some(materialized) = materialized.as_ref() {
                let mut key = materialized.key;
                if let Some(existing) = plan.group_key_repr.get(&materialized.key) {
                    if existing.as_slice() != materialized.repr.as_slice() {
                        key = remap_group_key_for_collision(
                            materialized.key,
                            &materialized.repr,
                            &plan.group_key_repr,
                        );
                        log_group_key_collision(
                            runtime_handle,
                            plan_handle,
                            chunk_id,
                            row,
                            materialized.key,
                            key,
                            existing,
                            &materialized.repr,
                        );
                    }
                }
                key
            } else {
                build_group_key_with_runtime(&key_data, row, Some(&*runtime))
            };
            let entry = match plan.group_state.entry(key) {
                Entry::Vacant(v) => {
                    plan.group_keys.push(key);
                    if let Some(materialized) = materialized {
                        plan.group_key_repr.insert(key, materialized.repr);
                    }
                    v.insert(new_group_state())
                }
                Entry::Occupied(o) => o.into_mut(),
            };

            if count_only {
                if let Some(GroupAggState::Numeric(s)) = entry.aggs.get_mut(0) {
                    s.count = s.count.saturating_add(1);
                }
                return 0;
            }
            for (idx, agg_input) in agg_inputs.iter().enumerate() {
                let state = match entry.aggs.get_mut(idx) {
                    Some(s) => s,
                    None => continue,
                };
                match (agg_input.kind, state) {
                    (AGG_KIND_COUNT_STAR, GroupAggState::Numeric(s)) => {
                        s.count = s.count.saturating_add(1);
                    }
                    (AGG_KIND_COUNT, GroupAggState::Numeric(s)) => {
                        if let Some(nulls) = agg_input.nulls {
                            if !is_valid(nulls, row) {
                                continue;
                            }
                        }
                        s.count = s.count.saturating_add(1);
                    }
                    (
                        AGG_KIND_SUM | AGG_KIND_MIN | AGG_KIND_MAX | AGG_KIND_AVG,
                        GroupAggState::Numeric(s),
                    ) => {
                        if let Some(nulls) = agg_input.nulls {
                            if !is_valid(nulls, row) {
                                continue;
                            }
                        }
                        let col = match agg_input.col {
                            Some(c) => c,
                            None => continue,
                        };
                        let data = match agg_input.data {
                            Some(d) => d,
                            None => continue,
                        };
                        let value = read_value_f64(col, data, row);
                        s.count = s.count.saturating_add(1);
                        s.sum += value;
                        if value < s.min {
                            s.min = value;
                        }
                        if value > s.max {
                            s.max = value;
                        }
                    }
                    (AGG_KIND_APPROX_DISTINCT, GroupAggState::Distinct(set)) => {
                        if let Some(nulls) = agg_input.nulls {
                            if !is_valid(nulls, row) {
                                continue;
                            }
                        }
                        let col = match agg_input.col {
                            Some(c) => c,
                            None => continue,
                        };
                        let data = match agg_input.data {
                            Some(d) => d,
                            None => continue,
                        };
                        if let Some(key) = read_distinct_key(col, data, row) {
                            set.insert(key);
                        }
                    }
                    _ => {}
                }
            }
            0
        };

        if combined_full {
            for row in 0..rows_in_chunk {
                if !row_is_valid_for_all(row, &key_nulls) {
                    continue;
                }
                processed_rows = processed_rows.saturating_add(1);
                let code = process_row(row);
                if code != 0 {
                    return code;
                }
            }
        } else {
            for row in iter_mask(&combined, rows_in_chunk) {
                if !row_is_valid_for_all(row, &key_nulls) {
                    continue;
                }
                processed_rows = processed_rows.saturating_add(1);
                let code = process_row(row);
                if code != 0 {
                    return code;
                }
            }
        }
        if debug_v2 {
            eprintln!(
                "WCOL_V2_GROUP chunk={} processed_rows={} raw_bytes={}",
                chunk_id,
                processed_rows,
                plan.group_rows_raw_with_keys.len()
            );
        }
        plan.timing.add_ms_group(t_group.elapsed());
    }

    let t_rows = Tic::start();
    if let Err(code) = execute_row_phase(
        plan,
        runtime,
        &schema,
        &data_pages,
        &mut decoded,
        &mut null_cache,
        &combined,
        combined_full,
        rows_in_chunk,
        chunk_id,
        header.rows_per_chunk,
    ) {
        return code;
    }
    plan.timing.add_ms_rows(t_rows.elapsed());
    plan.timing.inc_chunks();

    mask_count(&combined) as i32
}

fn exec_cached_ptrs(
    plan_handle: u32,
    runtime_handle: u32,
) -> Result<(*mut crate::types::Plan, *mut crate::types::Runtime), i32> {
    if let Some(hit) = EXEC_PTR_CACHE.with(|cache| cache.get()) {
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
        let plan_ptr = (&**plan) as *const crate::types::Plan as *mut crate::types::Plan;
        drop(plans);

        let mut runtimes = lock_runtimes_timed();
        let runtime = match runtimes.get_mut(&runtime_handle) {
            Some(r) => r,
            None => return Err(-1),
        };
        let runtime_ptr = (&mut **runtime) as *mut crate::types::Runtime;
        (plan_ptr, runtime_ptr)
    };

    EXEC_PTR_CACHE.with(|cache| {
        cache.set(Some(ExecPtrCache {
            plan_handle,
            runtime_handle,
            plan_ptr,
            runtime_ptr,
        }));
    });
    Ok((plan_ptr, runtime_ptr))
}

fn remap_group_key_for_collision(
    base: crate::types::GroupKey,
    bytes: &[u8],
    existing: &FxHashMap<crate::types::GroupKey, Vec<u8>>,
) -> crate::types::GroupKey {
    let mut seed = 1u64;
    loop {
        let candidate = crate::types::GroupKey {
            a: base.a,
            b: xxh3_64_with_seed(bytes, base.b ^ seed),
        };
        if !existing.contains_key(&candidate) {
            return candidate;
        }
        seed = seed.saturating_add(1);
    }
}

fn collision_log_enabled() -> bool {
    std::env::var("WCOL_GROUP_KEY_COLLISION_LOG")
        .map(|v| v != "0")
        .unwrap_or(true)
}

fn log_group_key_collision(
    runtime_handle: u32,
    plan_handle: u32,
    chunk_id: u32,
    row: usize,
    base: crate::types::GroupKey,
    remapped: crate::types::GroupKey,
    existing: &[u8],
    incoming: &[u8],
) {
    if !collision_log_enabled() {
        return;
    }
    eprintln!(
        "WCOL_GROUP_KEY_COLLISION runtime={} plan={} chunk={} row={} base_a={} base_b={} remap_a={} remap_b={} existing_len={} incoming_len={} existing_prefix={} incoming_prefix={}",
        runtime_handle,
        plan_handle,
        chunk_id,
        row,
        base.a,
        base.b,
        remapped.a,
        remapped.b,
        existing.len(),
        incoming.len(),
        hex_prefix(existing, 32),
        hex_prefix(incoming, 32),
    );
}

fn hex_prefix(bytes: &[u8], n: usize) -> String {
    let mut out = String::new();
    for b in bytes.iter().take(n) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}
