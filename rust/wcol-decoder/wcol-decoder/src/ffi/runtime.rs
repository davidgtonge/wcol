use crate::constants::{ROWS_PER_CHUNK, ROW_COUNT_COL_ID, WCOL_VERSION};
use wcol_format::HEADER_INFO_BYTES;
use crate::ffi::{
    next_handle, write_i32, write_u32, write_u64, PLAN_LOCK_COUNT, PLAN_LOCK_WAIT_NS, RUNTIMES,
    RUNTIME_LOCK_COUNT, RUNTIME_LOCK_WAIT_NS,
};
use crate::parse::{parse_dicts, parse_header, parse_schema, read_u64};
use crate::types::Runtime;
use std::mem::size_of;

#[no_mangle]
pub unsafe extern "C" fn create_runtime() -> u32 {
    let handle = next_handle();
    let runtime = Runtime {
        header: None,
        schema: std::sync::Arc::from([]),
        toc: Vec::new(),
        dicts: rustc_hash::FxHashMap::default(),
        index_cache: rustc_hash::FxHashMap::default(),
    };
    RUNTIMES.lock().unwrap().insert(handle, Box::new(runtime));
    handle
}

#[no_mangle]
pub unsafe extern "C" fn destroy_runtime(handle: u32) {
    RUNTIMES.lock().unwrap().remove(&handle);
}

#[no_mangle]
pub unsafe extern "C" fn runtime_set_header(handle: u32, ptr: *const u8, len: usize) -> i32 {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match parse_header(bytes) {
        Ok(header) => {
            if header.version != WCOL_VERSION {
                return -2;
            }
            if header.rows_per_chunk as usize != ROWS_PER_CHUNK {
                return -2;
            }
            if let Some(runtime) = RUNTIMES.lock().unwrap().get_mut(&handle) {
                runtime.header = Some(header);
                runtime.index_cache.clear();
                return 0;
            }
            -1
        }
        Err(_) => -3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn runtime_header_info(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let header = match RUNTIMES.lock().unwrap().get(&handle).and_then(|r| r.header) {
        Some(h) => h,
        None => return -1,
    };

    if out_len < HEADER_INFO_BYTES {
        return -(HEADER_INFO_BYTES as isize);
    }

    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u32(out, 0, header.version as u32);
    write_u32(out, 4, header.flags as u32);
    write_u32(out, 8, header.ncols);
    write_u32(out, 12, header.nchunks);
    write_u32(out, 16, header.rows_per_chunk);
    write_u64(out, 20, header.total_rows);
    write_u64(out, 28, header.schema_off);
    write_u64(out, 36, header.schema_len);
    write_u64(out, 44, header.index_off);
    write_u64(out, 52, header.index_len);
    write_u64(out, 60, header.dict_off);
    write_u64(out, 68, header.dict_len);
    write_u64(out, 76, header.data_off);
    write_u64(out, 84, header.dict_raw_len);

    HEADER_INFO_BYTES as isize
}

#[no_mangle]
pub unsafe extern "C" fn runtime_set_schema(handle: u32, ptr: *const u8, len: usize) -> i32 {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let header = match RUNTIMES.lock().unwrap().get(&handle).and_then(|r| r.header) {
        Some(h) => h,
        None => return -1,
    };
    match parse_schema(bytes, header.ncols as usize) {
        Ok(schema) => {
            if let Some(runtime) = RUNTIMES.lock().unwrap().get_mut(&handle) {
                runtime.schema = schema.into();
                runtime.index_cache.clear();
                return 0;
            }
            -1
        }
        Err(_) => -2,
    }
}

#[no_mangle]
pub unsafe extern "C" fn runtime_set_toc(handle: u32, ptr: *const u8, len: usize) -> i32 {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let header = match RUNTIMES.lock().unwrap().get(&handle).and_then(|r| r.header) {
        Some(h) => h,
        None => return -1,
    };
    let count = header.nchunks as usize;
    if bytes.len() < count * 8 {
        return -2;
    }
    let mut toc = Vec::with_capacity(count);
    for i in 0..count {
        toc.push(read_u64(bytes, i * 8));
    }
    if let Some(runtime) = RUNTIMES.lock().unwrap().get_mut(&handle) {
        runtime.toc = toc;
        runtime.index_cache.clear();
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn runtime_chunk_index_span(
    handle: u32,
    chunk_id: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    if runtime.toc.is_empty() {
        return -2;
    }
    let idx = chunk_id as usize;
    if idx >= runtime.toc.len() {
        return -3;
    }
    let header = match runtime.header {
        Some(h) => h,
        None => return -4,
    };
    let offset = runtime.toc[idx];
    let next = if idx + 1 < runtime.toc.len() {
        runtime.toc[idx + 1]
    } else {
        header.index_off + header.index_len
    };
    let comp_len = next.saturating_sub(offset);
    if comp_len > u32::MAX as u64 {
        return -5;
    }
    const SPAN_BYTES: isize = 12;
    if out_len < SPAN_BYTES as usize {
        return -SPAN_BYTES;
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(out, 0, offset);
    write_u32(out, 8, comp_len as u32);
    SPAN_BYTES
}

#[no_mangle]
pub unsafe extern "C" fn runtime_index_cache_stats(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    let needed = 32usize;
    if out_len < needed {
        return -(needed as isize);
    }

    let chunk_count = runtime.index_cache.len() as u64;
    let entries_len: u64 = runtime.index_cache.values().map(|v| v.len() as u64).sum();
    let entries_cap: u64 = runtime
        .index_cache
        .values()
        .map(|v| v.capacity() as u64)
        .sum();
    let bytes_est = entries_cap.saturating_mul(size_of::<crate::types::IndexEntry>() as u64);

    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(out, 0, chunk_count);
    write_u64(out, 8, entries_len);
    write_u64(out, 16, entries_cap);
    write_u64(out, 24, bytes_est);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn runtime_global_stats(out_ptr: *mut u8, out_len: usize) -> isize {
    let runtimes = RUNTIMES.lock().unwrap();
    let needed = 40usize;
    if out_len < needed {
        return -(needed as isize);
    }
    let mut index_chunks_total = 0u64;
    let mut index_entries_len_total = 0u64;
    let mut index_entries_cap_total = 0u64;
    let mut index_bytes_est_total = 0u64;
    for runtime in runtimes.values() {
        index_chunks_total = index_chunks_total.saturating_add(runtime.index_cache.len() as u64);
        let entries_len: u64 = runtime.index_cache.values().map(|v| v.len() as u64).sum();
        let entries_cap: u64 = runtime
            .index_cache
            .values()
            .map(|v| v.capacity() as u64)
            .sum();
        index_entries_len_total = index_entries_len_total.saturating_add(entries_len);
        index_entries_cap_total = index_entries_cap_total.saturating_add(entries_cap);
        index_bytes_est_total = index_bytes_est_total.saturating_add(
            entries_cap.saturating_mul(size_of::<crate::types::IndexEntry>() as u64),
        );
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(out, 0, runtimes.len() as u64);
    write_u64(out, 8, index_chunks_total);
    write_u64(out, 16, index_entries_len_total);
    write_u64(out, 24, index_entries_cap_total);
    write_u64(out, 32, index_bytes_est_total);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn ffi_lock_stats(out_ptr: *mut u8, out_len: usize) -> isize {
    let needed = 32usize;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(
        out,
        0,
        PLAN_LOCK_COUNT.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_u64(
        out,
        8,
        PLAN_LOCK_WAIT_NS.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_u64(
        out,
        16,
        RUNTIME_LOCK_COUNT.load(std::sync::atomic::Ordering::Relaxed),
    );
    write_u64(
        out,
        24,
        RUNTIME_LOCK_WAIT_NS.load(std::sync::atomic::Ordering::Relaxed),
    );
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn lz4_decompress(
    src_ptr: *const u8,
    src_len: usize,
    raw_len: usize,
    dst_ptr: *mut u8,
    dst_len: usize,
) -> i32 {
    if dst_len < raw_len {
        return -1;
    }
    if raw_len == 0 {
        return 0;
    }
    let src = std::slice::from_raw_parts(src_ptr, src_len);
    let decoded = match lz4_flex::block::decompress(src, raw_len) {
        Ok(bytes) => bytes,
        Err(_) => return -2,
    };
    if decoded.len() != raw_len {
        return -3;
    }
    let dst = std::slice::from_raw_parts_mut(dst_ptr, raw_len);
    dst.copy_from_slice(&decoded);
    0
}

#[no_mangle]
pub unsafe extern "C" fn runtime_set_dicts(handle: u32, ptr: *const u8, len: usize) -> i32 {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match parse_dicts(bytes) {
        Ok(dicts) => {
            if let Some(runtime) = RUNTIMES.lock().unwrap().get_mut(&handle) {
                runtime.dicts = dicts;
                return 0;
            }
            -1
        }
        Err(_) => -2,
    }
}

#[no_mangle]
pub unsafe extern "C" fn runtime_column_id_by_name(
    handle: u32,
    name_ptr: *const u8,
    name_len: usize,
) -> i32 {
    let name_bytes = unsafe { std::slice::from_raw_parts(name_ptr, name_len) };
    let name = match std::str::from_utf8(name_bytes) {
        Ok(s) => s,
        Err(_) => return -2,
    };
    if let Some(runtime) = RUNTIMES.lock().unwrap().get(&handle) {
        for col in runtime.schema.iter() {
            if col.name == name {
                return col.id as i32;
            }
        }
        return -3;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn runtime_column_info(
    handle: u32,
    col_id: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    let col = match runtime.schema.get(col_id as usize) {
        Some(c) => c,
        None => return -2,
    };
    let needed = 12;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    out[0] = col.logical_type;
    out[1] = col.physical_type;
    out[2] = col.flags;
    out[3] = col.encoding;
    write_u32(out, 4, col.dict_id);
    write_i32(out, 8, col.scale);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn runtime_column_name(
    handle: u32,
    col_id: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    if col_id == ROW_COUNT_COL_ID {
        let name = b"count_star()";
        if out_len < name.len() {
            return -(name.len() as isize);
        }
        let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
        out[..name.len()].copy_from_slice(name);
        return name.len() as isize;
    }
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    let col = match runtime.schema.get(col_id as usize) {
        Some(c) => c,
        None => return -2,
    };
    let name = col.name.as_bytes();
    if out_len < name.len() {
        return -(name.len() as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    out[..name.len()].copy_from_slice(name);
    name.len() as isize
}
