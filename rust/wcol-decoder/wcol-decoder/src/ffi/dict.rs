use crate::ffi::{write_u32, RUNTIMES};
use crate::runtime::dict_value_bytes;

#[no_mangle]
pub unsafe extern "C" fn runtime_dict_lookup(
    handle: u32,
    col_id: u32,
    value_ptr: *const u8,
    value_len: usize,
) -> i32 {
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    let col = match runtime.schema.get(col_id as usize) {
        Some(c) => c,
        None => return -2,
    };
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return -3,
    };
    let value = unsafe { std::slice::from_raw_parts(value_ptr, value_len) };
    if !dict.lookup.is_empty() {
        let value = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return -4,
        };
        return match dict.lookup.get(value) {
            Some(id) => *id as i32,
            None => -5,
        };
    }
    if !dict.offsets.is_empty() {
        for idx in 0..dict.offsets.len().saturating_sub(1) {
            let start = dict.offsets[idx] as usize;
            let end = dict.offsets[idx + 1] as usize;
            if dict.blob.get(start..end) == Some(value) {
                return idx as i32;
            }
        }
    }
    if dict.lookup.is_empty() {
        let value_str = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return -4,
        };
        if let Ok(pos) = dict
            .values
            .binary_search_by(|v| v.as_bytes().cmp(value_str.as_bytes()))
        {
            return pos as i32;
        }
    }
    -5
}

#[no_mangle]
pub unsafe extern "C" fn runtime_dict_blob_info(
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
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return -3,
    };
    if dict.offsets.is_empty() {
        return -4;
    }
    let needed = 16;
    if out_len < needed {
        return -(needed as isize);
    }
    let offsets_ptr = dict.offsets.as_ptr() as usize;
    let blob_ptr = dict.blob.as_ptr() as usize;
    if offsets_ptr > u32::MAX as usize || blob_ptr > u32::MAX as usize {
        return -5;
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u32(out, 0, offsets_ptr as u32);
    write_u32(out, 4, dict.offsets.len() as u32);
    write_u32(out, 8, blob_ptr as u32);
    write_u32(out, 12, dict.blob.len() as u32);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn runtime_dict_value(
    handle: u32,
    col_id: u32,
    value_id: u32,
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
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return -3,
    };
    let value = match dict_value_bytes(dict, value_id as usize) {
        Some(v) => v,
        None => return -4,
    };
    if out_len < value.len() {
        return -(value.len() as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    out[..value.len()].copy_from_slice(value);
    value.len() as isize
}

#[no_mangle]
pub unsafe extern "C" fn runtime_dict_len(handle: u32, col_id: u32) -> i32 {
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&handle) {
        Some(r) => r,
        None => return -1,
    };
    let col = match runtime.schema.get(col_id as usize) {
        Some(c) => c,
        None => return -2,
    };
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return -3,
    };
    if !dict.offsets.is_empty() {
        dict.offsets.len().saturating_sub(1) as i32
    } else {
        dict.values.len() as i32
    }
}
