//! Wasm FFI for stream-pool mailboxes (up to 100 ingress / 100 egress).
//!
//! # Safety
//!
//! Every export shares one contract: a `*mut StreamPoolHandle` is null or a live handle from
//! `wcs_sp_open_*` that has not been passed to `wcs_sp_close`, and every `(ptr, len)` pair
//! names `len` readable (or, for slots, writable) bytes in Wasm memory. The host is
//! single-threaded and never calls into a handle re-entrantly.

// The shared contract above covers each `unsafe extern "C"` export.
#![allow(clippy::missing_safety_doc)]

use std::ptr;
use std::slice;

use crate::error::{clear_last_error, set_last_error, WcolError};
use crate::exec::stream_pool::StreamPoolSession;

pub struct StreamPoolHandle(pub StreamPoolSession);

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_open_echo(
    input_ports: u32,
    output_ports: u32,
    input_kib: u32,
    output_kib: u32,
) -> *mut StreamPoolHandle {
    match StreamPoolSession::open_echo(
        input_ports,
        output_ports,
        input_kib * 1024,
        output_kib * 1024,
    ) {
        Ok(session) => {
            clear_last_error();
            Box::into_raw(Box::new(StreamPoolHandle(session)))
        }
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_open_graph(
    program: *const u8,
    program_len: u32,
    scratch_capacity: u32,
    input_ports: u32,
    output_ports: u32,
    input_kib: u32,
    output_kib: u32,
    budget_share: u32,
) -> *mut StreamPoolHandle {
    if program.is_null() || program_len == 0 {
        set_last_error(WcolError::from("wcs_sp_open_graph: empty program"));
        return ptr::null_mut();
    }
    let bytes = slice::from_raw_parts(program, program_len as usize);
    match StreamPoolSession::open_graph(
        bytes,
        scratch_capacity as usize,
        input_ports,
        output_ports,
        input_kib * 1024,
        output_kib * 1024,
        budget_share,
    ) {
        Ok(session) => {
            clear_last_error();
            crate::ops::query::batch::clear_spill_boundaries();
            Box::into_raw(Box::new(StreamPoolHandle(session)))
        }
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_close(p: *mut StreamPoolHandle) {
    if !p.is_null() {
        drop(Box::from_raw(p));
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_step(p: *mut StreamPoolHandle, budget: u32) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.step(budget)
}

/// The planned graph retains its sink between explicit commands.
#[no_mangle]
pub unsafe extern "C" fn wcs_sp_is_resident(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    u32::from((*p).0.is_resident())
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_command(
    p: *mut StreamPoolHandle,
    kind: u32,
    bytes: *const u8,
    len: u32,
) -> u32 {
    if p.is_null() || (bytes.is_null() && len != 0) {
        return 0;
    }
    let bytes = if len == 0 {
        &[]
    } else {
        slice::from_raw_parts(bytes, len as usize)
    };
    match (*p).0.resident_command(kind, bytes) {
        Ok(()) => {
            clear_last_error();
            1
        }
        Err(error) => {
            set_last_error(error);
            0
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_response_ptr(p: *mut StreamPoolHandle) -> *const u8 {
    if p.is_null() {
        return ptr::null();
    }
    (*p).0.resident_response().as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_response_len(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.resident_response().len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_in_ptr(p: *mut StreamPoolHandle, port: u32) -> *mut u8 {
    if p.is_null() {
        return ptr::null_mut();
    }
    (*p).0.input_ptr(port)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_in_free(p: *mut StreamPoolHandle, port: u32) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.input_free(port)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_in_commit(p: *mut StreamPoolHandle, port: u32, length: u32) -> i32 {
    if p.is_null() {
        return -1;
    }
    match (*p).0.input_commit(port, length) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err);
            -2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_in_end(p: *mut StreamPoolHandle, port: u32) -> i32 {
    if p.is_null() {
        return -1;
    }
    match (*p).0.input_end(port) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err);
            -2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_in_absent(p: *mut StreamPoolHandle, port: u32) -> i32 {
    if p.is_null() {
        return -1;
    }
    match (*p).0.input_absent(port) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err);
            -2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_out_len(p: *mut StreamPoolHandle, port: u32) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.output_len(port)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_out_ptr(p: *mut StreamPoolHandle, port: u32) -> *const u8 {
    if p.is_null() {
        return ptr::null();
    }
    (*p).0.output_ptr(port)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_out_release(p: *mut StreamPoolHandle, port: u32) -> i32 {
    if p.is_null() {
        return -1;
    }
    match (*p).0.output_release(port) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err);
            -2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_out_done(p: *mut StreamPoolHandle, port: u32) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.output_done(port)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_read_at_ticket(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.read_at().0
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_read_at_source(p: *mut StreamPoolHandle) -> u16 {
    if p.is_null() {
        return 0;
    }
    (*p).0.read_at().1
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_read_at_offset(p: *mut StreamPoolHandle) -> u64 {
    if p.is_null() {
        return 0;
    }
    (*p).0.read_at().2
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_read_at_len(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.read_at().3
}

/// Claim the exposed ReadAt so the next step can expose another.
#[no_mangle]
pub unsafe extern "C" fn wcs_sp_take_read_at(p: *mut StreamPoolHandle) {
    if !p.is_null() {
        (*p).0.take_read_at();
    }
}

/// Complete a ReadAt with `len` bytes at `buf`, which must come from `wcs_alloc(len)`. The
/// pool takes the allocation over (the host must not free it), so a fetch is kept without
/// a second copy.
#[no_mangle]
pub unsafe extern "C" fn wcs_sp_complete_read_at(
    p: *mut StreamPoolHandle,
    ticket: u32,
    buf: *mut u8,
    len: u32,
) -> i32 {
    let bytes = if len == 0 || buf.is_null() {
        Vec::new()
    } else {
        // `wcs_alloc` allocates with `Layout::from_size_align(len, 1)`, which is the layout
        // a `Vec<u8>` of capacity `len` frees with.
        Vec::from_raw_parts(buf, len as usize, len as usize)
    };
    if p.is_null() {
        return 0;
    }
    match (*p).0.complete_read_at(ticket, bytes) {
        Ok(()) => {
            clear_last_error();
            1
        }
        Err(err) => {
            set_last_error(err);
            0
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_error_ptr(p: *mut StreamPoolHandle) -> *const u8 {
    if p.is_null() {
        return ptr::null();
    }
    match (*p).0.error() {
        Some(msg) => msg.as_ptr(),
        None => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_error_len(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.error().map(|msg| msg.len() as u32).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_op(p: *mut StreamPoolHandle) -> u8 {
    if p.is_null() {
        return 0;
    }
    (*p).0.spill_op()
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_append_len(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.spill_append_bytes().len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_append_ptr(p: *mut StreamPoolHandle) -> *const u8 {
    if p.is_null() {
        return ptr::null();
    }
    let slice = (*p).0.spill_append_bytes();
    if slice.is_empty() {
        ptr::null()
    } else {
        slice.as_ptr()
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_run_upload_len(p: *mut StreamPoolHandle) -> u32 {
    if p.is_null() {
        return 0;
    }
    (*p).0.spill_run_upload_body().len() as u32
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_run_upload_ptr(p: *mut StreamPoolHandle) -> *const u8 {
    if p.is_null() {
        return ptr::null();
    }
    let slice = (*p).0.spill_run_upload_body();
    if slice.is_empty() {
        ptr::null()
    } else {
        slice.as_ptr()
    }
}

/// Buffer for the pending run read; null on error. Fill it, then `wcs_sp_spill_complete`
/// with no scan bytes.
#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_read_slot(p: *mut StreamPoolHandle, len: u32) -> *mut u8 {
    if p.is_null() {
        return ptr::null_mut();
    }
    match (*p).0.spill_run_read_slot(len as usize) {
        Ok(slot) => {
            clear_last_error();
            slot
        }
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn wcs_sp_spill_complete(
    p: *mut StreamPoolHandle,
    ok: u32,
    scan_ptr: *const u8,
    scan_len: u32,
) -> i32 {
    if p.is_null() {
        return -1;
    }
    let scan = if scan_ptr.is_null() || scan_len == 0 {
        &[][..]
    } else {
        slice::from_raw_parts(scan_ptr, scan_len as usize)
    };
    match (*p).0.complete_spill(ok != 0, scan) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err);
            -2
        }
    }
}
