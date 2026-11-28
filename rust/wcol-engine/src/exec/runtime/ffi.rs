//! Wasm ABI for the production operator bundle.
//!
//! # Safety
//!
//! Every export shares one contract: each `(ptr, len)` pair names `len` valid bytes in Wasm
//! memory, a pointer passed to `wcs_dealloc` came from `wcs_alloc` with the same length, a
//! handle pointer is null or live (not yet closed), and the host is single-threaded and never
//! calls in re-entrantly.

// The shared contract above covers each `unsafe extern "C"` export.
#![allow(clippy::missing_safety_doc)]

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
#[cfg(feature = "coordinator-iterative-ffi")]
use std::cell::RefCell;
use std::ptr;

#[cfg(feature = "coordinator-iterative-ffi")]
use std::slice;

#[cfg(feature = "coordinator-iterative-ffi")]
use crate::error::{clear_last_error, set_last_error, WcolError};
#[cfg(feature = "coordinator-iterative-ffi")]
use crate::ops::iterative::{IterativeKernel, KMeansConfig, KMeansKernel};

#[cfg(feature = "coordinator-iterative-ffi")]
fn ffi_fail(msg: &'static str) -> u32 {
    set_last_error(WcolError::from(msg));
    0
}

#[cfg(feature = "coordinator-iterative-ffi")]
fn ffi_fail_msg(msg: impl Into<String>) -> u32 {
    set_last_error(WcolError::msg(msg));
    0
}

#[cfg(feature = "coordinator-iterative-ffi")]
use crate::ops::iterative::wire::{
    decode_kmeans_moments, encode_kmeans_moments, encode_kmeans_normalization, kmeans_normalization,
};

#[cfg(feature = "coordinator-iterative-ffi")]
thread_local! {
    static ITERATIVE_OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Merge compact per-resident moments and bounded initialization samples in Rust.
#[no_mangle]
#[cfg(feature = "coordinator-iterative-ffi")]
pub unsafe extern "C" fn wcol_iterative_kmeans_merge_moments(
    left_ptr: *const u8,
    left_len: u32,
    right_ptr: *const u8,
    right_len: u32,
    dimensions: u32,
) -> u32 {
    if left_ptr.is_null() || right_ptr.is_null() || dimensions == 0 {
        return ffi_fail("invalid K-means moments merge request");
    }
    let dims = dimensions as usize;
    let (mut left, mut left_sample) =
        match decode_kmeans_moments(slice::from_raw_parts(left_ptr, left_len as usize), dims) {
            Ok(value) => value,
            Err(error) => return ffi_fail_msg(error),
        };
    let (right, right_sample) =
        match decode_kmeans_moments(slice::from_raw_parts(right_ptr, right_len as usize), dims) {
            Ok(value) => value,
            Err(error) => return ffi_fail_msg(error),
        };
    if let Err(error) = left.merge(&right) {
        return ffi_fail_msg(format!("iterative moments merge: {error:?}"));
    }
    let remaining_rows = 4096usize.saturating_sub(left_sample.len() / dims);
    left_sample.extend(right_sample.into_iter().take(dims * remaining_rows));
    store_iterative_output(encode_kmeans_moments(&left, &left_sample))
}

/// Compute global normalization and deterministic initial centroids in Rust.
#[no_mangle]
#[cfg(feature = "coordinator-iterative-ffi")]
pub unsafe extern "C" fn wcol_iterative_kmeans_prepare(
    moments_ptr: *const u8,
    moments_len: u32,
    dimensions: u32,
    clusters: u32,
    max_iterations: u32,
    tolerance: f64,
    seed: u64,
) -> u32 {
    if moments_ptr.is_null()
        || moments_len == 0
        || dimensions == 0
        || clusters == 0
        || max_iterations == 0
    {
        return ffi_fail("invalid K-means prepare request");
    }
    let dims = dimensions as usize;
    let (moments, sample) = match decode_kmeans_moments(
        slice::from_raw_parts(moments_ptr, moments_len as usize),
        dims,
    ) {
        Ok(value) => value,
        Err(error) => return ffi_fail_msg(error),
    };
    if sample.is_empty() {
        return ffi_fail("K-means initialization sample is empty");
    }
    let (means, scales) = kmeans_normalization(&moments);
    let normalized: Vec<f32> = sample
        .chunks_exact(dims)
        .flat_map(|row| {
            row.iter().enumerate().map(|(column, value)| {
                if value.is_nan() || scales[column] == 0.0 {
                    0.0
                } else {
                    ((value - means[column]) / scales[column]) as f32
                }
            })
        })
        .collect();
    let kernel = KMeansKernel {
        config: KMeansConfig {
            clusters: clusters as usize,
            dimensions: dims,
            max_iterations,
            tolerance,
        },
    };
    let state = match kernel
        .seeded_centroids(&normalized, seed)
        .and_then(|state| kernel.encode_state(&state))
    {
        Ok(bytes) => bytes,
        Err(error) => return ffi_fail_msg(format!("iterative centroid seed: {error:?}")),
    };
    let normalization = encode_kmeans_normalization(&means, &scales);
    let mut output = Vec::with_capacity(12 + state.len() + normalization.len());
    output.extend_from_slice(b"WKP1");
    output.extend_from_slice(&(state.len() as u32).to_le_bytes());
    output.extend_from_slice(&state);
    output.extend_from_slice(&(normalization.len() as u32).to_le_bytes());
    output.extend_from_slice(&normalization);
    store_iterative_output(output)
}

/// Merge two versioned K-means partials using the shared Rust kernel.
#[cfg(feature = "coordinator-iterative-ffi")]
#[no_mangle]
pub unsafe extern "C" fn wcol_iterative_kmeans_merge(
    left_ptr: *const u8,
    left_len: u32,
    right_ptr: *const u8,
    right_len: u32,
    dimensions: u32,
    clusters: u32,
    max_iterations: u32,
    tolerance: f64,
) -> u32 {
    if left_ptr.is_null()
        || right_ptr.is_null()
        || left_len == 0
        || right_len == 0
        || dimensions == 0
        || clusters == 0
        || max_iterations == 0
    {
        return ffi_fail("invalid iterative partial merge request");
    }
    let kernel = KMeansKernel {
        config: KMeansConfig {
            clusters: clusters as usize,
            dimensions: dimensions as usize,
            max_iterations,
            tolerance,
        },
    };
    let left = match kernel.decode_partial(slice::from_raw_parts(left_ptr, left_len as usize)) {
        Ok(partial) => partial,
        Err(error) => return ffi_fail_msg(format!("iterative partial decode: {error:?}")),
    };
    let right = match kernel.decode_partial(slice::from_raw_parts(right_ptr, right_len as usize)) {
        Ok(partial) => partial,
        Err(error) => return ffi_fail_msg(format!("iterative partial decode: {error:?}")),
    };
    match kernel
        .merge(left, right)
        .and_then(|partial| kernel.encode_partial(&partial))
    {
        Ok(output) => store_iterative_output(output),
        Err(error) => ffi_fail_msg(format!("iterative partial merge: {error:?}")),
    }
}

/// Apply one merged partial to global state. Output: `WKR1`, convergence byte, state length,
/// encoded state, then this round's inertia as f64.
#[cfg(feature = "coordinator-iterative-ffi")]
#[no_mangle]
pub unsafe extern "C" fn wcol_iterative_kmeans_update(
    state_ptr: *const u8,
    state_len: u32,
    partial_ptr: *const u8,
    partial_len: u32,
    dimensions: u32,
    clusters: u32,
    max_iterations: u32,
    tolerance: f64,
) -> u32 {
    if state_ptr.is_null()
        || partial_ptr.is_null()
        || state_len == 0
        || partial_len == 0
        || dimensions == 0
        || clusters == 0
        || max_iterations == 0
    {
        return ffi_fail("invalid iterative global update request");
    }
    let kernel = KMeansKernel {
        config: KMeansConfig {
            clusters: clusters as usize,
            dimensions: dimensions as usize,
            max_iterations,
            tolerance,
        },
    };
    let mut state = match kernel.decode_state(slice::from_raw_parts(state_ptr, state_len as usize))
    {
        Ok(state) => state,
        Err(error) => return ffi_fail_msg(format!("iterative state decode: {error:?}")),
    };
    let partial =
        match kernel.decode_partial(slice::from_raw_parts(partial_ptr, partial_len as usize)) {
            Ok(partial) => partial,
            Err(error) => return ffi_fail_msg(format!("iterative partial decode: {error:?}")),
        };
    let inertia = partial.inertia;
    let converged = match kernel.update(&mut state, partial) {
        Ok(converged) => converged,
        Err(error) => return ffi_fail_msg(format!("iterative state update: {error:?}")),
    };
    let state = match kernel.encode_state(&state) {
        Ok(state) => state,
        Err(error) => return ffi_fail_msg(format!("iterative state encode: {error:?}")),
    };
    let mut output = Vec::with_capacity(17 + state.len());
    output.extend_from_slice(b"WKR1");
    output.push(u8::from(converged));
    output.extend_from_slice(&(state.len() as u32).to_le_bytes());
    output.extend_from_slice(&state);
    output.extend_from_slice(&inertia.to_le_bytes());
    store_iterative_output(output)
}

/// Decode global state inside Rust and serialize the centroid values for the model artifact.
#[cfg(feature = "coordinator-iterative-ffi")]
#[no_mangle]
pub unsafe extern "C" fn wcol_iterative_kmeans_centroids_json(
    state_ptr: *const u8,
    state_len: u32,
    dimensions: u32,
    clusters: u32,
    max_iterations: u32,
    tolerance: f64,
) -> u32 {
    if state_ptr.is_null() || state_len == 0 || dimensions == 0 || clusters == 0 {
        return ffi_fail("invalid iterative state serialization request");
    }
    let kernel = KMeansKernel {
        config: KMeansConfig {
            clusters: clusters as usize,
            dimensions: dimensions as usize,
            max_iterations: max_iterations.max(1),
            tolerance,
        },
    };
    let state = match kernel.decode_state(slice::from_raw_parts(state_ptr, state_len as usize)) {
        Ok(state) => state,
        Err(error) => return ffi_fail_msg(format!("iterative state decode: {error:?}")),
    };
    let values = state
        .centroids
        .iter()
        .map(f64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    store_iterative_output(format!("{{\"centroids\":[{values}]}}").into_bytes())
}

#[cfg(feature = "coordinator-iterative-ffi")]
fn store_iterative_output(output: Vec<u8>) -> u32 {
    match u32::try_from(output.len()) {
        Ok(len) => {
            ITERATIVE_OUTPUT.with(|cell| *cell.borrow_mut() = output);
            clear_last_error();
            len
        }
        Err(_) => ffi_fail("iterative output exceeds 4 GiB"),
    }
}

#[cfg(feature = "coordinator-iterative-ffi")]
#[no_mangle]
pub extern "C" fn wcol_iterative_kmeans_output_ptr() -> *const u8 {
    ITERATIVE_OUTPUT.with(|output| output.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn wcs_alloc(len: u32) -> *mut u8 {
    if len == 0 {
        return ptr::null_mut();
    }
    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    // Hosts write through the pointer without checking it: out of memory must trap here,
    // not hand back null.
    let p = unsafe { alloc(layout) };
    if p.is_null() {
        handle_alloc_error(layout);
    }
    p
}

#[no_mangle]
pub unsafe extern "C" fn wcs_dealloc(p: *mut u8, len: u32) {
    if p.is_null() || len == 0 {
        return;
    }
    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    dealloc(p, layout);
}

#[no_mangle]
pub unsafe extern "C" fn wcs_last_error_ptr() -> *const u8 {
    crate::error::last_error_bytes()
        .map(|(ptr, _)| ptr)
        .unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn wcs_last_error_len() -> u32 {
    crate::error::last_error_bytes()
        .map(|(_, len)| len)
        .unwrap_or(0)
}

/// Groups in the hash table at the last aggregate finish. Zero when the program did not group.
#[no_mangle]
pub extern "C" fn wcs_last_resident_groups() -> u64 {
    crate::ops::query::batch::last_resident_groups()
}

/// Group-table bytes at the last aggregate finish. Zero when the program did not group.
#[no_mangle]
pub extern "C" fn wcs_last_resident_group_bytes() -> u64 {
    crate::ops::query::batch::last_resident_group_bytes()
}

#[no_mangle]
pub extern "C" fn wcs_last_spill_runs() -> u64 {
    crate::ops::query::batch::last_spill_runs()
}

#[no_mangle]
pub extern "C" fn wcs_last_spill_groups_flushed() -> u64 {
    crate::ops::query::batch::last_spill_groups_flushed()
}

#[no_mangle]
pub extern "C" fn wcs_last_spill_peak_live_bytes() -> u64 {
    crate::ops::query::batch::last_spill_peak_live_bytes()
}

#[no_mangle]
pub extern "C" fn wcs_dict_traffic_len() -> u32 {
    crate::exec::dict_traffic::PUBLISHED_BYTES
}

/// Growth samples: 11 little-endian u64s per row.
#[no_mangle]
pub extern "C" fn wcs_mem_growth_len() -> u32 {
    crate::exec::mem_growth::published_len()
}

#[no_mangle]
pub extern "C" fn wcs_mem_growth_ptr() -> *const u8 {
    crate::exec::mem_growth::published_words()
}

#[no_mangle]
pub extern "C" fn wcs_dict_traffic_ptr() -> *const u8 {
    crate::exec::dict_traffic::published_words()
}

#[no_mangle]
pub extern "C" fn wcs_last_spill_peak_interner_bytes() -> u64 {
    crate::ops::query::batch::last_spill_peak_interner_bytes()
}

/// Little-endian records of 27 u32s, one per completed spill run. The pointer is
/// valid until the next spill boundary or stream-pool open.
#[no_mangle]
pub extern "C" fn wcs_spill_boundary_len() -> u32 {
    crate::ops::query::batch::spill_boundary_wire().1
}

#[no_mangle]
pub extern "C" fn wcs_spill_boundary_ptr() -> *const u8 {
    crate::ops::query::batch::spill_boundary_wire().0
}

/// Function address kept alive by the cdylib. `*const ()` is not `Sync`.
/// The pointer is intentionally unread; the relocation is the point.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub struct FfiSymbol(*const ());
unsafe impl Sync for FfiSymbol {}
unsafe impl Send for FfiSymbol {}

/// Node and coordinator cdylib exports. Stream pool plus diagnostics.
/// Length of [`NODE_EXPORTED_FFI`]; crates that keep the symbols alive size their copy by it.
pub const NODE_EXPORTED_FFI_LEN: usize = 48;

pub static NODE_EXPORTED_FFI: [FfiSymbol; NODE_EXPORTED_FFI_LEN] = [
    FfiSymbol(wcs_alloc as *const ()),
    FfiSymbol(wcs_dealloc as *const ()),
    FfiSymbol(wcs_last_error_ptr as *const ()),
    FfiSymbol(wcs_last_error_len as *const ()),
    FfiSymbol(wcs_last_resident_groups as *const ()),
    FfiSymbol(wcs_last_resident_group_bytes as *const ()),
    FfiSymbol(wcs_last_spill_runs as *const ()),
    FfiSymbol(wcs_last_spill_groups_flushed as *const ()),
    FfiSymbol(wcs_last_spill_peak_live_bytes as *const ()),
    FfiSymbol(wcs_last_spill_peak_interner_bytes as *const ()),
    FfiSymbol(wcs_dict_traffic_len as *const ()),
    FfiSymbol(wcs_dict_traffic_ptr as *const ()),
    FfiSymbol(wcs_mem_growth_len as *const ()),
    FfiSymbol(wcs_mem_growth_ptr as *const ()),
    FfiSymbol(wcs_spill_boundary_len as *const ()),
    FfiSymbol(wcs_spill_boundary_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_is_resident as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_command as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_response_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_response_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_open_echo as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_open_graph as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_close as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_step as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_in_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_in_free as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_in_commit as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_in_end as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_in_absent as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_out_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_out_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_out_release as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_out_done as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_read_at_ticket as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_read_at_source as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_read_at_offset as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_read_at_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_complete_read_at as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_take_read_at as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_error_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_error_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_op as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_append_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_append_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_run_upload_len as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_run_upload_ptr as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_read_slot as *const ()),
    FfiSymbol(crate::exec::runtime::stream_pool_ffi::wcs_sp_spill_complete as *const ()),
];

/// Coordinator-only K-means kernel operations. Resident compute remains in the node bundle.
#[cfg(feature = "coordinator-iterative-ffi")]
pub static ITERATIVE_COORDINATOR_FFI: [FfiSymbol; 6] = [
    FfiSymbol(wcol_iterative_kmeans_merge_moments as *const ()),
    FfiSymbol(wcol_iterative_kmeans_prepare as *const ()),
    FfiSymbol(wcol_iterative_kmeans_output_ptr as *const ()),
    FfiSymbol(wcol_iterative_kmeans_merge as *const ()),
    FfiSymbol(wcol_iterative_kmeans_update as *const ()),
    FfiSymbol(wcol_iterative_kmeans_centroids_json as *const ()),
];
