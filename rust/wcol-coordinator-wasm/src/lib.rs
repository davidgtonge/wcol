//! Coordinator Wasm. Planning (`wcol_plan_query`), streaming reduce, and the
//! engine session used for coordinator fragments.

#![deny(unused, dead_code)]

/// `getrandom` on `wasm32-unknown-unknown` has no OS entropy. Planning does not need any.
#[cfg(all(
    feature = "datafusion",
    target_arch = "wasm32",
    getrandom_backend = "custom"
))]
#[no_mangle]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom::Error> {
    if !dest.is_null() {
        core::ptr::write_bytes(dest, 0, len);
    }
    Ok(())
}

#[allow(dead_code)]
#[used]
static KEEP_NODE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol;
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI_LEN] =
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI;

#[allow(dead_code)]
#[used]
static KEEP_ITERATIVE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol; 6] =
    wcol_engine::exec::runtime::ffi::ITERATIVE_COORDINATOR_FFI;

#[allow(dead_code)]
#[used]
static KEEP_REDUCE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol; 5] =
    wcol_engine::exec::runtime::ffi::REDUCE_EXPORTED_FFI;

#[allow(dead_code)]
#[used]
static KEEP_PLAN_FFI: [wcol_plan::plan::host_ffi::FfiSymbol;
    wcol_plan::plan::host_ffi::EXPORTED_FFI_LEN] = wcol_plan::plan::host_ffi::EXPORTED_FFI;
