//! Coordinator Wasm. Planning (`wcol_plan_query`), streaming reduce, and the
//! engine session used for coordinator fragments.

#![deny(unused, dead_code)]

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
