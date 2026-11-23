//! Node Wasm. Relocations below keep the session/spill/partial FFI.
//! `ReduceOp` stays linked because `GraphHost` runs hash reducers. The
//! `wcol_reduce_*` C ABI is behind `reduce-ffi`, which this crate does not enable.

#![deny(unused, dead_code)]

#[allow(dead_code)]
#[used]
static KEEP_NODE_FFI: [wcol_engine::exec::runtime::ffi::FfiSymbol;
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI_LEN] =
    wcol_engine::exec::runtime::ffi::NODE_EXPORTED_FFI;
