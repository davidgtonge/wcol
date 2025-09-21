//! # wcol-decoder
//!
//! **Wasm-safe surface** (always built for `wcol-wasm`): `decode/`, `ffi/`, `query/`, `parse/`,
//! `runtime/`, `types/`, and optional `sql_plan` behind `sql_api`.
//!
//! Optional `timing` feature collects plan/decode stage timings (requires `wcol_now_ms` on wasm).
//!
//! **Native-only** (`#[cfg(not(target_arch = "wasm32"))]`): `native` and any experimental engines
//! (`simple`, `simple_wcol`). These must not be linked into the wasm32 build graph.
#[cfg(feature = "bench")]
pub mod bench;
mod constants;
mod decode;
mod exec;
#[cfg(not(feature = "bench"))]
mod ffi;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub mod native;
mod parse;
mod query;
mod runtime;
// When merging native-only experiments, gate each module:
// #[cfg(not(target_arch = "wasm32"))]
// pub mod simple;
// #[cfg(not(target_arch = "wasm32"))]
// pub mod simple_wcol;
#[cfg(feature = "sql_api")]
mod sql_plan;
mod timing;
mod types;

#[allow(unused_imports)]
pub(crate) use constants::*;
#[allow(unused_imports)]
pub(crate) use decode::*;
#[allow(unused_imports)]
pub(crate) use parse::*;
#[allow(unused_imports)]
pub(crate) use runtime::*;
#[allow(unused_imports)]
pub(crate) use types::*;

#[cfg(test)]
mod tests;
