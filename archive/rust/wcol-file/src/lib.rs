//! **Archived.** On-disk `.wcol` v7 engine: layout constants, decode runtime, native encoder and
//! file query kernels. Kept for the browser explorer and legacy tooling; new work goes in
//! `rust/wcol` (Parquet → WCS stream → GraphHost). See `archive/README.md`.

pub mod on_disk;

pub mod query;

pub mod decode;

#[cfg(not(target_arch = "wasm32"))]
pub mod encode;

pub use on_disk::*;
