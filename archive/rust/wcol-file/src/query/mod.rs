//! `.wcol` file query kernels (filters, masks, aggregates). File I/O stays in `formats/wcol/decode`.

pub mod agg;
pub mod compare;
pub mod filter;
pub mod filter_literals;
pub mod group;
pub mod group_dict_hist;
pub mod hll;
pub mod mask;
pub mod plan;
pub mod scan;
pub mod scale;
