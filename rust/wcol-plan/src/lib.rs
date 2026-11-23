//! Coordinator planner. Depends on `wcol-engine`. The engine does not depend on this crate.

#![deny(unused, dead_code)]

pub use wcol_engine::error;
pub use wcol_engine::exec;
pub use wcol_engine::formats;
pub use wcol_engine::ir;
pub use wcol_engine::ops;
pub use wcol_engine::program;
pub use wcol_engine::stream;

pub mod plan;

#[cfg(test)]
mod batch_parity;
