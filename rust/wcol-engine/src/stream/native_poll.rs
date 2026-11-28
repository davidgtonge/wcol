//! Message for native drivers, which have no spill host to answer [`Action::SpillExec`].
//!
//! [`Action::SpillExec`]: super::core::Action::SpillExec

pub const SPILL_EXEC_NATIVE: &str = "SpillExec requires the Wasm R2 spill host";
