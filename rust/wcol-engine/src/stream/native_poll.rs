//! Exhaustive native poll handling for integration tests (no Wasm spill host).

use super::core::Action;

pub const SPILL_EXEC_NATIVE: &str = "SpillExec requires the Wasm DO SQLite spill host";

/// Map a poll action for native drivers that do not implement spill I/O.
pub fn map_native_poll(action: Action) -> Result<NativePollStep, String> {
    match action {
        Action::NeedInput => Ok(NativePollStep::NeedInput),
        Action::HaveOutput => Ok(NativePollStep::HaveOutput),
        Action::Finished => Ok(NativePollStep::Finished),
        Action::Yield => Ok(NativePollStep::Yield),
        Action::ReadAt => Ok(NativePollStep::ReadAt),
        Action::SpillExec => Err(SPILL_EXEC_NATIVE.into()),
        Action::Failed => Ok(NativePollStep::Failed),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativePollStep {
    NeedInput,
    HaveOutput,
    Finished,
    Yield,
    ReadAt,
    Failed,
}
