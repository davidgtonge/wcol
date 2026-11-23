//! Reduce: merge partial aggregate states (same schema).

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::stream::partials::{PartialStateMerger, PartialStateWcol};

pub use crate::ops::query::merge_partial_snapshots;

pub struct ReduceOp {
    merged: Option<PartialStateMerger>,
    finished: bool,
}

impl ReduceOp {
    pub fn new() -> Self {
        Self {
            merged: None,
            finished: false,
        }
    }

    /// Merge one partial into the running state. Only the merged state stays resident, so a
    /// caller can feed partials one at a time as they arrive.
    pub fn push(&mut self, partial: PartialStateWcol) -> Result<(), String> {
        match self.merged.as_mut() {
            None => self.merged = Some(PartialStateMerger::new(partial)),
            Some(merger) => merger.merge(&partial)?,
        }
        Ok(())
    }

    /// The merged state of every pushed partial.
    pub fn finish_state(&mut self) -> Result<PartialStateWcol, String> {
        self.finished = true;
        self.merged
            .take()
            .ok_or_else(|| "reduce received no partials".to_string())?
            .finish()
    }
}

impl Default for ReduceOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Operator for ReduceOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.finished {
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        // Take the input when the driver allows it, so its columns move instead of copying.
        let taken = io.input_slot.as_mut().and_then(|slot| slot.take());
        let partial = match taken {
            Some(item) => Some(item.into_partial_state()?),
            None => io
                .input_item()
                .map(|item| item.try_partial_state())
                .transpose()?,
        };
        if let Some(partial) = partial {
            self.push(partial)?;
            return Ok(StreamProgress::NeedInput);
        }
        if io.input_eof {
            let partial = self.finish_state()?;
            *io.output = Some(WcolStreamItem::from_partial_state(partial));
            return Ok(StreamProgress::HaveOutput);
        }
        Ok(StreamProgress::NeedInput)
    }
}
