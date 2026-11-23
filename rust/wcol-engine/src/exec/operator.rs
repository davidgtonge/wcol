//! Canonical WCOL execution contract (I-01): one [`Operator`] trait and [`OperatorIo`] lifecycle.
//!
//! Host byte ports stay outside this trait. Query operators do not parse WCS frames.

use std::any::Any;

use crate::stream::partials::{
    partial_state_from_batch, partial_state_from_owned_batch, partial_state_to_batch,
    PartialStateWcol,
};
use crate::stream::OwnedWcolBatch;

/// One message on the logical WCOL stream between operators (always an [`OwnedWcolBatch`]).
///
/// Row batches and aggregate partial state differ by [`OwnedWcolBatch::is_partial_batch`].
#[derive(Clone)]
pub struct WcolStreamItem(pub OwnedWcolBatch);

impl WcolStreamItem {
    pub fn batch(&self) -> &OwnedWcolBatch {
        &self.0
    }

    pub fn into_batch(self) -> OwnedWcolBatch {
        self.0
    }

    pub fn from_row_batch(batch: OwnedWcolBatch) -> Self {
        debug_assert!(batch.is_row_batch());
        Self(batch)
    }

    pub fn from_partial_state(state: PartialStateWcol) -> Self {
        Self(partial_state_to_batch(state))
    }

    pub fn as_row_batch(&self) -> Option<&OwnedWcolBatch> {
        if self.0.is_row_batch() {
            Some(&self.0)
        } else {
            None
        }
    }

    pub fn as_row_batch_mut(&mut self) -> Option<&mut OwnedWcolBatch> {
        if self.0.is_row_batch() {
            Some(&mut self.0)
        } else {
            None
        }
    }

    pub fn try_partial_state(&self) -> Result<PartialStateWcol, String> {
        partial_state_from_batch(&self.0)
    }

    /// Take the partial state out of this item without copying its columns.
    pub fn into_partial_state(self) -> Result<PartialStateWcol, String> {
        partial_state_from_owned_batch(self.0)
    }

    pub fn is_partial_batch(&self) -> bool {
        self.0.is_partial_batch()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamProgress {
    NeedInput,
    HaveOutput,
    Finished,
    Blocked,
    /// Source operator needs host bytes at `offset` (Parquet ReadAt, etc.).
    NeedHostRead {
        ticket: u32,
        source: u16,
        offset: u64,
        length: u32,
    },
}

/// Operator I/O: typed WCOL stream plus optional host byte offer for sources.
pub struct OperatorIo<'a, 'b> {
    pub input: Option<&'a WcolStreamItem>,
    /// When set, operators may [`Option::take`] the input item (moves batch without column copy).
    pub input_slot: Option<&'b mut Option<WcolStreamItem>>,
    pub input_eof: bool,
    pub output: &'b mut Option<WcolStreamItem>,
    /// When set, source operators may consume a prefix (advanced by the operator).
    pub host_bytes: Option<&'a [u8]>,
    /// Sink operators write encoded bytes into this slice (may be partial per poll).
    pub output_port: Option<&'a mut [u8]>,
    /// Bytes copied into [`Self::output_port`] on the last poll (sinks only).
    pub bytes_written: &'b mut usize,
}

impl<'a, 'b> OperatorIo<'a, 'b> {
    pub fn output_slot_free(&self) -> bool {
        self.output.is_none()
    }

    pub fn input_item(&self) -> Option<&WcolStreamItem> {
        if let Some(item) = self.input {
            return Some(item);
        }
        match &self.input_slot {
            Some(slot) => (*slot).as_ref(),
            None => None,
        }
    }
}

/// Query, source, and sink operators on the logical WCOL stream.
pub trait Operator: Any {
    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String>;

    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}
