//! Skip and then keep row-query rows. Filtering uses the selection mask.

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::stream::OwnedWcolBatch;

pub struct LimitOp {
    skip: u64,
    fetch: Option<u64>,
    seen: u64,
    emitted: u64,
    done: bool,
}

impl LimitOp {
    pub fn new(skip: u64, fetch: Option<u64>) -> Self {
        Self {
            skip,
            fetch,
            seen: 0,
            emitted: 0,
            done: false,
        }
    }

    fn take_rows(&mut self, batch: &OwnedWcolBatch) -> Result<Option<OwnedWcolBatch>, String> {
        if self.done {
            return Ok(None);
        }
        let mask = batch.effective_selection_mask();
        let mut keep = Vec::new();
        for row in 0..batch.row_count() {
            if !mask.is_selected(row) {
                continue;
            }
            if self.seen < self.skip {
                self.seen += 1;
                continue;
            }
            if self.fetch.is_some_and(|fetch| self.emitted >= fetch) {
                self.done = true;
                break;
            }
            keep.push(row);
            self.emitted += 1;
            self.seen += 1;
        }
        if self.fetch.is_some_and(|fetch| self.emitted >= fetch) {
            self.done = true;
        }
        if keep.is_empty() {
            return Ok(None);
        }
        let mut out = OwnedWcolBatch::empty();
        out.append_rows_from(batch, &keep)?;
        Ok(Some(out))
    }
}

impl Operator for LimitOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.done {
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        if let Some(slot) = io.input_slot.as_mut() {
            if let Some(item) = slot.take() {
                if let Some(batch) = self.take_rows(&item.into_batch())? {
                    *io.output = Some(WcolStreamItem::from_row_batch(batch));
                    return Ok(StreamProgress::HaveOutput);
                }
                if self.done {
                    return Ok(StreamProgress::Finished);
                }
                return Ok(StreamProgress::NeedInput);
            }
        } else if io.input_item().is_some() {
            return Err("limit requires OperatorIo::input_slot (zero-copy batch move)".to_string());
        }
        if io.input_eof {
            self.done = true;
            return Ok(StreamProgress::Finished);
        }
        Ok(StreamProgress::NeedInput)
    }
}
