//! Resident graphs: loaded once, then driven by host commands.

use super::*;

impl GraphHost {
    pub fn is_resident(&self) -> bool {
        self.core.resident.is_some()
    }

    pub fn resident_ready(&self) -> bool {
        self.core
            .resident
            .as_ref()
            .is_some_and(|resident| resident.ready())
    }

    pub fn resident_response(&self) -> &[u8] {
        self.core
            .resident
            .as_ref()
            .map_or(&[], |resident| resident.response())
    }

    pub fn resident_command(&mut self, kind: u32, bytes: &[u8]) -> WcolResult<()> {
        self.core
            .resident
            .as_mut()
            .ok_or_else(|| WcolError::from("graph is not resident"))?
            .command(kind, bytes)
            .map_err(WcolError::msg)
    }

    pub(super) fn poll_resident_graph(&mut self) -> WcolResult<Action> {
        let resident = self.core.resident.as_mut().expect("resident graph");
        if !resident.loading() {
            resident.poll_command().map_err(WcolError::msg)?;
            return Ok(Action::NeedInput);
        }
        if let Some(batch) = self.core.graph.pop_row_batch() {
            resident.load_batch(&batch).map_err(WcolError::msg)?;
            return Ok(Action::Yield);
        }
        if self.core.graph.ingest_complete() {
            self.core.graph.flush_row_ops().map_err(WcolError::msg)?;
            if let Some(batch) = self.core.graph.pop_row_batch() {
                resident.load_batch(&batch).map_err(WcolError::msg)?;
                return Ok(Action::Yield);
            }
            resident.finish_load().map_err(WcolError::msg)?;
            return Ok(Action::NeedInput);
        }
        match self
            .core
            .graph
            .drive_ingest(&mut &[][..])
            .map_err(WcolError::msg)?
        {
            GraphPump::ReadAt {
                ticket,
                source,
                offset,
                length,
            } => {
                self.read_at_ticket = ticket;
                self.read_at_source = source;
                self.read_at_offset = offset;
                self.read_at_len = length;
                Ok(Action::ReadAt)
            }
            GraphPump::AwaitHost => Ok(Action::NeedInput),
            GraphPump::Progress | GraphPump::RowsReady | GraphPump::Done => Ok(Action::Yield),
        }
    }
}
