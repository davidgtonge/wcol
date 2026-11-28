//! Host side of a sorted-run spill: expose the request, take the completion.

use super::*;

impl GraphHost {
    pub fn spill_op(&self) -> u8 {
        self.spill_op
    }

    pub fn spill_append_bytes(&self) -> &[u8] {
        &self.spill_append_buf
    }

    /// Where the host writes the pending run read. Valid until the next Wasm allocation, so
    /// the host fills it immediately and then calls `complete_spill`.
    pub fn spill_run_read_slot(&mut self, len: usize) -> WcolResult<*mut u8> {
        if self.spill_op != crate::spill::SPILL_OP_RUN_READ {
            return Err(WcolError::from("run read slot with no pending run read"));
        }
        let bridge = self
            .core
            .spill_bridge
            .as_ref()
            .ok_or_else(|| WcolError::from("spill read without session"))?;
        Ok(bridge.borrow_mut().run_read_slot(len).as_mut_ptr())
    }

    /// Take the host's answer to the pending spill op. A failed host op (`ok == false`)
    /// fails the session, so the next step reports it, and is not itself an error: the
    /// host still holds the original I/O error and rethrows that.
    pub fn complete_spill(&mut self, ok: bool) -> WcolResult<()> {
        if !ok {
            self.spill_op = 0;
            self.fail_message("spill host operation failed");
            return Ok(());
        }
        let Some(bridge) = self.core.spill_bridge.as_ref() else {
            return Err(WcolError::from("spill complete without session"));
        };
        let mut bridge = bridge.borrow_mut();
        let op = self.spill_op;
        if op == 0 {
            return Err(WcolError::from("spill complete with no pending op"));
        }
        match op {
            crate::spill::SPILL_OP_RUN_PUT => {
                bridge.complete_run_put().map_err(WcolError::msg)?;
            }
            crate::spill::SPILL_OP_RUN_COMMIT => {
                bridge.complete_run_commit().map_err(WcolError::msg)?;
            }
            crate::spill::SPILL_OP_RUN_READ => {
                bridge.complete_run_read().map_err(WcolError::msg)?;
            }
            _ => return Err(WcolError::from("unknown spill op")),
        }
        drop(bridge);
        if op == crate::spill::SPILL_OP_RUN_PUT {
            // Hand the uploaded body back for the next run to write into. Wasm memory never
            // shrinks, so a freed body only fragments it: the refilling table takes part of
            // the hole and the next run's body grows the memory by a whole run again.
            let mut spare = std::mem::take(&mut self.spill_run_upload_mirror);
            spare.clear();
            if let Some(bridge) = self.core.spill_bridge.as_ref() {
                bridge.borrow_mut().run_upload = spare;
            }
            if let Some(aggregate) = self.core.row_aggregate.as_mut() {
                aggregate
                    .on_spill_run_put_handoff()
                    .map_err(WcolError::msg)?;
            }
        } else if op == crate::spill::SPILL_OP_RUN_COMMIT {
            if let Some(aggregate) = self.core.row_aggregate.as_mut() {
                aggregate
                    .on_spill_run_commit_host_complete()
                    .map_err(WcolError::msg)?;
            }
        } else if op == crate::spill::SPILL_OP_RUN_READ {
            if let Some(aggregate) = self.core.row_aggregate.as_mut() {
                aggregate.ingest_spill_host_read().map_err(WcolError::msg)?;
            }
        }
        self.spill_op = 0;
        Ok(())
    }

    pub(super) fn fill_spill_exec(&mut self, req: crate::spill::HostSpillRequest) {
        self.spill_op = req.op;
        self.spill_append_buf = req.append_bytes;
        if req.op == crate::spill::SPILL_OP_RUN_PUT {
            if let Some(bridge) = self.core.spill_bridge.as_ref() {
                self.spill_run_upload_mirror = std::mem::take(&mut bridge.borrow_mut().run_upload);
            }
        }
    }

    pub(super) fn expose_spill_exec(&mut self) -> bool {
        if self.spill_op != 0 {
            return true;
        }
        let Some(bridge) = self.core.spill_bridge.as_ref() else {
            return false;
        };
        let mut bridge = bridge.borrow_mut();
        let Some(req) = bridge.take_pending() else {
            // Recover a stranded host_inflight bit with no queued op and no
            // exposed spill_op — otherwise finish_put parks forever.
            bridge.clear_stranded_inflight();
            return false;
        };
        drop(bridge);
        self.fill_spill_exec(req);
        true
    }

    pub fn spill_run_upload_body(&self) -> &[u8] {
        &self.spill_run_upload_mirror
    }
}
