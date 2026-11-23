//! Row frames: drain, sources, row egress and Parquet bytes.

use super::*;

impl GraphHostCore {
    pub(super) fn drain_row_frames(&mut self) -> WcolResult<()> {
        self.clear_hold_frames();
        self.reconcile_spill_host_reads()?;
        if self.spill_host_pending() && !self.spill_bridge_host_pending() {
            if let Some(aggregate) = self.row_aggregate.as_mut() {
                if aggregate.live_group_len() == 0 {
                    match aggregate.flush_live_run() {
                        Ok(SpillProgress::Ready(()) | SpillProgress::AwaitHost) => {}
                        Err(e) => {
                            return Err(WcolError::msg(format!("spill host drain failed: {e}")));
                        }
                    }
                }
            }
        }
        self.retry_spill_resume_input()?;
        if self.spill_blocks_rows() {
            self.release_finished_stream_dicts();
            return Ok(());
        }
        let frames = self.take_pending_frames();
        let mut leftover = Vec::new();
        let mut stop = false;
        for (port, frame) in frames {
            if stop {
                leftover.push((port, frame));
                continue;
            }
            match frame.frame_type {
                3 => self.handle_wcs_data_batch(port, &frame.payload)?,
                FRAME_PARTIAL => self.handle_wcs_partial_batch(&frame.payload)?,
                FRAME_DICTIONARY => self.handle_wcs_dictionary(port, &frame.payload)?,
                FRAME_END => self.handle_wcs_end(port, &frame.payload)?,
                _ => {}
            }
            if self.spill_blocks_rows() {
                stop = true;
            }
        }
        if !leftover.is_empty() {
            self.prepend_pending_frames(leftover);
        }
        self.release_finished_stream_dicts();
        Ok(())
    }

    pub(super) fn poll_wcs_parquet(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if let Some(progress) = self.take_parquet_bytes(output, 0)? {
            return Ok(progress);
        }
        self.drain_row_frames()?;
        if let Some(progress) = self.take_parquet_bytes(output, 0)? {
            return Ok(progress);
        }
        Ok(progress_after_io(0, 0, false))
    }

    pub(super) fn take_parquet_bytes(
        &mut self,
        output: &mut [u8],
        consumed: usize,
    ) -> WcolResult<Option<crate::stream::core::Progress>> {
        use crate::exec::graph_scheduler::ByteSinkDrive;
        match self
            .graph
            .drive_ingress_byte_sink(output)
            .map_err(|e| WcolError::msg(format!("parquet sink: {e}")))?
        {
            ByteSinkDrive::Wrote(n) => Ok(Some(progress_after_io(consumed, n, false))),
            ByteSinkDrive::Finished => Ok(Some(progress_after_io(consumed, 0, true))),
            ByteSinkDrive::NeedInput => Ok(None),
            ByteSinkDrive::AwaitHost => Ok(Some(progress_after_io(consumed, 0, false))),
            ByteSinkDrive::ReadAt {
                ticket,
                source,
                offset,
                length,
            } => Ok(Some(crate::stream::core::Progress::with_read_at(
                ticket, source, offset, length,
            ))),
        }
    }

    pub(super) fn poll_row_source(
        &mut self,
        input: &[u8],
        _eof: bool,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        use crate::stream::core::Progress;
        crate::exec::mem_growth::set_phase(crate::exec::mem_growth::PHASE_SCAN);
        if let Some(n) = self.write_queued_rows(output)? {
            return Ok(Progress {
                consumed: 0,
                produced: n,
                finished: false,
                ..Default::default()
            });
        }
        if self.row_end_queued && !self.row_egress_pending() {
            return Ok(Progress {
                consumed: 0,
                produced: 0,
                finished: true,
                ..Default::default()
            });
        }
        let mut slice = input;
        let consumed = input.len();
        let pump = self
            .graph
            .drive_ingest(&mut slice)
            .map_err(|e| WcolError::msg(format!("graph drive: {e}")))?;
        let input_consumed = consumed - slice.len();
        match pump {
            GraphPump::ReadAt {
                ticket,
                source,
                offset,
                length,
            } => {
                if input_consumed > 0 {
                    return Ok(Progress {
                        consumed: input_consumed,
                        produced: 0,
                        finished: false,
                        ..Default::default()
                    });
                }
                Ok(Progress::with_read_at(ticket, source, offset, length))
            }
            GraphPump::AwaitHost => Ok(Progress {
                consumed: input_consumed,
                produced: 0,
                finished: false,
                ..Default::default()
            }),
            GraphPump::Progress | GraphPump::RowsReady | GraphPump::Done => {
                let produced = self.write_queued_rows(output)?.unwrap_or(0);
                let finished = produced == 0 && self.row_end_queued && !self.row_egress_pending();
                Ok(Progress {
                    consumed: input_consumed,
                    produced,
                    finished,
                    ..Default::default()
                })
            }
        }
    }

    pub(super) fn poll_wcs_row_output(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if let Some(n) = self.write_queued_rows(output)? {
            return Ok(progress_after_io(0, n, false));
        }
        if self.row_end_queued && !self.row_egress_pending() {
            return Ok(progress_after_io(0, 0, true));
        }
        self.drain_row_frames()?;
        let produced = self.write_queued_rows(output)?.unwrap_or(0);
        let finished = produced == 0 && self.row_end_queued && !self.row_egress_pending();
        Ok(progress_after_io(0, produced, finished))
    }

    pub(super) fn row_egress_pending(&self) -> bool {
        if let Some(fanout) = &self.hash_fanout {
            return fanout
                .egress
                .iter()
                .any(|egress| egress.has_pending_output());
        }
        self.row_egress
            .as_ref()
            .is_some_and(|egress| egress.has_pending_output())
    }

    /// Write one queued frame. `Ok(None)` means nothing was waiting and the end frame is queued
    /// or the source is still running.
    pub(super) fn write_queued_rows(&mut self, output: &mut [u8]) -> WcolResult<Option<usize>> {
        self.stage_row_frame()?;
        if self.hash_fanout.is_some() {
            if self.direct_port_egress {
                return Ok(None);
            }
            return self.write_hash_frame(output);
        }
        let Some(egress) = self.row_egress.as_mut() else {
            return Ok(None);
        };
        if !egress.has_pending_output() {
            return Ok(None);
        }
        let n = egress.poll_write(output)?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(n))
    }

    pub(super) fn stage_row_frame(&mut self) -> WcolResult<()> {
        if self.hash_fanout.is_some() {
            return self.stage_hash_frame();
        }
        if self.row_egress_pending() || self.row_end_queued {
            return Ok(());
        }
        if let Some(batch) = self.graph.pop_row_batch() {
            let egress = self
                .row_egress
                .as_mut()
                .ok_or_else(|| WcolError::from("missing row egress"))?;
            push_row_frame(egress, &batch)?;
            return Ok(());
        }
        if self.graph.ingest_complete() {
            self.graph
                .flush_row_ops()
                .map_err(|e| WcolError::msg(format!("row flush failed: {e}")))?;
            if let Some(batch) = self.graph.pop_row_batch() {
                let egress = self
                    .row_egress
                    .as_mut()
                    .ok_or_else(|| WcolError::from("missing row egress"))?;
                push_row_frame(egress, &batch)?;
                return Ok(());
            }
            let egress = self
                .row_egress
                .as_mut()
                .ok_or_else(|| WcolError::from("missing row egress"))?;
            egress.push_end()?;
            self.row_end_queued = true;
        }
        Ok(())
    }
}
