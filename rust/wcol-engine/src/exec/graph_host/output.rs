//! Output polling: partial payloads, Parquet, byte sinks, finalize and shard export.

use super::*;

impl GraphHostCore {
    pub(super) fn ensure_partial_payload(&mut self) -> WcolResult<()> {
        if self.wcs_has_pending_row_work() {
            return Ok(());
        }
        if !self.partial_payloads.is_empty() || self.spill_chunks_finished() {
            return Ok(());
        }
        if let Some(aggregate) = self.row_aggregate.as_mut() {
            if aggregate.has_group_spill() && aggregate.needs_spill_chunk_finish() {
                match aggregate.begin_finish_chunks() {
                    Ok(SpillProgress::Ready(())) => {}
                    Ok(SpillProgress::AwaitHost) => return Ok(()),
                    Err(e) => {
                        return Err(WcolError::msg(format!("hash reducer finish failed: {e}")));
                    }
                }
                // A chunk that only merges into the distinct-count buffer sends nothing,
                // so keep pulling until a payload is queued, the host owes I/O, or finish ends.
                while self.partial_payloads.is_empty() {
                    let aggregate = self.row_aggregate.as_mut().expect("row aggregate");
                    match aggregate.finish_next_chunk() {
                        Ok(SpillProgress::Ready(Some(partial))) => {
                            self.push_finish_chunk(&partial)?
                        }
                        Ok(SpillProgress::Ready(None)) => {
                            self.flush_distinct_counts()?;
                            let aggregate = self.row_aggregate.as_mut().expect("row aggregate");
                            match aggregate.finish_chunks_done() {
                                Ok(SpillProgress::Ready(())) => {
                                    self.set_chunks_finished();
                                    return Ok(());
                                }
                                Ok(SpillProgress::AwaitHost) => return Ok(()),
                                Err(e) => {
                                    return Err(WcolError::msg(format!(
                                        "hash reducer finish failed: {e}"
                                    )));
                                }
                            }
                        }
                        Ok(SpillProgress::AwaitHost) => return Ok(()),
                        Err(e) => {
                            return Err(WcolError::msg(format!("hash reducer finish failed: {e}")));
                        }
                    }
                }
                return Ok(());
            } else {
                let partial = match aggregate.finish_partial_state() {
                    Ok(SpillProgress::Ready(partial)) => partial,
                    Ok(SpillProgress::AwaitHost) => return Ok(()),
                    Err(e) => {
                        return Err(WcolError::msg(format!("hash reducer finish failed: {e}")));
                    }
                };
                if has_count_distinct(&partial) {
                    self.push_finish_chunk(&partial)?;
                    self.flush_distinct_counts()?;
                } else {
                    self.enqueue_partial_state(&partial)?;
                }
                return Ok(());
            }
        }
        if let Some(mut reduce) = self.reduce.take() {
            let merged = reduce
                .finish_state()
                .map_err(|e| WcolError::msg(format!("partial reduce finish failed: {e}")))?;
            self.enqueue_partial_state(&merged)?;
            return Ok(());
        }
        let partial = self
            .graph
            .finish_partial_state()
            .map_err(|e| WcolError::msg(format!("partial finish failed: {e}")))?;
        self.enqueue_partial_state(&partial)?;
        Ok(())
    }

    /// Send one spilled finish chunk. Distinct counts are merged first: merged pairs arrive in
    /// key order with the group parts leading, so a chunk of 256 pairs often holds one or two
    /// groups, and sending each would make the finalizer merge thousands of tiny partials.
    /// Splitting a group across sends stays exact: the finalizer sums counts.
    pub(super) fn push_finish_chunk(
        &mut self,
        partial: &crate::stream::partials::PartialStateWcol,
    ) -> WcolResult<()> {
        if !has_count_distinct(partial) {
            return self.enqueue_partial_state(partial);
        }
        let counts = distinct_counts_of(partial)
            .map_err(|e| WcolError::msg(format!("hash reducer distinct counts: {e}")))?;
        let full = match &mut self.spill_drive.distinct {
            DistinctFold::Rank(top) => {
                top.as_mut()
                    .ok_or_else(|| WcolError::from("ranked distinct counts already sent"))?
                    .push(counts)
                    .map_err(|e| WcolError::msg(format!("hash reducer distinct counts: {e}")))?;
                false
            }
            DistinctFold::Merge(Some(merger)) => {
                merger
                    .merge(&counts)
                    .map_err(|e| WcolError::msg(format!("hash reducer distinct counts: {e}")))?;
                merger.group_count() >= DISTINCT_COUNT_CHUNK_GROUPS
            }
            DistinctFold::Merge(slot) => {
                *slot = Some(crate::ops::query::PartialSnapshotMerger::new(counts));
                false
            }
        };
        if full {
            self.flush_distinct_counts()?;
        }
        Ok(())
    }

    /// Send the counts held back: the coalesced chunk, or once at the end, the ranked top.
    pub(super) fn flush_distinct_counts(&mut self) -> WcolResult<()> {
        let snapshot = match &mut self.spill_drive.distinct {
            DistinctFold::Merge(merger) => merger.take().map(|merger| merger.finish()),
            DistinctFold::Rank(top) => top.take().and_then(|top| top.finish()),
        };
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let state = crate::stream::partials::partial_state_from_snapshot(&snapshot)
            .map_err(|e| WcolError::msg(format!("hash reducer distinct counts: {e}")))?;
        self.enqueue_partial_state(&state)
    }

    pub(super) fn enqueue_partial_state(
        &mut self,
        state: &crate::stream::partials::PartialStateWcol,
    ) -> WcolResult<()> {
        let frames = partial_state_payloads(state, MAX_WCS_PAYLOAD)
            .map_err(|e| WcolError::msg(format!("partial encode failed: {e}")))?;
        self.partial_payloads.extend(frames);
        Ok(())
    }

    pub(super) fn emit_partial_frame(&mut self, output: &mut [u8]) -> WcolResult<usize> {
        self.ensure_partial_payload()?;
        if self.partial_payloads.is_empty() {
            if self.spill_chunks_finished() {
                self.emit.finish()?;
                return self.emit.flush(output);
            }
            return Ok(0);
        }
        let chunked_spill = self.spill_chunks_finished()
            || self
                .row_aggregate
                .as_ref()
                .is_some_and(|agg| agg.has_group_spill());
        let partial = self.partial_payloads.pop_front().expect("partial payload");
        if chunked_spill || !self.partial_payloads.is_empty() {
            self.emit.emit_more(&partial, output)
        } else {
            self.emit.emit(&partial, output)
        }
    }

    pub(super) fn poll_host_parquet(
        &mut self,
        input: &[u8],
        eof: bool,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        use crate::stream::core::Progress;
        crate::exec::mem_growth::set_phase(crate::exec::mem_growth::PHASE_SCAN);
        if self.row_egress.is_some() || self.hash_fanout.is_some() {
            return self.poll_row_source(input, eof, output);
        }
        if self.graph.has_byte_sink() {
            return self.poll_byte_sink(output);
        }
        if self.emit.wire_complete() {
            return Ok(Progress {
                consumed: 0,
                produced: 0,
                finished: true,
                ..Default::default()
            });
        }
        let flushed = self.emit.flush(output)?;
        if flushed > 0 {
            return Ok(Progress {
                consumed: 0,
                produced: flushed,
                finished: false,
                ..Default::default()
            });
        }
        let mut slice = input;
        let consumed = input.len();
        let pump = self
            .graph
            .drive_ingest(&mut slice)
            .map_err(|e| WcolError::msg(format!("graph drive: {e}")))?;
        match pump {
            GraphPump::ReadAt {
                ticket,
                source,
                offset,
                length,
            } => {
                let input_consumed = consumed - slice.len();
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
            GraphPump::AwaitHost => {
                let input_consumed = consumed - slice.len();
                Ok(Progress {
                    consumed: input_consumed,
                    produced: 0,
                    finished: false,
                    ..Default::default()
                })
            }
            GraphPump::Progress => unreachable!("drive_ingest loops through Progress"),
            GraphPump::RowsReady => Err(WcolError::from("aggregate graph queued a row batch")),
            GraphPump::Done => {
                let input_consumed = consumed - slice.len();
                let produced = self.emit_partial_frame(output)?;
                let finished = (eof || input_consumed == consumed)
                    && self.emit.wire_complete()
                    && !self.spill_host_pending();
                Ok(Progress {
                    consumed: input_consumed,
                    produced,
                    finished,
                    ..Default::default()
                })
            }
        }
    }

    pub(super) fn poll_byte_sink(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        use crate::stream::core::Progress;
        match self
            .graph
            .drive_byte_sink(output)
            .map_err(|e| WcolError::msg(format!("graph drive: {e}")))?
        {
            ByteSinkDrive::ReadAt {
                ticket,
                source,
                offset,
                length,
            } => Ok(Progress::with_read_at(ticket, source, offset, length)),
            ByteSinkDrive::AwaitHost => Ok(Progress {
                consumed: 0,
                produced: 0,
                finished: false,
                ..Default::default()
            }),
            ByteSinkDrive::Wrote(n) => Ok(Progress {
                consumed: 0,
                produced: n,
                finished: false,
                ..Default::default()
            }),
            ByteSinkDrive::Finished => Ok(Progress {
                consumed: 0,
                produced: 0,
                finished: true,
                ..Default::default()
            }),
            ByteSinkDrive::NeedInput => {
                Err(WcolError::from("parquet sink waited for ingress on a scan"))
            }
        }
    }

    pub(super) fn handle_wcs_end(&mut self, _port: u32, payload: &[u8]) -> WcolResult<()> {
        let _ = payload;
        self.try_finish_wcs();
        Ok(())
    }

    pub(super) fn handle_wcs_partial_frame(&mut self, payload: &[u8]) -> WcolResult<()> {
        use crate::exec::runtime::wcol_stream::partial_state_from_wcs_bytes;
        let partial = partial_state_from_wcs_bytes(payload)
            .map_err(|e| WcolError::msg(format!("partial frame decode failed: {e}")))?;
        let reduce = self
            .reduce
            .as_mut()
            .ok_or_else(|| WcolError::from("partial reduce missing state"))?;
        reduce
            .push(partial)
            .map_err(|e| WcolError::msg(format!("partial reduce push failed: {e}")))?;
        Ok(())
    }

    /// Finalizer: partial frames feed the reduce; once every producer has ended, the merged
    /// groups go into the row graph as one batch and the graph's rows or Parquet go out.
    pub(super) fn poll_wcs_finalize(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if matches!(self.finalize, FinalizePhase::Streaming) {
            return self.poll_shard_export(output);
        }
        if !self.is_finalized() {
            for (port, frame) in self.take_pending_frames() {
                match frame.frame_type {
                    FRAME_PARTIAL => self.handle_wcs_partial_frame(&frame.payload)?,
                    FRAME_END => self.handle_wcs_end(port, &frame.payload)?,
                    _ => {}
                }
            }
            if !self.wcs_input_ended() {
                return Ok(progress_after_io(0, 0, false));
            }
            let merged = self
                .reduce
                .take()
                .ok_or_else(|| WcolError::from("finalizer missing reduce state"))?
                .finish_state()
                .map_err(|e| WcolError::msg(format!("partial reduce finish failed: {e}")))?;
            let snap = crate::stream::partials::partial_snapshot_from_state(&merged)
                .map_err(|e| WcolError::msg(format!("partial snapshot failed: {e}")))?;
            let batch = crate::ops::finalize::finalized_groups_batch(&snap)
                .map_err(|e| WcolError::msg(format!("finalize failed: {e}")))?;
            self.graph
                .inject_row_batch(batch)
                .map_err(|e| WcolError::msg(format!("finalize inject failed: {e}")))?;
            self.graph.mark_ingest_done();
            self.finalize = FinalizePhase::Emitted;
        }
        if self.graph.has_byte_sink() {
            if let Some(progress) = self.take_parquet_bytes(output, 0)? {
                return Ok(progress);
            }
            return Ok(progress_after_io(0, 0, false));
        }
        let produced = self.write_queued_rows(output)?.unwrap_or(0);
        let finished = produced == 0 && self.row_end_queued && !self.row_egress_pending();
        Ok(progress_after_io(0, produced, finished))
    }

    /// Drain the sink first, so at most the frames landed since the last poll wait as rows.
    pub(super) fn poll_shard_export(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if let Some(progress) = self.take_parquet_bytes(output, 0)? {
            return Ok(progress);
        }
        for (port, frame) in self.take_pending_frames() {
            match frame.frame_type {
                FRAME_PARTIAL => {
                    use crate::exec::runtime::wcol_stream::partial_state_from_wcs_bytes;
                    let partial = partial_state_from_wcs_bytes(&frame.payload)
                        .map_err(|e| WcolError::msg(format!("partial frame decode failed: {e}")))?;
                    let snap = crate::stream::partials::partial_snapshot_from_state(&partial)
                        .map_err(|e| WcolError::msg(format!("partial snapshot failed: {e}")))?;
                    let batch = crate::ops::finalize::finalized_groups_batch(&snap)
                        .map_err(|e| WcolError::msg(format!("finalize failed: {e}")))?;
                    self.graph
                        .inject_row_batch(batch)
                        .map_err(|e| WcolError::msg(format!("shard export inject failed: {e}")))?;
                }
                FRAME_END => self.handle_wcs_end(port, &frame.payload)?,
                _ => {}
            }
        }
        if self.wcs_input_ended() {
            self.graph.mark_ingest_done();
            self.finalize = FinalizePhase::Emitted;
        }
        Ok(self
            .take_parquet_bytes(output, 0)?
            .unwrap_or_else(|| progress_after_io(0, 0, false)))
    }

    pub(super) fn poll_wcs_partial_reduce(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if self.emit.wire_complete() {
            return Ok(progress_after_io(0, 0, true));
        }
        let flushed = self.emit.flush(output)?;
        if flushed > 0 {
            return Ok(progress_after_io(0, flushed, self.emit.wire_complete()));
        }
        self.reconcile_spill_host_reads()?;
        for (port, frame) in self.take_pending_frames() {
            match frame.frame_type {
                FRAME_PARTIAL => self.handle_wcs_partial_frame(&frame.payload)?,
                FRAME_END => self.handle_wcs_end(port, &frame.payload)?,
                _ => {}
            }
        }
        if self.wcs_input_ended() {
            let produced = self.emit_partial_frame(output)?;
            if produced == 0 {
                if self.spill_host_pending() {
                    return Ok(progress_after_io(0, 0, false));
                }
                let flushed = self.emit.flush(output)?;
                return Ok(progress_after_io(0, flushed, self.emit.wire_complete()));
            }
            return Ok(progress_after_io(0, produced, self.emit.wire_complete()));
        }
        Ok(progress_after_io(0, 0, false))
    }
}
