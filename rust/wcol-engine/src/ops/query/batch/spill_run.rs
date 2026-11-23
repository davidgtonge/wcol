//! Flush the live table as sorted runs and consume rows while spilling.

use super::*;

impl BatchQuery {
    /// Flush the live hash table as one sorted spill run (canonical key order), then clear groups.
    pub(crate) fn flush_live_run(&mut self) -> Result<SpillProgress<()>, String> {
        if self.spill.is_none() {
            return Ok(SpillProgress::Ready(()));
        }
        spill_pull!(self.spill.as_mut().expect("spill").reconcile_host_append());
        if self.live_group_len() == 0 {
            spill_pull!(self.drain_spill_finish_runs());
            if let Some(spill) = self.spill.as_ref() {
                publish_spill_stats(spill.stats());
            }
            return Ok(SpillProgress::Ready(()));
        }
        let _flush = crate::exec::trace::guard("flush", self.live_group_len() as u64);
        crate::exec::phase::mark(
            crate::exec::phase::FLUSH_ENTER,
            0,
            self.live_group_len() as u32,
            0,
            u32::try_from(self.resident_state_bytes()).unwrap_or(u32::MAX),
        );
        if self.spill_flush_at == 0 {
            let live_before = crate::formats::parquet::alloc_track::snapshot().live_bytes;
            reset_flush_probe(live_before.min(u32::MAX as usize) as u32);
            FLUSH_START.with(|cell| {
                cell.set(Some(FlushStart {
                    wasm_before: wasm_linear_bytes(),
                    live_groups: self.live_group_len() as u32,
                    live_table: self.groups.resident_bytes() as u32,
                    interner_before: self.interner.resident_bytes() as u32,
                }))
            });
            self.prepare_spill_flush_order();
            let rows = self.spill_sort_scratch.len();
            self.spill.as_mut().expect("spill").expect_run_rows(rows);
        }
        let n = self.spill_sort_scratch.len();
        let max_rows = self.spill.as_ref().expect("spill").config().max_batch_rows;
        let max_bytes = self.spill.as_ref().expect("spill").config().max_batch_bytes;
        while self.spill_flush_at < n {
            let mut rows = Vec::new();
            let mut used = 0usize;
            let mut next = self.spill_flush_at;
            while next < n && rows.len() < max_rows {
                let slot = self.spill_sort_scratch[next];
                let row = if self.pairs.is_some() {
                    self.encode_pair_slot(slot)?
                } else {
                    self.encode_main_slot(slot)?
                };
                let row_bytes = row.0.len().saturating_add(row.1.len());
                if !rows.is_empty() && used.saturating_add(row_bytes) > max_bytes {
                    break;
                }
                used = used.saturating_add(row_bytes);
                rows.push(row);
                next += 1;
            }
            let page_cap = rows.iter().fold(0usize, |sum, (key, value)| {
                sum.saturating_add(key.capacity())
                    .saturating_add(value.capacity())
            }) as u32;
            if page_cap > 0 {
                LAST_PAGE.with(|cell| cell.set((used as u32, page_cap)));
                note_spill_serialize(used as u32, page_cap);
            }
            match self.spill.as_mut().expect("spill").append_run_page(&rows)? {
                SpillProgress::Ready(()) => self.spill_flush_at = next,
                SpillProgress::AwaitHost => {
                    // `begin` awaits before the page is stored. Leaving the cursor
                    // here writes that page on resume instead of skipping it.
                    if self.spill.as_ref().expect("spill").append_queued() {
                        self.spill_flush_at = next;
                    }
                    self.stamp_memory_phase(crate::exec::mem_growth::PHASE_SPILL_WRITE);
                    return Ok(SpillProgress::AwaitHost);
                }
            }
        }
        let sql_acked_live = crate::formats::parquet::alloc_track::snapshot()
            .live_bytes
            .min(u32::MAX as usize) as u32;
        FLUSH_PROBE.with(|cell| {
            let mut probe = cell.get();
            probe.live_sql_acked = sql_acked_live;
            cell.set(probe);
        });
        self.reset_live_after_run();
        if n > 0 {
            if let Some(spill) = self.spill.as_ref() {
                if !spill.needs_finish_run() {
                    self.spill.as_mut().expect("spill").note_run_flushed();
                }
            }
        }
        let start = FLUSH_START.with(|cell| cell.take());
        let (page_bytes, page_cap) = LAST_PAGE.with(|cell| cell.get());
        if let Some(start) = start {
            let run = self
                .spill
                .as_ref()
                .map(|spill| spill.stats().runs)
                .unwrap_or(0)
                .saturating_add(1);
            let mem = crate::formats::parquet::alloc_track::snapshot();
            let probe = FLUSH_PROBE.with(|cell| cell.get());
            let (merge_scan_begins, merge_steps) = spill_merge_counters();
            note_spill_boundary(SpillBoundary {
                run: u32::try_from(run).unwrap_or(u32::MAX),
                wasm_before: start.wasm_before,
                wasm_after: wasm_linear_bytes(),
                live_groups: start.live_groups,
                live_table_before: start.live_table,
                table_after: self.groups.resident_bytes() as u32,
                index_cap_after: self.groups.index_capacity() as u32,
                interner_before: start.interner_before,
                interner_after: self.interner.resident_bytes() as u32,
                sort_cap: self.spill_sort_scratch.capacity() as u32,
                page_bytes,
                page_cap,
                live_after: mem.live_bytes.min(u32::MAX as usize) as u32,
                peak_after: mem.peak_bytes.min(u32::MAX as usize) as u32,
                live_before: probe.live_before,
                serialize_bytes: probe.serialize_bytes,
                append_buf_cap: probe.append_buf_cap,
                io_encode_cap: probe.io_encode_cap,
                host_pending_ops: probe.host_pending_ops,
                sqlite_write_bytes: probe.sqlite_write_bytes,
                host_pending_sql_bytes: probe.host_pending_sql_bytes,
                rust_queued_spill_bytes: probe.rust_queued_spill_bytes,
                rust_inflight_spill_bytes: probe.rust_inflight_spill_bytes,
                live_serialized_peak: probe.live_serialized_peak,
                live_sql_acked: probe.live_sql_acked,
                merge_scan_begins,
                merge_steps,
            });
            let (queued, inflight) = spill_payload_pair();
            debug_assert!(
                queued == 0 && inflight == 0,
                "spill payload retained after flush run (queued={queued}, inflight={inflight})"
            );
        }
        spill_pull!(self.drain_spill_finish_runs());
        if let Some(spill) = self.spill.as_ref() {
            publish_spill_stats(spill.stats());
        }
        self.stamp_memory_phase(crate::exec::mem_growth::PHASE_SPILL_WRITE);
        Ok(SpillProgress::Ready(()))
    }

    /// Flush a completed run. Park only while the host still owes a spill op.
    ///
    /// A forced `AwaitHost` after a fully committed run used to leave a resume
    /// batch parked while the stream pool went idle on `NeedInput`, which deadlocks
    /// Durable Object reducers (ports stay full, scans stop, stall watchdog fires).
    pub(super) fn flush_live_run_cooperative(&mut self) -> Result<SpillProgress<()>, String> {
        spill_pull!(self.flush_live_run());
        if self.spill_flush_incomplete() {
            return Ok(SpillProgress::AwaitHost);
        }
        Ok(SpillProgress::Ready(()))
    }

    pub(super) fn live_slot(&mut self, key: GroupKey) -> Result<SpillProgress<u32>, String> {
        if let Some(slot) = self.groups.get_slot(&key) {
            return Ok(SpillProgress::Ready(slot));
        }
        if self.spill.is_some() {
            self.note_spill_live();
            if self.spill_flush_due() {
                spill_pull!(self.flush_live_run_cooperative());
            }
        }
        Ok(SpillProgress::Ready(self.groups.slot(key)))
    }

    pub(super) fn bump_count_star(&mut self, slot: u32, agg_index: usize) {
        self.groups.accs[agg_index][slot as usize].count += 1;
    }

    pub(super) fn consume_single_i64_group_spill(
        &mut self,
        cols: &Cols<'_>,
        rows_sel: &[u32],
        values: &[i64],
        nulls: &[u8],
    ) -> Result<SpillProgress<()>, String> {
        let n_aggs = self.aggs.len();
        let count_star_indices: Vec<usize> = self
            .aggs
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.kind == KIND_COUNT_STAR)
            .map(|(index, _)| index)
            .collect();
        let mut sel_at = self.spill_consume_at.min(rows_sel.len());
        while sel_at < rows_sel.len() {
            let row = rows_sel[sel_at] as usize;
            let key = if nulls[row] == 0 {
                GroupTable::null_key(1)
            } else {
                GroupTable::value_key(values[row])
            };
            let slot = match self.live_slot(key)? {
                SpillProgress::Ready(slot) => slot,
                SpillProgress::AwaitHost => {
                    self.spill_consume_at = sel_at;
                    return Ok(SpillProgress::AwaitHost);
                }
            };
            for &index in &count_star_indices {
                self.bump_count_star(slot, index);
            }
            for index in 0..n_aggs {
                if self.aggs[index].kind == KIND_COUNT_STAR {
                    continue;
                }
                let agg_col = self.aggs[index].column;
                let col = column(cols, agg_col)?;
                if col.nulls()[row] == 0 {
                    continue;
                }
                self.bump_main_row_agg(slot, index, row, col);
            }
            sel_at += 1;
        }
        self.finish_spill_batch(rows_sel.len())
    }

    pub(super) fn consume_groups_spilling(
        &mut self,
        cols: &Cols<'_>,
        rows_sel: &[u32],
        key_cols: &[ColRef<'_>],
    ) -> Result<SpillProgress<()>, String> {
        let n_aggs = self.aggs.len();
        let count_star_indices: Vec<usize> = self
            .aggs
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.kind == KIND_COUNT_STAR)
            .map(|(index, _)| index)
            .collect();
        let mut sel_at = self.spill_consume_at.min(rows_sel.len());
        while sel_at < rows_sel.len() {
            let row = rows_sel[sel_at] as usize;
            let slot = match self.live_slot_for_row(key_cols, row)? {
                SpillProgress::Ready(slot) => slot,
                SpillProgress::AwaitHost => {
                    self.spill_consume_at = sel_at;
                    return Ok(SpillProgress::AwaitHost);
                }
            };
            for &index in &count_star_indices {
                self.bump_count_star(slot, index);
            }
            for index in 0..n_aggs {
                if self.aggs[index].kind == KIND_COUNT_STAR {
                    continue;
                }
                let agg_col = self.aggs[index].column;
                let col = column(cols, agg_col)?;
                if col.nulls()[row] == 0 {
                    continue;
                }
                self.bump_main_row_agg(slot, index, row, col);
            }
            sel_at += 1;
        }
        self.finish_spill_batch(rows_sel.len())
    }
}
