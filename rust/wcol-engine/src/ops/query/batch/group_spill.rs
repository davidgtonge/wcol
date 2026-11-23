//! Flush-and-reset spill session: append sorted runs, then merge them in key order.

use crate::ops::query::{merge_partial_groups, PartialAggSpec, PartialGroup};
use crate::spill::{
    decode_canonical_key, decode_partial_group_payload, SpillConfig, SpillMemoryLedger,
    SpillProgress, SpillScanResume, SpillStore,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct FlushStats {
    /// Runs whose commit has completed.
    pub runs: u64,
    /// Live-table flushes. Independent of whether the run commit has completed.
    pub pressure_flushes: u64,
    pub groups: u64,
    pub bytes: u64,
    pub peak_live_groups: u64,
    pub peak_live_bytes: u64,
    pub peak_interner_bytes: u64,
}

pub(crate) struct GroupSpill {
    config: SpillConfig,
    ledger: SpillMemoryLedger,
    store: Box<dyn SpillStore>,
    session_begun: bool,
    host_append_in_flight: bool,
    scan_resume: SpillScanResume,
    merge_current_key: Option<Vec<u8>>,
    merge_current: Option<PartialGroup>,
    merge_scan_open: bool,
    stats: FlushStats,
}

impl GroupSpill {
    /// The resident memory this spill may use: live table plus encode scratch.
    pub(crate) fn combined_budget_bytes(&self) -> u64 {
        self.config.combined_budget_bytes
    }

    pub fn open(config: SpillConfig, store: Box<dyn SpillStore>) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            config,
            ledger: SpillMemoryLedger::new(config),
            store,
            session_begun: false,
            host_append_in_flight: false,
            scan_resume: SpillScanResume::default(),
            merge_current_key: None,
            merge_current: None,
            merge_scan_open: false,
            stats: FlushStats::default(),
        })
    }

    pub fn config(&self) -> &SpillConfig {
        &self.config
    }

    pub fn stats(&self) -> FlushStats {
        self.stats
    }

    pub(crate) fn growth_buffers(&self) -> (u64, u64, u64) {
        let held_key = self
            .merge_current_key
            .as_ref()
            .map(|key| key.capacity())
            .unwrap_or(0) as u64;
        (
            self.stats.runs,
            self.store.pending_write_bytes() as u64,
            (self.store.merge_buffer_bytes() as u64).saturating_add(held_key),
        )
    }

    pub(crate) fn note_run_flushed(&mut self) {
        self.stats.pressure_flushes = self.stats.pressure_flushes.saturating_add(1);
        self.stats.runs = self.stats.runs.saturating_add(1);
    }

    pub fn note_live(&mut self, groups: u64, table_bytes: u64, interner_bytes: u64) {
        self.ledger
            .set_main(table_bytes as usize, interner_bytes as usize);
        self.stats.peak_live_groups = self.stats.peak_live_groups.max(groups);
        self.stats.peak_live_bytes = self.stats.peak_live_bytes.max(table_bytes);
        self.stats.peak_interner_bytes = self.stats.peak_interner_bytes.max(interner_bytes);
    }

    /// `resident_bytes` is the whole live grouping structure: keys, accumulators,
    /// retained hash capacity, sketches, and the interner.
    pub fn resident_exceeds(&self, resident_bytes: u64) -> bool {
        resident_bytes > 0 && resident_bytes >= self.config.main_lock_threshold()
    }

    pub fn append_queued(&self) -> bool {
        self.host_append_in_flight
    }

    pub fn host_pending(&self) -> bool {
        self.host_append_in_flight || self.store.host_op_pending()
    }

    pub(crate) fn ingest_host_read_completion(&mut self) -> Result<(), String> {
        self.store.ingest_host_read_completion()
    }

    pub(crate) fn on_spill_run_put_handoff(&mut self) -> Result<(), String> {
        self.store.on_run_put_handoff()
    }

    pub(crate) fn on_spill_run_commit_host_complete(&mut self) -> Result<(), String> {
        let before = self.store.committed_run_count();
        self.store.on_run_commit_host_complete()?;
        let added = self.store.committed_run_count().saturating_sub(before);
        if added > 0 {
            self.stats.runs = self.stats.runs.saturating_add(added as u64);
        }
        Ok(())
    }

    pub(crate) fn needs_finish_run(&self) -> bool {
        self.store.needs_finish_run()
    }

    pub(crate) fn reconcile_host_append(&mut self) -> Result<SpillProgress<()>, String> {
        if !self.host_append_in_flight {
            return Ok(SpillProgress::Ready(()));
        }
        if self.store.host_op_pending() {
            return Ok(SpillProgress::AwaitHost);
        }
        self.host_append_in_flight = false;
        Ok(SpillProgress::Ready(()))
    }

    pub(crate) fn spill_finalization_needed(&self) -> bool {
        self.session_begun || self.stats.runs > 0
    }

    fn ensure_session(&mut self) -> Result<SpillProgress<()>, String> {
        if self.session_begun {
            return Ok(SpillProgress::Ready(()));
        }
        match self.store.begin()? {
            SpillProgress::Ready(()) => {
                self.session_begun = true;
                Ok(SpillProgress::Ready(()))
            }
            SpillProgress::AwaitHost => {
                self.session_begun = true;
                Ok(SpillProgress::AwaitHost)
            }
        }
    }

    pub(crate) fn expect_run_rows(&mut self, rows: usize) {
        self.store.expect_run_rows(rows);
    }

    /// Append one bounded page of a sorted run. Caller resets live state after the last page.
    pub fn append_run_page(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<SpillProgress<()>, String> {
        if rows.is_empty() {
            return Ok(SpillProgress::Ready(()));
        }
        if matches!(self.ensure_session()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        let groups = rows.len() as u64;
        let bytes = rows
            .iter()
            .map(|(key, partial)| key.len().saturating_add(partial.len()) as u64)
            .sum::<u64>();
        match self.store.append_partials(rows)? {
            SpillProgress::Ready(()) => {
                self.host_append_in_flight = false;
                self.stats.groups = self.stats.groups.saturating_add(groups);
                self.stats.bytes = self.stats.bytes.saturating_add(bytes);
                Ok(SpillProgress::Ready(()))
            }
            SpillProgress::AwaitHost => {
                self.host_append_in_flight = true;
                self.stats.groups = self.stats.groups.saturating_add(groups);
                self.stats.bytes = self.stats.bytes.saturating_add(bytes);
                Ok(SpillProgress::AwaitHost)
            }
        }
    }

    pub fn finish_run(&mut self) -> Result<SpillProgress<()>, String> {
        let before = self.store.committed_run_count();
        if matches!(self.store.finalize_run()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        let added = self.store.committed_run_count().saturating_sub(before);
        if added > 0 {
            self.stats.runs = self.stats.runs.saturating_add(added as u64);
        }
        Ok(SpillProgress::Ready(()))
    }

    pub fn begin_merge_scan(&mut self) -> Result<SpillProgress<()>, String> {
        crate::ops::query::batch::note_spill_merge_scan_begin();
        if matches!(self.store.begin_ordered_scan()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        self.scan_resume = SpillScanResume::default();
        self.merge_current_key = None;
        self.merge_current = None;
        self.merge_scan_open = true;
        Ok(SpillProgress::Ready(()))
    }

    pub fn merge_step(
        &mut self,
        partial_aggs: &[PartialAggSpec],
    ) -> Result<SpillProgress<(Vec<PartialGroup>, bool)>, String> {
        if !self.merge_scan_open {
            return Err("spill merge scan not started".to_string());
        }
        crate::ops::query::batch::note_spill_merge_step();
        let mut completed = Vec::new();
        let batch = match self.store.next_ordered_batch(
            &mut self.scan_resume,
            self.config.scan_page_rows,
            self.config.max_batch_bytes,
        )? {
            SpillProgress::AwaitHost => {
                let (runs, pending, merge) = self.growth_buffers();
                crate::exec::mem_growth::set_phase(crate::exec::mem_growth::PHASE_SPILL_MERGE);
                crate::exec::mem_growth::set_spill(runs, pending, merge);
                crate::exec::mem_growth::observe(false);
                return Ok(SpillProgress::AwaitHost);
            }
            SpillProgress::Ready(batch) => batch,
        };
        if batch.rows.is_empty() {
            if batch.exhausted {
                if let Some(group) = self.merge_current.take() {
                    completed.push(group);
                }
                self.merge_scan_open = false;
                return Ok(SpillProgress::Ready((completed, true)));
            }
            return Ok(SpillProgress::Ready((completed, false)));
        }
        for (_seq, key_blob, payload) in batch.rows {
            let key = decode_canonical_key(&key_blob)?;
            let group = decode_partial_group_payload(&payload, key, partial_aggs)?;
            if self.merge_current_key.as_ref() == Some(&key_blob) {
                merge_partial_groups(self.merge_current.as_mut().expect("merge slot"), &group)?;
            } else {
                if let Some(finished) = self.merge_current.take() {
                    completed.push(finished);
                }
                self.merge_current_key = Some(key_blob);
                self.merge_current = Some(group);
            }
        }
        let exhausted = batch.exhausted;
        if exhausted {
            if let Some(group) = self.merge_current.take() {
                completed.push(group);
            }
            self.merge_scan_open = false;
        }
        Ok(SpillProgress::Ready((completed, exhausted)))
    }

    pub fn end_merge_scan(&mut self) -> Result<(), String> {
        if self.merge_scan_open {
            self.store.end_ordered_scan()?;
            self.merge_scan_open = false;
        }
        self.merge_current_key = None;
        self.merge_current = None;
        Ok(())
    }

    pub fn end(&mut self) -> Result<SpillProgress<()>, String> {
        if !self.session_begun {
            return Ok(SpillProgress::Ready(()));
        }
        self.store.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::{HostSpillBridge, HostSpillStore, SPILL_OP_BEGIN};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn ensure_session_marks_begun_on_spill_await_host() {
        let bridge = Rc::new(RefCell::new(HostSpillBridge::new(99)));
        let store = Box::new(HostSpillStore::new(bridge.clone()));
        let config = SpillConfig {
            combined_budget_bytes: 4096,
            scratch_reserve_bytes: 512,
            max_batch_rows: 64,
            max_batch_bytes: 4096,
            scan_page_rows: 8,
            finalize_batch_rows: 4,
        };
        let mut spill = GroupSpill::open(config, store).unwrap();
        assert!(matches!(
            spill.ensure_session().unwrap(),
            SpillProgress::AwaitHost
        ));
        assert!(spill.session_begun);
        bridge
            .borrow_mut()
            .complete_begin()
            .expect("begin complete");
        assert!(spill.ensure_session().is_ok());
        let pending = bridge.borrow().pending_request();
        assert!(pending.is_none() || pending.unwrap().op != SPILL_OP_BEGIN);
    }

    fn open_r2_spill() -> (GroupSpill, Rc<RefCell<HostSpillBridge>>) {
        let bridge = Rc::new(RefCell::new(HostSpillBridge::new(7)));
        let store = Box::new(crate::spill::HostR2SortedRunStore::new(bridge.clone()));
        let spill = GroupSpill::open(
            SpillConfig {
                combined_budget_bytes: 4096,
                scratch_reserve_bytes: 512,
                max_batch_rows: 64,
                max_batch_bytes: 4096,
                scan_page_rows: 8,
                finalize_batch_rows: 4,
            },
            store,
        )
        .unwrap();
        (spill, bridge)
    }

    /// Drive `finish_run` until the R2 commit is waiting on the host.
    fn suspend_at_run_commit(spill: &mut GroupSpill, bridge: &Rc<RefCell<HostSpillBridge>>) {
        let rows = vec![(b"k".to_vec(), b"v".to_vec())];
        spill.expect_run_rows(rows.len());
        assert!(matches!(
            spill.append_run_page(&rows).unwrap(),
            SpillProgress::Ready(())
        ));
        assert!(matches!(
            spill.finish_run().unwrap(),
            SpillProgress::AwaitHost
        ));
        assert_eq!(
            bridge.borrow().pending_request().expect("put").op,
            crate::spill::SPILL_OP_RUN_PUT
        );
        bridge.borrow_mut().complete_run_put().unwrap();
        spill.on_spill_run_put_handoff().unwrap();
        assert!(matches!(
            spill.finish_run().unwrap(),
            SpillProgress::AwaitHost
        ));
        assert_eq!(
            bridge.borrow().pending_request().expect("commit").op,
            crate::spill::SPILL_OP_RUN_COMMIT
        );
    }

    #[test]
    fn commit_completion_owns_one_run_across_suspend_resume() {
        let (mut spill, bridge) = open_r2_spill();
        suspend_at_run_commit(&mut spill, &bridge);
        let before_suspend = spill.store.committed_run_count();
        assert_eq!(before_suspend, 0);
        assert_eq!(spill.stats().runs, 0);
        assert_eq!(spill.stats().pressure_flushes, 0);

        bridge.borrow_mut().complete_run_commit().unwrap();
        spill.on_spill_run_commit_host_complete().unwrap();
        let after_completion = spill.store.committed_run_count();
        assert_eq!(
            after_completion, 1,
            "the store registers the run at commit completion"
        );
        assert_eq!(
            spill.stats().runs, 1,
            "committed-run telemetry must update when the commit completes, before finish_run resumes"
        );
        assert_eq!(spill.stats().pressure_flushes, 0);

        assert!(matches!(
            spill.finish_run().unwrap(),
            SpillProgress::Ready(())
        ));
        let after_resume = spill.store.committed_run_count();
        assert_eq!(after_resume, 1);
        assert_eq!(
            spill.stats().runs,
            1,
            "resuming finish_run must not count the run again"
        );
        assert_eq!(spill.stats().pressure_flushes, 0);
    }

    #[test]
    fn failed_commit_does_not_register_a_run() {
        let (mut spill, bridge) = open_r2_spill();
        suspend_at_run_commit(&mut spill, &bridge);
        assert_eq!(spill.store.committed_run_count(), 0);
        // Host failure returns before the commit handler. The staged run stays uncommitted.
        assert!(matches!(
            spill.finish_run().unwrap(),
            SpillProgress::AwaitHost
        ));
        assert_eq!(spill.store.committed_run_count(), 0);
        assert_eq!(spill.stats().runs, 0);
        assert_eq!(spill.stats().pressure_flushes, 0);
        let _ = bridge;
    }
}
