//! Cloudflare R2 sorted-run spill store (host round-trips via spill bridge).

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::rc::Rc;

use super::host::{HostSpillBridge, SpillProgress};
use super::{SpillScanBatch, SpillScanResume, SpillStore};

mod codec;
mod merge;
mod put;

pub use codec::{
    decode_run_put_open, decode_run_read_open, encode_run_put_open, encode_run_read_open,
    RunPutPlan, RunReadPlan, RUN_PART_BYTES, RUN_READ_CHUNK,
};
use merge::{MergeCursor, MergePhase, RunReader};
use put::PutPhase;

#[derive(Clone, Debug)]
struct CommittedRunMeta {
    object_key: String,
    byte_length: u64,
    record_count: u64,
    seq_base: i64,
}

/// Encodes each appended page straight into the run body and uploads it to R2 one
/// [`RUN_PART_BYTES`] part at a time, so a run never sits whole in Wasm memory.
pub struct HostR2SortedRunStore {
    bridge: Rc<RefCell<HostSpillBridge>>,
    begun: bool,
    /// The open run's bytes not yet uploaded, already in wire format.
    run_body: Vec<u8>,
    /// Bytes past a full part, held while that part uploads.
    run_carry: Vec<u8>,
    run_records: u64,
    /// Records the open run will hold. Its header goes out in the first part, before the
    /// last page is written, so the count must be declared up front.
    run_expected: u64,
    run_parts: u32,
    run_bytes: u64,
    /// Last appended key: pages must arrive in canonical key order.
    run_last_key: Vec<u8>,
    /// Raw read bytes the merge shares across its open runs (`run_read_chunk`).
    merge_read_budget: u64,
    run_seq: u32,
    runs: Vec<CommittedRunMeta>,
    global_seq: i64,
    merge_heap: BinaryHeap<MergeCursor>,
    /// Completed rows held across a host read; never advance past them on retry.
    pending_emit: Vec<(i64, Vec<u8>, Vec<u8>)>,
    merge_readers: Vec<RunReader>,
    merge: MergePhase,
    put: PutPhase,
}

impl HostR2SortedRunStore {
    pub fn new(bridge: Rc<RefCell<HostSpillBridge>>) -> Self {
        Self {
            bridge,
            begun: false,
            run_body: Vec::new(),
            run_carry: Vec::new(),
            run_records: 0,
            run_expected: 0,
            run_parts: 0,
            run_bytes: 0,
            run_last_key: Vec::new(),
            merge_read_budget: codec::MERGE_READ_BUDGET,
            run_seq: 0,
            runs: Vec::new(),
            global_seq: 0,
            merge_heap: BinaryHeap::new(),
            pending_emit: Vec::new(),
            merge_readers: Vec::new(),
            merge: MergePhase::Closed,
            put: PutPhase::Idle,
        }
    }
}

impl HostR2SortedRunStore {
    /// Start the next run in the buffer the host gave back after the last upload, if any.
    fn adopt_spare_body(&mut self) {
        let mut bridge = self.bridge.borrow_mut();
        if bridge.run_upload.is_empty() && bridge.run_upload_off == 0 {
            self.run_body = std::mem::take(&mut bridge.run_upload);
        }
    }

    /// Size the run body once from its first page: the whole run if it fits in one part,
    /// else the part buffer. A doubling `Vec` would briefly hold the
    /// old and new buffers, and Wasm memory never shrinks back from that peak.
    fn reserve_run_body(&mut self, first_page: &[(Vec<u8>, Vec<u8>)]) {
        let page_bytes = first_page
            .iter()
            .map(|(key, payload)| super::sorted_run::record_wire_bytes(key, payload))
            .sum::<usize>();
        let per_row = page_bytes.div_ceil(first_page.len());
        let rows = usize::try_from(self.run_expected).unwrap_or(usize::MAX);
        let body = super::sorted_run::RUN_HEADER_LEN.saturating_add(per_row.saturating_mul(rows));
        // A sixteenth of slack absorbs keys longer than the first page's.
        let wanted = body
            .saturating_add(body / 16)
            .min(put::PART_BUFFER_BYTES.max(RUN_PART_BYTES.saturating_add(page_bytes)));
        self.run_body
            .reserve_exact(wanted.saturating_sub(self.run_body.len()));
    }

    fn open_run(&mut self, first_page: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
        if self.run_expected == 0 {
            return Err("an R2 run needs its record count before its first page".into());
        }
        if self.run_body.capacity() == 0 {
            self.adopt_spare_body();
        }
        self.reserve_run_body(first_page);
        self.run_seq = self.run_seq.saturating_add(1);
        super::sorted_run::encode_header(&mut self.run_body, self.run_expected);
        Ok(())
    }
}

impl SpillStore for HostR2SortedRunStore {
    fn host_op_pending(&self) -> bool {
        self.bridge.borrow().has_pending()
            || !matches!(self.put, PutPhase::Idle)
            || self.read_target().is_some()
    }

    fn ingest_host_read_completion(&mut self) -> Result<(), String> {
        self.reconcile_host_read()
    }

    fn on_run_put_handoff(&mut self) -> Result<(), String> {
        self.resume_after_part();
        Ok(())
    }

    fn on_run_commit_host_complete(&mut self) -> Result<(), String> {
        if matches!(self.put, PutPhase::CommitPending { .. }) {
            self.commit_staged_run()?;
        }
        Ok(())
    }

    fn needs_finish_run(&self) -> bool {
        self.run_records > 0 || !matches!(self.put, PutPhase::Idle)
    }

    fn committed_run_count(&self) -> usize {
        self.runs.len()
    }

    fn pending_write_bytes(&self) -> usize {
        self.run_body
            .capacity()
            .saturating_add(self.run_carry.capacity())
            .saturating_add(self.run_last_key.capacity())
            .saturating_add(self.bridge.borrow().pending_write_bytes())
    }

    fn merge_buffer_bytes(&self) -> usize {
        let readers = self.merge_readers.iter().fold(0usize, |sum, reader| {
            let pending = reader.head.as_ref().map_or(0, |(key, value)| {
                key.capacity().saturating_add(value.capacity())
            });
            sum.saturating_add(reader.decoder.retained_bytes())
                .saturating_add(pending)
        });
        let heap = self.merge_heap.iter().fold(0usize, |sum, cursor| {
            sum.saturating_add(cursor.key.capacity())
                .saturating_add(cursor.payload.capacity())
        });
        let emit = super::store::keyed_row_bytes(&self.pending_emit);
        readers
            .saturating_add(heap)
            .saturating_add(emit)
            .saturating_add(self.bridge.borrow().merge_read_bytes())
    }

    fn begin(&mut self) -> Result<SpillProgress<()>, String> {
        self.begun = true;
        Ok(SpillProgress::Ready(()))
    }

    fn expect_run_rows(&mut self, rows: usize) {
        if self.run_records == 0 {
            self.run_expected = rows as u64;
        }
    }

    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<SpillProgress<()>, String> {
        if !self.begun {
            return Err("r2 sorted run store not begun".into());
        }
        // The next run may start while the last one awaits its commit, not mid-part.
        if matches!(self.put, PutPhase::PartInFlight) {
            return Err("run page appended while a run part uploads".into());
        }
        if rows.is_empty() {
            return Ok(SpillProgress::Ready(()));
        }
        if self.run_records == 0 {
            self.open_run(rows)?;
        }
        for (key, payload) in rows {
            if self.run_records > 0 && key.as_slice() < self.run_last_key.as_slice() {
                return Err("sorted run pages must arrive in key order".into());
            }
            super::sorted_run::encode_record(&mut self.run_body, key, payload)?;
            self.run_last_key.clear();
            self.run_last_key.extend_from_slice(key);
            self.run_records += 1;
        }
        if self.run_records > self.run_expected {
            return Err(format!(
                "sorted run passed its declared {} records",
                self.run_expected
            ));
        }
        self.send_full_part()
    }

    fn finalize_run(&mut self) -> Result<SpillProgress<()>, String> {
        self.finish_put()
    }

    fn begin_ordered_scan(&mut self) -> Result<SpillProgress<()>, String> {
        self.open_ordered_scan()
    }

    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SpillProgress<SpillScanBatch>, String> {
        self.pull_ordered_batch(resume, max_rows, max_bytes)
    }

    fn end_ordered_scan(&mut self) -> Result<(), String> {
        self.close_ordered_scan()
    }

    fn end(&mut self) -> Result<SpillProgress<()>, String> {
        self.begun = false;
        Ok(SpillProgress::Ready(()))
    }
}

#[cfg(test)]
mod tests;
