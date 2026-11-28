//! Ordered merge of committed R2 runs.

use std::cmp::Ordering;
use std::rc::Rc;

use super::super::host::{await_host, HostSpillRequest, SpillProgress, SPILL_OP_RUN_READ};
use super::super::sorted_run::RunStreamDecoder;
use super::super::{KeyedRow, SpillScanBatch, SpillScanResume, SpillStore};
use super::codec::{encode_run_read_open, run_read_chunk, RunReadPlan};
use super::put::PutPhase;
use super::HostR2SortedRunStore;

#[derive(Clone, Debug)]
pub(super) struct RunReader {
    pub(super) decoder: RunStreamDecoder,
    pub(super) file_offset: u64,
    pub(super) emitted: u64,
    /// The next record, decoded from `decoder` only when the merge asks for it.
    pub(super) head: Option<(Vec<u8>, Vec<u8>)>,
    pub(super) at_eof: bool,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct MergeCursor {
    pub(super) key: Vec<u8>,
    pub(super) seq: i64,
    pub(super) payload: Vec<u8>,
    pub(super) run_index: usize,
    pub(super) record_index: u64,
}

impl Ord for MergeCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse both comparisons for (key, seq) ascending.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for MergeCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) enum MergePhase {
    Closed,
    Bootstrapping {
        next_run: usize,
        read_target: Option<(usize, u64)>,
    },
    Scanning {
        read_target: Option<(usize, u64)>,
    },
}

impl HostR2SortedRunStore {
    pub(super) fn merge_is_open(&self) -> bool {
        !matches!(self.merge, MergePhase::Closed)
    }

    pub(super) fn bootstrap_run(&self) -> usize {
        match self.merge {
            MergePhase::Bootstrapping { next_run, .. } => next_run,
            MergePhase::Scanning { .. } => self.runs.len(),
            MergePhase::Closed => 0,
        }
    }

    pub(super) fn read_target_mut(&mut self) -> Option<&mut Option<(usize, u64)>> {
        match &mut self.merge {
            MergePhase::Bootstrapping { read_target, .. }
            | MergePhase::Scanning { read_target } => Some(read_target),
            MergePhase::Closed => None,
        }
    }

    pub(super) fn read_target(&self) -> Option<(usize, u64)> {
        match self.merge {
            MergePhase::Bootstrapping { read_target, .. }
            | MergePhase::Scanning { read_target } => read_target,
            MergePhase::Closed => None,
        }
    }

    pub(super) fn note_bootstrap_advanced(&mut self) {
        let MergePhase::Bootstrapping {
            next_run,
            read_target,
        } = self.merge
        else {
            return;
        };
        let next_run = next_run.saturating_add(1);
        self.merge = if next_run >= self.runs.len() {
            MergePhase::Scanning { read_target }
        } else {
            MergePhase::Bootstrapping {
                next_run,
                read_target,
            }
        };
    }

    pub(super) fn queue_run_read(
        &mut self,
        run_index: usize,
        record_index: u64,
    ) -> Result<SpillProgress<()>, String> {
        if self.bridge.borrow().has_pending() {
            return Ok(SpillProgress::AwaitHost);
        }
        let meta = self
            .runs
            .get(run_index)
            .ok_or("run read index out of range")?;
        if record_index >= meta.record_count {
            return Ok(SpillProgress::Ready(()));
        }
        let reader = self
            .merge_readers
            .get(run_index)
            .ok_or("run reader missing")?;
        if reader.at_eof {
            return Ok(SpillProgress::Ready(()));
        }
        let remaining = meta.byte_length.saturating_sub(reader.file_offset);
        if remaining == 0 {
            return Ok(SpillProgress::Ready(()));
        }
        let chunk = run_read_chunk(self.runs.len(), self.merge_read_budget);
        let length = chunk.min(remaining.min(u64::from(u32::MAX)) as u32);
        let plan = RunReadPlan {
            object_key: meta.object_key.clone(),
            byte_length: meta.byte_length,
            offset: reader.file_offset,
            length,
        };
        *self
            .read_target_mut()
            .ok_or("run read queued before merge scan")? = Some((run_index, record_index));
        await_host(
            &mut self.bridge.borrow_mut(),
            HostSpillRequest {
                op: SPILL_OP_RUN_READ,
                append_bytes: encode_run_read_open(&plan)?,
            },
        )
    }

    pub(super) fn reconcile_host_read(&mut self) -> Result<(), String> {
        if self.read_target().is_some() && !self.bridge.borrow().has_pending() {
            self.apply_completed_read()?;
        }
        Ok(())
    }

    pub(super) fn ingest_run_read(&mut self, run_index: usize, bytes: &[u8]) -> Result<(), String> {
        let byte_length = self
            .runs
            .get(run_index)
            .ok_or("run read index out of range")?
            .byte_length;
        let reader = self
            .merge_readers
            .get_mut(run_index)
            .ok_or("run reader missing")?;
        reader.file_offset = reader.file_offset.saturating_add(bytes.len() as u64);
        if bytes.is_empty() || reader.file_offset >= byte_length {
            reader.at_eof = true;
        }
        reader.decoder.feed(bytes);
        Ok(())
    }

    pub(super) fn ensure_record(
        &mut self,
        run_index: usize,
        record_index: u64,
    ) -> Result<SpillProgress<Option<KeyedRow>>, String> {
        let meta = &self.runs[run_index];
        if record_index >= meta.record_count {
            return Ok(SpillProgress::Ready(None));
        }
        let seq_base = meta.seq_base;
        let reader = &mut self.merge_readers[run_index];
        if reader.emitted == record_index {
            if reader.head.is_none() {
                reader.head = reader
                    .decoder
                    .next_record()?
                    .map(|record| (record.key, record.payload));
            }
            if let Some((key, payload)) = &reader.head {
                let seq =
                    seq_base.saturating_add(i64::try_from(reader.emitted).unwrap_or(i64::MAX));
                return Ok(SpillProgress::Ready(Some((
                    seq,
                    key.clone(),
                    payload.clone(),
                ))));
            }
        } else if reader.emitted > record_index {
            return Err("run record cursor moved backward".into());
        }
        if matches!(
            self.queue_run_read(run_index, record_index)?,
            SpillProgress::AwaitHost
        ) {
            return Ok(SpillProgress::AwaitHost);
        }
        if self.read_target().is_some() {
            return Ok(SpillProgress::AwaitHost);
        }
        Err("run read stalled: no host bytes and record not buffered".into())
    }

    pub(super) fn consume_record(
        &mut self,
        run_index: usize,
        record_index: u64,
    ) -> Result<(), String> {
        let reader = self.merge_readers.get_mut(run_index).expect("reader");
        if reader.emitted != record_index {
            return Err("run record consume mismatch".into());
        }
        if reader.head.take().is_none() {
            return Err("run record consume without buffered row".into());
        }
        reader.emitted = reader.emitted.saturating_add(1);
        Ok(())
    }

    pub(super) fn apply_completed_read(&mut self) -> Result<(), String> {
        let Some(slot) = self.read_target_mut() else {
            return Ok(());
        };
        let Some((run_index, _record_index)) = slot.take() else {
            return Ok(());
        };
        let bridge = Rc::clone(&self.bridge);
        let mut bridge = bridge.borrow_mut();
        self.ingest_run_read(run_index, bridge.run_read_bytes())?;
        bridge.clear_run_read();
        Ok(())
    }

    pub(super) fn bootstrap_next_run(&mut self) -> Result<SpillProgress<bool>, String> {
        while self.bootstrap_run() < self.runs.len() {
            let run_index = self.bootstrap_run();
            match self.ensure_record(run_index, 0)? {
                SpillProgress::Ready(Some((seq, key, payload))) => {
                    self.consume_record(run_index, 0)?;
                    self.merge_heap.push(MergeCursor {
                        key,
                        seq,
                        payload,
                        run_index,
                        record_index: 0,
                    });
                    self.note_bootstrap_advanced();
                }
                SpillProgress::Ready(None) => {
                    self.note_bootstrap_advanced();
                }
                SpillProgress::AwaitHost => return Ok(SpillProgress::AwaitHost),
            }
        }
        Ok(SpillProgress::Ready(true))
    }

    pub(super) fn open_ordered_scan(&mut self) -> Result<SpillProgress<()>, String> {
        if !self.merge_is_open() {
            if !matches!(self.put, PutPhase::Idle) {
                return Err("merge opened while a run upload is in flight".into());
            }
            // No run is written once the merge opens: its reads go into the part buffer.
            self.run_carry = Vec::new();
            self.run_last_key = Vec::new();
            let body = std::mem::take(&mut self.run_body);
            let mut bridge = self.bridge.borrow_mut();
            let spare = std::mem::take(&mut bridge.run_upload);
            bridge.adopt_read_buffer(body);
            bridge.adopt_read_buffer(spare);
            drop(bridge);
            self.merge = MergePhase::Bootstrapping {
                next_run: 0,
                read_target: None,
            };
            self.merge_heap.clear();
            self.pending_emit.clear();
            self.merge_readers = self
                .runs
                .iter()
                .map(|_| RunReader {
                    decoder: RunStreamDecoder::new(),
                    file_offset: 0,
                    emitted: 0,
                    head: None,
                    at_eof: false,
                })
                .collect();
        }
        self.reconcile_host_read()?;
        if self.host_op_pending() {
            return Ok(SpillProgress::AwaitHost);
        }
        match self.bootstrap_next_run()? {
            SpillProgress::Ready(true) => Ok(SpillProgress::Ready(())),
            SpillProgress::Ready(false) | SpillProgress::AwaitHost => Ok(SpillProgress::AwaitHost),
        }
    }

    pub(super) fn pull_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SpillProgress<SpillScanBatch>, String> {
        if !self.merge_is_open() {
            return Err("r2 sorted merge scan not started".into());
        }
        self.reconcile_host_read()?;
        if self.host_op_pending() {
            return Ok(SpillProgress::AwaitHost);
        }
        // Return already-merged rows before advancing any cursor further.
        // Those rows were staged when the next run record required host I/O.
        if !self.pending_emit.is_empty() {
            let rows = std::mem::take(&mut self.pending_emit);
            if let Some(last) = rows.last() {
                resume.started = true;
                resume.seq = last.0;
                resume.key_blob.clone_from(&last.1);
            }
            return Ok(SpillProgress::Ready(SpillScanBatch {
                rows,
                exhausted: false,
            }));
        }
        if self.bootstrap_run() < self.runs.len() {
            self.reconcile_host_read()?;
            if self.host_op_pending() {
                return Ok(SpillProgress::AwaitHost);
            }
            match self.bootstrap_next_run()? {
                SpillProgress::Ready(true) => {}
                SpillProgress::Ready(false) | SpillProgress::AwaitHost => {
                    return Ok(SpillProgress::AwaitHost);
                }
            }
        }
        let mut rows = Vec::new();
        let mut used = 0usize;
        while rows.len() < max_rows {
            let Some(top) = self.merge_heap.pop() else {
                break;
            };
            let row_bytes = top.key.len().saturating_add(top.payload.len());
            if !rows.is_empty() && used.saturating_add(row_bytes) > max_bytes {
                self.merge_heap.push(top);
                break;
            }
            let next_index = top.record_index.saturating_add(1);
            let meta = &self.runs[top.run_index];
            if next_index < meta.record_count {
                // Do not emit `top` until its successor is known. Otherwise
                // suspending here loses `rows` or replays `top` on resume.
                match self.ensure_record(top.run_index, next_index) {
                    Ok(SpillProgress::Ready(Some((seq, key, payload)))) => {
                        if let Err(error) = self.consume_record(top.run_index, next_index) {
                            self.merge_heap.push(top);
                            self.pending_emit = rows;
                            return Err(error);
                        }
                        self.merge_heap.push(MergeCursor {
                            key,
                            seq,
                            payload,
                            run_index: top.run_index,
                            record_index: next_index,
                        });
                    }
                    Ok(SpillProgress::Ready(None)) => {}
                    Ok(SpillProgress::AwaitHost) => {
                        // `top` has not been emitted. Keep it on the heap and
                        // preserve previously emitted rows for the next call.
                        self.merge_heap.push(top);
                        self.pending_emit = rows;
                        return Ok(SpillProgress::AwaitHost);
                    }
                    Err(error) => {
                        self.merge_heap.push(top);
                        self.pending_emit = rows;
                        return Err(error);
                    }
                }
            }
            used = used.saturating_add(row_bytes);
            rows.push((top.seq, top.key, top.payload));
        }
        let exhausted = self.merge_heap.is_empty() && self.bootstrap_run() >= self.runs.len();
        if let Some(last) = rows.last() {
            resume.started = true;
            resume.seq = last.0;
            resume.key_blob.clone_from(&last.1);
        } else if exhausted {
            resume.started = true;
        }
        Ok(SpillProgress::Ready(SpillScanBatch { rows, exhausted }))
    }

    pub(super) fn close_ordered_scan(&mut self) -> Result<(), String> {
        self.merge = MergePhase::Closed;
        self.merge_heap.clear();
        self.pending_emit.clear();
        self.merge_readers.clear();
        Ok(())
    }
}
