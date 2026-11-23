//! In-memory sorted runs for native tests and host-free spill merge proofs.

use super::sorted_run::{decode_run, encode_run, planned_run_bytes};
use super::{SpillScanBatch, SpillScanResume, SpillStore};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone, Debug)]
struct CommittedRun {
    #[allow(dead_code)]
    bytes: Vec<u8>,
    scan_records: Vec<(i64, Vec<u8>, Vec<u8>)>,
}

/// Accepts partial pages during one flush, commits one sorted run blob per `finalize_run`.
pub struct MemorySortedRunStore {
    begun: bool,
    pending_rows: Vec<(Vec<u8>, Vec<u8>)>,
    runs: Vec<CommittedRun>,
    run_seq: i64,
    merge_heap: BinaryHeap<MergeCursor>,
    merge_open: bool,
    global_seq: i64,
}

#[derive(Clone, Eq, PartialEq)]
struct MergeCursor {
    key: Vec<u8>,
    seq: i64,
    payload: Vec<u8>,
    run_index: usize,
    record_index: usize,
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

impl MemorySortedRunStore {
    pub fn new() -> Self {
        Self {
            begun: false,
            pending_rows: Vec::new(),
            runs: Vec::new(),
            run_seq: 0,
            merge_heap: BinaryHeap::new(),
            merge_open: false,
            global_seq: 0,
        }
    }

    pub fn committed_run_count(&self) -> usize {
        self.runs.len()
    }
}

impl Default for MemorySortedRunStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SpillStore for MemorySortedRunStore {
    fn begin(&mut self) -> Result<super::SpillProgress<()>, String> {
        self.begun = true;
        Ok(super::SpillProgress::Ready(()))
    }

    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<super::SpillProgress<()>, String> {
        if !self.begun {
            return Err("memory sorted run store not begun".into());
        }
        self.pending_rows.extend(rows.iter().cloned());
        Ok(super::SpillProgress::Ready(()))
    }

    fn finalize_run(&mut self) -> Result<super::SpillProgress<()>, String> {
        if self.pending_rows.is_empty() {
            return Ok(super::SpillProgress::Ready(()));
        }
        // No re-sort: like the R2 store, a run must arrive in key order (`encode_run` checks).
        let mut blob = Vec::new();
        encode_run(&mut blob, &self.pending_rows)?;
        debug_assert_eq!(blob.len(), planned_run_bytes(&self.pending_rows) as usize);
        let records = decode_run(&blob)?;
        let scan_records = records
            .into_iter()
            .map(|row| {
                self.global_seq += 1;
                (self.global_seq, row.key, row.payload)
            })
            .collect();
        self.run_seq += 1;
        self.runs.push(CommittedRun {
            bytes: blob,
            scan_records,
        });
        self.pending_rows.clear();
        Ok(super::SpillProgress::Ready(()))
    }

    fn pending_write_bytes(&self) -> usize {
        super::store::pair_bytes(&self.pending_rows)
    }

    fn merge_buffer_bytes(&self) -> usize {
        if !self.merge_open {
            return 0;
        }
        let runs = self.runs.iter().fold(0usize, |sum, run| {
            sum.saturating_add(run.bytes.capacity())
                .saturating_add(super::store::keyed_row_bytes(&run.scan_records))
        });
        let heap = self.merge_heap.iter().fold(0usize, |sum, cursor| {
            sum.saturating_add(cursor.key.capacity())
                .saturating_add(cursor.payload.capacity())
        });
        runs.saturating_add(heap)
    }

    fn begin_ordered_scan(&mut self) -> Result<super::SpillProgress<()>, String> {
        self.merge_open = true;
        self.merge_heap.clear();
        for (run_index, run) in self.runs.iter().enumerate() {
            if let Some((seq, key, payload)) = run.scan_records.first() {
                self.merge_heap.push(MergeCursor {
                    key: key.clone(),
                    seq: *seq,
                    payload: payload.clone(),
                    run_index,
                    record_index: 0,
                });
            }
        }
        Ok(super::SpillProgress::Ready(()))
    }

    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<super::SpillProgress<SpillScanBatch>, String> {
        if !self.merge_open {
            return Err("memory sorted merge scan not started".into());
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
            used = used.saturating_add(row_bytes);
            rows.push((top.seq, top.key.clone(), top.payload.clone()));
            let run = &self.runs[top.run_index];
            let next_index = top.record_index + 1;
            if let Some((seq, key, payload)) = run.scan_records.get(next_index) {
                self.merge_heap.push(MergeCursor {
                    key: key.clone(),
                    seq: *seq,
                    payload: payload.clone(),
                    run_index: top.run_index,
                    record_index: next_index,
                });
            }
        }
        let exhausted = self.merge_heap.is_empty();
        if let Some(last) = rows.last() {
            resume.started = true;
            resume.seq = last.0;
            resume.key_blob.clone_from(&last.1);
        } else if exhausted {
            resume.started = true;
        }
        Ok(super::SpillProgress::Ready(SpillScanBatch {
            rows,
            exhausted,
        }))
    }

    fn end_ordered_scan(&mut self) -> Result<(), String> {
        self.merge_open = false;
        self.merge_heap.clear();
        Ok(())
    }

    fn end(&mut self) -> Result<super::SpillProgress<()>, String> {
        self.begun = false;
        Ok(super::SpillProgress::Ready(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifty_runs_merge_repeated_keys() {
        let mut store = MemorySortedRunStore::new();
        store.begin().unwrap();
        for run in 0..50u8 {
            store
                .append_partials(&[(b"user".to_vec(), vec![run])])
                .unwrap();
            store.finalize_run().unwrap();
        }
        store.begin_ordered_scan().unwrap();
        let mut resume = SpillScanResume::default();
        let mut total = 0usize;
        loop {
            let batch = store
                .next_ordered_batch(&mut resume, 100, 1024 * 1024)
                .unwrap()
                .expect_ready("merge page");
            total += batch.rows.len();
            if batch.exhausted {
                break;
            }
        }
        assert_eq!(total, 50);
    }
}
