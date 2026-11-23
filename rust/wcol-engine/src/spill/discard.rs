//! Diagnostic spill sink: accept sorted encoded runs and drop them.
//!
//! Used to prove flush-and-reset bounds live aggregation memory without R2 or SQLite.

use super::{SpillScanBatch, SpillScanResume, SpillStore};

/// Counts flushed partials and discards their bytes.
#[derive(Clone, Debug, Default)]
pub struct DiscardRunStore {
    pub appends: u64,
    pub groups: u64,
    pub bytes: u64,
    begun: bool,
}

impl DiscardRunStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SpillStore for DiscardRunStore {
    fn begin(&mut self) -> Result<super::SpillProgress<()>, String> {
        self.begun = true;
        Ok(super::SpillProgress::Ready(()))
    }

    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<super::SpillProgress<()>, String> {
        if !self.begun {
            return Err("discard spill store not begun".to_string());
        }
        if !rows.is_empty() {
            self.appends += 1;
        }
        self.groups = self.groups.saturating_add(rows.len() as u64);
        self.bytes = self.bytes.saturating_add(
            rows.iter()
                .map(|(key, partial)| key.len().saturating_add(partial.len()))
                .sum::<usize>() as u64,
        );
        Ok(super::SpillProgress::Ready(()))
    }

    fn begin_ordered_scan(&mut self) -> Result<super::SpillProgress<()>, String> {
        Ok(super::SpillProgress::Ready(()))
    }

    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        _max_rows: usize,
        _max_bytes: usize,
    ) -> Result<super::SpillProgress<SpillScanBatch>, String> {
        resume.started = true;
        Ok(super::SpillProgress::Ready(SpillScanBatch {
            rows: Vec::new(),
            exhausted: true,
        }))
    }

    fn end_ordered_scan(&mut self) -> Result<(), String> {
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
    fn discard_store_counts_and_drops_rows() {
        let mut store = DiscardRunStore::new();
        store.begin().unwrap();
        store
            .append_partials(&[
                (b"a".to_vec(), b"p1".to_vec()),
                (b"b".to_vec(), b"p2".to_vec()),
            ])
            .unwrap();
        assert_eq!(store.appends, 1);
        assert_eq!(store.groups, 2);
        assert_eq!(store.bytes, 6);
        store.begin_ordered_scan().unwrap();
        let mut resume = SpillScanResume::default();
        let page = store
            .next_ordered_batch(&mut resume, 10, 1024)
            .unwrap()
            .expect_ready("discard page");
        assert!(page.rows.is_empty());
        assert!(page.exhausted);
        store.end().unwrap();
    }
}
