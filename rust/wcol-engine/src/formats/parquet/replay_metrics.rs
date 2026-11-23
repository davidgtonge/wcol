//! Skip-replay baseline instrumentation.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanReplayMetrics {
    /// Times a page body was decoded from the start (first pass + each resume).
    pub page_replay_count: u64,
    /// Every `PageEvent` visited while decoding (includes replay skips).
    pub page_events_visited: u64,
    /// Sum of logical Parquet input events emitted across completed data pages in this scan.
    pub page_events_committed: u64,
    /// Owned-path Snappy inflations (one per data page body).
    pub snappy_page_inflates: u64,
}

impl ScanReplayMetrics {
    /// `page_events_visited / max(1, page_events_committed)` for this column scan (chunk lifetime).
    pub fn replay_amplification(&self) -> f64 {
        self.page_events_visited as f64 / self.page_events_committed.max(1) as f64
    }

    pub fn merge(&mut self, other: Self) {
        self.page_replay_count = self
            .page_replay_count
            .saturating_add(other.page_replay_count);
        self.page_events_visited = self
            .page_events_visited
            .saturating_add(other.page_events_visited);
        self.page_events_committed = self
            .page_events_committed
            .saturating_add(other.page_events_committed);
        self.snappy_page_inflates = self
            .snappy_page_inflates
            .saturating_add(other.snappy_page_inflates);
    }
}

static INGEST_REPLAY_COUNT: AtomicU64 = AtomicU64::new(0);
static INGEST_REPLAY_VISITED: AtomicU64 = AtomicU64::new(0);
static INGEST_REPLAY_COMMITTED: AtomicU64 = AtomicU64::new(0);

/// Reset totals recorded by [`ingest_replay_merge`] (plateau harness start).
pub fn ingest_replay_reset() {
    INGEST_REPLAY_COUNT.store(0, Ordering::Relaxed);
    INGEST_REPLAY_VISITED.store(0, Ordering::Relaxed);
    INGEST_REPLAY_COMMITTED.store(0, Ordering::Relaxed);
}

/// Accumulate decode replay across colocated ingest row groups.
pub fn ingest_replay_merge(metrics: ScanReplayMetrics) {
    INGEST_REPLAY_COUNT.fetch_add(metrics.page_replay_count, Ordering::Relaxed);
    INGEST_REPLAY_VISITED.fetch_add(metrics.page_events_visited, Ordering::Relaxed);
    INGEST_REPLAY_COMMITTED.fetch_add(metrics.page_events_committed, Ordering::Relaxed);
}

pub fn ingest_replay_totals() -> ScanReplayMetrics {
    ScanReplayMetrics {
        page_replay_count: INGEST_REPLAY_COUNT.load(Ordering::Relaxed),
        page_events_visited: INGEST_REPLAY_VISITED.load(Ordering::Relaxed),
        page_events_committed: INGEST_REPLAY_COMMITTED.load(Ordering::Relaxed),
        snappy_page_inflates: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_accumulates_committed_across_scans() {
        ingest_replay_reset();
        ingest_replay_merge(ScanReplayMetrics {
            page_replay_count: 2,
            page_events_visited: 100,
            page_events_committed: 40,
            snappy_page_inflates: 1,
        });
        ingest_replay_merge(ScanReplayMetrics {
            page_replay_count: 0,
            page_events_visited: 10,
            page_events_committed: 10,
            snappy_page_inflates: 0,
        });
        let t = ingest_replay_totals();
        assert_eq!(t.page_replay_count, 2);
        assert_eq!(t.page_events_visited, 110);
        assert_eq!(t.page_events_committed, 50);
        assert!((t.replay_amplification() - 2.2).abs() < 0.001);
    }
}
