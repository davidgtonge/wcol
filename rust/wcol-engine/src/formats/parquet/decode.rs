//! Bounded Parquet → batch decode contract (ingest primitive).
//!
//! ## Demand-driven input (target)
//!
//! ```text
//! Decoder requests bytes (NeedInput)
//!     → host fills bounded compressed ring (ReadAt)
//!     → decoder processes available input
//!     → WCOL batch ready (BatchReady)
//!     → downstream accepts or blocks
//!     → only then decoder requests more input
//! ```
//!
//! Prefetch is allowed only when the planner assigns explicit memory credits; blocked output
//! must not pull unbudgeted column chunks.

use super::ingest_memory::ScanMemoryBudget;

/// Incremental decode state exposed to schedulers (host maps `NeedInput` ↔ ReadAt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeProgress {
    /// Decoded rows are available in the scan pending buffer (call [`NumericScan::advance_batch`] after handoff).
    BatchReady { rows: usize },
    /// More compressed bytes are required before decode can continue.
    NeedInput,
    /// Parser state advanced with no rows emitted (e.g. end-of-page after a full output batch).
    Advanced,
    /// All assigned compressed input for this column chunk has been consumed.
    Finished,
}

/// Hard caps for one column scan inside an execution group (I-04 guardrails).
///
/// **I-05** (bounded streaming decoder) is not satisfied by counts alone; see [`ScanMemoryBudget`].
#[derive(Debug, Clone, Copy)]
pub struct ScanLimits {
    /// Max rows returned per `next_batch_into` call.
    pub max_batch_rows: usize,
    /// Max decoded values buffered inside the scan between batch handoffs.
    pub max_pending_rows: usize,
    /// Max values accepted from a single data page before decode.
    pub max_page_values: usize,
    /// Max dictionary entries retained for one chunk.
    pub max_dictionary_entries: usize,
    /// Byte budget for compressed input, codec scratch, dictionary, batch, and output regions.
    pub budget: ScanMemoryBudget,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            max_batch_rows: 32_768,
            max_pending_rows: 32_768,
            max_page_values: 262_144,
            max_dictionary_entries: 1_048_576,
            budget: ScanMemoryBudget::default(),
        }
    }
}

impl ScanLimits {
    pub fn for_batch_rows(batch_rows: usize) -> Self {
        Self {
            max_batch_rows: batch_rows,
            max_pending_rows: batch_rows,
            ..Default::default()
        }
    }

    pub fn for_batch_rows_and_budget(batch_rows: usize, budget: ScanMemoryBudget) -> Self {
        Self {
            max_batch_rows: batch_rows,
            max_pending_rows: batch_rows,
            budget,
            ..Default::default()
        }
    }

    /// Colocated execution group: 8 MiB resident dictionary budget (separate from batch/codec).
    pub fn for_execution_group(batch_rows: usize) -> Self {
        const MIB: usize = 1024 * 1024;
        let mut budget = ScanMemoryBudget::ingest_do_example();
        budget.dictionary_bytes = 8 * MIB;
        // ClickBench PLAIN pages, fair-split windows for narrow scans, and chunk-sized floors
        // for full-schema row queries (`SELECT *`) when each chunk fits the group cap.
        budget.codec_scratch_bytes = 32 * MIB;
        budget.compressed_bytes = 8 * MIB;
        let mut limits = Self::for_batch_rows_and_budget(batch_rows, budget);
        // ClickBench-style chunks often ship one data page per column per row group (>262k values).
        limits.max_page_values = limits.max_page_values.max(1_048_576);
        limits
    }

    /// I-09 restrictive gate: tight [`ScanMemoryBudget::q3_acceptance_restrictive`] per execution group.
    pub fn for_q3_restrictive_acceptance(batch_rows: usize) -> Self {
        let mut limits = Self::for_batch_rows_and_budget(
            batch_rows,
            ScanMemoryBudget::q3_acceptance_restrictive(),
        );
        limits.max_page_values = limits.max_page_values.max(1_048_576);
        limits
    }
}
