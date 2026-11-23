//! Combined resident memory budget for spill aggregation.

/// Bytes the planner charges for one group, including hash-table overhead.
pub const ESTIMATED_GROUP_BYTES: u64 = 384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpillConfig {
    /// Total in-isolate budget for the live hash table + interner.
    pub combined_budget_bytes: u64,
    /// Headroom reserved for encode scratch (subtracted before a flush is triggered).
    pub scratch_reserve_bytes: u64,
    /// Max rows per append flush.
    pub max_batch_rows: usize,
    /// Max bytes per encoded flush blob (host + 2 MiB row cap).
    pub max_batch_bytes: usize,
    /// Rows per SQLite scan page during finalize.
    pub scan_page_rows: usize,
    /// Max groups per partial-state chunk emitted during spill finalization.
    pub finalize_batch_rows: usize,
}

impl SpillConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.combined_budget_bytes == 0 {
            return Err("spill combined budget must be > 0".to_string());
        }
        if self.scratch_reserve_bytes >= self.combined_budget_bytes {
            return Err("spill scratch reserve must be less than combined budget".to_string());
        }
        if self.max_batch_rows == 0 || self.max_batch_bytes == 0 || self.scan_page_rows == 0 {
            return Err("spill batch and scan limits must be > 0".to_string());
        }
        if self.finalize_batch_rows == 0 {
            return Err("spill finalize batch rows must be > 0".to_string());
        }
        Ok(())
    }

    pub fn main_lock_threshold(&self) -> u64 {
        self.combined_budget_bytes
            .saturating_sub(self.scratch_reserve_bytes)
    }

    pub fn scratch_budget_bytes(&self) -> u64 {
        self.scratch_reserve_bytes
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            // Production flush target; hard ceiling and 8 MiB reclaim live on the planner / BatchQuery path.
            combined_budget_bytes: 16 * 1024 * 1024,
            scratch_reserve_bytes: 1024 * 1024,
            max_batch_rows: 1000,
            max_batch_bytes: 256 * 1024,
            scan_page_rows: 256,
            finalize_batch_rows: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SpillMemoryLedger {
    main_table_bytes: usize,
    main_interner_bytes: usize,
}

impl SpillMemoryLedger {
    pub fn new(_config: SpillConfig) -> Self {
        Self::default()
    }

    pub fn set_main(&mut self, table_bytes: usize, interner_bytes: usize) {
        self.main_table_bytes = table_bytes;
        self.main_interner_bytes = interner_bytes;
    }

    pub fn combined_resident(
        &self,
        main_table_bytes: usize,
        main_interner_bytes: usize,
        overflow_bytes: usize,
    ) -> u64 {
        main_table_bytes
            .saturating_add(main_interner_bytes)
            .saturating_add(overflow_bytes) as u64
    }

    pub fn main_table_bytes(&self) -> usize {
        self.main_table_bytes
    }
}
