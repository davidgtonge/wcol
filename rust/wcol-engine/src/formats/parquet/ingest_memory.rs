//! Host + Rust memory samples for Parquet ingest plateau tests (I-05 acceptance).

use super::alloc_track;

/// Byte caps for one ingest pipeline instance (peak simultaneous, not steady-state only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanMemoryBudget {
    pub compressed_bytes: usize,
    pub codec_scratch_bytes: usize,
    pub dictionary_bytes: usize,
    pub batch_bytes: usize,
    pub output_bytes: usize,
}

impl Default for ScanMemoryBudget {
    fn default() -> Self {
        Self::permissive_guardrails_only()
    }
}

impl ScanMemoryBudget {
    /// Default until callers pass an explicit ingest budget (`for_execution_group`, tests).
    pub fn permissive_guardrails_only() -> Self {
        Self {
            compressed_bytes: usize::MAX / 4,
            codec_scratch_bytes: usize::MAX / 4,
            dictionary_bytes: usize::MAX / 4,
            batch_bytes: usize::MAX / 4,
            output_bytes: usize::MAX / 4,
        }
    }

    /// Illustrative ingest-DO split (~32 MiB explicit working set). Not yet enforced end-to-end.
    pub fn ingest_do_example() -> Self {
        const MIB: usize = 1024 * 1024;
        Self {
            compressed_bytes: 4 * MIB,
            codec_scratch_bytes: 8 * MIB,
            dictionary_bytes: 8 * MIB,
            batch_bytes: 4 * MIB,
            output_bytes: 4 * MIB,
        }
    }

    /// Deliberately tight budget for Q3 plateau tests once ReadAt + streaming decode land.
    pub fn q3_acceptance_restrictive() -> Self {
        Self::ingest_do_example()
    }

    pub fn total_cap(&self) -> usize {
        self.compressed_bytes
            .saturating_add(self.codec_scratch_bytes)
            .saturating_add(self.dictionary_bytes)
            .saturating_add(self.batch_bytes)
            .saturating_add(self.output_bytes)
    }
}

/// One observation point (typically after finishing a row group's column chunks).
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestMemorySample {
    pub row_group: u32,
    pub rust_live_bytes: usize,
    pub rust_peak_bytes: usize,
    pub wasm_linear_bytes: usize,
    pub driver_chunk_in_flight: usize,
    pub driver_max_chunk_seen: usize,
    pub outstanding_http_requests: usize,
    pub http_reserved_bytes: usize,
}

impl IngestMemorySample {
    pub fn capture(row_group: u32, driver: &DriverMemoryCounters) -> Self {
        let snap = alloc_track::snapshot();
        Self {
            row_group,
            rust_live_bytes: snap.live_bytes,
            rust_peak_bytes: snap.peak_bytes,
            wasm_linear_bytes: snap.wasm_linear_bytes,
            driver_chunk_in_flight: driver.chunk_in_flight,
            driver_max_chunk_seen: driver.max_chunk_seen,
            outstanding_http_requests: driver.outstanding_http_requests,
            http_reserved_bytes: driver.http_reserved_bytes,
        }
    }
}

/// Host-side bytes the plateau harness tracks (ReadAt ring + prefetch credits).
#[derive(Debug, Clone, Copy, Default)]
pub struct DriverMemoryCounters {
    pub chunk_in_flight: usize,
    pub max_chunk_seen: usize,
    pub outstanding_http_requests: usize,
    pub http_reserved_bytes: usize,
}

impl DriverMemoryCounters {
    pub fn begin_chunk_fetch(&mut self, reserved_capacity: usize) {
        self.outstanding_http_requests = self.outstanding_http_requests.saturating_add(1);
        self.http_reserved_bytes = self.http_reserved_bytes.saturating_add(reserved_capacity);
        self.chunk_in_flight = reserved_capacity;
        self.max_chunk_seen = self.max_chunk_seen.max(reserved_capacity);
    }

    pub fn finish_chunk_fetch(&mut self) {
        self.outstanding_http_requests = self.outstanding_http_requests.saturating_sub(1);
        self.http_reserved_bytes = self
            .http_reserved_bytes
            .saturating_sub(self.chunk_in_flight);
        self.chunk_in_flight = 0;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IngestMemoryHighWater {
    pub rust_live_bytes: usize,
    pub rust_peak_bytes: usize,
    pub wasm_linear_bytes: usize,
    pub driver_chunk_in_flight: usize,
    pub driver_max_chunk_seen: usize,
    pub http_reserved_bytes: usize,
}

impl IngestMemoryHighWater {
    pub fn observe(&mut self, sample: &IngestMemorySample) {
        self.rust_live_bytes = self.rust_live_bytes.max(sample.rust_live_bytes);
        self.rust_peak_bytes = self.rust_peak_bytes.max(sample.rust_peak_bytes);
        self.wasm_linear_bytes = self.wasm_linear_bytes.max(sample.wasm_linear_bytes);
        self.driver_chunk_in_flight = self
            .driver_chunk_in_flight
            .max(sample.driver_chunk_in_flight);
        self.driver_max_chunk_seen = self.driver_max_chunk_seen.max(sample.driver_max_chunk_seen);
        self.http_reserved_bytes = self.http_reserved_bytes.max(sample.http_reserved_bytes);
    }
}

/// Compare late-row-group samples to an early baseline (after `warmup_groups`).
#[derive(Debug)]
pub struct PlateauReport {
    pub baseline_row_group: u32,
    pub baseline_wasm_linear: usize,
    pub max_late_wasm_linear: usize,
    pub baseline_rust_peak: usize,
    pub max_late_rust_peak: usize,
    pub wasm_plateau_ok: bool,
    pub rust_peak_plateau_ok: bool,
}

pub fn plateau_report(
    samples: &[IngestMemorySample],
    warmup_groups: usize,
    slack_bytes: usize,
) -> Option<PlateauReport> {
    if samples.len() <= warmup_groups {
        return None;
    }
    let baseline = &samples[warmup_groups];
    let mut max_late_wasm = baseline.wasm_linear_bytes;
    let mut max_late_peak = baseline.rust_peak_bytes;
    for sample in samples.iter().skip(warmup_groups + 1) {
        max_late_wasm = max_late_wasm.max(sample.wasm_linear_bytes);
        max_late_peak = max_late_peak.max(sample.rust_peak_bytes);
    }
    Some(PlateauReport {
        baseline_row_group: baseline.row_group,
        baseline_wasm_linear: baseline.wasm_linear_bytes,
        max_late_wasm_linear: max_late_wasm,
        baseline_rust_peak: baseline.rust_peak_bytes,
        max_late_rust_peak: max_late_peak,
        wasm_plateau_ok: max_late_wasm <= baseline.wasm_linear_bytes.saturating_add(slack_bytes),
        rust_peak_plateau_ok: max_late_peak <= baseline.rust_peak_bytes.saturating_add(slack_bytes),
    })
}
