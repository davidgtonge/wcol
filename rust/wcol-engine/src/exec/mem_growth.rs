//! Snapshots taken when Wasm linear memory grows.
//!
//! Wasm memory does not shrink after a free. `wasm_linear_bytes` is the
//! high-water mark of the instance. `rust_live_bytes` is what the tracking
//! allocator still holds. A sample whose Wasm figure is far above its live
//! figure is retained address space, not retained objects.

use std::cell::{Cell, RefCell};

use crate::formats::parquet::alloc_track;

/// Parquet scan and hash-exchange encode.
pub const PHASE_SCAN: u8 = 1;
/// Reducer WCS decode, before rows enter the hash table.
pub const PHASE_WCS_INGRESS: u8 = 2;
/// Hash aggregation, including string interning.
pub const PHASE_HASH_AGGREGATE: u8 = 3;
/// Sorting and encoding a live table into a spill run.
pub const PHASE_SPILL_WRITE: u8 = 4;
/// Reading committed runs back and merging them.
pub const PHASE_SPILL_MERGE: u8 = 5;

const GROWTH_STEP: usize = 8 * 1024 * 1024;
const CAP: usize = 128;
const WORDS: usize = 11;

#[derive(Clone, Copy, Debug, Default)]
struct Gauges {
    phase: u8,
    group_table_bytes: u64,
    string_interner_bytes: u64,
    spill_runs_committed: u64,
    spill_pending_write_bytes: u64,
    spill_merge_buffer_bytes: u64,
    wcs_pending_bytes: u64,
    active_input_streams: u64,
    /// Encoded spill bytes sitting on the graph host, outside the store.
    host_spill_bytes: u64,
}

thread_local! {
    static PHASE: Cell<u8> = const { Cell::new(0) };
    static GAUGES: Cell<Gauges> = const { Cell::new(Gauges {
        phase: 0,
        group_table_bytes: 0,
        string_interner_bytes: 0,
        spill_runs_committed: 0,
        spill_pending_write_bytes: 0,
        spill_merge_buffer_bytes: 0,
        wcs_pending_bytes: 0,
        active_input_streams: 0,
        host_spill_bytes: 0,
    }) };
    static LAST_WASM: Cell<usize> = const { Cell::new(0) };
    static SAMPLES: RefCell<Vec<[u64; WORDS]>> = const { RefCell::new(Vec::new()) };
    static WIRE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn reset_baseline() {
    PHASE.set(0);
    GAUGES.set(Gauges::default());
    LAST_WASM.set(alloc_track::snapshot().wasm_linear_bytes);
    SAMPLES.with(|cell| cell.borrow_mut().clear());
}

pub fn set_phase(phase: u8) {
    PHASE.set(phase);
}

pub fn set_aggregate(
    group_table_bytes: u64,
    string_interner_bytes: u64,
    spill_runs_committed: u64,
    spill_pending_write_bytes: u64,
    spill_merge_buffer_bytes: u64,
) {
    GAUGES.with(|cell| {
        let mut gauges = cell.get();
        gauges.group_table_bytes = group_table_bytes;
        gauges.string_interner_bytes = string_interner_bytes;
        gauges.spill_runs_committed = spill_runs_committed;
        gauges.spill_pending_write_bytes = spill_pending_write_bytes;
        gauges.spill_merge_buffer_bytes = spill_merge_buffer_bytes;
        cell.set(gauges);
    });
}

pub fn set_spill(runs: u64, pending_write_bytes: u64, merge_buffer_bytes: u64) {
    GAUGES.with(|cell| {
        let mut gauges = cell.get();
        gauges.spill_runs_committed = runs;
        gauges.spill_pending_write_bytes = pending_write_bytes;
        gauges.spill_merge_buffer_bytes = merge_buffer_bytes;
        cell.set(gauges);
    });
}

pub fn set_ingress(wcs_pending_bytes: u64, active_input_streams: u64, host_spill_bytes: u64) {
    GAUGES.with(|cell| {
        let mut gauges = cell.get();
        gauges.wcs_pending_bytes = wcs_pending_bytes;
        gauges.active_input_streams = active_input_streams;
        gauges.host_spill_bytes = host_spill_bytes;
        cell.set(gauges);
    });
}

pub fn observe(force: bool) {
    let snap = alloc_track::snapshot();
    let wasm = snap.wasm_linear_bytes;
    let last = LAST_WASM.with(|cell| cell.get());
    let empty = SAMPLES.with(|cell| cell.borrow().is_empty());
    if !should_record(last, wasm, GROWTH_STEP, force, empty) {
        return;
    }
    LAST_WASM.with(|cell| cell.set(wasm));
    let mut gauges = GAUGES.with(|cell| cell.get());
    gauges.phase = PHASE.get();
    let sample = [
        u64::from(gauges.phase),
        wasm as u64,
        snap.live_bytes as u64,
        snap.peak_bytes as u64,
        gauges.group_table_bytes,
        gauges.string_interner_bytes,
        gauges.spill_runs_committed,
        gauges
            .spill_pending_write_bytes
            .saturating_add(gauges.host_spill_bytes),
        gauges.spill_merge_buffer_bytes,
        gauges.wcs_pending_bytes,
        gauges.active_input_streams,
    ];
    SAMPLES.with(|cell| {
        let mut samples = cell.borrow_mut();
        if samples.len() == CAP {
            samples.remove(1);
        }
        samples.push(sample);
    });
}

pub(crate) fn should_record(
    last: usize,
    wasm: usize,
    step: usize,
    force: bool,
    empty: bool,
) -> bool {
    wasm >= last.saturating_add(step) || (force && (empty || wasm != last))
}

/// Little-endian `WORDS` u64s per sample. The pointer is valid until the next read.
pub fn published_words() -> *const u8 {
    SAMPLES.with(|samples| {
        let samples = samples.borrow();
        WIRE.with(|wire| {
            let mut wire = wire.borrow_mut();
            wire.clear();
            wire.reserve(samples.len() * WORDS * 8);
            for sample in samples.iter() {
                for word in sample {
                    wire.extend_from_slice(&word.to_le_bytes());
                }
            }
            wire.as_ptr()
        })
    })
}

pub fn published_len() -> u32 {
    SAMPLES.with(|cell| (cell.borrow().len() * WORDS * 8) as u32)
}

#[cfg(test)]
mod tests {
    use super::should_record;

    #[test]
    fn growth_step_and_finish_are_distinct() {
        assert!(!should_record(0, 1024, 8 * 1024 * 1024, false, true));
        assert!(should_record(
            0,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            true
        ));
        assert!(should_record(100, 100, 8 * 1024 * 1024, true, true));
        assert!(!should_record(100, 100, 8 * 1024 * 1024, true, false));
        assert!(should_record(100, 200, 8 * 1024 * 1024, true, false));
    }
}
