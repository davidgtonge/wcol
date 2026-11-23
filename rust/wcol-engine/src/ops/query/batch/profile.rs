//! Phase timers and spill-boundary probes for the batch aggregate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use super::group_spill;

pub(super) static PHASE_HASH_NS: AtomicU64 = AtomicU64::new(0);
pub(super) static PHASE_FINISH_NS: AtomicU64 = AtomicU64::new(0);
pub(super) static PHASE_FILTER_NS: AtomicU64 = AtomicU64::new(0);
pub(super) static PHASE_GROUPS_IN: AtomicU64 = AtomicU64::new(0);
pub(super) static PHASE_GROUPS_OUT: AtomicU64 = AtomicU64::new(0);
thread_local! {
    pub(super) static RESIDENT_GROUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(super) static RESIDENT_GROUP_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SPILL_RUNS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SPILL_GROUPS_FLUSHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SPILL_PEAK_LIVE_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SPILL_PEAK_INTERNER_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) fn phase_on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("WCOL_PHASE").is_some())
}

/// Clear the phase counters. Query bench reads them after one planned run when `WCOL_PHASE` is set.
pub fn reset_phase_timers() {
    PHASE_HASH_NS.store(0, Ordering::Relaxed);
    PHASE_FINISH_NS.store(0, Ordering::Relaxed);
    PHASE_FILTER_NS.store(0, Ordering::Relaxed);
    PHASE_GROUPS_IN.store(0, Ordering::Relaxed);
    PHASE_GROUPS_OUT.store(0, Ordering::Relaxed);
}

/// Groups in the hash table at the last finish. One reducer at a time.
pub fn last_resident_groups() -> u64 {
    RESIDENT_GROUPS.with(|groups| groups.get())
}

/// Hash-table bytes at the last finish. One reducer at a time.
pub fn last_resident_group_bytes() -> u64 {
    RESIDENT_GROUP_BYTES.with(|bytes| bytes.get())
}

pub fn last_spill_runs() -> u64 {
    SPILL_RUNS.with(|cell| cell.get())
}

pub fn last_spill_groups_flushed() -> u64 {
    SPILL_GROUPS_FLUSHED.with(|cell| cell.get())
}

pub fn last_spill_peak_live_bytes() -> u64 {
    SPILL_PEAK_LIVE_BYTES.with(|cell| cell.get())
}

pub fn last_spill_peak_interner_bytes() -> u64 {
    SPILL_PEAK_INTERNER_BYTES.with(|cell| cell.get())
}

/// One completed spill run. Bytes are the capacities left after the table reset.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpillBoundary {
    pub run: u32,
    pub wasm_before: u32,
    pub wasm_after: u32,
    pub live_groups: u32,
    pub live_table_before: u32,
    pub table_after: u32,
    pub index_cap_after: u32,
    pub interner_before: u32,
    pub interner_after: u32,
    pub sort_cap: u32,
    pub page_bytes: u32,
    pub page_cap: u32,
    pub live_after: u32,
    pub peak_after: u32,
    /// Allocator live bytes at flush entry.
    pub live_before: u32,
    /// Peak serialized spill payload handed to the host (append len and page cap).
    pub serialize_bytes: u32,
    /// `GraphHost.spill_append_buf` capacity at serialize peak.
    pub append_buf_cap: u32,
    /// Io bridge encoded SQLite record buffer capacity at peak.
    pub io_encode_cap: u32,
    /// Host outstanding I/O ops when the last SQLite chunk of the run was written.
    pub host_pending_ops: u32,
    /// Bytes in the last SQLite write chunk for the run.
    pub sqlite_write_bytes: u32,
    /// Host spill SQL payload still charged in the host ledger at the last ack.
    pub host_pending_sql_bytes: u32,
    /// Rust bridge queue: pending append payload not yet handed to graph host.
    pub rust_queued_spill_bytes: u32,
    /// Rust graph/io buffers holding serialized spill payload (append + sqlite encode).
    pub rust_inflight_spill_bytes: u32,
    /// Peak allocator live bytes while serializing spill pages this run.
    pub live_serialized_peak: u32,
    /// Allocator live bytes after every append page reached SQL ack, before group reset.
    pub live_sql_acked: u32,
    /// Cumulative merge scan opens seen before this boundary (0 during ingest-only flushes).
    pub merge_scan_begins: u32,
    /// Cumulative merge steps seen before this boundary.
    pub merge_steps: u32,
}

const SPILL_BOUNDARY_CAP: usize = 64;

thread_local! {
    static SPILL_BOUNDARIES: std::cell::RefCell<Vec<SpillBoundary>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy)]
pub(super) struct FlushStart {
    pub(super) wasm_before: u32,
    pub(super) live_groups: u32,
    pub(super) live_table: u32,
    pub(super) interner_before: u32,
}

thread_local! {
    pub(super) static FLUSH_START: std::cell::Cell<Option<FlushStart>> = const { std::cell::Cell::new(None) };
    pub(super) static LAST_PAGE: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
    pub(super) static FLUSH_PROBE: std::cell::Cell<FlushProbe> = const { std::cell::Cell::new(FlushProbe::EMPTY) };
    static SPILL_PAYLOAD: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
    static SPILL_MERGE: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
    static SPILL_BOUNDARY_WIRE: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Default)]
pub(super) struct FlushProbe {
    pub(super) live_before: u32,
    pub(super) serialize_bytes: u32,
    pub(super) append_buf_cap: u32,
    pub(super) io_encode_cap: u32,
    pub(super) host_pending_ops: u32,
    pub(super) sqlite_write_bytes: u32,
    pub(super) host_pending_sql_bytes: u32,
    pub(super) rust_queued_spill_bytes: u32,
    pub(super) rust_inflight_spill_bytes: u32,
    pub(super) live_serialized_peak: u32,
    pub(super) live_sql_acked: u32,
}

impl FlushProbe {
    const EMPTY: Self = Self {
        live_before: 0,
        serialize_bytes: 0,
        append_buf_cap: 0,
        io_encode_cap: 0,
        host_pending_ops: 0,
        sqlite_write_bytes: 0,
        host_pending_sql_bytes: 0,
        rust_queued_spill_bytes: 0,
        rust_inflight_spill_bytes: 0,
        live_serialized_peak: 0,
        live_sql_acked: 0,
    };
}

pub(super) fn spill_merge_counters() -> (u32, u32) {
    SPILL_MERGE.with(|cell| cell.get())
}

pub fn note_spill_merge_scan_begin() {
    SPILL_MERGE.with(|cell| {
        let (begins, steps) = cell.get();
        cell.set((begins.saturating_add(1), steps));
    });
}

pub fn note_spill_merge_step() {
    SPILL_MERGE.with(|cell| {
        let (begins, steps) = cell.get();
        cell.set((begins, steps.saturating_add(1)));
    });
}

pub(super) fn spill_payload_pair() -> (u32, u32) {
    SPILL_PAYLOAD.with(|cell| cell.get())
}

fn note_spill_payload_probe() {
    let (queued, inflight) = spill_payload_pair();
    FLUSH_PROBE.with(|cell| {
        let mut probe = cell.get();
        probe.rust_queued_spill_bytes = probe.rust_queued_spill_bytes.max(queued);
        probe.rust_inflight_spill_bytes = probe.rust_inflight_spill_bytes.max(inflight);
        cell.set(probe);
    });
}

pub fn note_spill_payload_queued(bytes: u32) {
    if bytes == 0 {
        return;
    }
    SPILL_PAYLOAD.with(|cell| {
        let (queued, inflight) = cell.get();
        cell.set((queued.saturating_add(bytes), inflight));
    });
    note_spill_payload_probe();
}

pub fn note_spill_payload_move_queued_to_inflight(bytes: u32) {
    if bytes == 0 {
        return;
    }
    SPILL_PAYLOAD.with(|cell| {
        let (queued, inflight) = cell.get();
        cell.set((queued.saturating_sub(bytes), inflight.saturating_add(bytes)));
    });
    note_spill_payload_probe();
}

pub fn note_spill_payload_release_inflight(bytes: u32) {
    if bytes == 0 {
        return;
    }
    SPILL_PAYLOAD.with(|cell| {
        let (queued, inflight) = cell.get();
        cell.set((queued, inflight.saturating_sub(bytes)));
    });
    note_spill_payload_probe();
}

pub fn note_spill_payload_inflight_add(bytes: u32) {
    if bytes == 0 {
        return;
    }
    SPILL_PAYLOAD.with(|cell| {
        let (queued, inflight) = cell.get();
        cell.set((queued, inflight.saturating_add(bytes)));
    });
    note_spill_payload_probe();
}

pub fn reset_flush_probe(live_before: u32) {
    FLUSH_PROBE.with(|cell| {
        cell.set(FlushProbe {
            live_before,
            ..FlushProbe::EMPTY
        })
    });
}

pub fn note_spill_serialize(append_len: u32, append_cap: u32) {
    let live = crate::formats::parquet::alloc_track::snapshot()
        .live_bytes
        .min(u32::MAX as usize) as u32;
    FLUSH_PROBE.with(|cell| {
        let mut probe = cell.get();
        probe.serialize_bytes = probe.serialize_bytes.max(append_len);
        probe.append_buf_cap = probe.append_buf_cap.max(append_cap);
        probe.live_serialized_peak = probe.live_serialized_peak.max(live);
        cell.set(probe);
    });
}

pub fn note_spill_io_encode(record_len: u32, record_cap: u32) {
    FLUSH_PROBE.with(|cell| {
        let mut probe = cell.get();
        probe.io_encode_cap = probe.io_encode_cap.max(record_cap.max(record_len));
        cell.set(probe);
    });
}

pub fn note_spill_host_ack(pending_ops: u32, write_bytes: u32, pending_sql_bytes: u32) {
    FLUSH_PROBE.with(|cell| {
        let mut probe = cell.get();
        probe.host_pending_ops = pending_ops;
        probe.sqlite_write_bytes = write_bytes;
        probe.host_pending_sql_bytes = pending_sql_bytes;
        cell.set(probe);
    });
}

pub(super) fn wasm_linear_bytes() -> u32 {
    #[cfg(target_arch = "wasm32")]
    {
        (core::arch::wasm32::memory_size(0) as u32).saturating_mul(65536)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

pub fn clear_spill_boundaries() {
    SPILL_BOUNDARIES.with(|cell| cell.borrow_mut().clear());
    SPILL_BOUNDARY_WIRE.with(|wire| wire.borrow_mut().clear());
    FLUSH_START.with(|cell| cell.set(None));
    LAST_PAGE.with(|cell| cell.set((0, 0)));
    FLUSH_PROBE.with(|cell| cell.set(FlushProbe::EMPTY));
    SPILL_PAYLOAD.with(|cell| cell.set((0, 0)));
    SPILL_MERGE.with(|cell| cell.set((0, 0)));
}

pub(super) fn note_spill_boundary(row: SpillBoundary) {
    SPILL_BOUNDARIES.with(|cell| {
        let mut rows = cell.borrow_mut();
        if rows.len() < SPILL_BOUNDARY_CAP {
            rows.push(row);
        }
        let mut bytes = Vec::with_capacity(rows.len() * 108);
        for sample in rows.iter() {
            bytes.extend_from_slice(&sample.run.to_le_bytes());
            bytes.extend_from_slice(&sample.wasm_before.to_le_bytes());
            bytes.extend_from_slice(&sample.wasm_after.to_le_bytes());
            bytes.extend_from_slice(&sample.live_groups.to_le_bytes());
            bytes.extend_from_slice(&sample.live_table_before.to_le_bytes());
            bytes.extend_from_slice(&sample.table_after.to_le_bytes());
            bytes.extend_from_slice(&sample.index_cap_after.to_le_bytes());
            bytes.extend_from_slice(&sample.interner_before.to_le_bytes());
            bytes.extend_from_slice(&sample.interner_after.to_le_bytes());
            bytes.extend_from_slice(&sample.sort_cap.to_le_bytes());
            bytes.extend_from_slice(&sample.page_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.page_cap.to_le_bytes());
            bytes.extend_from_slice(&sample.live_after.to_le_bytes());
            bytes.extend_from_slice(&sample.peak_after.to_le_bytes());
            bytes.extend_from_slice(&sample.live_before.to_le_bytes());
            bytes.extend_from_slice(&sample.serialize_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.append_buf_cap.to_le_bytes());
            bytes.extend_from_slice(&sample.io_encode_cap.to_le_bytes());
            bytes.extend_from_slice(&sample.host_pending_ops.to_le_bytes());
            bytes.extend_from_slice(&sample.sqlite_write_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.host_pending_sql_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.rust_queued_spill_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.rust_inflight_spill_bytes.to_le_bytes());
            bytes.extend_from_slice(&sample.live_serialized_peak.to_le_bytes());
            bytes.extend_from_slice(&sample.live_sql_acked.to_le_bytes());
            bytes.extend_from_slice(&sample.merge_scan_begins.to_le_bytes());
            bytes.extend_from_slice(&sample.merge_steps.to_le_bytes());
        }
        drop(rows);
        SPILL_BOUNDARY_WIRE.with(|wire| *wire.borrow_mut() = bytes);
    });
}

pub fn spill_boundary_wire() -> (*const u8, u32) {
    SPILL_BOUNDARY_WIRE.with(|wire| {
        let buf = wire.borrow();
        (buf.as_ptr(), buf.len() as u32)
    })
}

pub(super) fn publish_spill_stats(stats: group_spill::FlushStats) {
    SPILL_RUNS.with(|cell| cell.set(stats.runs));
    SPILL_GROUPS_FLUSHED.with(|cell| cell.set(stats.groups));
    SPILL_PEAK_LIVE_BYTES.with(|cell| cell.set(stats.peak_live_bytes));
    SPILL_PEAK_INTERNER_BYTES.with(|cell| cell.set(stats.peak_interner_bytes));
}

/// Hash, finish, filter, groups retained, groups emitted. Nanoseconds, then group counts.
pub fn phase_timers() -> (u64, u64, u64, u64, u64) {
    (
        PHASE_HASH_NS.load(Ordering::Relaxed),
        PHASE_FINISH_NS.load(Ordering::Relaxed),
        PHASE_FILTER_NS.load(Ordering::Relaxed),
        PHASE_GROUPS_IN.load(Ordering::Relaxed),
        PHASE_GROUPS_OUT.load(Ordering::Relaxed),
    )
}

pub(super) struct Phase {
    start: Option<Instant>,
    cell: &'static AtomicU64,
}

impl Drop for Phase {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.cell
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

pub(super) fn phase(cell: &'static AtomicU64) -> Phase {
    Phase {
        start: phase_on().then(Instant::now),
        cell,
    }
}
