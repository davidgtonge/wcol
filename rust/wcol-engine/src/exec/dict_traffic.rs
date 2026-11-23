//! Dictionary bytes shipped on a hash fanout and retained on a reducer.
//!
//! One Wasm instance is one scan or one reducer. Counters are thread-local to that instance.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::stream::DictionaryValues;

#[derive(Clone, Copy, Debug, Default)]
pub struct DictTraffic {
    pub entries_sent: u64,
    pub bytes_sent: u64,
    pub entries_referenced: u64,
    pub bytes_referenced: u64,
    pub bytes_retained: u64,
    pub bytes_released: u64,
    pub peak_retained: u64,
    pub max_stream_bytes: u64,
    pub streams_released: u64,
    pub active_dicts: u64,
    pub peak_active_dicts: u64,
    pub capacity_retained: u64,
    pub peak_capacity: u64,
}

struct PortDict {
    bits: Vec<u64>,
}

struct State {
    traffic: DictTraffic,
    /// `(partition, scope, column)` bits of dictionary ids a shipped row used.
    referenced: HashMap<(u32, u32, u32), PortDict>,
    /// `(port, scope, column)` payload bytes and allocation capacity currently installed.
    installed: HashMap<(u32, u32, u32), (u64, u64)>,
}

impl State {
    fn new() -> Self {
        Self {
            traffic: DictTraffic::default(),
            referenced: HashMap::new(),
            installed: HashMap::new(),
        }
    }
}

thread_local! {
    static TRAFFIC: RefCell<State> = RefCell::new(State::new());
    static PUBLISHED: RefCell<[u64; 13]> = const { RefCell::new([0; 13]) };
}

pub fn reset() {
    TRAFFIC.with(|cell| *cell.borrow_mut() = State::new());
}

pub fn payload_bytes(dictionary: &DictionaryValues) -> u64 {
    payload_bytes_from(dictionary, 0)
}

pub fn payload_bytes_from(dictionary: &DictionaryValues, from: usize) -> u64 {
    match &dictionary.entries {
        crate::stream::DictEntries::Int(entries) => entries.len().saturating_sub(from) as u64 * 8,
        crate::stream::DictEntries::Float(entries) => entries.len().saturating_sub(from) as u64 * 8,
        crate::stream::DictEntries::Bytes { offsets, .. } => {
            let end = offsets.last().copied().unwrap_or(0);
            let start = offsets.get(from).copied().unwrap_or(end);
            u64::from(end.saturating_sub(start))
        }
    }
}

fn entry_bytes(dictionary: &DictionaryValues, id: usize) -> u64 {
    match &dictionary.entries {
        crate::stream::DictEntries::Int(entries) if id < entries.len() => 8,
        crate::stream::DictEntries::Float(entries) if id < entries.len() => 8,
        crate::stream::DictEntries::Bytes { offsets, .. } if id + 1 < offsets.len() => {
            u64::from(offsets[id + 1].saturating_sub(offsets[id]))
        }
        _ => 0,
    }
}

/// A partition stream queued this dictionary. Later batches of the same scope do not send it again.
pub fn note_sent(entries: u64, bytes: u64) {
    if entries == 0 && bytes == 0 {
        return;
    }
    TRAFFIC.with(|cell| {
        let traffic = &mut cell.borrow_mut().traffic;
        traffic.entries_sent = traffic.entries_sent.saturating_add(entries);
        traffic.bytes_sent = traffic.bytes_sent.saturating_add(bytes);
    });
}

/// One shipped row used dictionary `id`. Counted once per partition stream.
pub fn note_referenced(partition: u32, dictionary: &DictionaryValues, column: u32, id: usize) {
    let entry_count = dictionary.entry_count();
    if id >= entry_count {
        return;
    }
    let bytes = entry_bytes(dictionary, id);
    TRAFFIC.with(|cell| {
        let state = &mut *cell.borrow_mut();
        let slot = state
            .referenced
            .entry((partition, dictionary.scope, column))
            .or_insert_with(|| PortDict {
                bits: vec![0; entry_count.div_ceil(64)],
            });
        if slot.bits.len() <= id / 64 {
            slot.bits.resize(entry_count.div_ceil(64), 0);
        }
        let word = &mut slot.bits[id / 64];
        let mask = 1u64 << (id % 64);
        if *word & mask != 0 {
            return;
        }
        *word |= mask;
        state.traffic.entries_referenced = state.traffic.entries_referenced.saturating_add(1);
        state.traffic.bytes_referenced = state.traffic.bytes_referenced.saturating_add(bytes);
    });
}

pub fn allocation_bytes(dictionary: &DictionaryValues) -> u64 {
    match &dictionary.entries {
        crate::stream::DictEntries::Int(entries) => entries.capacity() as u64 * 8,
        crate::stream::DictEntries::Float(entries) => entries.capacity() as u64 * 8,
        crate::stream::DictEntries::Bytes { offsets, bytes } => {
            offsets.capacity() as u64 * 4 + bytes.capacity() as u64
        }
    }
}

pub fn note_installed(port: u32, scope: u32, column: u32, bytes: u64, capacity: u64) {
    TRAFFIC.with(|cell| {
        let state = &mut *cell.borrow_mut();
        let key = (port, scope, column);
        let (previous_bytes, previous_capacity) = state
            .installed
            .insert(key, (bytes, capacity))
            .unwrap_or((0, 0));
        state.traffic.bytes_retained = state
            .traffic
            .bytes_retained
            .saturating_sub(previous_bytes)
            .saturating_add(bytes);
        state.traffic.capacity_retained = state
            .traffic
            .capacity_retained
            .saturating_sub(previous_capacity)
            .saturating_add(capacity);
        state.traffic.peak_retained = state
            .traffic
            .peak_retained
            .max(state.traffic.bytes_retained);
        state.traffic.peak_capacity = state
            .traffic
            .peak_capacity
            .max(state.traffic.capacity_retained);
        state.traffic.active_dicts = state.installed.len() as u64;
        state.traffic.peak_active_dicts = state
            .traffic
            .peak_active_dicts
            .max(state.traffic.active_dicts);
        let port_bytes = state
            .installed
            .iter()
            .filter(|(installed, _)| installed.0 == port)
            .fold(0u64, |sum, (_, (bytes, _))| sum.saturating_add(*bytes));
        state.traffic.max_stream_bytes = state.traffic.max_stream_bytes.max(port_bytes);
    });
}

pub fn note_stream_released(port: u32, keys: &[(u32, u32)]) {
    TRAFFIC.with(|cell| {
        let state = &mut *cell.borrow_mut();
        let mut stream_bytes = 0u64;
        for (scope, column) in keys {
            if let Some((bytes, capacity)) = state.installed.remove(&(port, *scope, *column)) {
                stream_bytes = stream_bytes.saturating_add(bytes);
                state.traffic.bytes_retained = state.traffic.bytes_retained.saturating_sub(bytes);
                state.traffic.capacity_retained =
                    state.traffic.capacity_retained.saturating_sub(capacity);
            }
        }
        state.traffic.active_dicts = state.installed.len() as u64;
        if stream_bytes == 0 && keys.is_empty() {
            return;
        }
        state.traffic.bytes_released = state.traffic.bytes_released.saturating_add(stream_bytes);
        state.traffic.streams_released = state.traffic.streams_released.saturating_add(1);
        state.traffic.max_stream_bytes = state.traffic.max_stream_bytes.max(stream_bytes);
    });
}

pub fn snapshot() -> DictTraffic {
    TRAFFIC.with(|cell| cell.borrow().traffic)
}

pub fn published_words() -> *const u8 {
    let traffic = snapshot();
    let words = [
        traffic.entries_sent,
        traffic.bytes_sent,
        traffic.entries_referenced,
        traffic.bytes_referenced,
        traffic.bytes_retained,
        traffic.bytes_released,
        traffic.peak_retained,
        traffic.max_stream_bytes,
        traffic.streams_released,
        traffic.active_dicts,
        traffic.peak_active_dicts,
        traffic.capacity_retained,
        traffic.peak_capacity,
    ];
    PUBLISHED.with(|cell| {
        *cell.borrow_mut() = words;
        cell.borrow().as_ptr() as *const u8
    })
}

pub const PUBLISHED_BYTES: u32 = 13 * 8;
