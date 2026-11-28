//! Plan-driven object fetches: one host ReadAt per [`FetchRequest`], served to decode via slices.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::program::types::FetchPiece;
use crate::program::{FetchRequest, LogicalRead};

/// Host tickets for whole-plan fetches (distinct from decode slice tickets).
pub const FETCH_TICKET_BASE: u32 = 0x4000_0000;

/// Fetched bytes one scan may hold at once, in flight plus waiting to decode. Scans are
/// bound by request latency, so this is eight 2 MiB requests' worth: it was 32 MiB of a
/// scan's 128 MB isolate, and the isolate is shared with other node objects.
pub const FETCH_BUFFER_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FetchStageMetrics {
    pub host_requests: u32,
    pub host_bytes: u64,
    /// Largest total of fetched bytes held at once, waiting for the decoder.
    pub peak_resident_bytes: u64,
}

/// Why a decode read cannot be served from loaded fetches yet.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SliceError {
    /// A fetch the read needs has not arrived: load it, then retry the read.
    #[error("fetch {fetch_id} not loaded")]
    NotLoaded { fetch_id: u32 },
    /// The fetch plan cannot serve this read; retrying will not help.
    #[error("{0}")]
    Plan(String),
}

impl SliceError {
    fn plan(message: &str) -> Self {
        Self::Plan(message.to_string())
    }
}

/// The part of one fetch piece a read takes, in fetch-buffer coordinates.
struct PieceTake {
    fetch_id: u32,
    in_fetch: u64,
    len: u64,
}

impl PieceTake {
    fn end_in_fetch(&self) -> u64 {
        self.in_fetch.saturating_add(self.len)
    }
}

/// The bytes of `piece` inside the read-relative range `[rel_start, rel_end)`, if any.
fn piece_take(piece: &FetchPiece, rel_start: u64, rel_end: u64) -> Option<PieceTake> {
    let piece_end = piece.read_offset.saturating_add(piece.length);
    if piece_end <= rel_start || piece.read_offset >= rel_end {
        return None;
    }
    let take_start = piece.read_offset.max(rel_start);
    Some(PieceTake {
        fetch_id: piece.fetch_id,
        in_fetch: piece
            .fetch_offset
            .saturating_add(take_start - piece.read_offset),
        len: piece_end.min(rel_end) - take_start,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostFetchRead {
    pub ticket: u32,
    pub offset: u64,
    pub length: u32,
}

pub struct FetchStage {
    fetches: Vec<FetchRequest>,
    logical_reads: Vec<LogicalRead>,
    fetch_depth: usize,
    /// Cap on in-flight plus resident bytes (see [`Self::poll_host_fetch`]).
    buffer_bytes: u64,
    buffers: BTreeMap<u32, Vec<u8>>,
    /// Bytes not yet handed to the decoder from a loaded fetch.
    remaining_in_fetch: BTreeMap<u32, u64>,
    /// Host ticket → fetch id.
    in_flight: BTreeMap<u32, u32>,
    in_flight_bytes: u64,
    /// Bytes received so far for fetches the host delivers in several ring-sized parts.
    partial: BTreeMap<u32, Vec<u8>>,
    queue: VecDeque<usize>,
    next_ticket: u32,
    metrics: FetchStageMetrics,
    /// With claimed units: only fetches whose row groups were all granted may load.
    gate: Option<FetchGate>,
}

struct FetchGate {
    granted: BTreeSet<u32>,
    /// Row groups each fetch (by index) serves.
    groups: Vec<BTreeSet<u32>>,
}

impl FetchStage {
    pub fn new(
        fetches: Vec<FetchRequest>,
        logical_reads: Vec<LogicalRead>,
        fetch_depth: usize,
        buffer_bytes: u64,
    ) -> Self {
        let queue = decode_order(&fetches, &logical_reads);
        Self {
            fetches,
            logical_reads,
            fetch_depth: fetch_depth.max(1),
            buffer_bytes,
            buffers: BTreeMap::new(),
            remaining_in_fetch: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            in_flight_bytes: 0,
            partial: BTreeMap::new(),
            queue,
            next_ticket: FETCH_TICKET_BASE,
            metrics: FetchStageMetrics::default(),
            gate: None,
        }
    }

    /// Load only fetches of row groups granted with [`Self::grant`]: a scan that claims its
    /// units at run time must not read a unit another scan owns.
    pub fn gate_by_row_group(&mut self) {
        let groups = self
            .fetches
            .iter()
            .map(|fetch| {
                fetch
                    .logical_reads
                    .iter()
                    .filter_map(|id| self.logical_reads.iter().find(|read| read.id == *id))
                    .map(|read| read.row_group)
                    .collect()
            })
            .collect();
        self.gate = Some(FetchGate {
            granted: BTreeSet::new(),
            groups,
        });
    }

    pub fn grant(&mut self, row_groups: &[u32]) {
        if let Some(gate) = self.gate.as_mut() {
            gate.granted.extend(row_groups.iter().copied());
        }
    }

    /// Forget fetches that serve `row_groups` (another scan owns them).
    pub fn drop_row_groups(&mut self, row_groups: &[u32]) {
        let Some(gate) = self.gate.as_ref() else {
            return;
        };
        let dropped: BTreeSet<u32> = row_groups.iter().copied().collect();
        self.queue
            .retain(|&index| gate.groups[index].is_disjoint(&dropped));
    }

    fn may_load(&self, index: usize) -> bool {
        self.gate
            .as_ref()
            .is_none_or(|gate| gate.groups[index].is_subset(&gate.granted))
    }

    pub fn metrics(&self) -> &FetchStageMetrics {
        &self.metrics
    }

    pub fn is_fetch_ticket(ticket: u32) -> bool {
        ticket >= FETCH_TICKET_BASE
    }

    /// Move `fetch_id` to the front of the load queue unless it is loaded or in flight.
    pub fn prioritize_fetch(&mut self, fetch_id: u32) {
        if self.buffers.contains_key(&fetch_id) || self.in_flight.values().any(|id| *id == fetch_id)
        {
            return;
        }
        let Some(index) = self.fetches.iter().position(|fetch| fetch.id == fetch_id) else {
            return;
        };
        self.queue.retain(|&i| i != index);
        self.queue.push_front(index);
    }

    /// Slice bytes for a decode ReadAt at absolute file `offset`. Every piece is checked
    /// before any is consumed: a read straddling a loaded fetch and one still on its way
    /// must leave the loaded buffer and its remaining count untouched for the retry.
    pub fn slice_for_range(&mut self, offset: u64, length: u32) -> Result<Vec<u8>, SliceError> {
        let length = u64::from(length);
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| SliceError::plan("read range overflow"))?;
        let read = self
            .logical_reads
            .iter()
            .find(|read| offset >= read.offset && end <= read.offset.saturating_add(read.length))
            .ok_or_else(|| SliceError::Plan(format!("no logical read covers [{offset}, {end})")))?;
        if read.pieces.is_empty() {
            return Err(SliceError::plan("logical read has no fetch pieces"));
        }
        let rel_start = offset - read.offset;
        let takes: Vec<PieceTake> = read
            .pieces
            .iter()
            .filter_map(|piece| piece_take(piece, rel_start, rel_start + length))
            .collect();
        let covered: u64 = takes.iter().map(|take| take.len).sum();
        if covered != length {
            return Err(SliceError::Plan(format!(
                "plan pieces did not cover decode read (got {covered} want {length})"
            )));
        }
        for take in &takes {
            let buffer = self
                .buffers
                .get(&take.fetch_id)
                .ok_or(SliceError::NotLoaded {
                    fetch_id: take.fetch_id,
                })?;
            if take.end_in_fetch() > buffer.len() as u64 {
                return Err(SliceError::plan("fetch buffer shorter than plan"));
            }
            if !self.remaining_in_fetch.contains_key(&take.fetch_id) {
                return Err(SliceError::plan("fetch remaining bytes missing"));
            }
        }
        let mut out = Vec::with_capacity(length as usize);
        for take in &takes {
            let buffer = &self.buffers[&take.fetch_id];
            out.extend_from_slice(&buffer[take.in_fetch as usize..take.end_in_fetch() as usize]);
            let rem = self
                .remaining_in_fetch
                .get_mut(&take.fetch_id)
                .ok_or_else(|| SliceError::plan("fetch remaining bytes missing"))?;
            *rem = rem.saturating_sub(take.len);
            if *rem == 0 {
                self.remaining_in_fetch.remove(&take.fetch_id);
                self.buffers.remove(&take.fetch_id);
            }
        }
        Ok(out)
    }

    /// Next fetch to request from the host. At most `fetch_depth` are in flight, and in-flight
    /// plus resident bytes stay within `buffer_bytes`. A demand load (the decoder is blocked
    /// on that fetch) may pass the byte cap once nothing else is in flight: a row group can
    /// need one buffer per column before any of them frees, and it must still make progress.
    pub fn poll_host_fetch(&mut self, demand: bool) -> Option<HostFetchRead> {
        if self.in_flight.len() >= self.fetch_depth {
            return None;
        }
        let held = self.in_flight_bytes + self.resident_bytes() as u64;
        while let Some(position) = self.queue.iter().position(|&index| self.may_load(index)) {
            let index = self.queue[position];
            let fetch = &self.fetches[index];
            if self.buffers.contains_key(&fetch.id)
                || self.in_flight.values().any(|id| *id == fetch.id)
            {
                self.queue.remove(position);
                continue;
            }
            let fits = held + fetch.length <= self.buffer_bytes;
            if !(fits || (demand && self.in_flight.is_empty())) {
                return None;
            }
            self.queue.remove(position);
            let ticket = self.next_ticket;
            self.next_ticket = self.next_ticket.wrapping_add(1);
            self.in_flight.insert(ticket, fetch.id);
            self.in_flight_bytes += fetch.length;
            let length = u32::try_from(fetch.length).unwrap_or(u32::MAX);
            return Some(HostFetchRead {
                ticket,
                offset: fetch.offset,
                length,
            });
        }
        None
    }

    /// Accept bytes for an in-flight fetch, keeping the host's buffer as the fetch buffer. A
    /// fetch larger than the host's input ring arrives in several parts with the same ticket;
    /// it completes when all its bytes are in.
    pub fn complete_host_fetch(&mut self, ticket: u32, bytes: Vec<u8>) -> Result<(), String> {
        let fetch_id = *self
            .in_flight
            .get(&ticket)
            .ok_or_else(|| "unknown fetch ticket".to_string())?;
        let fetch = self
            .fetches
            .iter()
            .find(|fetch| fetch.id == fetch_id)
            .ok_or_else(|| "fetch id missing".to_string())?;
        let received = self.partial.get(&ticket).map_or(0, Vec::len) as u64;
        if received + bytes.len() as u64 > fetch.length {
            return Err(format!(
                "fetch {} length mismatch: got {} want {}",
                fetch.id,
                received + bytes.len() as u64,
                fetch.length
            ));
        }
        if received + (bytes.len() as u64) < fetch.length {
            self.partial
                .entry(ticket)
                .or_insert_with(|| Vec::with_capacity(fetch.length as usize))
                .extend_from_slice(&bytes);
            return Ok(());
        }
        self.in_flight.remove(&ticket);
        self.in_flight_bytes -= fetch.length;
        let bytes = match self.partial.remove(&ticket) {
            Some(mut head) => {
                head.extend_from_slice(&bytes);
                head
            }
            None => bytes,
        };
        self.metrics.host_requests += 1;
        self.metrics.host_bytes = self.metrics.host_bytes.saturating_add(fetch.length);
        // Free the buffer once every byte a logical read takes from it has been handed out.
        // Coalesced gap bytes belong to no read, so they are not counted.
        let needed: u64 = self
            .logical_reads
            .iter()
            .flat_map(|read| &read.pieces)
            .filter(|piece| piece.fetch_id == fetch.id)
            .map(|piece| piece.length)
            .sum();
        if needed == 0 {
            return Ok(());
        }
        self.remaining_in_fetch.insert(fetch.id, needed);
        self.buffers.insert(fetch.id, bytes);
        let resident = self.resident_bytes() as u64;
        self.metrics.peak_resident_bytes = self.metrics.peak_resident_bytes.max(resident);
        Ok(())
    }

    pub fn cancel_host_fetch(&mut self, ticket: u32) -> Result<(), String> {
        let fetch_id = self
            .in_flight
            .remove(&ticket)
            .ok_or_else(|| "unknown fetch ticket".to_string())?;
        self.partial.remove(&ticket);
        if let Some(fetch) = self.fetches.iter().find(|fetch| fetch.id == fetch_id) {
            self.in_flight_bytes -= fetch.length;
        }
        // Put the fetch back so a later demand load can request it again.
        if let Some(index) = self.fetches.iter().position(|fetch| fetch.id == fetch_id) {
            self.queue.push_front(index);
        }
        Ok(())
    }

    pub fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    pub fn resident_bytes(&self) -> usize {
        self.buffers.values().map(Vec::len).sum()
    }

    /// Granted fetches not yet handed to the decoder: queued, in flight, or loaded. A
    /// claiming scan asks for more units while this is under the fetch depth.
    pub fn granted_backlog(&self) -> usize {
        let queued = self
            .queue
            .iter()
            .filter(|&&index| self.may_load(index))
            .count();
        queued + self.in_flight.len() + self.buffers.len()
    }

    /// Granted row groups with a fetch still queued or in flight. Empty without claims.
    pub fn loading_row_groups(&self) -> BTreeSet<u32> {
        let Some(gate) = self.gate.as_ref() else {
            return BTreeSet::new();
        };
        let in_flight = self
            .fetches
            .iter()
            .enumerate()
            .filter(|(_, fetch)| self.in_flight.values().any(|id| *id == fetch.id))
            .map(|(index, _)| index);
        let queued = self
            .queue
            .iter()
            .copied()
            .filter(|&index| self.may_load(index));
        queued
            .chain(in_flight)
            .flat_map(|index| gate.groups[index].iter().copied())
            .filter(|group| gate.granted.contains(group))
            .collect()
    }

    pub fn fetch_depth(&self) -> usize {
        self.fetch_depth
    }

    /// A queued fetch could be requested now (unloaded, not in flight, and not gated).
    pub fn has_pending_loads(&self) -> bool {
        self.queue.iter().any(|&index| {
            let fetch = &self.fetches[index];
            self.may_load(index)
                && !self.buffers.contains_key(&fetch.id)
                && !self.in_flight.values().any(|id| *id == fetch.id)
        })
    }
}

/// Fetch indices in the order the decoder needs them. A row group decodes its columns
/// together, each at the rate its rows go by, so the decoder first needs a fetch at the row
/// fraction where its earliest piece starts. File-offset order would load all of one long
/// column (URL is several fetches) before the first bytes of the columns stored after it,
/// and those fetches would sit resident until the decoder caught up with them.
fn decode_order(fetches: &[FetchRequest], logical_reads: &[LogicalRead]) -> VecDeque<usize> {
    // Packed keys keep this to one sort of `u64`s, which the Wasm build already carries:
    // row group (24 bits), row fraction (20 bits), then the fetch index (20 bits), which
    // breaks ties in file order because the planner numbers fetches by offset.
    let mut keys: Vec<u64> = (0..fetches.len() as u64)
        .map(|index| (u64::MAX << 20) | index)
        .collect();
    for read in logical_reads {
        for piece in &read.pieces {
            let Ok(index) = fetches.binary_search_by_key(&piece.fetch_id, |fetch| fetch.id) else {
                continue;
            };
            // Chunks are far below 2^44 bytes, so the shift cannot overflow.
            let fraction = (piece.read_offset << 20) / read.length.max(1);
            let start = (u64::from(read.row_group.min(0xFF_FFFF)) << 40)
                | (fraction.min(0xF_FFFF) << 20)
                | index as u64;
            keys[index] = keys[index].min(start);
        }
    }
    keys.sort_unstable();
    keys.into_iter()
        .map(|key| (key & 0xF_FFFF) as usize)
        .collect()
}

#[cfg(test)]
mod tests;
