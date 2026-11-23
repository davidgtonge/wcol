//! Shared poll actions, progress, edge buffers, and WCS1 size constants.
//!
//! Query execution is [`GraphHost`](crate::exec::graph_host::GraphHost). This module does not
//! run operators. [`EdgeBuffer`] is the fixed ring between the host and the graph.

pub const WCS_HEADER_BYTES: usize = 32;
/// Largest WCS1 payload. Producers split logical batches so each frame fits.
pub const WCS_MAX_PAYLOAD: usize = 64 * 1024;
pub const WCS_MAGIC: u32 = 0x3153_4357; // WCS1 little-endian

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    NeedInput,
    HaveOutput,
    Finished,
    Yield,
    ReadAt,
    SpillExec,
    Failed,
}

impl Action {
    pub fn code(self) -> i32 {
        match self {
            Self::NeedInput => 1,
            Self::HaveOutput => 2,
            Self::Finished => 3,
            Self::Yield => 4,
            Self::ReadAt => 5,
            Self::SpillExec => 6,
            Self::Failed => -1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Progress {
    pub consumed: usize,
    pub produced: usize,
    pub finished: bool,
    pub read_at_ticket: u32,
    pub read_at_source: u16,
    pub read_at_offset: u64,
    pub read_at_len: u32,
}

impl Progress {
    pub fn with_read_at(ticket: u32, source: u16, offset: u64, len: u32) -> Self {
        Self {
            read_at_ticket: ticket,
            read_at_source: source,
            read_at_offset: offset,
            read_at_len: len,
            ..Default::default()
        }
    }

    pub fn read_at(&self) -> Option<(u32, u16, u64, u32)> {
        if self.read_at_len == 0 {
            None
        } else {
            Some((
                self.read_at_ticket,
                self.read_at_source,
                self.read_at_offset,
                self.read_at_len,
            ))
        }
    }
}

/// Bounded buffer for one materialized **local edge** (byte stream between operators).
///
/// The scheduler owns the edge; operators borrow `writable` / `readable` slices for one
/// `poll` only. `commit` / `consume` advance producer and consumer cursors.
/// For colocated fusion without serialization, operators may bypass an edge entirely.
pub struct EdgeBuffer {
    buf: Box<[u8]>,
    read: usize,
    write: usize,
    used: usize,
    closed: bool,
}

impl EdgeBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: vec![0; capacity].into_boxed_slice(),
            read: 0,
            write: 0,
            used: 0,
            closed: false,
        }
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn readable(&self) -> &[u8] {
        if self.used == 0 {
            return &self.buf[..0];
        }
        let end = if self.read < self.write {
            self.write
        } else {
            self.buf.len()
        };
        &self.buf[self.read..end]
    }

    pub fn writable(&mut self) -> &mut [u8] {
        if self.closed || self.used == self.buf.len() {
            return &mut self.buf[..0];
        }
        let end = if self.write < self.read {
            self.read
        } else {
            self.buf.len()
        };
        &mut self.buf[self.write..end]
    }

    /// Bytes committed by the producer (alias: `produce` in older notes).
    pub fn commit(&mut self, n: usize) {
        assert!(n <= self.buf.len() - self.used);
        self.write = (self.write + n) % self.buf.len();
        self.used += n;
    }

    /// Bytes consumed by the downstream operator.
    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.used);
        self.read = (self.read + n) % self.buf.len();
        self.used -= n;
        if self.used == 0 {
            self.read = 0;
            self.write = 0;
        }
    }

    pub(crate) fn mark_closed(&mut self) {
        self.closed = true;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}
