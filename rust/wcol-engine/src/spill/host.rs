//! Host round-trips for the R2 sorted-run spill: at most one run put, commit or read is
//! pending at a time, and the host completes it before the engine queues the next.

/// Host suspension is a normal progress state. `Err` is only a real failure.
#[derive(Debug)]
pub enum SpillProgress<T> {
    Ready(T),
    AwaitHost,
}

impl<T> SpillProgress<T> {
    pub fn expect_ready(self, message: &str) -> T {
        match self {
            Self::Ready(value) => value,
            Self::AwaitHost => panic!("{message}: unexpected spill await"),
        }
    }
}

pub const SPILL_OP_RUN_PUT: u8 = 5;
pub const SPILL_OP_RUN_COMMIT: u8 = 6;
pub const SPILL_OP_RUN_READ: u8 = 7;

/// One host operation: the op code and its encoded open payload (run put or read plan).
#[derive(Clone, Debug)]
pub struct HostSpillRequest {
    pub op: u8,
    pub append_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct HostSpillBridge {
    session: u64,
    pending: Option<HostSpillRequest>,
    /// Set when the graph host has taken the pending request; cleared on complete.
    host_inflight: bool,
    /// Encoded sorted run body for [`super::SPILL_OP_RUN_PUT`]; drained by the I/O bridge.
    pub run_upload: Vec<u8>,
    pub run_upload_off: usize,
    /// Last R2 run read chunk, written in place by the host (see `run_read_slot`).
    run_read_buf: Vec<u8>,
}

impl HostSpillBridge {
    pub fn new(session: u64) -> Self {
        Self {
            session,
            ..Self::default()
        }
    }

    pub fn take_pending(&mut self) -> Option<HostSpillRequest> {
        let req = self.pending.take()?;
        self.host_inflight = true;
        Some(req)
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some() || self.host_inflight
    }

    /// Drop a stranded `host_inflight` with no queued request (host already took the op).
    pub fn clear_stranded_inflight(&mut self) {
        if self.pending.is_none() && self.host_inflight {
            self.host_inflight = false;
        }
    }

    pub fn pending_request(&self) -> Option<HostSpillRequest> {
        self.pending.clone()
    }

    pub fn pending_op(&self) -> Option<u8> {
        self.pending.as_ref().map(|p| p.op)
    }

    pub fn session_id(&self) -> u64 {
        self.session
    }

    pub fn pending_write_bytes(&self) -> usize {
        let pending = self
            .pending
            .as_ref()
            .map(|request| request.append_bytes.capacity())
            .unwrap_or(0);
        pending.saturating_add(self.run_upload.capacity())
    }

    pub fn merge_read_bytes(&self) -> usize {
        self.run_read_buf.capacity()
    }

    pub fn complete_run_put(&mut self) -> Result<(), String> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.op != SPILL_OP_RUN_PUT)
        {
            return Err("spill complete mismatch (expected run put)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        self.run_upload_off = 0;
        self.run_upload.clear();
        Ok(())
    }

    pub fn complete_run_commit(&mut self) -> Result<(), String> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.op != SPILL_OP_RUN_COMMIT)
        {
            return Err("spill complete mismatch (expected run commit)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        Ok(())
    }

    /// The buffer the host writes one run read into. It is reused across reads: a fresh
    /// chunk-sized allocation per R2 read fragmented Wasm memory during the merge.
    /// Take `buffer` as the read slot when it is the larger. The merge reuses the last run
    /// part's buffer instead of freeing it and allocating another: Wasm memory never shrinks,
    /// so the freed part would stay a hole beside the new read slot.
    pub fn adopt_read_buffer(&mut self, mut buffer: Vec<u8>) {
        if buffer.capacity() > self.run_read_buf.capacity() {
            buffer.clear();
            self.run_read_buf = buffer;
        }
    }

    pub fn run_read_slot(&mut self, len: usize) -> &mut [u8] {
        self.run_read_buf.clear();
        self.run_read_buf.resize(len, 0);
        &mut self.run_read_buf
    }

    /// The host has filled `run_read_slot`.
    pub fn complete_run_read(&mut self) -> Result<(), String> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.op != SPILL_OP_RUN_READ)
        {
            return Err("spill complete mismatch (expected run read)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        Ok(())
    }

    pub fn run_read_bytes(&self) -> &[u8] {
        &self.run_read_buf
    }

    /// Mark the read bytes consumed; the capacity stays for the next read.
    pub fn clear_run_read(&mut self) {
        self.run_read_buf.clear();
    }

    #[cfg(test)]
    pub fn fill_run_read(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.run_read_slot(bytes.len()).copy_from_slice(bytes);
        self.complete_run_read()
    }

    fn queue(&mut self, req: HostSpillRequest) -> Result<SpillProgress<()>, String> {
        if self.pending.is_some() {
            return Err("spill host operation already outstanding".into());
        }
        self.pending = Some(req);
        Ok(SpillProgress::AwaitHost)
    }
}

pub(crate) fn await_host(
    bridge: &mut HostSpillBridge,
    req: HostSpillRequest,
) -> Result<SpillProgress<()>, String> {
    bridge.queue(req)
}
