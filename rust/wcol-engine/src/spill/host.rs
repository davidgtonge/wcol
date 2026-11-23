//! Durable Object SQLite spill via host round-trips (keyset pages, no live cursor).

use std::cell::RefCell;
use std::rc::Rc;

use super::{KeyPayload, SpillScanBatch, SpillScanResume, SpillStore};

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

pub const SPILL_OP_BEGIN: u8 = 1;
pub const SPILL_OP_APPEND: u8 = 2;
pub const SPILL_OP_SCAN: u8 = 3;
pub const SPILL_OP_END: u8 = 4;
pub const SPILL_OP_RUN_PUT: u8 = 5;
pub const SPILL_OP_RUN_COMMIT: u8 = 6;
pub const SPILL_OP_RUN_READ: u8 = 7;

#[derive(Clone, Debug)]
pub struct HostSpillRequest {
    pub op: u8,
    pub session: u64,
    pub resume: SpillScanResume,
    pub max_rows: u32,
    pub max_bytes: u32,
    pub append_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct HostSpillBridge {
    session: u64,
    session_open: bool,
    pending: Option<HostSpillRequest>,
    /// Set when the graph host has taken the pending request; cleared on complete.
    host_inflight: bool,
    scan_page: Option<SpillScanBatch>,
    append_scratch: Vec<u8>,
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
            .map(|request| {
                request
                    .append_bytes
                    .capacity()
                    .saturating_add(request.resume.key_blob.capacity())
            })
            .unwrap_or(0);
        pending
            .saturating_add(self.append_scratch.capacity())
            .saturating_add(self.run_upload.capacity())
    }

    pub fn merge_read_bytes(&self) -> usize {
        let page = self
            .scan_page
            .as_ref()
            .map(|page| super::store::keyed_row_bytes(&page.rows))
            .unwrap_or(0);
        page.saturating_add(self.run_read_buf.capacity())
    }

    pub fn complete_begin(&mut self) -> Result<(), String> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.op != SPILL_OP_BEGIN)
        {
            return Err("spill complete mismatch (expected begin)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        self.session_open = true;
        Ok(())
    }

    pub fn complete_append(&mut self) -> Result<(), String> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.op != SPILL_OP_APPEND)
        {
            return Err("spill complete mismatch (expected append)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        self.append_scratch.clear();
        Ok(())
    }

    pub fn complete_scan(&mut self, page: SpillScanBatch) -> Result<(), String> {
        if self.pending.as_ref().is_some_and(|p| p.op != SPILL_OP_SCAN) {
            return Err("spill complete mismatch (expected scan)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        self.scan_page = Some(page);
        Ok(())
    }

    pub fn complete_end(&mut self) -> Result<(), String> {
        if self.pending.as_ref().is_some_and(|p| p.op != SPILL_OP_END) {
            return Err("spill complete mismatch (expected end)".into());
        }
        self.pending = None;
        self.host_inflight = false;
        self.session_open = false;
        self.scan_page = None;
        Ok(())
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
        if req.op == super::SPILL_OP_APPEND {
            crate::ops::query::batch::note_spill_payload_queued(req.append_bytes.len() as u32);
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

/// After a bounded scan page, advance the composite cursor (matches native [`super::store::fetch_ordered_batch`]).
pub fn advance_scan_resume(resume: &mut SpillScanResume, batch: &SpillScanBatch) {
    if let Some((seq, key, _)) = batch.rows.last() {
        resume.started = true;
        resume.seq = *seq;
        resume.key_blob.clone_from(key);
    } else if !resume.started {
        resume.started = true;
    }
}

pub struct HostSpillStore {
    bridge: Rc<RefCell<HostSpillBridge>>,
    resume: SpillScanResume,
    scan_open: bool,
}

impl HostSpillStore {
    pub fn new(bridge: Rc<RefCell<HostSpillBridge>>) -> Self {
        Self {
            bridge,
            resume: SpillScanResume::default(),
            scan_open: false,
        }
    }
}

impl SpillStore for HostSpillStore {
    fn host_op_pending(&self) -> bool {
        self.bridge.borrow().has_pending()
    }

    fn begin(&mut self) -> Result<SpillProgress<()>, String> {
        if self.bridge.borrow().session_open {
            return Ok(SpillProgress::Ready(()));
        }
        let session = self.bridge.borrow().session;
        await_host(
            &mut self.bridge.borrow_mut(),
            HostSpillRequest {
                op: SPILL_OP_BEGIN,
                session,
                resume: SpillScanResume::default(),
                max_rows: 0,
                max_bytes: 0,
                append_bytes: Vec::new(),
            },
        )
    }

    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<SpillProgress<()>, String> {
        let mut bytes = Vec::new();
        encode_append_batch(&mut bytes, rows)?;
        let session = self.bridge.borrow().session;
        let mut bridge = self.bridge.borrow_mut();
        await_host(
            &mut bridge,
            HostSpillRequest {
                op: SPILL_OP_APPEND,
                session,
                resume: SpillScanResume::default(),
                max_rows: rows.len() as u32,
                max_bytes: bytes.len() as u32,
                append_bytes: bytes,
            },
        )
    }

    fn begin_ordered_scan(&mut self) -> Result<SpillProgress<()>, String> {
        self.scan_open = true;
        self.resume = SpillScanResume::default();
        Ok(SpillProgress::Ready(()))
    }

    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SpillProgress<SpillScanBatch>, String> {
        if !self.scan_open {
            return Err("spill ordered scan not started".into());
        }
        if let Some(page) = self.bridge.borrow_mut().scan_page.take() {
            advance_scan_resume(&mut self.resume, &page);
            *resume = self.resume.clone();
            return Ok(SpillProgress::Ready(page));
        }
        let session = self.bridge.borrow().session;
        self.resume = resume.clone();
        match await_host(
            &mut self.bridge.borrow_mut(),
            HostSpillRequest {
                op: SPILL_OP_SCAN,
                session,
                resume: resume.clone(),
                max_rows: max_rows as u32,
                max_bytes: max_bytes as u32,
                append_bytes: Vec::new(),
            },
        )? {
            SpillProgress::AwaitHost => Ok(SpillProgress::AwaitHost),
            SpillProgress::Ready(()) => unreachable!("spill scan always awaits host"),
        }
    }

    fn end_ordered_scan(&mut self) -> Result<(), String> {
        self.scan_open = false;
        Ok(())
    }

    fn end(&mut self) -> Result<SpillProgress<()>, String> {
        if !self.bridge.borrow().session_open {
            return Ok(SpillProgress::Ready(()));
        }
        let session = self.bridge.borrow().session;
        await_host(
            &mut self.bridge.borrow_mut(),
            HostSpillRequest {
                op: SPILL_OP_END,
                session,
                resume: SpillScanResume::default(),
                max_rows: 0,
                max_bytes: 0,
                append_bytes: Vec::new(),
            },
        )
    }
}

pub fn encode_append_batch(out: &mut Vec<u8>, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (key, partial) in rows {
        if key.len() > u32::MAX as usize || partial.len() > u32::MAX as usize {
            return Err("spill row too large".into());
        }
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(partial.len() as u32).to_le_bytes());
        out.extend_from_slice(partial);
    }
    Ok(())
}

pub fn decode_append_batch(bytes: &[u8]) -> Result<Vec<KeyPayload>, String> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut off = 0;
    let count = read_u32(bytes, &mut off)? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = read_u32(bytes, &mut off)? as usize;
        let key = read_blob(bytes, &mut off, key_len)?;
        let value_len = read_u32(bytes, &mut off)? as usize;
        let value = read_blob(bytes, &mut off, value_len)?;
        rows.push((key, value));
    }
    Ok(rows)
}

pub fn encode_scan_page(batch: &SpillScanBatch) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(batch.rows.len() as u32).to_le_bytes());
    for (seq, key, partial) in &batch.rows {
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(partial.len() as u32).to_le_bytes());
        out.extend_from_slice(partial);
    }
    out.push(u8::from(batch.exhausted));
    out
}

pub fn decode_scan_page(bytes: &[u8]) -> Result<SpillScanBatch, String> {
    if bytes.len() < 5 {
        return Ok(SpillScanBatch {
            rows: Vec::new(),
            exhausted: bytes.get(4).copied().unwrap_or(1) != 0,
        });
    }
    let mut off = 0;
    let row_count = read_u32(bytes, &mut off)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let seq = read_u64(bytes, &mut off)?;
        let key_len = read_u32(bytes, &mut off)? as usize;
        let key = read_blob(bytes, &mut off, key_len)?;
        let partial_len = read_u32(bytes, &mut off)? as usize;
        let partial = read_blob(bytes, &mut off, partial_len)?;
        rows.push((seq, key, partial));
    }
    let exhausted = *bytes.get(off).unwrap_or(&1) != 0;
    Ok(SpillScanBatch { rows, exhausted })
}

pub fn table_name(session: u64) -> String {
    format!("spill_{session:016x}")
}

fn read_u32(bytes: &[u8], off: &mut usize) -> Result<u32, String> {
    let end = off.saturating_add(4);
    let chunk: [u8; 4] = bytes
        .get(*off..end)
        .ok_or("truncated spill wire")?
        .try_into()
        .unwrap();
    *off = end;
    Ok(u32::from_le_bytes(chunk))
}

fn read_u64(bytes: &[u8], off: &mut usize) -> Result<i64, String> {
    let end = off.saturating_add(8);
    let chunk: [u8; 8] = bytes
        .get(*off..end)
        .ok_or("truncated spill wire")?
        .try_into()
        .unwrap();
    *off = end;
    Ok(i64::from_le_bytes(chunk))
}

fn read_blob(bytes: &[u8], off: &mut usize, len: usize) -> Result<Vec<u8>, String> {
    let end = off.saturating_add(len);
    let slice = bytes.get(*off..end).ok_or("truncated spill blob")?;
    *off = end;
    Ok(slice.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn host_store_advances_resume_after_scan_page() {
        let bridge = Rc::new(RefCell::new(HostSpillBridge::new(7)));
        let mut store = HostSpillStore::new(bridge.clone());
        store
            .begin_ordered_scan()
            .unwrap()
            .expect_ready("scan open");
        let mut resume = SpillScanResume::default();
        assert!(matches!(
            store.next_ordered_batch(&mut resume, 2, 4096).unwrap(),
            SpillProgress::AwaitHost
        ));
        let page1 = SpillScanBatch {
            rows: vec![
                (1, b"k1".to_vec(), b"p1".to_vec()),
                (2, b"k1".to_vec(), b"p2".to_vec()),
            ],
            exhausted: false,
        };
        bridge.borrow_mut().complete_scan(page1).unwrap();
        let got = store
            .next_ordered_batch(&mut resume, 2, 4096)
            .unwrap()
            .expect_ready("scan page");
        assert_eq!(got.rows.len(), 2);
        assert_eq!(resume.seq, 2);
        assert_eq!(resume.key_blob, b"k1");
        assert!(matches!(
            store.next_ordered_batch(&mut resume, 2, 4096).unwrap(),
            SpillProgress::AwaitHost
        ));
        let pending = bridge.borrow().pending_request().expect("pending scan");
        assert_eq!(pending.resume.seq, 2);
        assert_eq!(pending.resume.key_blob, b"k1");
    }
}
