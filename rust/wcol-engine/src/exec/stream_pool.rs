//! Fixed input mailboxes and logical output ports (up to 128 each, see below)
//! backed by one bounded egress buffer per logical output port.

use crate::error::{WcolError, WcolResult};
use crate::exec::wcol_session::{open_wcol_plan_shared, WcolSession};
use crate::stream::core::Action;

/// Streams one pool task may take in or send out: the most scans one reducer reads, and the
/// most reducers one scan feeds. The host splits fixed buffer totals across the ports
/// (`port-budget.ts`), so widening a job does not grow a task's memory.
pub const MAX_STREAM_INPUT_PORTS: usize = 128;
pub const MAX_STREAM_OUTPUT_PORTS: usize = 128;

/// Status bits [`StreamPoolSession::step`] returns (the Wasm host reads the same bits).
pub const STEP_PROGRESS: u32 = 1;
pub const STEP_RUNNABLE: u32 = 2;
pub const STEP_FINISHED: u32 = 4;
pub const STEP_FAILED: u32 = 8;
pub const STEP_SPILL_EXEC: u32 = 16;
pub const STEP_RESIDENT_READY: u32 = 32;

struct SlotBuf {
    data: Box<[u8]>,
    len: usize,
}

impl SlotBuf {
    fn new(cap: usize) -> Self {
        Self {
            data: vec![0u8; cap].into_boxed_slice(),
            len: 0,
        }
    }
}

struct InPort {
    buf: SlotBuf,
    full: bool,
    ended: bool,
    absent: bool,
    writer_free: bool,
    end_notified: bool,
    absent_notified: bool,
}

struct OutPort {
    buf: SlotBuf,
    full: bool,
    len: usize,
}

enum Driver {
    Echo {
        cursor: usize,
    },
    Graph {
        host: Box<WcolSession>,
        finished: bool,
        /// Exposed ReadAt `(ticket, source, offset, len)`; all zero when none is pending.
        read_at: (u32, u16, u64, u32),
    },
}

pub struct StreamPoolSession {
    inputs: Vec<InPort>,
    outputs: Vec<OutPort>,
    driver: Driver,
    error: Option<WcolError>,
}

impl StreamPoolSession {
    pub fn open_echo(
        input_ports: u32,
        output_ports: u32,
        input_bytes: u32,
        output_bytes: u32,
    ) -> WcolResult<Self> {
        Self::open(
            input_ports,
            output_ports,
            input_bytes,
            output_bytes,
            Driver::Echo { cursor: 0 },
        )
    }

    pub fn open_graph(
        program: &[u8],
        scratch_capacity: usize,
        input_ports: u32,
        output_ports: u32,
        input_bytes: u32,
        output_bytes: u32,
        budget_share: u32,
    ) -> WcolResult<Self> {
        Self::validate_counts(input_ports, output_ports)?;
        let mut host = open_wcol_plan_shared(program, scratch_capacity, budget_share)?;
        if host.hash_partition_count() > output_ports {
            return Err(WcolError::msg(format!(
                "program writes {} hash partitions but the pool has {output_ports} output ports",
                host.hash_partition_count()
            )));
        }
        host.enable_direct_port_egress();
        Self::open(
            input_ports,
            output_ports,
            input_bytes,
            output_bytes,
            Driver::Graph {
                host: Box::new(host),
                finished: false,
                read_at: (0, 0, 0, 0),
            },
        )
    }

    fn open(
        input_ports: u32,
        output_ports: u32,
        input_bytes: u32,
        output_bytes: u32,
        driver: Driver,
    ) -> WcolResult<Self> {
        Self::validate_counts(input_ports, output_ports)?;
        let in_cap = input_bytes as usize;
        let out_cap = if output_ports == 0 {
            0
        } else {
            output_bytes as usize
        };
        Ok(Self {
            inputs: (0..input_ports as usize)
                .map(|_| InPort {
                    buf: SlotBuf::new(in_cap),
                    full: false,
                    ended: false,
                    absent: false,
                    writer_free: true,
                    end_notified: false,
                    absent_notified: false,
                })
                .collect(),
            outputs: (0..output_ports as usize)
                .map(|_| OutPort {
                    buf: SlotBuf::new(out_cap),
                    full: false,
                    len: 0,
                })
                .collect(),
            driver,
            error: None,
        })
    }

    fn validate_counts(input_ports: u32, output_ports: u32) -> WcolResult<()> {
        if input_ports as usize > MAX_STREAM_INPUT_PORTS {
            return Err(WcolError::msg(format!(
                "stream pool input port count exceeds {MAX_STREAM_INPUT_PORTS}"
            )));
        }
        if output_ports as usize > MAX_STREAM_OUTPUT_PORTS {
            return Err(WcolError::msg(format!(
                "stream pool output port count exceeds {MAX_STREAM_OUTPUT_PORTS}"
            )));
        }
        if input_ports == 0 && output_ports == 0 {
            return Err(WcolError::from("stream pool needs at least one port"));
        }
        Ok(())
    }

    fn publish_output(
        outputs: &mut [OutPort],
        port: usize,
        data: &[u8],
        error: &mut Option<WcolError>,
    ) -> bool {
        let Some(out) = outputs.get_mut(port) else {
            return false;
        };
        if out.full || out.buf.data.is_empty() {
            return false;
        }
        if data.len() > out.buf.data.len() {
            *error = Some(WcolError::from("output chunk exceeds slot capacity"));
            return false;
        }
        let n = data.len();
        out.buf.data[..n].copy_from_slice(data);
        out.buf.len = n;
        out.len = n;
        out.full = true;
        true
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_ref().map(WcolError::as_str)
    }

    pub fn input_ptr(&mut self, port: u32) -> *mut u8 {
        self.inputs
            .get_mut(port as usize)
            .map_or(std::ptr::null_mut(), |p| p.buf.data.as_mut_ptr())
    }

    pub fn input_free(&self, port: u32) -> u32 {
        self.inputs.get(port as usize).map_or(0, |p| {
            u32::from(p.writer_free && !p.full && !p.ended && !p.absent)
        })
    }

    pub fn input_commit(&mut self, port: u32, length: u32) -> WcolResult<()> {
        let p = self
            .inputs
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::from("invalid input port"))?;
        if p.full || p.ended || p.absent {
            return Err(WcolError::from("input slot busy or closed"));
        }
        if length == 0 || length as usize > p.buf.data.len() {
            return Err(WcolError::from("invalid input commit length"));
        }
        p.buf.len = length as usize;
        p.full = true;
        p.writer_free = false;
        Ok(())
    }

    pub fn input_end(&mut self, port: u32) -> WcolResult<()> {
        let p = self
            .inputs
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::from("invalid input port"))?;
        if p.full || p.ended || p.absent {
            return Err(WcolError::from("input slot busy or closed"));
        }
        p.ended = true;
        p.writer_free = false;
        Ok(())
    }

    pub fn input_absent(&mut self, port: u32) -> WcolResult<()> {
        let p = self
            .inputs
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::from("invalid input port"))?;
        if p.absent {
            return Ok(());
        }
        if p.full || p.ended {
            return Err(WcolError::from("input slot busy"));
        }
        p.absent = true;
        p.ended = true;
        p.writer_free = true;
        Ok(())
    }

    pub fn output_len(&self, port: u32) -> u32 {
        self.outputs
            .get(port as usize)
            .map_or(0, |p| if p.full { p.len as u32 } else { 0 })
    }

    pub fn output_ptr(&self, port: u32) -> *const u8 {
        let out = match self.outputs.get(port as usize) {
            Some(out) if out.full => out,
            _ => return std::ptr::null(),
        };
        out.buf.data.as_ptr()
    }

    pub fn output_release(&mut self, port: u32) -> WcolResult<()> {
        let p = self
            .outputs
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::from("invalid output port"))?;
        if !p.full {
            return Err(WcolError::from("output slot empty"));
        }
        p.buf.len = 0;
        p.full = false;
        p.len = 0;
        Ok(())
    }

    pub fn output_done(&self, port: u32) -> u32 {
        let Some(out) = self.outputs.get(port as usize) else {
            return 0;
        };
        if out.full {
            return 0;
        }
        match &self.driver {
            Driver::Graph { finished, host, .. } => {
                if host.resident_ready() {
                    return 1;
                }
                if !*finished {
                    return 0;
                }
                if host.hash_partition_count() > 0 {
                    let part = port as usize;
                    if part >= host.hash_partition_count() as usize {
                        return 1;
                    }
                    if out.full {
                        return 0;
                    }
                    if host.hash_opened(part) && host.hash_partition_has_pending_output(part) {
                        return 0;
                    }
                    return 1;
                }
                if port == 0 && !host.output().is_empty() {
                    return 0;
                }
                1
            }
            Driver::Echo { .. } => {
                if self.inputs.is_empty() {
                    return 0;
                }
                u32::from(self.is_finished())
            }
        }
    }

    pub fn step(&mut self, max_work: u32) -> u32 {
        if self.error.is_some() {
            return STEP_FAILED;
        }
        let (processed, spill_exec) = match &mut self.driver {
            Driver::Echo { cursor } => (
                Self::step_echo_ports(
                    &mut self.inputs,
                    &mut self.outputs,
                    &mut self.error,
                    max_work as usize,
                    cursor,
                ),
                false,
            ),
            Driver::Graph {
                host,
                finished,
                read_at,
            } => Self::step_graph_ports(
                &mut self.inputs,
                &mut self.outputs,
                &mut self.error,
                host,
                finished,
                read_at,
                max_work as usize,
            ),
        };
        let mut status = 0;
        if spill_exec {
            status |= STEP_SPILL_EXEC;
        }
        if processed > 0 {
            status |= STEP_PROGRESS;
        }
        if self.error.is_some() {
            status |= STEP_FAILED;
        } else if self.resident_ready() {
            status |= STEP_RESIDENT_READY;
        } else if self.is_finished() {
            status |= STEP_FINISHED;
        } else {
            status |= STEP_RUNNABLE;
        }
        status
    }

    pub fn is_resident(&self) -> bool {
        self.graph_host().is_some_and(|host| host.is_resident())
    }

    pub fn resident_ready(&self) -> bool {
        self.graph_host().is_some_and(|host| host.resident_ready())
    }

    pub fn resident_response(&self) -> &[u8] {
        self.graph_host()
            .map_or(&[], |host| host.resident_response())
    }

    pub fn resident_command(&mut self, kind: u32, bytes: &[u8]) -> WcolResult<()> {
        if self.error.is_some() {
            return Err(WcolError::from("resident graph failed"));
        }
        self.graph_host_mut()
            .ok_or_else(|| WcolError::from("not a graph session"))?
            .resident_command(kind, bytes)
    }

    fn is_finished(&self) -> bool {
        match &self.driver {
            Driver::Echo { .. } => {
                self.inputs.iter().all(|p| (p.ended || p.absent) && !p.full)
                    && self.outputs.iter().all(|p| !p.full)
            }
            Driver::Graph { finished, .. } => *finished,
        }
    }

    fn release_input_slot(inputs: &mut [InPort], port: usize) {
        if let Some(p) = inputs.get_mut(port) {
            p.full = false;
            p.buf.len = 0;
            p.writer_free = true;
        }
    }

    fn step_echo_ports(
        inputs: &mut [InPort],
        outputs: &mut [OutPort],
        error: &mut Option<WcolError>,
        max_work: usize,
        cursor: &mut usize,
    ) -> u32 {
        let count = inputs.len();
        if count == 0 {
            return 0;
        }
        let mut examined = 0;
        let mut processed = 0;
        while examined < count && processed < max_work {
            let port = (*cursor + examined) % count;
            examined += 1;
            let Some(input) = inputs.get_mut(port) else {
                continue;
            };
            if input.absent {
                if input.full {
                    Self::release_input_slot(inputs, port);
                    processed += 1;
                }
                continue;
            }
            if !input.full && input.ended {
                input.writer_free = true;
                continue;
            }
            if !input.full {
                continue;
            }
            if outputs.is_empty() {
                Self::release_input_slot(inputs, port);
                processed += 1;
                continue;
            }
            let dest = port % outputs.len();
            if outputs[dest].full {
                continue;
            }
            let data = &input.buf.data[..input.buf.len];
            if !Self::publish_output(outputs, dest, data, error) {
                continue;
            }
            Self::release_input_slot(inputs, port);
            processed += 1;
        }
        *cursor = (*cursor + examined) % count.max(1);
        processed as u32
    }

    fn step_graph_ports(
        inputs: &mut [InPort],
        outputs: &mut [OutPort],
        error: &mut Option<WcolError>,
        host: &mut WcolSession,
        finished: &mut bool,
        read_at: &mut (u32, u16, u64, u32),
        max_work: usize,
    ) -> (u32, bool) {
        let mut work: u32 = 0;
        for port in 0..inputs.len() {
            if work >= max_work as u32 {
                break;
            }
            let full = inputs.get(port).is_some_and(|p| p.full);
            if !full {
                continue;
            }
            let len = inputs[port].buf.len;
            let n = {
                let bytes = &inputs[port].buf.data[..len];
                match host.try_feed_input_port(port as u32, bytes) {
                    Ok(n) => n,
                    Err(e) => {
                        *error = Some(WcolError::msg(e.to_string()));
                        return (work, false);
                    }
                }
            };
            if n == 0 {
                continue;
            }
            if n > len {
                *error = Some(WcolError::from("ingress consumed past the mailbox"));
                return (work, false);
            }
            if n == len {
                Self::release_input_slot(inputs, port);
            } else {
                let buf = &mut inputs[port].buf;
                buf.data.copy_within(n..len, 0);
                buf.len = len - n;
            }
            work += 1;
        }
        for port in 0..inputs.len() {
            let Some(input) = inputs.get_mut(port) else {
                continue;
            };
            if input.absent && !input.full && !input.absent_notified {
                if let Err(e) = host.mark_input_absent(port as u32) {
                    *error = Some(WcolError::msg(e.to_string()));
                    return (work, false);
                }
                input.absent_notified = true;
            } else if input.ended && !input.full && !input.end_notified {
                if let Err(e) = host.end_input_port(port as u32) {
                    *error = Some(WcolError::msg(e.to_string()));
                    return (work, false);
                }
                input.end_notified = true;
            }
        }
        for _ in 0..max_work.saturating_sub(work as usize) {
            let action = host.poll();
            host.note_memory_growth(matches!(action, Action::Finished));
            match action {
                Action::NeedInput => {
                    // Ingress mailboxes still holding bytes means the graph is
                    // blocked on draining pending frames, not on new host input.
                    // Parking here leaves attachInput blocked on a full port.
                    if inputs.iter().any(|port| port.full) {
                        work = work.saturating_add(1);
                        continue;
                    }
                    break;
                }
                // Return to the host on Yield so Durable Objects can run RPCs
                // and the stream pool keeps STEP_PROGRESS (idle Yield loops
                // previously returned work=0 and stopped the advance loop).
                Action::Yield => {
                    work = work.saturating_add(1);
                    break;
                }
                Action::ReadAt => {
                    *read_at = host.read_at();
                    break;
                }
                Action::SpillExec => return (work, true),
                Action::Failed => {
                    let msg = host.error().unwrap_or("graph host failed");
                    *error = Some(WcolError::msg(msg.to_string()));
                    break;
                }
                Action::HaveOutput => {
                    if !Self::drain_host_output(outputs, error, host) {
                        break;
                    }
                    work += 1;
                }
                Action::Finished => {
                    *finished = true;
                    break;
                }
            }
        }
        (work, false)
    }

    fn drain_host_output(
        outputs: &mut [OutPort],
        error: &mut Option<WcolError>,
        host: &mut WcolSession,
    ) -> bool {
        if host.hash_partition_count() > 0 {
            let parts = host.hash_partition_count() as usize;
            for part in 0..parts {
                // `open_graph` checked the partition count against the output ports.
                let Some(out) = outputs.get_mut(part) else {
                    *error = Some(WcolError::from("hash partition has no output port"));
                    return false;
                };
                if out.buf.data.is_empty() {
                    return false;
                }
                if out.full || !host.hash_opened(part) {
                    continue;
                }
                let cap = out.buf.data.len();
                let n = match host.hash_write_partition(part, &mut out.buf.data[..cap]) {
                    Ok(n) => n,
                    Err(e) => {
                        *error = Some(WcolError::msg(e.to_string()));
                        return false;
                    }
                };
                if n == 0 {
                    continue;
                }
                if n > cap {
                    *error = Some(WcolError::from(
                        "hash partition write exceeded slot capacity",
                    ));
                    return false;
                }
                out.buf.len = n;
                out.len = n;
                out.full = true;
                return true;
            }
            return false;
        }
        if outputs.is_empty() || outputs[0].full || outputs[0].buf.data.is_empty() {
            return false;
        }
        let pending = host.output();
        if pending.is_empty() {
            return false;
        }
        // The graph ring can hold a full scratch buffer. Publish one slot and
        // leave the rest until the host drains this one.
        let n = pending.len().min(outputs[0].buf.data.len());
        if n == 0 {
            return false;
        }
        if !Self::publish_output(outputs, 0, &pending[..n], error) {
            return false;
        }
        if let Err(err) = host.consume_output(n) {
            *error = Some(err);
            return false;
        }
        true
    }

    pub fn read_at(&self) -> (u32, u16, u64, u32) {
        match &self.driver {
            Driver::Graph { read_at, .. } => *read_at,
            Driver::Echo { .. } => (0, 0, 0, 0),
        }
    }

    pub fn complete_read_at(&mut self, ticket: u32, bytes: Vec<u8>) -> WcolResult<()> {
        // Completion is by ticket and leaves the exposed slot alone: it may already
        // hold a newer request, which the host claims with `take_read_at`.
        let Driver::Graph { host, .. } = &mut self.driver else {
            return Err(WcolError::from("read_at on echo session"));
        };
        if bytes.is_empty() {
            return Err(WcolError::from("read completion missing bytes"));
        }
        if let Err(err) = host.complete_read_bytes(ticket, bytes) {
            self.error = Some(WcolError::msg(err.to_string()));
            return Err(err);
        }
        Ok(())
    }

    /// The host has taken the exposed ReadAt; clear the slot so stepping can expose the
    /// next one while earlier reads are still in flight (bounded by the plan's fetch depth).
    pub fn take_read_at(&mut self) {
        if let Driver::Graph { read_at, .. } = &mut self.driver {
            *read_at = (0, 0, 0, 0);
        }
    }

    fn graph_host_mut(&mut self) -> Option<&mut WcolSession> {
        match &mut self.driver {
            Driver::Graph { host, .. } => Some(host),
            Driver::Echo { .. } => None,
        }
    }

    fn graph_host(&self) -> Option<&WcolSession> {
        match &self.driver {
            Driver::Graph { host, .. } => Some(host),
            Driver::Echo { .. } => None,
        }
    }

    pub fn spill_op(&self) -> u8 {
        self.graph_host().map(WcolSession::spill_op).unwrap_or(0)
    }

    pub fn spill_append_bytes(&self) -> &[u8] {
        static EMPTY: [u8; 0] = [];
        self.graph_host()
            .map(WcolSession::spill_append_bytes)
            .unwrap_or(&EMPTY)
    }

    pub fn spill_run_upload_body(&self) -> &[u8] {
        static EMPTY: [u8; 0] = [];
        self.graph_host()
            .map(WcolSession::spill_run_upload_body)
            .unwrap_or(&EMPTY)
    }

    pub fn spill_run_read_slot(&mut self, len: usize) -> WcolResult<*mut u8> {
        self.graph_host_mut()
            .ok_or_else(|| WcolError::from("spill on echo session"))?
            .spill_run_read_slot(len)
    }

    pub fn complete_spill(&mut self, ok: bool) -> WcolResult<()> {
        let Some(host) = self.graph_host_mut() else {
            return Err(WcolError::from("spill on echo session"));
        };
        host.complete_spill(ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::stream_program::encode_wcs_partial_reduce_program;

    #[test]
    fn output_slot_pool_matches_logical_ports() {
        let one = StreamPoolSession::open_echo(1, 1, 64, 128).unwrap();
        assert_eq!(one.outputs.len(), 1);
        assert_eq!(one.outputs[0].buf.data.len(), 128);
        let many = StreamPoolSession::open_echo(1, 128, 64, 128).unwrap();
        assert_eq!(many.outputs.len(), 128);
        assert!(many.outputs.iter().all(|port| port.buf.data.len() == 128));
        assert!(StreamPoolSession::open_echo(1, 129, 64, 128).is_err());
        let none = StreamPoolSession::open_echo(1, 0, 64, 128).unwrap();
        assert!(none.outputs.is_empty());
    }

    #[test]
    fn partial_reduce_absent_port_notifies_host_once() {
        let program = encode_wcs_partial_reduce_program(2);
        let mut pool =
            StreamPoolSession::open_graph(&program, 256 * 1024, 2, 1, 64 * 1024, 128 * 1024, 1)
                .unwrap();
        pool.input_absent(0).unwrap();
        for _ in 0..32 {
            let status = pool.step(8);
            assert_eq!(status & STEP_FAILED, 0, "pool error: {:?}", pool.error());
        }
        assert!(pool.inputs[0].absent_notified);
    }

    #[test]
    fn echo_slow_output_does_not_block_other_ports() {
        let mut s = StreamPoolSession::open_echo(2, 2, 1024, 1024).unwrap();
        s.inputs[0].buf.data[0] = 1;
        s.input_commit(0, 1).unwrap();
        s.inputs[1].buf.data[0] = 2;
        s.input_commit(1, 1).unwrap();
        assert_eq!(s.step(10) & STEP_PROGRESS, STEP_PROGRESS);
        assert!(s.output_len(0) > 0);
        assert!(s.output_len(1) > 0);
        assert_eq!(s.step(10) & STEP_PROGRESS, 0);
        s.output_release(1).unwrap();
        s.inputs[1].buf.data[0] = 3;
        s.input_commit(1, 1).unwrap();
        assert_ne!(s.step(10) & STEP_PROGRESS, 0);
        assert_eq!(s.output_len(1), 1);
        assert!(s.outputs[0].full);
    }
}
