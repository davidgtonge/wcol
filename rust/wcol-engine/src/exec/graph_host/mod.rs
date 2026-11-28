//! Production Wasm/native driver: [`LinearColocatedGraph`] + host I/O rings.
//!
//! Node and coordinator fragments open here. WCS1 framing applies only to the output edge.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::error::{WcolError, WcolResult};
use crate::exec::graph_scheduler::{ByteSinkDrive, GraphPump, LinearColocatedGraph};
use crate::exec::operators::{
    owned_batch_from_projection_body, owned_batch_from_projection_body_with_dicts,
    progress_after_io, CompleteFrame, EmitPartial, RowBatchWcsEgress,
};
use crate::exec::wcs_port::{self, WcsPort};
use crate::formats::parquet::ingest_replay_reset;
use crate::formats::parquet::stream_wcs::{decode_dictionary_chunk, merge_dictionary_chunk};
use crate::ops::{AggregateOp, BatchPartitionPlan, HashPartitionPlan, ReduceOp};
use crate::program::stream_program::PlanCursor;
use crate::spill::{HostR2SortedRunStore, HostSpillBridge, SpillProgress};
use crate::stream::core::{Action, EdgeBuffer};
use crate::stream::partials::partial_state_payloads;
use crate::stream::wcs::{FRAME_DICTIONARY, FRAME_END, FRAME_PARTIAL, MAX_WCS_PAYLOAD};
use crate::stream::DictionaryValues;

const PUMP_BUDGET: usize = 256;

/// Complete WCS frames waiting for the graph, tagged with their input port.
type PendingFrames = VecDeque<(u32, CompleteFrame)>;

enum GraphHostIngress {
    HostParquet,
    WcsRowBatches {
        ports: Vec<WcsPort>,
        pending: PendingFrames,
        input_ended: bool,
        next_fresh_scope: u32,
        dicts: HashMap<(u32, u32), DictionaryValues>,
    },
    /// Worker PWCL partial streams merged with [`ReduceOp`].
    WcsPartialReduce {
        ports: Vec<WcsPort>,
        pending: PendingFrames,
        input_ended: bool,
    },
}

fn wcs_row_batches(producer_count: u32) -> GraphHostIngress {
    GraphHostIngress::WcsRowBatches {
        ports: wcs_port::empty_ports(producer_count),
        pending: VecDeque::new(),
        input_ended: false,
        next_fresh_scope: 0x8000_0000,
        dicts: HashMap::new(),
    }
}

fn wcs_partial_reduce(producer_count: u32) -> GraphHostIngress {
    GraphHostIngress::WcsPartialReduce {
        ports: wcs_port::empty_ports(producer_count),
        pending: VecDeque::new(),
        input_ended: false,
    }
}

enum FinalizePhase {
    Inactive,
    /// Merging every producer's partial before the steps run.
    Pending,
    /// One producer's groups are already final: each frame goes through the steps as it lands.
    Streaming,
    Emitted,
}

enum SpillPhase {
    Idle,
    Resume {
        input: ReducerInput,
        yield_host: bool,
    },
    HoldFrames,
}

/// What a hash reducer consumes from a scan edge: raw rows, or a scan-side combiner's
/// partial state. A spill wait holds it and the same input resumes once.
enum ReducerInput {
    Rows(crate::stream::OwnedWcolBatch),
    Partial(crate::stream::partials::PartialStateWcol),
}

struct SpillDrive {
    phase: SpillPhase,
    /// Chunked spill finish emitted every partial; FRAME_END is queued once the spill store ends.
    chunks_finished: bool,
    /// What becomes of the reducer's folded distinct counts.
    distinct: DistinctFold,
}

/// A reducer's folded `COUNT(DISTINCT)` output.
enum DistinctFold {
    /// Groups may span reducers (the exchange also partitioned on the counted value): send
    /// every count, coalesced (see `push_finish_chunk`); the finalizer sums them.
    Merge(Option<crate::ops::query::PartialSnapshotMerger>),
    /// Every group is complete here and the query ranks by a count under a LIMIT: send only
    /// this reducer's top groups. `None` once sent.
    Rank(Option<crate::ops::query::FoldedTopCounts>),
}

/// Groups of coalesced distinct counts a spilled reducer sends at once.
const DISTINCT_COUNT_CHUNK_GROUPS: usize = 4096;

struct GraphHostCore {
    resident: Option<crate::ops::iterative::resident::ResidentExec>,
    graph: LinearColocatedGraph,
    ingress: GraphHostIngress,
    emit: EmitPartial,
    row_egress: Option<RowBatchWcsEgress>,
    hash_fanout: Option<HashFanout>,
    row_aggregate: Option<AggregateOp>,
    row_end_queued: bool,
    partial_payloads: VecDeque<Vec<u8>>,
    reduce: Option<ReduceOp>,
    /// Finalizer: after the reduce, finalized groups go through `graph` as rows.
    finalize: FinalizePhase,
    spill_bridge: Option<Rc<RefCell<HostSpillBridge>>>,
    spill_drive: SpillDrive,
    /// When true, hash fan-out stages per-partition frames instead of mixing into one ring.
    direct_port_egress: bool,
}

/// Typed graph executor with host ReadAt / WCS row ingress and PWCL output edge.
pub struct GraphHost {
    core: GraphHostCore,
    input: EdgeBuffer,
    output: EdgeBuffer,
    read_at_offset: u64,
    read_at_len: u32,
    read_at_source: u16,
    read_at_ticket: u32,
    ring_capacity: usize,
    spill_op: u8,
    spill_append_buf: Vec<u8>,
    spill_run_upload_mirror: Vec<u8>,
    error: Option<WcolError>,
}

fn has_count_distinct(partial: &crate::stream::partials::PartialStateWcol) -> bool {
    partial
        .schema
        .aggs
        .iter()
        .any(|agg| agg.kind == crate::ops::query::KIND_COUNT_DISTINCT)
}

fn distinct_counts_of(
    partial: &crate::stream::partials::PartialStateWcol,
) -> Result<crate::ops::query::PartialSnapshot, String> {
    crate::stream::partials::partial_snapshot_from_state(partial)
        .and_then(|snap| crate::ops::query::distinct_counts_snapshot(&snap))
}

mod hash;
mod ingress;
mod open;
mod output;
mod ports;
mod resident;
mod rows;
mod spill;
#[cfg(test)]
mod tests;
mod wcs_input;

use hash::*;

impl GraphHost {
    pub fn scratch_bytes(&self) -> usize {
        self.ring_capacity * 2
    }

    pub fn read_at(&self) -> (u32, u16, u64, u32) {
        (
            self.read_at_ticket,
            self.read_at_source,
            self.read_at_offset,
            self.read_at_len,
        )
    }

    /// Complete an outstanding ReadAt; input ring must hold exactly the response bytes.
    pub fn complete_read(&mut self, ticket: u32) -> WcolResult<()> {
        if self.error.is_some() {
            return Err(WcolError::from("graph host failed"));
        }
        let len = self.input.readable().len();
        if len == 0 {
            return Err(WcolError::from("read completion missing input bytes"));
        }
        let bytes = self.input.readable()[..len].to_vec();
        self.complete_read_bytes(ticket, bytes)?;
        self.input.consume(len);
        Ok(())
    }

    /// Complete ReadAt with host-fetched bytes (stream pool bypasses the input ring size cap).
    pub fn complete_read_bytes(&mut self, ticket: u32, bytes: Vec<u8>) -> WcolResult<()> {
        if self.error.is_some() {
            return Err(WcolError::from("graph host failed"));
        }
        if bytes.is_empty() {
            return Err(WcolError::from("read completion missing bytes"));
        }
        match &mut self.core.ingress {
            GraphHostIngress::HostParquet => self
                .core
                .graph
                .complete_ingest_read(ticket, bytes)
                .map_err(|e| WcolError::msg(format!("complete_read failed: {e}")))?,
            GraphHostIngress::WcsRowBatches { .. } | GraphHostIngress::WcsPartialReduce { .. } => {
                return Err(WcolError::from("complete_read on wcs ingress"));
            }
        }
        Ok(())
    }

    pub fn cancel_read(&mut self, ticket: u32) -> WcolResult<()> {
        match &mut self.core.ingress {
            GraphHostIngress::HostParquet => self
                .core
                .graph
                .cancel_ingest_read(ticket)
                .map_err(|e| WcolError::msg(format!("cancel_read failed: {e}")))?,
            GraphHostIngress::WcsRowBatches { .. } | GraphHostIngress::WcsPartialReduce { .. } => {
                return Err(WcolError::from("cancel_read on wcs ingress"));
            }
        }
        Ok(())
    }

    /// Upcoming ReadAt ranges for host prefetch on a node fragment.
    pub fn read_at_hints(&self, max: usize) -> Vec<(u64, u32)> {
        match &self.core.ingress {
            GraphHostIngress::HostParquet => self.core.graph.parquet_read_hints(max),
            GraphHostIngress::WcsRowBatches { .. } | GraphHostIngress::WcsPartialReduce { .. } => {
                Vec::new()
            }
        }
    }
}

impl GraphHost {
    pub fn error(&self) -> Option<&str> {
        self.error.as_ref().map(WcolError::as_str)
    }

    pub fn error_bytes(&self) -> Option<(*const u8, u32)> {
        self.error.as_ref().map(|e| {
            let s = e.as_str();
            (s.as_ptr(), s.len() as u32)
        })
    }

    pub fn enable_direct_port_egress(&mut self) {
        self.core.direct_port_egress = true;
    }

    fn ingress_gauges(&self) -> (u64, u64) {
        match &self.core.ingress {
            GraphHostIngress::WcsRowBatches { ports, pending, .. }
            | GraphHostIngress::WcsPartialReduce { ports, pending, .. } => {
                let queued = wcs_port::pending_queue_bytes(pending) as u64;
                let retained = ports.iter().fold(0u64, |sum, port| {
                    sum.saturating_add(port.retained_bytes() as u64)
                });
                let active = ports.iter().filter(|port| !port.transport_eof).count() as u64;
                (queued.saturating_add(retained), active)
            }
            GraphHostIngress::HostParquet => (0, 0),
        }
    }

    fn hash_has_pending_output(&self) -> bool {
        self.core.hash_fanout.as_ref().is_some_and(|fanout| {
            fanout
                .egress
                .iter()
                .any(RowBatchWcsEgress::has_pending_output)
        })
    }

    fn fail_message(&mut self, message: &str) {
        if self.error.is_none() {
            self.error = Some(WcolError::msg(message.to_string()));
        }
    }
}

impl GraphHost {
    pub fn poll(&mut self) -> Action {
        if self.error.is_some() {
            return Action::Failed;
        }
        if self.core.resident.is_some() {
            return match self.poll_resident_graph() {
                Ok(action) => action,
                Err(error) => {
                    self.error = Some(error);
                    Action::Failed
                }
            };
        }
        if let Err(err) = self.core.reconcile_spill_host_reads() {
            self.error = Some(err);
            return Action::Failed;
        }
        if self.core.direct_port_egress && self.hash_has_pending_output() {
            return Action::HaveOutput;
        }
        if !self.output().is_empty() {
            return Action::HaveOutput;
        }
        if self.core.take_spill_yield() {
            return Action::Yield;
        }
        if self.expose_spill_exec() {
            return Action::SpillExec;
        }
        for _ in 0..PUMP_BUDGET {
            if let Err(err) = self.core.drain_held_ingress() {
                self.error = Some(err);
                return Action::Failed;
            }
            let step = {
                let input = self.input.readable();
                let eof = self.input.is_closed() && input.is_empty();
                let input_len = input.len();
                let out = self.output.writable();
                if out.is_empty() {
                    if self.expose_spill_exec() {
                        return Action::SpillExec;
                    }
                    return Action::Yield;
                }
                let out_len = out.len();
                let finalize = self.core.finalize_mode();
                let step = match &mut self.core.ingress {
                    GraphHostIngress::HostParquet => self.core.poll_host_parquet(input, eof, out),
                    GraphHostIngress::WcsRowBatches { .. } => self.core.poll_wcs_rows(out),
                    GraphHostIngress::WcsPartialReduce { .. } if finalize => {
                        self.core.poll_wcs_finalize(out)
                    }
                    GraphHostIngress::WcsPartialReduce { .. } => {
                        self.core.poll_wcs_partial_reduce(out)
                    }
                };
                let step = match step {
                    Ok(p) => p,
                    Err(err) => {
                        self.error = Some(err);
                        return Action::Failed;
                    }
                };
                if step.consumed > input_len || step.produced > out_len {
                    self.error = Some(WcolError::from("graph host exceeded buffer boundary"));
                    return Action::Failed;
                }
                (step, eof && input.is_empty())
            };
            let (step, input_eof_idle) = step;
            if let Some((ticket, source, offset, len)) = step.read_at() {
                self.read_at_ticket = ticket;
                self.read_at_source = source;
                self.read_at_offset = offset;
                self.read_at_len = len;
                return Action::ReadAt;
            }
            self.input.consume(step.consumed);
            self.output.commit(step.produced);
            if self.core.take_spill_yield() {
                return Action::Yield;
            }
            if !self.output().is_empty() {
                return Action::HaveOutput;
            }
            if self.core.direct_port_egress && self.hash_has_pending_output() {
                return Action::HaveOutput;
            }
            if step.finished {
                return Action::Finished;
            }
            if step.consumed == 0 && step.produced == 0 {
                if self.expose_spill_exec() {
                    return Action::SpillExec;
                }
                // A parked spill-resume batch must not report NeedInput: the host
                // stops advancing, ingress ports stay blocked, and scans deadlock.
                if self.core.spill_resume_pending() {
                    if self.core.spill_yield_pending() {
                        return Action::Yield;
                    }
                    continue;
                }
                if self.core.wcs_awaiting_input() {
                    return Action::NeedInput;
                }
                if matches!(self.core.ingress, GraphHostIngress::HostParquet) {
                    if !self.input.is_closed() && self.input.readable().is_empty() {
                        return Action::NeedInput;
                    }
                    if input_eof_idle {
                        return Action::NeedInput;
                    }
                }
            }
        }
        Action::Yield
    }
}
