//! Immutable execution graph: tasks, slot placement, and edge transport derived from slots.
//!
//! The host runs this graph; it does not re-derive placement or HTTP vs RPC.

use crate::plan::output_plan::{OutputFinalizer, OutputPlan};
use crate::plan::physical_plan::{OpKind, PhysicalPlannerConfig, SortKey, WcolPhysicalPlan};
use crate::plan::query_dispatch::{NodeProgram, QueryDispatch};
use crate::program::stream_program::{
    OP_COORDINATOR_FRAGMENT, OP_FINALIZE_FRAGMENT, OP_HASH_REDUCER, PLAN_V2,
};
use wcol_engine::exec::stream_pool::{MAX_STREAM_INPUT_PORTS, MAX_STREAM_OUTPUT_PORTS};

/// One node isolate may attach at most this many ingress/egress streams (`createStreamPool`).
fn check_stream_pool_layout(inputs: u32, outputs: u32, role: &str) -> Result<(), String> {
    if inputs as usize > MAX_STREAM_INPUT_PORTS {
        return Err(format!(
            "{role} needs {inputs} input streams; host limit is {MAX_STREAM_INPUT_PORTS}"
        ));
    }
    if outputs as usize > MAX_STREAM_OUTPUT_PORTS {
        return Err(format!(
            "{role} needs {outputs} output streams; host limit is {MAX_STREAM_OUTPUT_PORTS}"
        ));
    }
    Ok(())
}

/// One Wasm isolate / DO execution slot (not the same as a fused program).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u32);

/// Reserved slot for coordinator-hosted tasks (final merge, row coordinator).
pub const COORDINATOR_SLOT: SlotId = SlotId(u32::MAX);

/// Worker scan slots use `NodeId.0` today (one DO per planner node slot).
pub fn worker_slot(node: crate::plan::physical_plan::NodeId) -> SlotId {
    SlotId(node.0)
}

/// Where a graph's root (the finalizer) runs. A one-node graph stays colocated in the
/// coordinator; otherwise the root gets its own node slot after every scan slot, so merging
/// never happens in the coordinator and the scan nodes keep their memory for scanning.
fn root_slot(nodes: &[NodeProgram]) -> SlotId {
    if nodes.len() == 1 {
        return COORDINATOR_SLOT;
    }
    let last = nodes.iter().map(|node| node.node.0).max().unwrap_or(0);
    SlotId(last + 1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeTransport {
    /// Same `SlotId`: bounded in-process pipe between two GraphHost sessions.
    InProcess,
    /// Different slots; Workers RPC native WCS stream (default).
    DoRpcWcs,
    /// Different slots; HTTP `fetch` egress (planner config only).
    DoFetchWcs,
    /// One worker fragment shares the coordinator isolate. No node Durable Object.
    Colocated,
}

impl EdgeTransport {
    pub fn wire_byte(self) -> u8 {
        match self {
            Self::InProcess => 0,
            Self::DoRpcWcs => 1,
            Self::DoFetchWcs => 2,
            Self::Colocated => 3,
        }
    }

    pub fn from_wire(byte: u8) -> Result<Self, String> {
        match byte {
            0 => Ok(Self::InProcess),
            1 => Ok(Self::DoRpcWcs),
            2 => Ok(Self::DoFetchWcs),
            3 => Ok(Self::Colocated),
            _ => Err(format!("unknown edge transport {byte}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionTask {
    pub id: u32,
    pub slot: SlotId,
    pub program: Vec<u8>,
    pub request_count: u32,
    pub fetched_bytes: u64,
    /// A scan that takes units of work at run time: its queue on the coordinator.
    pub work: Option<TaskWork>,
}

/// One scan's run-time work: the units planned for it, and the group (the object's source
/// index) whose scans may take each other's units.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskWork {
    pub group: u32,
    pub own_units: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionEdge {
    pub from: u32,
    pub to: u32,
    pub transport: EdgeTransport,
    /// `u32::MAX` is the whole producer stream. Any other value is one hash partition.
    pub partition: u32,
}

/// The producer sends its whole stream. Hash edges name one partition.
pub const WHOLE_STREAM: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootOutputKind {
    /// Root task emits PWCL on its WCS egress (aggregate path).
    Partial,
    /// Root task emits typed row frames.
    Rows,
    /// Root task emits a Parquet file (ordered export). Unordered export has no edges:
    /// every task writes its own file.
    Parquet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionPlan {
    /// Footer-only answer; no tasks.
    MetadataPartial(Vec<u8>),
    Graph {
        tasks: Vec<ExecutionTask>,
        edges: Vec<ExecutionEdge>,
        root_task_id: u32,
        root_output: RootOutputKind,
        /// Present for parquet/json jobs planned through `plan_job`.
        output: Option<OutputPlan>,
        finalizer: OutputFinalizer,
    },
}

fn execution_graph(
    tasks: Vec<ExecutionTask>,
    edges: Vec<ExecutionEdge>,
    root_task_id: u32,
    root_output: RootOutputKind,
) -> ExecutionPlan {
    ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        root_output,
        output: None,
        finalizer: OutputFinalizer::None,
    }
}

/// Attach output layout and optional concat finaliser to a planned graph.
pub fn attach_output_plan(
    exec: ExecutionPlan,
    output: OutputPlan,
    finalizer: OutputFinalizer,
) -> Result<ExecutionPlan, String> {
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        root_output,
        ..
    } = exec
    else {
        return Err("metadata partial has no output plan".into());
    };
    Ok(ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        root_output,
        output: Some(output),
        finalizer,
    })
}

pub fn edge_transport(from: SlotId, to: SlotId, config: &PhysicalPlannerConfig) -> EdgeTransport {
    if from == to {
        return EdgeTransport::InProcess;
    }
    if config.inter_do_fetch_wcs {
        EdgeTransport::DoFetchWcs
    } else {
        EdgeTransport::DoRpcWcs
    }
}

/// Build an execution graph from an already-planned query dispatch.
pub fn execution_plan_from_dispatch(
    dispatch: &QueryDispatch,
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
) -> Result<ExecutionPlan, String> {
    execution_plan_for(dispatch, plan, config, None)
}

/// [`execution_plan_from_dispatch`] with explicit finalizer steps for an aggregate (an export
/// adds its projection and Parquet sink; see [`crate::plan::result::export_steps`]).
pub fn execution_plan_for(
    dispatch: &QueryDispatch,
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
    finalize: Option<Vec<crate::program::CoordinatorStep>>,
) -> Result<ExecutionPlan, String> {
    match dispatch {
        QueryDispatch::Partial(partial) => Ok(ExecutionPlan::MetadataPartial(partial.clone())),
        QueryDispatch::Nodes(programs) => {
            if let Some((keys, partitions)) = plan_range_exchange(plan) {
                let steps = finalize.ok_or("range-sort parquet export needs export steps")?;
                build_range_sort_graph(programs, plan, config, steps, partitions, &keys)
            } else if plan_hash(plan).is_some() {
                build_hash_graph(programs, plan, config, finalize)
            } else {
                build_aggregate_graph(programs, plan, config, finalize)
            }
        }
        QueryDispatch::Rows { nodes, coordinator } => {
            build_row_graph(nodes, coordinator, config, plan)
        }
        QueryDispatch::Parquet(nodes) => build_parquet_parts(nodes),
    }
}

/// Range shuffle on a row scan (global sort parquet export).
pub fn plan_range_exchange(plan: &WcolPhysicalPlan) -> Option<(Vec<SortKey>, u32)> {
    plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::RangeExchange {
            keys,
            partitions,
            splits: _,
        } => Some((keys.clone(), *partitions)),
        _ => None,
    })
}

fn program_producer_count(program: &[u8]) -> Option<u32> {
    if program.first().copied()? != PLAN_V2 {
        return None;
    }
    let mut at = 5usize;
    let opcode = *program.get(at)?;
    at += 1;
    match opcode {
        OP_FINALIZE_FRAGMENT | OP_COORDINATOR_FRAGMENT => {
            let bytes = program.get(at..at + 4)?;
            Some(u32::from_le_bytes(bytes.try_into().ok()?))
        }
        _ => None,
    }
}

fn program_has_parquet_step(program: &[u8]) -> bool {
    let steps = match decode_finalize_steps(program) {
        Ok(steps) => steps,
        Err(_) => return false,
    };
    steps
        .iter()
        .any(|step| matches!(step, crate::program::CoordinatorStep::Parquet { .. }))
}

fn decode_finalize_steps(program: &[u8]) -> Result<Vec<crate::program::CoordinatorStep>, String> {
    if program.first().copied() != Some(PLAN_V2) {
        return Err("not plan v2".into());
    }
    let opcode = program.get(5).copied().ok_or("missing opcode")?;
    if opcode != OP_FINALIZE_FRAGMENT && opcode != OP_COORDINATOR_FRAGMENT {
        return Err("not coordinator/finalize fragment".into());
    }
    let at = 14usize;
    let body_len = u32::from_le_bytes(
        program
            .get(10..14)
            .ok_or("missing body len")?
            .try_into()
            .unwrap(),
    ) as usize;
    let body = program.get(at..at + body_len).ok_or("truncated body")?;
    crate::program::decode_coordinator_steps(body).map_err(|e| format!("{e:?}"))
}

/// Tasks that each write one Parquet attempt object (hash/range shard export).
pub fn parquet_export_shard_task_ids(tasks: &[ExecutionTask], edges: &[ExecutionEdge]) -> Vec<u32> {
    let mut ids: Vec<u32> = tasks
        .iter()
        .filter(|task| {
            let Some(producers) = program_producer_count(&task.program) else {
                return false;
            };
            if producers != 1 {
                return false;
            }
            if !program_has_parquet_step(&task.program) {
                return false;
            }
            let opcode = task.program.get(5).copied().unwrap_or(0);
            if opcode == OP_HASH_REDUCER {
                return false;
            }
            edges.iter().any(|edge| edge.to == task.id)
        })
        .map(|task| task.id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn hash_parquet_shard_export(
    steps: &[crate::program::CoordinatorStep],
    root_output: RootOutputKind,
) -> bool {
    root_output == RootOutputKind::Parquet
        && !steps.iter().any(|step| {
            matches!(
                step,
                crate::program::CoordinatorStep::Sort { .. }
                    | crate::program::CoordinatorStep::Limit { .. }
            )
        })
}

mod builders;
#[cfg(test)]
mod tests;
mod wire;

pub use builders::*;
pub use wire::*;
