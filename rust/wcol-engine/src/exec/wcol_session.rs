//! Production session: [`GraphHost`] only. The stream pool opens it with [`open_wcol_plan`].

use crate::error::{WcolError, WcolResult};
use crate::exec::graph_host::GraphHost;
use crate::program::stream_program::{
    PlanCursor, OP_COORDINATOR_FRAGMENT, OP_FINALIZE_FRAGMENT, OP_HASH_REDUCER, OP_NODE_FRAGMENT,
    OP_PAGE_COMPACT, OP_SHARD_EXPORT, OP_WCS_PARTIAL_REDUCE, PLAN_V2,
};

/// Graph executor opened from a v2 program. The stream-pool FFI holds this type.
pub type WcolSession = GraphHost;

pub fn open_wcol_plan(program: &[u8], capacity: usize) -> WcolResult<GraphHost> {
    open_wcol_plan_shared(program, capacity, 1)
}

/// [`open_wcol_plan`] for a task that shares its isolate's memory with `budget_share - 1`
/// other tasks: a hash reducer flushes its groups at that fraction of the planned budget.
pub fn open_wcol_plan_shared(
    program: &[u8],
    capacity: usize,
    budget_share: u32,
) -> WcolResult<GraphHost> {
    if program.first() != Some(&PLAN_V2) {
        return Err(WcolError::from("open_wcol_plan requires a PLAN_V2 program"));
    }
    let body = &program[1..];
    let mut cur = PlanCursor::new(body);
    let count = cur.u32()? as usize;
    if count != 1 {
        return Err(WcolError::from(
            "open_wcol_plan supports exactly one v2 operator",
        ));
    }
    match cur.u8()? {
        OP_NODE_FRAGMENT => GraphHost::open_node_fragment(&mut cur, capacity),
        OP_COORDINATOR_FRAGMENT => GraphHost::open_coordinator_fragment(&mut cur, capacity),
        OP_PAGE_COMPACT => GraphHost::open_page_compact(&mut cur, capacity),
        OP_WCS_PARTIAL_REDUCE => GraphHost::open_wcs_partial_reduce(&mut cur, capacity),
        OP_FINALIZE_FRAGMENT => GraphHost::open_finalize_fragment(&mut cur, capacity),
        OP_SHARD_EXPORT => GraphHost::open_shard_export(&mut cur, capacity),
        OP_HASH_REDUCER => GraphHost::open_hash_reducer(&mut cur, capacity, budget_share),
        _ => Err(WcolError::from("unsupported v2 opcode")),
    }
}
