# I-04 — Operators independent of placement

**Status: pass (colocated query class)** (2026-10-28)

In DoD, **I-04** means query operators do not depend on whether batches arrive over typed memory, fused host Parquet, or WCS1 row frames. (This is separate from **STREAMING_EXECUTION.md** “I-04”, which refers to per-row allocation / bounded scratch.)

## Shared query graph

Filter → Aggregate → Reduce → Finalize are always [`Operator`](../../rust/wcol/src/exec/operator.rs) nodes on [`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs), opened from [`ColocatedGraphPlan`](../../rust/wcol/src/exec/colocated_plan.rs).

Placement only changes **ingress**:

| [`QueryIngressPlacement`](../../rust/wcol/src/plan/placement.rs) | V2 opcode | How rows enter |
|----------------------------------|-----------|----------------|
| `FusedHostParquet` | 10 | Host Parquet bytes + ReadAt |
| `WcsRowBatches` | 3 | WCS `DATA_BATCH` projections |
| `TypedMemory` | — | Direct `inject_row_batch` (tests / colocated decode) |

[`GraphHost`](../../rust/wcol/src/exec/graph_host.rs) is the sole production driver; opcodes **3** and **10** differ only in ingress, not query logic.

## Evidence

- Rust test [`placement_independence.rs`](../../rust/wcol/tests/runtime/placement_independence.rs): typed inject vs opcode-3 `Pipeline` on the same batches → equivalent partials.
- Lib test `wcs_projection_injected_graph_matches_direct_ingest` in [`graph_scheduler.rs`](../../rust/wcol/src/exec/graph_scheduler.rs).

## Out of scope (future)

- Planner emitting placement on every edge (not only query ingress).
- Automatic op **2→3** chain equivalence test (decode DO + query DO).
- I-12 retry/duplicate semantics across remote edges.
