# I-02 — One graph executor

**Status: pass (colocated + Wasm v2)** (2026-10-28)

## Canonical executor

[`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs) is the only in-process query graph driver:

- Optional source [`Operator`](../../rust/wcol/src/exec/operator.rs) (Parquet ingest) or external row inject
- Stream operators (Filter → Aggregate) per batch
- Tail operators (Reduce, then Finalize on `finish()`) on EOF

## Physical plan → graph

[`ColocatedGraphPlan`](../../rust/wcol/src/exec/colocated_plan.rs) describes source + SQL + catalog. Opening the plan builds operators; the scheduler does **not** branch on SQL shape beyond what the plan encodes.

| Source variant | Use |
|----------------|-----|
| `ParquetIngest` | Opcode 10 / local Parquet ReadAt |
| `MetadataCount` | `COUNT(*)` from row-group footers |
| `ExternalRowStream` | WCS row batches (`WcsQueryIngressOp`, decode harness) |

Entry points:

- `ColocatedGraphPlan::ingest_query(...).open()` — ingest + SQL
- `ColocatedGraphPlan::row_stream(...).open()` — inject-only graphs
- `ingest_query_uses_fused_execution` — plan metadata probe (no graph open)

## Host surface

[`GraphHost`](../../rust/wcol/src/exec/graph_host.rs) is the **only** `wcs_open` executor ([`WcolSession`](../../rust/wcol/src/exec/wcol_session.rs) type alias). It owns host I/O rings and drives [`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs):

[`Pipeline`](../../rust/wcol/src/stream/core.rs) is **test-only** byte transport (copy / WCS guard) via [`stream::core::open_plan`](../../rust/wcol/src/stream/core.rs), not production query execution.

All SQL on v2 plans runs inside `GraphHost` → `LinearColocatedGraph`:

| Opcode | Wire mode | Graph plan |
|--------|-----------|------------|
| 10 `OP_COLOCATED_PARQUET_QUERY` | Host Parquet bytes + ReadAt | `ColocatedGraphPlan::ingest_query` |
| 3 `OP_QUERY_WCS` | WCS row-batch ingress | `ColocatedGraphPlan::row_stream` |

`ColocatedParquetQueryOp` / `WcsQueryIngressOp` are type aliases for [`GraphHost`](../../rust/wcol/src/exec/graph_host.rs).

## Back-compat aliases

- `ColocatedParquetQuery` → `LinearColocatedGraph` (deprecated)

## Out of scope (future)

- Arbitrary operator DAG physical plan (not only colocated SQL template).
- `ParquetIngestOp` (opcode 2) still a separate decode wire op when not fused into opcode 10.
