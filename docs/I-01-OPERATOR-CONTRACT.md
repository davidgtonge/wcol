# I-01 — One canonical operator interface

**Status: Pass** (2026-10-28)

## Contract

| Piece | Location |
|-------|----------|
| **`Operator`** + **`OperatorIo`** + **`StreamProgress`** | [`rust/wcol/src/exec/operator.rs`](../rust/wcol/src/exec/operator.rs) |
| Query transforms | [`ops/filter.rs`](../rust/wcol/src/ops/filter.rs), `aggregate`, `reduce`, `finalize` — all `impl Operator` |
| Parquet source | [`ops/parquet_source.rs`](../rust/wcol/src/ops/parquet_source.rs) — `impl Operator` |
| Graph scheduler | [`exec/graph_scheduler.rs`](../rust/wcol/src/exec/graph_scheduler.rs) — polls `dyn Operator` only |

## Wire ports (not query operators)

Host/Wasm byte edges use **`WireOperator`** ([`stream/core.rs`](../rust/wcol/src/stream/core.rs)):

- `ColocatedParquetQueryOp` — ReadAt ingest + WPP1 emit; **no** query logic
- `WcsQueryIngressOp` — WCS frame parse + `LinearColocatedGraph::inject_row_batch`
- `ParquetIngestOp`, `CopyBytes`, `WcsGuard` — transport/format only

Adding a query stage = `impl Operator` in `ops/` and register on a graph; **no** byte-query opcodes.

## Removed (violations)

- `FilterWcsOp`, `AggregateWcsOp`, `ReduceWcsOp` (opcodes 7–9)
- `ParquetDecodeWcsOp`, `ColocatedQueryWcsOp` (chain 6→5)
- `QueryStreamOp` (direct `Running` on WCS bytes) → `WcsQueryIngressOp` + typed graph
- Opcode **4** duplicate ingest runner (plans use opcode **10** only)

## Tests

- `exec::graph_scheduler::tests::custom_graph_routes_through_stream_ops`
- `exec::graph_scheduler::tests::wcs_projection_injected_graph_matches_direct_ingest`
- `clickbench_exec_group`, `vertical_slice`, `colocated_ops`, `typed_colocated_graph`
