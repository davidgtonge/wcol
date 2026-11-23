# Executor consolidation (post review)

**Production path (2026-10-28):**

```text
Wasm wcs_open → GraphHost (WcolSession type alias)
   ↓
LinearColocatedGraph  →  Filter / Aggregate / Reduce / Finalize
   ↑
ParquetSourceOp (ReadAt)  or  WcsIngress (opcode 3)  or  ParquetWcsProjection (opcode 2)
   ↓
EmitPartial → WCS FRAME_PARTIAL (PWCL) on output ring only
```

There is **one** production executor: [`GraphHost`](../rust/wcol/src/exec/graph_host.rs) driving [`LinearColocatedGraph`](../rust/wcol/src/exec/graph_scheduler.rs).

| Plan | Opens via |
|------|-----------|
| Opcode **10** | `GraphHost::open_parquet_ingest` |
| Opcode **3** | `GraphHost::open_wcs_query` |
| Opcode **2** | `GraphHost::open_parquet_wcs_projection` |

[`open_wcol_plan`](../rust/wcol/src/exec/wcol_session.rs) accepts **PLAN_V2 with exactly one operator** (2, 3, or 10). It does not open the byte [`Pipeline`](../rust/wcol/src/stream/core.rs).

The byte [`Pipeline`](../rust/wcol/src/stream/core.rs) remains for **unit tests** (`stream/core` tests, `dod_acceptance` copy/WCS guard) via [`stream::core::open_plan`](../rust/wcol/src/stream/core.rs)—not wired through `wcs_open`.

## Removed (consolidation milestone)

- `WcolSession::Transport(Pipeline)` vs `Graph(GraphHost)` enum
- `open_v2_pipeline` multi-op byte chain on `wcs_open`
- `exec/scheduler.rs` duplicate helpers (`pump_row_batch`, `finish_query_partials`)
- `exec/graph.rs` re-export shim

## Still open (perf / I-09)

- `NumericScan` skip-replay / `PageCursor`
- Overlapping `ReadAt` in `ParquetDecode`
- Colocated memory ring for compressed chunks
