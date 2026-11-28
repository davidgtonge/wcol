# `wcol-engine` source layout

The columnar execution engine. SQL parsing and planning live in `wcol-plan`; this crate runs owned IR, the wire programs the planner emits (node and coordinator fragments, PWCL partial state, WCS1 byte streams) and host I/O. The public modules are declared in [`lib.rs`](lib.rs).

## How the pieces connect

```text
wcol-plan (outside this crate)
    │  physical plan + fragments
    ▼
program/          fragment wire formats, ingest groups; to_ir.rs → ir/
    ▼
exec/             GraphHost, LinearColocatedGraph, fetch stage, stream pool, Wasm FFI
    ▼
ops/              filter, aggregate, reduce, exchange, sort, top-k, …
    │  batch kernels in ops/query/batch/
    ▼
stream/           OwnedWcolBatch, partial state (PWCL), WCS1 framing
    ▲
formats/parquet/  Parquet column chunks → WCOL batches, and Parquet sinks (no query logic)

spill/            sorted runs of partial groups, in memory or in R2
```

In production the TypeScript host ([`src/runtime/host/stream-pool.ts`](../../../src/runtime/host/stream-pool.ts)) calls the `wcs_sp_*` exports in [`exec/runtime/stream_pool_ffi.rs`](exec/runtime/stream_pool_ffi.rs). They drive a `StreamPoolSession` ([`exec/stream_pool.rs`](exec/stream_pool.rs), up to 128 input and 128 output ports), which polls `GraphHost` ([`exec/graph_host/`](exec/graph_host/mod.rs)). `GraphHost` runs one task's `LinearColocatedGraph` of operators and answers each poll with an `Action` ([`stream/core.rs`](stream/core.rs)): need input, have output, read at, spill, finished or failed.

## Where to look

| For | Look in |
|---|---|
| Errors and the FFI last-error slot | [`error.rs`](error.rs) |
| Engine IR (columns, predicates, aggregates, sort keys) | [`ir/mod.rs`](ir/mod.rs) |
| Node and coordinator fragment wire formats | [`program/fragment.rs`](program/fragment.rs), [`program/coordinator.rs`](program/coordinator.rs), [`program/codec.rs`](program/codec.rs) |
| Byte-stream operator programs, ingest groups | [`program/stream_program.rs`](program/stream_program.rs), [`program/ingest.rs`](program/ingest.rs) |
| Row ids for late reads | [`program/row_id.rs`](program/row_id.rs) |
| Fragment → IR | [`program/to_ir.rs`](program/to_ir.rs) |
| The task driver and its host I/O | [`exec/graph_host/`](exec/graph_host/mod.rs) |
| Fragment → operator graph | [`exec/colocated_plan.rs`](exec/colocated_plan.rs), [`exec/graph_scheduler.rs`](exec/graph_scheduler.rs) |
| Range reads a plan asks for | [`exec/fetch_stage/`](exec/fetch_stage/mod.rs) |
| Per-edge WCS decoding | [`exec/wcs_port.rs`](exec/wcs_port.rs) |
| Stream-pool ports and its Wasm exports | [`exec/stream_pool.rs`](exec/stream_pool.rs), [`exec/runtime/stream_pool_ffi.rs`](exec/runtime/stream_pool_ffi.rs) |
| Other Wasm exports | [`exec/runtime/ffi.rs`](exec/runtime/ffi.rs) |
| Parquet footer, decode and scans | [`formats/parquet/schema.rs`](formats/parquet/schema.rs), [`formats/parquet/batch_decode.rs`](formats/parquet/batch_decode.rs), [`formats/parquet/numeric_scan.rs`](formats/parquet/numeric_scan.rs) |
| Parquet output: encoder, `partitionBy`, page-copy compaction | [`formats/parquet/encode.rs`](formats/parquet/encode.rs), [`formats/parquet/partitioned.rs`](formats/parquet/partitioned.rs), [`formats/parquet/page_compact.rs`](formats/parquet/page_compact.rs) |
| Operators | [`ops/`](ops/mod.rs): [`filter.rs`](ops/filter.rs), [`project.rs`](ops/project.rs), [`limit.rs`](ops/limit.rs), [`sort/`](ops/sort/mod.rs), [`topk.rs`](ops/topk.rs), [`aggregate/`](ops/aggregate/mod.rs), [`reduce.rs`](ops/reduce.rs), [`finalize.rs`](ops/finalize.rs) |
| Hash and range exchange | [`ops/exchange/`](ops/exchange/mod.rs) |
| Group tables, batch kernels, group spill | [`ops/query/batch/`](ops/query/batch/mod.rs) |
| Partial-state algebra, HLL | [`ops/query/mod.rs`](ops/query/mod.rs), [`ops/query/hll.rs`](ops/query/hll.rs) |
| Iterative kernels (k-means) | [`ops/iterative.rs`](ops/iterative.rs), [`ops/iterative/`](ops/iterative/resident.rs) |
| Batches and selection masks | [`stream/batch/`](stream/batch/mod.rs) |
| WCS1 frames and PWCL partial state | [`stream/wcs.rs`](stream/wcs.rs), [`stream/partial_wcs/`](stream/partial_wcs/mod.rs), [`stream/partials.rs`](stream/partials.rs) |
| Native poll loop for tests | [`stream/native_poll.rs`](stream/native_poll.rs) |
| Spill runs, stores and merges | [`spill/`](spill/mod.rs), [`spill/r2/`](spill/r2/mod.rs) |
