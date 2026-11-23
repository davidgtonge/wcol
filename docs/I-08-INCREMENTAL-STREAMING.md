# I-08 — Incremental streaming (DoD)

**Status: pass (colocated Parquet path)** (2026-10-28)

DoD **I-08** means operators and ingest **advance in batches** and accept **partial host input** without requiring the full column chunk up front. (This is separate from **STREAMING_EXECUTION.md** “I-08 — Partial progress is legal”, which is the same idea on the byte `WireOperator` poll contract.)

## Production path (opcode 10)

| Stage | Behavior |
|-------|----------|
| [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs) | Ticketed [`host_read_request`](../../rust/wcol/src/formats/parquet/batch_decode.rs) → `complete_read(ticket)` (legacy alias: `push_input` / `ingest_input`) |
| [`NumericScan`](../../rust/wcol/src/formats/parquet/numeric_scan.rs) | `push_compressed` grows buffer as bytes arrive; `poll` → `BatchReady` in `max_batch_rows` units |
| [`ParquetSourceOp`](../../rust/wcol/src/ops/parquet_source.rs) | One `RowBatch` per filled batch; `NeedHostRead` between slices |
| [`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs) | `drive_ingest` / `pump` routes each batch through Filter → Aggregate before next ReadAt |

## Evidence

- [`incremental_streaming.rs`](../../rust/wcol/tests/runtime/incremental_streaming.rs) — multiple pump progress events; `Running::passed_rows` increases mid-query.
- [`numeric_scan_batches.rs`](../../rust/wcol/tests/parquet/numeric_scan_batches.rs) — `incremental_push_before_full_chunk`.
- [`drive_ingest`](../../rust/wcol/src/exec/graph_scheduler.rs) drains decoded batches before yielding ReadAt (I-11 parity).

## Not I-08 (I-09 bounded memory)

- Whole-column compressed retention is removed: [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs) emits batches before each column chunk is fully fetched; per-column windows share one execution-group budget.
- Page-scratch verification, skip-replay / `PageCursor`, and plateau proof remain [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md) / **I-09**.
