# I-06 — Formats contain no query logic

**Status: pass** (2026-10-28)

## Rule

**`formats/`** materializes columns and file bytes. **SQL semantics** (filters, aggregates, partial merge, group keys) live in **`ops/query/`**.

| Area | Module | Role |
|------|--------|------|
| Parquet | [`formats/parquet/`](../../rust/wcol/src/formats/parquet/) | Scan, page decode, projection → `WcolBatch` |
| `.wcol` file | [`formats/wcol/decode/`](../../rust/wcol/src/formats/wcol/decode/) | Chunk parse, column materialization, FFI I/O |
| Query kernels | [`ops/query/wcol/`](../../rust/wcol/src/ops/query/wcol/) | Masks, agg, group-by, HLL, plan helpers |
| Chunk oracle | [`ops/query/parquet_chunk.rs`](../../rust/wcol/src/ops/query/parquet_chunk.rs) | Reference agg on decoded cells (tests / legacy oracle) |
| Production graph | [`exec/`](../../rust/wcol/src/exec/) + [`ops/*Op`](../../rust/wcol/src/ops/) | Operators call `ops/query` after decode |

`.wcol` decode **orchestration** (`runtime.rs`, `ffi/exec.rs`) may call into `ops::query::wcol` but does not duplicate kernel implementations.

## Removed debt

- Deleted `formats/wcol/decode/query/*` re-export shims (2026-10-28). All call sites import `crate::ops::query::wcol::*` directly.

## Allowed in formats (not “query logic”)

- Plan **types** and column **layout** in `formats/wcol/decode/types.rs`
- `runtime.rs` helpers: validity masks, dict lookup, agg **key encoding** for FFI wire layout
- Parquet `aggregate_column` in `formats/parquet/mod.rs` — thin wrapper over `ops::query::aggregate_chunk` for file-level oracle APIs

## Evidence

- Rust test `formats_no_query_forwarder_tree` in [`formats_no_query.rs`](../../rust/wcol/tests/runtime/formats_no_query.rs)
- Grep: no `formats/wcol/decode/query` module in tree
