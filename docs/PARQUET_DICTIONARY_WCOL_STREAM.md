# Parquet dictionary → WCOL streaming (implementation map)

**Architecture:** [STREAMING_PIPELINE.md](./STREAMING_PIPELINE.md) — ParquetDecode is decode-only; operators own filter/aggregate; dict flows Parquet → WCOL → operators → sinks without expansion unless an operator materializes.

**Brief:** Preserve dictionary encoding end-to-end (`Parquet dict page → bounded store → WCOL dict IDs + validity`), without expanding IDs to values except where an operator requires materialization.

## Current state (2026-03)

| Area | Status |
| --- | --- |
| Bounded `DictionaryStore` (int + byte arena, chunk-scoped) | Done in `wcol-parquet/src/dictionary.rs` |
| Parquet page decode + mid-page resume (skip-replay) | Done in `numeric_scan.rs` (replay metrics on Q3) |
| **Materialized i64** path (expand dict IDs → `pending`) | Done — used by `ExecutionGroup`, ClickBench Q3 |
| **Dict-ID** path (compact `u32` + validity) | In progress — `ScanValueMode::DictionaryIds` |
| `BatchBuffer` / colocated aggregate | **i64 only** — needs dict-aware `Running` or on-demand resolve |
| `ParquetIngestOp` WCS projection | **PROJ_KIND_I64 only** — wire `PROJ_KIND_DICT_ID` + `FRAME_DICTIONARY` |
| WCS `DICTIONARY` payload | TS codec: **UTF-8 strings**; Rust adds **numeric** variant (`stream_wcs.rs`) |
| 8 MiB execution-group dictionary budget | `ScanLimits::for_execution_group` |
| Cross–row-group dict dedup | Out of scope (new scope id per RG × column) |

## Target memory model

`O(active dictionary bytes + fixed batch buffers + bounded codec/transport scratch)` — not `O(rows)`.

Normative budgets: [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md), [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md).

## Work phases

1. **Decode:** `DictionaryIds` mode on `NumericScan`; validate IDs; no `pending` i64 expansion.
2. **WCOL batch:** `BatchBuffer` column kinds (i64 / dict-id / f64); borrowed `BatchView`.
3. **Remote:** Install dictionary once per `(dict_scope, dict_id)`; projection batches reference scope; numeric dict payload in Rust (TS host forwards bytes).
4. **Query operators (not decode):** Filter/aggregate consume WCOL dict-id batches + installed dictionary; predicate-on-dictionary is an operator optimization, not ParquetDecode logic.
5. **Lifetime:** Reclaim dictionary when no in-flight batch references scope; execution-group byte cap enforced at `push_*`.
6. **Tests:** Brief §8 matrix + AdvEngineID/Q3 plateau (226 RG) on full `hits.parquet`.

## Dictionary identity

- **Parquet:** one dictionary per column **chunk** (row group × column).
- **WCOL stream:** `dict_scope` = row group (today); `dict_id` = catalog column id (or stable column id in plan).
- New RG → new dictionary install even if bytes match; operators must not compare raw IDs across scopes.

## Files to touch

- `wcol-parquet`: `dictionary.rs`, `numeric_scan.rs`, `projection.rs`, `stream_wcs.rs`, `decode.rs`
- `wcol-stream-operators`: `batch.rs`, `parquet_ingest.rs`, `exec_group.rs`, `wcs.rs`
- `archive/legacy-ts-stream/codec.ts`: decode numeric dictionary payload (transport only)
- Tests: `numeric_scan_dictionary.rs`, new `dict_wcs_roundtrip.rs`, extend `parquet-ingest-stream.test.ts`
