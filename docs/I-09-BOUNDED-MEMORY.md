# I-09 — Bounded memory (DoD)

**Status: pass (guardrails + harness)** (2026-10-28)

Normative byte budgets and plateau proof live in [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md). DoD **I-09** here means **enforceable caps and stable hot-path buffers** on the colocated Parquet path. Whole-column compressed retention is removed (sliding window + shared group budget); **page scratch verification**, skip-replay / `PageCursor`, and restrictive plateau gates remain open.

## Enforced today

| Mechanism | Location |
|-----------|----------|
| `ScanMemoryBudget::compressed_bytes` | Per-column sliding window; `read_credit()` + `try_compact()` truncate live bytes |
| Shared group budget | One [`PageScratch`](../../rust/wcol/src/formats/parquet/page_scratch.rs) per row group in [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs) |
| Bound ReadAt | Ticketed [`HostReadAt`](../../rust/wcol/src/formats/parquet/batch_decode.rs) (max **2** outstanding, reserved credit) + [`HOST_READ_LIMIT`](../../rust/wcol/src/formats/parquet/batch_decode.rs) (256 KiB); [`ticketed_read_at.rs`](../../rust/wcol/tests/runtime/ticketed_read_at.rs) |
| Fixed batch buffers | `next_batch_into` capacity check; tests in `memory_plateau`, `numeric_scan_batches` |
| Shared dict entries | [`DictionaryValues::int_entries`](../../rust/wcol/src/stream/batch/wcol_batch.rs) `Arc<[i64]>`; one build per column chunk in [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs) |
| Plateau harness | [`memory_plateau.rs`](../../rust/wcol/tests/runtime/memory_plateau.rs) + `ingest_memory::plateau_report` |

## Evidence (always-on tests)

- [`bounded_memory.rs`](../../rust/wcol/tests/runtime/bounded_memory.rs) — window credit, resident live bytes, shared dict `Arc`
- [`streaming_compressed_window.rs`](../../rust/wcol/tests/runtime/streaming_compressed_window.rs) — early batch, multi-page window, oversize page error, group budget
- [`numeric_scan_next_batch_into_does_not_grow_batch_buffers`](../../rust/wcol/tests/runtime/memory_plateau.rs)
- Optional full Q3: `HITS_PARQUET=… cargo test -p wcol --features memory-plateau --test memory_plateau q3_memory_plateau_local -- --ignored`

- [`page_cursor_replay.rs`](../../rust/wcol/tests/runtime/page_cursor_replay.rs) — zero replay on owned PLAIN + dict-id (required + nullable) ingest
- [`page_scratch.rs`](../../rust/wcol/src/formats/parquet/page_scratch.rs) unit tests + oversize page errors via [`streaming_compressed_window.rs`](../../rust/wcol/tests/runtime/streaming_compressed_window.rs)

## P0 gate (HTTP full hits)

226-row-group Q3 with [`IngestMemoryProfile::Q3Restrictive`](../../rust/wcol/src/plan/physical.rs) against the ClickHouse HTTP object (range reads, no local 14 GiB file):

```bash
npm run test:restrictive-plateau-hits
```

`HITS_PARQUET=/path/to/hits.parquet` still runs the native `memory-plateau` harness. The HTTP gate asserts replay **0** and wasm linear memory stops growing by row group **64**, then stays flat through row group 225.

Flat fixture gate: `q3_memory_plateau_restrictive_flat_fixture` (always-on).

## Future (still BOUNDED_MEMORY.md)

- Delta-binary (**encoding 5**) and other unsupported pages stay on legacy replay until owned cursor extends
