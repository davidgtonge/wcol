# Bounded memory (Parquet → WCOL ingest)

**Status:** Normative target; **DoD I-09 pass** = guardrails + harness ([I-09-BOUNDED-MEMORY.md](./I-09-BOUNDED-MEMORY.md)). Full ring decode / restrictive Q3 gate still open here.  
**Companion:** [STREAMING_EXECUTION.md §8](./STREAMING_EXECUTION.md#8-normative-invariants) (I-04, I-05, I-07), [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md)

## Architectural invariant

> **Parquet ingestion has bounded working memory independent of input dataset and row-group size.** Every allocation is either a fixed/reusable workspace, explicitly budgeted dictionary state, or a bounded in-flight buffer. Exhausting the budget produces backpressure or an explicit error, never uncontrolled Wasm memory growth.

**Decisive corollary:** An ingest node can process an arbitrarily long sequence of *supported* Parquet row groups without increasing its working-memory requirement. Oversized pages or dictionaries may fail with an explicit resource-limit error; ordinary row groups must not accumulate memory.

This is stronger than “the Thrift parser is zero-allocation.” Snappy decompression, dictionaries, page decode, batch assembly, and transport copies must all respect the same budget.

## I-04 vs I-05 (current milestone)

| | I-04 | I-05 |
| --- | --- | --- |
| **What** | Fixed-capacity batch buffers; count guardrails | Bounded-memory **streaming** decoder + plateau proof |
| **Today** | `next_batch_into`, `ScanLimits` counts, partial byte checks | Whole column chunks in `ExecutionGroup`; page `Vec<Cell>`; no demand-driven ReadAt |
| **`ScanLimits`** | Allocation guardrails | **Not** sufficient alone — needs byte budget + harness pass |

## Implementation order (instrument first)

| Priority | Work | What it proves |
| --- | --- | --- |
| **1** | **Wasm / native memory plateau harness** | Where memory grows today (baseline before refactors) |
| **2** | **ReadAt + bounded compressed ring** | No whole-column-chunk buffering |
| **3** | **Incremental page decode** | No page-sized `Vec<Cell>`; resumable mid-page |
| **4** | **Slow-sink backpressure test** | Blocked output stops source I/O within prefetch credits |
| **5** | **Full Q3 on deployed DOs** | End-to-end guarantee under isolate limits |

Harness: `./scripts/run-memory-plateau.sh`  
Native probes: `cargo test -p wcol-stream-operators --features memory-plateau --test memory_plateau`  
Q3 baseline (226 RG): `HITS_PARQUET=… cargo test … --test memory_plateau q3_memory_plateau_baseline_harness -- --ignored --nocapture`

## Demand-driven ReadAt (target)

```text
Parquet decoder requests bytes (NeedInput)
    → host fills bounded compressed ring (ReadAt)
    → decoder processes available input
    → WCOL batch ready (BatchReady / HaveOutput)
    → downstream accepts or blocks
    → only then decoder requests more input
```

Prefetch is allowed only when the planner assigns **explicit memory credits**. A bounded input ring does **not** bound decompression: the decoder needs resumable codec state or a maximum uncompressed page size; unsupported oversized pages fail **before** allocating.

## Byte budget (normative shape)

Count limits (`max_batch_rows`, `max_pending_rows`, …) are useful guardrails but **memory limits are bytes**. Dictionary entry count does not bound dictionary bytes; row count does not bound representation size.

```rust
pub struct ScanMemoryBudget {
    pub compressed_bytes: usize,
    pub codec_scratch_bytes: usize,
    pub dictionary_bytes: usize,
    pub batch_bytes: usize,
    pub output_bytes: usize,
}
```

Implemented in `wcol_parquet::ingest_memory::ScanMemoryBudget`, carried on `ScanLimits::budget`. The decoder must not expand a region without reserved capacity; the budget covers **peak simultaneous** allocations (e.g. JS response + Wasm copy).

Illustrative ingest-DO split: `ScanMemoryBudget::ingest_do_example()` (~32 MiB working regions). Restrictive acceptance: `q3_acceptance_restrictive()` (for future gate test).

## Intended stack

```text
HTTP / R2 ReadAt (demand-driven)
    → bounded compressed-input scratch (no whole column chunks)
    → incremental Parquet page decode (reusable codec + dictionary state)
    → fixed-capacity WCOL batch (column values + validity)
    → output edge:
        colocated: borrowed BatchBuffer (+ SelectionMask) to query ops
        remote: bounded WCS1 frame buffer
```

When the batch is full, the producer returns **`HaveOutput`** / **`BatchReady`**. It must **not** keep fetching/decoding while downstream holds the batch, except bytes already counted in prefetch credits.

## Five enforceable rules

| # | Rule |
| --- | --- |
| 1 | **No whole column chunks in memory.** Bounded range reads; process pages incrementally. |
| 2 | **No unbounded page decode.** Oversized page/dictionary → **resource-limit error** or streaming decode. |
| 3 | **Fixed-capacity batch buffers.** `next_batch_into`; vectors must not grow with total rows processed. |
| 4 | **Backpressure to the source.** Blocked sink → stop `ReadAt`; JS-held bytes count toward budget. |
| 5 | **Memory plateau.** Row group 200 ≈ row group 2 for decoder working set. |

## Acceptance test (Q3)

Full ClickBench **Q3**, **226** row groups, deliberately restrictive ingest budget (once ReadAt lands).

After **each row group**, record:

- Live Rust allocations and high-water mark (`alloc_track`; native via `--features memory-plateau`, Wasm via global probe)
- Wasm linear memory (Wasm builds)
- Max compressed / decompressed bytes resident (driver + decoder accounting)
- Max WCOL batch and pending-output bytes
- Outstanding HTTP requests and reserved byte capacity (`DriverMemoryCounters` in harness)

**Pass:** plateau after warmup; slow sink stops reads within prefetch allowance; correct aggregates; no DO restarts.

**Types:** `IngestMemorySample`, `IngestMemoryHighWater`, `plateau_report()` in `wcol_parquet::ingest_memory`.

## Decoder contract (target)

```rust
pub enum DecodeProgress {
    NeedInput,
    BatchReady { rows: usize },
    Finished,
}
```

Mid-page resume when the batch fills. Codecs that cannot stream within the workspace fail explicitly.

## Current gaps (honest)

| Gap | Location |
| --- | --- |
| ~~Whole-column compressed retention~~ | **Fixed:** per-column sliding window in [`NumericScan`](../../rust/wcol/src/formats/parquet/numeric_scan.rs); shared group budget in [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs); tests [`streaming_compressed_window.rs`](../../rust/wcol/tests/runtime/streaming_compressed_window.rs) |
| **Skip-replay baseline** (legacy path; delta / unsupported encodings still replay) | `PageResume::input_events_consumed`; owned wiring in [`numeric_scan.rs`](../../rust/wcol/src/formats/parquet/numeric_scan.rs) (`ENABLE_OWNED_PAGE_CURSOR`) |
| Owned **`PageCursor`** (single inflate; required + optional flat `max_def ≤ 1`, PLAIN/RLE dict data pages) | [`page_cursor.rs`](../../rust/wcol/src/formats/parquet/page_cursor.rs) + tests [`page_cursor_replay.rs`](../../rust/wcol/tests/runtime/page_cursor_replay.rs), [`page_cursor_optional_batch.rs`](../../rust/wcol/tests/parquet/page_cursor_optional_batch.rs) |

### Skip-replay baseline (current)

When a page outlives one output batch, the decoder **re-feeds the same compressed page body** and skips committed **logical input events** (`PageEvent` count, not output rows). Invariants:

1. **Input positions, not emitted rows** — nulls and filters can make output count ≠ input events; resume uses `input_events_consumed`.
2. **No duplicate output** — skipped events do not push to `pending` / dictionary again.
3. **Bounded page memory** — body is a subslice of the column ring; no extra decompressed page allocation per replay.
4. **Measure replay amplification** — `ScanReplayMetrics`: `page_replay_count`, `page_events_visited`, `page_events_committed`, and `replay_amplification()` (= visited / committed).

Large pages (e.g. 450k values, 8k batches) could approach **~28× decode work** on the **legacy replay path**. Q3 / flat harness tests assert **`page_replay_count == 0`** when owned cursor covers the ingested columns (`assert_owned_ingest_zero_replays` in [`memory_plateau.rs`](../../rust/wcol/tests/runtime/memory_plateau.rs)).

| ~~Single-flight anonymous ReadAt~~ | **Fixed:** ticketed [`HostReadAt`](../../rust/wcol/src/formats/parquet/batch_decode.rs) (depth **2**, reserved credit, `complete_read` / `cancel_read`); host via `wcs_read_at_ticket` + `wcs_complete_read` in [`run-wcol-engine.ts`](../../src/runtime/stream/run-wcol-engine.ts) |
| I-05 plateau assert (opt-in) | `WCOL_I05_ASSERT_PLATEAU=1` on `q3_memory_plateau_baseline_harness`; `q3_memory_plateau_local` always asserts |
| Restrictive budget gate (opt-in) | `q3_memory_plateau_restrictive_budget` — ticketed ingest + per–row-group samples; enable `--ignored` after `ScanMemoryBudget::q3_acceptance_restrictive` is enforced on decode |

See [STREAMING_EXECUTION.md Appendix A](./STREAMING_EXECUTION.md#appendix-a--code-map-workspace-2026-03) for the living code map.
