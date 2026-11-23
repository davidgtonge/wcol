# Q3 pipeline vs chunk oracle (76s vs 26s)

The opcode **10** path (`GraphHost` + `ParquetDecode` + filter/aggregate) is often slower than the legacy chunk oracle (`runParquetScan`, concurrency **4**). That gap is **not** inherent to WCOL streaming; the main suspects were **skip-replay decode** and **serialized HTTP `ReadAt`** (partially addressed below).

## Likely contributors

| Factor | Pipeline (opcode 10) | Chunk oracle |
|--------|----------------------|--------------|
| Decode | Ticketed ReadAt; **owned `PageCursor`** for flat `max_def ≤ 1` PLAIN / dict RLE (materialized + dict-id); legacy replay for delta (**5**) and unsupported encodings | One pass per chunk |
| HTTP | Ticketed ReadAt depth **2** (`complete_read(ticket)`); TS overlaps fetches, Rust schedules | Up to 2 in-flight range reads + host prefetch hints |
| Q3 data | ~40 MB compressed / 452 reads / 2 columns | Same bytes, different scheduling |

Q8 reads one small column per row group (~0.9 MB total), so replay and I/O hurt less.

## Measure (HTTP)

Rebuild stream wasm, then:

```bash
cd wcol/rust && cargo build -p wcol --target wasm32-unknown-unknown --release
cd wcol && npx tsx tests/hits-http-exec-group.ts --query q3 --all-row-groups --profile
```

`--profile` adds:

| Field | Meaning |
|-------|---------|
| `readAtWaitMs` / `readAtCalls` | Host time blocked on HTTP range reads |
| `pipelineMsExcludingReadAt` | Wall clock minus read wait (decode + query + wasm pump) |
| `ingestReplay.pageReplayCount` | Page decode passes that restarted mid-page |
| `ingestReplay.pageEventsVisited` | Parquet events visited (includes skipped replay) |
| `ingestReplay.pageEventsCommitted` | Logical events emitted (sum per completed page) |
| `ingestReplay.amplification` | `visited / committed` |

## Measure (native, local file)

Avoid network to separate decode replay from I/O:

Record three wall times on the same fixture when tuning (not a CI gate):

| Run | Host / decode | Notes |
|-----|----------------|-------|
| A | `readAtConcurrency: 1` | Serial ReadAt baseline |
| B | Default host (overlap + tickets) | Current TS [`run-wcol-engine.ts`](../../src/runtime/stream/run-wcol-engine.ts) |
| C | Same as B with owned cursor (**on** in [`numeric_scan.rs`](../../rust/wcol/src/formats/parquet/numeric_scan.rs)) | Expect `ingestReplay.pageReplayCount == 0` on Q3 columns (see `assert_owned_ingest_zero_replays` in [`memory_plateau.rs`](../../rust/wcol/tests/runtime/memory_plateau.rs)) |

Log `ingestReplay.*` from wasm stats or [`memory_plateau`](../../rust/wcol/tests/runtime/memory_plateau.rs) `log_replay_metrics`.

```bash
# 1) legacy-style serial host (readAtConcurrency: 1)
# 2) default host (overlap + cursor)
# 3) compare ingestReplay.pageReplayCount in run stats
```

```bash
HITS_PARQUET=/path/to/hits.parquet cargo test -p wcol --features memory-plateau --test memory_plateau -- --ignored --nocapture
```

Uses `ingest_replay_*` totals and optional alloc probes (`BOUNDED_MEMORY.md`).

## Fixes in flight / planned

1. **`page_events_committed`** — now **accumulates** per completed page (was overwritten each page/replay pass); global merge via `ingest_replay_merge` is meaningful again.
2. **Owned `PageCursor`** — **on** for required + optional flat columns (`MaterializedI64` and `DictionaryIds`); legacy fallback for delta / unsupported encodings. Tests: [`page_cursor_replay.rs`](../../rust/wcol/tests/runtime/page_cursor_replay.rs), [`memory_plateau.rs`](../../rust/wcol/tests/runtime/memory_plateau.rs) `assert_owned_ingest_zero_replays`.
3. **Overlapping `ReadAt`** — **done:** up to **two** outstanding tickets in [`ParquetDecode`](../../rust/wcol/src/formats/parquet/batch_decode.rs); `wcs_read_at_ticket` + `wcs_complete_read` + overlapping fetches in [`run-wcol-engine.ts`](../../src/runtime/stream/run-wcol-engine.ts). Optional `wcs_read_at_hints` + `ReadAtPool` still warm extra ranges (default concurrency 4).

## Interpretation

- If `readAtWaitMs ≈ pipelineMs` on HTTP but local Q3 is fast → prioritize **read overlap**.
- If `ingestReplay.amplification` is high (e.g. ≫ 2 on wide row groups) with local file → prioritize **PageCursor / no-replay decode**.
