# Retire leftover engine paths

**Status:** Planned (2026-10-29)  
**Rule:** One scan, one aggregate kernel, one partial wire, one reduce. Delete a path only after every caller below has a replacement and the listed checks pass. Do not change hot-loop shape (`Acc::add`, packed masks, one predicate per dictionary entry) while doing this.

Production queries already go through `GraphHost` → `LinearColocatedGraph` → batch `FilterOp` / `AggregateOp`, and emit PWCL. Opcode **10** is the ingest plan. Opcode **2** (Parquet→WCS) and opcode **3** (WCS query) use the same graph. This plan is the code that still sits beside that path.

Related: [EXECUTOR-CONSOLIDATION.md](./EXECUTOR-CONSOLIDATION.md), [I-14-LEGACY-DELETED.md](./I-14-LEGACY-DELETED.md). I-14 removed the old graph types and opcodes 5–9. The items here are the next cut.

## Already started

On branch `cleanup-src-minimal` (`wcol-src-cleanup` worktree, uncommitted): `FilterOp` and `AggregateOp` hold only the batch kernels, and `query_bench` no longer times a row engine. `Running` / `RowFilter` stay in `ops/query/mod.rs` as the test oracle. Land that branch before phase 2 so `main` matches it. `main` still falls back to `FilterOp::open_rows` when `BatchFilter::open` returns `None`.

The worktree also deletes `exec/edge.rs`, `exec/memory.rs`, `exec/runtime/batch.rs`, `exec/runtime/batch_lifecycle.rs`, and `stream/wire.rs`. `exec/edge.rs` is one of the WPP1 callers below, so recheck the phase 4 caller list after the branch lands. The branch is cut from `main` at 9dbd8e5.

## Order

Each phase is safe to ship on its own. Later phases depend on the earlier replacement, not on deleting the old type in the same change.

| Phase | Retire | Replacement | Delete when |
|-------|--------|-------------|-------------|
| 0 | Code nothing calls, and `dead_code` / `unused` warnings | None needed. | `cargo build -p wcol` has no `unused` or `dead_code` warnings. |
| 1 | Row fallback inside `FilterOp` / `AggregateOp` / `query_bench` | Batch kernels only. Unsupported SQL returns an error. | `cargo test -p wcol ops::filter ops::aggregate` and `query_bench` build. |
| 2 | `Running` and the row consume loop | Tests assert sums, or build the fixture with `BatchQuery`. | Batch parity tests no longer call `Running::open`. |
| 3 | `exec_sql`, `aggregate_chunk`, `decode_chunk` | ClickBench and scan parity compare to DuckDB or a stored fixture. | No Rust or TS caller names those functions. |
| 4 | WPP1 as the host view | Host reads PWCL. Bench and DuckDB tests read `PartialSnapshot`. | `wcol_partial_wire_to_wpp1` has no callers. |
| 5 | `PLAN_BYTE_PIPE` opcode 2 (`WcsPartialReduce`) | Coordinator merges with `ReduceOp` on typed partials. | `reducePartialFromWcsStream` is gone. Keep opcode 0/1 only while the bench consumer still checks frames. |
| 6 | `run_page_legacy` | `PageCursor` decodes dictionary pages and the encodings it still refuses. | `numeric_scan` has one page driver, and files from writers other than DuckDB still decode. |

## Phase 0 — Delete what nothing calls

No replacement is needed, so this can land before or alongside `cleanup-src-minimal`. Do it after that branch lands, because the branch already removes some of these (it takes `cargo build -p wcol` from 23 warnings to 10).

**Public functions with no caller in Rust, TS, or tests** (all in `ops/query/mod.rs`):

| Item | Notes |
|------|-------|
| `exec_decoded`, `select_decoded`, `decoded_cells`, `DecodedColumn` | Row execution over pre-decoded columns. Nothing outside the module names them. |
| `merge_encoded` | WPP1 merge wrapper. `merge_wpp1_partials` is the one in use. |
| `columns_for_rows` | `output_columns_for_rows` is the one `exec/colocated_plan.rs` and `ops/project.rs` call. |

`exec_fast` and the module's own `aggregate_column` exist only to serve `exec_sql`. They go in phase 3, not here.

**Warnings left on `cleanup-src-minimal`:**

| Location | Item |
|----------|------|
| `exec/graph_scheduler.rs` | Unused import `ReduceOp` |
| `stream/partials.rs` | Unused import `merge_wpp1_partials` |
| `exec/operators/wcs_frame.rs` | `CompleteFrame::sequence` never read |
| `formats/parquet/batch_decode.rs` | `ParquetDecode::dict_columns` never read. The constructor already copies it into each chunk's `dict_encoded`, so only the stored set goes. |
| `formats/parquet/alloc_track.rs` | `Slot::load`, `write_stats` |
| `formats/parquet/dictionary.rs` | `DictPhysical` |
| `formats/parquet/page_cursor.rs` | `ActivePlainPage` fields `header`, `spec`, `body_end`, `snappy_inflates` |
| `formats/parquet/page_compact.rs` | `SchemaCol::physical` |
| `plan/page_compaction.rs` | `FlatRowGroup::rows` |

`main` today also warns on `BatchLifecycle`, `Utf8Batch`, `push_utf8_value_bytes`, `IngestQuery::has_pending`, unused `wcs_frame` methods, and a few imports. The branch removes those; re-run the build after it lands and treat whatever is left as this list.

**Work:**

- Delete the uncalled functions and the warned items. Where a field is only written for debugging (`snappy_inflates`, the `alloc_track` stats), delete it unless a bench reads it through a path the compiler cannot see, such as an FFI export.
- Add `#![deny(unused, dead_code)]` to `wcol` only once the build is clean, so later phases cannot leave new strays behind. Leave `#[cfg(test)]` and `native`-feature code alone. Check with `--features native` too, so bin-only uses still count.

**Check:** `cargo build -p wcol` and `cargo build -p wcol --features native` have no warnings. `cargo test -p wcol` passes. `npm run -s build:stream-wasm` still builds.

## Phase 1 — Operators are batch-only

**Callers today (`main`):** `FilterOp::open` tries `BatchFilter`, then `open_rows`. `AggregateOp` can still run the row kernel. `src/bin/query_bench.rs` times both and compares partials.

**Used for:** A second execution of the same SQL when the batch kernel declines a plan, and a bench that checks the two engines agree.

**What the fallback still catches:** `BatchQuery::open` never declines now. `BatchFilter::open` declines two predicate shapes: ordered text comparisons (`URL > 'x'`, `<`, `<=`, `>=`) and comparisons against a NaN literal. The row kernel cannot run ordered text comparisons either. It fails mid-scan with `"utf8 columns cannot be resolved to i64 slots"`. Phase 1 therefore loses no working query. It moves that failure to open time with a clear message.

**Work:**

- Land the `cleanup-src-minimal` operator change: `FilterOp` stores `BatchFilter`, `AggregateOp` stores `BatchQuery`, `BatchQuery::open` returns `Result<Self, String>`.
- Keep the LIKE-on-numeric error from `reject_unsupported_filters` (`"LIKE needs a text column"`). A plan the batch kernel cannot run returns `"filter is not supported by the batch kernel"`.
- Bench prints decode time and batch time. Group print still goes through WPP1 until phase 4.

**Leave in place:** `Running` and `RowFilter`. Phase 2 owns those.

## Phase 2 — `Running` stops being the oracle

`Running` walks rows and builds the same partial the batch kernel builds. The graph does not open it.

| Caller | Used for |
|--------|----------|
| `rust/wcol/src/ops/query/batch/tests.rs` | About 40 random seeds: batch bytes must equal `Running`. Both engines reject GROUP BY with more than 4 keys and text aggregates other than `COUNT`. The batch engine also runs aggregate offsets, `COUNT(DISTINCT)`, `approx_count_distinct`, and `COUNT` on text, which `Running` rejects (`row_kernel_still_rejects_offsets_and_distinct_is_rejected`). The byte compare therefore does not cover those features today. |
| `rust/wcol/src/stream/partials.rs` tests | `SUM(a)` fixture. Round-trip through WCOL columns and WPP1. |
| `rust/wcol/src/stream/partial_wcs.rs` tests | Same fixture. PWCL magic, WPP1 magic, wire sum 10 or 5. |
| `rust/wcol/src/formats/json.rs` tests | Same fixture. JSON body contains `sum_i` and kind `sum`. |

**Work:**

- Move the random-batch parity onto the brute-force oracles that already exist. `rows_of` in `batch/tests.rs` (numeric) and `batch/string_tests.rs` (text, NULL, PLAIN and dictionary batches) resolve every row and compute the expected groups directly. Those tests already cover offsets, `COUNT(DISTINCT)`, and string keys. Extend them to the SQL shapes the `Running` compare covers, then drop the byte-for-byte compare.
- Partial, wire, and JSON tests build that `SUM(a)` state with `BatchQuery` (or a tiny `PartialStateWcol` literal) and keep the round-trip asserts.
- Delete `RowFilter`, `Running`, and the row `consume` loop in `ops/query/mod.rs`, along with their re-exports in `src/lib.rs` and `src/exec/runtime/mod.rs`.
- Keep `Stat` / `Partial` / `PartialSnapshot` and the WPP1 codec until phase 4. `BatchQuery` and the tests still encode through them.

**Check:** `cargo test -p wcol ops::query::batch stream::partials stream::partial_wcs formats::json`.

## Phase 3 — One Parquet page walk

`ops/query/parquet_chunk.rs` (`aggregate_chunk`, `decode_chunk`) and `exec_sql` in `ops/query/mod.rs` decode a column chunk with their own page walk, then aggregate. `NumericScan` is the scan the graph uses. The chunk walker exists so tests can compare against something that did not come from `NumericScan`.

| Caller | Used for |
|--------|----------|
| `rust/wcol/tests/runtime/clickbench_exec_group.rs` | Q1, Q3, Q8: graph partial must match `exec_sql` on the same row groups. Skipped without `HITS_PARQUET`. |
| `ops/query/mod.rs` `plans_and_filters_the_flat_fixture` | `flat.parquet`: filtered `SUM` matches the chunk oracle. |
| `formats/parquet` `aggregate_column` | Sums one column by calling `aggregate_chunk` per chunk. `tests/parquet/roundtrip.rs` uses it. |
| `dod_acceptance.rs`, `numeric_scan_dictionary.rs`, `hits_adv_engine_id_parity.rs`, `hits_adv_engine_id_diff.rs` | Scan sums and cell diffs against `aggregate_chunk` / `decode_chunk`. |
| `src/runtime/stream/hits-exec-group.ts` `oracleHitsPartial` | HTTP ClickBench: `runParquetScan` over the same ranges, then compare to the graph partial. |
| `scripts/clickbench.ts` | Local script. `runParquetScan` is the SQL oracle next to DuckDB. |
| `src/runtime/parquet.ts` `ParquetFile.aggregate` | `tests/parquet.test.ts`. One-column total via wasm `parquet_aggregate_chunk`. |

Current Rust does not export `parquet_exec_sql` or `parquet_aggregate_chunk`. The TypeScript calls above target an older wasm. Cloudflare already stubs `runParquetScan` out of the Durable Object.

**Work:**

- Point `clickbench_exec_group`, `hits-exec-group.ts`, and `scripts/clickbench.ts` at DuckDB (the script already shells out to it) or at a checked-in expected partial.
- Point scan parity tests at a fixture of expected values per page, or at `NumericScan` alone plus a tiny hand-built page.
- Move `aggregate_column` onto `NumericScan`, then delete `exec_sql`, `parquet_chunk.rs`, `runParquetScan` / `execParquetSql`, and `ParquetFile.aggregate`.

**Check:** `cargo test -p wcol --test clickbench_exec_group` with `HITS_PARQUET` when the file is present; `tests/parquet.test.ts`; one `scripts/clickbench.ts` query against DuckDB.

## Phase 4 — Host reads PWCL

The graph edge is PWCL. WPP1 is the view the host parses after `wcol_partial_wire_to_wpp1` normalizes either body into WPP1 bytes.

| Caller | Used for |
|--------|----------|
| `exec/runtime/ffi.rs` `wcol_partial_wire_to_wpp1` | Wasm export. `src/runtime/stream/partial-wire.ts` copies a PWCL or WPP1 body into WPP1. |
| `src/runtime/stream/partial-oracle.ts` | `parsePartialQueryFromWire`, `partialsEquivalent`. Distributed HTTP acceptance, `parquet-job` display, Cloudflare scripts `cf-shard-lifecycle.ts`, `cf-production-validate.ts`, `cf-production-distributed.ts`, `cf-coordinator-dispatch-recovery.ts`. |
| `query_bench --print-groups` | Prints group keys from a WPP1 encoding of the batch result. |
| DuckDB fixture and distributed Rust tests | `partial_state_to_wpp1` then a `PartialSnapshot` so sums can be compared. |

Encode and decode live in `ops/query/mod.rs` (`encode_wpp1_partial`, `decode_wpp1_partial`, `merge_wpp1_partials`), `stream/partials.rs` (`partial_state_to_wpp1`, `partial_state_from_wpp1`), and `stream/partial_wcs.rs` (`partial_wire_to_wpp1`). `JsonEncode::write_partial` tries PWCL, then WPP1.

**Work:**

- Teach `partial-oracle.ts` to read PWCL columns directly. `partialsEquivalent` compares those columns.
- Bench `--print-groups` and the Rust fixture tests read `PartialSnapshot` from `PartialStateWcol` with no WPP1 hop.
- JSON sink writes PWCL only.
- Delete the WPP1 codec and `wcol_partial_wire_to_wpp1`.

**Check:** `hits-http-distributed-acceptance.test.ts`, `vertical-slice.test.ts`, `partial-oracle.test.ts`, and `cargo test -p wcol duckdb_fixture distributed_semantics`.

## Phase 5 — Coordinator reduce is `ReduceOp`

`wcs_open` still accepts prefix `0xBE` (`PLAN_BYTE_PIPE`) and opens `stream::core::Pipeline`. That pipeline is transport, not the SQL graph.

| Opcode | Operator | Caller | Used for |
|--------|----------|--------|----------|
| 0 | `CopyBytes` | `BYTE_COPY_ONLY_PLAN` | Byte passthrough. Tests and any host that wants a copy. |
| 1 | `WcsGuard` | `WCS_VALIDATE_AND_COPY_PLAN` in `cloudflare/src/wcs-bench-node.ts`; `byte_pipe_ffi.rs`; `wcs_row_stream.rs` | Validate WCS frames and copy them. Bench consumer. |
| 2 | `WcsPartialReduce` | `WCS_PARTIAL_REDUCE_PLAN` via `reducePartialFromWcsStream` | Merge worker WCS streams into one PWCL. Called from `cloudflare/src/graph-coordinator-run.ts` and `cloudflare/src/reduce-stage-node.ts`. |

**Work:**

- Replace opcode 2 with the same `ReduceOp` the in-process graph uses. The coordinator reads partial frames into `PartialStateWcol` and merges them. Output stays one PWCL.
- After that, delete `WcsPartialReduce` and stop sending `[0xBE, 2]`.
- Leave opcode 0 and 1 until the bench consumer no longer needs a frame-checking pipe. Deleting the whole `0xBE` prefix is a separate change once that bench is gone.

**Check:** Cloudflare coordinator dispatch recovery and a reduce-stage run that merges at least two worker streams. `cargo test -p wcol byte_pipe_ffi` stays green while opcode 1 remains.

## Phase 6 — `PageCursor` owns every page `NumericScan` sees

`run_page_legacy` in `formats/parquet/numeric_scan.rs` is on the scan. `decode_step` calls it when a page is paused, and `start_page_if_ready` calls it when the owned cursor is off or returns `"page encoding not supported by owned cursor yet"`. The `#[allow(dead_code)]` on that function is wrong; it has callers.

`owned_cursor_supports_data_page` takes:

- PLAIN (`encoding` 0) boolean, int32, int64, byte array, fixed-length byte array
- RLE boolean (`encoding` 3)
- Dictionary-encoded data pages (`encoding` 2 and 8) whenever the chunk has a dictionary, in every value mode (materialized i64, dictionary ids, and PLAIN UTF-8)

Legacy still takes dictionary pages themselves (one per column chunk), nested columns (`max_def > 1`), and the other data-page encodings `compact-parquet-thrift` decodes: DELTA_BINARY_PACKED (int32, int64), DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, and PLAIN float and double. Neither driver decodes BYTE_STREAM_SPLIT, and `physical_of` rejects INT96 before either driver runs. Those two are new work, not a port.

**What actually reaches legacy today:** `data/hits_subset_500k.parquet` (DuckDB 1.3.2) uses only PLAIN and RLE_DICTIONARY. On hits, legacy decodes only the dictionary page at the start of each dictionary chunk. That costs little, and every data page already goes through the owned cursor. The risk in this phase is files from other writers. pyarrow with data page v2, Spark, and Trino emit DELTA encodings and float and double columns, and today those decode only through legacy. Spark and older Hive and Trino files also carry INT96 timestamps, which neither driver reads. Deleting legacy before porting them turns readable files into hard errors, which matters for the serverless Trino goal.

**Work:**

- Add Parquet fixtures from at least one non-DuckDB writer (pyarrow with `data_page_version="2.0"` and `use_dictionary=False` on some columns, so DELTA appears), with float and double columns. Record each column's current values through the legacy path before changing anything.
- Extend `PageCursor` one encoding family at a time. Dictionary pages first: they are the only legacy pages on hits, and they are simple (PLAIN values into `self.dict`). Then DELTA_BINARY_PACKED, the DELTA byte-array pair, and PLAIN float and double. BYTE_STREAM_SPLIT and INT96 are separate features, so track them outside this phase.
- After each family, a scan test that used to hit `run_page_legacy` runs on the owned cursor and matches the recorded values.
- Delete `PageResume` and `run_page_legacy` when `start_page_if_ready` always begins an owned page. Nested columns can stay unsupported with a hard error if no caller scans them; do not keep a second driver only for that error. Any encoding still unported at that point must fail with a named error, not silently fall through.

**Check:** `cargo test -p wcol numeric_scan page_cursor hits_adv_engine_id`, plus the non-DuckDB fixtures above. hits parity alone cannot show that this phase is safe, because hits never sends a data page to legacy. This phase is the one that can move scan numbers; gate it on value parity across both file sets, not only on compile.

## Out of scope

- `rust/wcol-sql` DataFusion planner spike. It does not execute these paths.
- Hot-loop rewrites inside `numeric_scan`, `page_cursor`, or the batch kernels.
- Opcodes 2, 3, and 10 on `GraphHost`. Those stay.
- Archived TypeScript under `archive/`. I-14 already quarantined it.
