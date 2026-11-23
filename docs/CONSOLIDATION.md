# WCOL repository consolidation

**Status:** In progress (Phase 4 — delete archived TS paths when parity allows; I-15 executor unify)  
**Rule:** One engine implementation. **Stop adding re-export shims and new features** until implementations live in their final modules and duplicate paths are removed.

**Definition of Done:** [DEFINITION_OF_DONE_STATUS.md](./DEFINITION_OF_DONE_STATUS.md) — **I-01–I-15 Pass** (2026-10-28). Remaining work is optional cleanup (`archive/`, clippy debt).

## Target layout

```text
wcol/
├── rust/
│   ├── wcol/           # ONE engine crate (canonical imports)
│   ├── wcol-wasm/      # Wasm ABI only
│   └── wcol-cli/       # Native CLI only
├── src/                # Public API + host I/O (no query logic)
├── cloudflare/         # Worker / DO lifecycle
├── tests/              # E2E, parity, placement
└── docs/               # ARCHITECTURE.md + this file + FORMAT / memory / streaming
```

Module map inside `rust/wcol/src/`: `stream/`, `formats/`, `ops/`, `exec/`, `plan/`, `host.rs`.

## Phase checklist

### Phase 1 — Inventory (baseline)

| Area | Authoritative today | Target module | Action |
|------|---------------------|---------------|--------|
| WCOL batches + dict | `wcol-parquet` `wcol_batch`, `dictionary` | `stream/batch`, `stream/dictionary` | **Move** |
| Partial state WCOL | `wcol-parquet` `agg_partial_wcol` | `stream/schema` | **Move** |
| WCS1 / byte pipeline | `wcol-stream-core` | `stream/wire`, `exec/scheduler` | **Move** |
| Parquet decode/scan | `wcol-parquet` | `formats/parquet/` | **Move** |
| `.wcol` file | `wcol-decoder`, `wcol-encoder`, `wcol-format` | `formats/wcol/` | **Move** |
| Parquet SQL kernel | `wcol-parquet` `query.rs` | `ops/query` | **Done** |
| Partial state WCOL | `agg_partial_wcol` | `stream/partials` | **Done** |
| JSON sink | `wcol-stream-operators` `JsonEncode` | `formats/json` | **Move** |
| Filter/Agg/Reduce/Finalize | `wcol-stream-operators` `wcol_stream/` | `ops/*` | **Move** |
| Colocated graph / exec group | `wcol-stream-operators` | `exec/graph`, `exec/scheduler` | **Move** |
| SQL plan | `wcol-sql-parser` | `plan/logical` | **Move** |
| Physical ingest plan | `wcol-stream-operators` `ingest_plan` | `plan/physical` | **Move** |
| `.wcol` query runtime | `wcol-decoder` FFI + native | `exec/` + `formats/wcol` | **Merge** then **delete** duplicate paths |
| TS `runtime/exec/*` | chunk workers, merge | Rust `exec/` | **Delete** (Phase 4) |
| TS `runtime/query/*` | plan JSON | Rust `plan/` | **Delete** |
| TS `runtime/parquet*.ts` | partial decode, job | Rust `formats/parquet` | **Delete** |
| TS `runtime/stream/*` except driver | codecs, graph | Rust `stream/`, `exec/` | **Delete** |
| `wcol-synth-groupby` | benchmarks | `tests/` or **delete** | **Delete** if unused |
| `archive/legacy-ts-stream` | old DAG | — | **Delete** when parity proven |

**Baselines to keep green during migration:** ClickBench Q1/Q3/Q8 (`clickbench_exec_group` on local subset; optional `npm run test:hits-http-full-q1` / `HITS_HTTP_CLICKBENCH=1` against full hits over HTTP), `pipeline_json`, `wcol_operator_chain`, memory plateau (hits subset), `.wcol` file roundtrip (decoder/encoder tests).

### Phase 2 — Consolidate Rust

- [x] Create `rust/wcol` crate with module tree (`stream/`, `formats/`, `ops/`, `exec/`, `plan/`).
- [x] **2b:** Move `wcol-parquet` → `wcol/src/formats/parquet/`; `wcol-stream-operators` → `wcol/src/exec/runtime/`; **delete** those workspace crates.
- [x] Integration tests under `wcol/tests/{runtime,parquet}/`; `cargo test -p wcol`.
- [x] **2c:** Move `wcol-sql-parser`, `wcol-stream-core`, `wcol-format` into `wcol`; delete workspace crates.
- [x] **2d:** Move `wcol-decoder` → `formats/wcol/decode/`, `wcol-encoder` → `formats/wcol/encode/` (native); `wcol-wasm` / `wcol-cli` depend on `wcol` only; **delete** decoder/encoder crates.

### Phase 3 — Real module consolidation (current)

Directory names match the target layout, but many files are still **re-exports** (`ops/filter.rs` → `exec/runtime`, `stream/batch.rs` → parquet + exec). Dependencies must flow **formats / ops → `stream` → `exec`**, never backward.

| Step | Action | Status |
|------|--------|--------|
| 3a | **`stream/batch`**: canonical `WcolBatch`, `OwnedWcolBatch`, `DictionaryValues` | **Done** — moved from `formats/parquet/wcol_batch` and `exec/runtime/pipeline/owned_batch`; deleted sources |
| 3b | Move `exec/runtime/wcol_stream/{filter,aggregate,reduce,finalize}.rs` bodies into `ops/` | **Done** — `exec/runtime/stream_io.rs` holds I/O + `WcolOperator`; deleted four files under `wcol_stream/` |
| 3c | Fold `exec/runtime/pipeline/*` into one graph executor; delete `execute()` driver | **Done** — `ParquetDecode` in `formats/parquet/batch_decode`; `JsonEncode` in `formats/json`; `QueryPipeline` = `ColocatedQueryGraph`; removed `pipeline/`; reference ingest loop only in `tests/runtime/decode_query_harness.rs` |
| 3d | Single v2 plan registry + stream operators under `exec/operators/`; physical encode in `plan/` | **Done** — `plan/stream_program.rs`, `plan/physical.rs`; `ingest_query` / `parquet_wcs_ingest` / `query_wcs`; deleted `exec/runtime/{exec_group,parquet_ingest,query_stream,ingest_query,ingest_plan}.rs` |
| 3e | Lift reusable kernels from `formats/wcol/decode/query/*` into `ops/`; file decode only in `formats/` | **Done** — implementations in `ops/query/wcol/*`; `formats/wcol/decode/query/*` are thin re-exports; decode I/O (`decode/`, `ffi/`, `parse/`, column materialization) stays in `formats/` |
| 3f | Remove `formats/parquet/agg.rs` after reference tests preserved elsewhere | **Done** — oracle moved to `ops/query/parquet_chunk.rs`; crate root re-exports `aggregate_chunk` / `Agg` from `ops::query` |

**Known technical debt (fix during 3b–3d, not new features):**

1. ~~**Parquet compressed ingest**~~ — sliding compressed window + shared group budget (`ParquetDecode`); skip-replay / plateau still I-09 (`BOUNDED_MEMORY.md`).
2. **Dictionary batches** — `parquet_decode` still `copy_int_entries()` per batch; install dict once per scope and share across batches.
3. **Universal pipeline** — `QueryPipeline::finish_partial()` → WPP1 bytes → `JsonEncode` re-decode is a legacy shortcut; sink should consume typed partial / row batches on the WCOL edge.

### Phase 3 — Host cutover

- [x] Generic TS host (`run-wcol-engine.ts`): ReadAt / ReadPort / WritePort / Yield only.
- [ ] Rust `ReadAt` from planned sources for all ingest ops (not only ExecutionGroup).
- [ ] Cloudflare worker uses driver; no operator-name dispatch in TS.

### Phase 4 — Deletion (mandatory)

- [x] Remove `wcol-parquet`, `wcol-stream-operators` (sources under `wcol/`).
- [x] Remove `wcol-stream-core`, `wcol-format`, `wcol-sql-parser` (sources under `wcol/`).
- [x] Remove `wcol-decoder`, `wcol-encoder` (sources under `formats/wcol/decode|encode`).
- [ ] Delete `src/runtime/exec`, `query`, `parquet.ts`, stream codecs/reducers in TS (still required by `.wcol` workers + ClickBench scripts).
- [x] Remove deleted ingest driver references — `run-parquet-ingest-stream.ts` gone; legacy `ingest-parquet-stream` uses `runWcolEngine` + `ReadAt`.
- [x] Drop unused TS: `parquet-stream.ts`, `host/engine-host.ts` stub.
- [ ] Remove dead FFI exports and feature-flag dual paths.
- [ ] CI / package.json import only `wcol` artifacts; retire `archive/legacy-ts-stream` re-exports from public API when CF/scripts migrate.

### Execution core — next milestone (post layout consolidation)

One executor, no legacy paths. Track these before new features:

| # | Gap | Target |
|---|-----|--------|
| 1 | Typed colocated scheduler | **Done** — [`ColocatedGraphPlan`](rust/wcol/src/exec/colocated_plan.rs) → [`LinearColocatedGraph`](rust/wcol/src/exec/graph_scheduler.rs); **no WCS between internal stages**; [`GraphHost`](rust/wcol/src/exec/graph_host.rs) is sole `wcs_open` executor; [`Pipeline`](rust/wcol/src/stream/core.rs) test-only transport |
| 2 | `stream::core::Operator` vs `exec::operator::WcolOperator` | **Fail (DoD I-01)** — stop extending op 7–9; delete chain 6→5; converge traits |
| 3 | Byte `Pipeline` + WCS between local stages | **Partial** — default opcode **10**; **deleted** chain 5–6 and stages 7–9; WCS projection edge covered by unit test |
| 4 | Partial state on wire | **Partial** — logical `PartialStateWcol` in-process; WCS edge emits **`PWCL`** ([`partial_wcs.rs`](rust/wcol/src/stream/partial_wcs.rs)); WPP1 legacy decode + oracle normalize |
| 5 | `ParquetDecode::fill_batch` dict copy per batch | Install dict once per scope; reuse batch slots |
| 6 | ~~`NumericScan` peak chunk buffer~~ | **Done** — sliding window + canonical `ParquetDecode` (`streaming_compressed_window` tests) |
| 7 | `JsonEncode` hard-coded `sum_i` | General partial/row sink |

Scaffolding to delete once parity holds: ~~`formats/wcol/decode/query/*` forwarders~~ (**done**), ~~`stream/dictionary` / `stream/wire` delegates~~ (**done** — canonical `stream/wcs`, `stream/projection`, `stream/dictionary`).

**Fixed:** `ExecutionGroup` `EmitPartial::flush()` now sets `partial_emitted` (test in `exec/operators/ingest_query.rs`).

### Phase 5 — Validate

End-to-end: ParquetDecode → Filter → Aggregate → Reduce → Finalize → JsonEncode, colocated + remote WCS1, dictionaries, backpressure, Q3/Q8 vs DuckDB, memory plateau.

## Import convention (now)

New Rust code should depend on **`wcol`** and use module paths:

```rust
use wcol::stream::batch::WcolBatch;
use wcol::exec::{execute, ExecutionGroup};
use wcol::plan::logical::parse_sql_v0;
use wcol::formats::wcol::decode::native::NativeRuntime; // native CLI / bench
use wcol::formats::wcol::encode::convert_to_wcol;      // native convert
```

Do **not** add new dependencies on legacy crate names unless touching unmigrated code.

## Documentation

| Doc | Role |
|-----|------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Single overview (target state) |
| [UNIVERSAL_WCOL_PIPELINE.md](./UNIVERSAL_WCOL_PIPELINE.md) | Operator contract + acceptance |
| [STREAMING_PIPELINE.md](./STREAMING_PIPELINE.md) | Decode / ops / encode |
| [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md) | Budgets |
| [FORMAT.md](./FORMAT.md) | On-disk `.wcol` |

Retire or merge conflicting docs when Phase 4 completes.
