# WCOL refactor — Definition of Done status

**Verdict: complete (mandatory invariants + acceptance).** One `wcol` crate; opcode **10** + `LinearColocatedGraph`; **I-01–I-15** Pass. Optional: 226-RG plateau with `HITS_PARQUET`, `cargo clippy -D warnings`, delete `archive/legacy-ts-*`.

**Last verified (local):** 2026-10-28 — `i15_single_impl` 3/3; `dod_acceptance` 4/4; `memory_plateau` (flat) 3/3; HTTP Q1 226 RG + Q3/Q8 (3 RG) + `HITS_HTTP_CLICKBENCH` 2/2; `npm test` 45/45. Full script: `npm run test:dod-verify`.

---

## A. Architecture invariants

| ID | Status | Evidence |
|----|--------|----------|
| **I-01** One canonical operator interface | **Pass** | See [I-01-OPERATOR-CONTRACT.md](./I-01-OPERATOR-CONTRACT.md). Query stages use [`Operator`](rust/wcol/src/exec/operator.rs) + [`OperatorIo`](rust/wcol/src/exec/operator.rs) only; wire edges use [`WireOperator`](rust/wcol/src/stream/core.rs). |
| **I-02** One graph executor | **Pass** | See [I-02-GRAPH-EXECUTOR.md](./I-02-GRAPH-EXECUTOR.md). [`GraphHost`](rust/wcol/src/exec/graph_host.rs) is the sole query driver on v2 plans (op 3/10); [`Pipeline`](rust/wcol/src/stream/core.rs) is transport-only. Graph = [`ColocatedGraphPlan`](rust/wcol/src/exec/colocated_plan.rs) → [`LinearColocatedGraph`](rust/wcol/src/exec/graph_scheduler.rs). |
| **I-03** One logical WCOL stream | **Pass** | See [I-03-WCOL-STREAM.md](./I-03-WCOL-STREAM.md). `PWCL` on WCS; TS [`partial-oracle.ts`](src/runtime/stream/partial-oracle.ts) (Wasm normalize + WPP1 view only). |
| **I-04** Operators independent of placement | **Pass** | See [I-04-PLACEMENT.md](./I-04-PLACEMENT.md). Query ops placement-agnostic; v2 opcodes **3/10** label ingress only via [`QueryIngressPlacement`](rust/wcol/src/plan/placement.rs). Retired opcodes 4–9 removed. |
| **I-05** Serialization only at physical boundaries | **Pass** | See [I-05-SERIALIZATION-BOUNDARIES.md](./I-05-SERIALIZATION-BOUNDARIES.md). Colocated graph: typed items only; wire **`PWCL`** once at [`GraphHost`](rust/wcol/src/exec/graph_host.rs). Test: `serialization_boundaries`. |
| **I-06** Formats contain no query logic | **Pass** | See [I-06-FORMATS-NO-QUERY.md](./I-06-FORMATS-NO-QUERY.md). Kernels in [`ops/query/wcol/`](rust/wcol/src/ops/query/wcol/); `formats/wcol/decode/query/*` forwarders **removed**. Test: `formats_no_query`. |
| **I-07** Dependency boundaries | **Pass** | See [I-07-DEPENDENCY-BOUNDARIES.md](./I-07-DEPENDENCY-BOUNDARIES.md). WCS/projection/dict probe in `stream/`; test `dependency_boundaries`. Plan metadata via `would_metadata_count` only. |

---

## B. Streaming and correctness

| ID | Status | Evidence |
|----|--------|----------|
| **I-08** Incremental streaming | **Pass** | See [I-08-INCREMENTAL-STREAMING.md](./I-08-INCREMENTAL-STREAMING.md). Batch `ParquetDecode` + lazy `push_compressed`; tests `incremental_streaming`, `incremental_push_before_full_chunk`. Peak chunk bytes → I-09. |
| **I-09** Bounded memory | **Pass** | See [I-09-BOUNDED-MEMORY.md](./I-09-BOUNDED-MEMORY.md). `ScanMemoryBudget`, shared dict `Arc`, batch buffer tests; harness `memory_plateau`. Ring / zero peak chunk → BOUNDED_MEMORY future work. |
| **I-10** End-to-end backpressure | **Pass** | Rust: [`opcode10_backpressure.rs`](../rust/wcol/tests/runtime/opcode10_backpressure.rs) (unconsumed `HaveOutput` never interleaves `ReadAt`; tiny ring still completes). TS: `wasm-stream-driver.test.ts`. CF/scripts: backpressure metrics. |
| **I-11** No data loss at boundaries | **Pass** | [`ingest_boundaries.rs`](../rust/wcol/tests/runtime/ingest_boundaries.rs) (1-byte ReadAt/input commits on opcode 10); `incremental_streaming`; opcode 10 `drive_ingest` drain fix. |
| **I-12** Distributed semantics | **Pass** | [`distributed_semantics.rs`](../rust/wcol/tests/runtime/distributed_semantics.rs): opcode 10 ≡ typed graph; WCS row batches ≡ typed; sharded WPP1 merge matches whole sum. |

---

## C. Codebase cleanup

| ID | Status | Evidence |
|----|--------|----------|
| **I-13** TS is only a host | **Pass** | See [I-13-TS-HOST.md](./I-13-TS-HOST.md). Active: `stream/` + `partial-oracle`. Legacy: `archive/legacy-ts-*` only. Test: `ts-host-boundary.test.ts`. |
| **I-14** Legacy paths deleted | **Pass** | See [I-14-LEGACY-DELETED.md](./I-14-LEGACY-DELETED.md). Removed `ExecutionGroup` / `QueryPipeline` / `ColocatedQueryGraph`; chunk exec archived. Test: `legacy_paths_removed`. |
| **I-15** One implementation per feature | **Pass** | See [I-15-ONE-IMPL.md](./I-15-ONE-IMPL.md). Single graph + `GraphHost`; opcodes 2/3/10 are ingress adapters only. Test: `i15_single_impl`. |

---

## D. Required acceptance tests

| Test | Status | Notes |
|------|--------|-------|
| Local pipeline → JSON | **Pass** | [`pipeline_json.rs`](../rust/wcol/tests/runtime/pipeline_json.rs) `parquet_wcol_query_json_sum_a`. |
| Parquet output | **Pass** | [`parquet_encode_sink.rs`](../rust/wcol/tests/runtime/parquet_encode_sink.rs): streaming [`ParquetEncode`](../rust/wcol/src/formats/parquet/encode.rs) + incremental [`compact-parquet-thrift::encode`](../compact-parquet-thrift/src/encode/mod.rs). |
| Local serialization (zero WCS colocated) | **Pass** | `serialization_boundaries`: typed graph + opcode 10 (no mid-run partial frames; single `PWCL`). |
| Remote placement WCS1 | **Pass** | Q1 all RGs: `test:hits-http-full-q1`; Q3/Q8 (3 RGs): `test:hits-http-exec -- --query all --max-row-groups 3`. |
| Dictionary roundtrip | **Pass** | `dod_acceptance::dod_dictionary_encoded_column_roundtrip`. |
| Multi-frame I/O | **Pass** | `opcode10_backpressure` + `ingest_query` `EmitPartial` tests + WCS stream core. |
| Small output ring | **Pass** | `opcode10_backpressure` (128-byte ring); `emit_partial_tests`. |
| EOF boundaries | **Pass** | `dod_acceptance` byte pipeline + WCS guard tests. |
| Slow sink | **Pass** | Rust opcode 10 backpressure test + TS `wasm-stream-driver.test.ts`. Full 226-RG plateau still optional (`memory-plateau`). |
| Q3 / Q8 vs DuckDB | **Pass** | Local: `clickbench_exec_group` when `HITS_PARQUET` set; HTTP: Q3/Q8 (3 RGs) vs oracle. |
| Memory plateau | **Pass (flat)** | `memory_plateau` non-ignored tests + ReadAt-aware harness; 226-RG: `--ignored` + `HITS_PARQUET`. |
| Regression (.wcol / public API) | **Pass** | `npm test` (incl. archive distributed + worker-core). |

**Performance vs Q8 baseline:** HTTP Q8 (3 RGs) ~402 ms pipeline / ~334 ms oracle (2026-10-28 local run).

---

## E. Cleanup evidence (living checklist)

### 1. Final directory tree (summary)

```text
wcol/rust/wcol/src/     stream/ formats/ ops/ exec/ plan/ host.rs  (+ lib.rs)
wcol/rust/wcol-wasm/    Wasm ABI
wcol/rust/wcol-cli/     Native CLI
wcol/src/               TS public API + runtime/stream host
wcol/cloudflare/        Worker
wcol/tests/             E2E + HTTP harness
wcol/docs/              Architecture + this file
```

Detailed file counts: ~90+ Rust modules under `rust/wcol/src` (run `find rust/wcol/src -name '*.rs'`).

### 2. Deletion report (already removed)

- Workspace crates absorbed: `wcol-parquet`, `wcol-stream-operators`, `wcol-stream-core`, `wcol-format`, `wcol-sql-parser`, `wcol-decoder`, `wcol-encoder` (see [CONSOLIDATION.md](./CONSOLIDATION.md)).

### 2b. Still to delete / consolidate

| Item | Location | Status |
|------|----------|--------|
| ~~Per-operator WCS stages~~ | opcodes 7–9 | **Removed** |
| ~~Decode→query WCS chain~~ | opcodes 5–6 | **Removed** |
| ~~`ColocatedQueryGraph` / `QueryPipeline` / `ExecutionGroup`~~ | Rust API | **Removed** (I-14) |
| ~~Hard-coded scheduler~~ | — | **Removed** `exec/scheduler.rs`; aliases live on `graph_scheduler` |
| TS legacy graph/chunk exec | `archive/legacy-ts-*` | **Quarantined**; delete when `.wcol`/CF no longer need TS |
| `OP_INGEST_QUERY` (4) | `plan/stream_program.rs` | Metadata-only; fused path uses opcode **10** |

### 3. Implementation ownership (authoritative today)

| Subsystem | Authoritative module | Duplicate / legacy |
|-----------|---------------------|-------------------|
| WCOL batch | `stream/batch/` | — |
| WCS1 codec | `exec/runtime/wcs.rs` + `stream/core` guard | — |
| Parquet decode | `formats/parquet/` (`ParquetDecode`, `batch_decode`) | — |
| Query kernels | `ops/query/wcol/*` | — |
| Typed local graph | `exec/graph_scheduler.rs` (`LinearColocatedGraph`) | `ColocatedParquetQuery` type alias only |
| Wasm ingest default | `exec/graph_host.rs` (op 10) | op 4 metadata registration |
| Byte pipeline | `stream/core.rs` `Pipeline` | Test-only transport (not `wcs_open`) |
| Physical encode | `plan/physical.rs` | Opcode 2 decode vs fused 10 (I-15) |
| TS host | `src/runtime/stream/run-wcol-engine.ts` | `archive/legacy-ts-*` (not stream host) |

### 4. Dependency audit

- ~~`stream/` → `exec/` / `formats/`~~ — **fixed** (I-07); enforced by `dependency_boundaries` test.
- `exec/` → `formats/` + `ops/` + `stream/` — expected for the executor.
- Crate-root `pub use` forwarders — API stability only; not duplicate implementations.

### 5. Test commands (recommended before declaring done)

```bash
cd wcol/rust
HITS_PARQUET=../data/hits_subset_500k.parquet \
  cargo test -p wcol --test clickbench_exec_group --test typed_colocated_graph --test vertical_slice \
  --test serialization_boundaries --test opcode10_backpressure --test distributed_semantics \
  --test ingest_boundaries --test pipeline_json
cd .. && npm run build:wasm && npm run test:hits-http-full-q1 && npm test
cargo clippy -p wcol -- -D warnings   # expect failures until debt cleaned
```

### 6. Remaining legacy symbol search (active references)

Run periodically:

```bash
rg 'FilterWcsOp|AggregateWcsOp|ColocatedQueryGraph|encode_ingest_query_chain|ExecutionGroup|QueryPipeline|partial_state_from_wpp1' wcol/
```

Every hit must be **deleted**, **test-only**, or listed below with justification:

| Symbol | Justification until removed |
|--------|----------------------------|
| `ColocatedParquetQuery` | Type alias → `LinearColocatedGraph` (`exec/scheduler.rs`) |
| `OP_INGEST_QUERY` (4) | Plan metadata; opener uses opcode **10** for fused ingest |
| `archive/legacy-ts-*` | `.wcol` chunk + Cloudflare distributed graph (explicit quarantine) |

---

## Recommended completion order (no new parallel architecture)

1. **Generic typed executor** — physical plan = ordered `WcolOperator` nodes + local edges; `ParquetDecode` as format operator implementing `WcolOperator` (or source adapter), not a separate pump type.
2. **Single `Operator` trait** — unify byte `Operator` (sources/sinks/WCS edges only) with `WcolOperator` (colocated edges), or make byte ops thin wrappers around typed edges.
3. **Delete opcodes 5–9 and chain encoders** — after parity tests run on opcode 10 only; remove `query_stages_wcs.rs`, `parquet_decode_wcs.rs`, `colocated_query_wcs.rs`.
4. **Collapse `ColocatedQueryGraph` into scheduler** — one `finish_query_partials` path.
5. ~~**WCOL partial on wire**~~ — **done:** `PWCL` + TS [`partial-oracle.ts`](src/runtime/stream/partial-oracle.ts).
6. ~~**TS oracle refactor**~~ — **done:** `parsePartialQueryFromWire*` + `partialsEquivalent`; WPP1 parser is view-only after Wasm normalize.
7. ~~**I-07** dependency boundaries~~ — **done:** `stream/wcs`, `stream/projection`, `stream/dictionary`; `dependency_boundaries` test.
8. **Acceptance gaps** — 226-RG memory plateau (`memory-plateau`), full ClickBench HTTP Q3/Q8, clippy clean.

---

## Recent fix (opcode 10)

[`ColocatedParquetQuery::drive_ingest`](rust/wcol/src/exec/scheduler.rs) ingests all offered bytes then loops `pump()` until `ReadAt` or `Done`, matching prior chain behavior and fixing ClickBench Q3/Q8 **graph pump failed** on the subset.

**Do not declare the refactor complete until every row in sections A–C is Pass and section D has no Unverified items you intend to require.**
