# `wcol-engine` source layout

This crate is the **columnar execution engine** for WCOL. SQL parsing, logical planning, and DataFusion live in **`wcol-plan`**. Everything under `src/` runs **owned IR**, **wire programs** (node/coordinator fragments, PWCL partial state, WCS1 byte streams), and **host I/O**—not SQL text.

Public surface is declared in [`lib.rs`](lib.rs): `error`, `exec`, `formats`, `ir`, `ops`, `program`, `spill`, and `stream`.

---

## How the pieces connect

```text
wcol-plan (outside this crate)
    │  physical plan + fragments
    ▼
program/          encode/decode fragments, plan types, ingest groups
    │  to_ir.rs → ir/
    ▼
exec/             GraphHost, scheduler, fetch, host I/O rings, Wasm FFI
    │  colocated LinearColocatedGraph of ops/
    ▼
ops/              filter, aggregate, reduce, exchange, join, …
    │  batch kernels in ops/query/batch/
    ▼
stream/           OwnedWcolBatch, partial state, WCS1 framing
    ▲
formats/parquet/  Parquet column chunks → WcolBatch (no query logic)

spill/            out-of-core partial groups (SQLite / R2 / file), when enabled
```

**Production path:**

```text
TypeScript host
    → wcs_sp_* (stream pool FFI)
    → StreamPoolSession
    → GraphHost::poll()
    → LinearColocatedGraph
    → operators
```

The planner emits a **node fragment**. `colocated_plan` builds the graph. **`GraphHost`** (alias **`WcolSession`**) polls operators and returns **`Action`** (NeedInput, HaveOutput, ReadAt, SpillExec, …). Partial aggregates leave the node as **PWCL** on **WCS `FRAME_PARTIAL`**. A **coordinator fragment** merges/sorts/limits/projects on the host side.

[`exec/stream_pool.rs`](exec/stream_pool.rs) **`StreamPoolSession`** is the only Wasm session. It wraps **`GraphHost`** with up to **100 ingress mailboxes / 100 egress ports**. The Graph driver feeds WCS ingress, drains hash-exchange ports after **`enable_direct_port_egress`**, and surfaces **ReadAt** plus **R2 spill** through [`exec/runtime/stream_pool_ffi.rs`](exec/runtime/stream_pool_ffi.rs) (`wcol/src/runtime/host/stream-pool.ts`). The Echo driver copies port→port for mailbox contract tests.

---

## Top-level files

| File | Role |
|------|------|
| [`lib.rs`](lib.rs) | Crate root; re-exports `GraphHost`, `GraphPump`, ingest helpers, `CatalogColumn`, `OwnedWcolBatch`, `WcolError`. |
| [`error.rs`](error.rs) | Unified `WcolError` / `WcolResult`; last-error slot for FFI. |

---

## `ir/` — execution IR (no SQL, no planner types)

Owned values that **operators and batch kernels** consume. The planner converts physical predicates into these types via [`program/to_ir.rs`](program/to_ir.rs).

| File | Role |
|------|------|
| [`ir/mod.rs`](ir/mod.rs) | `CatalogColumn`, `ColumnKind`, `Scalar`, `Predicate`, `CmpOp`, `AggKind`, `AggExpr`, `SortKey`, `MAX_GROUP_KEYS`. |

---

## `program/` — wire contract (node + coordinator)

No SQL and no DataFusion. Encodes what each **worker** and the **coordinator** need as binary programs.

| File | Role |
|------|------|
| [`program/mod.rs`](program/mod.rs) | Module exports; public types for fragments, ingest, coordinator. |
| [`program/types.rs`](program/types.rs) | Execution-facing plan values on the wire (`FetchRequest`, `LogicalRead`, `NodeId`, …). |
| [`program/codec.rs`](program/codec.rs) | Shared binary codecs for predicates, aggregates, sort keys. |
| [`program/fragment.rs`](program/fragment.rs) | **Node fragment**: row groups, fetches, operator steps for one worker slot. |
| [`program/coordinator.rs`](program/coordinator.rs) | **Coordinator fragment**: post-exchange sort, limit, project, finalize steps. |
| [`program/stream_program.rs`](program/stream_program.rs) | V2 byte-stream operator program (opcodes, cursor, catalog encoding). |
| [`program/ingest.rs`](program/ingest.rs) | Physical stream programs; **ingest groups** for multi-table / grouped decode; `IngestMemoryProfile`. |
| [`program/page_copy.rs`](program/page_copy.rs) | Page-copy compaction shapes (`CopySegment`, merged row-group plans). |
| [`program/to_ir.rs`](program/to_ir.rs) | Fragment / plan fields → **`ir`** for kernels. |

---

## `stream/` — in-memory WCOL + WCS1 wire

Canonical batches, dictionaries, aggregate **partial state**, and **WCS1** frame helpers.

| File / dir | Role |
|------------|------|
| [`stream/mod.rs`](stream/mod.rs) | Re-exports batches, partial WCS, framing constants. |
| [`stream/core.rs`](stream/core.rs) | **`Action`**, **`Progress`**, **`EdgeBuffer`**, WCS1 size constants. |
| [`stream/wcs.rs`](stream/wcs.rs) | WCS1 frame encode/decode (`FRAME_DATA_BATCH`, `FRAME_PARTIAL`, `FRAME_END`, …). |
| [`stream/partial_wcs.rs`](stream/partial_wcs.rs) | PWCL payload inside `FRAME_PARTIAL`. |
| [`stream/partials.rs`](stream/partials.rs) | Typed partial-state columns (`PartialStateWcol`); merge/finalize semantics. |
| [`stream/schema.rs`](stream/schema.rs) | Stream-layer schema helpers. |
| [`stream/projection.rs`](stream/projection.rs) | `WCOLpjv1` projection blobs. |
| [`stream/dictionary.rs`](stream/dictionary.rs) | Dictionary-encoded column helpers. |
| [`stream/native_poll.rs`](stream/native_poll.rs) | Native test harness for exhaustive poll loops (no Wasm spill host). |
| **`stream/batch/`** | |
| [`batch/mod.rs`](stream/batch/mod.rs) | Owned + borrowed batch types. |
| [`batch/wcol_batch.rs`](stream/batch/wcol_batch.rs) | `WcolBatch` / `OwnedWcolBatch` views (source-agnostic). |
| [`batch/owned_batch.rs`](stream/batch/owned_batch.rs) | Owned column storage. |
| [`batch/selection_mask.rs`](stream/batch/selection_mask.rs) | Row mask between filter and downstream ops (no compaction). |

---

## `formats/` — source/sink codecs (no query logic)

| File / dir | Role |
|------------|------|
| [`formats/mod.rs`](formats/mod.rs) | Currently **Parquet** only. |
| **`formats/parquet/`** | Parquet → WCOL decode and related sinks. |
| [`parquet/mod.rs`](formats/parquet/mod.rs) | Crate-local Parquet module; `open_plan`, scan/decode entry points. |
| [`parquet/schema.rs`](formats/parquet/schema.rs) | Footer scan: leaves, chunks, row groups. |
| [`parquet/decode.rs`](formats/parquet/decode.rs) | Demand-driven decode contract (NeedInput / BatchReady). |
| [`parquet/batch_decode.rs`](formats/parquet/batch_decode.rs) | Column chunks → `WcolBatch`. |
| [`parquet/numeric_scan.rs`](formats/parquet/numeric_scan.rs) | Bounded compressed ReadAt → page events → i64 batches. |
| [`parquet/page_cursor.rs`](formats/parquet/page_cursor.rs) | Resumable plain-page decode buffer. |
| [`parquet/page_scratch.rs`](formats/parquet/page_scratch.rs) | Per-scan codec decompress budget. |
| [`parquet/dictionary.rs`](formats/parquet/dictionary.rs) | Bounded dict storage for one column chunk. |
| [`parquet/page_compact.rs`](formats/parquet/page_compact.rs) | Streaming page-copy Parquet compaction (opcode 11 sink). |
| [`parquet/stream_wcs.rs`](formats/parquet/stream_wcs.rs) | WCS1 dictionary payloads for ingest wire. |
| [`parquet/encode.rs`](formats/parquet/encode.rs) | WCOL row stream → incremental Parquet file bytes. |
| [`parquet/ingest_memory.rs`](formats/parquet/ingest_memory.rs) | Memory sampling for ingest plateau tests. |
| [`parquet/replay_metrics.rs`](formats/parquet/replay_metrics.rs) | Skip-replay instrumentation. |
| [`parquet/alloc_track.rs`](formats/parquet/alloc_track.rs) | `pub(crate)` allocator probe (Wasm buffer high-water). |

---

## `ops/` — query operators (WCOL in → WCOL out)

Each stage implements [`exec/operator.rs`](exec/operator.rs) **`Operator`** + **`OperatorIo`**. Row batches and partial batches are both `OwnedWcolBatch` (distinguished by `is_partial_batch`).

| File | Role |
|------|------|
| [`ops/mod.rs`](ops/mod.rs) | Operator exports; exchange and join surface. |
| [`ops/filter.rs`](ops/filter.rs) | Row batch + `SelectionMask` (no column drop). |
| [`ops/project.rs`](ops/project.rs) | Column projection in SELECT order. |
| [`ops/limit.rs`](ops/limit.rs) | OFFSET/LIMIT via mask. |
| [`ops/sort.rs`](ops/sort.rs) | Row sort (float total order, dict resolved values). |
| [`ops/topk.rs`](ops/topk.rs) | Bounded top-k without full sort budget. |
| [`ops/aggregate.rs`](ops/aggregate.rs) | Rows → typed partial-state batch. |
| [`ops/reduce.rs`](ops/reduce.rs) | Merge partial states (same schema). |
| [`ops/finalize.rs`](ops/finalize.rs) | Partials → result rows. |
| [`ops/parquet_source.rs`](ops/parquet_source.rs) | Planned Parquet chunks as graph **source** operator. |

### `ops/query/` — batch kernels + partial algebra

| File / dir | Role |
|------------|------|
| [`ops/query/mod.rs`](ops/query/mod.rs) | `PartialSnapshot`, merge/finalize helpers, validation; ties IR to kernels. |
| [`ops/query/hll.rs`](ops/query/hll.rs) | HyperLogLog for `approx_count_distinct`. |
| [`ops/query/float_key.rs`](ops/query/float_key.rs) | DuckDB-compatible float sort/hash keys. |
| **`ops/query/batch/`** | Column-at-a-time execution over `WcolBatch`. |
| [`batch/mod.rs`](ops/query/batch/mod.rs) | Batch query engine entry. |
| [`batch/profile.rs`](ops/query/batch/profile.rs) | Phase timers and spill-boundary probes. |
| [`batch/filter.rs`](ops/query/batch/filter.rs) | Predicate masks (dict evaluated once per entry). |
| [`batch/mask.rs`](ops/query/batch/mask.rs) | Packed `u32` row bitmasks. |
| [`batch/string_filter.rs`](ops/query/batch/string_filter.rs) | Byte-string predicates. |
| [`batch/string_intern.rs`](ops/query/batch/string_intern.rs) | GROUP BY string identity across batches. |
| [`batch/group_spill.rs`](ops/query/batch/group_spill.rs) | Flush-and-reset spill session for large GROUP BY. |
| [`batch/finish_stream.rs`](ops/query/batch/finish_stream.rs) | Emit partial chunks without holding all groups. |

### `ops/exchange/` — shuffle / partition

| File | Role |
|------|------|
| [`ops/exchange/mod.rs`](ops/exchange/mod.rs) | Exchange exports. |
| [`ops/exchange/partitioning.rs`](ops/exchange/partitioning.rs) | Physical partitioning metadata. |
| [`ops/exchange/hash_partition.rs`](ops/exchange/hash_partition.rs) | Hash-partition row batches to multiple output ports. |
| [`ops/exchange/typed_hash.rs`](ops/exchange/typed_hash.rs) | Versioned hashing on logical values (not local dict ids). |

### `ops/join/`

| File | Role |
|------|------|
| [`ops/join/mod.rs`](ops/join/mod.rs) | Re-exports `HashJoinOp` / plan types. |
| [`ops/join/hash_join.rs`](ops/join/hash_join.rs) | Streaming hash equijoin (build → probe). |

---

## `spill/` — durable partial groups

When in-memory aggregation exceeds budget, partial groups flush to **sorted runs** and merge in key order. Host backends: **SQLite** (Durable Object), **R2**, or **file** (native tests).

| File | Role |
|------|------|
| [`spill/mod.rs`](spill/mod.rs) | Public spill API and store traits. |
| [`spill/canonical.rs`](spill/canonical.rs) | Stable group key bytes across batches/dicts. |
| [`spill/encode.rs`](spill/encode.rs) | Partial group payload codec (stats; key in `key_blob`). |
| [`spill/eligibility.rs`](spill/eligibility.rs) | Planner/runtime checks for spill-enabled queries. |
| [`spill/memory.rs`](spill/memory.rs) | Resident memory ledger / `SpillConfig`. |
| [`spill/sorted_run.rs`](spill/sorted_run.rs) | Immutable sorted run format (key + partial). |
| [`spill/store.rs`](spill/store.rs) | Append-only spill table I/O; keyset scan pages. |
| [`spill/host.rs`](spill/host.rs) | SQLite spill via host round-trips (`HostSpillBridge`). |
| [`spill/r2/`](spill/r2/mod.rs) | R2 sorted-run store: `codec` (run-open payloads), `put` (upload/commit), `merge` (ordered scan). |
| [`spill/memory_sorted_store.rs`](spill/memory_sorted_store.rs) | In-memory runs for tests. |
| [`spill/discard.rs`](spill/discard.rs) | Drop runs (prove memory bounds without storage). |

---

## `exec/` — scheduling, graph driver, host session

| File | Role |
|------|------|
| [`exec/mod.rs`](exec/mod.rs) | Submodule tree and public re-exports. |
| [`exec/operator.rs`](exec/operator.rs) | **`Operator`** trait, `WcolStreamItem`, `OperatorIo` lifecycle (I-01). |
| [`exec/graph_scheduler.rs`](exec/graph_scheduler.rs) | **`LinearColocatedGraph`**, **`GraphPump`**, drives source + colocated chain. |
| [`exec/colocated_plan.rs`](exec/colocated_plan.rs) | **`NodeFragment`** → concrete operator graph. |
| [`exec/graph_host.rs`](exec/graph_host.rs) | Production driver: graph + host rings. **`enable_direct_port_egress`** leaves hash output queued for the stream pool to drain per port. |
| [`exec/fetch_stage.rs`](exec/fetch_stage.rs) | Plan **`FetchRequest`** → host ReadAt tickets → decode slices. |
| [`exec/phase.rs`](exec/phase.rs) | One-row phase breadcrumbs for host tracing. |
| [`exec/trace.rs`](exec/trace.rs) | Synchronous reducer trace lines. |
| [`exec/wcol_session.rs`](exec/wcol_session.rs) | `WcolSession` = **`GraphHost`**; `open_wcol_plan` decodes one **`PLAN_V2`** op (node, coordinator, page compact, partial reduce, finalize, hash reducer). |
| [`exec/stream_pool.rs`](exec/stream_pool.rs) | **`StreamPoolSession`**: Echo or Graph driver; `step` status flags (`STEP_SPILL_EXEC`, …); ReadAt + spill side channel for Graph mode. |
| [`exec/wcs_port.rs`](exec/wcs_port.rs) | `pub(crate)` per-edge WCS decoder (lazy ingress, dict remap, FRAME_END vs EOF). |

### `exec/operators/` — WCS helpers for GraphHost

| File | Role |
|------|------|
| [`exec/operators/mod.rs`](exec/operators/mod.rs) | WCS ingress/egress and projection helpers. |
| [`exec/operators/wcs_frame.rs`](exec/operators/wcs_frame.rs) | Incremental WCS1 parse/encode for query stages. |
| [`exec/operators/wcs_projection.rs`](exec/operators/wcs_projection.rs) | Projection payloads ↔ `OwnedWcolBatch`. |
| [`exec/operators/wcs_row_stream.rs`](exec/operators/wcs_row_stream.rs) | Row batch WCS egress/ingress for hash exchange. |
| [`exec/operators/ingest_query.rs`](exec/operators/ingest_query.rs) | Partial emission for typed ingest+query operators. |

### `exec/runtime/` — Wasm ABI + colocated driver glue

| File / dir | Role |
|------------|------|
| [`exec/runtime/mod.rs`](exec/runtime/mod.rs) | Re-exports WCS helpers and FFI modules. |
| [`exec/runtime/stream_io.rs`](exec/runtime/stream_io.rs) | Re-export operator I/O from `exec/operator`. |
| [`exec/runtime/ffi.rs`](exec/runtime/ffi.rs) | Stateless Wasm helpers (alloc, partials, spill diagnostics) and the symbol table that keeps `wcs_sp_*`. |
| [`exec/runtime/stream_pool_ffi.rs`](exec/runtime/stream_pool_ffi.rs) | Wasm FFI: `wcs_sp_*` mailboxes, ReadAt completion, spill append/upload/complete. |
| **`exec/runtime/wcol_stream/`** | |
| [`wcol_stream/mod.rs`](exec/runtime/wcol_stream/mod.rs) | Colocated driver + WCS edge helpers. |
| [`wcol_stream/drive.rs`](exec/runtime/wcol_stream/drive.rs) | Multi-operator driver (in-process batches, no WCS). |
| [`wcol_stream/pwcl_edge.rs`](exec/runtime/wcol_stream/pwcl_edge.rs) | Encode PWCL into `FRAME_PARTIAL`. |
| [`wcol_stream/partial_batch.rs`](exec/runtime/wcol_stream/partial_batch.rs) | Attach partial columns to `OwnedWcolBatch`. |

---

## Related docs (outside `src/`)

- `wcol/docs/COLOCATED_MEMORY.md` — fused operators and edge materialization.
- `wcol/docs/BOUNDED_MEMORY.md` — decode/spill memory budgets (referenced from Parquet modules).
- `wcol/docs/R2_GROUP_SPILL.md` — R2 sorted-run spill from the stream-pool side channel.

---

## Quick “where do I look?”

| Task | Start here |
|------|------------|
| Add a SQL-visible aggregate | `ir/`, `program/codec.rs`, `ops/query/batch/`, `ops/aggregate.rs` |
| Change node wire program | `program/fragment.rs`, `exec/colocated_plan.rs` |
| Host ReadAt / ports / spill | `exec/graph_host.rs`, `exec/fetch_stage.rs`, `exec/stream_pool.rs` |
| Multi-port DO / worker mailboxes | `exec/stream_pool.rs`, `exec/runtime/stream_pool_ffi.rs`, `wcol/src/runtime/host/stream-pool.ts` |
| WCS1 framing | `stream/wcs.rs`, `exec/operators/wcs_frame.rs` |
| Parquet decode only | `formats/parquet/` |
| GROUP BY spill | `ops/query/batch/group_spill.rs`, `spill/` |
| Hash join operator | `ops/join/hash_join.rs`, `exec/colocated_plan.rs` |
| Wasm exports | `exec/runtime/ffi.rs`, `exec/runtime/stream_pool_ffi.rs` |
