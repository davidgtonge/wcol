# Rust-first execution (non-negotiable boundary)

**Status:** Target architecture (authoritative)  
**Supersedes:** “JS orchestrates, Rust kernels” as a *product* model. Kernels-only is not sufficient.  
**Companion:** [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md) (streaming primitives, WCS1, invariants I-01–I-16)

## Actual target

**Rust / Wasm owns everything about execution:**

- SQL planner, job generation, operator graph, **scheduler**
- Parquet ingest, WCOL scan, filter, project, aggregate, shuffle, reduce, finalize
- Rust-owned buffers, framing, backpressure, operator state, fan-in / fan-out readiness

**TypeScript owns platform I/O only:**

- `fetch`, R2 bindings, DO RPC, Web Streams, storage, lifecycle, cancellation, metrics export

Rust owns the **scheduler**, not just the operators. TypeScript must not know which operator is running inside a colocated execution group.

## Host interface

The Cloudflare (or local) host loop should reduce to this shape. All query-specific branching lives in Rust.

```typescript
for (;;) {
  const action = wasm.nextAction(handle);

  if (action.kind === "done") break;

  const result = await hostIo.perform(action, wasm.memory);

  wasm.complete(
    handle,
    action.ticket,
    result.status,
    result.byteCount,
  );
}
```

`hostIo.perform()` satisfies external effects and transport. It **never** parses WCOL payloads, runs SQL, decodes projections, hashes partition keys, or merges partial aggregates.

### Host effects (conceptual)

| Effect | Host duty | Rust duty |
| --- | --- | --- |
| **ReadAt** | HTTP/R2 byte range → commit into Wasm scratch | Parquet page/chunk assembly, decode |
| **ReadPort** | Pull next byte chunk from inbound transport → commit input ring | Validate WCS1, run downstream operators |
| **WritePort** | Copy acknowledged slice to transport; wait for backpressure | Produce bounded output; hold until ack |
| **Spawn** | Start worker / DO / port per **placement plan** | Same compiled graph; different edges |
| **Persist** | Optional object store for checkpoints / large artifacts | Serialize Rust state only |
| **Yield** | Schedule continuation (microtask / IO wait) | Bounded poll quantum (I-13) |

Colocated operators run inside **one Rust scheduler** with **zero TypeScript calls between them**. Scratch vs edge ownership and fusion without WCS on local hops: [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md).

## Invariants (enforced at the boundary)

| Invariant | Rule |
| --- | --- |
| **One Rust engine** | Rust owns planning, scheduling, operators, framing, and all computation |
| **No Parquet SQL path** | Parquet emits canonical WCOL batches only (I-01) |
| **No JS columnar work** | No JS row loops, projection decode, hash partition, or aggregation |
| **Placement independence** | Same logical graph; moving operators between DOs changes transport only (I-02) |
| **Bounded memory** | Fixed scratch, explicit operator-state budgets, stable decoder memory (I-04, I-05); [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md) |
| **End-to-end backpressure** | Blocked sink eventually stops upstream reads (I-07) |
| **Ownership** | Rust scratch not reused until host ack (`commit_input` / `consume_output`) (I-06) |
| **Rust-controlled routing** | Fan-in/fan-out and producer readiness decided in Rust, not in `runStreamNode()` |
| **Failure visibility** | No silent truncation, lost partitions, or infinite wait after DO restart (I-14) |

Normative detail: [STREAMING_EXECUTION.md §8](./STREAMING_EXECUTION.md#8-normative-invariants).

## Rust components (one Wasm module)

Compile into a **single** Wasm artifact so colocated operators share one linear memory (no cross-module copies).

| Crate | Responsibility |
| --- | --- |
| **`wcol-planner`** | Logical graph, SQL, footer/task generation, physical execution groups, placement plan, budgets |
| **`wcol-stream`** | `Operator` trait, scheduler, [`EdgeBuffer`](./COLOCATED_MEMORY.md) per local edge, `ScratchPool`, WCS1 on remote edges, backpressure, host action queue |
| **`wcol-operators`** | Parquet ingest, WCOL scan, filter, project, shuffle, aggregate, reduce, finalize |

Today’s repo maps partially: `wcol-stream-core`, `wcol-stream-operators`, `wcol-parquet` (legacy fused engine), `wcol-wasm` (.wcol queries). Consolidation and renaming follow this table.

### Acceptance tests

1. **New operator, zero TS changes:** add a Rust operator and register it in the planner; host loop unchanged.
2. **1 DO vs 32 DOs:** identical results and semantics; only placement plan and `Spawn` / port wiring differ.
3. **Host ignorance:** TypeScript bundle contains no imports of `codec.ts` / `columnar.ts` / `shuffle.ts` on the hot path.

## What leaves TypeScript

These are **execution** responsibilities to remove from the product path (keep thin transport adapters only):

| Area | Today | Target |
| --- | --- | --- |
| Node dispatch | `runStreamNode()` op switch (~800 lines) | Deleted; one `EngineHost` loop |
| Framing / batches | `codec.ts`, `columnar.ts` | Rust WCS1 + typed batches |
| Shuffle / partition | `shuffle.ts`, partition checks in `count` | Rust hash partition operator |
| Partial merge | `reducer.ts`, `merge-parquet` in TS | Rust reduce/finalize |
| Graph semantics | `validate.ts`, `types.ts` operator matrix | Planner output validated in Rust |
| Parquet SQL | `scan-parquet-fold`, `parquet-engine.ts`, `engine.rs` | Ingest-only Parquet → WCOL |
| Ingest “success” metrics | `ingest-parquet-stream` decoding projection in TS to count rows | Rust emits row counts / partials in host-visible metadata only |

**Keep:** `byte-source.ts` (range I/O), DO worker lifecycle, `executeGraph` **coordination** until replaced by Rust `Spawn` + status RPC (coordination stays I/O; not operator logic).

## Current violations (honest, 2026-03)

The codebase still treats TypeScript as the execution engine. Examples:

- **`runStreamNode()`** — decodes WCOL, sums columns, applies transforms, routes Parquet/SQL/wcol paths per `node.op`.
- **`ingest-parquet-stream`** — Rust emits WCS1, then TS `FrameDecoder` + `decodeProjection()` only to increment `rows` / forward frames.
- **`scan-parquet-fold` / `wcol-parquet` engine** — fused Parquet + SQL (violates I-01).
- **Graph layer** — TS knows operator names and stream kinds (`validate.ts`, `buildQueryGraph` in TS).

Recent streaming work (`wcs_*`, `ParquetIngestOp`, `ReadAt` in ABI) is **foundational** but not yet a Rust scheduler or a host that ignores operator types.

## Migration sequence

1. **Host loop + actions** — `nextAction` / `complete` / tickets; map existing `wcs_poll` to a strict superset; implement `hostIo.perform` in TS only.
2. **Single-group executor** — planner emits one colocated program; Rust scheduler pumps local edges; TS only ReadAt / WritePort on external edges.
3. **Replace `runStreamNode` for one vertical slice** — **done (colocated ops):** `ExecutionGroup` runs **ParquetDecode → Filter (`SelectionMask`) → Aggregate → Emit** in one scheduler; opcode `4` plan unchanged; `IngestQueryOp` is an alias. Tests: `vertical_slice`, `colocated_ops` (incl. `WHERE a > 5`). Q8/Q3 ClickBench, remote WCS adapters, and `engine-host` still TODO.
4. **Planner-owned placement** — same bytecode, `Spawn` N DOs; retire TS operator matrix for that query class.
5. **Delete legacy paths** — `scan-parquet-fold`, TS reducer/shuffle on hot path, per-op Wasm loads in worker.

## Related docs

- [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md) — WCS1, Parquet ingest rules, memory, I-01–I-16
- [ARCHITECTURE.md](./ARCHITECTURE.md) — stack overview (updated for this boundary)
- [archive/docs/OPERATORS.md](../archive/docs/OPERATORS.md) — archived TS graph operators
