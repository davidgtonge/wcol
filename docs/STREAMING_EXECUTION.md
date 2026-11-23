# wcol Streaming Execution Architecture

**Status:** Streaming layer spec (companion to [RUST_FIRST_EXECUTION.md](./RUST_FIRST_EXECUTION.md))  
**Scope:** Rust/Wasm execution, Parquet ingestion, WCOL Stream, graph planning and Cloudflare/local placement  
**Principle:** One logical streaming engine; placement determines transport, not computation.

The **host** must not interpret column data or dispatch operators in TypeScript. That boundary is defined in [RUST_FIRST_EXECUTION.md](./RUST_FIRST_EXECUTION.md).

## 1. Purpose

wcol is a Rust streaming dataflow engine hosted by a thin TypeScript I/O runtime. A Rust planner turns a query and source metadata into a graph of jobs. The same streaming primitives run inside one Wasm instance, inside one Durable Object (DO), or across many DOs.

An example logical pipeline is:

```text
R2 / HTTP / S3
     |
     v
ReadAt -> Parquet decode -> WCOL batches -> Scan/filter/project
                                              |
                                              v
                                       Local aggregate
                                              |
                                              v
                                    Merge/reduce -> Finalize
                                              |
                                              v
                                      Result or WCOL Stream
```

**There is one query engine.** Parquet is an input format; WCOL is the canonical columnar stream and query-operator interface. We must not create a separate Parquet-aware SQL engine.

The planner is free to colocate adjacent operators. A local edge uses Rust-owned buffers; a remote edge carries WCOL Stream bytes. Both must implement the same logical stream semantics and return the same results.

## 2. Terminology and boundaries

| Term | Meaning |
| --- | --- |
| **Operator** | A stateful Rust primitive that consumes and/or produces a stream: decode, filter, project, aggregate, hash-partition, merge, finalize. |
| **Logical graph** | Operators and typed streams connecting them; independent of deployment. |
| **Task** | A finite work assignment, e.g. specific Parquet row groups and projected columns, with a stable ID. |
| **Execution group** | One or more adjacent operators colocated in one Rust/Wasm executor. |
| **Placement** | Assignment of execution groups to local workers or DO instances, plus local/remote edges. |
| **Local edge** | A bounded [`EdgeBuffer`](./COLOCATED_MEMORY.md) between colocated operators (or fused in-process handoff with no edge); no TypeScript data handling. |
| **Remote edge** | A bounded byte stream carrying WCOL Stream (WCS1) frames through a host transport. |
| **Host** | Small TypeScript layer providing asynchronous I/O, network streams, node lifecycle and cancellation. |

**Rust owns:** SQL/schema interpretation, footer and page parsing, query planning, physical task generation, columnar decoding, WCOL framing, operators, hash partitioning, aggregation/reduction, operator state, scratch allocation and accounting.

**TypeScript owns:** HTTP/R2/S3 range requests, stream plumbing, DO/RPC or Web Worker activation, asynchronous waits, cancellation, deadlines, metrics export and driving Rust's synchronous state machine. TS may inspect port/action metadata but **never interprets column values, executes SQL, or allocates per-row objects**.

## 3. Planning and placement

Planning has three explicit products:

1. **Logical query graph:** typed operators and edges, including relational semantics and any shuffle/merge boundaries.
2. **Work plan:** projected columns, Parquet row-group/page tasks, stable IDs, source validators, and estimated bytes/rows/state.
3. **Physical placement plan:** execution groups, worker/DO identities, port connections, resource budgets and retry policy.

The planner reads source metadata (including Parquet footers), parses SQL, performs column pruning and safe metadata-based row-group pruning, validates the graph, and emits a serialized plan. The host instantiates that plan; it does not assemble query-specific operator logic.

For example, the *same* logical graph:

```text
Read -> Parquet -> Filter -> Aggregate -> Merge -> Finalize
```

can be placed as:

```text
ONE DO:  [Read, Parquet, Filter, Aggregate, Merge, Finalize]

MANY DOs:
  DO A: [Read, Parquet] -> WCS1 -> DO B: [Filter, Aggregate]
                                     |
                                     v
                                  WCS1 -> DO C: [Merge, Finalize]
```

A fused execution group may pass borrowed columnar batches directly between Rust operators instead of serializing them. That is a *physical optimization*: the observable schema, row ordering where specified, null semantics, final values and stream completion remain unchanged.

A query that can be answered entirely from metadata, such as an unfiltered `COUNT(*)` over valid row-group row counts, may have no data-decoding tasks.

## 4. The universal streaming primitive

An ordinary single-input, single-output operator has a synchronous Rust interface:

```rust
pub trait Operator {
    fn poll(
        &mut self,
        input: &[u8],
        eof: bool,
        output: &mut [u8],
    ) -> Result<Progress, OperatorError>;
}

pub struct Progress {
    pub consumed: usize,
    pub produced: usize,
    pub finished: bool,
}
```

The input and output are **borrowed, bounded scratch slices**. An operator may consume/produce only part of either. Its persistent state lives in Rust. Neither slice may be retained after `poll()` returns. A correctly implemented operator never needs to allocate one buffer or object per row.

The execution-group runtime extends this to source effects and multiport operators (shuffle, fan-in reducers):

```rust
enum Action {
    NeedInput { port: u32 },
    ReadAt { source: u32, offset: u64, len: u32 },
    HaveOutput { port: u32, ptr: u32, len: u32 },
    Yield,
    Finished,
    Failed { code: u32 },
}
```

`ReadAt` is intentionally different from `NeedInput`: Parquet requires metadata-driven random byte ranges, whereas downstream WCOL operators consume ordinary sequential streams. The executor owns a fixed set of input/output/scratch buffers **per active port or local edge**, not a single global buffer regardless of fan-in/fan-out. Buffers may be reused or ownership transferred within Rust.

On `NeedInput` or `ReadAt`, TS performs asynchronous I/O and commits bytes into the provided Wasm scratch area. On `HaveOutput`, TS transfers/copies a bounded output chunk to transport-owned memory, waits for the downstream write, and acknowledges consumption. Rust does not reuse output storage until acknowledgment. No Wasm pointer or typed-array view is retained across an asynchronous host operation.

For colocated operators, the **Rust scheduler** pumps local edges until external I/O, backpressure, failure or a cooperative yield is required. TS must not mediate individual local operator transitions.

## 5. Data and wire format

**Canonical semantic representation:** a row-aligned columnar batch with schema, physical/logical types, row count, validity information, column buffers and dictionary scope. Use lossless numeric semantics (including 64-bit integers), well-defined null handling and stable group-key equality.

**Remote representation:** existing versioned WCOL Stream protocol (WCS1). Its 32-byte header carries frame type, payload length, row count, stream/source identity, sequence and attempt. Relevant frame kinds are `SCHEMA`, `DICTIONARY`, `DATA_BATCH`, `PARTIAL_RESULT`, `END` and `ERROR`. Checkpoint barriers remain reserved.

A remote transport is a **byte stream, not a frame stream**. Network chunks may divide a frame anywhere. Rust incrementally validates and consumes WCS1 headers/payloads; it must not require a whole frame to fit in scratch memory. The existing 32 MiB maximum payload is a *protocol acceptance limit*, not permission to allocate a 32 MiB input buffer on every node. Decompressed sizes also need explicit limits.

A local typed-batch edge may avoid encode/decode and copies, but must be observationally equivalent to crossing a WCS1 edge. The placement optimizer may choose either without changing query code.

## 6. Parquet ingestion

The Parquet primitive does **not** execute SQL. It receives a task describing source identity, selected row groups/columns and allowed byte ranges, then emits canonical WCOL batches.

It uses the existing allocation-free Parquet/Thrift/page primitives, reusable bounded codec scratch, incremental decompression and row alignment across independently paged columns. It must never create page-sized `Vec<Cell>` collections or copy a decoded column into a JavaScript per-value representation just to feed the query engine.

Metadata-driven scanning requires a `ReadAt` effect. A reader validates HTTP range responses and source identity (ETag/version where available). The reader requests pages or other bounded segments; it must not require a whole compressed column chunk to fit in memory. For multi-column batches, bounded alignment buffers are permitted and accounted for.

Parquet decoding may preserve compatible dictionary/RLE representations, or emit canonical decoded columns. Encoded passthrough is an optimization, never a requirement for correctness. The query operator must not know whether the batch originated from Parquet, a `.wcol` file, or another DO.

## 7. Backpressure, ownership and memory

Backpressure is **end-to-end**: if the final sink stops accepting bytes, each upstream stage eventually stops producing, and eventually the Parquet source stops requesting new ranges.

The runtime must distinguish buffer ownership and account for every allocation category:

```text
Total resident <= Wasm input/output/local rings
                + compressed/decompression scratch
                + decoded/dictionary state
                + query/operator state
                + transport-owned buffers and pending I/O
                + JS/runtime overhead and safety headroom
```

Each execution group has a configured budget within its host isolate's budget. Several colocated groups **share the isolate cap**; a per-DO limit alone is insufficient. In Cloudflare, plan with headroom below the 128 MiB isolate limit.

The steady-state decoder/transport hot path must use preallocated/reused storage; it must not grow with the number of row groups processed. Stateful aggregation is different: its memory may grow with **distinct group cardinality**, so it needs an explicit limit and either repartitioning/spill or a clear resource-exhausted failure. "Zero allocation" never means "an arbitrary GROUP BY uses constant memory."

All network responses and declared frame lengths are checked **before** acquiring or filling large payload memory. Read reservations precede payload reads; released/aborted work returns its credits. Fan-in edges share a receiving budget, but a node must not deadlock by holding an input reservation while waiting for output capacity backed by the same exhausted credit pool. Use separate input/output reservations or an explicit ownership transfer.

Do not conflate an `await writer.write()` with a zero-copy transfer of Wasm memory: a bounded copy to transport-owned bytes is acceptable and usually necessary. Never reuse an exported Rust buffer until the host acknowledges it.

## 8. Normative invariants

**I-01 — One query engine.** All query operators consume WCOL semantic batches or typed partial states. Parquet operators only decode; they do not filter, aggregate or interpret SQL.

**I-02 — Placement independence.** Moving an edge between local Rust memory and remote WCS1 transport cannot change query results or successful completion semantics.

**I-03 — Rust owns data semantics.** No SQL execution, row iteration, column transformation, hashing or aggregation in TypeScript. Host bindings remain scalar/pointer/port/action based.

**I-04 — No per-row allocation.** Ingestion and ordinary streaming operators allocate reusable workspace during initialization, then make no per-row or per-page-sized transient collections on their normal hot path. Bounded operator state is separately accounted for.

**I-05 — Strict memory budgets (not satisfied yet).** Every buffer, in-flight request and retained operator state is included in a per-execution-group and per-isolate bound. A scanner's working memory must stabilize across successive row groups, regardless of total dataset size. `ScanLimits` count guardrails and the memory plateau harness exist; bounded streaming decode + ReadAt backpressure are still in progress. Detail: [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md).

**I-06 — Explicit ownership.** Each input/output region has exactly one current owner. `commit_input` and `consume_output` are the only transitions that make committed input or acknowledged output reusable. Borrowed pointers are invalid after any Wasm memory growth and must be reacquired.

**I-07 — End-to-end backpressure.** No stage may continue to buffer unbounded input while downstream is stalled. Fan-in budgets are shared and local schedulers avoid credit cycles.

**I-08 — Partial progress is legal.** Operators can consume fewer bytes than supplied or produce fewer bytes than capacity. Zero progress must return a waiting/yield reason rather than spin indefinitely.

**I-09 — Exact source coverage.** A successful scan processes every assigned row-group task exactly once in the accepted attempt, with no gaps or overlaps. Retry identities must prevent duplicate partials from being counted twice.

**I-10 — Valid streams only.** Every WCS1 input validates frame lengths, ordering/sequence, schema and dictionary scope, and reaches exactly one valid terminal state (`END` or `ERROR`). Truncated connection closure is failure, never success.

**I-11 — Typed correctness.** Nulls, `COUNT(*)` vs `COUNT(column)`, integer width, floating-point equality (including signed zero), grouping keys, dictionary identity and aggregate merge semantics are consistent across sources and placements.

**I-12 — Associative partials.** Distributed aggregation merges sufficient states (e.g. `SUM` and `COUNT` for `AVG`) rather than local final results. Ordered, non-associative operators such as final global sort must be placed after the required merge/partition stages.

**I-13 — Bounded work and fairness.** An executor yields after a bounded quantum. One busy operator cannot starve control, cancellation, I/O completion or other local operators.

**I-14 — Failure is visible.** All pending reads/writes and downstream ports terminate on failure. No control path can wait indefinitely on an in-memory promise belonging to a restarted DO. A lost execution is retried using stable task/attempt identities or reported as failed/interrupted.

**I-15 — Immutable source snapshot.** Every range read must refer to the source version used during planning, or the task fails and replans. Never combine metadata and pages from different object versions.

**I-16 — Budgeted graph creation.** The planner validates cycles, missing ports, incompatible stream types, dropped partitions, row-group overlaps, fan-in/out and worst-case resource estimates before starting jobs.

## 8a. Hash shuffle buffering (16 scanners × 1000 reducers)

Target topology: each scanner may write to many reducer destinations; each reducer only ingresses from ~16 scanners. Logical edge count is `scans × partitions` (16 × 1000 = 16 000). That must not imply 16 000 independently reserved buffers.

| Knob | Meaning | Default |
|------|---------|---------|
| `WCOL_MAX_ACTIVE_INGRESS` | Concurrent streams **arriving at a reducer** (and the hash scan window tied to reducer pull readiness) | 8 |
| `WCOL_MAX_ACTIVE_SCANS` | Scanners running inside that window | 16 |
| `WCOL_MAX_ACTIVE_EGRESS` | Concurrent **outstanding partition writes** per scan (not a cap on open destination streams) | 32 |
| `WCOL_INGRESS_BYTES` | Shared incoming buffer pool per consumer isolate | 2 MiB |
| `WCOL_DEMUX_BYTES` | Shared scanner egress/shuffle pool | 2 MiB |
| `WCOL_DISPATCH_CONCURRENCY` | Concurrent RPC/stream **establishment** (burst smoother; not a Durable Object RPC hard cap) | 6 |

[`SharedBytePool`](../src/runtime/stream/shared-byte-pool.ts) charges **queued chunk bytes** and releases them when the consumer dequeues (implemented inside [`bounded-byte-pipe.ts`](../src/runtime/stream/bounded-byte-pipe.ts)). Opening a port takes only a concurrency slot.

Hash-exchange destinations are created **lazily on first write** on the **scanner** isolate (`demuxPartitionStream` + `acceptScanEdge`). `prepareReducer` means slots exist, not that the reducer is reading. A scan is admitted only in the contiguous ingress window after destinations are **consume-ready**. Unopened edges are completed with `completeScan` / `seal`, not 16 000 empty streams. A failed scan aborts the attempt; it is never treated as an empty partition.

Web Streams `highWaterMark` is a backpressure hint. Authoritative memory is the pool plus **retainedBytes** (ready chunks, pipe queues, demux pending). Tests report both `pool.peakUsed` and retained bytes.

**Coordinator isolate:** control only (prepare, windowed launch, completeScan/seal, retry). Bulk WCS goes scanner DO → reducer DO. Uniform high-cardinality data is expected to open ~16 000 edges; that is success, not a miss. Verify: `npm run test:cf-hash-fanout` (1×1 ownership, 2×2, 16×16); `HASH_FANOUT_16x1000=1` for the dense 16×1000 Durable Object bench.

Paid Workers default to 10 000 subrequests per invocation. ~1 000 RPCs per scanner isolate stays under that default; 16 000 from the coordinator isolate does not.

## 9. Lifecycle and scheduling

A job has a stable job ID, a versioned plan, task IDs, attempts, an execution-group placement and an explicit deadline. The coordinator persists enough state to distinguish `pending`, `running`, `complete`, `failed` and `interrupted`. Status reads return promptly rather than holding open a result request until all downstream work finishes.

Each source task produces a terminal status and, for aggregations, a partial tagged with its task/attempt identity. If a DO restarts, the coordinator must fail promptly or reschedule unfinished tasks; it may not treat partial progress as a successful stream or wait forever on its previous instance.

**Exactly-once recovery is not implemented by the primitive.** A future retry/checkpoint layer must deduplicate accepted attempts and make final result publication idempotent. Until then, interrupted distributed jobs fail visibly rather than silently returning incomplete results.

## 10. Validation and observability

Tests must run the **same logical graph** with local-only placement and remote/multi-DO placement:

| Test | Required outcome |
| --- | --- |
| Byte-stream fragmentation | Split header/payload at every possible boundary; frames remain valid or fail precisely. |
| Local vs remote | Same fixture and query produce identical rows, groups and partial aggregates. |
| Q8 / Q3 | Full ClickBench results match DuckDB under multiple placements. |
| Metadata-only count | `COUNT(*)` can use footer rows; `COUNT(column)` correctly handles nulls. |
| Slow sink | Upstream eventually blocks; no memory growth or lost rows; finishes when sink resumes. |
| Many row groups | Decoder memory reaches a bounded plateau; does not grow with task count. |
| Multi-input | Fair fan-in with one slow producer; no deadlock and all required `END`s observed. |
| High cardinality | Group-state budget is enforced or work is repartitioned; no unexplained isolate restart. |
| Restart/cancel | Interrupted nodes are detected, all dependents are released, and final output cannot be falsely marked complete. |
| Source mutation | Version/ETag mismatch stops the scan before mixing snapshots. |

Emit metrics per task, execution group and job: planned/completed row groups, rows and bytes decoded, range calls, header/body latency, queued and blocked time, first-output time, scratch and live operator allocations, Wasm high-water memory, transport-buffer peak, partial bytes, last progress timestamp, final result and failure reason.

## 11. Implementation state and next work

**Already prototyped:** WCS1 framing and distributed edges, a fixed-ring Rust `Operator::poll` pipeline, a scalar/pointer Wasm ABI, a TypeScript Web Streams host, and a basic incremental WCS1 guard. These pieces are foundations, **not yet one integrated production executor**. The standalone Rust primitive has not yet been compiled/tested in the provided artifact environment.

**Still needed:** planner-generated physical programs and placements; Rust Parquet-to-WCOL operators using truly bounded page scratch; Rust WCS1-to-query operators; multi-input/output Rust runtime; byte-stream adapters to the existing graph transport; shared isolate memory accounting; robust completion/cancellation and restart semantics.

**Recommended vertical slice:** one row-group Parquet reader -> WCOL batches -> Rust filter/aggregate -> partial -> Rust reducer -> result. Run it (a) colocated in one executor and (b) split across two or more DOs. Assert identical results, bounded memory and observable backpressure. Then scale to full ClickBench Q3 and Q8, and only afterwards add strings, large-cardinality shuffle and richer scheduling.

## 12. Non-goals for this iteration

Do not build a separate Parquet SQL path, a TypeScript columnar engine, a general distributed scheduler, arbitrary joins/sorts/windows, automatic spilling, or exactly-once checkpointing now. Do not make network serialization mandatory between colocated operators. The goal is to establish **one composable Rust stream interface with enforceable ownership and memory invariants**, while keeping placement a planner choice.

---

## Appendix A — Code map (workspace, 2026-03)

This appendix tracks the repo against §11; it is not part of the normative spec.

| Spec area | Current location | Notes |
| --- | --- | --- |
| `Operator::poll`, `EdgeBuffer`, WcsGuard | `rust/wcol-stream-core` | One bounded edge per hop; [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md); `ReadAt` on `Action` + `Progress`; not yet full `Executor` / multi-port scheduler. |
| Wasm ABI | `rust/wcol-stream-operators/src/ffi.rs` | `wcs_*`; output ack before reuse (I-06). |
| TS host | `src/runtime/stream/run-wcol-engine.ts`, `wasm-stream-driver.ts` | `ReadAt` / `ReadPort` / `WritePort`; copy at Wasm boundary (§7). |
| Parquet → WCS1 `DATA_BATCH` | `rust/wcol-stream-operators/src/parquet_ingest.rs` | No SQL; emits `WCOLpjv1` projection payloads. **Interim:** whole column chunk via host; decoder still uses `NumericScan` / page `Vec<Cell>` (I-04 gap). |
| Plan encoding | `src/runtime/stream/parquet-ingest-plan.ts`, `rust/.../ingest_plan.rs` | v2 physical program prefix `0xFF`. |
| Byte stream ↔ frame graph | `archive/legacy-ts-stream/byte-edge.ts` | `FrameDecoder` adapter for existing `StreamSink`. |
| Query on WCS1 (prototype) | `rust/wcol-stream-operators/src/query_stream.rs` | `Running::consume_i64`; serialized local edge (WCS1). |
| Fused ingest + aggregate | `rust/wcol-stream-operators/src/ingest_query.rs` | No inter-op edge; decode → `Running` in one op (vertical slice). |
| Legacy Parquet+SQL in one engine | `scan-parquet-fold`, `rust/wcol-parquet/src/engine.rs` | **Violates I-01**; retire when stream slice is default. |
| Distributed graph / DO | `archive/legacy-ts-stream/`, `cloudflare/src/worker.ts` | **Legacy:** TS `runStreamNode()` per op; frame-oriented edges (violates Rust-first — see RUST_FIRST_EXECUTION.md). |
| Target host loop (stub) | `src/runtime/host/engine-host.ts` | `nextAction` / `complete` shape; not wired to Wasm yet. |
| PageEvent decoder | `compact-parquet-thrift` | Allocation-free with caller scratch; not yet wired into ingest hot path. |

**Next engineering steps (Rust-first order):**

1. Unified Wasm **`nextAction` / `complete`** host ABI and TS `engine-host` (replace `runStreamNode` dispatch).
2. **`wcol-stream` scheduler** — multi-port colocated pump; TS only external ReadAt / ReadPort / WritePort.
3. Ingest on **`PageEvent`** + reusable scratch (I-04/I-05); operator emits row counts without TS `decodeProjection`.
4. Planner-generated programs in **`wcol-planner`**; 1 DO vs N DO parity on one row group, then Q8/Q3.
5. Retire **`scan-parquet-fold`** and TS **`codec.ts` / `columnar.ts` / `shuffle.ts`** on the hot path.
