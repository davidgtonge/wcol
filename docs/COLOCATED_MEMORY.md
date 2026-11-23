# Colocated memory: scratch, edges, and the executor

**Status:** Target model (authoritative for in-process execution groups)  
**Companion:** [RUST_FIRST_EXECUTION.md](./RUST_FIRST_EXECUTION.md), [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md)

Colocated operators run in **one Wasm linear memory**. They share buffers inside Rust; TypeScript is not involved between them. The stream abstraction defines **how** operators communicate logically; it does **not** require every operator to serialize output when the consumer sits in the same execution group.

## Scratch vs edge

| Kind | Role | Lifetime |
| --- | --- | --- |
| **Scratch** | Temporary working memory: decompression, decode staging, building a **remote** WCS1 frame, compaction copies when unavoidable | Reused by one operator across polls; not “data in flight” to the next operator |
| **Edge** | Holds bytes (or, at a higher layer, a logical WCOL batch view) that the **producer has committed** and the **consumer has not yet consumed** | Owned by the **edge**, not by either operator; scheduler mediates borrows |

For **colocated** operators on a **materialized** local connection, the consumer reads the same memory the producer wrote—no host call, no WCS1 encode/decode on that hop.

When the planner **fuses** adjacent operators (e.g. ingest + aggregate in one `IngestQueryOp`), there may be **no edge buffer at all**: decoded column values are passed as borrowed slices into `Running::consume_*` on the same poll stack. That is the simplest handoff and matches the goal of avoiding unnecessary intermediate representation.

## Edge ownership and handoff

The **scheduler** owns each edge buffer. Operators receive **time-limited borrows**; they must not retain `input`/`output` slices after `poll` returns.

Conceptual API (see `wcol-stream-core::EdgeBuffer`):

```rust
// Producer side
let output = edge.writable();
let produced = decoder.produce(output)?;
edge.commit(produced);

// Consumer side (after producer commit; often a later poll in the same pump)
let input = edge.readable();
let consumed = filter.consume(input)?;
edge.consume(consumed);
```

Handoff is **sequential**: no overlapping mutable borrows on the same edge; the producer must not overwrite bytes the consumer has not consumed. Rust’s borrow rules are the backstop; the scheduler must not schedule conflicting access.

**Progress** reports partial work and drives backpressure:

```rust
pub struct Progress {
    pub consumed: usize,
    pub produced: usize,
    // ...
}
```

If the downstream edge is full, the scheduler stops polling the producer and runs consumers (and their downstream), propagating pressure until the Parquet source stops issuing `ReadAt`. No JavaScript promises between colocated operators—the executor runs synchronously until an **external** port, `ReadAt`, or cooperative `Yield`.

## One edge per materialized local connection

Start with **one bounded buffer per materialized local edge** in the physical graph. That yields a predictable memory footprint: configured capacity × edges + operator state + scratch pools—not row count.

The planner may **fuse** operators and drop edges when semantics allow:

| Combination | Local handoff |
| --- | --- |
| Parquet → Filter | Borrow decoded columns; filter may emit a **selection mask** instead of copying rows |
| Filter → Aggregate | Borrow columns + mask; update aggregate map |
| Project → Aggregate | Borrow projected column views |
| Transform → Transform | Alternate or reuse two scratch/output buffers |
| Aggregate → Reduce | Pass partial aggregate state when finalized |
| Shuffle → remote DO | Encode WCOL into a **bounded edge** facing the host `WritePort` |

Example Q8-style chain in one group: decoder scratch + one column batch buffer + aggregate map; filter and aggregate consume the same decoded values without extra batches.

**Fan-out:** do not let one consumer overwrite a buffer others still read. Use immutable borrowed views with completion accounting, or copy into separately budgeted buffers when consumers need independent mutation.

## Local edge vs remote edge

```text
Local (colocated):
  Op A ── EdgeBuffer (shared Wasm memory) ── Op B
  No WCS1 on the wire; optional fusion with no edge at all.

Remote (DO / worker boundary):
  Op A ── encode WCS1 into bounded output scratch ── host WritePort
       ── network ── host ReadPort ── Op B incremental decode
```

Placement changes **only the edge implementation** between two operators; operator code stays the same. One fixed input and one fixed output buffer per **external port** is appropriate; **internal** graphs may need several bounded local edges depending on topology.

## Executor shape (target)

Three memory abstractions in the Rust execution group:

```text
Executor {
    scratch: ScratchPool,      // codec/transform working regions, reserved once
    edges: Vec<EdgeBuffer>,    // one per materialized local byte/batch edge
    operators: Vec<OperatorState>,  // aggregate maps, shuffle state, etc.
}
```

The scheduler chooses which operator to poll, grants `writable`/`readable` on the correct edge, and enforces budgets.

**Caveat:** zero **allocation** on the hot path is not zero **copying**. Row compaction, shuffle, and remote transfer may require copies. The goal is to avoid unnecessary serialization, per-row allocation, and unbounded buffering.

## Current codebase mapping

| Target | Today |
| --- | --- |
| `EdgeBuffer` | `wcol-stream-core::EdgeBuffer` (one per hop in `Pipeline`; byte-oriented `Operator` trait) |
| `ScratchPool` | Per-operator scratch (e.g. Parquet decode buffers inside ops); not yet a shared pool type |
| Colocated query chain | `ExecutionGroup`: **ParquetDecode** → **`ColocatedQueryGraph`** (Filter → Aggregate → Reduce) → **Emit** (`WPP1` on WCS only). Row batches stay borrowed; partial state is typed WCOL until the edge encoder. |
| Serialized local chain | `ParquetIngestOp` → `QueryStreamOp` over internal rings (WCS1 on the edge—valid when ops stay separate in the plan) |
| External ports | Host `input()` / `output()` on `Pipeline` pipes[0] and pipes[last]; `ReadAt` / `HaveOutput` actions |
| `Executor` | `Pipeline` + v2 plan in `wcol-stream-operators` (scheduler pump in `poll()`; not yet planner-owned topology) |

**Migration preference:** one fixed-capacity edge per local connection + Rust-controlled scheduling first; then fusion and in-place/mask handoffs as optimizations—without changing operator semantics when placement moves from one DO to two.

## Related

- Vertical slice (fused ingest + `SUM`): [RUST_FIRST_EXECUTION.md § Migration](./RUST_FIRST_EXECUTION.md#migration-sequence)
- WCS1 on remote edges only: [STREAMING_EXECUTION.md §5](./STREAMING_EXECUTION.md)
