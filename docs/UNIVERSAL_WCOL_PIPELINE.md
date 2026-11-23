# Universal WCOL streaming pipeline

**Status:** Authoritative dev brief for Rust streaming work  
**Related:** [STREAMING_PIPELINE.md](./STREAMING_PIPELINE.md), [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md), [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md)

## Objective

Make **every operator in WCOL consume and produce the same logical WCOL stream**, including `Aggregate`, `Reduce` and `Finalize`.

Operators must support incremental processing and work unchanged whether they execute:

- In the same Rust/Wasm executor
- On separate CPU threads
- On different machines or Cloudflare Durable Objects

**The planner determines placement. Operators must not know where their inputs come from or where their outputs go.**

## Target architecture

```text
ParquetDecode
      │
      ▼
  WCOL Stream
      │
      ▼
    Filter
      │
      ▼
  WCOL Stream
      │
      ▼
   Aggregate
      │
      ▼
  WCOL Stream (partial aggregate state)
      │
      ▼
    Reduce
      │
      ▼
  WCOL Stream (merged aggregate state)
      │
      ▼
   Finalize
      │
      ▼
  WCOL Stream (result columns)
      │
      ├── JsonEncode
      ├── ParquetEncode
      └── WCOL output
```

There is **one stream abstraction**, not separate protocols for data, partial aggregates and final results.

WCS1 is the serialized representation used at physical boundaries. Colocated operators pass borrowed WCOL batches without encoding.

## Operator contract

```rust
trait Operator {
    fn poll(
        &mut self,
        io: &mut OperatorIo<'_>,
    ) -> Result<Progress>;
}
```

Each operator:

- Accepts typed WCOL batches.
- Produces typed WCOL batches.
- Can change the output schema.
- Processes input incrementally.
- Respects output backpressure.
- Uses explicitly budgeted memory.
- Handles end-of-stream and errors.
- Is completely independent of transport and placement.

Sources have no upstream input. Sinks have no downstream WCOL output.

An operator may accumulate state before producing output. Streaming means it can consume input incrementally without materializing the entire upstream stream; it does not require emitting a batch for every input batch.

**Rust:** colocated typed edges use `wcol_stream_operators::wcol_stream` (`WcolOperator`, `WcolStreamItem`). Remote edges use `wcol-stream-core` byte `Operator` + WCS1 adapters (same logical graph).

## Aggregate → Reduce → Finalize

These must be ordinary WCOL operators, not special execution-engine paths.

Example:

```sql
SELECT country, AVG(income)
FROM data
GROUP BY country;
```

**Aggregate output — WCOL partial-state batches**

| country | sum_income | count_income |
|---|--:|--:|
| UK | 420000 | 200 |
| US | 780000 | 300 |

**Reduce output — same WCOL state schema**

| country | sum_income | count_income |
|---|--:|--:|
| UK | 840000 | 400 |
| US | 1560000 | 600 |

**Finalize output — WCOL result batches**

| country | avg_income |
|---|--:|
| UK | 2100 |
| US | 2600 |

`Aggregate` maintains partial state, `Reduce` merges partial states and `Finalize` converts them into result values.

Requirements:

- Partial states have explicit, versioned schemas.
- AVG uses sum/count; never average partial averages.
- COUNT, SUM, MIN, MAX and other supported aggregates have well-defined merge semantics.
- Null handling and type promotion remain correct.
- More complex states (variance, sketches, etc.) use typed or opaque binary WCOL columns as appropriate.
- Grouping keys are compared by values, not dictionary IDs from unrelated dictionaries.
- `Finalize` waits for the necessary completeness signal before emitting a definitive result.

Initially, emit one partial per completed partition or task. Do not introduce repeated cumulative snapshots until their replacement/deduplication semantics are explicitly defined.

**Phase 2 (done in Rust colocated path):** partial aggregate state is an `OwnedWcolBatch` with schema metadata plus synthetic column ids (`PARTIAL_KEY_*`, `PARTIAL_STAT_*`), wrapped as `WcolStreamItem`. **Physical edges emit `PWCL`** (typed partial WCOL + `WCOLpjv1` columns) in WCS frame type 4; **`WPP1` is legacy decode/oracle only** via `partial_state_from_wire_payload`.

## Local versus distributed execution

The **same Rust operator implementations** must support all placements.

| Placement | Edge implementation |
|---|---|
| Same executor | Borrowed WCOL column buffers |
| Separate threads | Bounded ownership-transferring queues |
| Separate machines / DOs | WCS1 over network streams |

The Rust executor owns scheduling, buffer lifetimes and backpressure.

Only remote edges need WCS1 serialization.

Do not serialize between colocated operators merely because the logical graph contains a WCOL stream boundary.

## Dictionary support

Preserve dictionary-encoded columns throughout the pipeline. No unconditional expansion in `ParquetDecode`. Dictionaries are scoped; operators resolve when needed. Remote edges install dictionary definitions before dependent batches.

## Bounded memory and backpressure

All components stream with bounded working memory. A full output edge blocks its producer and propagates backpressure to the source. Unbounded GROUP BY cardinality requires an explicit state limit until spilling exists.

## Implementation phases

| Phase | Work |
| --- | --- |
| **1** | Canonical `WcolOperator` + connect `ParquetDecode`, `Filter`, `Aggregate`, `Reduce`, `Finalize` |
| **2** | Partial state as typed WCOL batches (replace `WPP1` on the logical stream) |
| **3** | **Done (colocated):** `ColocatedQueryGraph` in `ExecutionGroup` / `QueryPipeline`; `WPP1` only in `finish_wpp1_edge` + WCS emit |
| **4** | WCS1 adapters at remote boundaries only; idempotent reduce |
| **5** | `JsonEncode`, `ParquetEncode`, WCOL passthrough sinks |

## Acceptance tests

1. Same query result colocated or across DOs.
2. Aggregate → Reduce → Finalize on WCOL batches (no special partial decode path in the executor).
3. Q3 and Q8 match DuckDB.
4. Dictionary-encoded grouping across row groups.
5. Small batches, nulls, batch boundaries.
6. Backpressure end-to-end.
7. Memory plateau over many row groups.
8. Remote retries cannot double-count.
9. ParquetEncode and JsonEncode consume the same final WCOL stream.
10. New Rust operators do not require TS transport changes.

## Non-goals

New SQL, new Parquet codecs, rewriting WCOL wire format, separate distributed engine, spilling/checkpointing in this milestone, premature dict-aware operator optimization.

## Definition of done

```text
ParquetDecode → Filter → Aggregate → Reduce → Finalize → JsonEncode / ParquetEncode
```

One stream format. One set of operators. One executor model. Multiple placements.
