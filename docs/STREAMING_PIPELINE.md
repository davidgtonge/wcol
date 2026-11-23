# Universal WCOL streaming pipeline

**Status:** Target architecture (authoritative for new Rust work)  
**Related:** [UNIVERSAL_WCOL_PIPELINE.md](./UNIVERSAL_WCOL_PIPELINE.md), [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md), [COLOCATED_MEMORY.md](./COLOCATED_MEMORY.md), [PARQUET_DICTIONARY_WCOL_STREAM.md](./PARQUET_DICTIONARY_WCOL_STREAM.md)

## Pipeline

Codecs sit at the edges. Everything in the middle speaks **WCOL** (typed column batches + dictionary scope), not Parquet or JSON.

```text
ParquetDecode          Query operators              Sinks
(read Parquet only)    (source-agnostic)            (encode only)

     │                        │                         │
     ▼                        ▼                         ▼
  WCOL Stream  ──►  filter · project · group  ──►  WCOL Stream
                    · aggregate · transform            │
                                                       ├── ParquetEncode → .parquet
                                                       └── JsonEncode    → JSON
```

Conceptual Rust shape:

```rust
let source = ParquetDecode::new(reader, limits);
let query = QueryPipeline::new(plan.operators);

let output = match plan.output {
    Output::Parquet => Sink::Parquet(ParquetEncode::new(writer)),
    Output::Json => Sink::Json(JsonEncode::new(writer)),
};

execute(source, query, output)?;
```

Rust entry point (native tests / CLI):

```rust
let mut source = ParquetDecode::new(batch_rows, catalog, groups, dict_columns);
let mut query = QueryPipeline::open(sql, &catalog)?;
let mut sink = JsonEncode::new();
let json = execute(&mut source, &mut query, &mut sink, &chunk_bytes)?;
```

Operators do not know whether the upstream batch came from Parquet, CSV, another WCOL stream, or a remote Durable Object (after WCS1 decode at the boundary).

## Three rules

1. **ParquetDecode has no query logic.** It reads Parquet and emits correctly typed, **bounded** WCOL batches. Column projection (read only needed columns) is fine; no SQL filters, aggregates, or transforms.
2. **Query operators consume and produce WCOL.** They compose independently of source and sink.
3. **Local = borrowed buffers; remote = WCS1.** Inside one execution group, operators share Rust-owned memory (dictionaries, batch slots, validity). Serialize to WCS1 only when crossing a host/DO/network edge. All dictionaries, batches, and codec scratch use explicit byte budgets ([BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md)).

**ParquetEncode** and **JsonEncode** are symmetric: no query logic, only WCOL → target format.

## Dictionary encoding

No expansion in the decode/encode path unless an operator explicitly materializes:

```text
Parquet dictionary + IDs
        ↓
WCOL dictionary + IDs   (install dict once per scope; batches carry ids + validity)
        ↓
Query operators         (resolve dictionary entries only when needed)
        ↓
WCOL dictionary + IDs   (when the plan preserves encoding)
        ↓
Parquet dictionary + IDs
```

Dictionary **identity** is scoped (e.g. per row group × column). Raw ids from different scopes must never be compared as equal values.

## Current repo vs target

| Today | Target |
| --- | --- |
| `NumericScan` / future `ParquetDecode` in `wcol-parquet` | Edge decoder → WCOL batches (+ dict install) |
| `ParquetIngestOp` emits WCS `DATA_BATCH` projection | Same batches locally; WCS only at remote edge |
| **`ExecutionGroup`** | **Fusion only:** polls `ParquetDecode` → `QueryPipeline` → WPP1 on the WCS edge (logical partials are typed WCOL between Aggregate/Reduce/Finalize) |
| `aggregate_chunk` in `exec_sql` oracle | Offline reference; not the streaming hot path |
| ClickBench Q3 via expanded **i64** in colocated path | Move to dict-id WCOL + dict-aware aggregate |

New features should extend **WCOL batch types** and **operator traits**, not add branches inside Parquet decode.

## Implementation pointers

| Concern | Crate / module |
| --- | --- |
| Parquet → WCOL decode | `wcol-parquet` (`dictionary`, `numeric_scan`, `projection`, `stream_wcs`) |
| Operator graph | `wcol-stream-operators` (`wcol_stream/`, `pipeline/`, `exec_group.rs`) |
| Partial state WCOL | `wcol-parquet::agg_partial_wcol` (`PartialStateWcol`, schema v1, synthetic column ids) |
| WCS1 framing | `wcol-stream-core`, `wcol-stream-operators::wcs`, `archive/legacy-ts-stream/` |
| On-disk `.wcol` | `wcol-decoder`, `wcol-encoder` (already dict-aware) |
| Host I/O | TypeScript transport only; no operator semantics in TS |
