# Architecture

WCOL is **one Rust streaming engine**, a **thin Wasm adapter**, a **native CLI**, and a **minimal TypeScript host**. All column processing, planning, scheduling, and query logic live in Rust.

**Consolidation tracker:** [CONSOLIDATION.md](./CONSOLIDATION.md) (migration phases and deletion criteria).

## End-to-end pipeline

```text
InputSource (file, HTTP, S3, …)
        │
        ▼
  ParquetDecode / WcolDecode     ← formats/ (bytes → WCOL batches only)
        │
        ▼
   WCOL stream                   ← stream/ (batches, partials, WCS1)
        │
        ▼
  Filter → Aggregate → Reduce → Finalize   ← ops/query + exec/runtime operators
        │
        ▼
   WCOL stream
        │
        ├── JsonEncode
        ├── ParquetEncode
        └── WcolEncode
        │
        ▼
  OutputSource
```

- **Query never lives under `formats/parquet`.** Parquet is footer plan + page scan → `WcolBatch`.
- **SQL planning** is `plan/sql_parser`; **execution state** is `ops/query` + `exec/runtime/wcol_stream/*`.
- **On-disk `.wcol` files** (archived) use `archive/rust/wcol-file` (`wcol_file::decode`) for page decode; the same operator chain consumes WCOL batches afterward.

## Repository layout

```text
wcol/
├── rust/wcol/          # Engine: stream, formats, ops, exec, plan, host
├── archive/rust/       # Archived .wcol file engine, browser Wasm, native CLI
├── src/                # Public JS/TS API + host driver (I/O, no SQL) — legacy paths retiring in Phase 4
├── cloudflare/         # Worker / DO
├── tests/              # Parity and E2E
└── docs/               # This file + streaming, memory, format specs
```

External dependency: [`compact-parquet-thrift`](../../compact-parquet-thrift) (Parquet thrift/pages only).

## Engine modules (`rust/wcol`)

| Module | Responsibility |
|--------|----------------|
| `stream/` | Batches, partial aggregate schema, WCS1 wire |
| `formats/` | Parquet / `.wcol` / JSON — **decode & encode only** |
| `ops/` | Query kernel (`ops/query`) + operator facades (filter, aggregate, …) |
| `exec/` | Operator trait, graph, scheduler, `ParquetDecode`, `JsonEncode`, … |
| `plan/` | SQL → logical plan, physical ingest placement |
| `host.rs` | External I/O effects (`ReadAt`, ports, yield) |

## Import convention

```rust
use wcol::formats::parquet::{open_plan, NumericScan, WcolBatch};
use wcol::ops::query::{CatalogColumn, Running, columns_for_sql};
use wcol::stream::partials::PartialStateWcol;
use wcol::exec::{execute, ParquetDecode, FilterOp, AggregateOp};
```

## Migration state

Rust engine consolidation (Phases 2c–2d) is done: one `wcol` crate. **Phase 4** removes TS `src/runtime/*` execution (including legacy `parquet_exec_sql` Wasm) in favor of the host driver + Rust pipeline above.

### Hosted ClickBench (HTTP Range → ExecutionGroup)

Build stream Wasm once:

```bash
cargo build -p wcol --target wasm32-unknown-unknown --release
```

Run colocated ingest+query against the public hits file (no local download):

```bash
npm run test:hits-http-exec
npx tsx tests/hits-http-exec-group.ts --query q3 --max-row-groups 5
```

Compares `wcol.wasm` pipeline output to the chunk oracle on the same row groups.

### Host vs engine boundary

TypeScript implements **generic I/O only** (`run-wcol-engine.ts`): `readAt`, optional `readPort`, `writePort`, and real event-loop `yield`. Rust owns the poll loop, input rings, read sizes (capped to ring capacity), EOF, and errors. Parquet chunk offsets and lengths come from the compiled plan; the host never sees SQL, dictionaries, or WCOL layout.

## Further reading

- [CONSOLIDATION.md](./CONSOLIDATION.md)
- [UNIVERSAL_WCOL_PIPELINE.md](./UNIVERSAL_WCOL_PIPELINE.md)
- [STREAMING_PIPELINE.md](./STREAMING_PIPELINE.md)
- [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md)
- [FORMAT.md](./FORMAT.md)
