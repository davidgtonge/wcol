# What we are working on (Rust-first stream path)

Everything else lives under [`archive/`](./archive/README.md) for reference and compatibility.

## Active TypeScript (host I/O only)

| Path | Role |
| --- | --- |
| [`src/runtime/host/engine-host.ts`](./src/runtime/host/engine-host.ts) | Target `nextAction` / `complete` loop (stub) |
| [`src/runtime/stream/run-wcol-engine.ts`](./src/runtime/stream/run-wcol-engine.ts) | Generic host poll loop (`ReadAt` / `ReadPort` / `WritePort`) |
| [`src/runtime/stream/wasm-stream-driver.ts`](./src/runtime/stream/wasm-stream-driver.ts) | Low-level Wasm ABI types + `runWcolStream` |
| [`src/runtime/stream/stream-wasm.ts`](./src/runtime/stream/stream-wasm.ts) | Load `wcol_stream_operators.wasm` |
| [`src/runtime/stream/parquet-ingest-plan.ts`](./src/runtime/stream/parquet-ingest-plan.ts) | Encode v2 opcode `2` ingest plan |
| [`src/runtime/stream/vertical-slice-plan.ts`](./src/runtime/stream/vertical-slice-plan.ts) | Encode opcode `4` colocated ingest+query |
| [`src/runtime/stream/index.ts`](./src/runtime/stream/index.ts) | Public exports (active + legacy re-exports) |
| [`src/runtime/io/byte-source.ts`](./src/runtime/io/byte-source.ts) | Range I/O for Parquet |

## Active Rust

| Path | Role |
| --- | --- |
| [`rust/wcol-stream-core/`](./rust/wcol-stream-core/) | `EdgeBuffer`, `Operator::poll`, `Pipeline`, backpressure |
| [`rust/wcol-stream-operators/`](./rust/wcol-stream-operators/) | `ExecutionGroup` (decode/filter/aggregate/emit), `ParquetIngestOp`, plans, `wcs_*` FFI |
| [`rust/wcol-parquet/`](./rust/wcol-parquet/) | Decode, query `Running`, partials (ingest side; **not** fused `engine.rs` hot path) |

## Active docs

| Doc | |
| --- | --- |
| [docs/RUST_FIRST_EXECUTION.md](./docs/RUST_FIRST_EXECUTION.md) | Host boundary, migration |
| [docs/COLOCATED_MEMORY.md](./docs/COLOCATED_MEMORY.md) | Scratch vs edge, fusion |
| [docs/BOUNDED_MEMORY.md](./docs/BOUNDED_MEMORY.md) | Ingest memory invariants (I-04/I-05), budgets, tests |
| `./scripts/run-memory-plateau.sh` | Priority-1 plateau harness (native + wasm build) |
| [docs/STREAMING_EXECUTION.md](./docs/STREAMING_EXECUTION.md) | WCS1, invariants |
| [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) | Stack overview |

## Active tests

| Test | |
| --- | --- |
| [`tests/vertical-slice.test.ts`](./tests/vertical-slice.test.ts) | Parquet → `SUM(a)` partial, no TS column decode |
| [`tests/parquet-ingest-stream.test.ts`](./tests/parquet-ingest-stream.test.ts) | Ingest-only Rust stream (legacy frame decode for row count — tighten later) |
| [`tests/wasm-stream-driver.test.ts`](./tests/wasm-stream-driver.test.ts) | Mock ABI driver |
| [`rust/wcol-stream-operators/tests/vertical_slice.rs`](./rust/wcol-stream-operators/tests/vertical_slice.rs) | Native colocated slice |
| [`rust/wcol-stream-operators/tests/colocated_ops.rs`](./rust/wcol-stream-operators/tests/colocated_ops.rs) | Filter + aggregate via selection mask |
| [`rust/wcol-stream-operators/tests/clickbench_exec_group.rs`](./rust/wcol-stream-operators/tests/clickbench_exec_group.rs) | Q8/Q3/Q1 vs oracle when `HITS_PARQUET` is set |

## Build

```sh
cd wcol/rust
cargo test -p wcol-stream-core -p wcol-stream-operators
cargo build -p wcol-stream-operators --target wasm32-unknown-unknown --release
cd ..
npx tsx tests/vertical-slice.test.ts
```

### ClickBench proof (local `hits.parquet`)

```sh
export HITS_PARQUET=/path/to/hits.parquet
./scripts/run-exec-group-clickbench.sh
```

Compares `ExecutionGroup` partials to the Rust `exec_sql` oracle for **Q8** (18 groups), **Q3**, and **Q1** (metadata row counts). Wall-clock vs the 3.5s baseline is not automated yet — run `npm run test:clickbench` separately for timing on the legacy graph path.
