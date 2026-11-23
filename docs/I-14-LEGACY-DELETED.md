# I-14 — Legacy paths deleted

**Status: pass (retired symbols)** (2026-10-28)

## Removed from active tree

| Item | Action |
|------|--------|
| Rust `ExecutionGroup` alias | Removed; use [`GraphHost`](../../rust/wcol/src/exec/graph_host.rs) / `ColocatedParquetQueryOp` |
| Rust `ColocatedQueryGraph` / `QueryPipeline` | Removed; use [`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs) |
| Opcodes **5–9** chain/stages | Removed (prior work) |
| TS `encodeVerticalSliceChainPlan` / stages encoders | Never shipped in current tree; opcode **10** only |
| TS `src/runtime/exec/*` | Moved to [`archive/legacy-ts-chunk-exec/`](../archive/legacy-ts-chunk-exec/) |
| Stream barrel re-export of `runStreamNode` / `executeGraph` | Removed from [`stream/index.ts`](../src/runtime/stream/index.ts) |

## Still registered (not duplicate engines)

| Symbol | Why |
|--------|-----|
| `OP_INGEST_QUERY` (4) | Plan constant; opener uses opcode **10** for fused ingest |
| `ColocatedParquetQuery` | Type alias → `LinearColocatedGraph` in `exec/scheduler.rs` (single impl) |
| Legacy graph in `archive/` | Explicitly quarantined; Cloudflare worker imports archive path |
| `ColocatedGraphWire` | **Deleted** — replaced by `GraphHost` (not a `WireOperator`) |

## Evidence

- Rust: [`legacy_paths_removed.rs`](../../rust/wcol/tests/runtime/legacy_paths_removed.rs)
