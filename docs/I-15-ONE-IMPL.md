# I-15 — One implementation per feature

**Status: pass** (2026-10-28)

## Query execution

| Concern | Single implementation |
|---------|------------------------|
| SQL graph | [`LinearColocatedGraph`](rust/wcol/src/exec/graph_scheduler.rs) only |
| Host/wire port | [`GraphHost`](rust/wcol/src/exec/graph_host.rs) (`ColocatedParquetQueryOp` type alias) |
| Default ingest+query plan | Opcode **10** only ([`encode_ingest_query_plan`](rust/wcol/src/plan/physical.rs)) |

## Physical plan opcodes (ingress adapters, not duplicate engines)

| Opcode | Role |
|--------|------|
| **2** `OP_PARQUET_WCS_INGEST` | Decode Parquet → WCS row batches via **`GraphHost`** (same plan body; not `Pipeline` WireOperator) |
| **3** `OP_QUERY_WCS` | Query on WCS row batches |
| **10** `OP_COLOCATED_PARQUET_QUERY` | Fused Parquet ReadAt + colocated graph (production default) |

Opcodes **2+3** compose a distributed edge; **10** is the same graph with fused host ingress. Both **3** and **10** open [`GraphHost`](rust/wcol/src/exec/graph_host.rs) directly (not `Pipeline`).

## Traits (intentional split)

| Trait | Layer |
|-------|--------|
| [`Operator`](rust/wcol/src/exec/operator.rs) | Typed colocated stages (filter/agg/reduce/finalize) |
| [`WireOperator`](rust/wcol/src/stream/core.rs) | Byte rings, ReadAt, WCS frames at plan boundaries |

Query logic lives only on `Operator` + `LinearColocatedGraph`; `WireOperator` does not implement SQL.

## Evidence

- [`i15_single_impl.rs`](../rust/wcol/tests/runtime/i15_single_impl.rs)
