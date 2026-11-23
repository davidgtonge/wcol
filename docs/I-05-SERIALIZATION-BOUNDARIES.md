# I-05 — Serialization only at physical boundaries

**Status: pass (colocated query path)** (2026-10-28)

DoD **I-05** (this doc) is separate from **STREAMING_EXECUTION.md** “I-05” (bounded memory); memory acceptance lives in [BOUNDED_MEMORY.md](./BOUNDED_MEMORY.md).

## Rule

| Zone | Representation |
|------|----------------|
| Colocated query graph (Filter → Aggregate → Reduce → Finalize) | [`WcolStreamItem`](../../rust/wcol/src/exec/operator.rs) — owned batches only (partial metadata on batch) |
| Physical host / network edge | WCS1 frames; partial body **`PWCL`** ([`partial_state_to_wcs_payload`](../../rust/wcol/src/stream/partial_wcs.rs)) |

No WCS encode/decode **between** query stages inside [`LinearColocatedGraph`](../../rust/wcol/src/exec/graph_scheduler.rs).

## Where serialization happens

| Boundary | Component | Payload |
|----------|-----------|---------|
| Host Parquet ingest (opcode **10**) | [`GraphHost`](../../rust/wcol/src/exec/graph_host.rs) | One `FRAME_PARTIAL` after ingest EOF |
| WCS row ingress (opcode **3**) | Same wire adapter | Decode `DATA_BATCH` → inject typed batches; one partial at EOF |
| Oracle / legacy consumers | Wasm `partial_wire_to_wpp1` | **Normalize only** — not on the hot colocated path |

Removed: opcodes **5–9** (decode→query WCS chain, per-stage WCS query ops).

## Evidence

- Rust: [`serialization_boundaries.rs`](../../rust/wcol/tests/runtime/serialization_boundaries.rs) — zero partial frames mid-run; single **`PWCL`** at finish on opcode 10; typed graph finishes via `PartialStateWcol` before wire encode.
- Lib: `partial_state_wcs_edge_matches_oracle` in [`wcol_operator_chain.rs`](../../rust/wcol/tests/runtime/wcol_operator_chain.rs).
- [`ops/`](../../rust/wcol/src/ops/) query kernels: no `encode_frame` / WCS calls.

## Non-goals (still allowed)

- **WPP1** encode/decode in tests, chunk oracle, and `partial_wire_to_wpp1` for host parity.
- WCS **row** frames between decode operator (op 2) and query ingress (op 3) when planner splits placement — that is a **physical** edge, not inter-stage query serialization.
- [`JsonEncode`](../../rust/wcol/src/formats/json.rs) reads wire partial bytes at the sink boundary only.
