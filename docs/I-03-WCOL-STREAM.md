# I-03 — One logical WCOL stream

**Status: pass (complete)** (2026-10-28)

## In-process (pass)

Colocated operators exchange [`WcolStreamItem`](../../rust/wcol/src/exec/operator.rs):

- `WcolStreamItem(OwnedWcolBatch)` — sole logical carrier; row vs partial distinguished by [`OwnedWcolBatch::is_partial_batch`](../../rust/wcol/src/stream/batch/owned_batch.rs) (partial batches attach `PartialStateSchemaV1` + `passed_rows` metadata)
- Filter attaches [`SelectionMask`](../../rust/wcol/src/stream/batch/selection_mask.rs) on the batch (no column compaction); aggregate consumes via [`effective_selection_mask`](../../rust/wcol/src/stream/batch/owned_batch.rs)

Filter → Aggregate → Reduce → Finalize never serialize between stages.

## Physical WCS edge (pass)

WCS1 `FRAME_PARTIAL` (type 4) payload is **`PWCL`**:

| Layer | Location |
|-------|----------|
| Logical | [`PartialStateWcol`](../../rust/wcol/src/stream/partials.rs) |
| Wire body | [`partial_state_to_wcs_payload`](../../rust/wcol/src/stream/partial_wcs.rs) |
| Frame | [`GraphHost`](../../rust/wcol/src/exec/graph_host.rs) (`EmitPartial`) |

Decode: [`partial_state_from_wire_payload`](../../rust/wcol/src/stream/partial_wcs.rs) (`PWCL` or legacy `WPP1`).

## TypeScript host oracle (pass)

Canonical module: [`partial-oracle.ts`](../../src/runtime/stream/partial-oracle.ts)

| API | Role |
|-----|------|
| `parsePartialQueryFromWire` / `…Auto` | Rust normalize → thin WPP1 **view** (`parquetQueryFromWpp1Partial`) |
| `partialFirstSumFromWire` / `…Auto` | `wcol_partial_first_sum_i` |
| `partialsEquivalent` | ClickBench / HTTP parity (PWCL vs legacy oracle WPP1) |

Transport-only: [`wcs-transport.ts`](../../src/runtime/stream/wcs-transport.ts) (`lastPartialPayload`).

**Not a second engine:** TS never parses PWCL columns; Wasm owns wire semantics.

Legacy parquet chunk oracle still emits WPP1; comparisons normalize the stream side only.

## Future (I-12)

- Remote reduce merge on typed partial batches without WPP1 shim.
- Unified WCS batch-kind enum (row projection vs partial `PWCL`).
