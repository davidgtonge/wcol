# Review `src(9).zip` — response tracker

## Fixed in tree

| ID | Fix |
|----|-----|
| **P0** | [`WcsEgress`](rust/wcol/src/exec/operators/wcs_frame.rs): pending frame + `pending_at` (no tail realloc on partial flush). [`ParquetIngestOp`](rust/wcol/src/exec/operators/parquet_wcs_ingest.rs): `end_queued` set when END is enqueued once; no duplicate END on later polls. Test: `parquet_wcs_ingest_emits_single_end_frame_on_tiny_ring`. |
| **P0** | [`PlannedChunk::dict_encoded`](rust/wcol/src/formats/parquet/batch_decode.rs) per row group via [`ParquetDecode::from_ingest_groups`](rust/wcol/src/formats/parquet/batch_decode.rs); colocated + opcode 2 ingest use ingest plan flags. Test: `batch_decode_mixed_dict::per_row_group_dict_flag_not_global`. |
| **P1** | [`PageScratch`](rust/wcol/src/formats/parquet/page_scratch.rs): Snappy/delta-byte scratch cannot grow past budget; errors propagate from [`numeric_scan`](rust/wcol/src/formats/parquet/numeric_scan.rs). Tests: `page_scratch` unit tests. |
| **P1** | Opcode 2 path encodes [`DictId`](rust/wcol/src/stream/projection.rs) via `encode_projection_columns` (not I64-only). |

## Still open (architecture / follow-up)

| ID | Item |
|----|------|
| **Arch** | ~~Retire opcode 2 WireOperator~~ **Done**: deleted `parquet_wcs_ingest.rs`; opcode **2** opens [`GraphHost::open_parquet_wcs_projection`](rust/wcol/src/exec/graph_host.rs) + [`ParquetWcsProjectionDriver`](rust/wcol/src/exec/parquet_wcs_projection.rs). |
| **Arch** | Single executor: drop `WcolSession::Transport(Pipeline)` vs `Graph(GraphHost)` duality. |
| **Plan** | `PLAN_V2` ingest chunks include trailing `dict_encoded` u8 — all producers/consumers must stay aligned (TS + Rust encoders updated together). |
| **P2** | WPQ1 describe buffer: dictionary flag deferred until `wcol_parquet.wasm` rebuild path is unified. |
| **JSON** | `JsonEncode` still snapshot/PWCL-oriented, not finalized WCOL result batches. |

## Suggested tests

```bash
cd wcol/rust && cargo test -p wcol --test parquet_wcs_ingest_backpressure \
  --test batch_decode_mixed_dict
cargo test -p wcol --lib page_scratch
```
