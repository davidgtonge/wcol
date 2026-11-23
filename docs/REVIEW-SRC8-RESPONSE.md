# Review `src(8).zip` — response tracker

Source inspection from external review (2026-10-28). Runtime verification: `cargo test` targets below.

## Fixed in tree (this pass)

| ID | Fix |
|----|-----|
| **P0** | [`ParquetIngestOp`](rust/wcol/src/exec/operators/parquet_wcs_ingest.rs) uses [`WcsEgress`](rust/wcol/src/exec/operators/wcs_frame.rs) partial flush (no whole-frame-or-nothing); refuses new frames while pending. Test: `parquet_wcs_ingest_backpressure`. |
| **P1** | [`host_read_request`](rust/wcol/src/formats/parquet/batch_decode.rs) returns `Result<Option<_>, _>`; `begin_row_group` errors propagate from `fill_batch` / read setup (not swallowed). Test: `invalid_plan_column_surfaces_on_fill_not_stall`. |
| **P1** | [`PageScratch::for_limits`](rust/wcol/src/formats/parquet/page_scratch.rs) splits codec budget across `previous`, `snappy_history`, and `snappy_output`. |
| **P1** | [`per_column_limits`](rust/wcol/src/formats/parquet/batch_decode.rs) fair split (`total / n`, min 1) — removed `.max(96)` per-column floor that exceeded group compressed budget. |
| **P2** | [`fill_batch`](rust/wcol/src/formats/parquet/batch_decode.rs) moves column payloads into `OwnedWcolBatch` without an extra clone pass. |
| **P2** | Dictionary-preserving ingest: footer `has_dictionary` on [`Chunk`](rust/wcol/src/formats/parquet/schema.rs), `dict_encoded` per plan chunk, [`dict_columns_from_ingest_groups`](rust/wcol/src/plan/physical.rs) wired through [`ColocatedGraphPlan::open`](rust/wcol/src/exec/colocated_plan.rs), opcode **2** / **10**, and TS vertical-slice encoders. Test: `ingest_groups_mark_dictionary_columns_from_footer`. |

## Still open (other agents / follow-up)

| ID | Item |
|----|------|
| **Arch** | Single Wasm executor (`GraphHost` only); retire opcode 2 dedicated ingest after WCS output adapter on graph. |
| **Validate** | Q3 repeated-RG memory plateau with corrected SUM/COUNT/AVG JSON (hand-written [`write_json_result`](rust/wcol/src/formats/json.rs)). |

## Suggested test command

```bash
cd wcol/rust && cargo test -p wcol --test streaming_compressed_window \
  --test parquet_wcs_ingest_backpressure --test flat_stream
```
