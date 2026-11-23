# Page-copy compaction (v1)

Physical Parquet compaction without WCOL batch decode: read compressed column-chunk bytes, concatenate row groups, rewrite footer.

## Rust

| Piece | Location |
| ----- | -------- |
| Footer scan + validation | `formats/parquet/schema.rs` (rejects dictionary, indexes, bloom) |
| Planner (~600 MB parts, ~128 MiB merged row groups) | `plan/page_compaction.rs` |
| Wasm opcode **11** sink | `formats/parquet/page_compact.rs` |
| Host | Same as row export: `GraphHost` byte sink + ReadAt (`source` index) + WritePort |
| CLI planner | `rust/wcol/src/bin/page_compact_plan.rs` |

Wire plan: `encode_page_compact_wire(part, leaves)`.

Footer rewrite uses `compact-parquet-thrift::encode::write_footer` with merged chunk layout (not copied source chunk metadata). Statistics and indexes are omitted on the merged file.

## Cloudflare

| Piece | Location |
| ----- | -------- |
| Coordinator | `cloudflare/src/page-compact-route.ts` → `POST /page-compact` |
| WcolNode worker | `handlePageCompactRequest` → `POST /compact` on DO |
| R2 reads | `R2RangeSource` — validated `inputKeys` from source export manifest only |
| Trial script | `scripts/cf-page-compact.ts` (3 → 1) |
| Full bench | `scripts/cf-page-compact-full.ts` (16 → 4, metrics, `--fail-worker` + `--retry`) |

## Not in v1

- Dictionary merge, codec change, schema evolution
- Manifest-aware cleanup (separate job)

## Local test

```bash
cd rust/wcol && cargo test --test page_compact
```

Integration test merges three row groups into one output file via opcode 11.

## Full bench (16-part R2 export)

1. Download `hits-eq0-bench-w16-1-w16-manifest.json` and local footers via `page_compact_plan`.
2. `plan_page_compaction` → 4 parts, ~128 MiB row groups.
3. Fan out 4 WcolNode jobs; each reads sources by R2 binding index (`readAt(source, …)`).
4. Publish manifest when all parts succeed; DuckDB `EXCEPT ALL` both ways vs the 16 inputs.

```bash
cd wcol
npx tsx scripts/cf-page-compact-full.ts
```

Metrics in the published manifest: `httpBytesRead`, `readAtCalls`, `wasmMemoryBytes`, `inputRowGroups` / `outputRowGroups`, per-part timing splits.
