# I-07 — Dependency boundaries

**Status: pass (layering)** (2026-10-28)

## Target flow

```text
plan/  ops/  host
         ↓
      formats/     (Parquet / .wcol bytes → columns)
         ↓
      stream/      (WCOL batches, WCS framing, PWCL, projection blobs)
         ↓
      exec/        (operators, graph, Wasm pipeline)
```

**Rule:** `stream/` does not import `exec/` or `formats/`. Higher layers may use lower layers.

## Canonical homes (2026-10-28)

| Concern | Module |
|---------|--------|
| WCS1 frame encode | [`stream/wcs.rs`](../../rust/wcol/src/stream/wcs.rs) |
| `WCOLpjv1` projection | [`stream/projection.rs`](../../rust/wcol/src/stream/projection.rs) |
| Parquet dict-page probe | [`stream/dictionary.rs`](../../rust/wcol/src/stream/dictionary.rs) |
| Byte pipeline surface | [`stream/wire.rs`](../../rust/wcol/src/stream/wire.rs) → `stream/core` + `stream/wcs` |

Back-compat: [`exec/runtime/wcs.rs`](../../rust/wcol/src/exec/runtime/wcs.rs), [`formats/parquet/projection.rs`](../../rust/wcol/src/formats/parquet/projection.rs) re-export from `stream/`.

## Plan ↔ exec

[`plan/physical.rs`](../../rust/wcol/src/plan/physical.rs) routes metadata-only `COUNT(*)` via [`ColocatedGraphPlan::would_metadata_count`](../../rust/wcol/src/exec/colocated_plan.rs) (SQL shape only — no graph open, no `exec` import from `plan`).

## Evidence

- Rust test [`dependency_boundaries.rs`](../../rust/wcol/tests/runtime/dependency_boundaries.rs) — scans `src/stream/*.rs` for forbidden `crate::exec::` / `crate::formats::` imports (doc comments excluded).

## Remaining (not I-07 blockers)

- Public `pub use` shims at crate root and `exec/mod.rs` for stable API.
- `exec/` naturally depends on `formats/`, `ops/`, and `stream/`.
