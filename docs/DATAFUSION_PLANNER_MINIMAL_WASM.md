# Minimal DataFusion planner Wasm (worktree spike)

Branch **`spike/df-planner-minimal`** in worktree **`wcol-df-planner-minimal`** (sibling of main `wcol` checkout).

## Goal

Measure the **smallest Wasm** that runs:

**SQL + catalog JSON → DataFusion optimized `LogicalPlan` (display string)**

with **zero `wcol` dependencies** — baseline before adding ingest lowering or linking the engine.

## Crates

| Crate | Role |
|-------|------|
| `rust/df-planner` | Native library: parse → SqlToRel → Analyzer → Optimizer |
| `rust/df-planner-wasm` | `cdylib`: `df_plan_sql` wasm_bindgen export |

Dependencies are slimmed with `default-features = false` on `datafusion-sql`, `datafusion-expr`, `datafusion-common`, `datafusion-optimizer`, `datafusion-functions`. Aggregates still register `count/sum/avg/min/max` for ClickBench-style SQL.

## Size (macOS, DataFusion 55.2, `wasm-size` profile)

| Build | Raw | Gzip |
|-------|-----|------|
| After codec/format patches (`arrow` + `datafusion-common`) | **13.64 MiB** (14,303,904 B) | **3.23 MiB** (3,389,516 B) |
| Before `arrow` patch (zstd already removed) | ~13.66 MiB | ~3.20 MiB |
| First `release` (pre-trim) | ~37 MiB | ~7.7 MiB |

Codec/format patches mostly clean the **dependency tree**; wasm binary size moved only slightly (LTO already dropped unused code). Next lever is shedding `object_store` / physical execution crates, not more Arrow format flags.

```bash
./scripts/measure-df-planner-wasm.sh wasm-size
./scripts/check-df-planner-codecs.sh   # must pass (no zstd/lz4)
```

**Codec / format trim (done):**

- `[patch.crates-io] datafusion-common` — no `arrow-ipc` lz4/zstd; `file_csv` feature gates CSV writer (off for `df-planner`).
- `[patch.crates-io] arrow` — `default = []` so meta crate does not pull `arrow-csv` / `arrow-json` / extra IPC.

`arrow-ipc` remains (slim, no codecs) for spill config types. `object_store` still comes from `datafusion-execution` via scalar functions.

## Node / Worker timing

| Environment | Wasm load | First plan | Warm plan |
|-------------|-----------|------------|-----------|
| Node 22 (`time-df-planner-node.mjs --spawn`) | ~13 ms | ~38 ms | ~1.4 ms |
| Wrangler local (`8799`, first isolate request) | ~9 ms | ~126 ms | ~4 ms |

Worker uses `pkg-web` (`wasm-bindgen --target web`) + `init({ module_or_path })`; Node uses `pkg/` (`--target nodejs`).

Next trimming experiments (not done here): drop `datafusion-functions-nested` + execution/physical paths, `wasm-opt -Oz`, fewer UDAFs.

## Related

Main decision doc: `docs/DATAFUSION_SQL_SPIKE.md` on `main` wcol tree.
