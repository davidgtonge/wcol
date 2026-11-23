# df-planner-wasm

Minimal **DataFusion-only** Wasm: SQL + catalog JSON → optimized logical plan string. **No `wcol` dependency.**

## Build

From `rust/`:

```bash
cargo build -p df-planner-wasm --target wasm32-unknown-unknown --profile wasm-size
```

Release (default workspace profile, faster compile, larger binary):

```bash
cargo build -p df-planner-wasm --target wasm32-unknown-unknown --release
```

Size report:

```bash
./scripts/measure-df-planner-wasm.sh wasm-size
```

## Export

- `df_plan_sql(sql: string, catalog_json: string) -> string` — JSON with `optimized_logical`, `analyzed_logical`, `raw_logical`, `required_columns`.
- `df_planner_wasm_version() -> u32`

Catalog JSON matches `wcol-sql-wasm` / `export_hits_catalog`:

```json
{ "table": "hits", "columns": [{ "name": "...", "physical": 1, "type_length": -1, "max_def": 1, "max_rep": 0 }] }
```

## Native sanity check

```bash
cargo test -p df-planner --test plan_smoke
```

### Node timing (wasm load + plan)

```bash
node scripts/build-df-planner-wasm.mjs
node scripts/time-df-planner-node.mjs          # warm-ish (same process)
node scripts/time-df-planner-node.mjs --spawn  # cold process
```

Worktree branch: `spike/df-planner-minimal` at repo root `wcol-df-planner-minimal`.
