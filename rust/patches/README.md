# Patched upstream crates (spike)

## `datafusion-common`

Upstream 55.2.0 enables `arrow-ipc` with `lz4` + `zstd` for spill/config types. Logical SQL planning does not need those codecs.

Changes vs crates.io:

- `arrow`: `default-features = false` (avoid pulling `ipc`/`csv`/`json` through the meta crate)
- `arrow-ipc`: no `lz4` / `zstd` features

## `datafusion-sql`

Remove hard dependency on `datafusion-functions-nested` (which pulled `datafusion-functions` → `datafusion-execution` → `object_store`). ANY/ALL array SQL returns a clear planner error instead.

## `df-planner` (direct deps)

- UDAFs from `datafusion_expr::test::function_stub` (logical signatures only; no `datafusion-functions-aggregate`).
- No `datafusion-functions` / `CoreFunctionPlanner` (ClickBench path does not need scalar UDF registry).

## Still linked: `datafusion-physical-expr`

`datafusion-optimizer` uses it in `SimplifyExpressions`, `PushDownFilter`, and `decorrelate` (`evaluates_to_null` via mini physical eval). Removing it requires a large optimizer fork or disabling those rules — not done here.

## `arrow` (59.3.2)

Upstream default features enable `csv`, `ipc`, and `json` on the meta crate. DataFusion never sets `default-features = false` on `arrow`, so those formats are unified into every build.

Change: `default = []` (callers still enable `prettyprint`, `chrono-tz`, etc. explicitly).
