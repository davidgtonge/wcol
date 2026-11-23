# DataFusion SQL frontend spike (wcol)

**Status:** in progress — **keep DataFusion logical planning; solve dependency weight, not SQL architecture.**

## Target architecture

```
SQL
  ↓
DataFusion SQL planner + analyzer        (datafusion-sql, datafusion-expr)
  ↓
DataFusion logical optimizer             (datafusion-optimizer; wcol-tuned rules)
  ↓
Optimized DataFusion LogicalPlan
  ↓
WCOL logical plan                        (today: IngestQuerySpec + ingest_spec_wire)
  ↓
WCOL physical optimizer                  (wcol: row groups, placement, coalescing, ops — not DataFusion)
  ↓
WCOL physical plan                       (ColocatedGraphPlan / opcode 10 / distributed jobs)
```

WCOL owns **physical** optimization and execution. DataFusion owns **SQL semantics and logical** optimization. We do **not** run DataFusion’s physical engine or RecordBatch execution.

**WCOL logical IR (current):** `IngestQuerySpec` (filters, group-by, aggregates, limit/offset, order) serialized on opcode **10** when `len & 0x8000_0000` (no SQL string). Renaming or extending to a fuller “WcolLogicalPlan” is a naming/product choice, not a different DataFusion boundary.

## Slim planner dependency profile (starting point)

Direct crates only — **not a guarantee of minimal linkage**:

```toml
datafusion-sql = { version = "55", default-features = false }   # drops unparser
datafusion-expr = { version = "55", default-features = false, features = ["sql"] }
datafusion-common = { version = "55", default-features = false, features = ["sql"] }
datafusion-optimizer = { version = "55", default-features = false }
# Minimal UDAFs + CoreFunctionPlanner only; avoid datafusion-functions default (regex/string/uuid/…)
datafusion-functions = { version = "55", default-features = false }
datafusion-functions-aggregate = { version = "55" }
```

Still linked today (planner-only consumers, no upstream “planner profile” yet):

- **Arrow** schemas / `ScalarValue` / casts (DataFusion’s IR — not optional without leaving DataFusion)
- **datafusion-functions-nested** via `datafusion-sql` (array SQL surface)
- **arrow-ipc** + codecs in `datafusion-common` manifest
- **sqlparser** full grammar (CREATE/ALTER code paths in the parser even for SELECT workloads)
- **tokio** (rt/sync) in common

**Tried:** `default-features = false` on sql + functions → native check OK; **planner wasm still ~10.7 MiB** (strip=symbols, LTO, opt=z). Most weight is transitive graph + **linking `wcol`** in `wcol-sql-wasm`, not optional scalar UDF modules.

## Two WASM modules (preferred if planner stays large)

```
SQL Planner WASM     DataFusion parse → analyze → optimize → compact WCOL IR (wire/spec)
        ↓
WCOL Engine WASM     Physical plan + GraphHost / DO execution (~655 KiB class today)
```

DOs and row workers load **engine only**; coordinator or a planning Worker loads **planner only**.

## Priority experiment (before a DataFusion fork)

Minimal export (real catalog, analyzer, optimizer, lowering — **no execution, no parquet I/O**):

```rust
// Target surface (name TBD)
pub fn optimize_sql(sql: &str, catalog: &WcolCatalog) -> Result<WcolLogicalPlan, PlanError>
```

**Measure:**

| Metric | Notes |
|--------|--------|
| Raw `.wasm` | release, `opt=z`, LTO, strip |
| Brotli | transfer ballpark |
| Cold instantiate | Worker / wasmtime |
| Plan latency | ClickBench Q3, Q12, Q8 on hits subset |

**Compare:**

1. **Planner-only wasm** — `wcol-sql` + lowering types **without** linking full `wcol` (move `ingest_spec_wire` to a thin `wcol-plan-ir` crate if needed).
2. **Current `wcol-sql-wasm`** — includes `wcol` for wire encoding (~10.7 MiB); **overstates planner-only cost**.

If (1) is still multi‑MiB after dropping `wcol`, pursue **upstream “planner-only” feature profile** (optional nested functions, arrow-ipc codecs, prettyprint) rather than reimplementing SQL optimization in wcol.

## Done in repo

| Item | Status |
|------|--------|
| `plan_sql()` + `wcol_optimizer()` (distinct rewrite rules off) | ✓ |
| Lowering → `IngestQuerySpec` + strict errors | ✓ |
| Opcode **10** spec wire + GraphHost (no SQL re-parse) | ✓ |
| E2E Q3, Q12, Q8 (distinct), DuckDB fixtures via spec + opcode 10 | ✓ |
| `wcol-sql-wasm`: `wcol_sql_plan_and_lower` (JSON summary + wire size) | ✓ |
| `plan_lower_bench` (native latency) | ✓ |

## Commands

```bash
cd wcol/rust
cargo test -p wcol-sql --features native --test datafusion_e2e
cargo test -p wcol --test ingest_spec_opcode10

# Planner wasm (needs LLVM clang for zstd on macOS)
export CC=/opt/homebrew/opt/llvm/bin/clang
cargo build --release --target wasm32-unknown-unknown -p wcol-sql-wasm
wc -c target/wasm32-unknown-unknown/release/wcol_sql_wasm.wasm
```

## Verdict

**Continue with DataFusion** for SQL + logical optimization. **Do not** replace that layer with wcol’s legacy SQL parser for new paths. Investment order: **fair planner-only wasm measurement → dual-module deployment → upstream/fork dependency trimming**.
