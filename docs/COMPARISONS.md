# Comparisons

Local measurements on macOS (Apple Silicon, Jun 2026). Reproduce with `npm run compare` after staging datasets under `data/` (not in git).

## Storage: Parquet vs `.wcol`

Encoder: `wcol-cli convert` (dictionary pages, typed columns, LZ4 chunk compression). Ratios are **wcol ÷ parquet** by on-disk size.

| Dataset | Parquet | `.wcol` | wcol / parquet |
|---------|--------:|--------:|---------------:|
| crates_versions | 97.4 MB | 71.3 MB | **73%** |
| crates_dependencies | 372.1 MB | 242.3 MB | **65%** |
| hits_subset_500k | 57.0 MB | 33.8 MB | **59%** |
| hits_subset_2m | 286.4 MB | 248.3 MB | **87%** |
| crates_categories | 3.6 MB | 3.5 MB | **100%** |
| flights-1m | 7.0 MB | 9.1 MB | **131%** |

**Takeaway:** On string-heavy and wide analytic tables (crates dumps, ClickBench subset), `.wcol` is typically **35–41% smaller** than Snappy Parquet. Tiny or already-compact tables can be a wash; highly numeric narrow sets (`flights-1m`) may be slightly larger until dictionary wins dominate.

---

## Query speed: DuckDB (native) vs wcol (native)

**Fixture:** ClickBench `hits` subset — **500k rows** (`data/hits_subset_500k.parquet` / `data/hits_subset_500k.refactor.wcol`, format v7).

**Queries** (from [wcol-sql-parser/readme.md](../rust/wcol-sql-parser/readme.md)):

| # | Query |
|---|-------|
| Q1 | `SELECT COUNT(*) FROM hits` |
| Q2 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0` |
| Q7 | `SELECT AdvEngineID, COUNT(*) … GROUP BY AdvEngineID ORDER BY COUNT(*) DESC` |
| Q14 | `SELECT RegionID, COUNT(DISTINCT UserID) … GROUP BY RegionID ORDER BY u DESC LIMIT 10` |

**wcol:** `wcol-cli bench`, runtime held open, **4 workers**, 2 warmup + 5 timed runs.

**DuckDB:** `@duckdb/node-api` — single in-process connection on the same Parquet file (`CREATE VIEW hits AS …`), 2 warmup + 5 timed runs. No CLI spawn overhead.

| Query | wcol mean | DuckDB mean | Faster |
|-------|----------:|------------:|--------|
| Q1 | **0.26 ms** | 8 ms | wcol ~30× |
| Q2 | **3.4 ms** | 7.5 ms | wcol ~2× |
| Q7 | **2.2 ms** | 6.4 ms | wcol ~3× |
| Q14 | **66 ms** | 26 ms | DuckDB ~2.5× |

**Caveats (read before comparing):**

1. **Q1 on wcol is metadata** — row count is stored in the file header; DuckDB still reads Parquet metadata/pages.
2. **Q14 on wcol uses `approx_count_distinct`** (ClickBench parity rewrite); DuckDB runs exact `COUNT(DISTINCT)` — different work, and DuckDB's hyperloglog-style path is mature here.
3. Different engines, different SQL surface — wcol runs a fixed plan kernel, not a full SQL planner.

**Takeaway:** On this 500k-row slice, wcol is **~2–3× faster** on filtered scans and group-by. DuckDB wins on the heavy exact-distinct aggregate. Q1 is not a scan benchmark for wcol.

---

## Decoder / query engine size: DuckDB Wasm vs wcol Wasm

Browser-relevant transfer sizes (gzip). wcol measured from this repo; DuckDB from `@duckdb/duckdb-wasm@1.29.0` on npm.

### Wasm module (decode + query kernels)

| Build | Raw | Gzip |
|-------|----:|-----:|
| wcol `build:wasm:size` | 196 KB | **77 KB** |
| wcol `build:wasm:speed` (Pages / explorer) | 336 KB | **114 KB** |
| wcol default `build:wasm` | 335 KB | **116 KB** |
| duckdb-wasm `duckdb-eh.wasm` | 34.0 MB | **6.9 MB** |
| duckdb-wasm `duckdb-coi.wasm` | 33.6 MB | **6.8 MB** |
| duckdb-wasm `duckdb-mvp.wasm` | 38.7 MB | **7.8 MB** |

### JS glue (orchestration, not the columnar engine)

| Artifact | Gzip |
|----------|-----:|
| wcol `dist/browser/main.js` | **10 KB** |
| wcol `dist/browser/worker.js` | **4 KB** |
| duckdb-wasm `duckdb-browser-eh.worker.js` | **743 KB** |
| duckdb-wasm `duckdb-browser-coi.worker.js` | **852 KB** (+ 641 KB pthread helper) |

### Minimal in-browser query stack (wasm + primary JS entry)

| Stack | Gzip total |
|-------|----------:|
| wcol size profile | **~91 KB** (77 + 10 + 4) |
| wcol speed profile | **~128 KB** (114 + 10 + 4) |
| duckdb-wasm EH (wasm + worker, no app code) | **~7.6 MB** |

**Takeaway:** wcol's Wasm decode+query target is **~60–85× smaller** gzipped than DuckDB Wasm. That gap is the main reason for a custom format: ship a columnar explorer in a blog post or dashboard without a multi-megabyte analytics runtime.

Native DuckDB CLI binary (~100 MB on disk) is a different deployment shape — full SQL, optimizer, extensions — and is not directly comparable to an in-page Wasm decoder.

---

## Reproduce

```bash
# storage + wasm sizes + native speed table (needs local data/, built wcol-cli, npm install)
npm run compare

# or step by step:
cargo build -p wcol-cli --release --manifest-path rust/Cargo.toml
node scripts/compare-report.mjs
```

DuckDB timings use `@duckdb/node-api` (devDependency) with one held connection — not the CLI.

# wasm size profile only
npm run build:wasm:size
gzip -c rust/wcol-wasm/pkg/wasm/wcol_wasm.simd.wasm | wc -c
```

Datasets are gitignored. Generate with `wcol-cli convert` from Parquet sources (ClickBench hits subset, crates.io dumps, etc.).
