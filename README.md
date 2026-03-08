# wcol

**Columnar storage for the browser and native** — explore whether you can match Parquet-class compression with a Wasm decoder small enough to ship in a web page, and run DuckDB-style analytical queries across multiple workers.

## The idea

I wanted to see if I could build a columnar format that:

1. **Compresses like Parquet** — dictionary encoding, typed pages, LZ4 chunk compression, range-friendly layout for HTTP `Range` reads.
2. **Decodes in a tiny Wasm module** — design target **&lt;100 KB gzipped** over the wire. With `npm run build:wasm:size` the SIMD decoder is **~77 KB gzipped** (~196 KB raw). The default release build trades size for speed (**~117 KB gzipped**, ~335 KB raw).
3. **Queries like DuckDB** — filters, group-by, aggregates, top-K, late `SELECT` projection over millions of rows without loading everything into JS heap.
4. **Scales across processes** — browser `Worker` pool for chunk-parallel scans; native thread pool in `wcol-cli` for the same plan on disk.

**wcol** is the result: a `.wcol` file format (v7), Rust encoder/decoder, a ~30 KB JS orchestration layer, and optional apps on top.

## Monorepo layout

| Path | Required | What it is |
|------|----------|------------|
| `rust/wcol-format` | yes | Format constants and layout |
| `rust/wcol-encoder` | yes | Parquet → `.wcol` |
| `rust/wcol-decoder` | yes | Decode + query kernels (native + Wasm) |
| `rust/wcol-wasm` | yes | Wasm FFI surface |
| `rust/wcol-cli` | yes | `convert`, `query`, `bench`, DuckDB parity |
| `rust/wcol-sql-parser` | yes | SQL → plan (CLI path) |
| `src/` | yes | JS runtime — `WcolFile`, worker pool, `QueryPlan` |
| `apps/explorer/` | **optional** | Crates.io explorer demo (Preact + `wcol-engine`) |
| `engine-shell/` | explorer only | Git submodule for the demo worker scaffold |

You do **not** need `apps/explorer` or `engine-shell` to use the columnar runtime or CLI.

## Quick start (runtime only)

```bash
git clone https://github.com/davidgtonge/wcol.git
cd wcol
npm install
npm run build          # Wasm + browser bundle → dist/browser/
```

```js
import { WcolFile, buildPlan } from "./dist/browser/main.js";

const file = await WcolFile.open("https://example.com/data.wcol");
const result = await file.query(
  buildPlan({
    filters: [{ column: "country", op: "==", value: "US" }],
    aggregates: [{ column: "price", op: "sum" }],
    limit: 100,
  }),
  { workers: 4 },
);
```

## CLI (native, multi-worker)

```bash
cargo run -p wcol-cli -- convert data/hits.parquet -o data/hits.wcol
cargo run -p wcol-cli -- query --file data/hits.wcol \
  --sql "SELECT COUNT(*) FROM hits;" --workers 4 --format summary
```

## Optional explorer demo

Interactive workspace over [crates.io](https://crates.io/data-access) dumps — query builder, charts, crate detail, saved views.

```bash
git clone --recurse-submodules https://github.com/davidgtonge/wcol.git
cd wcol
npm install
npm run build -w @wcol/explorer
npm run serve -w @wcol/explorer   # http://localhost:5173
```

**Live demo:** https://davidgtonge.github.io/wcol/ (built from `apps/explorer` in CI)

Large `.wcol` fixtures are not in git. Stage locally with `npm run prepare:datasets -w @wcol/explorer` after generating parquet/wcol under `/data`.

GitHub Pages builds with the **speed** Wasm profile (`npm run build:wasm:speed`) for faster queries in the browser.

## Architecture (summary)

```txt
JS (src/runtime)     — byte I/O, chunk queue, worker pool, result merge
       ↓ QueryPlan
Wasm (wcol-decoder)  — page decode, filters, group-by, aggregates
```

- **One query at a time per open file** — plan handle lifecycle is explicit.
- **Parallel when safe** — multiple workers scan disjoint chunks; reducer merges partial aggregates.
- **Worker failure → local fallback** when workers were auto-detected.

Details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/FORMAT.md](docs/FORMAT.md).

## Tests

```bash
npm run check:rust     # cargo check all core crates
npm run test           # JS runtime + plan unit tests
npm run test -w @wcol/explorer   # explorer query catalog (needs staged .wcol data)
```

## Related

- [query-predicate](https://github.com/davidgtonge/query-predicate) — Mongo-style filter predicates
- [engine-shell](https://github.com/davidgtonge/engine-shell) — only used by `apps/explorer`

## License

MIT — see [LICENSE](LICENSE).
