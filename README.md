# wcol

Browser-first columnar analytics for `.wcol` files — Rust/Wasm compute, JS orchestration, and an interactive **crates.io explorer** demo.

**Live demo:** https://davidgtonge.github.io/wcol/

## What this is

| Layer | Location | Role |
|-------|----------|------|
| Columnar runtime | `src/runtime/` + `rust/wcol-{decoder,wasm}` | Decode `.wcol`, plan queries, worker pool, chunk merge |
| App engine | `rust/wcol-engine/` | Canonical state, CBOR wire, view-model patches ([engine-shell](https://github.com/davidgtonge/engine-shell)) |
| Explorer UI | `demo/` | Preact workspace — query builder, charts, crate detail, saved views |

The `.wcol` format is a compressed columnar layout tuned for browser range reads and SIMD-friendly decode. The demo ships with public [crates.io database dump](https://crates.io/data-access) datasets (~2.4M crate versions and related tables).

## Quick start

```bash
git clone --recurse-submodules https://github.com/davidgtonge/wcol.git
cd wcol
npm install
npm run build:demo    # Wasm runtime + app engine + static bundle
npm run demo:serve    # http://localhost:5173
```

Open the app and load the bundled crates.io dataset, or pick a local `.wcol` file.

### Regenerate datasets (optional)

Large `.wcol` bundles are not committed. To rebuild from parquet:

```bash
./scripts/prepare-crates-parquet.sh
./scripts/convert-crates-wcol.sh
npm run prepare:demo
```

## Architecture

```txt
Preact UI  →  CBOR AppEvent  →  Worker  →  wcol-engine (Wasm)
                ↑                              ↓
         applyPatchBatch  ←  patches + effects  ←
                ↓
         wcol runtime (Wasm) — open file, execute QueryPlan, charts
```

- **JS** owns I/O, worker pool warm-up, and effect execution (`OpenSource`, `RunQuery`, `LoadCrateDetail`).
- **Rust/Wasm** owns game-like app state: routes, query drafts, saved views, pinned crates, undo/redo.
- **Columnar runtime** runs inside the same worker for `file.query(plan)` against loaded `.wcol` files.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/FORMAT.md](docs/FORMAT.md).

## CLI (native)

Convert parquet and run queries from the shell:

```bash
cargo run -p wcol-cli -- convert data/example.parquet -o data/example.wcol
cargo run -p wcol-cli -- query --file data/example.wcol \
  --sql "SELECT COUNT(*) FROM hits;" --workers 4 --format summary
```

## Tests

```bash
npm run check:rust          # cargo check all workspace crates
npm run test                # runtime + plan unit tests
npm run test:explorer       # explorer query catalog (needs demo dataset)
npm run test:cbor-interop   # engine CBOR round-trip
```

## Related repos

- [engine-shell](https://github.com/davidgtonge/engine-shell) — shared Wasm worker scaffold
- [rust-weather-spiral](https://github.com/davidgtonge/rust-weather-spiral) — same rust-ts boundary pattern
- [query-predicate](https://github.com/davidgtonge/query-predicate) — Mongo-style filter predicates (used in enrichment stacks)

## License

MIT — see [LICENSE](LICENSE).
