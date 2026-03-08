# Baseline commands

Core runtime and CLI checks. The explorer demo is optional — see [apps/explorer/README.md](../apps/explorer/README.md).

## Build

```bash
npm install
npm run build              # Wasm + dist/browser runtime bundle
npm run check:rust         # cargo check all workspace crates
npm run test               # JS runtime unit tests
```

## Wasm size profile

```bash
npm run build:wasm:size    # Rust `opt-level=z` + wasm-opt `-Oz`
# simd: ~77 KB gzipped, ~196 KB raw (Jun 2026)
ls -lh rust/wcol-wasm/pkg/wasm/
gzip -c rust/wcol-wasm/pkg/wasm/wcol_wasm.simd.wasm | wc -c
```

Default `npm run build` uses the speed-oriented profile (~117 KB gzipped).

## Comparisons (Parquet / DuckDB / wasm sizes)

```bash
cargo build -p wcol-cli --release --manifest-path rust/Cargo.toml
npm run compare
```

See [COMPARISONS.md](COMPARISONS.md) for methodology and tables.

## CLI smoke

```bash
cargo run -p wcol-cli -- --help
cargo run -p wcol-cli -- query --file data/hits_subset.wcol \
  --sql "SELECT COUNT(*) FROM hits;" --workers 2 --format summary
```

## Explorer (optional)

```bash
git submodule update --init
npm run build -w @wcol/explorer
npm run test:cbor-interop -w @wcol/explorer
```
