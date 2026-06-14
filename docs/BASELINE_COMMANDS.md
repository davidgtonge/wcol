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
npm run build:wasm:size    # optimize for size (Rust `z` + wasm-opt `-Oz`)
ls -lh rust/wcol-wasm/pkg/wasm/
```

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
