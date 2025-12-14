# Baseline commands

Commands for local development, CI-style gates, and optional perf spot-checks. Aligned with [minimal-wcol-stabilization-plan.md](./minimal-wcol-stabilization-plan.md) and [demo-and-validation-plan.md](./demo-and-validation-plan.md).

## Hard gates (fresh checkout)

```bash
npm run build:wasm
npm test
npm run typecheck
npm run check:rust
```

## Browser bundle and demo

```bash
npm run demo          # build:wasm + build:browser (includes demo/ → dist/browser/)
npm run demo:serve    # static server on http://localhost:5173
```

Open a local `.wcol` in the UI, or pass a URL if CORS allows range reads. Use **Warm worker pool** before parallel queries for steady timings.

```bash
npm run test:demo   # build + Playwright smoke (starts a temporary server on :5173)
```

## Optional dataset checks

```bash
npm run test:parity          # requires local fixtures
npm run test:fixtures        # read smoke against local .wcol
npm run perf:sanity          # Gate E timings; see docs/PERF_BASELINE.md
npm run perf:sanity:suite    # same + timestamped perf/sanity/<id>/
```

Perf harness env: `WCOL_PERF_FILE`, `WCOL_PERF_RUNS`, `WCOL_PERF_WARMUP`, `WCOL_PERF_WORKERS`. Skips with exit 0 when no `.wcol` fixture is found.

## Rust-only checks

```bash
npm run check:decoder
npm run check:encoder
npm run check:cli
npm run check:wasm
```

Or `npm run check:rust` for all of the above.

## Encode a file

```bash
cargo run --manifest-path rust/wcol-cli/Cargo.toml -- path/to/file.parquet -o path/to/file.wcol
```
