# Explorer demo (optional)

Browser workspace for exploring [crates.io database dumps](https://crates.io/data-access) with wcol — query builder, charts, crate detail panels, saved views, and URL-shareable state.

This app is **not required** to use wcol as a library or CLI. It lives under `apps/explorer/` and depends on the [`engine-shell`](../../engine-shell) submodule.

## Build

From the repo root (after `git submodule update --init`):

```bash
npm install
npm run build -w @wcol/explorer
npm run serve -w @wcol/explorer
```

## Datasets

`.wcol` bundles are gitignored. Stage fixtures into `demo/data/`:

```bash
# From repo root, after preparing parquet/wcol under /data
npm run prepare:datasets -w @wcol/explorer
```

| File | Rows (approx) |
|------|----------------|
| `crates_versions.wcol` | ~2.4M |
| `crates_dependencies.wcol` | ~27M |
| `version_downloads_daily.wcol` | ~35M |

See [demo/data/README.md](./demo/data/README.md) for the full list.

## Layout

| Path | Role |
|------|------|
| `demo/` | Preact UI, charts, wiring |
| `engine/` | `wcol-engine` — canonical state, CBOR wire, view-model patches |
| `scripts/` | Wasm + esbuild bundle for static hosting |

Output: `apps/explorer/dist/browser/` (uploaded to GitHub Pages in CI).

CI and `npm run build` here use the **speed** Wasm profile (`build:wasm:speed` on the root package) for interactive query performance.
