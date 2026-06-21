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

Five bundled `.wcol` files (~50 MB) are committed under `demo/data/` so the demo works from a fresh clone and in CI:

| File | Rows (approx) | Size |
|------|----------------|------|
| `hits_subset_500k.wcol` | 500k | ~34 MB |
| `trends_crate_downloads_30d.wcol` | ~271k | ~4 MB |
| `crate_maintainers.wcol` | ~307k | ~9 MB |
| `crates_categories.wcol` | ~237k | ~3.5 MB |
| `trends_serde_version_downloads.wcol` | ~315 | ~4 KB |

Larger tables (`crates_versions`, `crates_dependencies`, `version_downloads_daily`) are optional — encode under `data/` and run `npm run prepare:datasets` to copy them in locally. See [demo/data/README.md](./demo/data/README.md).

## Layout

| Path | Role |
|------|------|
| `demo/` | Preact UI, charts, wiring |
| `engine/` | `wcol-engine` — canonical state, CBOR wire, view-model patches |
| `scripts/` | Wasm + esbuild bundle for static hosting |

Output: `apps/explorer/dist/browser/` (uploaded to GitHub Pages in CI).

CI and `npm run build` here use the **speed** Wasm profile (`build:wasm:speed` on the root package) for interactive query performance.
