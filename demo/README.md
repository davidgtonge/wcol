# Browser demo — crates.io

Interactive demo for the wcol prototype using the **full published crate versions** table from the [crates.io database dump](https://crates.io/data-access).

## Run locally

```bash
npm run demo        # WASM + demo/data/crates_versions.wcol + dist/browser/
npm run demo:serve  # http://localhost:5173
```

Open the app and click **Load crates.io dataset** (serves `data/crates_versions.wcol` from the static bundle). You can also pick a local `.wcol` via the file picker.

## Dataset

| File | Rows | Size |
|------|------|------|
| `demo/data/crates_versions.wcol` | ~2.4M | ~71 MB |

Built from `data/crates_versions.parquet`:

```bash
./scripts/prepare-crates-parquet.sh
./scripts/convert-crates-wcol.sh
npm run demo
```

`prepare-demo-crates.mjs` copies `data/crates_versions.wcol` into `demo/data/` when present, or converts from parquet.

## Presets (crates.io)

| Preset | What it shows |
|--------|----------------|
| Top crates | `GROUP BY crate_name`, sum downloads — horizontal bar chart |
| By license | `GROUP BY license` — vertical bar chart |
| Edition × yanked | Two-key group-by — grouped bars |
| Projection | Late materialize `crate_name`, `license`, `downloads` — table |
| Popular versions | Filter `downloads > 1M` — row preview |

ClickBench **hits** presets appear automatically if you load a hits-format file (column names `CounterID`, etc.).

## Features

- **Data drawer** — load URL/file, schema, workers (off the main canvas)
- **Query builder** — text search (LIKE), filters, aggregate / table / find-rows modes
- Default **top-K (25)** on projections and group-by (engine fast-path when possible)
- Polished two-column UI: explore on the left, live timing + charts on the right
- Example queries at the bottom of the builder panel
- HTTP range open (`HttpRangeSource`) for hosted `.wcol`
- Collapsible schema + query plan JSON
- Worker pool warm-up, parallel `file.query(plan)`
- Default preset runs automatically after load (warm → query)
- Dict columns resolved to strings in projection / group previews

## Layout

| Path | Role |
|------|------|
| `index.html` | Shell (`#root` mount) |
| `app.tsx` | Entry — mounts `createApp` |
| `arch/events.ts` | `Event` union + panel event extracts |
| `arch/events.handlers.ts` | Reducer handler dict, `update`, `initialState` |
| `arch/effects.ts` | `Effect` union |
| `arch/effects.handlers.ts` | Async effect handler dict, `runEffect` |
| `arch/view-model.ts` | `ViewModel` UI slice + `projectViewModel(AppState)` |
| `arch/protocol.ts` | Worker wire types (`WorkerInput` / `WorkerOutput`) |
| `arch/cbor.ts` | CBOR encode/decode for worker messages |
| `arch/worker-client.ts` | Main-thread CBOR client + `openFile` transfer |
| `arch/worker-session.ts` | Canonical state + engine + Wasm effects (worker) |
| `arch/patch.ts` | Generic `diffViewModel` + `applyPatches` |
| `arch/view-model-store.ts` | Cached view model + patch batches |
| `worker/app-worker.ts` | Web Worker entry (Wasm + engine) |
| `arch/use-selector.ts` | `useSelector` via `useSyncExternalStore` |
| `arch/app-context.tsx` | Store + dispatch provider |
| `wiring/` | Connected components (`useSelector` → `input`) |
| `arch/runtime.tsx` | Preact shell — no Wasm on main thread |
| `components/` | Pure views (`input`, `onEvent` only) |
| `charts/` | Unified `BarChart`, grouped bars, tables |
| `data/presets.ts` | Crates.io query presets |
| `wcol-runtime.ts` | Re-exports wcol API from bundled `main.js` |
| `styles.css` | Layout + chart styling |
| `data/crates_versions.wcol` | Staged full dataset (not in git) |

Built to `dist/browser/` by `npm run build:demo` (`app.js` + `demo-worker.js` + `main.js` Wasm shim).

Architecture matches [rust-ts-arch.md](../docs/rust-ts-arch.md): canonical state and Wasm run in the worker; main thread receives CBOR-encoded view-model patches and renders Preact.

Styling uses the [Tailwind Play CDN](https://tailwindcss.com/docs/installation/play-cdn) (`cdn.tailwindcss.com`) — no build step. Shared class strings live in `ui/classes.ts`; `styles.css` only covers SVG chart text fills.

## Browser test

```bash
npm run test:demo
```

Uses `dist/browser/data/crates_versions.wcol` after `npm run demo`.
