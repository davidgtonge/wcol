# Run wcol on celld locally

[celld](https://celld.dev/) is a self-hosted Durable Objects runtime. This directory is the **local** Wrangler project for `celld dev`. Cloudflare production still deploys two Workers from `cloudflare/wrangler.toml` and `cloudflare/wrangler.node.toml`.

## Why one Worker here

celld runs **one application per fleet**. `celld deploy` accepts `script_name` on a Durable Object binding but does not keep it: the coordinator’s `WCOL_NODE` binding would resolve to a class on the same script, without the node Wasm. A dry-run of the split configs shows `WcolNode` on the coordinator as a non-SQLite Durable Object.

Local celld therefore uses [`wrangler.jsonc`](wrangler.jsonc) + [`entry.ts`](entry.ts): both classes, both Wasm modules, SQLite migrations for both. That is a celld-only composite (Cloudflare invariant I13 still holds for `wrangler deploy`).

The split configs remain for inspection and dry-run:

- [`wrangler.coordinator.jsonc`](wrangler.coordinator.jsonc)
- [`wrangler.node.jsonc`](wrangler.node.jsonc)

## One-time install

Apple Silicon and Linux. Pin a release if you want a known binary:

```sh
curl -fsSL https://celld.dev/install.sh | sh
# optional: CELLD_VERSION=v0.6.3 curl -fsSL https://celld.dev/install.sh | sh
```

Add `~/.local/bin` to `PATH` if the installer says to. `celld:dev` sets `CELLD_ESBUILD` to this repo’s `node_modules/.bin/esbuild`.

[Compatibility](https://celld.dev/docs/cloudflare-compat) · [R2](https://celld.dev/docs/services/r2)

## Run

From the `wcol` package root:

```sh
npm run celld:dev
```

That copies `cloudflare/dist` Wasm/JS into this directory, then starts celld on **http://127.0.0.1:9876**. Rebuild Wasm first if `dist/` is missing:

```sh
npm run build:node-wasm
npm run build:coordinator-wasm
npm run build:workers
npm run celld:dev
```

In another terminal:

```sh
npm run celld:smoke
```

Smoke seeds `OUTPUT` through `PUT /dev/r2/hits.parquet` (requires `WCOL_JOB_TEST_HOOKS=1`, which [`wrangler.jsonc`](wrangler.jsonc) and [`.dev.vars.example`](.dev.vars.example) set), then `POST /jobs` a GROUP BY against `data/hits_subset_500k.parquet` and checks DuckDB.

Use `--clean` after a binding or migration change. celld does not migrate stored state; a stale `.celld/dev` fails in ways that look unrelated:

```sh
npm run celld:prepare
celld dev cloudflare/celld/wrangler.jsonc --logs --clean
```

`.celld/`, `.dev.vars`, and the copied Wasm/JS are gitignored.

## Tuning

| Variable | Why |
|---|---|
| `CELLD_OPERATION_DEADLINE_MS` | Default 15s is tight for coordinator→node RPC chains. `celld:dev` sets `120000`. |
| `CELLD_MAX_CELL_REQUESTS` | Default 64 concurrent fetch events per cell. Over the limit, celld returns `503` with `X-Celld-Overload: cell`. |
| `CELLD_IDLE_EVICT_S` | Unset: idle cells stay resident unless pressured. Leave unset for smoke runs. |
| `WCOL_MAX_NODES` / `WCOL_MAX_ACTIVE_SCANS` | Local default is **8**. Concurrent scans buffer hash bytes, load parquet once through `wcol:r2-cache`, and skip progress R2 while the job runs. |
| `WCOL_BUFFER_SCAN_EDGES=1` | Buffer a scan’s hash output, then RPC completed `bytes` (not a stream). celld holds a streamed RPC until the body ends, so overlapping accept+write deadlocks. |
| `WCOL_JOB_SYNC_MS=0` | Return `202` immediately so `GET /jobs/:id` can poll. celld does not fire `setTimeout` during a compute-only Durable Object turn. |

## Differences vs Cloudflare

- No Workers observability / traces API.
- R2 objects live in the local store under `r2/<bucket_name>/`. Listing sort order can differ from Cloudflare for percent-encoded keys.
- A multipart upload stays on the node that opened it (fine for one-node `celld dev`).
- `celld r2 put` talks to a **fleet bucket**, not `.celld/dev`. Seed local R2 through `PUT /dev/r2/:key` (requires `WCOL_JOB_TEST_HOOKS=1`).
- `script_name` on Durable Object bindings is dropped at deploy. That is why this directory uses one Worker that exports both classes.
- Timers (`setTimeout`, `scheduler.wait`) stay frozen until an I/O boundary. Hash-scan yields use `crypto.subtle.digest` when `WCOL_BUFFER_SCAN_EDGES=1`.
- Concurrent range reads of one R2 object deadlock the local SQLite store. Scans load the object once via the `wcol:r2-cache` Durable Object, then slice in memory.
