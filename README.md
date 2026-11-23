# WCOL

Distributed SQL over Parquet on Cloudflare. A Rust engine, compiled to Wasm, runs inside Workers and Durable Objects. Native Rust reducers run in Cloudflare Containers when a query's group state is too large for Durable Objects.

It runs ClickBench (100M rows, one 14.8 GB Parquet file in R2) with nothing running between queries. Each job wakes the Durable Objects it needs. They hibernate when the job ends, and any containers are destroyed.

```text
POST /jobs ─► coordinator DO ─► plans the SQL into a graph of tasks
                    │
                    ├─► scan DOs        read Parquet from R2 by range, filter, partially aggregate
                    │        │ hash-partitioned streams (DO RPC)
                    ├─► reducer DOs     merge groups; spill sorted runs to R2
                    │   or containers   native reducer, up to the instance's memory, local-disk spill
                    │        │
                    └─► finalizer       sort / limit / format, in the coordinator
```

## What is here

| Path | What it is |
|---|---|
| `rust/wcol-engine` | Execution engine: Parquet decode, columnar batches, operators, the graph host that runs a task's fragment, spill |
| `rust/wcol-plan` | SQL parser, logical and physical planner, job planning. Runs in the coordinator |
| `rust/wcol` | Facade over the two, and the Rust test suite |
| `rust/wcol-node-wasm` | Wasm for scan and reducer Durable Objects (no planner) |
| `rust/wcol-coordinator-wasm` | Wasm for the coordinator: planning and the finalizer |
| `rust/wcol-reducer` | Native hash reducer served over HTTP, for container reducers |
| `src/runtime` | TypeScript host: feeds bytes and streams to the Wasm and runs the task graph. No SQL or column logic |
| `cloudflare/src` | The Workers: the coordinator and node Durable Objects, the container reducer class, the HTTP API |
| `cloudflare/console` | A browser console: submit queries, watch the graph run, see bytes and cost |
| `tests` | TypeScript tests, run against the release Wasm |
| `scripts` | Build, deploy and benchmark scripts |
| `docs` | [Architecture](docs/architecture.md), [job API](docs/job-api.md), and [illustrated explainers](https://davidgtonge.github.io/wcol/) |

Parquet's Thrift metadata and page decoding come from [`compact-parquet-thrift`](https://crates.io/crates/compact-parquet-thrift) ([source](https://github.com/davidgtonge/compact-parquet-thrift), [explainer](https://davidgtonge.github.io/compact-parquet-thrift/)).

## Build and test

You need Node 22, the Rust toolchain pinned in `rust/rust-toolchain.toml` (rustup installs it), the `wasm32-unknown-unknown` target, and `wasm-opt` from Binaryen.

```bash
git clone https://github.com/davidgtonge/wcol.git && cd wcol
rustup target add wasm32-unknown-unknown
npm install
(cd rust && cargo fetch)   # the Wasm build runs cargo --offline
npm run build          # both Wasm modules, then both Worker bundles
npm test               # TypeScript tests
npm run test:rust      # Rust tests
npm run typecheck
npm run lint:rust      # rustfmt and clippy
```

`npm run test:slow` runs the long spill tests. `npm run clickbench:local` runs ClickBench queries on Miniflare against a local Parquet file: `data/hits_subset_500k.parquet` by default, or a path passed after `--`. The `data/` directory is not in the repository.

## Deploy to Cloudflare

You need a Cloudflare account with Workers Paid (Durable Objects with SQLite storage, and Containers), an R2 bucket, and Docker to build the reducer image.

1. Set `bucket_name` under `[[r2_buckets]]` in `cloudflare/wrangler.toml` and `cloudflare/wrangler.node.toml`, and upload your Parquet file to it.
2. `npm run build:reducer` cross-compiles the native reducer for the container image.
3. `npm run build:wasm` if you have not already run `npm run build`. Deploy bundles the Workers itself but expects the Wasm to be built.
4. `npx wrangler login`, then `npm run deploy`, which deploys the node Worker, then the coordinator Worker, and waits for the rollout.
5. Optional: `npx wrangler secret put WCOL_API_TOKEN --config cloudflare/wrangler.toml` makes `/jobs` require `Authorization: Bearer <token>`.

The coordinator's URL serves the console at `/`. A job is a single request. `"bucket": "output"` names the Workers' R2 binding, not the bucket itself:

```bash
curl -X POST "$WCOL_BASE_URL/jobs" \
  -H "authorization: Bearer $WCOL_API_TOKEN" \
  -d '{
    "sources": { "hits": { "bucket": "output", "key": "hits.parquet" } },
    "sql": "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    "output": { "format": "json" }
  }'
```

`npm run clickbench:cloudflare` runs the ClickBench ladder against a deployment and checks answers against DuckDB. See the script's header for its options.

## Status

WCOL is a research project, not a product. It plans one source table per query: joins, window functions and a few ClickBench queries (Q18, Q35, Q42) are not supported.

Contributors, human or AI, should read [AGENTS.md](AGENTS.md), which sets out how the code is meant to be structured.

## License

MIT. See [LICENSE](LICENSE).
