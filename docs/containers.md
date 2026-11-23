# Container reducers

A node reducer is a Durable Object. It shares a 128 MB isolate with the engine's buffers, so it keeps 16 MiB of groups in memory and spills the rest to R2 as sorted runs. That suits most ClickBench queries. For a query like Q32 (`GROUP BY WatchID, ClientIP`, about 100M groups), it means writing gigabytes of spill to R2 and merging it again.

A container reducer runs the same engine as a native binary (`rust/wcol-reducer`) in a Cloudflare Container. It has the instance's memory, and spills to local disk.

## When they are used

A job chooses with `"reducers"`: `"node"`, `"container"` or `"auto"` (the default). With `auto`, the planner estimates the aggregate's group state. If that is more than node reducers hold in memory (128 × 16 MiB = 2 GiB), the job uses 64 `basic` containers. Otherwise it uses node reducers, which wake in under 200 ms, where a container takes 2–6 s to start. The choice and its reason appear in the job record and the console.

## How they run

- The container app uses the `durable_object` scheduling policy. Nothing is provisioned until a `WcolReducer` object starts its container.
- Each reducer in each attempt starts a fresh container when the coordinator prepares it. The coordinator prepares reducers before the scans start, so container start-up overlaps the scans' first reads.
- The reducer's HTTP API is small: `POST /start` with the reducer program, `PUT /input/{i}` for each scan edge (streamed), `POST /absent/{i}` for a scan with no rows for this partition, `GET /output` and `GET /status`. The `WcolReducer` object proxies scan edges to it. Each connection gets its own thread, so every edge is read as it arrives.
- Once the finalizer has read the output and the status is `ok`, the object destroys the container. An aborted attempt destroys it too. The inactivity timeout is only a backstop.
- The image is one static (musl) binary on `scratch`. `npm run build:reducer` cross-compiles it in Docker. Spill files go under `/tmp`.

## Memory

A reducer's group budget is the instance's memory less 128 MiB, which is left for edge buffers and the engine. A table of up to half the budget finishes in memory. A ranked finish (`ORDER BY … LIMIT k`) keeps only its top k, so the result is small. On Q32, a `basic` reducer finished a 396 MB table at 412 MB resident. A larger table spills sorted runs to local disk and merges them at the end.

| Instance | vCPU | Memory | Group budget |
|---|---:|---:|---:|
| `lite` | 1/16 | 256 MiB | 128 MiB |
| `basic` | 1/4 | 1 GiB | 896 MiB |
| `standard-1` | 1/2 | 4 GiB | 3.9 GiB |
| `standard-2` | 1 | 6 GiB | 5.9 GiB |
| `standard-3` | 2 | 8 GiB | 7.9 GiB |
| `standard-4` | 4 | 12 GiB | 11.9 GiB |

## Measurements (Q32, full hits)

- 32 `basic` reducers: 8.9 s. 64 `basic` reducers: 6.8 s.
- 32 `standard-1` ran no faster than 32 `basic`, at twice the cost, so `basic` is the default.
- On `lite`, the engine and buffers leave too little room: reducers were killed for running out of memory (exit 137).
- On node reducers, the same query wrote about 7 GB of R2 spill and took 20 s.
