# Inter-DO WCS1 transport

## Current (Milestone B): `fetch` + bounded pipe

Producer and consumer are **`WcolNode`** isolates. Bulk data never flows through the coordinator.

```text
Consumer DO  --fetch-->  Producer DO
                |
                v
         ReadableStream (bounded-byte-pipe)
                ^
                |
         GraphHost WritePort / WCS egress
```

Implementation:

- [`bounded-byte-pipe.ts`](../src/runtime/stream/bounded-byte-pipe.ts) — backpressure cap
- [`inter-do-wcs-pipe.ts`](../src/runtime/stream/inter-do-wcs-pipe.ts) — producer session + consumer helpers
- [`inter-do-wcs-paths.ts`](../src/runtime/stream/inter-do-wcs-paths.ts) — stable `/wcs/egress/*`, `/wcs/ingress/*`, `/wcs/stream/*`
- [`wcs-bench-node.ts`](../cloudflare/src/wcs-bench-node.ts) — reference implementation (+ legacy `/wcs/bench/*`)
- [`pipeWorkerShardWcs`](../src/runtime/stream/cf-shard-job.ts) — real WorkerShard partial stream

Verify: `npm run test:cf-wcs-bench`, `WCS_BENCH_GRAPH=1 npm run test:cf-wcs-bench`, `npx tsx --test tests/inter-do-wcs-pipe.test.ts`.

GraphHost shard egress emits **PARTIAL + END** ([`EmitPartial`](../rust/wcol/src/exec/operators/ingest_query.rs)); reducers may use **`WCS_VALIDATE_AND_COPY_PLAN`** on the wire.

Hash-partitioned row exchange (Milestone B) uses **`FRAME_DATA_BATCH` (3)** + **`WCOLpjv1`** via [`RowBatchWcsEgress`](../rust/wcol/src/exec/operators/wcs_row_stream.rs) / [`RowBatchWcsIngress`](../rust/wcol/src/exec/operators/wcs_row_stream.rs) — same guard+copy byte pipe as bench consumers (`ingest_wcs_through_guard`).

## Target (Milestone F): native RPC byte streams

Cloudflare Workers RPC can expose **native `ReadableStream`** handles between colocated DOs without serializing the full body through the coordinator HTTP proxy.

| Aspect | `fetch` body (today) | Native RPC (planned) |
|--------|----------------------|----------------------|
| API | `producer.fetch(url)` → `response.body` | RPC method returns stream handle |
| Backpressure | Bounded pipe on producer | Same pipe; attach to RPC sink |
| Coordinator | Not on data path | Not on data path |
| Graph contract | [`GraphEdge.transport`](../src/runtime/stream/graph-job.ts) `do_fetch_wcs` | `native_rpc_wcs` |

Migration plan:

1. Keep **`startInterDoBoundedWcsPipe`** as the producer-side primitive.
2. ~~Add consumer adapter `runInterDoWcsConsumer` / `consumeInterDoFetchWcs` → `runWcolStream`~~ — [`inter-do-wcs-pipe.ts`](../src/runtime/stream/inter-do-wcs-pipe.ts), used by WCS bench consumers.
3. ~~Introduce RPC entrypoints on `WcolNode` that return the same bounded stream contract as **`POST /wcs/egress/shard`** (`x-wcol-run-id`, octet-stream body).~~ — `WcolNode.openInterDoShardWcsEgress` / `openInterDoRawByteEgress`; consumer **`POST /wcs/ingress/shard-rpc`**; verify `test:cf-wcs-bench` (`inter_do_rpc_pipe_ok`, optional `graphhost_wcs_rpc_transport_ok` with `WCS_BENCH_GRAPH=1`).
4. ~~Switch reduce ingress from base64 **`GET /task/result`**~~ — async jobs use **`taskPartialTransport: "wcs_stream"`** with Rust byte-pipe **`OP_BYTE_WCS_PARTIAL_REDUCE`** (`WCS_PARTIAL_REDUCE_PLAN`: incremental frame parse + `ReduceOp` merge, PWCL out). Host still buffers only merged partial + fixed pipe capacity, not the full WCS wire. Multi-worker single-session concat remains future work.

## Related

- [DISTRIBUTED_STREAMING_GRAPH_EXECUTION.md](./DISTRIBUTED_STREAMING_GRAPH_EXECUTION.md)
- [DISTRIBUTED_FAILURE_MATRIX.md](./DISTRIBUTED_FAILURE_MATRIX.md)
