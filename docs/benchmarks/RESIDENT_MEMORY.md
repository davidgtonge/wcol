# Resident memory measurements

Local Miniflare/workerd run on 2026-11-12. Reproduce with `npm run test:iterative-miniflare`. The test uses real coordinator/node Wasm, SQLite Durable Objects, R2 emulation, and generated Parquet with 8 numeric features and 8,192-row groups. Each model uses 4 clusters and 2 workers; every run converged in 2 rounds.

All figures below are **MiB per worker**, taking the maximum across workers. Total rows are across both workers. Feature peaks conservatively include allocation overlap; Wasm bytes are observed linear-memory pages, including allocator and decoder overhead. Wasm memory does not shrink after freeing allocations. JavaScript and R2 buffers are not included.

| Total rows | Load feature peak | Normalization feature peak | Features retained during rounds | Wasm high-water |
|---:|---:|---:|---:|---:|
| 16,384 | 0.5 | 0.75 | 0.25 | 5.00 |
| 65,536 | 3 | 3 | 1 | 8.19 |
| 262,144 | 12 | 12 | 4 | 20.44 |
| 524,288 | 24 | 24 | 8 | 36.75 |

[Raw phase measurements, wall times and round counts](resident-memory-miniflare.json) are included. These are local memory checks; the small datasets and local emulation do not establish deployed throughput, cost, or 10M-row performance.

## Budget decision

Retain the provisional 40 MiB feature-storage default. The largest measured partition retains 8 MiB of normalized features, peaks conservatively at 24 MiB of feature allocations, and uses 36.75 MiB of Wasm memory. This supports keeping headroom for loading rather than sizing only from the final f32 matrix. The cap includes raw-buffer reallocation and raw/normalized overlap; it does not cap total Worker memory.

`WCOL_RESIDENT_MEMORY_BYTES` configures the node Worker budget. A separate integration test sets it to 1,024 bytes and verifies that an oversized partition fails on attempt one without publishing a model. Rust unit tests exercise exact boundary sizes, allocation overlap, overflow, and phase ordering.

Deployed memory measurements, Worker memory beyond Wasm, automatic partition sizing, distributed fan-in, and scale/cost measurements remain outstanding.
