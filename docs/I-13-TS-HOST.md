# I-13 — TypeScript is only a host

**Status: pass (Parquet / stream path)** (2026-10-28)

## Active host (`src/runtime/stream/`)

| Module | Role |
|--------|------|
| [`run-wcol-engine.ts`](../src/runtime/stream/run-wcol-engine.ts) | Wasm poll loop, ReadAt, write ports |
| [`host-loop.ts`](../src/runtime/stream/host-loop.ts) | Batched I/O driver (ABI v1; GraphHost not yet wired) |
| [`sqlite-byte-streams.ts`](../src/runtime/stream/sqlite-byte-streams.ts) | SQLite readable/writable byte streams |
| [`partial-oracle.ts`](../src/runtime/stream/partial-oracle.ts) | PWCL compare via Rust normalize |
| [`vertical-slice-plan.ts`](../src/runtime/stream/vertical-slice-plan.ts) | Encode Rust v2 opcode **10** only |

No SQL evaluation, filter, or aggregate logic on this path.

## Archived (not stream host)

| Path | Purpose |
|------|---------|
| [`archive/legacy-ts-stream/`](../archive/legacy-ts-stream/) | Distributed TS graph (`runStreamNode`) — Cloudflare/scripts only |
| [`archive/legacy-ts-chunk-exec/`](../archive/legacy-ts-chunk-exec/) | `.wcol` file chunk workers (`WcolFile.query`) |

Public package may re-export legacy graph helpers from `src/index.ts` for compatibility; they are **not** part of the Rust-first Parquet pipeline.

## Evidence

- [`ts-host-boundary.test.ts`](../tests/ts-host-boundary.test.ts)
