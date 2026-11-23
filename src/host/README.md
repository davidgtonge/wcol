# TypeScript host (target)

The host implements **`wcol::host::HostAction`** only: file/range reads, byte ports, yield, cancellation.

It must **not** parse WCS1 payloads, plan SQL, merge aggregates, or decode columns.

During consolidation, drivers still live under `src/runtime/stream/`; they move here in [CONSOLIDATION.md](../docs/CONSOLIDATION.md) Phase 3.

Planned files:

- `driver.ts` — generic Wasm poll loop
- `node.ts` / `browser.ts` — platform I/O adapters
