# engine-shell

Shared TypeScript + Rust scaffolding for browser apps with a **Wasm engine in a Web Worker**, **CBOR wire encoding**, and **view-model patches** on the main thread.

Used by [rust-weather-spiral](https://github.com/davidgtonge/rust-weather-spiral) and related demos.

## Packages

| Path | Role |
|------|------|
| `ts/` | `@dtonge/engine-shell` — worker client, view-model store, effect registry |
| `rust/engine-kernel/` | Patch diff/apply primitives for Rust/Wasm engines |

## Pattern

```txt
Preact UI  →  CBOR AppEvent  →  Worker  →  Wasm engine
                ↑                              ↓
         applyPatchBatch  ←  CBOR patches + effects  ←
```

Canonical state lives in Wasm. The main thread holds only a renderable view-model. Side effects (timers, I/O) run on the main thread and complete as events.

## Vite / worker bundling

`createWorkerClient` takes a `createWorker` factory — **not** a worker URL. Instantiate the worker in your app module so Vite can trace `new Worker(new URL(...))` and bundle the worker script and Wasm:

```ts
createWorkerClient({
  createWorker: () =>
    new Worker(new URL("./app-worker.ts", import.meta.url), { type: "module" }),
  // ...
});
```

## License

MIT
