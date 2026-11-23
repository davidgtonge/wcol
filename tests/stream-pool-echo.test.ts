import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createStreamPool, type StreamPool } from "../src/runtime/host/stream-pool.ts";
import { pumpReadableToWriter, READ_AT_LEAST_BYTES } from "../src/runtime/host/pool-streams.ts";
import { readableFrom } from "./helpers/streams.ts";

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array[]> {
  const reader = stream.getReader();
  const out: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    out.push(value);
  }
  return out;
}

async function shutdownPool(pool: StreamPool): Promise<void> {
  await pool.finished.catch(() => undefined);
  pool.close();
  await new Promise<void>((resolve) => setImmediate(resolve));
}

describe("stream pool echo", { concurrency: 1 }, () => {
test("echo pool: 100 streams and stalled output isolation", async () => {
  const wasm = await loadNodeWasm();
  const n = 100;
  const pool = createStreamPool({
    inputs: n,
    inputSize: 64,
    outputs: n,
    outputSize: 64,
    wasm,
    echo: true,
  });
  const payloads = Array.from({ length: n }, (_, i) => new Uint8Array([i & 0xff]));
  const readers = pool.outputStreams.map((stream, port) => readAll(stream).then((parts) => ({ port, parts })));
  await Promise.all(
    payloads.map((payload, port) =>
      pumpReadableToWriter(readableFrom([payload]), pool.inputStreams[port]!.getWriter()),
    ),
  );
  const results = await Promise.all(readers);
  for (const { port, parts } of results) {
    assert.equal(parts.length, 1);
    assert.equal(parts[0]![0], port & 0xff);
  }
  await pool.finished;
  await shutdownPool(pool);
});

test("echo pool: write() backpressure until slot frees", async () => {
  const wasm = await loadNodeWasm();
  const pool = createStreamPool({
    inputs: 1,
    inputSize: 64,
    outputs: 1,
    outputSize: 64,
    wasm,
    echo: true,
  });
  const writer = pool.inputStreams[0]!.getWriter();
  await writer.write(new Uint8Array([1]));
  const reader = pool.outputStreams[0]!.getReader();
  let resolved = 0;
  const second = writer.write(new Uint8Array([2])).then(() => {
    resolved += 1;
  });
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(resolved, 0, "second write blocked while output slot is full");
  await reader.read();
  await second;
  assert.equal(resolved, 1);
  await writer.close();
  await reader.read();
  await pool.finished;
  await shutdownPool(pool);
});

test("pumpReadableToWriter BYOB buffer is not wasm memory", async () => {
  const wasm = await loadNodeWasm();
  const pool = createStreamPool({
    inputs: 1,
    inputSize: 64,
    outputs: 1,
    outputSize: 64,
    wasm,
    echo: true,
  });
  const mem = wasm.memory.buffer;
  const source = readableFrom([new Uint8Array([42])]);
  await pumpReadableToWriter(source, pool.inputStreams[0]!.getWriter());
  assert.notEqual(new Uint8Array(READ_AT_LEAST_BYTES).buffer, mem);
  const out = await readAll(pool.outputStreams[0]!);
  assert.deepEqual(out[0], new Uint8Array([42]));
  await pool.finished;
  await shutdownPool(pool);
});
});
