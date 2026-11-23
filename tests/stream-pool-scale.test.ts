import assert from "node:assert/strict";
import { test } from "node:test";
import { loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createStreamPool } from "../src/runtime/host/stream-pool.ts";
import { pumpReadableToWriter } from "../src/runtime/host/pool-streams.ts";
import { readableFrom } from "./helpers/streams.ts";

async function readAll(stream: ReadableStream<Uint8Array>): Promise<number> {
  const reader = stream.getReader();
  let n = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    n += value.byteLength;
  }
  return n;
}

test("stress: 100 echo ports complete", async () => {
  const wasm = await loadNodeWasm();
  const ports = 100;
  const pool = createStreamPool({
    inputs: ports,
    inputSize: 64,
    outputs: ports,
    outputSize: 64,
    wasm,
    echo: true,
  });
  const readers = pool.outputStreams.map((stream) => readAll(stream));
  await Promise.all(
    Array.from({ length: ports }, (_, port) =>
      pumpReadableToWriter(readableFrom([new Uint8Array([port & 0xff])]), pool.inputStreams[port]!.getWriter()),
    ),
  );
  const counts = await Promise.all(readers);
  assert.ok(counts.every((n) => n === 1));
  await pool.finished;
  pool.close();
});
