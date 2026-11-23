import assert from "node:assert/strict";
import { test } from "node:test";
import { loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createStreamPool, type StreamPool } from "../src/runtime/host/stream-pool.ts";

async function shutdownPool(pool: StreamPool): Promise<void> {
  await pool.finished.catch(() => undefined);
  pool.close();
  await new Promise<void>((resolve) => setImmediate(resolve));
}

test("abort is not EOF", async () => {
  const wasm = await loadNodeWasm();
  const pool = createStreamPool({
    inputs: 1,
    inputSize: 64,
    outputs: 1,
    outputSize: 64,
    wasm,
    echo: true,
  });
  pool.abort(new Error("boom"));
  await assert.rejects(pool.finished, /boom/i);
  await shutdownPool(pool);
});
