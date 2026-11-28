import { strict as assert } from "node:assert";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createStreamPool } from "../src/runtime/host/stream-pool.ts";

/** `encode_wcs_partial_reduce_program(2)` */
const PARTIAL_REDUCE_2 = new Uint8Array([0xff, 1, 0, 0, 0, 14, 2, 0, 0, 0]);

test("partial reduce: a sealed absent input waits for the open one", async () => {
  const wasm = await loadNodeWasm();
  const pool = createStreamPool({
    inputs: 2,
    outputs: 1,
    inputSize: 64,
    outputSize: 128,
    wasm,
    graph: { program: PARTIAL_REDUCE_2, scratchBytes: 256 * 1024 },
  });
  const output = pool.outputStreams[0]!.pipeTo(new WritableStream());
  let settled = false;
  const finished = pool.finished.finally(() => {
    settled = true;
  });
  pool.sealInput(0);
  for (let step = 0; step < 2; step++) {
    pool.wake();
    await delay(50);
  }
  assert.equal(settled, false, "the pool must wait for its open input");
  // Only the last seal ends the reduce; with no partials from either input it has nothing to emit.
  pool.sealInput(1);
  pool.wake();
  await assert.rejects(finished, /received no partials/);
  await assert.rejects(output);
  await pool.close();
});
