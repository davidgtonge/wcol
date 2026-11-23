import { test } from "node:test";
import { loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createStreamPool } from "../src/runtime/host/stream-pool.ts";

/** `encode_wcs_partial_reduce_program(2)` */
const PARTIAL_REDUCE_2 = new Uint8Array([0xff, 1, 0, 0, 0, 14, 2, 0, 0, 0]);

async function drainOutput(pool: ReturnType<typeof createStreamPool>): Promise<void> {
  const reader = pool.outputStreams[0]!.getReader();
  try {
    for (;;) {
      const { done } = await reader.read();
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}

test("partial reduce: sealed absent input survives poll steps", async () => {
  const wasm = await loadNodeWasm();
  const pool = createStreamPool({
    inputs: 2,
    outputs: 1,
    inputSize: 64,
    outputSize: 128,
    wasm,
    graph: { program: PARTIAL_REDUCE_2, scratchBytes: 256 * 1024 },
  });
  void drainOutput(pool).catch(() => undefined);
  pool.sealInput(0);
  pool.wake();
  await new Promise((r) => setTimeout(r, 100));
  pool.wake();
  await new Promise((r) => setTimeout(r, 100));
  pool.close();
});
