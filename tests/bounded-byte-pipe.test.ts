import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { createBoundedBytePipe, drainReadable } from "../src/runtime/host/ports/rpc.ts";
import { createSharedBytePool } from "../src/runtime/host/shared-byte-pool.ts";
import { pumpSyntheticBytes } from "./helpers/streams.ts";

describe("bounded byte pipe", () => {
  it("transfers all bytes", async () => {
    const pipe = createBoundedBytePipe(64 * 1024);
    const pump = pumpSyntheticBytes(pipe.writable, 500_000, 17_000);
    const drained = drainReadable(pipe.readable);
    await pump;
    const n = await drained;
    assert.equal(n, 500_000);
    assert.equal(pipe.metrics().bytesWritten, 500_000);
    assert.equal(pipe.metrics().bytesRead, 500_000);
  });

  it("limits buffered bytes when the consumer is slow", async () => {
    const cap = 128 * 1024;
    const pipe = createBoundedBytePipe(cap);
    const total = 4 * 1024 * 1024;
    const pump = pumpSyntheticBytes(pipe.writable, total, 64 * 1024);
    const drained = drainReadable(pipe.readable, { delayMs: 5 });
    await Promise.all([pump, drained]);
    assert.equal(pipe.metrics().bytesRead, total);
    assert.ok(
      pipe.metrics().peakBufferedBytes <= cap + 64 * 1024,
      `peak ${pipe.metrics().peakBufferedBytes} exceeded cap ${cap}`,
    );
  });

  it("shares a byte pool across pipes and releases on drain", async () => {
    const pool = createSharedBytePool(8 * 1024);
    const pipes = [0, 1, 2, 3].map(() =>
      createBoundedBytePipe({ maxBufferedBytes: 64 * 1024, pool }),
    );
    const pumps = pipes.map((pipe) => pumpSyntheticBytes(pipe.writable, 16 * 1024, 1024));
    const drains = pipes.map((pipe) => drainReadable(pipe.readable, { delayMs: 1 }));
    await Promise.all([...pumps, ...drains]);
    assert.ok(pool.peakUsed <= 8 * 1024, `peak ${pool.peakUsed}`);
    assert.equal(pool.used, 0);
  });

  it("releases the pool on abort", async () => {
    const pool = createSharedBytePool(4096);
    const pipe = createBoundedBytePipe({ maxBufferedBytes: 64 * 1024, pool });
    const writer = pipe.writable.getWriter();
    await writer.write(new Uint8Array(1000));
    assert.equal(pool.used, 1000);
    await writer.abort();
    assert.equal(pool.used, 0);
  });

  it("an abort releases a writer blocked on the cap and errors the readable", async () => {
    const pool = createSharedBytePool(64 * 1024);
    const pipe = createBoundedBytePipe({ maxBufferedBytes: 1000, pool });
    const writer = pipe.writable.getWriter();
    const blocked = writer.write(new Uint8Array(2000));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(pipe.metrics().blockedWriters, 1);
    pipe.abort(new Error("scan aborted"));
    await blocked;
    assert.equal(pool.used, 0);
    // A cut edge must not read as a complete one.
    await assert.rejects(drainReadable(pipe.readable), /scan aborted/);
    await assert.rejects(writer.write(new Uint8Array(1)), /closed bounded pipe \(abort: scan aborted\)/);
  });
});
