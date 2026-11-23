import assert from "node:assert/strict";
import { test } from "node:test";
import { finalizerInputGuard, isFatalTaskError } from "../src/runtime/coordinator/task-errors.ts";

const chunks = (sizes: number[]) =>
  new ReadableStream<Uint8Array>({
    pull(controller) {
      const size = sizes.shift();
      if (size === undefined) return controller.close();
      controller.enqueue(new Uint8Array(size));
    },
  });

const drain = async (stream: ReadableStream<Uint8Array>): Promise<number> => {
  const reader = stream.getReader();
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return total;
    total += value.byteLength;
  }
};

test("streams under the shared budget pass through unchanged", async () => {
  const guard = finalizerInputGuard(100);
  assert.equal(await drain(guard.wrap(chunks([30, 20]), "r0")), 50);
  assert.equal(await drain(guard.wrap(chunks([40, 10]), "r1")), 50);
  assert.equal(guard.bytes(), 100);
});

test("the budget is shared: the stream that passes it fails fatally", async () => {
  const guard = finalizerInputGuard(100);
  assert.equal(await drain(guard.wrap(chunks([60]), "r0")), 60);
  let cancelled = false;
  const source = new ReadableStream<Uint8Array>({
    pull: (controller) => controller.enqueue(new Uint8Array(30)),
    cancel: () => void (cancelled = true),
  });
  await assert.rejects(drain(guard.wrap(source, "r1")), (error: unknown) => {
    assert.ok(isFatalTaskError(error), "a too-large result must not be retried");
    assert.match(String(error), /too large for a JSON response/);
    return true;
  });
  assert.equal(cancelled, true, "the reducer's body is cancelled");
});

test("a body that fails names its source", async () => {
  const guard = finalizerInputGuard(100);
  const lost = new Error("ReadableStream received over RPC disconnected prematurely.");
  const source = new ReadableStream<Uint8Array>({ pull: (controller) => controller.error(lost) });
  await assert.rejects(drain(guard.wrap(source, "reducer 44 (partition 12)")), (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.equal(error.message, "reducer 44 (partition 12): ReadableStream received over RPC disconnected prematurely.");
    assert.ok(!isFatalTaskError(error), "a lost reducer is retried");
    return true;
  });
});
