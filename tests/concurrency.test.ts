import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createLimiter } from "../src/runtime/concurrency.ts";

describe("createLimiter", () => {
  it("never runs more than max at once and starts waiters in arrival order", async () => {
    const limit = createLimiter(2);
    let active = 0;
    let peak = 0;
    const started: number[] = [];
    const releases: Array<() => void> = [];
    const tasks = [0, 1, 2, 3].map((id) =>
      limit(async () => {
        started.push(id);
        active += 1;
        peak = Math.max(peak, active);
        await new Promise<void>((resolve) => releases.push(resolve));
        active -= 1;
        return id;
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(started, [0, 1]);
    while (releases.length > 0) {
      releases.shift()!();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    assert.deepEqual(await Promise.all(tasks), [0, 1, 2, 3]);
    assert.deepEqual(started, [0, 1, 2, 3]);
    assert.equal(peak, 2);
  });

  it("releases the permit when a task fails", async () => {
    const limit = createLimiter(1);
    await assert.rejects(limit(async () => { throw new Error("boom"); }), /boom/);
    assert.equal(await limit(async () => "next"), "next");
  });

  it("rejects a non-positive limit", () => {
    assert.throws(() => createLimiter(0), RangeError);
  });
});
