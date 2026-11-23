import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  CoordinatorTaskRegistry,
  memoryAwareConcurrentJobs,
  runTaskWithRetries,
  taskIdForShard,
  taskIdForShardIndex,
} from "../src/runtime/coordinator/coordinator-tasks.ts";

describe("coordinator task registry", () => {
  it("taskIdForShardIndex is stable per shard slot", () => {
    assert.equal(
      taskIdForShardIndex("hits", "SELECT 1", 2),
      taskIdForShardIndex("hits", "SELECT 1", 2),
    );
    assert.notEqual(
      taskIdForShardIndex("hits", "SELECT 1", 2),
      taskIdForShardIndex("hits", "SELECT 1", 3),
    );
  });

  it("task ids are stable for row-group set order", () => {
    const a = taskIdForShard("hits", "SELECT 1", [3, 1, 2]);
    const b = taskIdForShard("hits", "SELECT 1", [1, 2, 3]);
    assert.equal(a, b);
    assert.notEqual(a, taskIdForShard("hits", "SELECT 2", [1, 2, 3]));
  });

  it("rejects duplicate completed task", () => {
    const reg = new CoordinatorTaskRegistry();
    const id = taskIdForShard("s", "q", [0]);
    const partial = new Uint8Array([1, 2, 3]);
    assert.equal(reg.registerCompletedAttempt(id, 1, partial).ok, true);
    const dup = reg.registerCompletedAttempt(id, 2, partial);
    assert.equal(dup.ok, false);
    if (!dup.ok) assert.equal(dup.reason, "duplicate_completed");
  });

  it("finalize fails when a task never completed", () => {
    const reg = new CoordinatorTaskRegistry();
    const a = taskIdForShard("s", "q", [1]);
    const b = taskIdForShard("s", "q", [2]);
    reg.registerCompletedAttempt(a, 1, new Uint8Array([1]));
    const fin = reg.finalizeForReduce([a, b]);
    assert.equal(fin.ok, false);
    if (!fin.ok) assert.equal(fin.error.kind, "missing_tasks");
  });

  it("runTaskWithRetries registers only after success", async () => {
    const reg = new CoordinatorTaskRegistry();
    const id = taskIdForShard("s", "q", [9]);
    let calls = 0;
    const out = await runTaskWithRetries(reg, id, 3, async () => {
      calls += 1;
      if (calls < 2) {
        throw new Error("simulated worker failure");
      }
      return { partial: new Uint8Array([42]) };
    });
    assert.equal(out.partial[0], 42);
    assert.equal(reg.failedAttemptCount(id), 1);
    assert.equal(reg.completedCount(), 1);
    const fin = reg.finalizeForReduce([id]);
    assert.equal(fin.ok, true);
  });

  it("runTaskWithRetries does not register after exhausting attempts", async () => {
    const reg = new CoordinatorTaskRegistry();
    const id = taskIdForShard("s", "q", [8]);
    await assert.rejects(
      runTaskWithRetries(reg, id, 2, async () => {
        throw new Error("always fails");
      }),
    );
    assert.equal(reg.completedCount(), 0);
    assert.equal(reg.failedAttemptCount(id), 2);
  });

  it("memory-aware concurrency respects budget", () => {
    const n = memoryAwareConcurrentJobs({
      requestedConcurrency: 16,
      availableMemoryBytes: 100 * 1024 * 1024,
      peakMemoryPerJobBytes: 13 * 1024 * 1024,
      headroomBytes: 8 * 1024 * 1024,
    });
    assert.ok(n >= 1 && n <= 16);
    const tight = memoryAwareConcurrentJobs({
      requestedConcurrency: 16,
      availableMemoryBytes: 32 * 1024 * 1024,
      peakMemoryPerJobBytes: 13 * 1024 * 1024,
    });
    assert.ok(tight < 16);
  });
});
