import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { estimateCost, summarizeUsage, WORKERS_PAID_PRICING } from "../src/runtime/coordinator/attempt-usage.ts";

describe("attempt usage and cost", () => {
  const usage = summarizeUsage([
    { role: "scan", startedAt: 0, endedAt: 8000, sourceReads: 200, sourceReadBytes: 400 },
    { role: "scan", startedAt: 100, endedAt: 6100, sourceReads: 100, sourceReadBytes: 200 },
    {
      role: "reducer",
      startedAt: 500,
      endedAt: 15500,
      spillIo: { puts: 2, putBytes: 40, reads: 11, readBytes: 40 },
    },
  ]);

  it("sums busy time per role and I/O across tasks", () => {
    assert.deepEqual(usage.scan, { tasks: 2, busyMs: 14000, maxMs: 8000 });
    assert.deepEqual(usage.reducer, { tasks: 1, busyMs: 15000, maxMs: 15000 });
    assert.equal(usage.sourceReads, 300);
    assert.equal(usage.spillPuts, 2);
    assert.equal(usage.spillReads, 11);
  });

  it("prices Durable Object time at 128 MB per object, plus requests and R2 operations", () => {
    const cost = estimateCost(usage, 16000, 1000);
    // 14 + 15 task seconds, plus 16 coordinator seconds.
    assert.equal(cost.doSeconds, 45);
    assert.ok(Math.abs(cost.doDuration - 45 * 0.128 * 12.5e-6) < 1e-12);
    assert.ok(Math.abs(cost.sourceReads - 300 * 0.36e-6) < 1e-12);
    assert.ok(Math.abs(cost.spill - (2 * 4.5e-6 + 11 * 0.36e-6)) < 1e-12);
    assert.ok(Math.abs(cost.doRequests - 1000 * WORKERS_PAID_PRICING.doRequest) < 1e-12);
    assert.ok(Math.abs(cost.total - (cost.doDuration + cost.doRequests + cost.sourceReads + cost.spill)) < 1e-12);
  });

  it("counts a task that ended before it started as zero", () => {
    assert.equal(summarizeUsage([{ role: "scan", startedAt: 10, endedAt: 5 }]).scan.busyMs, 0);
  });
});
