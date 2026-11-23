import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createStageTimeline, summarizeStages, type StageSpan } from "../src/runtime/coordinator/stage-timeline.ts";

describe("stage timeline", () => {
  it("records ok and failed spans with the clock at start and end", async () => {
    let now = 100;
    const timeline = createStageTimeline(() => now);
    await timeline.span("plan", {}, async () => {
      now = 130;
    });
    await assert.rejects(
      timeline.span("scan", { taskId: 2, scanIndex: 0 }, async () => {
        now = 175;
        throw new Error("scan failed");
      }),
      /scan failed/,
    );
    assert.deepEqual(timeline.spans(), [
      { stage: "plan", startMs: 100, endMs: 130, outcome: "ok" },
      { stage: "scan", taskId: 2, scanIndex: 0, startMs: 130, endMs: 175, outcome: "failed" },
    ]);
  });

  it("summarizes per stage relative to the first span, with overlap visible", () => {
    const spans: StageSpan[] = [
      { stage: "plan", startMs: 1000, endMs: 1200, outcome: "ok" },
      { stage: "scan", scanIndex: 0, startMs: 1200, endMs: 1700, outcome: "ok" },
      { stage: "scan", scanIndex: 1, startMs: 1300, endMs: 1900, outcome: "failed" },
      { stage: "seal", startMs: 1900, endMs: 1950, outcome: "ok" },
    ];
    assert.deepEqual(summarizeStages(spans), [
      { stage: "plan", count: 1, failed: 0, fromMs: 0, toMs: 200, totalMs: 200, maxMs: 200 },
      { stage: "scan", count: 2, failed: 1, fromMs: 200, toMs: 900, totalMs: 1100, maxMs: 600 },
      { stage: "seal", count: 1, failed: 0, fromMs: 900, toMs: 950, totalMs: 50, maxMs: 50 },
    ]);
    assert.deepEqual(summarizeStages([]), []);
  });
});
