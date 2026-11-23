/** Plain-language findings from a finished job's view. */
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { findings } from "../cloudflare/src/coordinator/live-findings.ts";
import type { LiveEdge, LiveJobView, LiveTask } from "../cloudflare/src/coordinator/live.ts";

const task = (role: LiveTask["role"], id: number, index: number, over: Partial<LiveTask> = {}): LiveTask => ({
  id,
  role,
  index,
  state: "done",
  startMs: 0,
  endMs: 3000,
  readyAtMs: null,
  colo: "FRA",
  runtime: role === "reducer" ? "container" : role === "finalizer" ? "coordinator" : "node",
  instance: role === "reducer" ? "basic" : null,
  readyMs: null,
  sourceReads: 0,
  sourceReadBytes: 0,
  bytesIn: 0,
  bytesOut: 0,
  ...over,
});
const edge = (from: number, to: number, endMs: number): LiveEdge => ({ from, to, partition: 0, bytes: 100, state: "done", startMs: 100, endMs });

/** Four scans into two reducers, a 10 s job; `over` adjusts the parts a test is about. */
function job(over: { reducers?: LiveTask[]; edges?: LiveEdge[]; elapsedMs?: number; state?: string } = {}): LiveJobView {
  const scans = [0, 1, 2, 3].map((i) => task("scan", i, i, { endMs: 6000, sourceReadBytes: 250 }));
  const reducers = over.reducers ?? [
    task("reducer", 10, 0, { readyAtMs: 1000, bytesIn: 1000 }),
    task("reducer", 11, 1, { readyAtMs: 1200, bytesIn: 1100 }),
  ];
  const edges = over.edges ?? scans.flatMap((scan) => reducers.map((reducer) => edge(scan.id, reducer.id, 5000)));
  return {
    jobId: "j",
    state: over.state ?? "completed",
    attempt: 1,
    elapsedMs: over.elapsedMs ?? 10_000,
    reducers: { runtime: "container", instance: "basic" },
    tasks: [...scans, ...reducers, task("finalizer", 99, 0)],
    edges,
    phases: [],
    totals: { fileBytes: 10_000, sourceReads: 4, sourceReadBytes: 1000, shuffleBytes: 2100, scans: 4, reducers: reducers.length },
    colos: [],
    flows: [],
    cost: { usd: 0, items: [], athenaUsd: 0 },
  };
}

const texts = (view: LiveJobView, level?: "info" | "warn") =>
  findings(view)
    .filter((finding) => !level || finding.level === level)
    .map((finding) => finding.text);

describe("findings", () => {
  it("say nothing until the job has finished", () => {
    assert.deepEqual(findings(job({ state: "running" })), []);
  });

  it("describe a clean run without warnings", () => {
    const view = job();
    assert.deepEqual(texts(view, "warn"), []);
    assert.deepEqual(texts(view), [
      "Read 1.0 KB of a 10.0 KB file (10%): only the columns and row groups the query needs.",
      "Waiting for containers to start took 1.2 s, 12% of the job.",
      "The last scan finished at 6.0 s; the job took another 4.0 s to finish.",
    ]);
  });

  it("warn about a slow cold start", () => {
    const slow = [task("reducer", 10, 0, { readyAtMs: 4000, bytesIn: 1000 }), task("reducer", 11, 1, { readyAtMs: 1000, bytesIn: 1000 })];
    assert.deepEqual(texts(job({ reducers: slow }), "warn"), ["Waiting for containers to start took 4.0 s, 40% of the job."]);
  });

  it("name the slowest straggling edge", () => {
    const reducers = job().tasks.filter((t) => t.role === "reducer");
    const edges = [0, 1, 2, 3].flatMap((scan) => reducers.map((r) => edge(scan, r.id, scan === 2 && r.id === 11 ? 9000 : 5000)));
    assert.deepEqual(texts(job({ edges }), "warn"), [
      "1 of 8 edges finished more than 1.5 s after the median (5.0 s); the slowest, s2 → r1, ended at 9.0 s.",
    ]);
  });

  it("name the reducers late edges cluster on", () => {
    const reducers = [0, 1, 2, 3, 4].map((i) => task("reducer", 10 + i, i, { bytesIn: 1000 }));
    // Every scan's edge to r3 ends late; the rest on time.
    const edges = [0, 1, 2, 3].flatMap((scan) => reducers.map((r) => edge(scan, r.id, r.index === 3 ? 9000 : 5000)));
    assert.deepEqual(texts(job({ reducers, edges }), "warn"), [
      "4 of 20 edges finished more than 1.5 s after the median (5.0 s); the slowest, s0 → r3, ended at 9.0 s.",
      "100% of the late edges ended at reducer r3: they were slow to take input, whichever scan sent it.",
    ]);
  });

  it("point out a skewed reducer", () => {
    const reducers = [task("reducer", 10, 0, { bytesIn: 1000 }), task("reducer", 11, 1, { bytesIn: 1000 }), task("reducer", 12, 2, { bytesIn: 3000 })];
    assert.deepEqual(texts(job({ reducers }), "warn"), ["Reducer r2 received 3.0× the median input (3.0 KB): uneven partitions."]);
  });

  it("explain a footer-only answer", () => {
    const view = { ...job(), tasks: [task("finalizer", 99, 0)] };
    assert.deepEqual(texts(view), ["Answered from the Parquet footer: no data was read."]);
  });
});
