/** The console's view of a job from a snapshot, its replay at any moment, and its cost. */
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { estimateCost, inputAt, liveJobView, PRICES, type LiveInput, type LiveSnapshot, type TaskFacts } from "../cloudflare/src/coordinator/live.ts";
import type { OpenSpan, StageSpan } from "../src/runtime/coordinator/stage-timeline.ts";

const T0 = 1_000_000;
// Two scans (0, 1) each feed two reducers (2, 3); the finalizer is task 9.
const graph: LiveInput["graph"] = {
  tasks: [
    { id: 0, slot: 0, role: "scan" },
    { id: 1, slot: 1, role: "scan" },
    { id: 2, slot: 2, role: "reducer" },
    { id: 3, slot: 3, role: "reducer" },
  ],
  edges: [
    { from: 0, to: 2, partition: 0 },
    { from: 0, to: 3, partition: 1 },
    { from: 1, to: 2, partition: 0 },
    { from: 1, to: 3, partition: 1 },
  ],
  rootTaskId: 9,
};
const span = (stage: StageSpan["stage"], taskId: number | undefined, start: number, end: number, outcome: "ok" | "failed" = "ok"): StageSpan => ({
  stage,
  taskId,
  startMs: T0 + start,
  endMs: T0 + end,
  outcome,
});
const open = (stage: OpenSpan["stage"], taskId: number, start: number): OpenSpan => ({ stage, taskId, startMs: T0 + start });
/** A scan's report at `at` ms, its two edges sent at 100 ms and ending at `ends`. */
const scanFacts = (colo: string, p0: number, p1: number, at = 4000, ends: [number, number] = [3000, 3500]): TaskFacts => ({
  kind: "scan",
  reportedAt: T0 + at,
  colo,
  sourceReads: 10,
  sourceReadBytes: 1000,
  partitionBytes: { "0": p0, "1": p1 },
  edgeTimes: { "0": [T0 + 100, T0 + ends[0]], "1": [T0 + 100, T0 + ends[1]] },
  outputBytes: p0 + p1,
});
const reducerFacts = (colo: string, at = 3000): TaskFacts => ({ kind: "reducer", reportedAt: T0 + at, colo, runtime: "container", instance: "basic", readyMs: 2800 });
const view = (overrides: Partial<LiveInput>) =>
  liveJobView({
    jobId: "j",
    state: "running",
    attempt: 1,
    startedAt: T0,
    now: T0 + 5000,
    graph,
    spans: [],
    open: [],
    facts: new Map(),
    reducers: { runtime: "container", instance: "basic" },
    coordinatorColo: "FRA",
    sourceBytes: 50_000,
    ...overrides,
  });

const finished: LiveSnapshot = {
  jobId: "j",
  state: "completed",
  attempt: 1,
  startedAt: T0,
  now: T0 + 5000,
  graph,
  spans: [
    span("prepareReducer", 2, 0, 3000),
    span("prepareReducer", 3, 0, 3000),
    span("scan", 0, 100, 4000),
    span("scan", 1, 100, 4500),
    span("finalize", 9, 4500, 5000),
  ],
  open: [],
  facts: [
    [0, scanFacts("VIE", 300, 500)],
    [1, scanFacts("WAW", 200, 100, 4500, [3200, 4400])],
    [2, reducerFacts("FRA")],
    [3, reducerFacts("FRA")],
  ],
  reducers: { runtime: "container", instance: "basic" },
  reducerReason: "test",
  coordinatorColo: "FRA",
  sourceBytes: 50_000,
  error: null,
  result: null,
};

describe("live job view", () => {
  it("shows reducers starting and scans streaming mid-run", () => {
    const live = view({
      now: T0 + 3200,
      spans: [span("prepareReducer", 2, 0, 3000), span("scan", 0, 100, 3100)],
      open: [open("prepareReducer", 3, 0), open("scan", 1, 100)],
      facts: new Map<number, TaskFacts>([
        [0, scanFacts("VIE", 300, 500, 3100, [3000, 3100])],
        [2, reducerFacts("FRA")],
      ]),
    });
    const task = (id: number) => live.tasks.find((t) => t.id === id)!;
    assert.deepEqual([task(0).state, task(0).bytesOut, task(0).index], ["done", 800, 0]);
    assert.deepEqual([task(1).state, task(1).index], ["running", 1]);
    assert.deepEqual([task(2).state, task(2).readyMs, task(2).readyAtMs, task(2).index], ["running", 2800, 3000, 0]);
    assert.equal(task(3).state, "starting");
    assert.deepEqual([task(9).state, task(9).colo], ["waiting", "FRA"]);
    // Scan 0 reported its edges' times; scan 1 has not, so its edges follow the tasks.
    const edge = (from: number, to: number) => live.edges.find((e) => e.from === from && e.to === to)!;
    assert.deepEqual([edge(0, 2).state, edge(0, 2).bytes, edge(0, 2).startMs, edge(0, 2).endMs], ["done", 300, 100, 3000]);
    assert.equal(edge(1, 2).state, "flowing");
    assert.equal(edge(1, 3).state, "idle");
    assert.equal(task(2).bytesIn, 300);
  });

  it("totals bytes, groups tasks by data centre, and sums flows between them once done", () => {
    const live = liveJobView(inputAt(finished, Infinity));
    assert.equal(live.state, "completed");
    assert.ok(live.tasks.every((task) => task.state === "done"));
    assert.deepEqual(live.totals, { fileBytes: 50_000, sourceReads: 20, sourceReadBytes: 2000, shuffleBytes: 1100, scans: 2, reducers: 2 });
    assert.deepEqual(live.colos, [
      { colo: "FRA", scans: 0, reducers: 2, coordinator: true },
      { colo: "VIE", scans: 1, reducers: 0, coordinator: false },
      { colo: "WAW", scans: 1, reducers: 0, coordinator: false },
    ]);
    assert.deepEqual(live.flows, [
      { from: "VIE", to: "FRA", bytes: 800 },
      { from: "WAW", to: "FRA", bytes: 300 },
    ]);
    assert.equal(live.tasks.find((t) => t.id === 3)!.bytesIn, 600);
    assert.deepEqual(live.phases, [{ stage: "finalize", startMs: 4500, endMs: 5000 }]);
  });

  it("replays a finished job as it stood at any moment", () => {
    const at = (ms: number) => liveJobView(inputAt(finished, T0 + ms));
    const state = (ms: number, id: number) => at(ms).tasks.find((t) => t.id === id)!.state;
    assert.equal(at(1000).state, "running");
    assert.deepEqual([state(1000, 0), state(1000, 2), state(1000, 9)], ["running", "starting", "waiting"]);
    // Facts appear when reported: scan 1 reported at 4.5 s.
    assert.equal(at(4400).tasks.find((t) => t.id === 1)!.colo, null);
    assert.equal(at(4600).tasks.find((t) => t.id === 1)!.colo, "WAW");
    // Once reported, scan 1's edge to reducer 3 is done from 4.4 s.
    assert.equal(at(4600).edges.find((e) => e.from === 1 && e.to === 3)!.state, "done");
    assert.deepEqual([state(4700, 9), state(5000, 9)], ["running", "done"]);
    assert.equal(at(99_999).elapsedMs, 5000);
  });

  it("shows a gather plan's scans, their reads and what each sent the finalizer", () => {
    const gather: LiveInput["graph"] = {
      tasks: [
        { id: 0, slot: 0, role: "scan" },
        { id: 1, slot: 1, role: "scan" },
      ],
      edges: [
        { from: 0, to: 9 },
        { from: 1, to: 9 },
      ],
      rootTaskId: 9,
    };
    const gathered = (colo: string, out: number): TaskFacts => ({
      kind: "scan",
      reportedAt: T0,
      colo,
      sourceReads: 3,
      sourceReadBytes: 500,
      partitionBytes: {},
      edgeTimes: {},
      outputBytes: out,
    });
    const live = view({
      graph: gather,
      reducers: { runtime: "node" },
      spans: [span("gatherTask", 0, 0, 900), span("gatherTask", 1, 0, 1000), span("finalize", 9, 1000, 1100)],
      facts: new Map<number, TaskFacts>([
        [0, gathered("FRA", 40)],
        [1, gathered("WAW", 60)],
      ]),
    });
    assert.deepEqual(live.totals, { fileBytes: 50_000, sourceReads: 6, sourceReadBytes: 1000, shuffleBytes: 100, scans: 2, reducers: 0 });
    assert.deepEqual(live.edges.map((edge) => [edge.bytes, edge.state]), [[40, "done"], [60, "done"]]);
    assert.ok(live.tasks.every((task) => task.state === "done"));
  });

  it("fails a scan whose span failed", () => {
    const live = view({ spans: [span("scan", 0, 0, 100, "failed")] });
    assert.equal(live.tasks.find((t) => t.id === 0)!.state, "failed");
  });
});

describe("cost estimate", () => {
  const usage = { elapsedMs: 10_000, scanMs: [5000, 5000], reducerMs: [8000], rpcCalls: 1_000_000, sourceReads: 1_000_000, sourceReadBytes: 2e12 };

  it("prices Durable Objects, R2 reads and container time at list prices", () => {
    const cost = estimateCost({ ...usage, container: "basic" });
    const usd = (label: string) => cost.items.find((item) => item.label.startsWith(label))!.usd;
    assert.equal(usd("Durable Object requests"), PRICES.doRequestPerMillion);
    assert.equal(usd("R2 reads"), PRICES.r2ClassBPerMillion);
    // 10 s coordinator + 10 s of scans + 8 s of reducers, at 128 MB.
    assert.ok(Math.abs(usd("Durable Object duration") - (28 * 0.128 * 12.5) / 1e6) < 1e-12);
    // basic: 1/4 vCPU, 1 GiB, 4 GB disk for 8 s.
    assert.ok(Math.abs(usd("Containers") - 8 * (0.25 * 0.00002 + 0.0000025 + 4 * 0.00000007)) < 1e-12);
    assert.ok(Math.abs(cost.usd - cost.items.reduce((sum, item) => sum + item.usd, 0)) < 1e-15);
  });

  it("compares with Athena's price for the bytes scanned, at least 10 MB", () => {
    assert.equal(estimateCost({ ...usage, container: null }).athenaUsd, 10);
    assert.equal(estimateCost({ ...usage, sourceReadBytes: 0, container: null }).athenaUsd, 0.00005);
  });

  it("has no container line for node reducers", () => {
    const cost = estimateCost({ ...usage, container: null });
    assert.ok(!cost.items.some((item) => item.label.startsWith("Containers")));
  });
});
