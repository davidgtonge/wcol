import assert from "node:assert/strict";
import { test } from "node:test";
import { COORDINATOR_SLOT, type ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import {
  assertNoStreamFanOut,
  isFullyColocated,
  shouldArmAlarm,
  sinksToReplay,
  type GraphPlan,
} from "../src/runtime/coordinator/job-replay.ts";

function plan(partial: Pick<GraphPlan, "tasks" | "edges"> & { rootTaskId?: number }): GraphPlan {
  return {
    kind: "graph",
    tasks: partial.tasks,
    edges: partial.edges,
    rootTaskId: partial.rootTaskId ?? partial.tasks[partial.tasks.length - 1]!.id,
    rootOutput: "rows",
    finalizer: { kind: "none" },
    resultColumns: new Uint8Array(),
  };
}

const task = (id: number, slot = COORDINATOR_SLOT) => ({
  id,
  slot,
  program: new Uint8Array([id]),
  requestCount: 1,
  fetchedBytes: 2,
});

test("a streamed task with two consumers is rejected", () => {
  const graph = plan({
    tasks: [task(0, 1), task(1, 2), task(2, 3)],
    edges: [
      { from: 0, to: 1, transport: "do_rpc_wcs" },
      { from: 0, to: 2, transport: "do_rpc_wcs" },
    ],
  });
  assert.throws(() => assertNoStreamFanOut(graph), /fan-out/);
});

test("edgeless object sinks are colocated only when every slot and edge says so", () => {
  const local = plan({ tasks: [task(0), task(1)], edges: [] });
  assert.equal(isFullyColocated(local), true);
  const remote = plan({
    tasks: [task(0, 1), task(1)],
    edges: [{ from: 0, to: 1, transport: "do_rpc_wcs" }],
  });
  assert.equal(isFullyColocated(remote), false);
  const rpcOnCoordinator = plan({
    tasks: [task(0), task(1)],
    edges: [{ from: 0, to: 1, transport: "do_rpc_wcs" }],
  });
  assert.equal(isFullyColocated(rpcOnCoordinator), false);
});

test("replay keeps verified object sinks and reruns every other sink", () => {
  const graph = plan({
    tasks: [task(0, 1), task(1, 2), task(2, 3)],
    edges: [{ from: 0, to: 2, transport: "do_rpc_wcs" }],
    rootTaskId: 2,
  });
  assert.deepEqual(sinksToReplay(graph, new Set([1])).sort(), [2]);
  assert.deepEqual(sinksToReplay(graph, new Set()).sort(), [1, 2]);
});

test("a live lease with a null alarm is not a stranded job", () => {
  const now = 1_000_000;
  assert.equal(shouldArmAlarm({ state: "running", leaseUntil: now + 1 }, null, now), false);
  assert.equal(shouldArmAlarm({ state: "accepted", leaseUntil: now - 1 }, null, now), true);
  assert.equal(shouldArmAlarm({ state: "running", leaseUntil: now - 1 }, now + 10, now), false);
  assert.equal(shouldArmAlarm({ state: "completed", leaseUntil: 0 }, null, now), false);
  assert.equal(shouldArmAlarm({ state: "failed" }, null, now), false);
});

test("metadata plans are not graphs", () => {
  const partial: ExecutionPlan = { kind: "metadata_partial", partial: new Uint8Array(), resultColumns: new Uint8Array() };
  assert.equal(partial.kind, "metadata_partial");
});
