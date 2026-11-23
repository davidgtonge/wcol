import assert from "node:assert/strict";
import { test } from "node:test";
import {
  emptyNodeSessionLease,
  graphSlotObjectName,
  prepareNodeSession,
  releaseNodeSession,
  type GraphTaskEnv,
} from "../cloudflare/src/node/node.ts";
import { attemptSuperseded, beginNodeExecution, checkDeploy, noteAborted, releaseIfOwner, type AbortedAttempts } from "../cloudflare/src/node/session.ts";

function nodeEnv(overrides: Partial<GraphTaskEnv> = {}): GraphTaskEnv {
  return { WCOL_NODE: {} as DurableObjectNamespace, ...overrides };
}

test("each job attempt names its own node objects", () => {
  assert.equal(graphSlotObjectName("j1", 2, 3), "graph:j1:a2:s3");
  assert.notEqual(graphSlotObjectName("j1", 1, 3), graphSlotObjectName("j1", 2, 3));
});

test("prepareNodeSession resets wasm build tracking", () => {
  const lease = emptyNodeSessionLease();
  lease.wasmBuild = "old";
  lease.jobId = "a";
  lease.executionAttempt = 1;
  prepareNodeSession(
    lease,
    nodeEnv({ WCOL_NODE_WASM_BUILD: "new" }),
    { jobId: "a", executionAttempt: 1 },
  );
  assert.equal(lease.wasmBuild, "new");
});

test("a late cleanup cannot release a newer session", () => {
  const first = beginNodeExecution(null, 1, { jobId: "j", executionAttempt: 1 });
  const ended = releaseIfOwner(first, first.id, "abortHashAttempt");
  assert.equal(ended.ended?.reason, "abortHashAttempt");
  const second = beginNodeExecution(ended.current, 2, { jobId: "j", executionAttempt: 2 });
  const stale = releaseIfOwner(second, first.id, "stream settled");
  assert.equal(stale.ended, null);
  assert.equal(stale.current?.id, second.id);
  assert.equal(stale.current?.executionAttempt, 2);
});

test("releaseNodeSession clears the job lease", () => {
  const lease = emptyNodeSessionLease();
  lease.wasmBuild = "b";
  lease.jobId = "j";
  lease.executionAttempt = 1;
  releaseNodeSession(lease);
  assert.equal(lease.jobId, null);
  assert.equal(lease.executionAttempt, null);
  assert.equal(lease.wasmBuild, "b");
});

test("a node instance from another deploy resets itself; unset ids never do", () => {
  // Ids without a time prefix keep the old rule: any mismatch resets the node.
  assert.deepEqual(checkDeploy("d1", "d2"), { kind: "node-stale", message: "stale node instance: deploy d1, coordinator is on d2" });
  assert.deepEqual(checkDeploy("d2", "d2"), { kind: "current" });
  assert.deepEqual(checkDeploy(undefined, "d2"), { kind: "current" });
  assert.deepEqual(checkDeploy("d1", undefined), { kind: "current" });
  // Deploy-time ids: only the older side is stale.
  const older = `${(1_000).toString(36)}-aaaa`;
  const newer = `${(2_000).toString(36)}-bbbb`;
  assert.equal(checkDeploy(older, newer).kind, "node-stale");
  assert.deepEqual(checkDeploy(newer, older), {
    kind: "coordinator-stale",
    message: `stale coordinator: deploy ${older}, node is on ${newer}`,
  });
});

test("work for an aborted or older attempt is refused at once", () => {
  const aborted: AbortedAttempts = new Map();
  const params = { jobId: "j", executionAttempt: 1 };
  assert.equal(attemptSuperseded(aborted, null, params), false);
  // A newer attempt of the same job holds the node.
  assert.equal(attemptSuperseded(aborted, { jobId: "j", executionAttempt: 2 }, params), true);
  assert.equal(attemptSuperseded(aborted, { jobId: "other", executionAttempt: 5 }, params), false);
  // The abort arrived before the work.
  noteAborted(aborted, "j", 1);
  assert.equal(attemptSuperseded(aborted, null, params), true);
  assert.equal(attemptSuperseded(aborted, null, { jobId: "j", executionAttempt: 2 }), false);
  // Only the newest jobs are remembered.
  for (let job = 0; job < 70; job++) noteAborted(aborted, `j${job}`, 1);
  assert.equal(aborted.has("j"), false);
  assert.equal(aborted.size <= 64, true);
});
