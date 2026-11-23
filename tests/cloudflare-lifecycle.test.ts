import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { publishModel, publishParquet, publishedPointerMatches } from "../cloudflare/src/coordinator/publish.ts";
import { rejectUnexecutable, type Prepared } from "../cloudflare/src/coordinator/plan.ts";
import { graphObjectKey, type GraphJobRecord } from "../cloudflare/src/coordinator/record.ts";
import { abortHashAttemptOnNode, acceptScanEdgeOnNode, refuseScanEdge } from "../cloudflare/src/node/reducer.ts";
import { destinationForPort } from "../cloudflare/src/node/scan.ts";
import { createReducerIngressSession } from "../src/runtime/host/scan-edge-session.ts";
import {
  NODE_HASH_LEASE_MS,
  beginNodeExecution,
  nodeLeaseExpired,
  ownsExecution,
  touchNodeLease,
} from "../cloudflare/src/node/session.ts";
import type { HashIngressHolder } from "../cloudflare/src/node/types.ts";

describe("node execution lease", () => {
  it("rejects a second execution and ignores a stale release", () => {
    const now = 1_000;
    const first = beginNodeExecution(null, 1, { jobId: "job", executionAttempt: 1 }, now);
    assert.throws(() => beginNodeExecution(first, 2, { jobId: "other", executionAttempt: 1 }), /node is executing job attempt 1/);
    let current: typeof first | null = first;
    const release = (id: number) => {
      if (!ownsExecution(current, id)) return false;
      current = null;
      return true;
    };
    const second = beginNodeExecution(null, 2, { jobId: "job", executionAttempt: 2 }, now);
    current = second;
    assert.equal(release(first.id), false);
    assert.equal(current?.id, second.id);
    assert.equal(release(second.id), true);
    assert.equal(current, null);
  });

  it("keeps a progressing reducer past the original alarm", () => {
    const now = 5_000;
    const execution = beginNodeExecution(null, 1, { jobId: "job", executionAttempt: 1 }, now);
    const alarmAt = now + NODE_HASH_LEASE_MS;
    touchNodeLease(execution, alarmAt - 1);
    assert.equal(nodeLeaseExpired(execution, alarmAt), false);
    assert.equal(nodeLeaseExpired(execution, execution.expiresAt), true);
  });

  it("does not abort a different attempt", () => {
    let aborted = false;
    const holder = {
      session: {
        jobId: "live",
        executionAttempt: 2,
        abort() {
          aborted = true;
        },
      },
    } as unknown as HashIngressHolder;
    abortHashAttemptOnNode(holder, { jobId: "old", executionAttempt: 1, error: "stale" });
    assert.equal(aborted, false);
    assert.equal(holder.session?.jobId, "live");
  });
});

describe("hash scan ports", () => {
  it("routes by partition id and rejects an unknown port", () => {
    const destinations = [{ partition: 2, slot: 9 }, { partition: 0, slot: 4 }];
    assert.equal(destinationForPort(destinations, 0).slot, 4);
    assert.throws(() => destinationForPort(destinations, 1), /missing destination 1/);
  });
});

/** A scan edge that records whether the reducer read it to its end or cancelled it. */
function edgeStream(chunks: number) {
  const seen = { pulled: 0, cancelled: false };
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (seen.pulled === chunks) return controller.close();
      seen.pulled += 1;
      controller.enqueue(new Uint8Array(4096));
    },
    cancel() {
      seen.cancelled = true;
    },
  });
  return { stream, seen };
}

describe("refused scan edges", () => {
  it("are read to their end, not cancelled: a cancelled RPC stream leaves the call open", async () => {
    const { stream, seen } = edgeStream(3);
    refuseScanEdge(stream);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(seen.pulled, 3);
    assert.equal(seen.cancelled, false);
  });

  it("a duplicate edge fails with its stream left whole for the refusal", async () => {
    const session = createReducerIngressSession({ jobId: "job", executionAttempt: 1, expectedScans: 1 });
    const holder = { session } as unknown as HashIngressHolder;
    const params = { jobId: "job", executionAttempt: 1, scanTaskId: 0, scanIndex: 0, partition: 0 };
    const first = edgeStream(1);
    void acceptScanEdgeOnNode(holder, { ...params, stream: first.stream });
    const second = edgeStream(2);
    await assert.rejects(acceptScanEdgeOnNode(holder, { ...params, stream: second.stream }), /duplicate acceptScanEdge/);
    assert.equal(second.seen.cancelled, false);
    assert.equal(second.stream.locked, false);
    refuseScanEdge(second.stream);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(second.seen.pulled, 2);
  });
});

describe("job publication", () => {
  it("rejects concat and accepts a gather or hash graph", () => {
    const concat = { plan: { kind: "concat" }, descriptor: { kind: "concat" }, graph: null } as Prepared;
    assert.match(rejectUnexecutable(concat) ?? "", /concat output cannot execute/);
    const gather = { plan: { kind: "json" }, descriptor: { kind: "json" }, graph: { edges: [{}] } } as unknown as Prepared;
    assert.equal(rejectUnexecutable(gather), null);
    const hashed = { plan: { kind: "parquet" }, descriptor: { kind: "parquet" }, graph: { edges: [{ partition: 0 }] } } as unknown as Prepared;
    assert.equal(rejectUnexecutable(hashed), null);
  });

  it("keeps each submission's graph at its own digest", () => {
    assert.equal(graphObjectKey("job", "abc"), "_jobs/job/graph-abc.bin");
    assert.notEqual(graphObjectKey("job", "abc"), graphObjectKey("job", "def"));
    assert.equal(graphObjectKey("job"), "_jobs/job/graph.bin");
  });

  it("does not complete a job when the pointer conditional put loses", async () => {
    const record = runningRecord();
    let stored = record;
    const ctx = fakeCtx(stored, (next) => {
      stored = next;
    }, async (_key, options) => (options?.onlyIf ? null : { etag: "manifest" }), async () =>
      JSON.stringify({ jobId: "other", parts: [{ attempt: 1, objectKey: "old", bytes: 1 }] }),
    );
    await assert.rejects(
      () => publishParquet(ctx, record, "out/", "output", [part()], 1),
      /published pointer already exists/,
    );
    assert.equal(stored.state, "running");
  });

  it("adopts a pointer that already names this attempt", async () => {
    const record = runningRecord();
    let stored = record;
    const manifest = {
      jobId: "job",
      parts: [{ attempt: 1, objectKey: "out/part.parquet", bytes: 3, taskId: 1 }],
    };
    const ctx = fakeCtx(stored, (next) => {
      stored = next;
    }, async () => null, async () => JSON.stringify(manifest));
    assert.equal(publishedPointerMatches(JSON.stringify(manifest), "job", 1), true);
    await publishParquet(ctx, record, "out/", "output", [part()], 1);
    assert.equal(stored.state, "completed");
    assert.equal(stored.manifestKey, "_jobs/job/manifest.json");
  });

  it("publishes model artifacts create-once and records status metadata", async () => {
    const record: GraphJobRecord = { ...runningRecord(), kind: "model" };
    let stored = record;
    const created = fakeCtx(stored, (next) => { stored = next; }, async () => ({ etag: "model" }), async () => "");
    await publishModel(created, record, 1, "segments", {
      centroids: [0.5, 1.5], features: ["income", "spending"], means: [10, 20], scales: [2, 4],
      options: { numClusters: 1, seed: 42 },
    }, { iterations: 3, converged: true, inertia: 0.25 });
    assert.equal(stored.state, "completed");
    assert.equal(stored.modelKey, "_models/segments/model.json");
    assert.deepEqual(stored.modelSummary, { iterations: 3, converged: true, inertia: 0.25 });

    const collisionRecord = { ...runningRecord(), kind: "model" as const };
    const collision = fakeCtx(collisionRecord, () => undefined, async () => null, async () => "");
    await assert.rejects(() => publishModel(collision, record, 1, "segments", {
      centroids: [], features: [], means: [], scales: [], options: {},
    }, { iterations: 1, converged: false, inertia: 0 }), /model segments already exists/);
  });
});

function runningRecord(): GraphJobRecord {
  return {
    jobId: "job",
    kind: "parquet",
    state: "running",
    executionAttempt: 1,
    failedAttempts: 0,
    nodeCount: 1,
    createdAt: 1,
    updatedAt: 1,
  };
}

function part() {
  return { objectKey: "out/part.parquet", bytes: 3, taskId: 1, parts: 1, partitionValues: [] as [] };
}

function fakeCtx(
  initial: GraphJobRecord,
  save: (record: GraphJobRecord) => void,
  put: (key: string, options?: { onlyIf?: { etagDoesNotMatch?: string } }) => Promise<{ etag?: string } | null>,
  text: () => Promise<string>,
) {
  let stored = initial;
  const store = {
    async get() {
      return stored;
    },
    async put(_key: string, value: GraphJobRecord) {
      stored = value;
      save(value);
    },
  };
  return {
    env: {
      OUTPUT: {
        put(_key: string, _body: string, options?: { onlyIf?: { etagDoesNotMatch?: string } }) {
          return put(_key, options);
        },
        async get() {
          return { body: new Uint8Array(), text };
        },
      },
    },
    store,
    transaction: async <T>(run: (txn: typeof store) => Promise<T>) => run(store),
    async deleteAlarm() {},
  } as unknown as Parameters<typeof publishParquet>[0];
}
