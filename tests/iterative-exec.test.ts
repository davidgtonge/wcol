import assert from "node:assert/strict";
import test from "node:test";

import {
  runIterativeExec,
  type IterativeCheckpoint,
  type IterativeKernel,
  type IterativeResident,
} from "../src/runtime/coordinator/iterative-exec.ts";

const kernel: IterativeKernel<number, number> = {
  encodeState: (state) => new Uint8Array([state]),
  decodePartial: (bytes) => bytes[0]!,
  merge: (left, right) => left + right,
  update: (state, partial) => ({
    state: state + partial,
    converged: partial === 0,
  }),
};

function checkpoint(
  initial: { iteration: number; state: number } | null = null,
): IterativeCheckpoint<number> & { commits: number[] } {
  const commits: number[] = [];
  return {
    commits,
    async load() {
      return initial;
    },
    async commit({ iteration, state }) {
      commits.push(iteration * 100 + state);
    },
  };
}

function resident(slot: number, outputs: number[]): IterativeResident {
  let next = 0;
  return {
    slot,
    async runRound({ jobId, attempt, sessionId, iteration }) {
      assert.equal(jobId, "job");
      assert.equal(attempt, 2);
      assert.equal(sessionId, "session");
      assert.ok(iteration > 0);
      return new Uint8Array([outputs[next++] ?? 0]);
    },
    async release() {},
  };
}

test("iterative execution uses bounded deterministic merges and commits each barrier", async () => {
  const durable = checkpoint();
  const result = await runIterativeExec({
    jobId: "job",
    attempt: 2,
    sessionId: "session",
    residents: [resident(0, [1, 0]), resident(1, [2, 0]), resident(2, [3, 0])],
    kernel,
    initialState: 0,
    maxIterations: 10,
    fanIn: 2,
    rpcConcurrency: 2,
    checkpoint: durable,
  });
  assert.deepEqual(result, { state: 6, iterations: 2, converged: true });
  assert.deepEqual(durable.commits, [106, 206]);
});

test("iterative execution resumes from the last committed state and releases residents", async () => {
  const durable = checkpoint({ iteration: 1, state: 4 });
  let released = 0;
  const worker = resident(0, [0]);
  const result = await runIterativeExec({
    jobId: "job",
    attempt: 2,
    sessionId: "session",
    residents: [
      {
        ...worker,
        async release() {
          released += 1;
        },
      },
    ],
    kernel,
    initialState: 0,
    maxIterations: 10,
    fanIn: 4,
    rpcConcurrency: 1,
    checkpoint: durable,
  });
  assert.deepEqual(result, { state: 4, iterations: 2, converged: true });
  assert.deepEqual(durable.commits, [204]);
  assert.equal(released, 1);
});

test("runWcolEngine retains the planned graph through rounds and releases it explicitly", async () => {
  const { readFile } = await import("node:fs/promises");
  const { loadNodeWasm, loadCoordinatorWasm } = await import(
    "../src/runtime/host/wasm.ts"
  );
  const { planJob, readParquetFooter } = await import(
    "../src/runtime/coordinator/plan-dispatch.ts"
  );
  const { runWcolEngine } = await import(
    "../src/runtime/host/run-wcol-engine.ts"
  );
  const { prepareResidentKMeans } = await import(
    "../src/runtime/host/iterative-wasm.ts"
  );
  const file = new Uint8Array(
    await readFile(new URL("./fixtures/duckdb_nulls.parquet", import.meta.url)),
  );
  const coordinator = await loadCoordinatorWasm();
  const node = await loadNodeWasm();
  const footer = await readParquetFooter(
    async (offset, length) => file.slice(offset, offset + length),
    file.byteLength,
  );
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { t: { key: "http://example.invalid/t.parquet" } },
      sql: "CREATE MODEL m OPTIONS (model_type='kmeans', num_clusters=2) AS SELECT d, p FROM t WHERE d > 0",
    }),
    footers: [{ fileSize: file.byteLength, footer }],
    maxNodes: 1,
  });
  assert.ok(planned.kind === "model" && planned.graph.kind === "graph");
  assert.equal(planned.graph.tasks.length, 1);
  const [task] = planned.graph.tasks;
  assert.ok(task);
  let graph:
    | import("../src/runtime/host/resident-graph.ts").ResidentGraph
    | undefined;
  let reads = 0;
  await assert.rejects(
    runWcolEngine({
      wasm: node,
      plan: task.program,
      residentMemoryLimitBytes: 0,
      layout: { inputs: 0, outputs: 1 },
      host: {
        writePort: () => assert.fail("invalid budget must not execute"),
        yield: async () => {},
      },
      onResidentReady: () => assert.fail("invalid budget must not load"),
    }),
    /positive 32-bit byte count/,
  );
  const stats = await runWcolEngine({
    wasm: node,
    plan: task.program,
    residentMemoryLimitBytes: 2 * 1024 * 1024,
    layout: { inputs: 0, outputs: 1 },
    readAt: async (_source, offset, length) => {
      reads++;
      return file.slice(Number(offset), Number(offset) + length);
    },
    host: {
      writePort: () => assert.fail("resident data must stay inside Rust"),
      yield: async () => {},
    },
    onResidentReady: (ready) => {
      graph = ready;
    },
  });
  assert.ok(graph);
  const resident = graph;
  try {
    assert.ok(reads > 0);
    assert.equal(stats.outputBytes, 0);
    const loadedReads = reads;
    const loadedMemory = await resident.memory();
    assert.ok(loadedMemory.featureBytes > 0);
    assert.equal(loadedMemory.normalizePeakBytes, 0);
    assert.equal(loadedMemory.limitBytes, 2 * 1024 * 1024);
    const prepared = prepareResidentKMeans(coordinator, resident.metadata, {
      dimensions: 2,
      clusters: 2,
      maxIterations: 100,
      tolerance: 1e-4,
      seed: 42,
    });
    await assert.rejects(resident.request(2, prepared.state), /phase/);
    await resident.request(1, prepared.normalization);
    const normalizedMemory = await resident.memory();
    assert.ok(normalizedMemory.featureBytes * 2 <= loadedMemory.featureBytes);
    assert.equal(
      normalizedMemory.normalizePeakBytes,
      loadedMemory.featureBytes + normalizedMemory.featureBytes,
    );
    assert.ok(normalizedMemory.wasmMemoryBytes >= loadedMemory.wasmMemoryBytes);
    const first = await resident.request(2, prepared.state);
    assert.ok(first.byteLength > 0);
    assert.deepEqual(await resident.request(2, prepared.state), first);
    assert.equal(
      reads,
      loadedReads,
      "resident rounds must not reload their source",
    );
  } finally {
    await resident.release();
  }
  await resident.release();
  await assert.rejects(resident.request(2, new Uint8Array()), /unavailable/);
});

test("failed rounds drain before release and preserve the execution failure", async () => {
  const failure = new Error("resident node lost");
  let finishRound: () => void = () =>
    assert.fail("second round has not started");
  const roundFinished = new Promise<void>((resolve) => {
    finishRound = resolve;
  });
  let active = 0;
  let released = 0;
  const durable = checkpoint();
  const running = runIterativeExec({
    jobId: "job",
    attempt: 2,
    sessionId: "session",
    residents: [
      {
        slot: 0,
        async runRound() {
          throw failure;
        },
        async release() {
          assert.equal(
            active,
            0,
            "release must wait for every round RPC to settle",
          );
          released++;
          throw new Error("release unavailable after node loss");
        },
      },
      {
        slot: 1,
        async runRound() {
          active++;
          await roundFinished;
          active--;
          return new Uint8Array([1]);
        },
        async release() {
          released++;
        },
      },
    ],
    kernel,
    initialState: 0,
    maxIterations: 10,
    fanIn: 2,
    rpcConcurrency: 2,
    checkpoint: durable,
  });
  // Keep one RPC pending while the other fails, without depending on timing delays.
  const rejected = assert.rejects(running, (error) => error === failure);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(released, 0);
  finishRound();
  await rejected;
  assert.deepEqual(
    durable.commits,
    [],
    "an incomplete round cannot commit a barrier",
  );
  assert.equal(released, 2);
});
