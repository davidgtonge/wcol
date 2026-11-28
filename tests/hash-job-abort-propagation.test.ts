import assert from "node:assert/strict";
import { describe, it } from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import { runHashJob, type HashScanDestination, type RunnableTask } from "../src/runtime/coordinator/run-hash-job.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { createBoundedBytePipe } from "../src/runtime/host/ports/rpc.ts";
import { createPoolInputLatch } from "../src/runtime/host/pool-streams.ts";
import { runWcolEngine, type WcolHost } from "../src/runtime/host/run-wcol-engine.ts";
import { createReducerIngressSession } from "../src/runtime/host/scan-edge-session.ts";
import { instantiateNodeWasm, loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

function graphOf(planned: ReturnType<typeof planJob>): Extract<ExecutionPlan, { kind: "graph" }> {
  const graph = planned.kind === "json" ? planned.graph : undefined;
  if (graph?.kind !== "graph") throw new Error("bad plan");
  return graph;
}

describe("hash job failure propagation", () => {
  it("scan failure aborts finalizer (no hang)", { timeout: 30_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-abort-scan-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT 1 AS k FROM range(256)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(parquet, "r");
      try {
        const out = new Uint8Array(length);
        await fh.read(out, 0, length, offset);
        return out;
      } finally {
        await fh.close();
      }
    };
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 128,
      hashReducers: 2,
    });
    const graph = graphOf(planned);

    await assert.rejects(
      () =>
        runHashJob({
          wasm: { node, coordinator },
          plan: graph,
          readAt,
          hashRemote: {
            prepareReducer: async (
              task: RunnableTask,
              expectedScans: number,
              finalizer,
              _exportProgram?: Uint8Array,
            ) => {
              const session = createReducerIngressSession({
                jobId: "abort-test",
                executionAttempt: 1,
                expectedScans,
              });
              const inputLatch = createPoolInputLatch();
              session.readyToAccept();
              const pipe = createBoundedBytePipe(64 * 1024);
              finalizer.attachFinalizerInput(pipe.readable);
              const writer = pipe.writable.getWriter();
              const host: WcolHost = {
                writePort: async (_port, chunk) => {
                  await writer.write(chunk);
                },
                yield: () => new Promise((r) => setTimeout(r, 0)),
              };
              void runWcolEngine({
                wasm: node,
                plan: task.program,
                host,
                scratchBytes: 256 * 1024,
                layout: { inputs: expectedScans, outputs: 1 },
                inputLatch,
              }).then(
                () => writer.close(),
                (error: unknown) => writer.abort(error),
              ).catch(() => undefined);
              return {
                invokedSlots: [task.slot],
                waitConsumeReady: (scanIndex: number) => session.waitConsumeReady(scanIndex),
                accept: async (scanIndex, stream) => {
                  session.acceptScanEdge(scanIndex, stream);
                  inputLatch.attachInput(scanIndex, stream);
                },
                completeScan: async (scanIndex) => {
                  session.completeScan(scanIndex);
                  inputLatch.sealInput(scanIndex);
                },
                seal: async () => {
                  session.seal();
                },
                abort: async (error) => {
                  session.abort(error);
                  inputLatch.abort(error);
                },
              };
            },
            runScan: async () => {
              throw new Error("scan failed");
            },
          },
        }),
      /scan failed/,
    );
  });

  it("finalizer failure aborts remote scans and surfaces its own error", { timeout: 30_000 }, async () => {
    // Cloudflare shape: aborting a reducer does not reach the scan DOs, so a scan
    // stays in flight until the coordinator aborts it. Before, the job waited for
    // the stall watchdog and reported "stalled" instead of the finalizer error.
    const parquet = path.join(os.tmpdir(), `wcol-abort-remote-scan-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT 1 AS k FROM range(256)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(parquet, "r");
      try {
        const out = new Uint8Array(length);
        await fh.read(out, 0, length, offset);
        return out;
      } finally {
        await fh.close();
      }
    };
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 128,
      hashReducers: 2,
    });
    const graph = graphOf(planned);

    const scanAborts = new Map<number, () => void>();
    const abortedTasks: number[] = [];
    await assert.rejects(
      () =>
        runHashJob({
          wasm: { node: coordinator, coordinator },
          plan: graph,
          readAt,
          hashRemote: {
            prepareReducer: async (task, _expectedScans, finalizer) => {
              finalizer.attachFinalizerInput(
                new ReadableStream<Uint8Array>({
                  start(controller) {
                    setTimeout(() => controller.error(new Error("reducer egress lost")), 20);
                  },
                }),
              );
              return {
                invokedSlots: [task.slot],
                waitConsumeReady: async () => undefined,
                accept: async () => undefined,
                completeScan: async () => undefined,
                seal: async () => undefined,
                abort: async () => undefined,
              };
            },
            runScan: (task) =>
              new Promise((_resolve, reject) => {
                scanAborts.set(task.taskId, () => reject(new Error("scan aborted")));
              }),
            abortTask: async (task) => {
              abortedTasks.push(task.taskId);
              scanAborts.get(task.taskId)?.();
            },
          },
        }),
      /reducer egress lost/,
    );
    assert.ok(scanAborts.size > 0, "scans were started");
    assert.deepEqual(
      [...scanAborts.keys()].filter((id) => !abortedTasks.includes(id)),
      [],
      "every in-flight scan was aborted",
    );
  });

  it("finalizer failure aborts reducers (no hang)", { timeout: 60_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-abort-finalizer-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT printf('user-%d', i) AS k FROM range(4096) t(i)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(parquet, "r");
      try {
        const out = new Uint8Array(length);
        await fh.read(out, 0, length, offset);
        return out;
      } finally {
        await fh.close();
      }
    };
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: 32 * 1024,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 512,
      hashReducers: 2,
    });
    const graph = graphOf(planned);

    await assert.rejects(
      () =>
        runLocalHashGraph({ node, coordinator }, graph, readAt, {
          reducerEgressPipeBytes: 64 * 1024,
          simulateFinalizerError: new Error("finalizer failed"),
        }),
      /finalizer failed/,
    );
  });

  it("a fatal producer failure cancels the other scan streams", { timeout: 60_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-abort-fatal-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT i::BIGINT AS k FROM range(4096) t(i)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)`,
    );
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(parquet, "r");
      try {
        const out = new Uint8Array(length);
        await fh.read(out, 0, length, offset);
        return out;
      } finally {
        await fh.close();
      }
    };
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 2,
      batchRows: 256,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    const graph = graphOf(planned);
    const scans = new Set(
      graph.edges.filter((edge) => edge.partition !== undefined).map((edge) => edge.from),
    );
    assert.ok(scans.size >= 2, `expected at least two scans, got ${scans.size}`);

    const fatal = new Error(
      "wcol-fatal: wcs: dictionary_entry_too_large (scope=0 column=0 index=0 encoded=80 max=64)",
    );
    const aborted: unknown[] = [];
    let streamCancelled = false;
    let releaseScan1: (error: unknown) => void = () => undefined;
    const scan1Gate = new Promise<never>((_resolve, reject) => {
      releaseScan1 = reject;
    });
    let markAttached: () => void = () => undefined;
    const attached = new Promise<void>((resolve) => {
      markAttached = resolve;
    });
    const handles: Array<{ accept: (scanIndex: number, stream: ReadableStream<Uint8Array>) => Promise<void> }> = [];
    // Scans may start before reducers are prepared: wait for the first one.
    let firstHandleReady: () => void = () => undefined;
    const firstHandle = new Promise<void>((resolve) => {
      firstHandleReady = resolve;
    });

    await assert.rejects(
      () =>
        runHashJob({
          wasm: { node, coordinator },
          plan: graph,
          readAt,
          maxActiveScans: 2,
          hashRemote: {
            prepareReducer: async (task, expectedScans, finalizer) => {
              const session = createReducerIngressSession({
                jobId: "fatal-abort",
                executionAttempt: 1,
                expectedScans,
              });
              const inputLatch = createPoolInputLatch();
              session.readyToAccept();
              const pipe = createBoundedBytePipe(64 * 1024);
              finalizer.attachFinalizerInput(pipe.readable);
              const writer = pipe.writable.getWriter();
              const host: WcolHost = {
                writePort: async (_port, chunk) => {
                  await writer.write(chunk);
                },
                yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
              };
              const reducerWasm = await instantiateNodeWasm();
              void runWcolEngine({
                wasm: reducerWasm,
                plan: task.program,
                host,
                scratchBytes: 256 * 1024,
                layout: { inputs: expectedScans, outputs: 1 },
                inputLatch,
              }).then(
                () => writer.close(),
                (error: unknown) => writer.abort(error),
              ).catch(() => undefined);
              const handle = {
                invokedSlots: [task.slot],
                waitConsumeReady: (scanIndex: number) => session.waitConsumeReady(scanIndex),
                accept: async (scanIndex: number, stream: ReadableStream<Uint8Array>) => {
                  session.acceptScanEdge(scanIndex, stream);
                  inputLatch.attachInput(scanIndex, stream);
                },
                completeScan: async (scanIndex: number) => {
                  session.completeScan(scanIndex);
                  inputLatch.sealInput(scanIndex);
                },
                seal: async () => {
                  session.seal();
                },
                abort: async (error: unknown) => {
                  aborted.push(error);
                  releaseScan1(error);
                  session.abort(error);
                  inputLatch.abort(error);
                },
              };
              handles.push(handle);
              firstHandleReady();
              return handle;
            },
            runScan: async (_task, scanIndex) => {
              if (scanIndex === 0) {
                await attached;
                throw fatal;
              }
              const stream = new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(new Uint8Array([1, 2, 3, 4]));
                },
                cancel() {
                  streamCancelled = true;
                },
              });
              await firstHandle;
              await handles[0]!.accept(scanIndex, stream);
              markAttached();
              await scan1Gate;
              return { openedPartitions: [], bytes: 0 };
            },
          },
        }),
      /wcol-fatal: wcs: dictionary_entry_too_large/,
    );
    assert.ok(aborted.length >= 2, `reducers aborted ${aborted.length}`);
    for (const error of aborted) {
      assert.match(error instanceof Error ? error.message : String(error), /dictionary_entry_too_large/);
    }
    assert.equal(streamCancelled, true);
    await fs.promises.unlink(parquet).catch(() => undefined);
  });
});
