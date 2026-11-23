/**
 * Real node Wasm, 2 scans × 2 hash reducers. Rust Open creates a stream only
 * for a partition that receives rows. A scanner that sends nothing is reported
 * with completeScan, which the reducer sees as an absent producer, not as
 * transport EOF.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { createBoundedBytePipe } from "../src/runtime/host/ports/rpc.ts";
import { runHashJob, type HashScanDestination, type RunnableTask } from "../src/runtime/coordinator/run-hash-job.ts";
import { createPoolInputLatch } from "../src/runtime/host/pool-streams.ts";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runWcolEngine, type WcolHost } from "../src/runtime/host/run-wcol-engine.ts";
import { runScanStreamPool } from "../src/runtime/host/run-scan-stream-pool.ts";
import { claimingReadAt, localWork } from "../src/runtime/coordinator/unit-claims.ts";
import {
  createReducerIngressSession,
  type ReducerIngressSession,
} from "../src/runtime/host/scan-edge-session.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { fileRangeReader } from "./helpers/streams.ts";

const parquet = path.join(os.tmpdir(), `wcol-direct-rpc-${process.pid}.parquet`);

const readFileAt = fileRangeReader(parquet);

describe("real wasm direct RPC hash exchange", () => {
  it("runs 2×2 with an empty partition and rejects a late edge", async () => {
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT 7 AS k FROM range(4096)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)`,
    );
    const expected = await duck.runAndReadAll(
      `SELECT k, COUNT(*) AS c FROM '${parquet.replaceAll("'", "''")}' GROUP BY k ORDER BY k`,
    );
    const expectedRows = (expected.getRows() as unknown[][]).map((row) =>
      row.map((value) => (value === null ? null : String(value))),
    );

    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readFileAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 2,
      batchRows: 128,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    assert.equal(planned.kind, "json");
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error("hash query did not plan a graph");
    }
    const plan = planned.graph;
    const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
    const scanIds = [...new Set(partitioned.map((edge) => edge.from))];
    assert.equal(scanIds.length, 2, `expected 2 scans, got ${scanIds.join(",")}`);
    assert.equal(new Set(partitioned.map((edge) => edge.partition)).size, 2);

    const sessions = new Map<number, ReducerIngressSession>();
    const latches = new Map<number, ReturnType<typeof createPoolInputLatch>>();
    const opened: string[] = [];
    const nextUnit = localWork(plan.tasks);

    const result = await runHashJob({
      wasm: { node, coordinator },
      plan,
      readAt: readFileAt,
      hashRemote: {
        prepareReducer: async (
          task: RunnableTask,
          expectedScans: number,
          finalizer,
          _exportProgram?: Uint8Array,
        ) => {
          const session = createReducerIngressSession({
            jobId: "engine-2x2",
            executionAttempt: 1,
            expectedScans,
          });
          sessions.set(task.taskId, session);
          const inputLatch = createPoolInputLatch();
          latches.set(task.taskId, inputLatch);
          session.readyToAccept();
          const pipe = createBoundedBytePipe(1024 * 1024);
          finalizer.attachFinalizerInput(pipe.readable);
          const writer = pipe.writable.getWriter();
          const host: WcolHost = {
            writePort: async (_port, chunk) => {
              await writer.write(chunk);
            },
            yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
          };
          const running = runWcolEngine({
            wasm: node,
            plan: task.program,
            host,
            scratchBytes: 1024 * 1024,
            layout: { inputs: expectedScans, outputs: 1 },
            inputLatch,
          });
          const done = running.then(
            () => writer.close(),
            (error: unknown) => writer.abort(error),
          );
          return {
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
              await done;
              session.seal();
              for (let index = 0; index < expectedScans; index++) inputLatch.sealInput(index);
            },
            abort: async (error: unknown) => {
              session.abort(error);
              inputLatch.abort(error);
            },
          };
        },
        runScan: async (task: RunnableTask, scanIndex: number, destinations: HashScanDestination[]) => {
          const openedPartitions: number[] = [];
          const partitionCount = Math.max(...destinations.map((d) => d.partition)) + 1;
          await runScanStreamPool({
            wasm: node,
            program: task.program,
            scratchBytes: 1024 * 1024,
            partitionCount,
            readAt: claimingReadAt(
              async (_source, offset, length) => readFileAt(Number(offset), length),
              () => nextUnit(task.taskId),
            ),
            onPartitionStream: (port, body) => {
              const dest = destinations.find((item) => item.partition === port);
              if (!dest) throw new Error(`scan missing destination ${port}`);
              const session = sessions.get(dest.taskId);
              if (!session) throw new Error(`reducer ${dest.taskId} has no session`);
              openedPartitions.push(port);
              opened.push(`${scanIndex}:${port}`);
              session.acceptScanEdge(scanIndex, body);
              const latch = latches.get(dest.taskId);
              if (!latch) throw new Error(`reducer ${dest.taskId} has no input latch`);
              latch.attachInput(scanIndex, body);
            },
          });
          return { openedPartitions, bytes: 0, invokedSlots: [task.slot] };
        },
      },
    });

    assert.ok(result.rows && result.rows.byteLength > 0, "finalizer produced no rows");
    assert.ok(opened.length > 0, "no partition was opened");
    assert.ok(
      opened.length < scanIds.length * 2,
      `every edge opened (${opened.join(",")}); expected an empty partition`,
    );
    const json = JSON.parse(resultJson(coordinator, plan, { rows: result.rows }, planned.rowCap)) as {
      rows?: unknown[][];
    };
    const ours = (json.rows ?? []).map((row) =>
      row.map((value) => (value === null ? null : String(value))),
    );
    assert.deepEqual(ours, expectedRows);
    const session = [...sessions.values()][0];
    assert.ok(session);
    assert.throws(
      () => session.acceptScanEdge(0, new ReadableStream({ start(c) { c.close(); } })),
      /after seal/,
    );
    await fs.promises.unlink(parquet).catch(() => undefined);
  });
});
