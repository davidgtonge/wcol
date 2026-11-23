/**
 * Real reducer Wasm, real host loop, in-memory pipes.
 * 32 planned scan ports, 16 I/O slots, 6 running producers.
 * The first six must finish before any later scan attaches.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { createBoundedBytePipe } from "../src/runtime/host/ports/rpc.ts";
import { createPoolInputLatch } from "../src/runtime/host/pool-streams.ts";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runWcolEngine, type WcolHost } from "../src/runtime/host/run-wcol-engine.ts";
import { runScanStreamPool } from "../src/runtime/host/run-scan-stream-pool.ts";
import { createReducerIngressSession, type ReducerIngressSession } from "../src/runtime/host/scan-edge-session.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import { claimingReadAt, localWork } from "../src/runtime/coordinator/unit-claims.ts";
import type { HashScanDestination, RunnableTask } from "../src/runtime/coordinator/run-hash-job.ts";
import { fileRangeReader } from "./helpers/streams.ts";

type HostWaitNotice = {
  phase?: string;
  pending?: number;
  completionsQueued?: number;
  tsCompleted?: number;
  rustDelivered?: number;
  runnable?: boolean;
  pendingOperations: { kind: string; port: number; ticket: number; startedAt: number }[];
};

const PLANNED_SCANS = 32;
const ROWS_PER_SCAN = 160_000;
const PIPE_CAPACITY = 64 * 1024;
const IO_SLOTS = 16;
const WAVE = 6;
// Scans claim row groups at run time, so the first wave may read every scan's rows while
// the later ports stay unattached: budget for all of them, not one wave's share.
const WAVE_TIMEOUT_MS = 30_000;

const parquet = path.join(os.tmpdir(), `wcol-unattached-ports-${process.pid}.parquet`);

const readFileAt = fileRangeReader(parquet);

function withTimeout<T>(work: Promise<T>, ms: number, diagnostics: () => unknown): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`timed out after ${ms}ms\n${JSON.stringify(diagnostics(), null, 2)}`));
    }, ms);
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

describe("reducer progresses with unattached scan ports", { concurrency: 1 }, () => {
  it("finishes the first six scanners before the other ports attach", { timeout: 60_000 }, async () => {
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    const totalRows = PLANNED_SCANS * ROWS_PER_SCAN;
    await duck.run(
      `COPY (SELECT 1::BIGINT AS k FROM range(${totalRows})) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE ${ROWS_PER_SCAN})`,
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
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: PLANNED_SCANS,
      minBytesPerNode: 1,
      batchRows: 128,
      hashReducers: 2,
    });
    assert.equal(planned.kind, "json");
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error("hash query did not plan a graph");
    }
    const plan: Extract<ExecutionPlan, { kind: "graph" }> = planned.graph;
    const nextUnit = localWork(plan.tasks);
    const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
    const scanIds = [...new Set(partitioned.map((edge) => edge.from))].sort((a, b) => a - b);
    const byPartition = new Map<number, number>();
    for (const edge of partitioned) byPartition.set(edge.partition!, edge.to);
    const reducers = [...byPartition.entries()]
      .sort((left, right) => left[0] - right[0])
      .map(([partition, id]) => ({ id, partition }));
    assert.equal(scanIds.length, PLANNED_SCANS, `planner produced ${scanIds.length} scans`);
    assert.equal(reducers.length, 2);
    assert.deepEqual(
      reducers.map((reducer) => reducer.partition),
      [0, 1],
    );

    const taskOf = (id: number) => {
      const task = plan.tasks.find((item) => item.id === id);
      if (!task) throw new Error(`graph missing task ${id}`);
      return task;
    };
    const runnable = (id: number): RunnableTask => {
      const task = taskOf(id);
      return { taskId: id, slot: task.slot, program: task.program, producers: [] };
    };
    const destinations: HashScanDestination[] = reducers.map((reducer) => ({
      partition: reducer.partition,
      slot: taskOf(reducer.id).slot,
      taskId: reducer.id,
    }));

    const sessions: ReducerIngressSession[] = [];
    const notices: Array<HostWaitNotice | null> = [];
    const scannerBytes = new Array<number>(PLANNED_SCANS).fill(0);
    const runningScans: Promise<unknown>[] = [];
    const aborts: Array<(error: unknown) => Promise<void>> = [];
    const finalizerLatch = createPoolInputLatch();
    const finalizerParts: Uint8Array[] = [];
    const root = taskOf(plan.rootTaskId);
    const finalizerWork = runWcolEngine({
      wasm: coordinator,
      plan: root.program,
      scratchBytes: 1024 * 1024,
      layout: { inputs: reducers.length, outputs: 1 },
      inputLatch: finalizerLatch,
      host: {
        writePort: async (_port, chunk) => {
          finalizerParts.push(chunk);
        },
        yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
      },
    });

    try {
      const prepared = await Promise.all(
        reducers.map(async (reducer, reducerIndex) => {
          const session = createReducerIngressSession({
            jobId: "unattached-ports",
            executionAttempt: 1,
            expectedScans: scanIds.length,
          });
          sessions.push(session);
          notices.push(null);
          const inputLatch = createPoolInputLatch();
          session.readyToAccept();
          const pipe = createBoundedBytePipe(1024 * 1024);
          finalizerLatch.attachInput(reducerIndex, pipe.readable);
          const writer = pipe.writable.getWriter();
          const host: WcolHost = {
            writePort: async (_port, chunk) => {
              await writer.write(chunk);
            },
            yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
          };
          const running = runWcolEngine({
            wasm: node,
            plan: taskOf(reducer.id).program,
            host,
            scratchBytes: 1024 * 1024,
            layout: { inputs: scanIds.length, outputs: 1 },
            inputLatch,
          });
          const done = running.then(
            () => writer.close(),
            (error: unknown) => writer.abort(error),
          );
          aborts.push(async (error) => {
            session.abort(error);
            inputLatch.abort(error);
          });
          return {
            session,
            inputLatch,
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
              for (let index = 0; index < scanIds.length; index++) inputLatch.sealInput(index);
            },
          };
        }),
      );

      const diagnostics = () => {
        const attached = new Set(prepared.flatMap((reducer) => reducer.session.acceptedScanIndexes()));
        return {
          plannedPorts: PLANNED_SCANS,
          attachedPorts: [...attached],
          maxIoSlots: IO_SLOTS,
          reducers: notices.map((notice, index) => ({
            partition: reducers[index]!.partition,
            pendingReads: notice?.pending ?? null,
            pendingReadPorts: notice?.pendingOperations.map((op) => op.port) ?? [],
            completionsQueued: notice?.completionsQueued ?? null,
            completedReads: notice?.tsCompleted ?? null,
            deliveredToRust: notice?.rustDelivered ?? null,
            runnable: notice?.runnable ?? null,
            phase: notice?.phase ?? null,
          })),
          scannerBytes: scannerBytes.slice(0, WAVE),
        };
      };

      const startScanner = (scanIndex: number) => {
        const task = runnable(scanIds[scanIndex]!);
        const openedPartitions: number[] = [];
        const work = (async () => {
          await Promise.all(prepared.map((reducer) => reducer.waitConsumeReady(scanIndex)));
          const partitionCount = Math.max(...destinations.map((d) => d.partition)) + 1;
          await runScanStreamPool({
            wasm: node,
            program: task.program,
            scratchBytes: 1024 * 1024,
            partitionCount,
            readAt: claimingReadAt((_source, offset, length) => readFileAt(Number(offset), length), () => nextUnit(task.taskId)),
            onPartitionStream: (port, body) => {
              const dest = destinations.find((item) => item.partition === port);
              if (!dest) throw new Error(`scan missing destination ${port}`);
              const target = prepared[dest.partition];
              if (!target) throw new Error(`reducer ${dest.partition} is not prepared`);
              openedPartitions.push(port);
              void target.accept(scanIndex, body);
            },
          });
          const opened = new Set(openedPartitions);
          await Promise.all(
            prepared.map((reducer, partition) =>
              opened.has(partition) ? Promise.resolve() : reducer.completeScan(scanIndex),
            ),
          );
        })();
        runningScans.push(work);
        return work;
      };

      const firstWave = Array.from({ length: WAVE }, (_, scan) => startScanner(scan));
      await withTimeout(Promise.all(firstWave), WAVE_TIMEOUT_MS, diagnostics);

      for (const notice of notices) {
        if (!notice) continue;
        const waiting = notice.pendingOperations.filter((op) => op.kind === "read" && op.port >= WAVE);
        assert.equal(waiting.length, 0, JSON.stringify(diagnostics(), null, 2));
      }

      for (let start = WAVE; start < PLANNED_SCANS; start += WAVE) {
        const count = Math.min(WAVE, PLANNED_SCANS - start);
        const wave = Array.from({ length: count }, (_, offset) => startScanner(start + offset));
        await withTimeout(Promise.all(wave), WAVE_TIMEOUT_MS, diagnostics);
      }
      await Promise.all(prepared.map((reducer) => reducer.seal()));
      for (let index = 0; index < reducers.length; index++) finalizerLatch.sealInput(index);

      await finalizerWork;
      const parts = finalizerParts;
      const rows = new Uint8Array(parts.reduce((sum, part) => sum + part.byteLength, 0));
      let at = 0;
      for (const part of parts) {
        rows.set(part, at);
        at += part.byteLength;
      }
      const json = JSON.parse(resultJson(coordinator, plan, { rows }, planned.rowCap)) as { rows?: unknown[][] };
      const ours = (json.rows ?? []).map((row) => row.map((value) => (value === null ? null : String(value))));
      assert.deepEqual(ours, [["1", String(totalRows)]]);
    } finally {
      const stop = new Error("test finished");
      await Promise.allSettled(aborts.map((abort) => abort(stop)));
      await Promise.allSettled(runningScans);
      await fs.promises.unlink(parquet).catch(() => undefined);
    }
  });

  it("drains productive streams while idle attached reads stay pending", { timeout: 60_000 }, async () => {
    const idleParquet = path.join(os.tmpdir(), `wcol-idle-ports-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    const totalRows = PLANNED_SCANS * ROWS_PER_SCAN;
    await duck.run(
      // 100k keys: a scan combines before the exchange, and one key would leave it nothing
      // to send. These keep each productive scan's edges busy while reducers stay small.
      `COPY (SELECT (i % 100000)::BIGINT AS k FROM range(${totalRows}) t(i)) TO '${idleParquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE ${ROWS_PER_SCAN})`,
    );
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(idleParquet, "r");
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
    const size = fs.statSync(idleParquet).size;
    const footer = await readParquetFooter(readAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k",
        output: { format: "json" },
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: PLANNED_SCANS,
      minBytesPerNode: 1,
      batchRows: 128,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("hash query did not plan a graph");
    const plan: Extract<ExecutionPlan, { kind: "graph" }> = planned.graph;
    const nextUnit = localWork(plan.tasks);
    const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
    const scanIds = [...new Set(partitioned.map((edge) => edge.from))].sort((a, b) => a - b);
    const byPartition = new Map<number, number>();
    for (const edge of partitioned) byPartition.set(edge.partition!, edge.to);
    const reducers = [...byPartition.entries()].sort((left, right) => left[0] - right[0]).map(([partition, id]) => ({ id, partition }));
    assert.equal(scanIds.length, PLANNED_SCANS);
    const taskOf = (id: number) => {
      const task = plan.tasks.find((item) => item.id === id);
      if (!task) throw new Error(`graph missing task ${id}`);
      return task;
    };
    const seen: HostWaitNotice[][] = [[], []];
    const scannerBytes = new Array<number>(PLANNED_SCANS).fill(0);
    const running: Promise<unknown>[] = [];
    const engines: Promise<unknown>[] = [];
    const aborts: Array<(error: unknown) => Promise<void>> = [];
    try {
      const prepared = await Promise.all(reducers.map(async (reducer, reducerIndex) => {
        const session = createReducerIngressSession({
          jobId: "idle-ports",
          executionAttempt: 1,
          expectedScans: scanIds.length,
        });
        const inputLatch = createPoolInputLatch();
        session.readyToAccept();
        const pipe = createBoundedBytePipe(1024 * 1024);
        const writer = pipe.writable.getWriter();
        const runningEngine = runWcolEngine({
          wasm: node,
          plan: taskOf(reducer.id).program,
          scratchBytes: 1024 * 1024,
          layout: { inputs: scanIds.length, outputs: 1 },
          inputLatch,
          host: {
            writePort: async (_port, chunk) => writer.write(chunk),
            yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
          },
        });
        engines.push(runningEngine);
        aborts.push(async (error) => {
          session.abort(error);
          inputLatch.abort(error);
          await writer.abort(error).catch(() => undefined);
        });
        for (let port = 0; port < IO_SLOTS; port++) {
          const stream = new ReadableStream<Uint8Array>({ pull: () => new Promise(() => undefined) });
          session.acceptScanEdge(port, stream);
          inputLatch.attachInput(port, stream);
        }
        return {
          partition: reducer.partition,
          slot: taskOf(reducer.id).slot,
          taskId: reducer.id,
          inputLatch,
          session,
        };
      }));
      const destinations = prepared.map((reducer) => ({
        partition: reducer.partition,
        slot: reducer.slot,
        taskId: reducer.taskId,
      }));
      const startProductive = (scanIndex: number) => {
        const opened = new Set<number>();
        const work = (async () => {
          const partitionCount = Math.max(...destinations.map((d) => d.partition)) + 1;
          const stats = await runScanStreamPool({
            wasm: node,
            program: taskOf(scanIds[scanIndex]!).program,
            scratchBytes: 1024 * 1024,
            partitionCount,
            readAt: claimingReadAt((_source, offset, length) => readAt(Number(offset), length), () => nextUnit(scanIds[scanIndex]!)),
            onPartitionStream: (port, body) => {
              const dest = destinations.find((item) => item.partition === port);
              if (!dest) throw new Error(`scan missing destination ${port}`);
              const target = prepared[dest.partition]!;
              opened.add(port);
              target.session.acceptScanEdge(scanIndex, body);
              target.inputLatch.attachInput(scanIndex, body);
            },
          });
          scannerBytes[scanIndex] = stats.profile.outputBytes;
          await Promise.all(prepared.map((reducer, partition) => {
            if (opened.has(partition)) return Promise.resolve();
            reducer.session.completeScan(scanIndex);
            reducer.inputLatch.sealInput(scanIndex);
            return Promise.resolve();
          }));
        })();
        running.push(work);
        return work;
      };
      const productive = Array.from({ length: IO_SLOTS }, (_, offset) => startProductive(IO_SLOTS + offset));
      await withTimeout(Promise.all(productive), WAVE_TIMEOUT_MS, () => ({
        scannerBytes: scannerBytes.slice(IO_SLOTS),
        idleAttached: prepared.map((reducer) => reducer.session.acceptedScanIndexes().slice(0, IO_SLOTS)),
        copySlots: seen.map((list) => list.at(-1)?.pendingOperations.map((op) => op.port) ?? []),
      }));
      for (let scan = IO_SLOTS; scan < PLANNED_SCANS; scan++) {
        assert.ok(scannerBytes[scan]! > PIPE_CAPACITY, `scan ${scan} wrote ${scannerBytes[scan]} bytes`);
      }
      for (const reducer of prepared) {
        for (let port = 0; port < IO_SLOTS; port++) {
          assert.ok(
            reducer.session.acceptedScanIndexes().includes(port),
            `idle attach ${port} on partition ${reducer.partition} missing`,
          );
        }
      }
      for (const list of seen) {
        for (const notice of list) {
          const occupied = notice.pendingOperations.filter((op) => op.kind === "read" && op.port < IO_SLOTS);
          assert.equal(occupied.length, 0, `idle port held a copy slot: ${JSON.stringify(occupied)}`);
        }
      }
    } finally {
      const stop = new Error("test finished");
      await Promise.allSettled(aborts.map((abort) => abort(stop)));
      await Promise.allSettled(engines);
      await Promise.allSettled(running);
      await fs.promises.unlink(idleParquet).catch(() => undefined);
    }
  });
});
