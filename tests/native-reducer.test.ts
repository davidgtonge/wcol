/**
 * Hash reducers as native processes (`rust/wcol-reducer`), the way container reducers run:
 * scans stay in this process and stream each partition to its reducer over HTTP; reducers
 * spill to local disk. The answer must match DuckDB. Needs `cargo build -p wcol-reducer`.
 */
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runHashJob, type HashRemote, type PreparedHashReducer } from "../src/runtime/coordinator/run-hash-job.ts";
import { claimingReadAt, localWork } from "../src/runtime/coordinator/unit-claims.ts";
import { runScanStreamPool } from "../src/runtime/host/run-scan-stream-pool.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const reducerBin = path.join(root, "rust/target/debug/wcol-reducer");
const parquet = path.join(root, "data/hits_subset_500k.parquet");
const SQL = "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC, UserID LIMIT 10";

type Graph = Extract<ExecutionPlan, { kind: "graph" }>;

/** One native reducer process on its own port, with its own spill directory. */
async function startReducer(port: number, spillDir: string): Promise<{ base: string; child: ChildProcess }> {
  const child = spawn(reducerBin, [], { env: { ...process.env, PORT: String(port), SPILL_DIR: spillDir }, stdio: "inherit" });
  const base = `http://127.0.0.1:${port}`;
  for (let tries = 0; tries < 100; tries++) {
    if (await fetch(`${base}/health`).then((r) => r.ok, () => false)) return { base, child };
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  child.kill();
  throw new Error(`reducer on ${port} did not start`);
}

/**
 * The reducer's output, failed with its status if the reducer did not finish cleanly. The
 * request is not awaited here: its headers only arrive with the first output bytes, which
 * come after every scan has sent, so waiting for them would hold the scans back.
 */
function checkedOutput(base: string): ReadableStream<Uint8Array> {
  const response = fetch(`${base}/output`);
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      const reader = (await response).body!.getReader();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        controller.enqueue(value);
      }
      controller.close();
    },
  });
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      async flush(controller) {
        const status = await (await fetch(`${base}/status`)).text();
        if (!status.startsWith("ok")) controller.error(new Error(`native reducer: ${status}`));
      },
    }),
  );
}

describe("native hash reducers", { skip: (!fs.existsSync(reducerBin) && "build wcol-reducer first") || (!fs.existsSync(parquet) && "hits subset missing") }, () => {
  it("answer a spilling GROUP BY like the Wasm reducers do", { timeout: 120_000 }, async () => {
    const fd = fs.openSync(parquet, "r");
    const readAt = async (offset: number, length: number) => {
      const out = new Uint8Array(length);
      fs.readSync(fd, out, 0, length, offset);
      return out;
    };
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { hits: { key: "http://example.invalid/hits.parquet" } },
        sql: SQL,
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        // Small enough that each reducer spills several runs to its local disk.
        targetGroupBytes: 1024 * 1024,
      }),
      footers: [{ fileSize: size, footer: await readParquetFooter(readAt, size) }],
      maxNodes: 4,
      minBytesPerNode: 1,
      batchRows: 4096,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("expected a hash graph");
    const graph: Graph = planned.graph;
    const nextUnit = localWork(graph.tasks);
    const spillRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wcol-native-spill-"));
    const children: ChildProcess[] = [];
    // Scans start while reducers are still being prepared: each waits for its reducer.
    const ready = new Map<number, { promise: Promise<PreparedHashReducer>; resolve: (r: PreparedHashReducer) => void }>();
    const reducerFor = (taskId: number) => {
      let entry = ready.get(taskId);
      if (!entry) {
        let resolve!: (r: PreparedHashReducer) => void;
        const promise = new Promise<PreparedHashReducer>((done) => (resolve = done));
        entry = { promise, resolve };
        ready.set(taskId, entry);
      }
      return entry;
    };
    let port = 20_000 + Math.floor(Math.random() * 20_000);

    const hashRemote: HashRemote = {
      prepareReducer: async (task, expectedScans, finalizer) => {
        const { base, child } = await startReducer(port++, path.join(spillRoot, String(task.taskId)));
        children.push(child);
        const started = await fetch(`${base}/start?inputs=${expectedScans}`, { method: "POST", body: task.program as Uint8Array<ArrayBuffer> });
        assert.equal(started.status, 200, await started.text());
        finalizer.attachFinalizerInput(checkedOutput(base));
        const opened = new Set<number>();
        const absent = async (scanIndex: number) => {
          if (opened.has(scanIndex)) return;
          opened.add(scanIndex);
          await fetch(`${base}/absent/${scanIndex}`, { method: "POST" });
        };
        const reducer: PreparedHashReducer = {
          waitConsumeReady: async () => undefined,
          accept: async (scanIndex: number, stream: ReadableStream<Uint8Array>) => {
            opened.add(scanIndex);
            const put = await fetch(`${base}/input/${scanIndex}`, { method: "PUT", body: stream, duplex: "half" } as RequestInit);
            if (!put.ok) throw new Error(`edge ${scanIndex}: ${await put.text()}`);
          },
          completeScan: absent,
          seal: async () => {
            for (let scan = 0; scan < expectedScans; scan++) await absent(scan);
          },
          abort: async () => {
            await fetch(`${base}/stop`, { method: "POST" }).catch(() => undefined);
          },
        };
        reducerFor(task.taskId).resolve(reducer);
        return reducer;
      },
      runScan: async (task, scanIndex, destinations) => {
        const opened: number[] = [];
        const edges: Promise<void>[] = [];
        await runScanStreamPool({
          wasm: node,
          program: task.program,
          scratchBytes: 1024 * 1024,
          partitionCount: Math.max(...destinations.map((d) => d.partition)) + 1,
          readAt: claimingReadAt((_source, offset, length) => readAt(Number(offset), length), () => nextUnit(task.taskId)),
          onPartitionStream: (partition, body) => {
            const dest = destinations.find((d) => d.partition === partition)!;
            opened.push(partition);
            edges.push(reducerFor(dest.taskId).promise.then((reducer) => reducer.accept(scanIndex, body)));
          },
        });
        await Promise.all(edges);
        return { openedPartitions: opened, bytes: 0 };
      },
    };

    try {
      const result = await runHashJob({ wasm: { node, coordinator }, plan: graph, readAt, hashRemote });
      const data = result.rows ? { rows: result.rows } : { partial: result.partial! };
      const ours = (JSON.parse(resultJson(coordinator, graph, data, planned.rowCap)) as { rows: unknown[][] }).rows
        .map((row) => row.map(String));
      const duck = await (await DuckDBInstance.create(":memory:")).connect();
      const expected = (await duck.runAndReadAll(SQL.replace("FROM hits", `FROM '${parquet}'`))).getRows()
        .map((row) => row.map(String));
      assert.deepEqual(ours, expected);
    } finally {
      for (const child of children) child.kill();
      fs.closeSync(fd);
      fs.rmSync(spillRoot, { recursive: true, force: true });
    }
  });
});
