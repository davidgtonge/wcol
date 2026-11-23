/**
 * Reducer → finalizer egress with a small pipe while the finalizer consumes concurrently.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";

const GROUPS = 800_000;

async function parquetWithGroups(outPath: string, groups: number): Promise<void> {
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await duck.run(
    `COPY (SELECT printf('user-%d', i) AS k FROM range(${groups}) t(i)) TO '${outPath.replaceAll("'", "''")}' (FORMAT PARQUET)`,
  );
}

describe("hash reducer egress backpressure", { concurrency: 1 }, () => {
  it("800k groups with 1 MiB reducer pipe completes", { timeout: 300_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-egress-1mib-${process.pid}.parquet`);
    await parquetWithGroups(parquet, GROUPS);
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
        sources: { t: { key: "http://example.invalid/data.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: 1024 * 1024,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 4096,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("bad plan");
    const { bytes } = await runLocalHashGraph(
      { node, coordinator },
      planned.graph,
      readAt,
      {
        reducerEgressPipeBytes: 1024 * 1024,
        spillR2Bucket: createMemoryR2SpillBucket(),
      },
    );
    assert.ok(bytes.byteLength > 0);
  });

  it("800k groups with 256 KiB pipe and slow finalizer completes", { timeout: 600_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-egress-256k-${process.pid}.parquet`);
    await parquetWithGroups(parquet, GROUPS);
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
        sources: { t: { key: "http://example.invalid/data.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: 1024 * 1024,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 4096,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("bad plan");
    const { bytes } = await runLocalHashGraph(
      { node, coordinator },
      planned.graph,
      readAt,
      {
        reducerEgressPipeBytes: 256 * 1024,
        finalizerWriteDelayMs: 1,
        spillR2Bucket: createMemoryR2SpillBucket(),
      },
    );
    assert.ok(bytes.byteLength > 0);
  });
});
