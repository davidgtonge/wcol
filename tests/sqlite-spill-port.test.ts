/**
 * Group-by spill uses R2 sorted runs (Open/Write/Close put + ranged get), not DO SQLite rows.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runGatherGraph } from "./helpers/run-local-graph.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import {
  createMemoryR2SpillBucket,
  type MemoryR2SpillBucket,
} from "./helpers/memory-r2-spill-bucket.ts";

const GROUPS = 800;

describe("R2 spill byte port", () => {
  it("spills a grouped aggregate through sorted run put and merge read", async () => {
    const file = path.join(os.tmpdir(), `wcol-spill-port-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT range AS a FROM range(${GROUPS})) TO '${file}' (FORMAT parquet)`,
    );
    const size = fs.statSync(file).size;
    const readAt = async (offset: number, length: number) => {
      const fh = await fs.promises.open(file, "r");
      try {
        const out = new Uint8Array(length);
        await fh.read(out, 0, length, offset);
        return out;
      } finally {
        await fh.close();
      }
    };
    const footer = await readParquetFooter(readAt, size);
    const coordinator = await loadCoordinatorWasm();
    const node = await loadNodeWasm();
    const sql = `SELECT a, count(*) FROM t GROUP BY a`;
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql,
        output: { format: "json" },
        groupSpill: true,
        targetGroupBytes: 64 * 1024,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      hashReducers: 2,
    });
    assert.equal(planned.kind, "json");
    assert.equal(planned.graph.kind, "graph");
    const bucket: MemoryR2SpillBucket = createMemoryR2SpillBucket();
    try {
      const graph = planned.graph;
      if (graph.kind !== "graph") throw new Error("expected a graph");
      const hooks = { spillR2Bucket: bucket };
      const bytes = graph.edges.some((edge) => edge.partition !== undefined)
        ? (await runLocalHashGraph({ node, coordinator }, graph, readAt, hooks)).bytes
        : await runGatherGraph({ node, coordinator }, graph, readAt, bucket);
      const parsed = JSON.parse(resultJson(coordinator, planned.graph, { rows: bytes }, 0)) as {
        rows?: unknown[][];
      };
      const rows = parsed.rows ?? [];
      assert.equal(rows.length, GROUPS);
      assert.ok(rows.every((row) => String(row[1]) === "1"));
      assert.ok(bucket.puts >= 1, "expected at least one spill run upload");
      assert.ok(bucket.bytesUploaded > 0);
      assert.ok(bucket.gets >= 1, "merge scan should read spill runs back");
      assert.ok([...bucket.objects.keys()].some((key) => key.includes("wcol-spill/")));
    } finally {
      await fs.promises.unlink(file).catch(() => undefined);
    }
  });
});
