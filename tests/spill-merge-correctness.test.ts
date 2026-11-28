/**
 * Spilled hash aggregation must match a non-spill reference on the same data.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";

const GROUPS = 50_000;

function parseRows(
  coordinator: Awaited<ReturnType<typeof loadCoordinatorWasm>>,
  planned: Extract<Awaited<ReturnType<typeof planJob>>, { kind: "json" }>,
  bytes: Uint8Array,
): (string | null)[][] {
  const json = JSON.parse(resultJson(coordinator, planned.graph, { rows: bytes }, planned.rowCap)) as {
    rows?: unknown[][];
  };
  return (json.rows ?? []).map((row) => row.map((v) => (v === null ? null : String(v))));
}

describe("spill merge correctness", { concurrency: 1 }, () => {
  it("matches DuckDB when spill forces multi-run R2 merge", { timeout: 300_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-merge-correct-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (
        SELECT printf('user-%d', (i % 5000)) AS k FROM range(${GROUPS}) t(i)
      ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const expected = (await duck.runAndReadAll(
      `SELECT k, COUNT(*)::VARCHAR AS c FROM '${parquet.replaceAll("'", "''")}' GROUP BY k ORDER BY k`,
    )).getRows() as string[][];
    expected.sort((a, b) => String(a[0]).localeCompare(String(b[0])));

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

    const planBody = {
      sources: { t: { key: "http://example.invalid/data.parquet" } },
      sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k",
      output: { format: "json", rowCap: 10_000 },
      hashReducers: 2,
      groupSpill: true,
    };

    const noSpill = planJob(coordinator, {
      body: JSON.stringify({ ...planBody, targetGroupBytes: 512 * 1024 * 1024 }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 4096,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (noSpill.kind !== "json" || noSpill.graph.kind !== "graph") throw new Error("bad plan");

    const withSpill = planJob(coordinator, {
      body: JSON.stringify({ ...planBody, targetGroupBytes: 32 * 1024 }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 4096,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (withSpill.kind !== "json" || withSpill.graph.kind !== "graph") throw new Error("bad plan");

    const spillR2 = createMemoryR2SpillBucket();
    const ref = await runLocalHashGraph(
      { node, coordinator },
      noSpill.graph,
      readAt,
      { reducerEgressPipeBytes: 1024 * 1024 },
    );
    const spilled = await runLocalHashGraph(
      { node, coordinator },
      withSpill.graph,
      readAt,
      { reducerEgressPipeBytes: 1024 * 1024, spillR2Bucket: spillR2 },
    );

    assert.ok(spillR2.puts >= 2, `expected multiple spill runs, puts=${spillR2.puts}`);
    assert.ok(spillR2.gets > 0, `expected spill run reads, gets=${spillR2.gets}`);

    const refRows = parseRows(coordinator, noSpill, ref.bytes).sort((a, b) => a[0]!.localeCompare(b[0]!));
    const spillRows = parseRows(coordinator, withSpill, spilled.bytes).sort((a, b) =>
      a[0]!.localeCompare(b[0]!),
    );
    assert.deepEqual(spillRows, refRows);
    assert.deepEqual(spillRows, expected);
  });
});
