/**
 * System checks past the component tests:
 * the largest legal hash mesh (100 scans × 64 reducers), and a Parquet
 * export of every group with no row cap.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runGatherGraph } from "../src/runtime/coordinator/run-local-graph.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

function heapMb(): number {
  return process.memoryUsage().heapUsed / (1024 * 1024);
}

async function readAt(file: string, offset: number, length: number): Promise<Uint8Array> {
  const fh = await fs.promises.open(file, "r");
  try {
    const out = new Uint8Array(length);
    await fh.read(out, 0, length, offset);
    return out;
  } finally {
    await fh.close();
  }
}

describe("scale milestone", { concurrency: 1 }, () => {
  it("runs a 100-scan by 64-reducer shuffle with bounded scan concurrency", { timeout: 180_000 }, async () => {
    const file = path.join(os.tmpdir(), `wcol-shuffle-100x64-${process.pid}.parquet`);
    const rows = 204_800;
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT i::BIGINT AS k FROM range(${rows}) t(i)) TO '${file.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)`,
    );
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(file).size;
    const footer = await readParquetFooter((offset, length) => readAt(file, offset, length), size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json", rowCap: 1 },
        hashReducers: 64,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 100,
      batchRows: 256,
      minBytesPerNode: 1,
      hashReducers: 64,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error(`shuffle did not plan a graph (${planned.kind})`);
    }
    const partitionEdges = planned.graph.edges.filter((edge) => edge.partition !== undefined);
    const scans = new Set(partitionEdges.map((edge) => edge.from));
    const reducers = new Set(partitionEdges.map((edge) => edge.to));
    const wasmSamples: number[] = [];
    const heapBefore = heapMb();
    const started = performance.now();
    try {
      const result = await runLocalHashGraph({ node, coordinator }, planned.graph, (offset, length) => readAt(file, offset, length), {
        scheduling: { openingConcurrency: 6, maxActiveScans: 16, maxActiveIngress: 8 },
        onScanProgress: (info) => wasmSamples.push(info.wasmMemoryBytes),
        onReducerProgress: (info) => wasmSamples.push(info.wasmMemoryBytes),
      });
      const json = JSON.parse(resultJson(coordinator, planned.graph, { rows: result.bytes }, planned.rowCap)) as {
        rows?: unknown[][];
      };
      const peakWasm = wasmSamples.reduce((max, sample) => Math.max(max, sample), 0);
      const report = {
        scans: scans.size,
        reducers: reducers.size,
        partitionEdges: partitionEdges.length,
        openedEdges: result.timing?.openedEdges ?? 0,
        peakSimultaneousScans: result.peakSimultaneousScans,
        wallMs: Math.round(performance.now() - started),
        heapGrowthMb: Number((heapMb() - heapBefore).toFixed(1)),
        peakWasmLinearBytes: peakWasm,
        hostLedgerPeakBytes: result.ledgerPeakAdapterBytes,
        resultRows: json.rows?.length ?? 0,
      };
      console.log(`shuffle-mesh ${JSON.stringify(report)}`);
      assert.equal(scans.size, 100);
      assert.equal(reducers.size, 64);
      assert.equal(partitionEdges.length, 100 * 64);
      // Scans claim row groups as they run, so on a small file a few fast scans may take
      // all the work. Each scan that ran opens every partition: its keys hash to all 64.
      const opened = result.timing?.openedEdges ?? 0;
      assert.ok(opened >= 64 && opened <= 100 * 64 && opened % 64 === 0, `opened edges ${opened}`);
      assert.ok(result.peakSimultaneousScans <= 16, `scans in flight ${result.peakSimultaneousScans}`);
      assert.equal(json.rows?.length, 1);
      assert.ok(peakWasm > 0 && peakWasm < 128 * 1024 * 1024);
    } finally {
      await fs.promises.unlink(file).catch(() => undefined);
    }
  });

  it("exports every group to Parquet with no row cap", { timeout: 180_000 }, async () => {
    const file = path.join(os.tmpdir(), `wcol-export-src-${process.pid}.parquet`);
    const out = path.join(os.tmpdir(), `wcol-export-out-${process.pid}.parquet`);
    const rows = 5_000;
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT i::BIGINT AS k FROM range(${rows}) t(i)) TO '${file.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(file).size;
    const footer = await readParquetFooter((offset, length) => readAt(file, offset, length), size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: {
          format: "parquet",
          bucket: "output",
          prefix: "out/",
          targetRowGroupRows: 1024,
          targetFileBytes: 64 * 1024 * 1024,
        },
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 2,
      batchRows: 1024,
      minBytesPerNode: 1,
    });
    if (planned.kind !== "parquet" || planned.graph.kind !== "graph") {
      throw new Error(`export did not plan parquet (${planned.kind})`);
    }
    const heapBefore = heapMb();
    const started = performance.now();
    try {
      const bytes = await runGatherGraph(
        { node, coordinator },
        planned.graph,
        (offset, length) => readAt(file, offset, length),
      );
      await fs.promises.writeFile(out, bytes);
      const check = await (await DuckDBInstance.create(":memory:")).connect();
      const reader = await check.runAndReadAll(
        `SELECT COUNT(*)::BIGINT AS n, SUM(c)::BIGINT AS total FROM read_parquet('${out.replaceAll("'", "''")}')`,
      );
      const gotRows = Number(reader.getRows()[0]?.[0] ?? 0);
      const gotTotal = Number(reader.getRows()[0]?.[1] ?? 0);
      const report = {
        outputBytes: bytes.byteLength,
        parquetGroups: gotRows,
        countedRows: gotTotal,
        wallMs: Math.round(performance.now() - started),
        heapGrowthMb: Number((heapMb() - heapBefore).toFixed(1)),
        startsWithParquetMagic: bytes[0] === 0x50 && bytes[1] === 0x41,
      };
      console.log(`parquet-export ${JSON.stringify(report)}`);
      assert.equal(gotRows, rows);
      assert.equal(gotTotal, rows);
      assert.ok(bytes.byteLength > 1000);
    } finally {
      await fs.promises.unlink(file).catch(() => undefined);
      await fs.promises.unlink(out).catch(() => undefined);
    }
  });
});
