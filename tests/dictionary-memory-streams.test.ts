/**
 * High-cardinality string exchange through the node Wasm isolate.
 * Reports peak linear memory and the host byte-pool high water.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { fileRangeReader } from "./helpers/streams.ts";

const ROWS = 12_000;
const ROW_GROUPS = 8;
const ISOLATE_LIMIT = 128 * 1024 * 1024;
const parquet = path.join(os.tmpdir(), `wcol-dict-memory-${process.pid}.parquet`);

const readFileAt = fileRangeReader(parquet);

describe("high-cardinality dictionary memory", { concurrency: 1 }, () => {
  it("stays inside the 128 MiB isolate while grouping unique strings", { timeout: 180_000 }, async () => {
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (
         SELECT 'https://example.test/' || i::VARCHAR || '/' || repeat('u', 32) AS url
         FROM range(${ROWS}) t(i)
       ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE ${ROWS / ROW_GROUPS})`,
    );
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readFileAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT url, COUNT(*) AS c FROM t GROUP BY url",
        output: { format: "json" },
        hashReducers: 4,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 4,
      batchRows: 128,
      minBytesPerNode: 1,
      hashReducers: 4,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error(`high-cardinality query did not plan a graph (${planned.kind})`);
    }
    const samples: number[] = [];
    try {
      const result = await runLocalHashGraph({ node, coordinator }, planned.graph, readFileAt, {
        onScanProgress: (info) => samples.push(info.wasmMemoryBytes),
        onReducerProgress: (info) => samples.push(info.wasmMemoryBytes),
      });
      const json = JSON.parse(
        resultJson(coordinator, planned.graph, { rows: result.bytes }, planned.rowCap),
      ) as { rows?: unknown[][] };
      const peakWasm = samples.reduce((max, sample) => Math.max(max, sample), 0);
      const report = {
        inputRows: ROWS,
        resultRows: json.rows?.length ?? 0,
        reducers: 4,
        peakWasmLinearBytes: peakWasm,
        hostLedgerPeakBytes: result.ledgerPeakAdapterBytes,
        samples: samples.length,
      };
      console.log(`dictionary-memory ${JSON.stringify(report)}`);
      assert.ok((json.rows?.length ?? 0) > 0, "query returned no rows");
      assert.ok(peakWasm > 0, "wasm linear memory was not observed");
      assert.ok(peakWasm < ISOLATE_LIMIT, `peak wasm ${peakWasm} exceeds ${ISOLATE_LIMIT}`);
    } finally {
      await fs.promises.unlink(parquet).catch(() => undefined);
    }
  });
});
