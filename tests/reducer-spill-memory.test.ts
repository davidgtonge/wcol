/**
 * A reducer that fills and spills several times must reuse its table.
 * Wasm linear memory may jump on the first flush. Later flushes must not
 * keep growing it.
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
import type { SpillBoundary } from "../src/runtime/host/host-types.ts";
import { fileRangeReader } from "./helpers/streams.ts";

const GROUPS = 4_000;
const TARGET_GROUP_BYTES = 64 * 1024;
const parquet = path.join(os.tmpdir(), `wcol-spill-memory-${process.pid}.parquet`);

const readFileAt = fileRangeReader(parquet);

describe("reducer spill memory", { concurrency: 1 }, () => {
  it("stops growing wasm memory after the first spill runs", { timeout: 120_000 }, async () => {
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (
        SELECT printf('user-%d', i) AS k FROM range(${GROUPS}) t(i)
      ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readFileAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: TARGET_GROUP_BYTES,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 128,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error("spill query did not plan a graph");
    }
    let boundaries: SpillBoundary[] = [];
    const { bytes: rows } = await runLocalHashGraph({ node, coordinator }, planned.graph, readFileAt, {
      onReducerProgress: (info) => {
        if ((info.spillBoundaries?.length ?? 0) > boundaries.length) {
          boundaries = info.spillBoundaries ?? boundaries;
        }
      },
    });
    const parsed = JSON.parse(resultJson(coordinator, planned.graph, { rows }, planned.rowCap)) as {
      rows?: unknown[];
    };
    assert.ok((parsed.rows?.length ?? 0) > 0, "query returned no rows");
    assert.ok(boundaries.length >= 4, `expected several spill runs, saw ${boundaries.length}`);
    assert.ok(boundaries[0]?.wasmAfter && boundaries[0].wasmAfter > 0, "wasm linear memory was not recorded");
    const warm = boundaries[1]!.wasmAfter;
    const later = boundaries.slice(1).map((row) => row.wasmAfter);
    const peak = Math.max(...later);
    assert.ok(
      peak - warm < 4 * 1024 * 1024,
      `wasm grew ${peak - warm} bytes after warmup: ${later.join(",")}`,
    );
    const tables = boundaries.slice(1).map((row) => row.tableAfter);
    const tablePeak = Math.max(...tables);
    assert.ok(
      tablePeak <= Math.max(tables[0]!, 1) * 2,
      `group table kept growing after warmup: ${tables.join(",")}`,
    );
    assert.ok(boundaries.every((row) => row.pageCap > 0), "spill page buffer capacity was not recorded");
  });
});
