/**
 * Same-sized spill cycles should reach a stable post-flush live baseline.
 * Growing liveAfter across dozens of runs indicates retained spill output.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import type { SpillBoundary } from "../src/runtime/host/host-types.ts";
import { fileRangeReader } from "./helpers/streams.ts";

const GROUPS = 12_000;
const TARGET_GROUP_BYTES = 32 * 1024;
const MIN_RUNS = 50;
const WARMUP = 5;
const parquet = path.join(os.tmpdir(), `wcol-spill-plateau-${process.pid}.parquet`);

const readFileAt = fileRangeReader(parquet);

describe("reducer spill live plateau", { concurrency: 1 }, () => {
  it("keeps post-flush live bytes flat after warm-up", { timeout: 180_000 }, async () => {
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
    await runLocalHashGraph({ node, coordinator }, planned.graph, readFileAt, {
      onReducerProgress: (info) => {
        if ((info.spillBoundaries?.length ?? 0) > boundaries.length) {
          boundaries = info.spillBoundaries ?? boundaries;
        }
      },
    });
    assert.ok(boundaries.length >= MIN_RUNS, `expected at least ${MIN_RUNS} spill runs, saw ${boundaries.length}`);
    const liveReset = boundaries.map((row) => row.liveAfter);
    const warmBaseline = liveReset[WARMUP]!;
    const tail = liveReset.slice(WARMUP);
    const tailPeak = Math.max(...tail);
    assert.ok(
      tailPeak - warmBaseline < 2 * 1024 * 1024,
      `liveAfter grew ${tailPeak - warmBaseline} bytes after run ${WARMUP + 1}: ${tail.join(",")}`,
    );
    const serialize = boundaries.slice(WARMUP).map((row) => row.serializeBytes);
    assert.ok(
      Math.max(...serialize) <= Math.max(...boundaries.slice(0, WARMUP + 1).map((row) => row.serializeBytes)) * 2,
      `serialize peak drift: ${serialize.join(",")}`,
    );
    const tailBoundaries = boundaries.slice(WARMUP);
    const maxQueued = Math.max(...tailBoundaries.map((row) => row.rustQueuedSpillBytes));
    const maxInflight = Math.max(...tailBoundaries.map((row) => row.rustInflightSpillBytes));
    const maxPage = Math.max(...boundaries.map((row) => row.serializeBytes));
    assert.ok(
      maxQueued <= maxPage * 2,
      `rust queued spill bytes should stay page-sized (maxQueued=${maxQueued}, maxPage=${maxPage})`,
    );
    assert.ok(
      maxInflight <= maxPage * 4,
      `rust inflight spill bytes should stay page-sized (maxInflight=${maxInflight}, maxPage=${maxPage})`,
    );
    const maxHostPendingSql = Math.max(...tailBoundaries.map((row) => row.hostPendingSqlBytes));
    assert.ok(
      maxHostPendingSql <= 512 * 1024,
      `host pending SQL bytes should be bounded by concurrent writes (saw ${maxHostPendingSql})`,
    );
  });
});
