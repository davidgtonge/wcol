import assert from "node:assert/strict";
import { describe, it } from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

describe("stream pool host reads", () => {
  it("keeps several plan fetches in flight on one scan, bounded by fetch depth", { timeout: 60_000 }, async () => {
    // One scan over many row groups: before, each ReadAt was awaited before the next step.
    const parquet = path.join(os.tmpdir(), `wcol-concurrent-reads-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      // Random values do not compress, so the read column spans many 2 MiB fetches.
      `COPY (SELECT (i % 97)::BIGINT AS k, (hash(i) % 4611686018427387903)::BIGINT AS v FROM range(3000000) t(i))
       TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)`,
    );
    const file = fs.readFileSync(parquet);
    const read = (offset: number, length: number) => new Uint8Array(file.subarray(offset, offset + length));
    const footer = await readParquetFooter(async (offset, length) => read(offset, length), file.byteLength);
    const coordinator = await loadCoordinatorWasm();
    const node = await loadNodeWasm();
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, SUM(v) AS s FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: file.byteLength, footer }],
      maxNodes: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("bad plan");

    let inFlight = 0;
    let peak = 0;
    let reads = 0;
    const slowReadAt = async (offset: number, length: number): Promise<Uint8Array> => {
      inFlight += 1;
      reads += 1;
      peak = Math.max(peak, inFlight);
      // Long enough that a sequential host would never overlap two reads.
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight -= 1;
      return read(offset, length);
    };
    await runLocalHashGraph({ node, coordinator }, planned.graph, slowReadAt);
    assert.ok(reads > 8, `expected many fetches, got ${reads}`);
    assert.ok(peak > 1, `reads never overlapped (peak ${peak})`);
    assert.ok(peak <= 8, `more reads in flight than the plan's fetch depth (peak ${peak})`);
  });
});
