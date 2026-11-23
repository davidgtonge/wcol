import assert from "node:assert/strict";
import { describe, it } from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runScanStreamPool } from "../src/runtime/host/run-scan-stream-pool.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

describe("hash scan edges", () => {
  it("a refused edge fails the scan instead of blocking it", { timeout: 30_000 }, async () => {
    // Cloudflare shape: the reducer refuses an edge and never reads it. The scan
    // then blocks on that full port; before the abort signal it never finished, so the
    // refusal in its pending pushes was never observed and the job hit the stall timeout.
    const parquet = path.join(os.tmpdir(), `wcol-edge-refusal-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (SELECT (i % 5000)::BIGINT AS k, i::BIGINT AS v FROM range(400000) t(i))
       TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 50000)`,
    );
    const file = fs.readFileSync(parquet);
    const readAt = (offset: number, length: number) => new Uint8Array(file.subarray(offset, offset + length));
    const footer = await readParquetFooter(async (offset, length) => readAt(offset, length), file.byteLength);
    const planned = planJob(await loadCoordinatorWasm(), {
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
    const graph = planned.graph;
    const partitioned = graph.edges.filter((edge) => edge.partition !== undefined);
    const scanId = partitioned[0]!.from;
    const scan = graph.tasks.find((task) => task.id === scanId)!;
    const partitionCount = new Set(partitioned.map((edge) => edge.partition)).size;

    const refusal = new Error("reducer refused edge: hash attempt is not the active node session");
    const abort = new AbortController();
    const started = Date.now();
    await assert.rejects(
      runScanStreamPool({
        wasm: await loadNodeWasm(),
        program: scan.program,
        scratchBytes: 1024 * 1024,
        partitionCount,
        readAt: async (_source, offset, length) => readAt(Number(offset), length),
        signal: abort.signal,
        onPartitionStream: () => {
          // Never read the edge; refuse it once it is open, like a fresh reducer instance.
          setTimeout(() => abort.abort(refusal), 20);
        },
      }),
      (error: unknown) => error === refusal,
    );
    assert.ok(Date.now() - started < 10_000, "scan took too long to fail");
  });
});
