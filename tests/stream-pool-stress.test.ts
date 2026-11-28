/**
 * Local soak: 100×100 stream pool with late attach + slow egress, then multi-spill hash job.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";
import { createStreamPool } from "../src/runtime/host/stream-pool.ts";
import { streamFromBytes } from "../src/runtime/host/pool-streams.ts";
import type { SpillBoundary } from "../src/runtime/host/host-types.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function heapMb(): number {
  return process.memoryUsage().heapUsed / (1024 * 1024);
}

describe("stream pool local stress", { concurrency: 1 }, () => {
  it("100 attachInput ports with staggered arrival and slow egress", { timeout: 120_000 }, async () => {
    const wasm = await loadNodeWasm();
    const ports = 100;
    const heapBefore = heapMb();
    const pool = createStreamPool({
      inputs: ports,
      inputSize: 64,
      outputs: ports,
      outputSize: 64,
      wasm,
      echo: true,
      yieldToEventLoop: () => sleep(0),
    });

    const outputDone = pool.outputStreams.map(async (stream, port) => {
      await sleep(port % 7);
      const reader = stream.getReader();
      let bytes = 0;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (port % 3 === 0) await sleep(0);
      }
      return bytes;
    });

    for (let port = 0; port < ports; port++) {
      if (port % 11 === 0) await sleep(0);
      pool.attachInput(port, streamFromBytes(new Uint8Array([port & 0xff])));
    }

    const [, ...counts] = await Promise.all([pool.finished, ...outputDone]);
    assert.ok(counts.every((n) => n === 1), "each echo port should return one byte");
    pool.close();
    const heapAfter = heapMb();
    assert.ok(
      heapAfter - heapBefore < 64,
      `heap grew ${(heapAfter - heapBefore).toFixed(1)} MiB during 100×100 echo soak`,
    );
  });

  it("hash group-by with R2 spill uses streaming upload without huge JS buffers", { timeout: 300_000 }, async () => {
    const parquet = path.join(os.tmpdir(), `wcol-stream-stress-spill-${process.pid}.parquet`);
    const groups = 8_000;
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(
      `COPY (
        SELECT printf('key-%d', i) AS k FROM range(${groups}) t(i)
      ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`,
    );

    const readAt = async (offset: number, length: number): Promise<Uint8Array> => {
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
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: 48 * 1024,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 2,
      batchRows: 256,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error("spill stress did not plan a graph");
    }

    const spillR2 = createMemoryR2SpillBucket();
    let boundaries: SpillBoundary[] = [];
    const heapBefore = heapMb();
    const { bytes: rows } = await runLocalHashGraph({ node, coordinator }, planned.graph, readAt, {
      spillR2Bucket: spillR2,
      reducerEgressPipeBytes: 1024 * 1024,
      onReducerProgress: (info) => {
        if ((info.spillBoundaries?.length ?? 0) > boundaries.length) {
          boundaries = info.spillBoundaries ?? boundaries;
        }
      },
    });
    const parsed = JSON.parse(resultJson(coordinator, planned.graph, { rows }, planned.rowCap)) as {
      rows?: unknown[];
    };
    assert.ok((parsed.rows?.length ?? 0) > 0, "spill stress returned no rows");
    assert.ok(boundaries.length >= 4, `expected multiple spill runs, saw ${boundaries.length}`);
    assert.ok(spillR2.puts >= 1, "expected at least one R2 spill put");
    assert.ok(spillR2.bytesUploaded > 0, "expected spill bytes uploaded");

    const heapAfter = heapMb();
    assert.ok(
      heapAfter - heapBefore < 128,
      `heap grew ${(heapAfter - heapBefore).toFixed(1)} MiB during spill stress (streaming upload should stay bounded)`,
    );

    await fs.promises.unlink(parquet).catch(() => undefined);
  });
});
