/**
 * One hash scanner over many Parquet row groups. Frames it has handed to the host must not stay
 * in Wasm linear memory: the scan's peak memory must not grow with the bytes it sends. The scan
 * reports once, when it ends, so the check compares the same query over 4x the rows.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const ROWS_PER_GROUP = 200_000;
/** Growth allowed between the small and the large run (allocator rounding, page buffers). */
const PEAK_SLACK = 8 * 1024 * 1024;

type ScanRun = { outputBytes: number; peakWasmBytes: number; rows: string[][]; expected: string[][] };

async function scanOnce(rowGroups: number): Promise<ScanRun> {
  const rows = rowGroups * ROWS_PER_GROUP;
  const parquet = path.join(os.tmpdir(), `wcol-egress-plateau-${process.pid}-${rowGroups}.parquet`);
  let fd = -1;
  const readFileAt = async (offset: number, length: number): Promise<Uint8Array> => {
    const out = new Uint8Array(length);
    fs.readSync(fd, out, 0, length, offset);
    return out;
  };
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  // Unique keys: a scan combines few-valued keys before the exchange and would send almost
  // nothing, while these put every row on the egress path this test measures.
  await duck.run(
    `COPY (SELECT i::BIGINT AS k FROM range(${rows}) t(i)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE ${ROWS_PER_GROUP})`,
  );
  fd = fs.openSync(parquet, "r");
  try {
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(parquet).size;
    const footer = await readParquetFooter(readFileAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        // Partitioned by k like a GROUP BY, so every row crosses the exchange, but each reducer
        // answers with one count: the result stays far under the JSON cap.
        sql: "SELECT COUNT(DISTINCT k) FROM t",
        output: { format: "json" },
        hashReducers: 2,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 2048,
      minBytesPerNode: 1,
      hashReducers: 2,
    });
    if (planned.kind !== "json" || planned.graph.kind !== "graph") {
      throw new Error("hash query did not plan a graph");
    }
    const scan = { outputBytes: 0, peakWasmBytes: 0 };
    const { bytes } = await runLocalHashGraph({ node, coordinator }, planned.graph, readFileAt, {
      onScanProgress: (info) => {
        scan.outputBytes += info.outputBytes;
        scan.peakWasmBytes = Math.max(scan.peakWasmBytes, info.wasmMemoryBytes);
      },
    });
    const json = JSON.parse(resultJson(coordinator, planned.graph, { rows: bytes }, planned.rowCap)) as {
      rows?: unknown[][];
    };
    const got = (json.rows ?? []).map((row) => row.map((value) => (value === null ? "null" : String(value))));
    return { ...scan, rows: got, expected: [[String(rows)]] };
  } finally {
    fs.closeSync(fd);
    await fs.promises.unlink(parquet).catch(() => undefined);
  }
}

describe("hash egress frame release", { concurrency: 1 }, () => {
  it("a scan's peak memory does not grow with the bytes it sends", { timeout: 240_000 }, async () => {
    // Both files are larger than the fetch stage's 16 MiB read buffer, so both runs fill it:
    // what is left to differ is what the scan keeps of the bytes it has sent.
    const small = await scanOnce(24);
    const large = await scanOnce(96);
    assert.deepEqual(small.rows, small.expected);
    assert.deepEqual(large.rows, large.expected);
    assert.ok(
      large.outputBytes > 3 * small.outputBytes,
      `the large scan sent ${large.outputBytes} bytes, the small one ${small.outputBytes}`,
    );
    assert.ok(
      large.peakWasmBytes - small.peakWasmBytes < PEAK_SLACK,
      `peak wasm grew from ${small.peakWasmBytes} to ${large.peakWasmBytes} as output grew from ${small.outputBytes} to ${large.outputBytes}`,
    );
  });
});
