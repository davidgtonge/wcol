import assert from "node:assert/strict";
import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { planParquetJob } from "../src/index.ts";
import { assignChunks, planParquetStream } from "../src/runtime/parquet-job.ts";

// These tests check the plans `planParquetJob` and `planParquetStream` build. The TS graph
// executor that used to run them (`archive/legacy-ts-stream`) was retired with the Parquet chunk
// oracle; query results are covered by the GraphHost tests (`vertical-slice`, `partial-oracle`).

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const wasm = path.join(root, "rust/target/wasm32-unknown-unknown/release/wcol_parquet.wasm");
const fixture = path.join(root, "tests/fixtures/flat.parquet");
const ready = existsSync(wasm) && existsSync(fixture);

test("grouped parquet stream plans one folding scan", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const planned = await planParquetStream({
    url: fixture,
    sql: "SELECT a, COUNT(*) FROM flat WHERE a > 5 GROUP BY a ORDER BY COUNT(*) DESC",
    parallelism: 1,
  });
  assert.equal(planned.nodes.some((node) => node.op === "scan-parquet"), false);
  assert.equal(planned.nodes.some((node) => node.op === "aggregate-rows"), false);
  assert.equal(planned.nodes.filter((node) => node.op === "scan-parquet-fold").length, 1);
});

test("count star plans no column reads", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const planned = await planParquetStream({
    url: fixture,
    sql: "SELECT COUNT(*) FROM flat",
    parallelism: 1,
  });
  assert.equal(planned.rangeRequests, 0);
  assert.equal(planned.compressedBytes, 0);
});

test("row parquet stream plans scan, filter, transform, count", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const planned = await planParquetStream({
    url: fixture,
    sql: "SELECT a FROM flat WHERE a > 5",
    parallelism: 1,
    shape: "rows",
  });
  assert.deepEqual(planned.nodes.map((node) => node.op), ["scan-parquet-rows", "filter-rows", "transform", "count"]);
});

test("parquet tasks cover every chunk once", () => {
  const groups = assignChunks([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 4);
  assert.equal(groups.length, 4);
  assert.deepEqual(groups.flat(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
});

test("parquet job plans one scan per file", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const planned = await planParquetJob({
    sources: [fixture, fixture],
    sql: "SELECT SUM(a) FROM flat",
    tasks: 4,
    footerBytes: 0,
  });
  assert.equal(planned.files, 2);
  assert.equal(planned.footerFiles, 2);
  assert.equal(planned.scans, 2);
  assert.equal(planned.nodes.filter((node) => node.op === "scan-parquet").length, 2);
});

test("parquet job carries the filter into each scan", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const planned = await planParquetJob({
    sources: [fixture, fixture],
    sql: "SELECT SUM(a) FROM flat WHERE a > 5",
    footerBytes: 0,
  });
  const scans = planned.nodes.filter((node) => node.op === "scan-parquet");
  assert.equal(scans.length, 2);
  assert.ok(scans.every((node) => node.parquet?.sql?.includes("WHERE a > 5")), JSON.stringify(scans.map((n) => n.parquet)));
});

test("parquet job plans from a footer read over HTTP ranges", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const size = statSync(fixture).size;
  const server = createServer((request, response) => {
    const range = request.headers.range;
    if (!range) {
      response.writeHead(200, { "content-length": size, "accept-ranges": "bytes" });
      if (request.method === "HEAD") {
        response.end();
        return;
      }
      createReadStream(fixture).pipe(response);
      return;
    }
    const suffix = /^bytes=-(\d+)$/.exec(range);
    const match = /^bytes=(\d+)-(\d+)$/.exec(range);
    let start: number;
    let end: number;
    if (suffix) {
      const length = Number(suffix[1]);
      start = Math.max(0, size - length);
      end = size - 1;
    } else if (match) {
      start = Number(match[1]);
      end = Number(match[2]);
    } else {
      response.writeHead(416).end();
      return;
    }
    response.writeHead(206, {
      "content-length": end - start + 1,
      "content-range": `bytes ${start}-${end}/${size}`,
      "accept-ranges": "bytes",
    });
    if (request.method === "HEAD") {
      response.end();
      return;
    }
    createReadStream(fixture, { start, end }).pipe(response);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address == null || typeof address === "string") throw new Error("test server has no port");
  try {
    const planned = await planParquetJob({
      sources: [`http://127.0.0.1:${address.port}/flat.parquet`],
      sql: "SELECT SUM(a) FROM flat",
      footerBytes: 0,
    });
    const scan = planned.nodes.find((node) => node.op === "scan-parquet");
    assert.ok(scan?.parquet?.groups?.[0]?.chunks.length);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});
