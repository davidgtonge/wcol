import assert from "node:assert/strict";
import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { ParquetFile } from "../src/index.ts";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const wasm = path.join(root, "rust/target/wasm32-unknown-unknown/release/wcol_parquet.wasm");
const fixture = path.join(root, "tests/fixtures/flat.parquet");
const ready = existsSync(wasm) && existsSync(fixture);

test("parquet file reads a flat footer through wasm", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
  const file = await ParquetFile.open(fixture);
  try {
    assert.equal(file.plan.rows, 4n);
    assert.deepEqual(file.plan.leaves.map((leaf) => leaf.name), ["a", "b", "c", "d", "e", "big"]);
    assert.equal(file.plan.chunks.length, 6);
  } finally {
    await file.close();
  }
});

test("parquet file reads a flat footer over HTTP range reads", { skip: ready ? false : "build wcol-parquet wasm and the flat fixture first" }, async () => {
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
    const file = await ParquetFile.open(`http://127.0.0.1:${address.port}/flat.parquet`);
    try {
      assert.equal(file.plan.rows, 4n);
      assert.deepEqual(file.plan.leaves.map((leaf) => leaf.name), ["a", "b", "c", "d", "e", "big"]);
    } finally {
      await file.close();
    }
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});
