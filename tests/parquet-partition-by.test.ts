/**
 * `partitionBy` exports: the plan's node sinks write one file per value under Hive paths,
 * and the host splits them into objects. Every task runs through the release Wasm and
 * `exportSink`, into an in-memory R2; DuckDB reads the files back with Hive partitioning.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { exportSink, type ExportObject } from "../src/runtime/host/hive-upload-sink.ts";
import { attemptObjectKeyJobs } from "../src/runtime/host/parquet-attempt-key.ts";
import type { R2MultipartSession } from "../src/runtime/host/ports/r2-write.ts";
import { runWcolEngine } from "../src/runtime/host/run-wcol-engine.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

/** Completed objects by key; multipart parts are joined in part order on complete. */
function memoryBucket() {
  const objects = new Map<string, Uint8Array>();
  const uploads = new Map<string, { key: string; parts: Map<number, Uint8Array> }>();
  let next = 0;
  const session = (key: string): R2MultipartSession => ({
    async create(objectKey) {
      const id = `u${next++}`;
      uploads.set(id, { key: objectKey, parts: new Map() });
      return id;
    },
    async uploadPart(id, number, body) {
      uploads.get(id)!.parts.set(number, body.slice());
      return `e${number}`;
    },
    async complete(id, parts) {
      const upload = uploads.get(id)!;
      assert.equal(upload.key, key);
      const bodies = parts.map((part) => upload.parts.get(part.partNumber)!);
      const out = new Uint8Array(bodies.reduce((n, body) => n + body.byteLength, 0));
      let at = 0;
      for (const body of bodies) {
        out.set(body, at);
        at += body.byteLength;
      }
      objects.set(upload.key, out);
      uploads.delete(id);
    },
    async abort(id) {
      uploads.delete(id);
    },
  });
  return { objects, session };
}

async function exportPartitioned(
  file: Buffer,
  body: Record<string, unknown>,
  maxNodes: number,
): Promise<{ objects: Map<string, Uint8Array>; written: ExportObject[]; layout: string }> {
  const readAt = (offset: number, length: number) => new Uint8Array(file.subarray(offset, offset + length));
  const footer = await readParquetFooter(async (offset, length) => readAt(offset, length), file.byteLength);
  const planned = planJob(await loadCoordinatorWasm(), {
    body: JSON.stringify(body),
    footers: [{ fileSize: file.byteLength, footer }],
    maxNodes,
    minBytesPerNode: 1,
  });
  if (planned.kind !== "parquet" || planned.graph.kind !== "graph") throw new Error(`not a parquet graph (${planned.kind})`);
  const graph = planned.graph;
  assert.equal(graph.edges.length, 0, "a partitioned export is scans only");
  const layout = graph.output?.layout ?? "single_file";
  const bucket = memoryBucket();
  const node = await loadNodeWasm();
  const written: ExportObject[] = [];
  for (const task of graph.tasks) {
    const sink = exportSink(layout, bucket.session, attemptObjectKeyJobs("job", task.id, 1));
    await runWcolEngine({
      wasm: node,
      plan: task.program,
      scratchBytes: 1024 * 1024,
      layout: { inputs: 0, outputs: 1 },
      host: {
        readAt: async (_source, offset, length) => readAt(offset, length),
        writePort: (port, chunk) => sink.writePort(port, chunk),
        yield: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
      },
    });
    written.push(...(await sink.finish()));
  }
  return { objects: bucket.objects, written, layout };
}

async function sourceFile(sql: string): Promise<Buffer> {
  const file = path.join(os.tmpdir(), `wcol-partition-src-${process.pid}-${Math.random().toString(36).slice(2)}.parquet`);
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await duck.run(`COPY (${sql}) TO '${file}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)`);
  const bytes = fs.readFileSync(file);
  fs.unlinkSync(file);
  return bytes;
}

/** Write the objects under a temp dir and read them as one Hive-partitioned table. */
async function readHive(objects: Map<string, Uint8Array>, sql: (glob: string) => string): Promise<unknown[][]> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wcol-hive-"));
  try {
    for (const [key, bytes] of objects) {
      const out = path.join(dir, key.slice(key.indexOf("/", key.indexOf("/", key.indexOf("/") + 1) + 1) + 1));
      fs.mkdirSync(path.dirname(out), { recursive: true });
      fs.writeFileSync(out, bytes);
    }
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    const reader = await duck.runAndReadAll(sql(`${dir}/**/*.parquet`));
    return reader.getRows();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

describe("partitionBy exports", () => {
  it("writes one Hive directory per value, without the partition column", { timeout: 60_000 }, async () => {
    const file = await sourceFile(
      "SELECT (i % 5)::BIGINT AS day, i::BIGINT AS v, ('s' || (i % 7))::VARCHAR AS tag FROM range(50000) t(i)",
    );
    const { objects, written, layout } = await exportPartitioned(
      file,
      {
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT day, v, tag FROM t WHERE v >= 1000",
        output: { format: "parquet", bucket: "output", prefix: "out/", partitionBy: ["day"] },
      },
      4,
    );
    assert.equal(layout, "hive");
    assert.ok(written.length >= 5, `objects ${written.length}`);
    for (const object of written) {
      assert.match(object.objectKey, /^_jobs\/job\/[0-9a-f]{16}\/1\/day=\d\/part-\d+\.parquet$/);
      assert.equal(object.partitionValues.length, 1);
      assert.ok(object.objectKey.includes(`day=${object.partitionValues[0]}/`));
      assert.equal(object.bytes, objects.get(object.objectKey)!.byteLength);
    }
    const rows = await readHive(
      objects,
      (glob) => `SELECT day, COUNT(*)::BIGINT, SUM(v)::BIGINT, COUNT(DISTINCT tag)::BIGINT
                 FROM read_parquet('${glob}', hive_partitioning = true) GROUP BY day ORDER BY day`,
    );
    const expected = [];
    for (let day = 0; day < 5; day++) {
      let count = 0n;
      let sum = 0n;
      for (let i = day; i < 50000; i += 5) {
        if (i >= 1000) {
          count += 1n;
          sum += BigInt(i);
        }
      }
      expected.push([BigInt(day), count, sum, 7n]);
    }
    assert.deepEqual(rows, expected);
    const counted = written.reduce((n, object) => n + (object.rows ?? 0), 0);
    assert.equal(counted, Number(expected.reduce((n, row) => n + (row[1] as bigint), 0n)));
    const columns = await readHive(objects, (glob) => `DESCRIBE SELECT * FROM read_parquet('${glob}', hive_partitioning = false)`);
    assert.deepEqual(columns.map((row) => row[0]), ["v", "tag"], "the files hold no partition column");
  });

  it("splits by text and nulls, and closes files past the open limit", { timeout: 60_000 }, async () => {
    // 20 values in a scattered order: more than the 8 files a task keeps open.
    const file = await sourceFile(
      `SELECT CASE WHEN i % 21 = 0 THEN NULL ELSE 'k/' || (i % 21) END AS key, i::BIGINT AS v FROM range(42000) t(i)`,
    );
    const { objects, written } = await exportPartitioned(
      file,
      {
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT key, v FROM t",
        output: { format: "parquet", bucket: "output", prefix: "out/", partitionBy: ["key"] },
      },
      1,
    );
    assert.ok(written.length > 21, `a closed value starts a new file (${written.length} files for 21 values)`);
    assert.ok(written.some((object) => object.partitionValues[0] === "__HIVE_DEFAULT_PARTITION__"));
    assert.ok(written.some((object) => object.partitionValues[0] === "k%2F7"));
    const rows = await readHive(
      objects,
      (glob) => `SELECT COUNT(DISTINCT key)::BIGINT, COUNT(*)::BIGINT, SUM(v)::BIGINT
                 FROM read_parquet('${glob}', hive_partitioning = true)`,
    );
    assert.deepEqual(rows, [[20n, 42000n, BigInt((42000 * 41999) / 2)]]);
  });

  it("rejects partitionBy on an aggregate", async () => {
    const file = await sourceFile("SELECT (i % 5)::BIGINT AS day FROM range(1000) t(i)");
    await assert.rejects(
      exportPartitioned(
        file,
        {
          sources: { t: { key: "http://example.invalid/t.parquet" } },
          sql: "SELECT day, COUNT(*) AS c FROM t GROUP BY day",
          output: { format: "parquet", bucket: "output", prefix: "out/", partitionBy: ["day"] },
        },
        2,
      ),
      /partitionBy is only supported on scan, filter, and projection/,
    );
  });
});
