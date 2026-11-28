/**
 * One Parquet object large enough for several 8 MiB multipart parts.
 * DuckDB reads the engine output, then the same bytes go through the upload
 * sink: a held part shows backpressure, a failed part aborts with no receipt,
 * and a retry completes.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runGatherGraph } from "./helpers/run-local-graph.ts";
import { finishParquetShard, ParquetUploadSink } from "../src/runtime/host/parquet-upload-sink.ts";
import { R2_PART_SIZE, type R2MultipartSession, type R2UploadedPart } from "../src/runtime/host/ports/r2-write.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const ROWS = 2_000_000;

class RecordingR2 implements R2MultipartSession {
  readonly completed: R2UploadedPart[][] = [];
  readonly aborted: string[] = [];
  readonly bodies: Uint8Array[] = [];
  failPartTimes = 0;
  holdPart: number | null = null;
  private hold: Promise<void> = Promise.resolve();
  private releaseHold: () => void = () => undefined;
  partStarted = 0;

  holdPartNumber(partNumber: number): void {
    this.holdPart = partNumber;
    this.hold = new Promise((resolve) => {
      this.releaseHold = resolve;
    });
  }

  release(): void {
    this.releaseHold();
  }

  async create(key: string): Promise<string> {
    return `upload:${key}`;
  }

  async uploadPart(uploadId: string, partNumber: number, body: Uint8Array): Promise<string> {
    this.partStarted = partNumber;
    if (this.failPartTimes > 0) {
      this.failPartTimes -= 1;
      throw new Error(`injected part failure ${partNumber}`);
    }
    if (this.holdPart === partNumber) {
      this.holdPart = null;
      await this.hold;
    }
    this.bodies.push(body);
    return `etag-${uploadId}-${partNumber}`;
  }

  async complete(uploadId: string, parts: R2UploadedPart[]): Promise<void> {
    if (this.aborted.includes(uploadId)) throw new Error("completed an aborted upload");
    this.completed.push(parts);
  }

  async abort(uploadId: string): Promise<void> {
    this.aborted.push(uploadId);
  }
}

async function readAt(file: string, offset: number, length: number): Promise<Uint8Array> {
  const fh = await fs.promises.open(file, "r");
  try {
    const out = new Uint8Array(length);
    await fh.read(out, 0, length, offset);
    return out;
  } finally {
    await fh.close();
  }
}

async function writeInChunks(sink: ParquetUploadSink, bytes: Uint8Array): Promise<void> {
  const chunk = 1024 * 1024;
  for (let offset = 0; offset < bytes.byteLength; offset += chunk) {
    await sink.writePort(0, bytes.subarray(offset, Math.min(bytes.byteLength, offset + chunk)));
  }
}

describe("large parquet export", () => {
  it("writes a multi-part object, verifies it, and retries after an upload failure", { timeout: 600_000 }, async () => {
    const file = path.join(os.tmpdir(), `wcol-large-src-${process.pid}.parquet`);
    const out = path.join(os.tmpdir(), `wcol-large-out-${process.pid}.parquet`);
    const duck = await (await DuckDBInstance.create(":memory:")).connect();
    await duck.run(`
      COPY (
        SELECT i::BIGINT AS k,
               md5(i::VARCHAR) || md5((i + 1000003)::VARCHAR) || md5((i + 2000003)::VARCHAR) AS payload
        FROM range(${ROWS}) t(i)
      ) TO '${file.replaceAll("'", "''")}' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 8192)
    `);
    const node = await loadNodeWasm();
    const coordinator = await loadCoordinatorWasm();
    const size = fs.statSync(file).size;
    const footer = await readParquetFooter((offset, length) => readAt(file, offset, length), size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/t.parquet" } },
        sql: "SELECT k, payload FROM t",
        output: {
          format: "parquet",
          bucket: "output",
          prefix: "out/",
          targetRowGroupRows: 256,
          targetFileBytes: 1024 * 1024 * 1024,
        },
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: 4096,
      minBytesPerNode: 1,
    });
    if (planned.kind !== "parquet" || planned.graph.kind !== "graph") {
      throw new Error(`export did not plan parquet (${planned.kind})`);
    }
    try {
      const started = performance.now();
      const bytes = await runGatherGraph(
        { node, coordinator },
        planned.graph,
        (offset, length) => readAt(file, offset, length),
      );
      await fs.promises.writeFile(out, bytes);
      const check = await (await DuckDBInstance.create(":memory:")).connect();
      const reader = await check.runAndReadAll(
        `SELECT COUNT(*)::BIGINT AS n, SUM(k)::BIGINT AS total FROM read_parquet('${out.replaceAll("'", "''")}')`,
      );
      const gotRows = Number(reader.getRows()[0]?.[0] ?? 0);
      const gotTotal = Number(reader.getRows()[0]?.[1] ?? 0);
      const expectedTotal = (ROWS * (ROWS - 1)) / 2;
      console.log(JSON.stringify({
        outputBytes: bytes.byteLength,
        outputMiB: Number((bytes.byteLength / (1024 * 1024)).toFixed(1)),
        partsAt8MiB: Math.ceil(bytes.byteLength / R2_PART_SIZE),
        rows: gotRows,
        wallMs: Math.round(performance.now() - started),
      }));
      assert.equal(gotRows, ROWS);
      assert.equal(gotTotal, expectedTotal);
      assert.ok(bytes.byteLength >= 192 * 1024 * 1024, `output ${bytes.byteLength} is under 192 MiB`);

      const held = new RecordingR2();
      held.holdPartNumber(2);
      const heldSink = new ParquetUploadSink(held, "out/large.parquet");
      let writeSettled = false;
      const writing = writeInChunks(heldSink, bytes).then(() => {
        writeSettled = true;
      });
      const startedWait = Date.now();
      while (held.partStarted < 2 && Date.now() - startedWait < 30_000) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.equal(held.partStarted, 2);
      assert.equal(writeSettled, false);
      assert.equal(held.bodies.length, 1);
      held.release();
      await writing;
      const receipt = await finishParquetShard(heldSink, "out/large.parquet", 1);
      assert.equal(receipt.bytes, bytes.byteLength);
      assert.ok(receipt.parts >= 24, `receipt parts ${receipt.parts}`);
      assert.ok(held.completed.length === 1);
      assert.ok(held.bodies.length >= 3);
      const uploaded = held.bodies.reduce((n, part) => n + part.byteLength, 0);
      assert.equal(uploaded, bytes.byteLength);

      const failed = new RecordingR2();
      failed.failPartTimes = 5;
      const failedSink = new ParquetUploadSink(failed, "out/large.parquet");
      await assert.rejects(writeInChunks(failedSink, bytes), /injected part failure/);
      assert.deepEqual(failed.aborted, ["upload:out/large.parquet"]);
      assert.equal(failed.completed.length, 0);
      await assert.rejects(finishParquetShard(failedSink, "out/large.parquet", 1));

      const retried = new RecordingR2();
      const retriedSink = new ParquetUploadSink(retried, "out/large.parquet");
      await writeInChunks(retriedSink, bytes);
      const retriedReceipt = await finishParquetShard(retriedSink, "out/large.parquet", 1);
      assert.equal(retried.aborted.length, 0);
      assert.equal(retriedReceipt.bytes, bytes.byteLength);
      assert.equal(retriedReceipt.parts, receipt.parts);
      assert.equal(retried.completed.length, 1);
      assert.ok((retried.completed[0]?.length ?? 0) >= 3);
    } finally {
      await fs.promises.unlink(file).catch(() => undefined);
      await fs.promises.unlink(out).catch(() => undefined);
    }
  });
});