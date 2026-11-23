import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { finishParquetShard, ParquetUploadSink } from "../src/runtime/host/parquet-upload-sink.ts";
import type { R2MultipartSession, R2UploadedPart } from "../src/runtime/host/ports/r2-write.ts";

class RecordingR2 implements R2MultipartSession {
  readonly events: string[] = [];
  readonly completed: R2UploadedPart[][] = [];
  readonly aborted: string[] = [];
  failComplete = false;

  async create(key: string): Promise<string> {
    this.events.push(`create:${key}`);
    return "upload-1";
  }

  async uploadPart(uploadId: string, partNumber: number, body: Uint8Array): Promise<string> {
    this.events.push(`part:${uploadId}:${partNumber}:${body.byteLength}`);
    return `etag-${partNumber}`;
  }

  async complete(uploadId: string, parts: R2UploadedPart[]): Promise<void> {
    this.events.push(`complete:${uploadId}`);
    if (this.failComplete) throw new Error("complete failed");
    this.completed.push(parts);
  }

  async abort(uploadId: string): Promise<void> {
    this.events.push(`abort:${uploadId}`);
    this.aborted.push(uploadId);
  }
}

describe("parquet shard receipts", () => {
  it("returns the receipt only after the multipart upload completes", async () => {
    const r2 = new RecordingR2();
    const sink = new ParquetUploadSink(r2, "jobs/7/shard.parquet");
    await sink.writePort(0, new Uint8Array([1, 2, 3, 4]));
    assert.equal(r2.completed.length, 0);
    const part = await finishParquetShard(sink, "jobs/7/shard.parquet", 7);
    assert.equal(r2.events.at(-1), "complete:upload-1");
    assert.equal(r2.completed.length, 1);
    assert.deepEqual(part, {
      objectKey: "jobs/7/shard.parquet",
      bytes: 4,
      taskId: 7,
      parts: 1,
      partitionValues: [],
    });
  });

  it("produces no receipt when the upload fails to complete", async () => {
    const r2 = new RecordingR2();
    r2.failComplete = true;
    const sink = new ParquetUploadSink(r2, "jobs/7/shard.parquet");
    await sink.writePort(0, new Uint8Array([9]));
    let receipt: unknown;
    await assert.rejects(
      finishParquetShard(sink, "jobs/7/shard.parquet", 7).then((part) => {
        receipt = part;
      }),
      /complete failed/,
    );
    assert.equal(receipt, undefined);
    assert.equal(r2.completed.length, 0);
    assert.deepEqual(r2.aborted, ["upload-1"]);
  });
});
