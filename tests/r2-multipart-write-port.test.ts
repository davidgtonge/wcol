import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  R2_PART_SIZE,
  R2MultipartWritePort,
  type R2MultipartSession,
  type R2UploadedPart,
} from "../src/runtime/host/ports/r2-write.ts";

class FakeR2 implements R2MultipartSession {
  readonly created: string[] = [];
  readonly uploaded: Array<{ uploadId: string; partNumber: number; bytes: number }> = [];
  readonly completed: R2UploadedPart[][] = [];
  readonly aborted: string[] = [];
  failPartTimes = 0;
  activeUploads = 0;
  maxActiveUploads = 0;
  releasePart: (() => void) | null = null;
  private partGate: Promise<void> | null = null;

  holdNextPart(): void {
    this.partGate = new Promise((resolve) => {
      this.releasePart = resolve;
    });
  }

  async create(key: string): Promise<string> {
    this.created.push(key);
    return "upload-1";
  }

  async uploadPart(uploadId: string, partNumber: number, body: Uint8Array): Promise<string> {
    this.activeUploads += 1;
    this.maxActiveUploads = Math.max(this.maxActiveUploads, this.activeUploads);
    try {
      if (this.partGate) {
        const gate = this.partGate;
        this.partGate = null;
        await gate;
      }
      if (this.failPartTimes > 0) {
        this.failPartTimes -= 1;
        throw new Error("injected part failure");
      }
      this.uploaded.push({ uploadId, partNumber, bytes: body.byteLength });
      return `etag-${partNumber}`;
    } finally {
      this.activeUploads -= 1;
    }
  }

  async complete(_uploadId: string, parts: R2UploadedPart[]): Promise<void> {
    this.completed.push(parts);
  }

  async abort(uploadId: string): Promise<void> {
    this.aborted.push(uploadId);
  }
}

describe("R2 multipart write port", () => {
  it("splits an 8 MiB boundary and publishes the short tail only on finish", async () => {
    const r2 = new FakeR2();
    const port = new R2MultipartWritePort(r2, "hits-advengine-zero.parquet");
    await port.write(new Uint8Array(R2_PART_SIZE + 3));
    assert.equal(r2.uploaded.length, 1);
    assert.equal(r2.uploaded[0]?.bytes, R2_PART_SIZE);
    assert.equal(r2.completed.length, 0);
    await port.finish();
    assert.equal(r2.uploaded[1]?.bytes, 3);
    assert.deepEqual(
      r2.completed[0]?.map((part) => part.partNumber),
      [1, 2],
    );
    assert.equal(port.snapshot().peakAccountedBytes, R2_PART_SIZE);
  });

  it("keeps one part upload in flight and blocks the caller until it finishes", async () => {
    const r2 = new FakeR2();
    r2.holdNextPart();
    const port = new R2MultipartWritePort(r2, "object.parquet");
    let settled = false;
    const pending = port.write(new Uint8Array(R2_PART_SIZE)).then(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(settled, false);
    assert.equal(r2.maxActiveUploads, 1);
    r2.releasePart?.();
    await pending;
    assert.equal(settled, true);
    assert.equal(r2.maxActiveUploads, 1);
  });

  it("retries a failed part without asking for the bytes again", async () => {
    const r2 = new FakeR2();
    r2.failPartTimes = 1;
    const port = new R2MultipartWritePort(r2, "object.parquet");
    await port.write(new Uint8Array(R2_PART_SIZE));
    await port.finish();
    assert.equal(r2.uploaded.length, 1);
    assert.equal(r2.aborted.length, 0);
    assert.equal(r2.completed.length, 1);
  });

  it("aborts the upload when a part keeps failing and does not complete", async () => {
    const r2 = new FakeR2();
    r2.failPartTimes = 5;
    const port = new R2MultipartWritePort(r2, "object.parquet", 2);
    await assert.rejects(port.write(new Uint8Array(R2_PART_SIZE)));
    assert.equal(r2.completed.length, 0);
    assert.deepEqual(r2.aborted, ["upload-1"]);
  });
});
