import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";
import { createRunUploads } from "../src/runtime/host/spill-r2-pool-driver.ts";
import { createHostBytes } from "../src/runtime/host/host-bytes.ts";
import type { RunPutPart } from "../src/runtime/host/ports/r2-spill-put.ts";

/** A memory bucket whose puts and part uploads wait until `release()`; `fail` rejects the next one. */
function gatedBucket() {
  const base = createMemoryR2SpillBucket();
  let gates: Array<() => void> = [];
  let failNext = false;
  const started: string[] = [];
  const gate = async (label: string) => {
    started.push(label);
    await new Promise<void>((resolve) => gates.push(resolve));
    if (failNext) {
      failNext = false;
      throw new Error(`r2 refused ${label}`);
    }
  };
  const bucket = {
    ...base,
    async put(...args: Parameters<typeof base.put>) {
      await gate(`put ${args[0]}`);
      return base.put(...args);
    },
    async createMultipartUpload(...args: Parameters<typeof base.createMultipartUpload>) {
      const upload = await base.createMultipartUpload(...args);
      return {
        ...upload,
        async uploadPart(part: number, body: Uint8Array) {
          await gate(`part ${part}`);
          return upload.uploadPart(part, body);
        },
        complete: upload.complete.bind(upload),
        async abort() {
          started.push(`abort ${args[0]}`);
          return upload.abort();
        },
      };
    },
  };
  return {
    base,
    bucket,
    started,
    release() {
      const open = gates;
      gates = [];
      for (const resolve of open) resolve();
    },
    fail() {
      failNext = true;
    },
  };
}

const part = (objectKey: string, n: number, last: boolean, byteLength: number): RunPutPart => ({
  objectKey,
  part: n,
  last,
  byteLength,
  recordCount: 1,
});

const settled = (promise: Promise<unknown>) =>
  Promise.race([promise.then(() => true), new Promise((resolve) => setTimeout(() => resolve(false), 10))]);

test("parts are accepted while earlier ones upload, up to the slot limit", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => key, 2);
  // R2 needs every part but the last to be at least 5 MiB.
  const full = 5 * 1024 * 1024;
  await uploads.accept(part("run", 1, false, full), () => new Uint8Array(full).fill(1));
  await uploads.accept(part("run", 2, false, full), () => new Uint8Array(full).fill(2));
  // Both slots are busy: the third part waits, and is not even copied yet.
  let copied = false;
  const third = uploads.accept(part("run", 3, true, 1), () => {
    copied = true;
    return Uint8Array.of(3);
  });
  assert.equal(await settled(third), false);
  assert.equal(copied, false);
  r2.release();
  await third;
  const uploaded = uploads.uploaded("run");
  assert.equal(await settled(uploaded), false, "the run is in R2 only after its last part");
  r2.release();
  await uploaded;
  const run = r2.base.objects.get("run") ?? new Uint8Array();
  assert.equal(run.byteLength, 2 * full + 1);
  assert.deepEqual([run[0], run[full], run[2 * full]], [1, 2, 3]);
});

test("a one-part run is a plain put, readable once uploaded", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => `spill/${key}`, 2);
  await uploads.accept(part("solo", 1, true, 3), () => Uint8Array.of(7, 8, 9));
  assert.deepEqual(r2.started, ["put spill/solo"]);
  const uploaded = uploads.uploaded("solo");
  assert.equal(await settled(uploaded), false);
  r2.release();
  await uploaded;
  assert.deepEqual([...(r2.base.objects.get("spill/solo") ?? [])], [7, 8, 9]);
  // A run this driver never wrote reads straight away.
  await uploads.uploaded("elsewhere");
});

test("a failed upload fails the run's read and every later put or commit", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => key, 2);
  await uploads.accept(part("bad", 1, true, 1), () => Uint8Array.of(1));
  r2.fail();
  r2.release();
  await assert.rejects(uploads.uploaded("bad"), /r2 refused put bad/);
  assert.throws(() => uploads.throwIfFailed(), /r2 refused put bad/);
  await assert.rejects(
    uploads.accept(part("next", 1, true, 1), () => Uint8Array.of(2)),
    /r2 refused put bad/,
  );
});

test("parts out of order are refused", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => key, 2);
  await assert.rejects(uploads.accept(part("run", 2, true, 1), () => Uint8Array.of(1)), /out of order/);
});

test("settle waits for parts still uploading, then aborts an unfinished run", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => key, 2);
  const full = 5 * 1024 * 1024;
  await uploads.accept(part("cut", 1, false, full), () => new Uint8Array(full));
  // The task fails here: its run never gets a last part.
  const settling = uploads.settle();
  assert.equal(await settled(settling), false, "settle returned while a part was uploading");
  r2.release();
  await settling;
  assert.deepEqual(r2.started, ["part 1", "abort cut"]);
});

test("settle is a no-op once every run is complete", async () => {
  const r2 = gatedBucket();
  const uploads = createRunUploads(r2.bucket, (key) => key, 2);
  await uploads.accept(part("done", 1, true, 1), () => Uint8Array.of(1));
  r2.release();
  await uploads.uploaded("done");
  await uploads.settle();
  assert.deepEqual(r2.started, ["put done"]);
});

test("a part's copy counts as host bytes until its upload settles", async () => {
  const r2 = gatedBucket();
  const hostBytes = createHostBytes();
  const uploads = createRunUploads(r2.bucket, (key) => key, 1, hostBytes);
  await uploads.accept(part("run", 1, true, 1000), () => new Uint8Array(1000));
  assert.equal(hostBytes.snapshot().current.spillUploads, 1000);
  r2.release();
  await uploads.uploaded("run");
  assert.equal(hostBytes.snapshot().current.spillUploads, 0);
  assert.equal(hostBytes.snapshot().peak.spillUploads, 1000);
});
