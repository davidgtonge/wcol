import type { HostBytes } from "./host-bytes.ts";
import type { PoolWasm } from "./wasm-stream.ts";
import type { WcolHost } from "./host-types.ts";
import type { R2MultipartUpload, R2OutputBucket, R2UploadedPart } from "./ports/r2-bucket.ts";
import { decodeRunPutOpen, decodeRunReadOpen, type RunPutPart } from "./ports/r2-spill-put.ts";
import { readWasmLastError } from "./wasm.ts";
import { createLimiter } from "../concurrency.ts";

export const SPILL_OP_RUN_PUT = 5;
export const SPILL_OP_RUN_COMMIT = 6;
export const SPILL_OP_RUN_READ = 7;

export type PoolSpillDriver = {
  advance(): Promise<void>;
  /** Wait out every upload and abort an unfinished run, so cleanup sees all its objects. */
  settle(): Promise<void>;
};

function viewAt(memory: WebAssembly.Memory, ptr: number, len: number): Uint8Array {
  return new Uint8Array(memory.buffer, ptr, len);
}

function readSpillAppend(wasm: PoolWasm, handle: number): Uint8Array {
  const len = wasm.wcs_sp_spill_append_len(handle);
  if (len <= 0) return new Uint8Array(0);
  return viewAt(wasm.memory, wasm.wcs_sp_spill_append_ptr(handle), len).slice();
}

function completeSpill(wasm: PoolWasm, handle: number, ok: boolean): void {
  const rc = wasm.wcs_sp_spill_complete(handle, ok ? 1 : 0);
  if (rc !== 0) throw new Error("wcs_sp_spill_complete failed");
}

/** Spill run key in the bucket: the Rust run name under the task's prefix. */
export function spillObjectKey(prefix: string | undefined, runKey: string): string {
  return prefix ? `${prefix}${runKey}` : runKey;
}

/**
 * Delete every spill run under a task's prefix. Runs are only scratch for the
 * merge, so they go whether the task succeeded or failed. Requires a prefix:
 * unscoped run names are shared across tasks and must not be swept.
 */
export async function deleteSpillRuns(bucket: R2OutputBucket, prefix: string): Promise<number> {
  if (!prefix || !bucket.list || !bucket.delete) return 0;
  const runPrefix = `${prefix}wcol-spill/`;
  // List everything first so paging never depends on cursor behaviour after deletes.
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await bucket.list({ prefix: runPrefix, cursor });
    for (const object of page.objects) keys.push(object.key);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  const R2_DELETE_BATCH = 1000;
  for (let at = 0; at < keys.length; at += R2_DELETE_BATCH) {
    await bucket.delete(keys.slice(at, at + R2_DELETE_BATCH));
  }
  return keys.length;
}

/**
 * R2 time a reducer spends on spill runs: how many puts and reads, their bytes, their wall
 * time. Read time is what the merge waited; `prefetchHits` reads were already fetched.
 */
export type SpillIo = {
  puts: number;
  putBytes: number;
  putMs: number;
  reads: number;
  readBytes: number;
  readMs: number;
  maxReadMs: number;
  prefetchHits: number;
};

export function emptySpillIo(): SpillIo {
  return {
    puts: 0,
    putBytes: 0,
    putMs: 0,
    reads: 0,
    readBytes: 0,
    readMs: 0,
    maxReadMs: 0,
    prefetchHits: 0,
  };
}

/** Exactly `length` bytes of `key` from `offset`, or an error. */
export type RunRangeRead = (key: string, offset: number, length: number) => Promise<Uint8Array>;

/**
 * Reads each run's next chunk while the merge works through the current one. The merge
 * reads a run front to back in equal chunks, one request at a time, so on Cloudflare it
 * spent almost all its time waiting on R2 (about 70 sequential reads of 60-150 ms each per
 * reducer). After serving `(key, offset, length)` this starts `(key, offset + length)`.
 * At most one chunk per run is ahead, so the extra memory is the merge's read budget.
 * A request that does not match the chunk ahead (the chunk size changes as runs finish)
 * reads directly, and a failed read-ahead is retried as a direct read.
 */
export function createRunPrefetch(read: RunRangeRead, hostBytes?: HostBytes) {
  const ahead = new Map<string, { offset: number; length: number; bytes: Promise<Uint8Array> }>();
  return {
    async read(
      key: string,
      offset: number,
      length: number,
      byteLength: number,
    ): Promise<{ bytes: Uint8Array; prefetched: boolean }> {
      const next = ahead.get(key);
      ahead.delete(key);
      // Held by the read-ahead from landing until it leaves the map, used or not.
      void next?.bytes.then((bytes) => hostBytes?.add("spillPrefetch", -bytes.byteLength), () => undefined);
      let bytes: Uint8Array | undefined;
      if (next && next.offset === offset && next.length === length) {
        bytes = await next.bytes.catch(() => undefined);
      }
      const prefetched = bytes !== undefined;
      bytes ??= await read(key, offset, length);
      const after = offset + length;
      const nextLength = Math.min(length, byteLength - after);
      if (nextLength > 0) {
        const pending = read(key, after, nextLength).then((bytes) => {
          hostBytes?.add("spillPrefetch", bytes.byteLength);
          return bytes;
        });
        // Awaited by the next request for this run, if one matches; never unhandled.
        pending.catch(() => undefined);
        ahead.set(key, { offset: after, length: nextLength, bytes: pending });
      }
      return { bytes, prefetched };
    },
  };
}

/**
 * Parts uploading at once per reducer. Each holds a JS copy of one part (about 5 MiB), and
 * two reducers can share an isolate: at 2, Q33 reducers began exceeding the isolate's
 * memory limit. One still lets Wasm encode the next part while the last uploads.
 */
export const MAX_PART_UPLOADS = 1;

/**
 * Uploads sorted runs in the background. Wasm used to wait for each part to land in R2,
 * which stalled reducer ingest for most of a spill (7-10 s per Q33 reducer). Now a part is
 * copied out and accepted once an upload slot is free, so Wasm waits only when
 * [`MAX_PART_UPLOADS`] parts are already in flight. A one-part run is a plain put; a
 * longer one is a multipart upload whose parts go up in order of acceptance and which completes
 * after its last part.
 *
 * A run counts as committed once its last part is accepted, not once it is in R2:
 * `uploaded` is awaited before the run's first read. A failed upload fails that read, and
 * every later put or commit, with the original error.
 */
export function createRunUploads(
  bucket: R2OutputBucket,
  objectKeyOf: (runKey: string) => string,
  maxParts = MAX_PART_UPLOADS,
  hostBytes?: HostBytes,
) {
  const slot = createLimiter(maxParts);
  const runs = new Map<string, Promise<void>>();
  /** Every upload still running, so a failed task can wait for them before cleanup. */
  const pending = new Set<Promise<unknown>>();
  let open: { runKey: string; upload: Promise<R2MultipartUpload>; parts: Promise<R2UploadedPart>[] } | undefined;
  let failure: { error: unknown } | undefined;

  const options = (recordCount: number) => ({
    httpMetadata: { contentType: "application/octet-stream" },
    customMetadata: { "wcol-spill-run": "1", "wcol-spill-records": String(recordCount) },
  });
  const track = <T>(upload: Promise<T>): Promise<T> => {
    // Awaited later by the run's read; record the first failure now so the next op fails fast.
    upload.catch((error) => {
      failure ??= { error };
    });
    pending.add(upload);
    void upload.finally(() => pending.delete(upload)).catch(() => undefined);
    return upload;
  };
  const throwIfFailed = () => {
    if (failure) throw failure.error;
  };
  /**
   * Resolves once a slot is free and `copy` has run, with the upload still running in that
   * slot. Wrapped in an object: resolving with the promise itself would wait for the upload.
   */
  const inSlot = <T>(copy: () => Uint8Array, upload: (body: Uint8Array) => Promise<T>) =>
    new Promise<{ done: Promise<T> }>((accepted, refused) => {
      const done: Promise<T> = slot(async () => {
        let body: Uint8Array;
        try {
          body = copy();
        } catch (error) {
          refused(error);
          throw error;
        }
        // The limiter may run this before `slot` returns, so hand `done` over a tick later.
        queueMicrotask(() => accepted({ done }));
        hostBytes?.add("spillUploads", body.byteLength);
        try {
          return await upload(body);
        } finally {
          hostBytes?.add("spillUploads", -body.byteLength);
        }
      });
      done.catch(() => undefined);
    });

  return {
    throwIfFailed,

    async accept(plan: RunPutPart, copy: () => Uint8Array): Promise<void> {
      throwIfFailed();
      const key = objectKeyOf(plan.objectKey);
      if (plan.part === 1 && plan.last) {
        const { done } = await inSlot(copy, (body) => bucket.put(key, body, options(plan.recordCount)));
        runs.set(plan.objectKey, track(done.then(() => undefined)));
        return;
      }
      if (plan.part === 1) {
        open = {
          runKey: plan.objectKey,
          upload: track(bucket.createMultipartUpload(key, options(plan.recordCount))),
          parts: [],
        };
      }
      const run = open;
      if (!run || run.runKey !== plan.objectKey || run.parts.length + 1 !== plan.part) {
        throw new Error(`spill run part ${plan.part} of ${key} arrived out of order`);
      }
      const { done } = await inSlot(copy, async (body) => (await run.upload).uploadPart(plan.part, body));
      run.parts.push(track(done));
      if (!plan.last) return;
      open = undefined;
      const complete = (async () => {
        const upload = await run.upload;
        try {
          await upload.complete(await Promise.all(run.parts));
        } catch (error) {
          await upload.abort().catch(() => undefined);
          throw error;
        }
      })();
      runs.set(plan.objectKey, track(complete));
    },

    /**
     * Wait for every upload to land or fail, then abort a run whose last part never came.
     * Without this a failed task's cleanup could list before late parts landed, and leave
     * them, and the open multipart upload, behind.
     */
    async settle(): Promise<void> {
      while (pending.size > 0) await Promise.allSettled([...pending]);
      const unfinished = open;
      open = undefined;
      if (unfinished) await (await unfinished.upload.catch(() => undefined))?.abort().catch(() => undefined);
    },

    /** Waits until the run is in R2; rethrows its upload's failure. */
    async uploaded(runKey: string): Promise<void> {
      await runs.get(runKey);
    },
  };
}

export function createPoolSpillDriver(
  wasm: PoolWasm,
  handle: number,
  host: Pick<WcolHost, "spillR2Bucket" | "spillKeyPrefix">,
  stats?: {
    spillExec: number;
    spillExecByOp: { begin: number; scan: number; end: number; commit: number };
    io: SpillIo;
  },
  hostBytes?: HostBytes,
): PoolSpillDriver {
  if (!host.spillR2Bucket) {
    throw new Error("stream pool spill requires spillR2Bucket on the host");
  }
  const bucket = host.spillR2Bucket;

  const note = (op: "begin" | "scan" | "end" | "commit") => {
    if (!stats) return;
    stats.spillExec += 1;
    stats.spillExecByOp[op] += 1;
  };

  const runReads = createRunPrefetch(async (objectKey, offset, length) => {
    await uploads.uploaded(objectKey);
    const obj = await bucket.get(spillObjectKey(host.spillKeyPrefix, objectKey), {
      range: { offset, length },
    });
    if (!obj?.body) {
      throw new Error(`Missing spill run: ${objectKey}`);
    }
    const bytes = new Uint8Array(await obj.arrayBuffer());
    if (bytes.byteLength !== length) {
      throw new Error(
        `Short spill read: ${objectKey}, offset=${offset}, expected=${length}, actual=${bytes.byteLength}`,
      );
    }
    return bytes;
  }, hostBytes);

  const uploads = createRunUploads(
    bucket,
    (objectKey) => spillObjectKey(host.spillKeyPrefix, objectKey),
    MAX_PART_UPLOADS,
    hostBytes,
  );

  return {
    settle: () => uploads.settle(),
    async advance(): Promise<void> {
      const op = wasm.wcs_sp_spill_op(handle);
      if (op === SPILL_OP_RUN_COMMIT) {
        note("commit");
        // The run's upload may still be running: its first merge read waits for it.
        try {
          uploads.throwIfFailed();
        } catch (error) {
          completeSpill(wasm, handle, false);
          throw error;
        }
        completeSpill(wasm, handle, true);
        return;
      }
      if (op === SPILL_OP_RUN_PUT) {
        const plan = decodeRunPutOpen(readSpillAppend(wasm, handle));
        const wasmPtr = wasm.wcs_sp_spill_run_upload_ptr(handle);
        const wasmLen = wasm.wcs_sp_spill_run_upload_len(handle);
        if (wasmLen !== plan.byteLength) {
          completeSpill(wasm, handle, false);
          throw new Error("sorted run part length mismatch");
        }
        note("begin");
        const putStarted = Date.now();
        try {
          // The copy is taken inside `accept`, once a slot is free and before Wasm resumes.
          await uploads.accept(plan, () => viewAt(wasm.memory, wasmPtr, wasmLen).slice());
          note("end");
          if (stats) {
            stats.io.puts += 1;
            stats.io.putBytes += wasmLen;
            stats.io.putMs += Date.now() - putStarted;
          }
          completeSpill(wasm, handle, true);
        } catch (error) {
          completeSpill(wasm, handle, false);
          throw error;
        }
        return;
      }
      if (op === SPILL_OP_RUN_READ) {
        const { objectKey, offset, length, byteLength } = decodeRunReadOpen(readSpillAppend(wasm, handle));
        const readStarted = Date.now();
        try {
          const { bytes, prefetched } = await runReads.read(objectKey, offset, length, byteLength);
          if (stats) {
            const ms = Date.now() - readStarted;
            stats.io.reads += 1;
            stats.io.readBytes += bytes.byteLength;
            stats.io.readMs += ms;
            stats.io.maxReadMs = Math.max(stats.io.maxReadMs, ms);
            if (prefetched) stats.io.prefetchHits += 1;
          }
          note("scan");
          // Write into the pool's reused read buffer: a fresh Wasm allocation per read
          // fragmented linear memory during the merge. Fill it before any other Wasm call.
          const slot = wasm.wcs_sp_spill_read_slot(handle, bytes.byteLength);
          if (!slot) throw new Error(readWasmLastError(wasm, "spill read slot failed"));
          viewAt(wasm.memory, slot, bytes.byteLength).set(bytes);
          completeSpill(wasm, handle, true);
        } catch (error) {
          completeSpill(wasm, handle, false);
          throw error;
        }
        return;
      }
      completeSpill(wasm, handle, false);
      throw new Error(`unsupported stream pool spill op ${op}`);
    },
  };
}
