import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";
import {
  SPILL_OP_RUN_READ,
  createPoolSpillDriver,
  deleteSpillRuns,
} from "../src/runtime/host/spill-r2-pool-driver.ts";
import type { WcolStreamWasm } from "../src/runtime/host/wasm-stream.ts";

const READ_SLOT = 65536;

function makeWasm(stubs: { op: number; append: Uint8Array }): WcolStreamWasm {
  const memory = new WebAssembly.Memory({ initial: 256 });
  let appendPtr = 0;
  if (stubs.append.byteLength > 0) {
    appendPtr = 1024;
    new Uint8Array(memory.buffer, appendPtr, stubs.append.byteLength).set(stubs.append);
  }
  return {
    memory,
    wcs_alloc: () => 2048,
    wcs_dealloc: () => {},
    wcs_sp_spill_op: () => stubs.op,
    wcs_sp_spill_append_len: () => stubs.append.byteLength,
    wcs_sp_spill_append_ptr: () => appendPtr,
    wcs_sp_spill_complete: () => 0,
    wcs_sp_spill_read_slot: () => READ_SLOT,
  } as unknown as WcolStreamWasm;
}

function encodeRunReadOpen(objectKey: string, byteLength: number, offset: number, length: number): Uint8Array {
  const keyBytes = new TextEncoder().encode(objectKey);
  const out = new Uint8Array(4 + keyBytes.length + 20);
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  view.setUint32(0, keyBytes.length, true);
  out.set(keyBytes, 4);
  const base = 4 + keyBytes.length;
  view.setBigUint64(base, BigInt(byteLength), true);
  view.setBigUint64(base + 8, BigInt(offset), true);
  view.setUint32(base + 16, length, true);
  return out;
}

test("RUN_READ uses each request key and range independently", async () => {
  const base = createMemoryR2SpillBucket();
  const runA = new Uint8Array([1, 2, 3]);
  const runB = new Uint8Array([9, 8, 7, 6]);
  base.objects.set("runs/a.bin", runA);
  base.objects.set("runs/b.bin", runB);
  const reads: Array<{ key: string; offset: number; length: number }> = [];
  const bucket = {
    ...base,
    async get(key: string, options?: { range?: { offset?: number; length?: number } }) {
      reads.push({
        key,
        offset: options?.range?.offset ?? 0,
        length: options?.range?.length ?? 0,
      });
      return base.get(key, options);
    },
  };
  const host = { spillR2Bucket: bucket };

  await createPoolSpillDriver(
    makeWasm({
      op: SPILL_OP_RUN_READ,
      append: encodeRunReadOpen("runs/a.bin", runA.byteLength, 0, 3),
    }),
    1,
    host,
  ).advance();
  await createPoolSpillDriver(
    makeWasm({
      op: SPILL_OP_RUN_READ,
      append: encodeRunReadOpen("runs/b.bin", runB.byteLength, 1, 2),
    }),
    2,
    host,
  ).advance();

  // Run a is read whole; run b's last byte is read ahead for the merge's next request.
  assert.deepEqual(reads, [
    { key: "runs/a.bin", offset: 0, length: 3 },
    { key: "runs/b.bin", offset: 1, length: 2 },
    { key: "runs/b.bin", offset: 3, length: 1 },
  ]);
});

test("RUN_READ fails spill complete on short read", async () => {
  const mem = createMemoryR2SpillBucket();
  mem.objects.set("runs/x.bin", new Uint8Array([1, 2]));
  const bucket = {
    ...mem,
    async get(key: string, options?: { range?: { offset?: number; length?: number } }) {
      const got = await mem.get(key, options);
      if (!got) return got;
      return {
        ...got,
        arrayBuffer: async () => {
          const whole = await got.arrayBuffer();
          return whole.slice(0, 1);
        },
      };
    },
  };
  const host = { spillR2Bucket: bucket };
  let completeOk = 1;
  const wasm = makeWasm({
    op: SPILL_OP_RUN_READ,
    append: encodeRunReadOpen("runs/x.bin", 2, 0, 2),
  });
  wasm.wcs_sp_spill_complete = (_handle, ok) => {
    completeOk = ok;
    return 0;
  };
  await assert.rejects(() => createPoolSpillDriver(wasm, 1, host).advance(), /Short spill read/);
  assert.equal(completeOk, 0);
});

test("RUN_READ scopes the Rust run name by the host spill key prefix", async () => {
  // Rust names runs per partition only (`wcol-spill/{partition+1}/run-N`), so two
  // attempts of the same reducer share a run name. The prefix keeps them apart.
  const bucket = createMemoryR2SpillBucket();
  const runKey = "wcol-spill/0000000000000001/run-000001.bin";
  bucket.objects.set(`_jobs/j/a1/t10/${runKey}`, new Uint8Array([1, 1]));
  bucket.objects.set(`_jobs/j/a2/t10/${runKey}`, new Uint8Array([2, 2]));
  const scans: number[][] = [];
  const readFor = async (prefix: string) => {
    const wasm = makeWasm({ op: SPILL_OP_RUN_READ, append: encodeRunReadOpen(runKey, 2, 0, 2) });
    await createPoolSpillDriver(wasm, 1, { spillR2Bucket: bucket, spillKeyPrefix: prefix }).advance();
    // Reads land in the pool's read slot, not the completion payload.
    scans.push([...new Uint8Array(wasm.memory.buffer, READ_SLOT, 2)]);
  };
  await readFor("_jobs/j/a1/t10/");
  await readFor("_jobs/j/a2/t10/");
  assert.deepEqual(scans, [[1, 1], [2, 2]]);
});

test("deleteSpillRuns removes only the task's runs, across list pages", async () => {
  const keys = new Set([
    "_jobs/j/a1/t10/wcol-spill/0000000000000001/run-000001.bin",
    "_jobs/j/a1/t10/wcol-spill/0000000000000001/run-000002.bin",
    "_jobs/j/a1/t10/wcol-spill/0000000000000001/run-000003.bin",
    "_jobs/j/a1/t10-progress.json",
    "_jobs/j/a1/t11/wcol-spill/0000000000000002/run-000001.bin",
  ]);
  const bucket = {
    async list({ prefix = "", cursor }: { prefix?: string; cursor?: string }) {
      const all = [...keys].filter((key) => key.startsWith(prefix)).sort();
      const start = cursor ? Number(cursor) : 0;
      const page = all.slice(start, start + 2);
      const next = start + 2 < all.length ? String(start + 2) : undefined;
      return { objects: page.map((key) => ({ key })), truncated: next !== undefined, cursor: next };
    },
    async delete(batch: string | string[]) {
      for (const key of [batch].flat()) keys.delete(key);
    },
  };
  const deleted = await deleteSpillRuns(bucket as never, "_jobs/j/a1/t10/");
  assert.equal(deleted, 3);
  assert.deepEqual([...keys].sort(), [
    "_jobs/j/a1/t10-progress.json",
    "_jobs/j/a1/t11/wcol-spill/0000000000000002/run-000001.bin",
  ]);
  assert.equal(await deleteSpillRuns(bucket as never, ""), 0, "no prefix, no sweep");
});
