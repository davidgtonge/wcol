import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { objectsToSweep, sweepJob } from "../cloudflare/src/coordinator/sweep.ts";
import type { R2OutputBucket } from "../cloudflare/src/r2.ts";

const task = "00000000000000ab";
const keys = [
  `_jobs/j/${task}/1/part.parquet`,
  `_jobs/j/${task}/2/part.parquet`,
  `_jobs/j/${task}/2/day=3/part-0.parquet`,
  `_jobs/j/finalizer/1/result.parquet`,
  `_jobs/j/a1/t4/wcol-spill/run-0`,
  `_jobs/j/a2/t4-progress.json`,
  `_jobs/j/timing.json`,
  `_jobs/j/manifest.json`,
  `_jobs/j/result.json`,
  `_jobs/j/graph-abc.bin`,
  `_jobs/jj/${task}/1/part.parquet`,
];

describe("sweeping a finished job", () => {
  it("deletes attempt outputs the result does not name, and spill runs", () => {
    assert.deepEqual(objectsToSweep("j", keys, new Set([`_jobs/j/${task}/2/day=3/part-0.parquet`])), [
      `_jobs/j/${task}/1/part.parquet`,
      `_jobs/j/${task}/2/part.parquet`,
      `_jobs/j/finalizer/1/result.parquet`,
      `_jobs/j/a1/t4/wcol-spill/run-0`,
    ]);
  });

  it("keeps the job's records and other jobs' objects", () => {
    const swept = new Set(objectsToSweep("j", keys, new Set()));
    for (const kept of ["timing.json", "manifest.json", "result.json", "graph-abc.bin", "a2/t4-progress.json"]) {
      assert.ok(!swept.has(`_jobs/j/${kept}`), kept);
    }
    assert.ok(!swept.has(`_jobs/jj/${task}/1/part.parquet`));
  });

  it("lists every page and deletes in batches", async () => {
    const stored = new Set(Array.from({ length: 2500 }, (_, i) => `_jobs/j/${task}/1/p${i}.parquet`));
    stored.add("_jobs/j/timing.json");
    const deletes: number[] = [];
    const bucket = {
      async list({ prefix, cursor }: { prefix?: string; cursor?: string }) {
        const all = [...stored].filter((key) => key.startsWith(prefix ?? "")).sort();
        const from = cursor ? Number(cursor) : 0;
        const page = all.slice(from, from + 1000);
        const more = from + 1000 < all.length;
        return { objects: page.map((key) => ({ key })), truncated: more, cursor: more ? String(from + 1000) : undefined };
      },
      async delete(keys: string | string[]) {
        const batch = Array.isArray(keys) ? keys : [keys];
        deletes.push(batch.length);
        for (const key of batch) stored.delete(key);
      },
    } as unknown as R2OutputBucket;
    assert.equal(await sweepJob(bucket, "j", []), 2500);
    assert.deepEqual(deletes, [1000, 1000, 500]);
    assert.deepEqual([...stored], ["_jobs/j/timing.json"]);
  });
});
