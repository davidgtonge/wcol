import assert from "node:assert/strict";
import { test } from "node:test";
import { createRunPrefetch } from "../src/runtime/host/spill-r2-pool-driver.ts";
import { createHostBytes } from "../src/runtime/host/host-bytes.ts";

/** A run of `size` bytes, value = offset mod 251; records every range read. */
function fakeRuns(size: number) {
  const calls: string[] = [];
  const read = async (key: string, offset: number, length: number) => {
    calls.push(`${key}@${offset}+${length}`);
    return Uint8Array.from({ length }, (_, i) => (offset + i) % 251);
  };
  return { calls, read, size };
}

test("each run's next chunk is read ahead and served from it", async () => {
  const runs = fakeRuns(25);
  const prefetch = createRunPrefetch(runs.read);
  const first = await prefetch.read("a", 0, 10, runs.size);
  assert.equal(first.prefetched, false);
  const second = await prefetch.read("a", 10, 10, runs.size);
  assert.equal(second.prefetched, true);
  assert.deepEqual([...second.bytes], Array.from({ length: 10 }, (_, i) => (10 + i) % 251));
  // The last chunk is shorter, and nothing is read past the end of the run.
  const last = await prefetch.read("a", 20, 5, runs.size);
  assert.equal(last.prefetched, true);
  assert.deepEqual(runs.calls, ["a@0+10", "a@10+10", "a@20+5"]);
});

test("a request that misses the chunk ahead reads directly", async () => {
  const runs = fakeRuns(100);
  const prefetch = createRunPrefetch(runs.read);
  await prefetch.read("a", 0, 10, runs.size);
  await prefetch.read("b", 0, 10, runs.size);
  // The chunk size grew once other runs finished: the chunk ahead no longer matches.
  const grown = await prefetch.read("a", 10, 20, runs.size);
  assert.equal(grown.prefetched, false);
  assert.equal(grown.bytes.byteLength, 20);
  const runB = await prefetch.read("b", 10, 10, runs.size);
  assert.equal(runB.prefetched, true);
});

test("a failed read-ahead is retried as a direct read", async () => {
  let failNext = false;
  const calls: number[] = [];
  const prefetch = createRunPrefetch(async (_key, offset, length) => {
    calls.push(offset);
    if (failNext) {
      failNext = false;
      throw new Error("r2 hiccup");
    }
    return new Uint8Array(length).fill(offset);
  });
  await prefetch.read("a", 0, 4, 12);
  failNext = true;
  // The read-ahead of offset 4 started above already ran; fail the next one instead.
  const second = await prefetch.read("a", 4, 4, 12);
  assert.equal(second.prefetched, true);
  const third = await prefetch.read("a", 8, 4, 12);
  assert.equal(third.prefetched, false);
  assert.deepEqual([...third.bytes], [8, 8, 8, 8]);
  assert.deepEqual(calls, [0, 4, 8, 8]);
});

test("bytes read ahead count as host bytes until they leave the read-ahead, used or not", async () => {
  const runs = fakeRuns(100);
  const hostBytes = createHostBytes();
  const prefetch = createRunPrefetch(runs.read, hostBytes);
  await prefetch.read("a", 0, 10, runs.size);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(hostBytes.snapshot().current.spillPrefetch, 10);
  // A request that misses the chunk ahead still drops it from the count.
  await prefetch.read("a", 10, 20, runs.size);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(hostBytes.snapshot().current.spillPrefetch, 20, "only the new chunk ahead is held");
});
