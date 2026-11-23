import assert from "node:assert/strict";
import { test } from "node:test";
import { createHostBytes } from "../src/runtime/host/host-bytes.ts";
import { createIsolatePeak, isolateLoad, joinIsolate } from "../cloudflare/src/node/isolate-load.ts";

test("the host-byte ledger keeps per-owner and total peaks", () => {
  const bytes = createHostBytes();
  bytes.add("edges", 100);
  bytes.add("spillUploads", 50);
  bytes.add("edges", -100);
  bytes.add("spillPrefetch", 70);
  const snap = bytes.snapshot();
  assert.deepEqual(snap.current, { edges: 0, spillUploads: 50, spillPrefetch: 70 });
  assert.deepEqual(snap.peak, { edges: 100, spillUploads: 50, spillPrefetch: 70 });
  assert.equal(snap.peakTotal, 150, "the sum peaked below the sum of the owners' peaks");
  assert.equal(bytes.total(), 120);
});

test("tasks sharing an isolate are counted and summed until they leave", () => {
  const leaveReducer = joinIsolate("reducer", 40, () => ({ wasmBytes: 30, hostBytes: 5 }));
  const peak = createIsolatePeak();
  const leaveScan = joinIsolate("scan", 3, () => ({ wasmBytes: 70, hostBytes: 2 }));
  assert.deepEqual(isolateLoad(), { tasks: 2, reducers: 1, scans: 1, wasmBytes: 100, hostBytes: 7 });
  peak.observe();
  leaveScan();
  peak.observe();
  assert.equal(isolateLoad().tasks, 1);
  assert.equal(peak.peak().tasks, 2);
  assert.equal(peak.peak().bytes, 107);
  leaveReducer();
  assert.equal(isolateLoad().tasks, 0);
});
