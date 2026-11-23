import assert from "node:assert/strict";
import { test } from "node:test";
import { EDGE_READ_AHEAD, INPUT_RINGS, OUTPUT_SLOTS, portBytes, portKiB } from "../src/runtime/host/port-budget.ts";

test("ports keep their sizes up to 64 and split a fixed total beyond", () => {
  assert.equal(portKiB(OUTPUT_SLOTS, 64), 128);
  assert.equal(portKiB(OUTPUT_SLOTS, 128), 64);
  assert.equal(portKiB(INPUT_RINGS, 32), 64);
  assert.equal(portKiB(INPUT_RINGS, 128), 32);
  assert.equal(portBytes(EDGE_READ_AHEAD, 100), 32 * 1024, "rounded down to a power of two");
});

test("one port, or none, gets the largest size; very many get the floor", () => {
  assert.equal(portKiB(OUTPUT_SLOTS, 1), 128);
  assert.equal(portKiB(OUTPUT_SLOTS, 0), 128);
  assert.equal(portKiB(INPUT_RINGS, 10_000), 16);
});
