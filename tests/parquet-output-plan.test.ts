import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  decodeExecutionPlanWire,
  WIRE_TAG_EXECUTION_GRAPH_V3,
} from "../src/runtime/coordinator/execution-graph.ts";

describe("output plan wire v3", () => {
  it("decodes output plan and concat finaliser trailer", () => {
    const parts: number[] = [WIRE_TAG_EXECUTION_GRAPH_V3];
    const pushU32 = (n: number) => {
      parts.push(n & 255, (n >> 8) & 255, (n >> 16) & 255, (n >> 24) & 255);
    };
    pushU32(0);
    pushU32(0);
    pushU32(0);
    parts.push(2);
    parts.push(1);
    parts.push(0);
    parts.push(0);
    const bucket = new TextEncoder().encode("b");
    pushU32(bucket.byteLength);
    parts.push(...bucket);
    const prefix = new TextEncoder().encode("p/");
    pushU32(prefix.byteLength);
    parts.push(...prefix);
    parts.push(0);
    parts.push(1);
    pushU32(2);
    pushU32(0);
    pushU32(1);
    pushU32(0);
    const wire = new Uint8Array(parts);
    const plan = decodeExecutionPlanWire(wire);
    assert.equal(plan.kind, "graph");
    if (plan.kind !== "graph") return;
    assert.equal(plan.output?.layout, "single_file");
    assert.equal(plan.finalizer.kind, "concat_parquet");
    if (plan.finalizer.kind === "concat_parquet") {
      assert.deepEqual(plan.finalizer.partitionOrder, [0, 1]);
    }
  });
});
