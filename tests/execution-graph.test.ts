import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  COORDINATOR_SLOT,
  decodeExecutionPlanWire,
  WHOLE_STREAM,
  WIRE_TAG_EXECUTION_GRAPH_V1,
  WIRE_TAG_EXECUTION_GRAPH_V2,
  WIRE_TAG_METADATA_PARTIAL,
} from "../src/runtime/coordinator/execution-graph.ts";

describe("execution graph wire", () => {
  it("decodes metadata partial tag 0", () => {
    const partial = new Uint8Array([1, 2, 3]);
    const wire = new Uint8Array(5 + partial.byteLength);
    wire[0] = WIRE_TAG_METADATA_PARTIAL;
    new DataView(wire.buffer).setUint32(1, partial.byteLength, true);
    wire.set(partial, 5);
    const plan = decodeExecutionPlanWire(wire);
    assert.ok(plan.kind === "metadata_partial", plan.kind);
    assert.deepEqual(plan.partial, partial);
  });

  it("decodes minimal graph tag 4", () => {
    const program = new Uint8Array([0xff, 1, 0, 0, 0, 14, 1, 0, 0, 0]);
    const parts: number[] = [WIRE_TAG_EXECUTION_GRAPH_V1];
    const pushU32 = (n: number) => {
      parts.push(n & 255, (n >> 8) & 255, (n >> 16) & 255, (n >> 24) & 255);
    };
    pushU32(1);
    pushU32(0);
    pushU32(COORDINATOR_SLOT);
    pushU32(0);
    const view = new ArrayBuffer(8);
    new DataView(view).setBigUint64(0, BigInt(0), true);
    parts.push(...new Uint8Array(view));
    pushU32(program.byteLength);
    parts.push(...program);
    pushU32(0xffff_ffff); // no run-time work
    pushU32(0);
    pushU32(0);
    parts.push(0);
    const wire = new Uint8Array(parts);
    const plan = decodeExecutionPlanWire(wire);
    assert.ok(plan.kind === "graph", plan.kind);
    assert.equal(plan.rootTaskId, 0);
    assert.equal(plan.tasks[0]?.slot, COORDINATOR_SLOT);
    assert.equal(plan.edges.length, 0);
  });

  it("decodes a partition id on tag 5 and leaves whole-stream and tag 4 edges unpartitioned", () => {
    const wire = graphWire(WIRE_TAG_EXECUTION_GRAPH_V2, 3);
    const plan = decodeExecutionPlanWire(wire);
    assert.ok(plan.kind === "graph", plan.kind);
    assert.equal(plan.edges[0]?.partition, 3);
    const whole = graphWire(WIRE_TAG_EXECUTION_GRAPH_V2, WHOLE_STREAM);
    const decoded = decodeExecutionPlanWire(whole);
    assert.ok(decoded.kind === "graph", decoded.kind);
    assert.equal(decoded.edges[0]?.partition, undefined);
    const v1 = decodeExecutionPlanWire(graphWire(WIRE_TAG_EXECUTION_GRAPH_V1, 3));
    assert.ok(v1.kind === "graph", v1.kind);
    assert.equal(v1.edges.length, 1);
    assert.equal(v1.edges[0]?.partition, undefined);
  });
});

function graphWire(tag: number, partition: number): Uint8Array {
  const parts: number[] = [tag];
  const pushU32 = (n: number) => {
    parts.push(n & 255, (n >> 8) & 255, (n >> 16) & 255, (n >> 24) & 255);
  };
  pushU32(1);
  pushU32(0);
  pushU32(COORDINATOR_SLOT);
  pushU32(0);
  parts.push(...new Uint8Array(8));
  pushU32(1);
  parts.push(0);
  pushU32(0xffff_ffff); // no run-time work
  pushU32(1);
  pushU32(0);
  pushU32(0);
  parts.push(1);
  if (tag === WIRE_TAG_EXECUTION_GRAPH_V2) pushU32(partition);
  pushU32(0);
  parts.push(1);
  return new Uint8Array(parts);
}

describe("execution graph wire errors", () => {
  it("rejects an unknown root output byte instead of defaulting it", () => {
    const wire = graphWire(WIRE_TAG_EXECUTION_GRAPH_V2, 3);
    wire[wire.length - 1] = 9;
    assert.throws(() => decodeExecutionPlanWire(wire), /unknown root output 9/);
  });
});
