import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  hashShardedParquetExport,
  localExportProgram,
  programProducerCount,
  rangeSortParquetExport,
} from "../src/runtime/coordinator/parquet-shard-graph.ts";
import type { GraphPlan } from "../src/runtime/coordinator/job-replay.ts";

describe("parquet shard graph helpers", () => {
  it("detects single-producer finalize programs", () => {
    const program = new Uint8Array([
      0xff, 1, 0, 0, 0, 15, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 4, 0, 0, 0, 0,
    ]);
    assert.equal(programProducerCount(program), 1);
  });

  it("finds the colocated shard export program of a hash reducer", () => {
    const plan: GraphPlan = {
      kind: "graph",
      tasks: [
        { id: 0, slot: 0, program: new Uint8Array([0xff, 0, 0, 0, 1, 16]), requestCount: 0, fetchedBytes: 0 },
        { id: 1, slot: 1, program: new Uint8Array([0xff, 1, 0, 0, 0, 17, 0, 0, 0, 0]), requestCount: 0, fetchedBytes: 0 },
      ],
      edges: [{ from: 0, to: 1, transport: "colocated" }],
      rootTaskId: 1,
      rootOutput: "parquet",
      finalizer: { kind: "none" },
      resultColumns: new Uint8Array(),
    };
    assert.ok(localExportProgram(plan, 0));
    assert.ok(hashShardedParquetExport(plan, [0]));
    // A single-producer finalizer merges its input: it is not a streaming shard export.
    const merging = { ...plan, tasks: [plan.tasks[0]!, { ...plan.tasks[1]!, program: new Uint8Array([0xff, 1, 0, 0, 0, 15, 1, 0, 0, 0]) }] };
    assert.equal(localExportProgram(merging, 0), undefined);
  });

  it("detects range-sort partition writers", () => {
    const worker = new Uint8Array([0xff, 1, 0, 0, 0, 13, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 4, 0, 0, 0, 0]);
    const plan: GraphPlan = {
      kind: "graph",
      tasks: [
        { id: 0, slot: 0, program: new Uint8Array([0xff, 0, 0, 0, 1, 12]), requestCount: 0, fetchedBytes: 0 },
        { id: 1, slot: 1, program: worker, requestCount: 0, fetchedBytes: 0 },
        { id: 2, slot: 2, program: worker, requestCount: 0, fetchedBytes: 0 },
      ],
      edges: [
        { from: 0, to: 1, transport: "do_rpc_wcs", partition: 0 },
        { from: 0, to: 2, transport: "do_rpc_wcs", partition: 1 },
      ],
      rootTaskId: 1,
      rootOutput: "parquet",
      finalizer: { kind: "concat_parquet", partitionOrder: [1, 2] },
      resultColumns: new Uint8Array(),
    };
    assert.ok(rangeSortParquetExport(plan));
  });
});
