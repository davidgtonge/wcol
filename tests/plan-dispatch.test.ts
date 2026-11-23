import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { planQueryDispatch, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runNodeProgram, runPlannedQuery } from "../src/runtime/coordinator/run-local-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const hits = path.join(root, "data/hits_subset_500k.parquet");

const QUERIES = [
  "SELECT COUNT(*) FROM hits",
  "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
  "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
  "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
  "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
  "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
  "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2) FROM hits",
];

function fileReader(file: string) {
  const fd = fs.openSync(file, "r");
  const size = fs.fstatSync(fd).size;
  let calls = 0;
  let bytes = 0;
  return {
    size,
    stats: () => ({ calls, bytes }),
    reset: () => {
      calls = 0;
      bytes = 0;
    },
    read: async (offset: number, length: number): Promise<Uint8Array> => {
      calls += 1;
      bytes += length;
      const out = new Uint8Array(length);
      fs.readSync(fd, out, 0, length, offset);
      return out;
    },
    close: () => fs.closeSync(fd),
  };
}

/**
 * Opcodes the stream pool runs (rust/wcol-engine/src/program/stream_program.rs). Opcode 10
 * (SQL body) is gone. 15 is the finalizer, 16 a hash reducer, 17 a shard export.
 */
const NODE_OPCODES = new Set([11, 12, 13, 14, 15, 16, 17]);

function assertNoSqlInProgram(program: Uint8Array): void {
  assert.equal(program[0], 0xff, "plan prefix");
  assert.ok(program.byteLength >= 6, "program header");
  const count = new DataView(program.buffer, program.byteOffset, program.byteLength).getUint32(1, true);
  assert.equal(count, 1, "one operator");
  const opcode = program[5]!;
  assert.ok(NODE_OPCODES.has(opcode), `opcode ${opcode} is not a node opcode`);
  assert.ok(!Buffer.from(program).toString("latin1").toLowerCase().includes("select"), "SQL text in program");
}

describe("queries run from the physical plan", { skip: !fs.existsSync(hits) && "hits subset missing" }, () => {
  it("node programs carry no SQL and read exactly the planned bytes", async () => {
    const coordinator = await loadCoordinatorWasm();
    const node = await loadNodeWasm();
    const reader = fileReader(hits);
    try {
      const footer = await readParquetFooter(reader.read, reader.size);
      const dispatch = planQueryDispatch(coordinator, { sql: QUERIES[4]!, objectId: "hits", footer, nodeCount: 2 });
      assert.equal(dispatch.kind, "graph");
      if (dispatch.kind !== "graph") return;
      for (const task of dispatch.tasks) assertNoSqlInProgram(task.program);
      const workers = dispatch.tasks.filter((t) => t.id !== dispatch.rootTaskId);
      assert.ok(workers.length >= 1);
      for (const task of workers) {
        reader.reset();
        await runNodeProgram(node, task.program, reader.read);
        assert.deepEqual(reader.stats(), { calls: task.requestCount, bytes: task.fetchedBytes });
      }
      const count = planQueryDispatch(coordinator, { sql: QUERIES[0]!, objectId: "hits", footer, nodeCount: 2 });
      assert.equal(count.kind, "metadata_partial", "COUNT(*) comes from the footer");
    } finally {
      reader.close();
    }
  });

  it("matches across 1, 2 and 4 nodes", async () => {
    const wasm = { node: await loadNodeWasm(), coordinator: await loadCoordinatorWasm() };
    const reader = fileReader(hits);
    try {
      const footer = await readParquetFooter(reader.read, reader.size);
      for (const sql of QUERIES) {
        const planned = planQueryDispatch(wasm.coordinator, {
          sql,
          objectId: "hits",
          footer,
          nodeCount: 2,
        });
        if (planned.kind === "graph") {
          for (const task of planned.tasks) assertNoSqlInProgram(task.program);
        }
        let expected: string | undefined;
        for (const nodeCount of [1, 2, 4]) {
          reader.reset();
          const result = await runPlannedQuery(wasm, {
            sql,
            sourceKey: "hits",
            footer,
            readAt: reader.read,
            nodeCount,
          });
          const json = resultJson(
            wasm.coordinator,
            planned,
            result.rows ? { rows: result.rows } : { partial: result.partial },
          );
          if (!expected) expected = json;
          assert.equal(json, expected, `${sql} on ${nodeCount} nodes`);
          assert.equal(reader.stats().calls, result.plannedRequests, `${sql}: reads made`);
          assert.equal(reader.stats().bytes, result.plannedBytes, `${sql}: bytes read`);
        }
      }
    } finally {
      reader.close();
    }
  });
});
