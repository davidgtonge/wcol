import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { loadStreamWasm } from "../src/runtime/host/wasm.ts";
import {
  parsePartialQueryFromWireAuto,
  partialFirstSumFromWireAuto,
} from "../src/runtime/stream/partial-oracle.ts";
import { partialWireFormat } from "../src/runtime/stream/partial-wire.ts";
import { planQueryDispatch, readParquetFooter, runNodeProgram } from "../archive/pre-abi-host/plan-run.ts";
import { ParquetFile } from "../src/runtime/parquet.ts";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixture = path.join(root, "tests/fixtures/flat.parquet");

describe("partial oracle (PWCL wire)", { skip: !fs.existsSync(fixture) && "flat.parquet missing" }, () => {
  it("stream partial is PWCL and sum matches via Rust decode", async () => {
    const wasm = await loadStreamWasm();
    const file = await ParquetFile.open(fixture);
    try {
      const catalog = file.plan.leaves.map((leaf) => ({
      name: leaf.name,
      kind: leaf.physical === 6 || leaf.physical === 7 ? ("text" as const) : ("number" as const),
      physical: leaf.physical,
      typeLength: leaf.typeLength,
      maxDef: leaf.maxDef,
      maxRep: leaf.maxRep,
      }));
      const fh = await fs.promises.open(fixture, "r");
      let partial: Uint8Array;
      try {
        const readAt = async (offset: number, length: number) => {
          const buf = Buffer.alloc(length);
          await fh.read(buf, 0, length, offset);
          return new Uint8Array(buf);
        };
        const footer = await readParquetFooter(readAt, (await fh.stat()).size);
        // PWCL is the node → finalizer wire; the graph's root emits result rows.
        const plan = planQueryDispatch(wasm, {
          sql: "SELECT SUM(a) FROM flat",
          objectId: fixture,
          footer,
          nodeCount: 1,
          batchRows: 4,
        });
        assert.equal(plan.kind, "graph");
        if (plan.kind !== "graph") return;
        const node = plan.tasks.find((task) => task.id !== plan.rootTaskId)!;
        partial = (await runNodeProgram(wasm, node.program, readAt)).partial;
      } finally {
        await fh.close();
      }
      assert.ok(partial.byteLength > 0);
      assert.equal(partialWireFormat(partial), "pwcl");

      assert.equal(await partialFirstSumFromWireAuto(partial), 20);
      const parsed = await parsePartialQueryFromWireAuto(catalog, partial);
      const sumAgg = parsed.sums["sum(a)"] ?? parsed.query.aggregates?.["sum(a)"]?.sum;
      assert.equal(sumAgg, 20);
    } finally {
      await file.close();
    }
  });
});
