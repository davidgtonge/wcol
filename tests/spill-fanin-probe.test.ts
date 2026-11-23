import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import {
  DEFAULT_CLICKBENCH_HITS_URL,
  openParquetByteSource,
  resolveHitsLocation,
} from "./helpers/spill-fanin-harness.ts";

const ROOT = path.resolve(import.meta.dirname, "..");

describe("spill fan-in full hits source", () => {
  it("resolves ClickBench HTTP when local hits.parquet is absent", async () => {
    const loc = resolveHitsLocation({ forFullGraph: true, repoRoot: ROOT });
    if (process.env.HITS_PARQUET || process.env.HITS_URL) {
      console.log(`note: using env hits location ${loc.slice(0, 72)}`);
    } else {
      assert.equal(loc, DEFAULT_CLICKBENCH_HITS_URL);
    }
    const source = await openParquetByteSource(loc);
    assert.ok(source.fileSize > 1_000_000_000, `unexpected file size ${source.fileSize}`);
    const footer = await readParquetFooter(source.readAt, source.fileSize);
    assert.ok(footer.byteLength > 100, "footer read failed");
  });
});
