import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import {
  openParquetByteSource,
  resolveHitsLocation,
} from "./helpers/spill-fanin-harness.ts";

const ROOT = path.resolve(import.meta.dirname, "..");

describe("spill fan-in full hits source", () => {
  // Reads ClickHouse's public 14.8 GB file over HTTP, so it runs only when asked to.
  const remote = process.env.HITS_URL ?? process.env.HITS_PARQUET;
  it("reads the full hits footer by range", { skip: remote ? false : "set HITS_URL to read the full hits file" }, async () => {
    const loc = resolveHitsLocation({ forFullGraph: true, repoRoot: ROOT });
    const source = await openParquetByteSource(loc);
    assert.ok(source.fileSize > 1_000_000_000, `unexpected file size ${source.fileSize}`);
    const footer = await readParquetFooter(source.readAt, source.fileSize);
    assert.ok(footer.byteLength > 100, "footer read failed");
  });
});
