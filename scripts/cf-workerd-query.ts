/**
 * POST /jobs on workerd: the coordinator plans with the hand parser and the
 * node Worker runs fragment programs. Two isolates, two Wasm binaries.
 *
 *   npx tsx scripts/cf-workerd-query.ts data/hits_subset_500k.parquet
 */

import { createServer } from "node:http";
import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, coordinatorOut, distDir, nodeOut, ROOT } from "./build-workers.ts";
const parquetPath = process.argv[2] ?? path.join(ROOT, "data/hits_subset_500k.parquet");
const sql = process.argv[3] ?? "SELECT SUM(AdvEngineID) FROM hits";
const expectedSum = process.argv[3] ? null : 76010;
const q15 = process.argv[4] === "q15";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function serveParquet(file: Buffer): Promise<{ url: string; close: () => Promise<void> }> {
  const server = createServer((req, res) => {
    const range = req.headers.range;
    if (!range) {
      res.writeHead(200, {
        "content-type": "application/octet-stream",
        "content-length": file.length,
        "accept-ranges": "bytes",
        etag: '"parquet-fixture"',
      });
      res.end(file);
      return;
    }
    const suffix = /bytes=-(\d+)/.exec(range);
    const match = suffix ? null : /bytes=(\d+)-(\d*)/.exec(range);
    if (!suffix && !match) {
      res.writeHead(416);
      res.end();
      return;
    }
    const start = suffix ? Math.max(0, file.length - Number(suffix[1])) : Number(match![1]);
    const end = suffix ? file.length - 1 : match![2] ? Number(match![2]) : file.length - 1;
    const slice = file.subarray(start, end + 1);
    res.writeHead(206, {
      "content-type": "application/octet-stream",
      "content-range": `bytes ${start}-${end}/${file.length}`,
      "content-length": slice.length,
      "accept-ranges": "bytes",
      etag: '"parquet-fixture"',
    });
    res.end(slice);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}/cf-tiny.parquet`,
        close: () =>
          new Promise((done) => {
            server.close(() => done());
          }),
      });
    });
  });
}

await buildWorkers();
const file = fs.readFileSync(parquetPath);
const served = await serveParquet(file);
const mf = new Miniflare({
  workers: [
    {
      name: "wcol-coordinator",
      modulesRoot: distDir,
      modules: [
        { type: "ESModule", path: coordinatorOut },
        { type: "CompiledWasm", path: path.join(distDir, "wcol_coordinator.wasm") },
      ],
      compatibilityDate: "2026-01-01",
      durableObjects: {
        WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
        WCOL_NODE: { className: "WcolNode", scriptName: "wcol-node", useSQLite: true },
      },
      r2Buckets: ["OUTPUT", "ANALYTICS"],
    },
    {
      name: "wcol-node",
      modulesRoot: distDir,
      modules: [
        { type: "ESModule", path: nodeOut },
        { type: "CompiledWasm", path: path.join(distDir, "wcol_node.wasm") },
      ],
      compatibilityDate: "2026-01-01",
      durableObjects: {
        WCOL_NODE: { className: "WcolNode", useSQLite: true },
      },
      r2Buckets: ["OUTPUT", "ANALYTICS"],
    },
  ],
});

async function postJob(extra: Record<string, unknown> = {}): Promise<{
  status: number;
  text: string;
  body: {
    planner?: string;
    kind?: string;
    sum?: number;
    nodeCount?: number;
    shardCount?: number;
    error?: string;
    partial?: string;
    jobId?: string;
    failedTaskAttempts?: number;
  };
}> {
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      sources: { hits: { key: served.url } },
      sql,
      output: { format: "json" },
      ...extra,
    }),
  });
  const text = await response.text();
  return { status: response.status, text, body: JSON.parse(text) as never };
}

/** `/jobs` answers with result rows: `{"columns":[..],"rows":[[..],..]}`. */
type JobRows = { columns?: string[]; rows?: unknown[][] };

try {
  const started = Date.now();
  const response = await postJob();
  const { body, text } = response;
  if (response.status !== 200) throw new Error(`${response.status} ${text.slice(0, 1200)}`);
  console.log(
    JSON.stringify({
      runtime: "miniflare",
      sql,
      columns: (body as JobRows).columns,
      nodeCount: body.nodeCount,
      rows: (body as JobRows).rows,
      wallMs: Date.now() - started,
    }),
  );
  const result = body as JobRows;
  assert(Array.isArray(result.columns) && Array.isArray(result.rows), "expected result rows");
  if (expectedSum != null) {
    const sum = result.rows![0]?.[0];
    assert(sum === expectedSum, `expected sum ${expectedSum}, got ${sum}`);
  }
  if (q15) {
    assert((body.nodeCount ?? 0) > 1, `Q15 should use several nodes, got ${body.nodeCount}`);
    assert(result.rows!.length > 0, "Q15 response has no rows");
    assert(body.jobId, "Q15 response missing jobId");
    const retry = await postJob({ failFirstAttemptTaskId: "n0" });
    if (retry.status !== 200) throw new Error(`${retry.status} ${retry.text.slice(0, 1200)}`);
    assert(JSON.stringify((retry.body as JobRows).rows) === JSON.stringify(result.rows), "mid-stream retry rows did not match");
    assert((retry.body.failedTaskAttempts ?? 0) >= 1, "mid-stream failure was not retried");
    const stored = await mf.dispatchFetch(`http://wcol.local/jobs/${body.jobId}`);
    const storedText = await stored.text();
    const record = JSON.parse(storedText) as { state?: string; failedAttempts?: number };
    assert(stored.ok, storedText.slice(0, 500));
    assert(record.state === "completed", `job state ${record.state}`);
    // The job record is all the coordinator stores for a job: small, and no data in it.
    const bytes = new TextEncoder().encode(storedText).byteLength;
    assert(bytes <= 4096, `job record is ${bytes} bytes`);
    for (const banned of ['"partial"', '"rows"', '"program"']) {
      assert(!storedText.includes(banned), `job record holds ${banned}`);
    }
    const { DuckDBInstance } = await import("@duckdb/node-api");
    const db = await DuckDBInstance.create(":memory:");
    const conn = await db.connect();
    const escaped = parquetPath.replaceAll("'", "''");
    const duck = await conn.runAndReadAll(
      `SELECT UserID, COUNT(*)::BIGINT FROM read_parquet('${escaped}') GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`,
    );
    const rows = duck.getRows() as unknown[][];
    const ours = result.rows!;
    assert(ours.length === rows.length, `groups ${ours.length} vs duckdb ${rows.length}`);
    for (let i = 0; i < rows.length; i++) {
      assert(String(ours[i]![0]) === String(rows[i]![0]), `row ${i} key ${ours[i]![0]} vs ${rows[i]![0]}`);
      assert(String(ours[i]![1]) === String(rows[i]![1]), `row ${i} count ${ours[i]![1]} vs ${rows[i]![1]}`);
    }
    console.log(JSON.stringify({ q15: "match", groups: rows.length, retried: retry.body.failedTaskAttempts }));
  }
} finally {
  await mf.dispose();
  await served.close();
}
