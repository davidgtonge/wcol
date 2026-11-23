import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { readJobRecord } from "../cloudflare/src/coordinator/coordinator.ts";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (file: string) => fs.readFileSync(path.join(root, file), "utf8");
const modules = (dir: string) =>
  fs.readdirSync(path.join(root, dir)).filter((name) => name.endsWith(".ts")).sort().map((name) => `${dir}/${name}`);

/** Job SQLite rows are written by the coordinator modules, never the node. */
const JOB_STORE = modules("cloudflare/src/coordinator");
const NODE = modules("cloudflare/src/node");

/** Every file on the job path: entry, coordinator, job modules, node tasks. */
const JOB_PATH = ["cloudflare/src/worker.ts", ...JOB_STORE, ...NODE];

describe("job path boundaries (I11, I12, I14, I15)", () => {
  it("moves programs and streams as bytes, never base64", () => {
    for (const file of JOB_PATH) {
      const text = read(file);
      for (const banned of ["bytesToBase64", "base64ToBytes", "btoa(", "atob("]) {
        assert.ok(!text.includes(banned), `${file} uses ${banned}`);
      }
    }
  });

  it("stores only bounded status records, and never merges on the coordinator", () => {
    for (const file of NODE) {
      const node = read(file);
      assert.ok(!node.includes("store.put(") && !node.includes("txn.put("), `${file} stores a job row`);
    }
    const jobs = JOB_STORE.map(read).join("\n");
    const puts = ["store.put(", "txn.put("].flatMap((marker) =>
      jobs.split(marker).slice(1).map((s) => s.slice(0, s.indexOf(");"))),
    );
    assert.ok(puts.length >= 3, "expected the job, idempotency and task records");
    for (const put of puts) {
      // Ids, states and counts only: a byte count is fine, the bytes are not.
      assert.ok(!/\b(rows|partial|program|programs|footer|footers)\b/.test(put), `stored record holds data: ${put}`);
      assert.ok(!/\bresult\b(?!\.bytes)/.test(put), `stored record holds a result: ${put}`);
    }
    for (const banned of ["reduceWcsStream", "wcol_reduce", "CoordinatorReducer"]) {
      assert.ok(!jobs.includes(banned), `job modules use ${banned}`);
    }
  });

  it("has one job entry point: the retired routes answer 410", () => {
    const worker = read("cloudflare/src/worker.ts");
    for (const route of ["/row-export", "/page-compact", "/export", "/compact", "/distributed"]) {
      assert.ok(worker.includes(`"${route}"`), `${route} is not in the retired list`);
    }
    for (const handler of ["handleRowExportJob", "handlePageCompactJob", "handleShardRequest", "handleStreamTaskStart"]) {
      assert.ok(!worker.includes(handler), `worker still routes to ${handler}`);
    }
  });

  it("drops the coordinator alarm when a job is terminal so the DO can hibernate", () => {
    const finish = ["finish.ts", "renewal.ts", "submit.ts"].map((file) => read(`cloudflare/src/coordinator/${file}`)).join("\n");
    const publish = read("cloudflare/src/coordinator/publish.ts");
    const node = read("cloudflare/src/node/node.ts");
    assert.ok(finish.includes("deleteAlarm"), "finishPrepared must clear the job alarm");
    assert.ok(finish.includes("stopped = true"), "lease renewal must not keep setInterval after stop");
    assert.ok(publish.includes("deleteAlarm"), "complete/fail must clear the job alarm");
    const complete = publish.slice(publish.indexOf("async function completePublish"));
    assert.ok(
      !complete.slice(0, 600).includes("setAlarm"),
      "completePublish must not re-arm after the job is done",
    );
    assert.ok(node.includes("async alarm("), "node DOs need an alarm to abort orphaned hash attempts");
    assert.ok(node.includes("deleteAlarm"), "node DOs must drop the hash lease alarm after work");
  });

  it("rejects a job ledger row that is missing identity fields", () => {
    assert.equal(readJobRecord(undefined), undefined);
    assert.equal(readJobRecord({ jobId: "j1", kind: "json", state: "running" }), undefined);
    const row = readJobRecord({
      jobId: "j1",
      kind: "json",
      state: "running",
      executionAttempt: 1,
      failedAttempts: 0,
      nodeCount: 0,
      createdAt: 1,
      updatedAt: 1,
    });
    assert.equal(row?.jobId, "j1");
    assert.equal(row?.executionAttempt, 1);
  });
});
