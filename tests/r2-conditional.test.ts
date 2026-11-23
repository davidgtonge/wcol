/**
 * Conditional R2 against the Workers binding (miniflare), not a mock that throws.
 * A lost `onlyIf` put returns null. A missed conditional get can carry metadata and no body.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { Miniflare } from "miniflare";

const probe = `
import { DurableObject } from "cloudflare:workers";
export class Probe extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.storage = state.storage;
  }
  async alarm() {}
  async fetch(request) {
    const url = new URL(request.url);
    const storage = this.storage;
    if (url.pathname === "/abort") {
      try {
        await storage.transaction(async (txn) => {
          await txn.put("graph-job:probe", { state: "accepted" });
          await txn.setAlarm(Date.now() + 60000);
          throw new Error("abort");
        });
      } catch {
        // rolled back
      }
      const alarm = await storage.getAlarm();
      return Response.json({ row: (await storage.get("graph-job:probe")) ?? null, alarm });
    }
    await storage.transaction(async (txn) => {
      await txn.put("graph-job:probe", { state: "accepted" });
      await txn.setAlarm(Date.now() + 60000);
    });
    const alarm = await storage.getAlarm();
    return Response.json({ row: await storage.get("graph-job:probe"), alarm });
  }
}
export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "probe";
    const stub = env.PROBE.get(env.PROBE.idFromName(name));
    return stub.fetch(request);
  },
};
`;

test("conditional put loses as null and a missed conditional get is not a body", async () => {
  const mf = new Miniflare({
    modules: true,
    script: `export default { fetch() { return new Response("ok"); } }`,
    compatibilityDate: "2026-01-01",
    r2Buckets: ["OUTPUT"],
  });
  try {
    const bucket = (await mf.getR2Bucket("OUTPUT")) as unknown as {
      put(key: string, value: string, options?: { onlyIf?: { etagDoesNotMatch?: string; etagMatches?: string } }): Promise<{ etag?: string; httpEtag?: string } | null>;
      get(key: string, options?: { onlyIf?: { etagMatches?: string } }): Promise<{ text(): Promise<string>; body?: unknown } | null>;
    };
    const first = await bucket.put("shared/key", "one");
    assert.ok(first);
    const lost = await bucket.put("shared/key", "two", { onlyIf: { etagDoesNotMatch: "*" } });
    assert.equal(lost, null);
    const kept = await bucket.get("shared/key");
    assert.equal(await kept?.text(), "one");
    const match = first.etag || String(first.httpEtag ?? "").replace(/"/g, "");
    const hit = await bucket.get("shared/key", { onlyIf: { etagMatches: match } });
    assert.equal(await hit?.text(), "one");
    const missed = await bucket.get("shared/key", { onlyIf: { etagMatches: "not-the-etag" } });
    const body = missed && "body" in missed ? (missed as { body?: unknown }).body : undefined;
    assert.ok(missed == null || body == null, "a failed conditional get is not a successful read");
  } finally {
    await mf.dispose();
  }
});

test("a SQLite transaction commits the job row and alarm together, and an abort leaves neither", async () => {
  const mf = new Miniflare({
    modules: true,
    script: probe,
    compatibilityDate: "2026-01-01",
    durableObjects: { PROBE: { className: "Probe", useSQLite: true } },
  });
  try {
    const committed = await mf.dispatchFetch("http://probe.local/commit?name=committed");
    const commitText = await committed.text();
    assert.equal(committed.status, 200, commitText);
    const commitBody = JSON.parse(commitText) as { row: { state: string } | null; alarm: number | null };
    assert.equal(commitBody.row?.state, "accepted");
    assert.ok(commitBody.alarm != null);

    const aborted = await mf.dispatchFetch("http://probe.local/abort?name=aborted");
    const abortBody = (await aborted.json()) as { row: unknown; alarm: number | null };
    assert.equal(abortBody.row, null);
    assert.equal(abortBody.alarm, null);
  } finally {
    await mf.dispose();
  }
});
