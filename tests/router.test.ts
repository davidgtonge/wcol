import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { createRouter } from "../cloudflare/src/router.ts";

describe("exact router", () => {
  it("matches method and path, and turns a thrown handler into a 500", async () => {
    const router = createRouter();
    router.get("/jobs", (_request, url) => Response.json({ jobId: url.searchParams.get("jobId") }));
    router.post("/jobs", async (request) => {
      const body = (await request.json()) as { ok?: boolean };
      if (!body.ok) throw new Error(`bad ${"x".repeat(600)}`);
      return Response.json({ ok: true });
    });

    const found = await router.fetch(new Request("http://do/jobs?jobId=j1"));
    assert.equal(found.status, 200);
    assert.deepEqual(await found.json(), { jobId: "j1" });

    const posted = await router.fetch(
      new Request("http://do/jobs", { method: "POST", body: JSON.stringify({ ok: true }) }),
    );
    assert.equal(posted.status, 200);

    const missing = await router.fetch(new Request("http://do/jobs/other"));
    assert.equal(missing.status, 404);
    assert.deepEqual(await missing.json(), { error: "not found" });

    const wrongMethod = await router.fetch(new Request("http://do/jobs", { method: "PUT" }));
    assert.equal(wrongMethod.status, 404);

    const failed = await router.fetch(
      new Request("http://do/jobs", { method: "POST", body: JSON.stringify({ ok: false }) }),
    );
    assert.equal(failed.status, 500);
    const error = ((await failed.json()) as { error: string }).error;
    assert.equal(error.length, 500);
    assert.ok(error.startsWith("bad "));
  });
});
