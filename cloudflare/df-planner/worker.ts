/**
 * Minimal Worker: POST /plan { sql, catalog? } → DataFusion logical plan JSON.
 * GET / → health + last timing from this isolate.
 */

import hitsCatalog from "../../rust/df-planner-wasm/fixtures/hits_catalog.json";
import wasmModule from "../../rust/df-planner-wasm/pkg-web/df_planner_wasm_bg.wasm";
import init, { df_plan_sql } from "../../rust/df-planner-wasm/pkg-web/df_planner_wasm.js";

type Planner = { df_plan_sql: typeof df_plan_sql };

let plannerPromise: Promise<{ planner: Planner; loadMs: number }> | null = null;

function loadPlanner() {
  if (!plannerPromise) {
    plannerPromise = (async () => {
      const t0 = performance.now();
      await init({ module_or_path: wasmModule });
      const loadMs = performance.now() - t0;
      return { planner: { df_plan_sql }, loadMs };
    })();
  }
  return plannerPromise;
}

const DEFAULT_SQL =
  "SELECT AdvEngineID, COUNT(*) AS cnt FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY cnt DESC LIMIT 10";

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return json({
        service: "df-planner-spike",
        plan: "POST /plan { sql?, catalog? }",
        bench: "POST /plan/bench (default ClickBench Q3 + hits catalog)",
      });
    }

    if (request.method === "POST" && (url.pathname === "/plan" || url.pathname === "/plan/bench")) {
      try {
        const tReq = performance.now();
        const { planner, loadMs } = await loadPlanner();
        const tAfterLoad = performance.now();

        let sql = DEFAULT_SQL;
        let catalogJson = JSON.stringify(hitsCatalog);

        if (url.pathname === "/plan") {
          const body = (await request.json()) as { sql?: unknown; catalog?: unknown };
          if (typeof body.sql === "string" && body.sql.trim()) sql = body.sql;
          if (body.catalog !== undefined) catalogJson = JSON.stringify(body.catalog);
        }

        const tPlan0 = performance.now();
        const raw = planner.df_plan_sql(sql, catalogJson);
        const planMs = performance.now() - tPlan0;

        const parsed = JSON.parse(raw) as { ok?: boolean; error?: string };
        if (!parsed.ok) {
          return json({ ...parsed, timing: timing(loadMs, planMs, tReq, tAfterLoad) }, 400);
        }

        return json({
          ...parsed,
          timing: timing(loadMs, planMs, tReq, tAfterLoad),
        });
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        return json({ ok: false, error: message }, 500);
      }
    }

    return json({ error: "not found" }, 404);
  },
};

function timing(
  wasmLoadMs: number,
  planMs: number,
  tReq: number,
  tAfterLoad: number,
): Record<string, number> {
  const handlerUntilLoadMs = tAfterLoad - tReq;
  const totalMs = performance.now() - tReq;
  return {
    wasmModuleLoadMs: round(wasmLoadMs),
    handlerUntilWasmReadyMs: round(handlerUntilLoadMs),
    planMs: round(planMs),
    totalRequestMs: round(totalMs),
  };
}

function round(ms: number): number {
  return Math.round(ms * 10) / 10;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
