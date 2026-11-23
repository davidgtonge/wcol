import test from "node:test";
import assert from "node:assert/strict";
import { buildPlan, EXAMPLE_PLAN } from "../src/runtime/query/plan-format.ts";

test("buildPlan normalizes EXAMPLE_PLAN shape", () => {
  const plan = buildPlan(EXAMPLE_PLAN);
  assert.equal(plan.limit, 10);
  assert.equal(plan.filters?.length, 4);
  assert.equal(plan.combine?.length, 1);
  assert.ok(plan.groupBy?.keys?.length === 2);
  assert.equal(plan.aggregates?.length, 2);
});

test("buildPlan omits empty arrays", () => {
  const plan = buildPlan({ limit: 5, filters: [], aggregates: [] });
  assert.equal(plan.limit, 5);
  assert.equal(plan.filters, undefined);
  assert.equal(plan.aggregates, undefined);
});

test("buildPlan includes select column refs", () => {
  const plan = buildPlan({
    limit: 3,
    filters: [{ column: "CounterID", op: "=", value: 1 }],
    select: ["CounterID", "EventDate"]
  });
  assert.equal(plan.select?.length, 2);
});
