import { FilterOp, FLAG_DICT, TYPE_STRING } from "../core/constants.ts";
import { isInOp, normalizeCombineToken, normalizeOp } from "./filter-ops.ts";
import { callStatus, withArray, withBytes } from "../wasm/helpers.ts";
import { getColumnInfo, getColumnName, normalizeInList, normalizeValue, resolveColumnId } from "../schema/columns.ts";
import { columnRef } from "../core/refs.ts";
import type { WcolContext } from "../core/context.ts";
import type { QueryPlan } from "../core/types.ts";

export async function applyPlan(ctx: WcolContext, planSpec: QueryPlan, plan: number): Promise<void> {
  validatePlanSpec(planSpec);
  setLimit(ctx, planSpec, plan);
  await addFilters(ctx, planSpec, plan);
  setCombine(ctx, planSpec, plan);
  await setGroupBy(ctx, planSpec, plan);
  setGroupOrderByCount(ctx, planSpec, plan);
  await addAggregates(ctx, planSpec, plan);
  await setSelect(ctx, planSpec, plan);
  callStatus(ctx.wasm.exports.plan_prepare_optimizations(plan));
}

function validatePlanSpec(planSpec: QueryPlan): void {
  const hasSelect = (planSpec.select?.length ?? 0) > 0;
  if (!hasSelect) {
    return;
  }
  if (planSpec.groupBy) {
    throw new Error("QueryPlan.select cannot be used with groupBy");
  }
  if ((planSpec.aggregates?.length ?? 0) > 0) {
    throw new Error("QueryPlan.select cannot be used with aggregates");
  }
}

async function setSelect(ctx: WcolContext, planSpec: QueryPlan, plan: number): Promise<void> {
  const refs = planSpec.select;
  if (!refs?.length) {
    return;
  }
  const colIds = await Promise.all(refs.map((ref) => resolveColumnId(ctx, ref)));
  const buffer = new Uint32Array(colIds);
  withArray(ctx.wasm, buffer, (ptr, len) =>
    callStatus(ctx.wasm.exports.plan_set_select(plan, ptr, len))
  );
}

function setLimit(ctx: WcolContext, planSpec: QueryPlan, plan: number): void {
  if (planSpec.limit) {
    callStatus(ctx.wasm.exports.plan_set_limit(plan, planSpec.limit));
  }
}

async function addFilters(ctx: WcolContext, planSpec: QueryPlan, plan: number): Promise<void> {
  for (const filter of planSpec.filters ?? []) {
    const colId = await resolveColumnId(ctx, columnRef(filter)!);
    const info = await getColumnInfo(ctx, colId);
    const opToken = filter.op ?? filter.operator;
    const isIn = isInOp(opToken);
    const op = isIn ? FilterOp.EQ : normalizeOp(opToken);

    if (op === FilterOp.LIKE || op === FilterOp.NOT_LIKE) {
      const pattern = String(filter.value ?? "");
      const index = ctx.wasm.exports.plan_add_filter(plan, colId, op, 0, 0);
      if (index < 0) {
        throw new Error("Failed to add filter");
      }
      const patternBytes = new TextEncoder().encode(pattern);
      withBytes(ctx.wasm, patternBytes, (ptr, len) => {
        const code = ctx.wasm.exports.plan_set_filter_value_str(plan, index, ptr, len);
        if (code < 0) {
          throw new Error(`Failed to set LIKE pattern (${code})`);
        }
      });
      continue;
    }

    if (isIn) {
      const values = await normalizeInList(ctx, colId, info, filter.value ?? []);
      const buffer = new Float64Array(values);
      withArray(ctx.wasm, buffer, (ptr, len) =>
        callStatus(ctx.wasm.exports.plan_add_filter_in(plan, colId, op, ptr, len))
      );
      continue;
    }

    const value = await normalizeValue(ctx, colId, info, filter.value);
    const value2 = filter.value2 !== undefined
      ? await normalizeValue(ctx, colId, info, filter.value2)
      : value;
    const index = ctx.wasm.exports.plan_add_filter(plan, colId, op, value, value2);
    if (index < 0) {
      throw new Error("Failed to add filter");
    }
  }
}

function setCombine(ctx: WcolContext, planSpec: QueryPlan, plan: number): void {
  if (!planSpec.combine) {
    return;
  }
  const tokens = planSpec.combine.map((token) => normalizeCombineToken(token));
  withArray(ctx.wasm, new Int32Array(tokens), (ptr, len) =>
    callStatus(ctx.wasm.exports.plan_set_combine(plan, ptr, len))
  );
}

function setGroupOrderByCount(ctx: WcolContext, planSpec: QueryPlan, plan: number): void {
  if (planSpec.groupOrderByCount) {
    callStatus(ctx.wasm.exports.plan_set_group_order_by_count(plan, 1));
  }
}

async function setGroupBy(ctx: WcolContext, planSpec: QueryPlan, plan: number): Promise<void> {
  if (!planSpec.groupBy) {
    return;
  }
  const keyRefs = planSpec.groupBy.keys ?? (planSpec.groupBy.key ? [planSpec.groupBy.key] : []);
  const keyIds = await Promise.all(keyRefs.map((key) => resolveColumnId(ctx, key)));
  const valueCol = planSpec.groupBy.value
    ? await resolveColumnId(ctx, planSpec.groupBy.value)
    : -1;
  const key1 = keyIds[0] ?? -1;
  const key2 = keyIds[1] ?? -1;
  callStatus(ctx.wasm.exports.plan_set_group_by(plan, key1, key2, valueCol));
}

async function addAggregates(ctx: WcolContext, planSpec: QueryPlan, plan: number): Promise<void> {
  for (const agg of planSpec.aggregates ?? []) {
    const colId = await resolveColumnId(ctx, columnRef(agg)!);
    const info = await getColumnInfo(ctx, colId);
    if ((info.flags & FLAG_DICT) !== 0 || info.physicalType === TYPE_STRING) {
      const name = await getColumnName(ctx, colId);
      throw new Error(`Aggregates require numeric columns; ${name} is not numeric`);
    }
    callStatus(ctx.wasm.exports.plan_add_aggregate(plan, colId));
  }
}
