import type { QueryPlan } from "../wcol-runtime.ts";
import type { PresetDef } from "../arch/types.ts";
import { DEFAULT_TOP_K } from "./constants.ts";

/** Apply default top-K and group ordering to preset plans. */
export function withTopK(plan: QueryPlan): QueryPlan {
  const limit = plan.limit && plan.limit > 0 ? plan.limit : DEFAULT_TOP_K;
  const hasGroup = Boolean(plan.groupBy);
  return {
    ...plan,
    limit,
    groupOrderByCount: hasGroup ? plan.groupOrderByCount ?? true : plan.groupOrderByCount,
  };
}

export function presetPlan(preset: PresetDef): QueryPlan {
  return withTopK(preset.plan);
}
