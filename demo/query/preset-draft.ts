import type { DatasetKind, QueryDraft } from "../arch/types.ts";
import { presetById } from "../data/presets.ts";
import { DEFAULT_TOP_K } from "./constants.ts";

export function draftFromPreset(kind: DatasetKind | null, presetId: string): QueryDraft | null {
  const preset = presetById(kind, presetId);
  if (!preset) return null;
  const plan = preset.plan;

  if (plan.groupBy) {
    const keys = plan.groupBy.keys ?? (plan.groupBy.key ? [plan.groupBy.key] : []);
    const keyNames = keys.map((k) => String(k));
    const agg = plan.aggregates?.[0];
    const aggCol = agg ? String(agg.column ?? agg.col ?? "downloads") : "downloads";
    return {
      mode: "aggregate",
      searchText: "",
      searchColumn: keyNames[0] ?? "crate_name",
      filters: [],
      groupKeys: keyNames,
      aggColumn: String(plan.groupBy.value ?? aggCol),
      selectColumns: ["crate_name", "license", "downloads"],
      topK: plan.limit ?? DEFAULT_TOP_K,
    };
  }

  if (plan.select?.length) {
    return {
      mode: "table",
      searchText: "",
      searchColumn: String(plan.select[0]),
      filters: [],
      groupKeys: ["crate_name"],
      aggColumn: "downloads",
      selectColumns: plan.select.map(String),
      topK: plan.limit ?? DEFAULT_TOP_K,
    };
  }

  return {
    mode: "search",
    searchText: "",
    searchColumn: "crate_name",
    filters: [],
    groupKeys: ["crate_name"],
    aggColumn: "downloads",
    selectColumns: [],
    topK: plan.limit ?? DEFAULT_TOP_K,
  };
}
