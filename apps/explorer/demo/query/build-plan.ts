import { buildPlan, type QueryPlan } from "../wcol-query.ts";
import type { DatasetKind, QueryDraft } from "../arch/types.ts";
import { DEFAULT_TOP_K } from "./constants.ts";

function combineTokens(filterCount: number): ("AND" | "OR")[] | undefined {
  if (filterCount <= 1) return undefined;
  return Array.from({ length: filterCount - 1 }, () => "AND");
}

function parseFilterValue(raw: string, op: string): unknown {
  const t = raw.trim();
  if (op === "in") {
    return t.split(",").map((s) => s.trim()).filter(Boolean);
  }
  if (op === "between") {
    const [a, b] = t.split(",").map((s) => s.trim());
    return { a, b };
  }
  if (/^-?\d+(\.\d+)?$/.test(t)) return Number(t);
  if (t === "true") return true;
  if (t === "false") return false;
  return t;
}

export function buildQueryPlan(draft: QueryDraft): QueryPlan {
  const filters = draft.filters
    .filter((f) => f.column && f.value.trim())
    .map((f) => {
      const op = f.op === "contains" ? "like" : f.op;
      if (f.op === "between") {
        const { a, b } = parseFilterValue(f.value, "between") as { a: string; b: string };
        return { column: f.column, op: "between" as const, value: a, value2: b };
      }
      return { column: f.column, op, value: parseFilterValue(f.value, f.op) };
    });

  const search = draft.searchText.trim();
  if (search && draft.searchColumn) {
    filters.unshift({ column: draft.searchColumn, op: "like", value: search });
  }

  const combine = combineTokens(filters.length);
  const topK = draft.topK > 0 ? draft.topK : DEFAULT_TOP_K;

  if (draft.mode === "aggregate") {
    const keys = draft.groupKeys.filter(Boolean).slice(0, 2);
    if (!keys.length) {
      throw new Error("Pick at least one group-by column");
    }
    const aggCol = draft.aggColumn || "downloads";
    return buildPlan({
      limit: topK,
      filters: filters.length ? filters : undefined,
      combine,
      groupBy: { keys, value: aggCol },
      aggregates: [{ column: aggCol }],
      groupOrderByCount: true,
    });
  }

  const select =
    draft.mode === "table" && draft.selectColumns.length ? draft.selectColumns : undefined;

  return buildPlan({
    limit: topK,
    filters: filters.length ? filters : undefined,
    combine,
    select,
  });
}

export function defaultQueryDraft(kind: DatasetKind | null): QueryDraft {
  if (kind === "categories") {
    return {
      mode: "aggregate",
      searchText: "",
      searchColumn: "crate_name",
      filters: [],
      groupKeys: ["category_name"],
      aggColumn: "crate_downloads",
      selectColumns: ["crate_name", "category_name", "crate_downloads"],
      topK: DEFAULT_TOP_K,
    };
  }
  if (kind === "maintainers") {
    return {
      mode: "search",
      searchText: "",
      searchColumn: "owner_login",
      filters: [],
      groupKeys: ["crate_name"],
      aggColumn: "crate_downloads",
      selectColumns: ["crate_name", "owner_login", "crate_downloads"],
      topK: DEFAULT_TOP_K,
    };
  }
  if (kind === "dependencies") {
    return {
      mode: "aggregate",
      searchText: "",
      searchColumn: "parent_crate_name",
      filters: [],
      groupKeys: ["dep_crate_name"],
      aggColumn: "dependency_id",
      selectColumns: ["parent_crate_name", "dep_crate_name", "optional"],
      topK: DEFAULT_TOP_K,
    };
  }
  if (kind === "hits") {
    return {
      mode: "search",
      searchText: "",
      searchColumn: "URL",
      filters: [],
      groupKeys: ["CounterID"],
      aggColumn: "ResolutionWidth",
      selectColumns: ["CounterID", "EventDate", "URL"],
      topK: DEFAULT_TOP_K,
    };
  }
  if (kind === "trends") {
    return {
      mode: "aggregate",
      searchText: "",
      searchColumn: "crate_name",
      filters: [],
      groupKeys: ["crate_name"],
      aggColumn: "downloads",
      selectColumns: ["date", "crate_name", "version", "downloads"],
      topK: DEFAULT_TOP_K,
    };
  }
  return {
    mode: "aggregate",
    searchText: "",
    searchColumn: "crate_name",
    filters: [],
    groupKeys: ["crate_name"],
    aggColumn: "downloads",
    selectColumns: ["crate_name", "license", "downloads"],
    topK: DEFAULT_TOP_K,
  };
}

export function planPreview(draft: QueryDraft): string {
  try {
    return JSON.stringify(buildQueryPlan(draft), null, 2);
  } catch (e) {
    return JSON.stringify({ error: e instanceof Error ? e.message : String(e) }, null, 2);
  }
}
