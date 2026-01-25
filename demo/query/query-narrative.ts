import type { DatasetKind, FilterDraft, QueryDraft } from "../arch/types.ts";

export type QueryChip = {
  id: string;
  label: string;
  value: string;
  editable?: boolean;
};

export type QueryNarrative = {
  headline: string;
  subline: string;
  chips: QueryChip[];
  explanation: string;
};

const COLUMN_LABELS: Record<string, string> = {
  crate_name: "crate name",
  license: "license",
  downloads: "downloads",
  yanked: "yanked",
  edition: "edition",
  version: "version",
  parent_crate_name: "parent crate",
  dep_crate_name: "dependency",
  dependency_id: "dependency edges",
  optional: "optional",
  kind: "kind",
  category_name: "category",
  category_slug: "category slug",
  crate_downloads: "crate downloads",
  owner_login: "owner login",
  owner_name: "owner name",
  owner_kind: "owner kind",
  date: "date",
  CounterID: "counter",
  EventDate: "event date",
  URL: "URL",
  ResolutionWidth: "resolution width",
};

function colLabel(column: string): string {
  return COLUMN_LABELS[column] ?? column.replace(/_/g, " ");
}

function formatFilter(f: FilterDraft): string {
  const col = colLabel(f.column);
  const op = f.op;
  const val = f.value.trim();
  if (!val) return `${col} (unset)`;
  if (op === "=") return `${col} is ${val}`;
  if (op === "!=") return `${col} is not ${val}`;
  if (op === "contains" || op === "like") return `${col} contains “${val}”`;
  if (op === ">") return `${col} > ${val}`;
  if (op === ">=") return `${col} ≥ ${val}`;
  if (op === "<") return `${col} < ${val}`;
  if (op === "<=") return `${col} ≤ ${val}`;
  if (op === "in") return `${col} in (${val})`;
  if (op === "between") return `${col} between ${val}`;
  return `${col} ${op} ${val}`;
}

function filterSummary(filters: FilterDraft[], searchText: string, searchColumn: string): string {
  const parts: string[] = [];
  if (searchText.trim() && searchColumn) {
    parts.push(`${colLabel(searchColumn)} contains “${searchText.trim()}”`);
  }
  for (const f of filters) {
    if (f.column && f.value.trim()) parts.push(formatFilter(f));
  }
  return parts.length ? parts.join(" · ") : "no filters";
}

function tableLabel(kind: DatasetKind | null): string {
  if (kind === "trends") return "daily download facts";
  if (kind === "dependencies") return "dependency edges";
  if (kind === "categories") return "category memberships";
  if (kind === "maintainers") return "maintainer rows";
  if (kind === "hits") return "events";
  return "versions";
}

export function describeQuery(draft: QueryDraft, kind: DatasetKind | null = "crates"): QueryNarrative {
  const limit = draft.topK;
  const filters = filterSummary(draft.filters, draft.searchText, draft.searchColumn);
  const filterChip = filters === "no filters" ? "none" : filters;
  const table = tableLabel(kind);

  if (draft.mode === "aggregate") {
    const keys = draft.groupKeys.filter(Boolean);
    const group = keys.map(colLabel).join(" × ") || "—";
    const metric = draft.aggColumn || "downloads";
    const headline =
      kind === "trends" && keys[0] === "crate_name" && metric === "downloads"
        ? `Top ${limit} crates by download volume`
        : kind === "trends" && keys[0] === "version" && metric === "downloads"
          ? `Top ${limit} versions by download volume`
          : keys[0] === "crate_name" && metric === "downloads"
            ? `Top ${limit} crates by total downloads`
            : `Top ${limit} groups by sum of ${colLabel(metric)}`;
    const subline = `Grouped by ${keys.map((k) => `\`${k}\``).join(", ") || "—"}, summed by \`${metric}\``;
    return {
      headline,
      subline,
      chips: [
        { id: "limit", label: "Limit", value: String(limit), editable: true },
        { id: "group", label: "Group by", value: group, editable: true },
        { id: "metric", label: "Metric", value: `sum(${colLabel(metric)})`, editable: true },
        { id: "filter", label: "Filter", value: filterChip, editable: true },
      ],
      explanation: `Ranking ${limit} groups from the ${table} table, ordered by total ${colLabel(metric)}.${filters !== "no filters" ? ` Only rows matching: ${filters}.` : ""}`,
    };
  }

  if (draft.mode === "table") {
    const cols = draft.selectColumns.length ? draft.selectColumns.join(", ") : "default columns";
    return {
      headline: kind === "trends" ? `Browse up to ${limit} daily download rows` : `Browse up to ${limit} matching versions`,
      subline: `Showing ${cols}`,
      chips: [
        { id: "limit", label: "Limit", value: String(limit), editable: true },
        { id: "columns", label: "Columns", value: cols, editable: true },
        { id: "filter", label: "Filter", value: filterChip, editable: true },
      ],
      explanation: `Projecting ${table} from the dataset with your facets applied.`,
    };
  }

  const search = draft.searchText.trim();
  return {
    headline: search
      ? `Search versions matching “${search}”`
      : `Find up to ${limit} matching versions`,
    subline: search
      ? `Substring search on \`${draft.searchColumn}\``
      : filters !== "no filters"
        ? `Filtered version rows`
        : `Add search text or facets to narrow results`,
    chips: [
      { id: "limit", label: "Limit", value: String(limit), editable: true },
      { id: "search", label: "Search", value: search || "any", editable: true },
      { id: "filter", label: "Filter", value: filterChip, editable: true },
    ],
    explanation:
      kind === "crates"
        ? `Returns version row ids that match your search and facets across ${limit.toLocaleString()} max results.`
        : `Returns matching row ids (top ${limit}).`,
  };
}

const PRESET_HEADLINES: Record<string, string> = {
  "Top crates": "Top crates by total downloads",
  "Most downloaded crates": "Top crates by total downloads",
  "Fastest-growing crates": "Top crates by recent download volume",
  "Serde version adoption": "Serde versions ranked by downloads",
  "Crates by license": "Downloads grouped by SPDX license",
  "Popular MIT crates": "Top MIT-licensed crates by downloads",
  "Crates with yanked versions": "Crates with yanked versions in the index",
  "Mega-download versions": "Individual versions with 10M+ downloads",
  "High-download versions": "Versions with unusually high download counts",
  "Edition × yanked": "Downloads by Rust edition and yanked status",
};

export function headlineFromResultLabel(label: string | null, draft: QueryDraft): string {
  if (!label || label === "Custom query") return describeQuery(draft).headline;
  return PRESET_HEADLINES[label] ?? label;
}
