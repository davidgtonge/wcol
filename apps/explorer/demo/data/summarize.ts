import type { QueryResult, WcolFile } from "../wcol-query.ts";
import type { ChartHint, ChartItem, QuerySummary, ResultView } from "../arch/types.ts";
import { DEFAULT_TOP_K } from "../query/constants.ts";
import { resolveGroupKey, resolveProjectionCell } from "./resolve-values.ts";

function formatBigInt(value: number | bigint): string {
  if (typeof value === "bigint") return value.toLocaleString();
  return Number(value).toLocaleString();
}

async function formatProjectionSample(
  file: WcolFile,
  result: QueryResult,
  topK = DEFAULT_TOP_K
): Promise<ResultView> {
  const proj = result.projection!;
  const rows = Math.min(topK, result.rows.length);
  const columns = proj.columns.map((c) => c.name);
  const tableRows: Record<string, string | number | boolean | null>[] = [];

  for (let r = 0; r < rows; r += 1) {
    const row: Record<string, string | number | boolean | null> = {};
    for (let c = 0; c < proj.columns.length; c += 1) {
      const meta = proj.columns[c];
      const col = proj.data[c];
      if (col.nulls[r] === 0) {
        row[meta.name] = null;
        continue;
      }
      row[meta.name] = await resolveProjectionCell(file, meta, col, r);
    }
    tableRows.push(row);
  }

  return {
    kind: "table",
    title: "Rows",
    columns,
    rows: tableRows,
  };
}

async function formatGroupResult(
  file: WcolFile,
  result: QueryResult,
  chartHint: ChartHint | undefined,
  topK: number
): Promise<ResultView> {
  const g = result.groups!;
  const n = g.keys.length;
  const items: ChartItem[] = [];

  const sums: { i: number; sum: number }[] = [];
  for (let i = 0; i < n; i += 1) {
    sums.push({ i, sum: g.values[i]?.[0]?.sum ?? 0 });
  }
  sums.sort((a, b) => b.sum - a.sum);
  const show = Math.min(topK, chartHint === "bar-v" ? 14 : 16);
  const topIndices = sums.slice(0, show);

  for (const { i, sum } of topIndices) {
    const label = await resolveGroupKey(file, g.keyInfo?.[0], g.keys[i]);
    const secondary = g.keys2
      ? await resolveGroupKey(file, g.keyInfo?.[1], g.keys2[i])
      : undefined;
    items.push({ label, value: sum, secondary });
  }
  const top = items;

  if (chartHint === "grouped" && g.keys2) {
    const byEdition = new Map<string, Map<string, number>>();
    for (const item of items) {
      const edition = item.label;
      const yanked = item.secondary ?? "?";
      if (!byEdition.has(edition)) byEdition.set(edition, new Map());
      byEdition.get(edition)!.set(yanked, item.value);
    }
    const groups = [...byEdition.keys()].slice(0, 8);
    const series = [
      { name: "Not yanked", values: groups.map((g) => byEdition.get(g)?.get("0") ?? byEdition.get(g)?.get("false") ?? 0) },
      { name: "Yanked", values: groups.map((g) => byEdition.get(g)?.get("1") ?? byEdition.get(g)?.get("true") ?? 0) },
    ];
    return {
      kind: "grouped-bar",
      title: "Grouped breakdown",
      subtitle: `${n.toLocaleString()} groups · top ${groups.length}`,
      groups,
      series,
      valueLabel: "sum",
    };
  }

  if (chartHint === "bar-v") {
    return {
      kind: "bar-v",
      title: "By group",
      subtitle: `${n.toLocaleString()} groups · top ${top.length}`,
      items: top,
      valueLabel: "sum",
    };
  }

  const title =
    g.keyInfo?.length === 1 && items.length
      ? `Top ${top.length} by total downloads`
      : "Top groups";
  return {
    kind: "bar-h",
    title,
    subtitle: `${n.toLocaleString()} groups · ranked by sum of downloads`,
    items: top,
    valueLabel: "Downloads",
  };
}

function inferChartHint(result: QueryResult, explicit?: ChartHint): ChartHint | undefined {
  if (explicit) return explicit;
  if (result.projection) return "table";
  if (result.groups) {
    const g = result.groups;
    if (g.keys2?.length) return "grouped";
    return "bar-h";
  }
  return "rows";
}

export async function summarizeResult(
  file: WcolFile,
  result: QueryResult,
  meta: { label: string; chartHint?: ChartHint; topK?: number; rowsScanned: number },
  timingMs: number,
  workers: number
): Promise<QuerySummary> {
  const chartHint = inferChartHint(result, meta.chartHint);
  const topK = meta.topK ?? DEFAULT_TOP_K;
  const resultCount = result.groups?.keys.length ?? result.rows.length;
  let view: ResultView;

  if (result.projection) {
    view = await formatProjectionSample(file, result, topK);
  } else if (result.groups) {
    view = await formatGroupResult(file, result, chartHint, topK);
  } else {
    const rowIds = result.rows.slice(0, topK).map((r) =>
      typeof r === "bigint" ? r.toString() : String(r)
    );
    view = {
      kind: "rows",
      title: "Matching rows",
      rowIds,
      rowCount: resultCount,
    };
  }

  return {
    label: meta.label,
    chartHint,
    timingMs,
    workers,
    rowsScanned: meta.rowsScanned,
    resultCount,
    view,
    aggregates: Object.keys(result.aggregates).length ? result.aggregates : undefined,
  };
}

export async function detectDatasetKind(
  file: WcolFile
): Promise<"crates" | "dependencies" | "categories" | "maintainers" | "trends" | "hits"> {
  const names = new Set<string>();
  for (let colId = 0; colId < file.header.ncols; colId += 1) {
    names.add(await file.getColumnName(colId));
  }
  if (names.has("dep_crate_name") && names.has("parent_crate_name")) return "dependencies";
  if (names.has("category_slug") && names.has("category_name")) return "categories";
  if (names.has("owner_login") && names.has("owner_kind")) return "maintainers";
  if (names.has("crate_name") && names.has("downloads") && !names.has("date")) return "trends";
  if (names.has("version") && names.has("downloads") && !names.has("crate_name")) return "trends";
  if (names.has("date") && names.has("version_id") && !names.has("license")) return "trends";
  if (names.has("CounterID") || names.has("EventDate")) return "hits";
  if (names.has("crate_name") || names.has("license")) return "crates";
  return "crates";
}

export async function loadSchema(file: WcolFile, maxCols = 512) {
  const h = file.header;
  const limit = Math.min(h.ncols, maxCols);
  const schema = [];
  const columnNames: string[] = [];
  for (let colId = 0; colId < limit; colId += 1) {
    const info = await file.getColumnInfo(colId);
    const name = await file.getColumnName(colId);
    columnNames.push(name);
    if (colId < 32) {
      schema.push({ id: colId, name, physicalType: String(info.physicalType) });
    }
  }
  if (h.ncols > 32) {
    schema.push({ id: -1, name: `… +${h.ncols - 32} more columns`, physicalType: "" });
  }
  return { schema, columnNames, truncated: h.ncols > limit, totalCols: h.ncols };
}
