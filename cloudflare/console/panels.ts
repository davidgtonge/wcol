/** The console's text panels: totals, findings, data-flow funnel, cost, task detail, result. */
import type { Finding } from "../src/coordinator/live-findings.ts";
import type { LiveJobView, LiveTask } from "../src/coordinator/live.ts";
import { bytes, escape, median, seconds, taskName, usd } from "./format.ts";

export type Result = { columns?: string[]; rows?: unknown[][] };

const stat = (value: string, label: string) => `<div class="stat"><b>${escape(value)}</b><span>${escape(label)}</span></div>`;

export function renderStats(target: HTMLElement, view: LiveJobView): void {
  const ready = view.tasks.flatMap((task) => (task.readyMs == null ? [] : [task.readyMs]));
  const runtime = view.reducers.runtime === "container" ? `containers · ${view.reducers.instance ?? "?"}` : "Durable Objects";
  target.innerHTML = [
    stat(seconds(view.elapsedMs), "wall time"),
    stat(bytes(view.totals.sourceReadBytes), `read from R2 · ${view.totals.sourceReads} reads`),
    stat(bytes(view.totals.shuffleBytes), view.totals.reducers > 0 ? "moved scans → reducers" : "sent scans → coordinator"),
    stat(`${view.totals.scans} / ${view.totals.reducers}`, view.totals.reducers > 0 ? `scans / reducers on ${runtime}` : "scans / reducers"),
    ...(ready.length ? [stat(seconds(median(ready)), "median container start")] : []),
    stat(usd(view.cost.usd), "estimated cost"),
  ].join("");
}

export function renderFindings(target: HTMLElement, found: readonly Finding[]): void {
  target.innerHTML = found
    .map((finding) => `<li class="${finding.level}"><span aria-hidden="true">${finding.level === "warn" ? "▲" : "●"}</span> ${escape(finding.text)}</li>`)
    .join("");
  target.classList.toggle("hidden", found.length === 0);
}

/** File → read → moved → result, as bars to the file's scale. */
export function renderFunnel(target: HTMLElement, view: LiveJobView, rows: number | null): void {
  const file = view.totals.fileBytes;
  if (file === 0) {
    target.innerHTML = `<p class="muted">—</p>`;
    return;
  }
  const steps: [string, number, string][] = [
    ["Parquet file", file, "every column and row group"],
    ["Read from R2", view.totals.sourceReadBytes, "only the columns and row groups needed"],
    [view.totals.reducers > 0 ? "Moved to reducers" : "Sent to the coordinator", view.totals.shuffleBytes, "rows or partial results, uncompressed"],
  ];
  const widest = Math.max(...steps.map(([, value]) => value), 1);
  const bars = steps
    .map(([label, value, why]) => {
      const share = `${((100 * value) / file).toFixed(value / file < 0.1 ? 1 : 0)}% of the file`;
      return `<div class="funnel-row"><span class="funnel-label">${escape(label)}<small>${escape(why)}</small></span><span class="funnel-bar"><i style="width:${Math.max(0.4, (100 * value) / widest)}%"></i></span><span class="funnel-value">${escape(bytes(value))}<small>${escape(share)}</small></span></div>`;
    })
    .join("");
  const result = rows == null ? "" : `<div class="funnel-row"><span class="funnel-label">Result<small>what the query returned</small></span><span class="funnel-bar"></span><span class="funnel-value">${rows} row${rows === 1 ? "" : "s"}</span></div>`;
  target.innerHTML = bars + result;
}

export function renderCost(target: HTMLElement, view: LiveJobView): void {
  const { cost } = view;
  target.innerHTML = `<table>${cost.items
    .map((item) => `<tr><td>${escape(item.label)}<br><span class="muted">${escape(item.detail)}</span></td><td class="num">${usd(item.usd)}</td></tr>`)
    .join("")}<tr class="total"><td>Total</td><td class="num">${usd(cost.usd)}</td></tr>
<tr class="compare"><td>Athena, same bytes scanned<br><span class="muted">$5 per TB, at least 10 MB</span></td><td class="num">${usd(cost.athenaUsd)}</td></tr></table>`;
}

export function renderDetail(target: HTMLElement, view: LiveJobView, id: number | null): void {
  const task = view.tasks.find((candidate) => candidate.id === id);
  if (!task) {
    target.innerHTML = `<p class="muted">Click a lane in the timeline or a cell in the matrix.</p>`;
    return;
  }
  const edges = view.edges.filter((edge) => edge.from === task.id || edge.to === task.id);
  const timed = edges.filter((edge) => edge.endMs != null);
  const slowest = timed.reduce<(typeof timed)[number] | null>((a, b) => (!a || (b.endMs ?? 0) > (a.endMs ?? 0) ? b : a), null);
  const byId = new Map(view.tasks.map((candidate) => [candidate.id, candidate] as const));
  const other = (edge: (typeof edges)[number]) => byId.get(edge.from === task.id ? edge.to : edge.from);
  const rows: [string, string][] = [
    ["Task", `${taskName(task)} · ${task.role} · ${task.state}`],
    ["Data centre", task.colo ?? "not reported yet"],
    ["Runs on", task.runtime === "container" ? `container (${task.instance ?? "?"})` : task.runtime === "node" ? "Durable Object" : "coordinator"],
    ["Started", task.startMs == null ? "—" : seconds(task.startMs)],
    ...(task.readyAtMs != null ? ([["Ready", `${seconds(task.readyAtMs)}${task.readyMs != null ? ` (container start ${seconds(task.readyMs)})` : ""}`]] as [string, string][]) : []),
    ["Ended", task.endMs == null ? "—" : seconds(task.endMs)],
    ...(task.sourceReadBytes ? ([["Read", `${bytes(task.sourceReadBytes)} in ${task.sourceReads} reads`]] as [string, string][]) : []),
    ...(task.bytesOut ? ([["Sent", bytes(task.bytesOut)]] as [string, string][]) : []),
    ...(task.bytesIn ? ([["Received", bytes(task.bytesIn)]] as [string, string][]) : []),
    ["Edges", `${edges.length}${timed.length ? `, done by ${seconds(Math.max(...timed.map((edge) => edge.endMs ?? 0)))}` : ""}`],
    ...(slowest
      ? ([["Slowest edge", `${taskName(other(slowest) ?? task)} · ${bytes(slowest.bytes)} · ${seconds(slowest.startMs ?? 0)} → ${seconds(slowest.endMs ?? 0)}`]] as [string, string][])
      : []),
  ];
  target.innerHTML = `<table>${rows.map(([key, value]) => `<tr><th>${escape(key)}</th><td>${escape(value)}</td></tr>`).join("")}</table>`;
}

export function renderResult(section: HTMLElement, target: HTMLElement, result: Result | null): void {
  section.classList.toggle("hidden", !result?.rows);
  if (!result?.rows) return;
  const head = (result.columns ?? []).map((column) => `<th>${escape(column)}</th>`).join("");
  const rows = result.rows.map((row) => `<tr>${row.map((cell) => `<td class="mono">${escape(String(cell))}</td>`).join("")}</tr>`).join("");
  target.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${rows}</tbody></table>`;
}

export function hoverText(task: LiveTask | null): string {
  if (!task) return "";
  const parts = [taskName(task), task.state, task.colo ?? ""];
  if (task.startMs != null) parts.push(`${seconds(task.startMs)} → ${task.endMs == null ? "…" : seconds(task.endMs)}`);
  if (task.readyMs != null) parts.push(`start ${seconds(task.readyMs)}`);
  return parts.filter(Boolean).join(" · ");
}
