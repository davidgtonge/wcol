/**
 * Runs kept in this browser, to compare settings: a time-against-cost scatter (the current
 * query's runs in full colour) and a table to reopen one. Storage may be unavailable; the
 * page works without it.
 */
import type { LiveJobView } from "../src/coordinator/live.ts";
import { escape, seconds, usd } from "./format.ts";

export type Run = {
  jobId: string;
  at: number;
  sql: string;
  label: string;
  runtime: string;
  scans: number;
  reducers: number;
  elapsedMs: number;
  usd: number;
  athenaUsd: number;
  readBytes: number;
  state: string;
};

const KEY = "wcol-console-runs";
const KEEP = 60;

export function loadRuns(): Run[] {
  try {
    return (JSON.parse(localStorage.getItem(KEY) ?? "[]") as Run[]).filter((run) => typeof run.jobId === "string");
  } catch {
    return [];
  }
}

export function recordRun(view: LiveJobView, sql: string, label: string): Run[] {
  const runs = loadRuns().filter((run) => run.jobId !== view.jobId);
  runs.unshift({
    jobId: view.jobId,
    at: Date.now(),
    sql,
    label,
    runtime: view.totals.reducers === 0 ? "no reducers" : view.reducers.runtime === "container" ? `containers ${view.reducers.instance ?? ""}`.trim() : "Durable Objects",
    scans: view.totals.scans,
    reducers: view.totals.reducers,
    elapsedMs: view.elapsedMs,
    usd: view.cost.usd,
    athenaUsd: view.cost.athenaUsd,
    readBytes: view.totals.sourceReadBytes,
    state: view.state,
  });
  const kept = runs.slice(0, KEEP);
  try {
    localStorage.setItem(KEY, JSON.stringify(kept));
  } catch {
    // Not kept beyond this page.
  }
  return kept;
}

const RUNTIME_CLASS = (runtime: string) => (runtime.startsWith("containers") ? "c" : runtime === "Durable Objects" ? "d" : "g");

export function renderHistory(target: HTMLElement, runs: readonly Run[], sql: string, onOpen: (jobId: string) => void): void {
  if (runs.length === 0) {
    target.innerHTML = `<p class="muted">Runs you make here are kept in this browser to compare.</p>`;
    return;
  }
  const W = 320;
  const H = 150;
  const pad = 26;
  const maxMs = Math.max(...runs.map((run) => run.elapsedMs), 1);
  const maxUsd = Math.max(...runs.map((run) => run.usd), 1e-9);
  const dots = runs
    .map((run) => {
      const x = pad + ((W - pad - 8) * run.elapsedMs) / maxMs;
      const y = H - pad - ((H - pad - 8) * run.usd) / maxUsd;
      const same = run.sql === sql ? "" : " other";
      return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4" class="dot ${RUNTIME_CLASS(run.runtime)}${same}"><title>${escape(`${run.label} · ${run.runtime} · ${run.scans}/${run.reducers} · ${seconds(run.elapsedMs)} · ${usd(run.usd)}`)}</title></circle>`;
    })
    .join("");
  const scatter = `<svg viewBox="0 0 ${W} ${H}" class="scatter" role="img" aria-label="Runs: time against cost">
<line x1="${pad}" y1="${H - pad}" x2="${W - 4}" y2="${H - pad}" class="axis"/><line x1="${pad}" y1="4" x2="${pad}" y2="${H - pad}" class="axis"/>
<text x="${W - 4}" y="${H - 8}" text-anchor="end" class="axis-label">time → ${escape(seconds(maxMs))}</text>
<text x="${pad + 4}" y="12" class="axis-label">cost ↑ ${escape(usd(maxUsd))}</text>${dots}</svg>
<p class="legend small"><span class="key c"></span>containers <span class="key d"></span>Durable Objects <span class="key g"></span>no reducers · faded: other queries</p>`;
  const rows = runs
    .slice(0, 12)
    .map(
      (run) =>
        `<tr data-job="${escape(run.jobId)}" class="${run.sql === sql ? "" : "muted"}"><td>${escape(run.label)}</td><td>${escape(run.runtime)}<br><span class="muted">${run.scans} scans · ${run.reducers} reducers</span></td><td class="num">${seconds(run.elapsedMs)}</td><td class="num">${usd(run.usd)}</td></tr>`,
    )
    .join("");
  target.innerHTML = `${scatter}<table class="runs"><thead><tr><th>Query</th><th>Settings</th><th class="num">Time</th><th class="num">Cost</th></tr></thead><tbody>${rows}</tbody></table>`;
  target.querySelectorAll<HTMLTableRowElement>("tr[data-job]").forEach((row) => {
    row.addEventListener("click", () => onOpen(row.dataset.job ?? ""));
  });
}
