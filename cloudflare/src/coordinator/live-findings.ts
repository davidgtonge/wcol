/**
 * What a finished job's view says about where its time went, in plain sentences: the checks
 * that explained slow runs by hand (cold starts, stragglers, skew). Pure, from `LiveJobView`.
 */
import type { LiveJobView, LiveTask } from "./live.ts";

export type Finding = { level: "info" | "warn"; text: string };

/** A container start above this share of the job is worth pointing out. */
const COLD_START_SHARE = 0.3;
/** An edge ending this long after the median edge is a straggler. */
const STRAGGLER_MS = 1500;
/** A reducer with this multiple of the median input is skewed. */
const SKEW_RATIO = 1.5;

const seconds = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
const percent = (part: number, whole: number) => `${Math.round((100 * part) / Math.max(whole, 1))}%`;

function median(values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length === 0 ? 0 : (sorted[Math.floor(sorted.length / 2)] ?? 0);
}

function bytes(value: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = value;
  let unit = 0;
  while (n >= 1000 && unit < units.length - 1) {
    n /= 1000;
    unit++;
  }
  return `${n >= 100 || unit === 0 ? Math.round(n) : n.toFixed(1)} ${units[unit]}`;
}

const label = (task: LiveTask) => (task.role === "scan" ? `s${task.index}` : task.role === "reducer" ? `r${task.index}` : "the finalizer");

export function findings(view: LiveJobView): Finding[] {
  if (view.state !== "completed") return [];
  const scans = view.tasks.filter((task) => task.role === "scan");
  const reducers = view.tasks.filter((task) => task.role === "reducer");
  if (scans.length === 0 && reducers.length === 0) {
    return [{ level: "info", text: "Answered from the Parquet footer: no data was read." }];
  }
  return [
    ...readShare(view),
    ...coldStart(view, reducers),
    ...tail(view, scans),
    ...stragglers(view),
    ...skew(reducers),
    ...unevenScans(scans),
    ...(reducers.length === 0
      ? [{ level: "info" as const, text: `No reducers: ${scans.length} scans sent ${bytes(view.totals.shuffleBytes)} of partial results straight to the coordinator.` }]
      : []),
  ];
}

function readShare(view: LiveJobView): Finding[] {
  const { fileBytes, sourceReadBytes } = view.totals;
  if (fileBytes === 0 || sourceReadBytes === 0) return [];
  return [
    {
      level: "info",
      text: `Read ${bytes(sourceReadBytes)} of a ${bytes(fileBytes)} file (${percent(sourceReadBytes, fileBytes)}): only the columns and row groups the query needs.`,
    },
  ];
}

function coldStart(view: LiveJobView, reducers: readonly LiveTask[]): Finding[] {
  const ready = reducers.flatMap((task) => (task.readyAtMs == null ? [] : [task.readyAtMs]));
  if (ready.length === 0) return [];
  const last = Math.max(...ready);
  const share = last / Math.max(view.elapsedMs, 1);
  const what = view.reducers.runtime === "container" ? "containers to start" : "reducers to be prepared";
  return [
    {
      level: share >= COLD_START_SHARE ? "warn" : "info",
      text: `Waiting for ${what} took ${seconds(last)}, ${percent(last, view.elapsedMs)} of the job.`,
    },
  ];
}

/** Time between the last scan's end and the job's: reducers finishing and the finalizer. */
function tail(view: LiveJobView, scans: readonly LiveTask[]): Finding[] {
  const ends = scans.flatMap((task) => (task.endMs == null ? [] : [task.endMs]));
  if (ends.length === 0) return [];
  const lastScan = Math.max(...ends);
  const after = view.elapsedMs - lastScan;
  return [
    {
      level: "info",
      text: `The last scan finished at ${seconds(lastScan)}; the job took another ${seconds(after)} to finish.`,
    },
  ];
}

function stragglers(view: LiveJobView): Finding[] {
  const timed = view.edges.filter((edge) => edge.endMs != null);
  if (timed.length < 4) return [];
  const typical = median(timed.map((edge) => edge.endMs ?? 0));
  const late = timed.filter((edge) => (edge.endMs ?? 0) - typical > STRAGGLER_MS);
  if (late.length === 0) return [];
  const slowest = late.reduce((a, b) => ((b.endMs ?? 0) > (a.endMs ?? 0) ? b : a));
  const byId = new Map(view.tasks.map((task) => [task.id, task] as const));
  const from = byId.get(slowest.from);
  const to = byId.get(slowest.to);
  const name = from && to ? `${label(from)} → ${label(to)}` : "one edge";
  return [
    {
      level: "warn",
      text: `${late.length} of ${timed.length} edges finished more than ${seconds(STRAGGLER_MS)} after the median (${seconds(typical)}); the slowest, ${name}, ended at ${seconds(slowest.endMs ?? 0)}.`,
    },
    ...lateReceivers(late, byId),
  ];
}

/**
 * Late edges that mostly end at a few tasks point at those tasks: they were slow to take
 * input, whichever scan sent it. Named when a fifth of the targets hold most late edges.
 */
function lateReceivers(late: readonly LiveJobView["edges"][number][], byId: ReadonlyMap<number, LiveTask>): Finding[] {
  const counts = new Map<number, number>();
  for (const edge of late) counts.set(edge.to, (counts.get(edge.to) ?? 0) + 1);
  const targets = new Set([...byId.values()].filter((task) => task.role === "reducer").map((task) => task.id));
  const ranked = [...counts].filter(([id]) => targets.has(id)).sort((a, b) => b[1] - a[1]);
  const few = ranked.slice(0, Math.max(1, Math.floor(targets.size / 5)));
  const covered = few.reduce((sum, [, count]) => sum + count, 0);
  if (targets.size < 5 || covered < late.length * 0.6) return [];
  const names = few
    .map(([id]) => byId.get(id))
    .filter((task): task is LiveTask => task !== undefined)
    .sort((a, b) => a.index - b.index)
    .map(label);
  return [
    {
      level: "warn",
      text: `${percent(covered, late.length)} of the late edges ended at ${names.length === 1 ? "reducer" : "reducers"} ${names.join(", ")}: they were slow to take input, whichever scan sent it.`,
    },
  ];
}

function skew(reducers: readonly LiveTask[]): Finding[] {
  if (reducers.length < 2) return [];
  const typical = median(reducers.map((task) => task.bytesIn));
  if (typical === 0) return [];
  const largest = reducers.reduce((a, b) => (b.bytesIn > a.bytesIn ? b : a));
  const ratio = largest.bytesIn / typical;
  return ratio < SKEW_RATIO
    ? []
    : [{ level: "warn", text: `Reducer ${label(largest)} received ${ratio.toFixed(1)}× the median input (${bytes(largest.bytesIn)}): uneven partitions.` }];
}

function unevenScans(scans: readonly LiveTask[]): Finding[] {
  const durations = scans.flatMap((task) => (task.startMs == null || task.endMs == null ? [] : [task.endMs - task.startMs]));
  if (durations.length < 2) return [];
  const typical = median(durations);
  const longest = Math.max(...durations);
  return typical > 0 && longest / typical >= 2
    ? [{ level: "info", text: `The slowest scan took ${seconds(longest)}, ${(longest / typical).toFixed(1)}× the median scan (${seconds(typical)}).` }]
    : [];
}
