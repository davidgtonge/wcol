import { formatCount } from "../charts/format.ts";
import { hint, statPill } from "../ui/classes.ts";

export type TimingInput = {
  timingMs: number;
  rowsScanned: number;
  resultCount?: number;
  workers: number;
  openMs?: number;
  warmMs?: number | null;
  loading?: boolean;
};

type Props = {
  input: TimingInput;
  compact?: boolean;
};

function rowsPerSec(rows: number, ms: number): string {
  if (ms <= 0) return "—";
  const rps = (rows / ms) * 1000;
  if (rps >= 1_000_000) return `${(rps / 1_000_000).toFixed(1)}M rows/s`;
  if (rps >= 1000) return `${(rps / 1000).toFixed(0)}k rows/s`;
  return `${Math.round(rps).toLocaleString()} rows/s`;
}

export function TimingDisplay({ input, compact = false }: Props) {
  const { timingMs, rowsScanned, workers, warmMs, loading } = input;

  if (loading) {
    if (compact) {
      return <p class={`${hint} animate-pulse`}>Scanning…</p>;
    }
    return (
      <div class="rounded-lg border border-blue-500/20 bg-blue-500/5 p-3" aria-busy="true" aria-label="Query running">
        <p class={`${hint} mb-2`}>Scanning columnar chunks…</p>
        <div class="skeleton-bar h-3 w-48 rounded" />
      </div>
    );
  }

  if (compact) {
    const parts = [
      `${timingMs < 10 ? timingMs.toFixed(2) : timingMs.toFixed(1)} ms`,
      `${formatCount(rowsScanned)} rows scanned`,
      warmMs != null ? `warm ${warmMs.toFixed(0)} ms` : null,
    ].filter(Boolean);
    return (
      <details class="text-xs text-slate-500 dark:text-slate-400">
        <summary class="cursor-pointer hover:text-slate-700 dark:hover:text-slate-300">{parts.join(" · ")}</summary>
        <p class="mt-1 pl-3">
          {rowsPerSec(rowsScanned, timingMs)} · {workers} worker{workers === 1 ? "" : "s"}
        </p>
      </details>
    );
  }

  return (
    <div class="rounded-lg border border-slate-200/80 p-3 dark:border-wcol-border">
      <p class={`${hint} mb-1`}>Query stats</p>
      <div class="flex flex-wrap gap-2">
        <span class={statPill}>{timingMs.toFixed(1)} ms</span>
        <span class={statPill}>{formatCount(rowsScanned)} scanned</span>
        <span class={statPill}>{rowsPerSec(rowsScanned, timingMs)}</span>
        <span class={statPill}>
          {workers} worker{workers === 1 ? "" : "s"}
        </span>
        {warmMs != null ? <span class={statPill}>warm {warmMs.toFixed(0)} ms</span> : null}
      </div>
    </div>
  );
}
