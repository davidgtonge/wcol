/**
 * Numbers as people read them, for the console, its findings and the coordinator's
 * reducer reasons. Pure: no DOM, no runtime imports.
 */

function scaled(value: number, base: number, units: readonly string[], digitsBelow: number): string {
  let n = value;
  let unit = 0;
  while (n >= base && unit < units.length - 1) {
    n /= base;
    unit++;
  }
  return `${n >= digitsBelow || unit === 0 ? Math.round(n) : n.toFixed(1)} ${units[unit]}`;
}

/** Decimal units (KB = 1000 B), as the console shows data sizes. */
export const bytes = (value: number): string => scaled(value, 1000, ["B", "KB", "MB", "GB", "TB"], 100);

/** Binary units (KiB = 1024 B), as memory budgets are set. */
export const binaryBytes = (value: number): string => scaled(value, 1024, ["B", "KiB", "MiB", "GiB", "TiB"], 10);

export const seconds = (ms: number): string => `${(ms / 1000).toFixed(1)} s`;

export const percent = (part: number, whole: number): string => `${Math.round((100 * part) / Math.max(whole, 1))}%`;

export function median(values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length === 0 ? 0 : (sorted[Math.floor(sorted.length / 2)] ?? 0);
}

/** `s3`, `r1` or `finalizer`: a task as the console names it. */
export const taskName = (task: { role: string; index: number }): string =>
  task.role === "scan" ? `s${task.index}` : task.role === "reducer" ? `r${task.index}` : "finalizer";
