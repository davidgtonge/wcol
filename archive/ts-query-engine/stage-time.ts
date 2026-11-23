const totals = new Map<string, number>();
const counts = new Map<string, number>();

/** Add milliseconds to a named stage. Parallel callers accumulate, so sums can exceed wall time. */
export function addStage(name: string, elapsedMs: number): void {
  totals.set(name, (totals.get(name) ?? 0) + elapsedMs);
}

export function stageSync<T>(name: string, fn: () => T): T {
  const started = performance.now();
  try {
    return fn();
  } finally {
    addStage(name, performance.now() - started);
  }
}

export function addCount(name: string, amount = 1): void {
  counts.set(name, (counts.get(name) ?? 0) + amount);
}

export function resetStages(): void {
  totals.clear();
  counts.clear();
}

export function stageReport(): { ms: Record<string, number>; counts: Record<string, number> } {
  return {
    ms: Object.fromEntries([...totals.entries()].map(([name, ms]) => [name, Math.round(ms * 10) / 10])),
    counts: Object.fromEntries(counts),
  };
}
