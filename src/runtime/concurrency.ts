/** Run at most `max` tasks at once; the rest wait in arrival order. The one limiter for host work. */
export function createLimiter(max: number): <T>(run: () => Promise<T>) => Promise<T> {
  if (!Number.isInteger(max) || max < 1) throw new RangeError(`concurrency limit must be a positive integer (got ${max})`);
  let active = 0;
  const waiters: Array<() => void> = [];
  return async function limit<T>(run: () => Promise<T>): Promise<T> {
    while (active >= max) {
      await new Promise<void>((resolve) => waiters.push(resolve));
    }
    active += 1;
    try {
      return await run();
    } finally {
      // Release in `finally` so a failed task never strands its permit.
      active -= 1;
      waiters.shift()?.();
    }
  };
}
