/**
 * Shared byte budget: charge actual buffered chunks, not a pipe's lifetime cap.
 * Waiters are FIFO. A chunk larger than the pool is an error, not a stall.
 */

export type SharedBytePool = {
  readonly maxBytes: number;
  readonly used: number;
  readonly peakUsed: number;
  /** Alias of {@link peakUsed} for demux tests. */
  readonly peak: number;
  tryAcquire(bytes: number): boolean;
  acquire(bytes: number): Promise<void>;
  release(bytes: number): void;
  /** Wake when used may have dropped (non-acquiring wait). */
  wait(): Promise<void>;
  abort(reason?: unknown): void;
};

export function createSharedBytePool(maxBytes: number): SharedBytePool {
  if (maxBytes < 1) throw new Error("byte pool must be positive");
  let used = 0;
  let peakUsed = 0;
  const waiters: Array<() => void> = [];
  let aborted: unknown = null;

  const wake = () => {
    const pending = waiters.splice(0);
    for (const resolve of pending) resolve();
  };

  const tryAcquire = (bytes: number): boolean => {
    if (bytes < 0) throw new Error("byte pool acquire must be non-negative");
    if (bytes === 0) return true;
    if (bytes > maxBytes) {
      throw new Error(`chunk is ${bytes} bytes and the byte pool is ${maxBytes}`);
    }
    if (aborted) throw aborted;
    if (used + bytes > maxBytes) return false;
    used += bytes;
    if (used > peakUsed) peakUsed = used;
    return true;
  };

  return {
    maxBytes,
    get used() {
      return used;
    },
    get peakUsed() {
      return peakUsed;
    },
    get peak() {
      return peakUsed;
    },
    tryAcquire,
    async acquire(bytes: number) {
      for (;;) {
        if (aborted) throw aborted;
        if (tryAcquire(bytes)) return;
        await new Promise<void>((resolve) => waiters.push(resolve));
      }
    },
    release(bytes: number) {
      if (bytes <= 0) return;
      used = Math.max(0, used - bytes);
      wake();
    },
    wait() {
      return new Promise<void>((resolve) => waiters.push(resolve));
    },
    abort(reason?: unknown) {
      aborted ??= reason ?? new Error("byte pool aborted");
      wake();
    },
  };
}

