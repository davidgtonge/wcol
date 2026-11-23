/**
 * Ingress: concurrent established pipes (slots) plus an optional shared byte pool.
 * Opening permits cover only the wait for response headers.
 */

import { createSharedBytePool, type SharedBytePool } from "./shared-byte-pool.ts";

export const DEFAULT_EDGE_BYTES = 256 * 1024;
export const DEFAULT_OPENING_CONCURRENCY = 6;
export const DEFAULT_MAX_ACTIVE_INGRESS = 8;
export const DEFAULT_MAX_ACTIVE_SCANS = 16;
/** Concurrent outstanding partition writes per scan, not open destination streams. */
export const DEFAULT_MAX_ACTIVE_EGRESS = 32;
export const DEFAULT_INGRESS_BYTES = 2 * 1024 * 1024;
export const DEFAULT_DEMUX_BYTES = DEFAULT_INGRESS_BYTES;

export type OpeningLimiter = <T>(run: () => Promise<T>) => Promise<T>;

/**
 * Caps in-flight RPC/stream establishment. Default 6 is a burst smoother, not a
 * Cloudflare Durable Object RPC hard cap — override with `WCOL_DISPATCH_CONCURRENCY`.
 */
export function createOpeningLimiter(max = DEFAULT_OPENING_CONCURRENCY): OpeningLimiter {
  if (max == null || !Number.isFinite(max)) {
    max = DEFAULT_OPENING_CONCURRENCY;
  }
  if (max < 1) throw new Error("opening concurrency must be >= 1");
  let active = 0;
  const waiters: (() => void)[] = [];
  const wake = () => {
    const pending = waiters.splice(0);
    for (const resolve of pending) resolve();
  };
  return async function limit<T>(run: () => Promise<T>): Promise<T> {
    while (active >= max) {
      await new Promise<void>((resolve) => waiters.push(resolve));
    }
    active += 1;
    try {
      return await run();
    } finally {
      active -= 1;
      wake();
    }
  };
}

export type IngressSchedulerOptions = {
  maxActive?: number;
  /** Shared incoming buffer budget. Slot reservation does not charge this. */
  pool?: SharedBytePool;
  maxBytes?: number;
  /**
   * Test-only lifetime reservation slice. Production omits this: opening a
   * port takes a concurrency slot only.
   */
  edgeBytes?: number;
  legacyReservation?: boolean;
};

/**
 * Concurrent pipe slots for one consumer. Byte accounting lives on
 * {@link SharedBytePool} (actual buffered chunks), except `legacyReservation`.
 */
export class IngressScheduler {
  readonly maxActive: number;
  readonly maxBytes: number;
  readonly edgeBytes: number;
  readonly pool: SharedBytePool | undefined;
  readonly legacyReservation: boolean;
  peakActive = 0;
  peakBytes = 0;
  private active = 0;
  private bytes = 0;
  private waiters: (() => void)[] = [];

  constructor(options: IngressSchedulerOptions = {}) {
    this.maxActive = options.maxActive ?? DEFAULT_MAX_ACTIVE_INGRESS;
    this.edgeBytes = options.edgeBytes ?? DEFAULT_EDGE_BYTES;
    this.legacyReservation = options.legacyReservation === true || options.edgeBytes != null;
    this.pool =
      options.pool ??
      (this.legacyReservation ? undefined : createSharedBytePool(options.maxBytes ?? DEFAULT_INGRESS_BYTES));
    this.maxBytes = this.pool?.maxBytes ?? options.maxBytes ?? DEFAULT_INGRESS_BYTES;
    if (this.maxActive < 1 || this.maxBytes < 1 || this.edgeBytes < 1) {
      throw new Error("ingress budget must be positive");
    }
  }

  /**
   * Wait until this consumer can hold another pipe.
   * Legacy mode returns a reserved byte cap; otherwise 0 (slot only).
   */
  async reserve(): Promise<number> {
    for (;;) {
      if (this.legacyReservation) {
        const room = this.maxBytes - this.bytes;
        const pipe = Math.min(this.edgeBytes, room);
        if (this.active < this.maxActive && pipe >= 1) {
          this.active += 1;
          this.bytes += pipe;
          this.peakActive = Math.max(this.peakActive, this.active);
          this.peakBytes = Math.max(this.peakBytes, this.bytes);
          return pipe;
        }
      } else if (this.active < this.maxActive) {
        this.active += 1;
        this.peakActive = Math.max(this.peakActive, this.active);
        return 0;
      }
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
  }

  release(pipe = 0): void {
    this.active = Math.max(0, this.active - 1);
    if (this.legacyReservation) {
      this.bytes = Math.max(0, this.bytes - pipe);
    }
    const pending = this.waiters.splice(0);
    for (const resolve of pending) resolve();
  }
}

export type OpenedProducer = {
  stream: ReadableStream<Uint8Array>;
  release: () => void | Promise<void>;
};

export type DispatchLimitEnv = {
  WCOL_DISPATCH_CONCURRENCY?: string;
  WCOL_MAX_ACTIVE_INGRESS?: string;
  WCOL_MAX_ACTIVE_SCANS?: string;
  WCOL_MAX_ACTIVE_EGRESS?: string;
  WCOL_INGRESS_BYTES?: string;
  WCOL_DEMUX_BYTES?: string;
};

export type DispatchLimits = {
  openingConcurrency: number;
  maxActiveIngress: number;
  maxActiveScans: number;
  maxActiveEgress: number;
  ingressBytes: number;
  demuxBytes: number;
};

export function dispatchLimitsFromEnv(
  env: DispatchLimitEnv = {},
  override?: Pick<Partial<DispatchLimits>, "maxActiveIngress">,
): DispatchLimits {
  const maxActiveIngress = Number(env.WCOL_MAX_ACTIVE_INGRESS ?? DEFAULT_MAX_ACTIVE_INGRESS);
  const limits: DispatchLimits = {
    openingConcurrency: Number(env.WCOL_DISPATCH_CONCURRENCY ?? DEFAULT_OPENING_CONCURRENCY),
    maxActiveIngress,
    maxActiveScans: Number(env.WCOL_MAX_ACTIVE_SCANS ?? DEFAULT_MAX_ACTIVE_SCANS),
    maxActiveEgress: Number(env.WCOL_MAX_ACTIVE_EGRESS ?? DEFAULT_MAX_ACTIVE_EGRESS),
    ingressBytes: Number(env.WCOL_INGRESS_BYTES ?? DEFAULT_INGRESS_BYTES),
    demuxBytes: Number(env.WCOL_DEMUX_BYTES ?? DEFAULT_DEMUX_BYTES),
  };
  if (override?.maxActiveIngress != null && Number.isFinite(override.maxActiveIngress) && override.maxActiveIngress >= 1) {
    limits.maxActiveIngress = Math.floor(override.maxActiveIngress);
  }
  return limits;
}

/**
 * Test helper: drain producers in edge order. Production fan-in uses
 * {@link PortIngress} with Rust-issued per-port reads. `openAt` reserves
 * ingress itself and releases via the returned callback.
 */
export function multiplexBounded(
  count: number,
  maxActive: number,
  openAt: (index: number) => Promise<OpenedProducer>,
): ReadableStream<Uint8Array> {
  const window = Math.max(1, maxActive);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      void (async () => {
        const pending: Promise<OpenedProducer>[] = [];
        let next = 0;
        let inflight = 0;
        const fill = () => {
          while (inflight < window && next < count) {
            inflight += 1;
            pending.push(openAt(next++));
          }
        };
        try {
          fill();
          while (pending.length > 0) {
            const handle = await pending.shift()!;
            const reader = handle.stream.getReader();
            try {
              for (;;) {
                const chunk = await reader.read();
                if (chunk.done) break;
                if (chunk.value?.byteLength) controller.enqueue(chunk.value);
              }
            } finally {
              reader.releaseLock();
              await handle.release();
              inflight -= 1;
              fill();
            }
          }
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      })();
    },
  });
}
