/**
 * Demand-driven fan-in: at most one pending read and one ready chunk per active
 * port. The engine asks for any ready port; a slow sibling does not stall a
 * producer that already has bytes.
 */

import type { OpenedProducer } from "./dispatch-limits.ts";
import { OwnedByteReader } from "../../src/runtime/host/ports/readable.ts";
import type { SharedBytePool } from "./shared-byte-pool.ts";

export type ReadyInput = {
  port: number;
  chunk?: Uint8Array;
  eof?: true;
};

type PortState = {
  owned: OwnedByteReader | null;
  release: (() => void | Promise<void>) | null;
  ready: ReadyInput | null;
  done: boolean;
  close: Promise<void> | null;
};

export class ReadyPortScheduler {
  private nextOpen = 0;
  private inflight = 0;
  private readonly window: number;
  private readonly ports = new Map<number, PortState>();
  private readonly readyQueue: number[] = [];
  private readonly starting = new Set<Promise<void>>();
  private readonly closing = new Set<Promise<void>>();
  private waiters: Array<() => void> = [];
  private failure: unknown = null;
  private aborted = false;
  private shutdown: Promise<void> | null = null;

  constructor(
    readonly count: number,
    maxActive: number,
    private readonly openAt: (index: number) => Promise<OpenedProducer>,
    private readonly pool?: SharedBytePool,
  ) {
    if (count < 0) throw new Error("ready-port count must be >= 0");
    this.window = Math.max(1, maxActive);
  }

  /** Bytes held in ready chunks (not yet delivered to the engine). */
  get retainedBytes(): number {
    let n = 0;
    for (const state of this.ports.values()) {
      n += state.ready?.chunk?.byteLength ?? 0;
    }
    return n;
  }

  /** True while any port still holds a reader lock. For tests. */
  get locksHeld(): number {
    let n = 0;
    for (const state of this.ports.values()) {
      if (state.owned?.locked) n += 1;
    }
    return n;
  }

  /** True while any port has an unsettled `read()`. For tests. */
  get outstandingReads(): number {
    let n = 0;
    for (const state of this.ports.values()) {
      if (state.owned?.pending) n += 1;
    }
    return n;
  }

  /** Next ready chunk or EOF. FIFO among ports that already have input. */
  async nextReady(): Promise<ReadyInput> {
    this.fill();
    for (;;) {
      if (this.failure) throw this.failure;
      if (this.aborted) throw new Error("ready-port scheduler aborted");
      const port = this.readyQueue.shift();
      if (port !== undefined) {
        const state = this.ports.get(port);
        if (!state?.ready) continue;
        const input = state.ready;
        state.ready = null;
        if (input.chunk && this.pool) this.pool.release(input.chunk.byteLength);
        if (input.eof) this.enqueueClose(port);
        else this.armRead(port);
        return input;
      }
      if (this.allDelivered()) {
        throw new Error("nextReady after every input port completed");
      }
      await this.waitNotify();
      this.fill();
    }
  }

  /**
   * Common cleanup for success, failure, and cancellation. Stops new reads,
   * cancels open readers, waits for outstanding reads, then releases locks.
   * `reason` is recorded only when no execution error is already stored.
   */
  async abort(reason?: unknown): Promise<void> {
    if (this.shutdown) return this.shutdown;
    this.aborted = true;
    if (reason != null) this.failure ??= reason;
    this.wake();
    this.shutdown = this.closeAll();
    await this.shutdown;
  }

  private async closeAll(): Promise<void> {
    await Promise.allSettled([...this.starting]);
    for (const port of [...this.ports.keys()]) this.enqueueClose(port);
    await Promise.allSettled([...this.closing]);
  }

  private allDelivered(): boolean {
    if (this.nextOpen < this.count || this.inflight > 0 || this.readyQueue.length > 0) {
      return false;
    }
    for (const state of this.ports.values()) {
      if (!state.done || state.ready) return false;
    }
    return this.count === 0 || this.ports.size === this.count;
  }

  private fill(): void {
    while (!this.aborted && this.failure == null && this.inflight < this.window && this.nextOpen < this.count) {
      const port = this.nextOpen;
      this.nextOpen += 1;
      this.inflight += 1;
      this.startPort(port);
    }
  }

  private startPort(port: number): void {
    const run = this.openPort(port);
    this.starting.add(run);
    void run.finally(() => this.starting.delete(run));
  }

  private async openPort(port: number): Promise<void> {
    try {
      const opened = await this.openAt(port);
      if (this.aborted || this.failure) {
        this.inflight = Math.max(0, this.inflight - 1);
        const owned = new OwnedByteReader(opened.stream);
        await owned.close(this.failure);
        await Promise.resolve(opened.release()).catch(() => undefined);
        this.wake();
        return;
      }
      this.ports.set(port, {
        owned: new OwnedByteReader(opened.stream),
        release: opened.release,
        ready: null,
        done: false,
        close: null,
      });
      this.armRead(port);
    } catch (error) {
      this.failure ??= error;
      this.inflight = Math.max(0, this.inflight - 1);
      this.wake();
    }
  }

  private armRead(port: number): void {
    const state = this.ports.get(port);
    if (!state?.owned || state.owned.pending || state.done || state.ready || this.aborted) return;
    void state.owned.read().then(
      async (chunk) => {
        state.owned?.consume();
        if (this.aborted || state.done) return;
        if (chunk.done) {
          state.ready = { port, eof: true };
        } else {
          const value = chunk.value ?? new Uint8Array();
          if (value.byteLength === 0) {
            this.armRead(port);
            return;
          }
          if (this.pool && value.byteLength > 0) {
            try {
              await this.pool.acquire(value.byteLength);
            } catch (error) {
              this.failure ??= error;
              this.wake();
              return;
            }
            if (this.aborted || state.done) {
              this.pool.release(value.byteLength);
              return;
            }
          }
          state.ready = { port, chunk: value };
        }
        this.readyQueue.push(port);
        this.wake();
      },
      (error) => {
        state.owned?.consume();
        this.failure ??= error;
        this.wake();
      },
    );
  }

  private enqueueClose(port: number): void {
    const run = this.closePort(port);
    this.closing.add(run);
    void run.finally(() => this.closing.delete(run));
  }

  private async closePort(port: number): Promise<void> {
    const state = this.ports.get(port);
    if (!state) return;
    if (state.close) return state.close;
    state.done = true;
    if (state.ready?.chunk && this.pool) this.pool.release(state.ready.chunk.byteLength);
    state.ready = null;
    const run = this.finishPort(state);
    state.close = run;
    await run;
  }

  private async finishPort(state: PortState): Promise<void> {
    const owned = state.owned;
    const release = state.release;
    state.release = null;
    try {
      await owned?.close(this.failure);
    } catch {
      // close() does not throw; keep the original execution error if it did.
    }
    this.inflight = Math.max(0, this.inflight - 1);
    try {
      await release?.();
    } catch (error) {
      this.failure ??= error;
    }
    this.fill();
    this.wake();
  }

  private waitNotify(): Promise<void> {
    return new Promise((resolve) => {
      this.waiters.push(resolve);
      if (this.failure || this.aborted || this.readyQueue.length > 0) this.wake();
    });
  }

  private wake(): void {
    const pending = this.waiters.splice(0);
    for (const resolve of pending) resolve();
  }
}
