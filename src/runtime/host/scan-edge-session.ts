/**
 * Reducer-side hash ingress slots. Prepare registers identities; accept installs a
 * transferred byte stream; completeScan/seal EOF unregistered slots. Consume-ready
 * fires when the reducer starts waiting on a slot, not when bytes arrive.
 */

function waiter(): { wait: () => Promise<void>; wake: () => void } {
  const waiters: Array<() => void> = [];
  return {
    wait: () => new Promise((resolve) => waiters.push(resolve)),
    wake: () => {
      const pending = waiters.splice(0);
      for (const resolve of pending) resolve();
    },
  };
}

export function emptyByteStream(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.close();
    },
  });
}

export type ScanEdgeSessionArgs = {
  jobId: string;
  executionAttempt: number;
  expectedScans: number;
};

type Slot = {
  accepted: boolean;
  empty: boolean;
  consumeReady: boolean;
  stream: ReadableStream<Uint8Array> | null;
  failed: unknown | null;
  signal: ReturnType<typeof waiter>;
};

export type ReducerIngressSession = {
  readonly jobId: string;
  readonly executionAttempt: number;
  readonly expectedScans: number;
  sealed: boolean;
  acceptScanEdge(scanIndex: number, stream: ReadableStream<Uint8Array>): { accepted: true };
  completeScan(scanIndex: number): void;
  seal(): void;
  /** Scanners may open edges. Does not read a port and does not mean EOF. */
  readyToAccept(): void;
  abort(error: unknown): void;
  waitConsumeReady(scanIndex: number): Promise<void>;
  waitForStream(scanIndex: number): Promise<ReadableStream<Uint8Array>>;
  ingresses(): ReadableStream<Uint8Array>[];
  acceptedScanIndexes(): number[];
  /** Registrations in arrival order. `outstanding` is the count after this accept. */
  admissions(): { scanIndex: number; enteredAt: number; outstanding: number }[];
  /** Scanner finished and never accepted a stream for this slot. */
  producedNothing(scanIndex: number): boolean;
  assertAttempt(jobId: string, executionAttempt: number): void;
};

function requireIndex(scanIndex: number, expectedScans: number): void {
  if (!Number.isInteger(scanIndex) || scanIndex < 0 || scanIndex >= expectedScans) {
    throw new Error(`scan index ${scanIndex} is outside 0..${expectedScans - 1}`);
  }
}

export function createReducerIngressSession(args: ScanEdgeSessionArgs): ReducerIngressSession {
  if (args.expectedScans < 1) throw new Error("expectedScans must be >= 1");
  const slots: Slot[] = Array.from({ length: args.expectedScans }, () => ({
    accepted: false,
    empty: false,
    consumeReady: false,
    stream: null,
    failed: null,
    signal: waiter(),
  }));
  let sealed = false;
  let failed: unknown = null;
  const admissions: { scanIndex: number; enteredAt: number; outstanding: number }[] = [];
  const consumeSignals = slots.map(() => waiter());

  const wake = (slot: Slot) => slot.signal.wake();

  const markConsumeReady = (scanIndex: number) => {
    const slot = slots[scanIndex]!;
    if (slot.consumeReady) return;
    slot.consumeReady = true;
    consumeSignals[scanIndex]!.wake();
  };

  const installed = (slot: Slot): ReadableStream<Uint8Array> | null => {
    if (slot.failed) throw slot.failed;
    if (failed) throw failed;
    if (slot.stream) return slot.stream;
    if (slot.empty) return emptyByteStream();
    return null;
  };

  const session: ReducerIngressSession = {
    jobId: args.jobId,
    executionAttempt: args.executionAttempt,
    expectedScans: args.expectedScans,
    get sealed() {
      return sealed;
    },
    assertAttempt(jobId: string, executionAttempt: number) {
      if (jobId !== args.jobId || executionAttempt !== args.executionAttempt) {
        throw new Error(
          `stale hash attempt ${jobId}@${executionAttempt}; have ${args.jobId}@${args.executionAttempt}`,
        );
      }
    },
    acceptScanEdge(scanIndex, stream) {
      requireIndex(scanIndex, args.expectedScans);
      if (failed) throw failed;
      if (sealed) throw new Error("acceptScanEdge after seal");
      const slot = slots[scanIndex]!;
      if (slot.failed) throw slot.failed;
      if (slot.accepted) throw new Error(`duplicate acceptScanEdge for scan ${scanIndex}`);
      if (slot.empty) throw new Error(`acceptScanEdge for completed scan ${scanIndex}`);
      slot.accepted = true;
      slot.stream = stream;
      admissions.push({
        scanIndex,
        enteredAt: Date.now(),
        outstanding: slots.reduce((count, item) => count + (item.accepted ? 1 : 0), 0),
      });
      wake(slot);
      return { accepted: true as const };
    },
    completeScan(scanIndex) {
      requireIndex(scanIndex, args.expectedScans);
      if (failed) throw failed;
      const slot = slots[scanIndex]!;
      if (slot.accepted || slot.empty) return;
      slot.empty = true;
      wake(slot);
    },
    seal() {
      if (failed) throw failed;
      sealed = true;
      for (const slot of slots) {
        if (!slot.accepted && !slot.empty) {
          slot.empty = true;
          wake(slot);
        }
      }
    },
    readyToAccept() {
      for (let index = 0; index < slots.length; index++) markConsumeReady(index);
    },
    abort(error) {
      failed ??= error;
      for (let index = 0; index < slots.length; index++) {
        const slot = slots[index]!;
        slot.failed ??= error;
        void slot.stream?.cancel(error).catch(() => undefined);
        wake(slot);
        consumeSignals[index]!.wake();
      }
    },
    async waitConsumeReady(scanIndex) {
      requireIndex(scanIndex, args.expectedScans);
      const slot = slots[scanIndex]!;
      while (!slot.consumeReady) {
        if (failed) throw failed;
        if (slot.failed) throw slot.failed;
        await consumeSignals[scanIndex]!.wait();
      }
      if (failed) throw failed;
      if (slot.failed) throw slot.failed;
    },
    async waitForStream(scanIndex) {
      requireIndex(scanIndex, args.expectedScans);
      markConsumeReady(scanIndex);
      const slot = slots[scanIndex]!;
      for (;;) {
        const stream = installed(slot);
        if (stream) return stream;
        await slot.signal.wait();
      }
    },
    ingresses() {
      return slots.map((_, scanIndex) => {
        let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
        return new ReadableStream<Uint8Array>(
          {
            async pull(controller) {
              markConsumeReady(scanIndex);
              const inner = await session.waitForStream(scanIndex);
              reader ??= inner.getReader();
              const chunk = await reader.read();
              if (chunk.done) {
                controller.close();
                return;
              }
              if (chunk.value && chunk.value.byteLength > 0) controller.enqueue(chunk.value);
            },
            cancel(reason) {
              markConsumeReady(scanIndex);
              return reader?.cancel(reason).catch(() => undefined) ?? Promise.resolve();
            },
          },
          { highWaterMark: 0 },
        );
      });
    },
    acceptedScanIndexes() {
      return slots.flatMap((slot, index) => (slot.accepted ? [index] : []));
    },
    admissions() {
      return admissions.slice();
    },
    producedNothing(scanIndex) {
      requireIndex(scanIndex, args.expectedScans);
      const slot = slots[scanIndex]!;
      return slot.empty && !slot.accepted;
    },
  };
  return session;
}
