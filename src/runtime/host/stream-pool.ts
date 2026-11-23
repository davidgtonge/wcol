import type { HostBytes } from "./host-bytes.ts";
import type { WcolStreamWasm } from "./wasm-stream.ts";
import { readWasmLastError } from "./wasm.ts";
import { pumpReadableToWriter } from "./pool-streams.ts";

const MAX_INPUT_PORTS = 128;
const MAX_OUTPUT_PORTS = 128;
const STEPS_PER_ADVANCE = 64;

const STEP_PROGRESS = 1;
const STEP_FINISHED = 4;
const STEP_FAILED = 8;
const STEP_RESIDENT_READY = 32;
const STEP_SPILL_EXEC = 16;

/** May answer at once or later: the pool wraps every answer in a promise. */
export type ReadAtCallback = (
  source: number,
  offset: bigint,
  length: number,
) => Uint8Array | Promise<Uint8Array>;

export interface CreateStreamPoolOptions {
  inputs: number;
  /** Slot size in KiB (each ingress mailbox). */
  inputSize: number;
  outputs: number;
  /** Slot size in KiB (each egress mailbox). */
  outputSize: number;
  wasm: WcolStreamWasm;
  /** Echo driver for tests; omit when `graph` is set. */
  echo?: boolean;
  /**
   * `budgetShare`: tasks sharing this isolate's memory, this one included (1 when alone).
   * A hash reducer flushes its groups at that fraction of the planned budget.
   */
  graph?: { program: Uint8Array; scratchBytes: number; budgetShare?: number };
  /** Input bytes leave the host's `edges` count here, once copied into Wasm. */
  hostBytes?: HostBytes;
  residentMemoryLimitBytes?: number;
  readAt?: ReadAtCallback;
  /** Called when Rust returns `STEP_SPILL_EXEC` (R2 sorted-run spill). */
  onSpillExec?: (handle: number) => Promise<void>;
  /** Optional macrotask yield between advance batches. */
  yieldToEventLoop?: () => Promise<void>;
}

export interface StreamPool {
  readonly resident: boolean;
  /** Initial resident load completed. The graph remains open. */
  readonly ready: Promise<void>;
  response(): Uint8Array;
  request(kind: number, bytes: Uint8Array): Promise<Uint8Array>;
  inputStreams: WritableStream<Uint8Array>[];
  outputStreams: ReadableStream<Uint8Array>[];
  /** Start pumping `stream` into ingress port `port` (BYOB when supported). Idempotent per port until done. */
  attachInput(port: number, stream: ReadableStream<Uint8Array>): void;
  /** Mark a reducer scan input absent (not EOF). No-op if a stream is already attached. */
  sealInput(port: number): void;
  abort(reason?: unknown): void;
  finished: Promise<void>;
  /** Epoch ms when every input port had ended or been sealed; undefined until then. */
  inputsEndedAt(): number | undefined;
  /** Drain advance chain and release the Wasm handle (await before starting another job on the same isolate). */
  close(): Promise<void>;
  /** Wake the Rust step loop (eg. after external input/output I/O). */
  wake(): void;
  stats(): PoolStats;
}

/**
 * Where a pool's wall time went. Step and completion time is sync Wasm work;
 * `readBusyMs` is wall time with at least one host read in flight. Workers clocks stand
 * still during sync Wasm, so CPU time is read as wall time minus the idle split: a host
 * read in flight, output not yet taken by its consumer, or neither (waiting on input).
 */
export type PoolProfile = {
  idleReadMs: number;
  idleOutputMs: number;
  idleInputMs: number;
  /** Output ports still holding bytes, summed over idle exits on output. */
  idleOutputPorts: number;
  idleOutputExits: number;
  outputBytes: number;
  stepMs: number;
  completeMs: number;
  reads: number;
  readBytes: number;
  readMs: number;
  readBusyMs: number;
  maxInFlight: number;
  firstReadAt: number;
  lastReadAt: number;
};

/** Step counts and the time profile, for the task and scan profile logs. */
export type PoolStats = {
  readonly steps: number;
  readonly maxStepMs: number;
  /** Longest synchronous stretch between event-loop yields. */
  readonly maxSyncMs: number;
  readonly profile: PoolProfile;
};

function requireSp(wasm: WcolStreamWasm): NonNullable<WcolStreamWasm["wcs_sp_open_echo"]> {
  if (!wasm.wcs_sp_open_echo || !wasm.wcs_sp_step || !wasm.wcs_sp_close || !wasm.wcs_sp_take_read_at) {
    throw new Error("wasm missing stream pool exports (rebuild wcol_node.wasm)");
  }
  return wasm.wcs_sp_open_echo;
}

function viewAt(memory: WebAssembly.Memory, ptr: number, len: number): Uint8Array {
  return new Uint8Array(memory.buffer, ptr, len);
}

type WaitEntry = {
  when: () => boolean;
  resolve: () => void;
  reject: (error: unknown) => void;
  onAbort: () => void;
};

export function createStreamPool(options: CreateStreamPoolOptions): StreamPool {
  const { inputs, inputSize, outputs, outputSize, wasm } = options;
  if (inputs > MAX_INPUT_PORTS || outputs > MAX_OUTPUT_PORTS || (inputs === 0 && outputs === 0)) {
    throw new RangeError(
      `stream pool expects 0–${MAX_INPUT_PORTS} inputs, 0–${MAX_OUTPUT_PORTS} outputs, at least one port total`,
    );
  }
  requireSp(wasm);

  const yieldToEventLoop =
    options.yieldToEventLoop ??
    (() => new Promise<void>((resolve) => setTimeout(resolve, 0)));

  const inputBytes = inputSize * 1024;
  let handle = 0;
  let programPtr = 0;
  let programLen = 0;

  if (options.graph) {
    programLen = options.graph.program.byteLength;
    programPtr = wasm.wcs_alloc!(programLen);
    viewAt(wasm.memory, programPtr, programLen).set(options.graph.program);
    handle = wasm.wcs_sp_open_graph!(
      programPtr,
      programLen,
      options.graph.scratchBytes,
      inputs,
      outputs,
      inputSize,
      outputSize,
      options.graph.budgetShare ?? 1,
    );
  } else {
    handle = wasm.wcs_sp_open_echo!(inputs, outputs, inputSize, outputSize);
  }
  if (!handle) {
    throw new Error(`wcs_sp_open failed: ${readWasmLastError(wasm, "no reason recorded")}`);
  }

  const resident = wasm.wcs_sp_is_resident?.(handle) === 1;
  if (resident && options.residentMemoryLimitBytes !== undefined) {
    const limit = options.residentMemoryLimitBytes;
    let ptr = 0;
    try {
      if (!Number.isSafeInteger(limit) || limit <= 0 || limit > 0xffff_ffff) {
        throw new Error("wcol-fatal: resident memory limit must be a positive 32-bit byte count");
      }
      ptr = wasm.wcs_alloc!(8);
      if (!ptr) throw new Error("resident limit allocation failed");
      new DataView(wasm.memory.buffer).setBigUint64(ptr, BigInt(limit), true);
      if (!wasm.wcs_sp_command!(handle, 0, ptr, 8)) {
        throw new Error(readWasmLastError(wasm, "resident memory limit rejected"));
      }
    } catch (error) {
      wasm.wcs_sp_close!(handle);
      if (programPtr) wasm.wcs_dealloc!(programPtr, programLen);
      throw error;
    } finally {
      if (ptr) wasm.wcs_dealloc!(ptr, 8);
    }
  }
  let residentIdle = false;
  let readyResolve!: () => void;
  let readyReject!: (error: unknown) => void;
  const ready = new Promise<void>((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  void ready.catch(() => undefined);
  let commandWaiter: { resolve(): void; reject(error: unknown): void } | null = null;

  const abortController = new AbortController();
  let closed = false;
  let sessionDone = false;
  let advancing = false;
  let advanceQueued = false;
  let needsAdvance = false;
  let advanceChain: Promise<void> = Promise.resolve();
  const inputReaders = new Set<ReadableStreamDefaultReader<Uint8Array> | ReadableStreamBYOBReader>();
  const waits: WaitEntry[] = [];
  let steps = 0;
  let maxStepMs = 0;
  let maxSyncMs = 0;
  const profile: PoolProfile = {
    idleReadMs: 0,
    idleOutputMs: 0,
    idleInputMs: 0,
    idleOutputPorts: 0,
    idleOutputExits: 0,
    outputBytes: 0,
    stepMs: 0,
    completeMs: 0,
    reads: 0,
    readBytes: 0,
    readMs: 0,
    readBusyMs: 0,
    maxInFlight: 0,
    firstReadAt: 0,
    lastReadAt: 0,
  };
  let inFlight = 0;
  let busySince = 0;
  let idle: { since: number; on: "idleReadMs" | "idleOutputMs" | "idleInputMs" } | undefined;
  const pendingOutputPorts = (): number => {
    let pending = 0;
    for (let port = 0; port < outputs; port++) {
      if (wasm.wcs_sp_out_len!(handle, port) > 0) pending += 1;
    }
    return pending;
  };
  let syncSince = performance.now();
  const noteSync = (): void => {
    maxSyncMs = Math.max(maxSyncMs, performance.now() - syncSince);
  };
  const yieldNow = async (): Promise<void> => {
    noteSync();
    await yieldToEventLoop();
    syncSince = performance.now();
  };

  let finishResolve!: () => void;
  let finishReject!: (error: unknown) => void;
  const finished = new Promise<void>((resolve, reject) => {
    finishResolve = resolve;
    finishReject = reject;
  });

  void finished.catch(() => undefined);

  const settleFinished = (error?: unknown): void => {
    if (sessionDone) return;
    sessionDone = true;
    if (error === undefined) finishResolve();
    else finishReject(error);
  };

  const poolError = (): Error => {
    const ptr = wasm.wcs_sp_error_ptr?.(handle) ?? 0;
    if (ptr) {
      const byteLen = wasm.wcs_sp_error_len?.(handle) ?? 0;
      const len = byteLen > 0 ? byteLen : viewAt(wasm.memory, ptr, 256).findIndex((b) => b === 0);
      const msg = new TextDecoder().decode(
        viewAt(wasm.memory, ptr, len >= 0 && len > 0 ? len : 256),
      );
      if (msg) return new Error(msg);
    }
    return new Error(readWasmLastError(wasm, "stream pool failed"));
  };

  const inputFree = (port: number): boolean => wasm.wcs_sp_in_free!(handle, port) === 1;

  const notifyWaiters = (): void => {
    for (let i = waits.length - 1; i >= 0; i--) {
      const entry = waits[i]!;
      if (entry.when()) {
        abortController.signal.removeEventListener("abort", entry.onAbort);
        waits.splice(i, 1);
        entry.resolve();
      }
    }
  };

  const fail = (error: unknown): void => {
    if (!abortController.signal.aborted) {
      abortController.abort(error);
    }
    for (const reader of inputReaders) {
      void reader.cancel(error).catch(() => undefined);
    }
    inputReaders.clear();
    const pending = waits.splice(0);
    for (const entry of pending) {
      abortController.signal.removeEventListener("abort", entry.onAbort);
      entry.reject(error);
    }
    readyReject(error);
    commandWaiter?.reject(error);
    commandWaiter = null;
    if (!sessionDone) {
      settleFinished(error);
    }
  };

  const waitWhen = (when: () => boolean): Promise<void> => {
    if (when()) return Promise.resolve();
    if (abortController.signal.aborted) {
      return Promise.reject(abortController.signal.reason ?? new Error("stream pool aborted"));
    }
    return new Promise((resolve, reject) => {
      const onAbort = (): void => {
        const index = waits.findIndex((entry) => entry.onAbort === onAbort);
        if (index >= 0) waits.splice(index, 1);
        reject(abortController.signal.reason ?? new Error("stream pool aborted"));
      };
      waits.push({ when, resolve, reject, onAbort });
      abortController.signal.addEventListener("abort", onAbort);
    });
  };

  /** One host read; completes into Wasm by ticket and wakes the step loop. */
  const startRead = (ticket: number, source: number, offset: bigint, length: number): void => {
    const readAt = options.readAt!;
    const issued = performance.now();
    if (inFlight === 0) busySince = issued;
    inFlight += 1;
    profile.maxInFlight = Math.max(profile.maxInFlight, inFlight);
    profile.reads += 1;
    if (profile.firstReadAt === 0) profile.firstReadAt = Date.now();
    const settled = (): void => {
      const now = performance.now();
      profile.readMs += now - issued;
      inFlight -= 1;
      if (inFlight === 0) profile.readBusyMs += now - busySince;
      profile.lastReadAt = Date.now();
    };
    new Promise<Uint8Array>((resolve) => resolve(readAt(source, offset, length))).then(
      (bytes) => {
        settled();
        profile.readBytes += bytes.byteLength;
        if (closed || sessionDone || !handle) return;
        const completeStarted = performance.now();
        const ptr = wasm.wcs_alloc!(bytes.byteLength);
        viewAt(wasm.memory, ptr, bytes.byteLength).set(bytes);
        // The pool keeps this allocation as the fetch buffer and frees it itself.
        const ok = wasm.wcs_sp_complete_read_at!(handle, ticket, ptr, bytes.byteLength);
        profile.completeMs += performance.now() - completeStarted;
        if (!ok) {
          fail(poolError());
          return;
        }
        requestAdvance();
      },
      (error: unknown) => {
        settled();
        fail(error);
      },
    );
  };

  const requestAdvance = (): void => {
    needsAdvance = true;
    if (closed || sessionDone || advancing || advanceQueued) return;
    advanceQueued = true;
    queueMicrotask(() => {
      advanceQueued = false;
      advanceChain = advanceChain.then(() => runAdvance(), () => runAdvance());
    });
  };

  const runAdvance = async (): Promise<void> => {
    if (closed || sessionDone) return;
    syncSince = performance.now();
    if (idle) {
      profile[idle.on] += syncSince - idle.since;
      idle = undefined;
    }
    advancing = true;
    needsAdvance = false;
    let madeProgress = false;
    let stepsSinceYield = 0;
    try {
      for (let step = 0; step < STEPS_PER_ADVANCE; step++) {
        if (abortController.signal.aborted) {
          throw abortController.signal.reason ?? new Error("stream pool aborted");
        }

        const stepStarted = performance.now();
        const status = wasm.wcs_sp_step!(handle, 100);
        const stepMs = performance.now() - stepStarted;
        profile.stepMs += stepMs;
        maxStepMs = Math.max(maxStepMs, stepMs);
        steps += 1;
        notifyWaiters();

        if (status & STEP_FAILED) {
          throw poolError();
        }
        if (status & STEP_RESIDENT_READY) {
          if (commandWaiter) {
            const waiter = commandWaiter;
            commandWaiter = null;
            waiter.resolve();
          } else {
            residentIdle = true;
            readyResolve();
          }
          madeProgress = false;
          needsAdvance = false;
          return;
        }
        if (status & STEP_FINISHED) {
          readyResolve();
          settleFinished();
          notifyWaiters();
          return;
        }

        if (status & STEP_SPILL_EXEC && options.onSpillExec) {
          noteSync();
          await options.onSpillExec(handle);
          syncSince = performance.now();
          madeProgress = true;
          stepsSinceYield += 1;
          // Spill uploads can run dozens of host ops; yield so DO RPCs and
          // progress heartbeats keep moving between chunks.
          if (stepsSinceYield >= 8) {
            await yieldNow();
            stepsSinceYield = 0;
          }
          continue;
        }
        if (status & STEP_SPILL_EXEC) {
          throw new Error("stream pool SpillExec pending but no onSpillExec handler");
        }

        const readLen = wasm.wcs_sp_read_at_len?.(handle) ?? 0;
        if (readLen > 0) {
          if (!options.readAt) {
            throw new Error(
              `stream pool pending ReadAt (${readLen} bytes) but no readAt handler was configured`,
            );
          }
          const ticket = wasm.wcs_sp_read_at_ticket!(handle);
          const source = wasm.wcs_sp_read_at_source!(handle);
          const offset = wasm.wcs_sp_read_at_offset!(handle);
          // Claim it and keep stepping: Rust exposes the next fetch while this one is
          // in flight, up to the plan's fetch depth. Scans are R2-latency bound.
          wasm.wcs_sp_take_read_at!(handle);
          startRead(ticket, source, offset, readLen);
          madeProgress = true;
          stepsSinceYield += 1;
          continue;
        }

        if (status & STEP_PROGRESS) {
          madeProgress = true;
          stepsSinceYield += 1;
          if (stepsSinceYield >= 16) {
            await yieldNow();
            stepsSinceYield = 0;
          }
          continue;
        }
        await yieldNow();
        // Pending spill host ops must keep the loop alive even when the last
        // step reported no row progress (common between R2 put chunks).
        if ((wasm.wcs_sp_spill_op?.(handle) ?? 0) !== 0) {
          madeProgress = true;
          continue;
        }
        const pendingPorts = inFlight > 0 ? 0 : pendingOutputPorts();
        if (pendingPorts > 0) {
          profile.idleOutputPorts += pendingPorts;
          profile.idleOutputExits += 1;
        }
        idle = {
          since: performance.now(),
          on: inFlight > 0 ? "idleReadMs" : pendingPorts > 0 ? "idleOutputMs" : "idleInputMs",
        };
        break;
      }
    } catch (error) {
      fail(error);
    } finally {
      advancing = false;
      if (!sessionDone && !closed && (needsAdvance || madeProgress)) {
        requestAdvance();
      }
    }
  };

  const onSlotChange = (): void => {
    requestAdvance();
  };

  requestAdvance();

  const inputAttached = new Set<number>();
  const inputPumpDone = new Set<number>();
  let inputsEndedAt: number | undefined;
  const noteInputDone = (port: number): void => {
    inputPumpDone.add(port);
    if (inputsEndedAt === undefined && inputPumpDone.size === inputs) inputsEndedAt = Date.now();
  };
  let closeTask: Promise<void> | null = null;

  const inputStreams = Array.from({ length: inputs }, (_, port) => {
    return new WritableStream<Uint8Array>({
      async write(chunk) {
        for (let offset = 0; offset < chunk.byteLength; ) {
          await waitWhen(() => inputFree(port));
          const n = Math.min(inputBytes, chunk.byteLength - offset);
          const ptr = wasm.wcs_sp_in_ptr!(handle, port);
          if (!ptr) throw new Error(`missing input slot ${port}`);
          viewAt(wasm.memory, ptr, n).set(chunk.subarray(offset, offset + n));
          const rc = wasm.wcs_sp_in_commit!(handle, port, n);
          if (rc !== 0) throw poolError();
          options.hostBytes?.add("edges", -n);
          onSlotChange();
          offset += n;
          await waitWhen(() => inputFree(port));
        }
      },
      async close() {
        await waitWhen(() => inputFree(port));
        const rc = wasm.wcs_sp_in_end!(handle, port);
        if (rc !== 0) throw poolError();
        onSlotChange();
      },
      abort(reason) {
        fail(reason);
      },
    });
  });

  const outputStreams = Array.from({ length: outputs }, (_, port) => {
    return new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          await waitWhen(
            () => wasm.wcs_sp_out_len!(handle, port) > 0 || wasm.wcs_sp_out_done!(handle, port) === 1,
          );
          const len = wasm.wcs_sp_out_len!(handle, port);
          if (len === 0) {
            controller.close();
            return;
          }
          const ptr = wasm.wcs_sp_out_ptr!(handle, port);
          if (!ptr) throw new Error(`missing output slot ${port}`);
          const copy = new Uint8Array(len);
          copy.set(viewAt(wasm.memory, ptr, len));
          profile.outputBytes += len;
          controller.enqueue(copy);
          const rc = wasm.wcs_sp_out_release!(handle, port);
          if (rc !== 0) throw poolError();
          onSlotChange();
        },
        cancel(reason) {
          fail(reason);
        },
      },
      { highWaterMark: 0 },
    );
  });

  const response = (): Uint8Array => {
    if (closed || !resident) throw new Error("resident graph is unavailable");
    const length = wasm.wcs_sp_response_len!(handle);
    return viewAt(wasm.memory, wasm.wcs_sp_response_ptr!(handle), length).slice();
  };

  return {
    resident,
    ready,
    response,
    async request(kind, bytes) {
      if (!resident || !residentIdle || closed || sessionDone) throw new Error("resident graph is unavailable or busy");
      residentIdle = false;
      try {
        const ptr = bytes.byteLength ? wasm.wcs_alloc(bytes.byteLength) : 0;
        if (bytes.byteLength && !ptr) throw new Error("resident command allocation failed");
        try {
          if (bytes.byteLength) viewAt(wasm.memory, ptr, bytes.byteLength).set(bytes);
          if (!wasm.wcs_sp_command!(handle, kind, ptr, bytes.byteLength)) {
            throw new Error(readWasmLastError(wasm, "resident command rejected"));
          }
        } finally {
          if (ptr) wasm.wcs_dealloc(ptr, bytes.byteLength);
        }
        const completion = new Promise<void>((resolve, reject) => { commandWaiter = { resolve, reject }; });
        requestAdvance();
        await completion;
        return response();
      } finally {
        residentIdle = !closed && !sessionDone;
      }
    },
    inputStreams,
    outputStreams,
    attachInput(port: number, stream: ReadableStream<Uint8Array>) {
      if (port < 0 || port >= inputs) {
        throw new RangeError(`attachInput port ${port} outside 0..${inputs - 1}`);
      }
      if (inputAttached.has(port) || inputPumpDone.has(port)) {
        throw new Error(`input port ${port} already has an attached stream`);
      }
      inputAttached.add(port);
      const writer = inputStreams[port]!.getWriter();
      void pumpReadableToWriter(stream, writer, abortController.signal, inputReaders).then(
        () => {
          noteInputDone(port);
        },
        (error) => {
          if (!abortController.signal.aborted) fail(error);
        },
      );
    },
    sealInput(port: number) {
      if (port < 0 || port >= inputs) {
        throw new RangeError(`sealInput port ${port} outside 0..${inputs - 1}`);
      }
      if (inputAttached.has(port) || inputPumpDone.has(port)) return;
      const rc = wasm.wcs_sp_in_absent!(handle, port);
      if (rc !== 0) throw poolError();
      noteInputDone(port);
      onSlotChange();
    },
    abort(reason?: unknown) {
      fail(reason ?? new Error("stream pool aborted"));
    },
    finished,
    inputsEndedAt: () => inputsEndedAt,
    close() {
      if (closeTask) return closeTask;
      if (closed) return Promise.resolve();
      closed = true;
      if (!sessionDone) {
        if (resident && residentIdle) settleFinished();
        else fail(new Error("stream pool closed"));
      }
      closeTask = (async () => {
        await finished.catch(() => undefined);
        await advanceChain.catch(() => undefined);
        for (const reader of inputReaders) {
          await reader.cancel(new Error("stream pool closed")).catch(() => undefined);
        }
        inputReaders.clear();
        if (programPtr) wasm.wcs_dealloc!(programPtr, programLen);
        if (handle) wasm.wcs_sp_close!(handle);
        handle = 0;
        programPtr = 0;
      })();
      return closeTask;
    },
    wake: onSlotChange,
    stats: () => ({
      steps,
      maxStepMs,
      maxSyncMs,
      profile: {
        ...profile,
        stepMs: Math.round(profile.stepMs),
        completeMs: Math.round(profile.completeMs),
        readMs: Math.round(profile.readMs),
        readBusyMs: Math.round(profile.readBusyMs),
        idleReadMs: Math.round(profile.idleReadMs),
        idleOutputMs: Math.round(profile.idleOutputMs),
        idleInputMs: Math.round(profile.idleInputMs),
      },
    }),
  };
}
