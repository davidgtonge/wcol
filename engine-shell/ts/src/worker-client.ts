import { decodeCbor, encodeCbor } from "./cbor";
import type { EngineUpdate, ViewModelPatch, WireMessage } from "./types";

export type WorkerClient<
  TInput,
  TOutput,
  TViewModel,
  TPatch extends ViewModelPatch,
  TEffect,
> = {
  init: (input: TInput) => Promise<EngineUpdate<TViewModel, TPatch, TEffect>>;
  dispatch: (input: TInput) => Promise<EngineUpdate<TViewModel, TPatch, TEffect>>;
  dispose: () => void;
};

export type WorkerClientOptions<
  TInput,
  TOutput,
  TViewModel,
  TPatch extends ViewModelPatch,
  TEffect,
> = {
  /** Construct the worker in app code so bundlers (e.g. Vite) emit the worker + Wasm chunks. */
  createWorker: () => Worker;
  encodeInput: (input: TInput) => Uint8Array;
  decodeOutput: (bytes: ArrayBuffer) => TOutput;
  parseUpdate: (output: TOutput) => EngineUpdate<TViewModel, TPatch, TEffect>;
  onDebug?: (output: TOutput, wireMs: number) => void;
};

type Job<TInput, TViewModel, TPatch extends ViewModelPatch, TEffect> = {
  input: TInput;
  resolve: (u: EngineUpdate<TViewModel, TPatch, TEffect>) => void;
  reject: (e: Error) => void;
};

export function createWorkerClient<TInput, TOutput, TViewModel, TPatch extends ViewModelPatch, TEffect>(
  options: WorkerClientOptions<TInput, TOutput, TViewModel, TPatch, TEffect>,
): WorkerClient<TInput, TOutput, TViewModel, TPatch, TEffect> {
  const worker = options.createWorker();
  const jobs: Job<TInput, TViewModel, TPatch, TEffect>[] = [];
  let busy = false;

  worker.onmessage = (event: MessageEvent<WireMessage>) => {
    const job = jobs.shift();
    if (!job) {
      busy = false;
      return;
    }

    const t0 = performance.now();
    const output = options.decodeOutput(event.data.bytes);
    const elapsedMs = performance.now() - t0;
    options.onDebug?.(output, elapsedMs);

    try {
      job.resolve(options.parseUpdate(output));
    } catch (err) {
      job.reject(err instanceof Error ? err : new Error(String(err)));
    }

    busy = false;
    pump();
  };

  worker.onerror = (err) => {
    const message = err.message || "Worker failed";
    jobs.forEach((j) => j.reject(new Error(message)));
    jobs.length = 0;
    busy = false;
  };

  function pump(): void {
    if (busy || jobs.length === 0) return;
    const job = jobs[0]!;
    busy = true;
    const bytes = toArrayBuffer(options.encodeInput(job.input));
    const msg: WireMessage = { bytes };
    worker.postMessage(msg, [bytes]);
  }

  function enqueue(input: TInput): Promise<EngineUpdate<TViewModel, TPatch, TEffect>> {
    return new Promise((resolve, reject) => {
      jobs.push({ input, resolve, reject });
      pump();
    });
  }

  return {
    init: enqueue,
    dispatch: enqueue,
    dispose: () => worker.terminate(),
  };
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/** Encode any worker input value as CBOR bytes. */
export function encodeWorkerInput<T>(input: T): Uint8Array {
  return encodeCbor(input);
}

/** Decode CBOR worker output bytes. */
export function decodeWorkerOutput<T>(bytes: ArrayBuffer): T {
  return decodeCbor<T>(bytes);
}
