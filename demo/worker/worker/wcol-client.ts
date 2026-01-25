import {
  decodeWorkerOutput,
  encodeWorkerInput,
  type EngineUpdate,
  type WireMessage,
} from "@dtonge/engine-shell";
import type {
  AppEvent,
  OpenFileWireMessage,
  ViewModel,
  ViewModelPatch,
  WorkerInput,
  WorkerOutput,
} from "../protocol/types.ts";

export type WcolUpdate = EngineUpdate<ViewModel, ViewModelPatch, never>;

type CborJob = {
  kind: "cbor";
  input: WorkerInput;
  resolve: (u: WcolUpdate) => void;
  reject: (e: Error) => void;
};

type OpenFileJob = {
  kind: "openFile";
  file: File;
  resolve: (u: WcolUpdate) => void;
  reject: (e: Error) => void;
};

type Job = CborJob | OpenFileJob;

function parseUpdate(output: WorkerOutput): WcolUpdate {
  if (output.kind === "error") {
    throw new Error(output.message);
  }
  if (output.kind === "initialized") {
    return {
      viewModel: output.viewModel,
      patches: [],
      effects: [],
      diagnostics: [],
    };
  }
  return {
    patches: output.patches,
    viewModel: output.viewModel,
    effects: [],
    diagnostics: output.diagnostics,
  };
}

function workerUrl(): URL {
  return new URL("./demo-worker.js", import.meta.url);
}

export function createWcolWorkerClient() {
  const worker = new Worker(workerUrl(), { type: "module" });
  const jobs: Job[] = [];
  let busy = false;

  worker.onmessage = (event: MessageEvent<WireMessage>) => {
    const job = jobs.shift();
    if (!job) {
      busy = false;
      return;
    }

    try {
      const output = decodeWorkerOutput<WorkerOutput>(event.data.bytes);
      job.resolve(parseUpdate(output));
    } catch (err) {
      job.reject(err instanceof Error ? err : new Error(String(err)));
    }

    busy = false;
    pump();
  };

  worker.onerror = (err) => {
    jobs.forEach((j) => j.reject(new Error(String(err.message))));
    jobs.length = 0;
    busy = false;
  };

  function pump(): void {
    if (busy || jobs.length === 0) return;
    const job = jobs[0]!;
    busy = true;

    if (job.kind === "openFile") {
      const msg: OpenFileWireMessage = { type: "openFile", file: job.file };
      worker.postMessage(msg);
      return;
    }

    const bytes = toArrayBuffer(encodeWorkerInput(job.input));
    const msg: WireMessage = { bytes };
    worker.postMessage(msg, [bytes]);
  }

  function enqueueCbor(input: WorkerInput): Promise<WcolUpdate> {
    return new Promise((resolve, reject) => {
      jobs.push({ kind: "cbor", input, resolve, reject });
      pump();
    });
  }

  return {
    init: (input: WorkerInput) => enqueueCbor(input),
    dispatch: (input: WorkerInput) => enqueueCbor(input),
    openFile: (file: File) =>
      new Promise<WcolUpdate>((resolve, reject) => {
        jobs.push({ kind: "openFile", file, resolve, reject });
        pump();
      }),
    dispose: () => worker.terminate(),
  };
}

export type WcolWorkerClient = ReturnType<typeof createWcolWorkerClient>;

export function wcolInitInput(): WorkerInput {
  return { kind: "init" };
}

export function wcolEventInput(event: AppEvent): WorkerInput {
  return { kind: "event", event };
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}
