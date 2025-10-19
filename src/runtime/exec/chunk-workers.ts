import { executeWorkerTask, prepareWorkerPlan } from "../workers/core.ts";
import {
  taskTransferables,
  type WorkerInboundMessage,
  type WorkerOutboundMessage,
  type WorkerPartialResult
} from "../workers/protocol.ts";
import type { WcolContext } from "../core/context.ts";
import type { ChunkPayload } from "./chunk-exec.ts";
import type { QueryPlan, WorkerRuntimeKind } from "../core/types.ts";

const BROWSER_BUILD = typeof __WCOL_BROWSER_BUILD__ !== "undefined" && __WCOL_BROWSER_BUILD__;

export type PartialResult = WorkerPartialResult;
export type PlanMsg = { planSpec?: QueryPlan; sql?: string };

export type WorkerHandle = {
  runTask: (task: ChunkPayload) => Promise<PartialResult>;
  setPlan: (planMsg: PlanMsg) => Promise<void>;
  close: () => Promise<void>;
};

export type WorkerPool = {
  kind: WorkerRuntimeKind;
  workers: WorkerHandle[];
  size: number;
  setPlan: (planMsg: PlanMsg) => Promise<void>;
  close: () => Promise<void>;
};

type WorkerAdapter = {
  postMessage: (msg: WorkerInboundMessage, transfer?: ArrayBuffer[]) => void;
  onMessage: (fn: (msg: WorkerOutboundMessage) => void) => void;
  onError: (fn: (err: Error) => void) => void;
  onClose: (fn: (err: Error) => void) => void;
  terminate: () => Promise<void>;
};

export function isNodeRuntime(): boolean {
  if (BROWSER_BUILD) return false;
  return typeof process !== "undefined" && !!(process as NodeJS.Process).versions?.node;
}

function hasBrowserWorkers(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

export async function resolveWorkerCount(options?: { workers?: number }): Promise<number> {
  if (options?.workers !== undefined) return Math.max(1, options.workers);
  if (!BROWSER_BUILD && isNodeRuntime()) {
    try {
      const os = await import(["node", "os"].join(":"));
      return Math.max(1, os.cpus().length);
    } catch {
      return 1;
    }
  }
  if (typeof navigator !== "undefined" && Number.isFinite(navigator.hardwareConcurrency)) {
    return Math.max(1, navigator.hardwareConcurrency);
  }
  return 1;
}

export function resolveWorkerRuntimeKind(): WorkerRuntimeKind {
  if (BROWSER_BUILD) return hasBrowserWorkers() ? "browser" : "local";
  if (isNodeRuntime()) return "node";
  if (hasBrowserWorkers()) return "browser";
  return "local";
}

async function createWorkerAdapter(kind: WorkerRuntimeKind): Promise<WorkerAdapter> {
  if (kind === "node") {
    const { Worker } = await import(["node", "worker_threads"].join(":"));
    const workerUrl = new URL("../workers/node.ts", import.meta.url);
    const execArgv = Array.isArray(process?.execArgv) ? [...process.execArgv] : [];
    if (!execArgv.includes("--import") && !execArgv.includes("--loader")) {
      try {
        await import(["t", "sx"].join(""));
        execArgv.push("--import", "tsx");
      } catch {
        // keep default execArgv
      }
    }
    const worker = new Worker(workerUrl, { execArgv });
    return {
      postMessage: (msg, transfer) => worker.postMessage(msg, transfer),
      onMessage: (fn) => worker.on("message", (msg: WorkerOutboundMessage) => fn(msg)),
      onError: (fn) => worker.on("error", (err) => fn(err instanceof Error ? err : new Error(String(err)))),
      onClose: (fn) => worker.on("exit", (code) => code !== 0 && fn(new Error(`Worker exited with code ${code}`))),
      terminate: () => worker.terminate()
    };
  }
  const url =
    typeof __WCOL_BROWSER_WORKER_URL__ !== "undefined" && __WCOL_BROWSER_WORKER_URL__
      ? __WCOL_BROWSER_WORKER_URL__
      : "../workers/browser.ts";
  const worker = new Worker(new URL(url, import.meta.url), { type: "module" });
  return {
    postMessage: (msg, transfer) => worker.postMessage(msg, transfer ?? []),
    onMessage: (fn) => worker.addEventListener("message", (e: MessageEvent<WorkerOutboundMessage>) => fn(e.data)),
    onError: (fn) => worker.addEventListener("error", (e: ErrorEvent) => fn(new Error(e.message || "Browser worker error"))),
    onClose: (fn) => worker.addEventListener("messageerror", () => fn(new Error("Browser worker message error"))),
    terminate: async () => worker.terminate()
  };
}

async function createWorkerHandle(
  adapter: WorkerAdapter,
  init: NonNullable<WcolContext["init"]>
): Promise<WorkerHandle> {
  const pending = new Map<number, { resolve: (v: PartialResult) => void; reject: (e: Error) => void }>();
  let readyResolve!: () => void;
  let readyReject!: (e: Error) => void;
  let plannedResolve: (() => void) | null = null;
  let plannedReject: ((e: Error) => void) | null = null;
  let readyDone = false;
  let plannedDone = false;

  const fail = (err: Error) => {
    for (const entry of pending.values()) entry.reject(err);
    pending.clear();
    if (!readyDone) readyReject(err);
    plannedReject?.(err);
  };

  adapter.onMessage((msg) => {
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "ready") {
      readyDone = true;
      readyResolve();
      return;
    }
    if (msg.type === "planned") {
      plannedDone = true;
      plannedResolve?.();
      return;
    }
    if (msg.type === "error") {
      fail(new Error(msg.error ?? "Worker error"));
      return;
    }
    if (msg.type === "result" && msg.taskId !== undefined) {
      const entry = pending.get(msg.taskId);
      if (!entry) return;
      pending.delete(msg.taskId);
      if (msg.result) entry.resolve(msg.result);
      else entry.reject(new Error("Worker result missing payload"));
    }
  });
  adapter.onError(fail);
  adapter.onClose(fail);
  adapter.postMessage({ type: "init", header: init.header, schema: init.schema, toc: init.toc, dicts: init.dicts });
  await new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });

  let nextTaskId = 1;
  return {
    setPlan: async (planMsg) => {
      plannedDone = false;
      const awaitPlan = new Promise<void>((resolve, reject) => {
        plannedResolve = resolve;
        plannedReject = reject;
      });
      adapter.postMessage({ type: "plan", ...planMsg });
      await awaitPlan;
    },
    runTask: (task) =>
      new Promise((resolve, reject) => {
        const taskId = nextTaskId++;
        pending.set(taskId, { resolve, reject });
        adapter.postMessage({ type: "task", taskId, ...task }, taskTransferables(task));
      }),
    close: () => adapter.terminate()
  };
}

export async function createWorkerPool(
  kind: WorkerRuntimeKind,
  init: NonNullable<WcolContext["init"]>,
  workers: number
): Promise<WorkerPool> {
  const poolKind = BROWSER_BUILD ? "browser" : kind;
  const adapterKind = poolKind === "node" ? "node" : "browser";
  const handles = await Promise.all(
    Array.from({ length: workers }, async () => createWorkerHandle(await createWorkerAdapter(adapterKind), init))
  );
  const setPlan = (planMsg: PlanMsg) => Promise.all(handles.map((h) => h.setPlan(planMsg))).then(() => undefined);
  const close = () => Promise.all(handles.map((h) => h.close())).then(() => undefined);
  return { kind: poolKind, workers: handles, size: workers, setPlan, close };
}

export async function createLocalHandle(ctx: WcolContext): Promise<WorkerHandle> {
  let planHandle: number | null = null;
  return {
    setPlan: async (planMsg) => {
      planHandle = await prepareWorkerPlan(ctx, { type: "plan", ...planMsg }, planHandle);
    },
    runTask: async (task) => {
      if (planHandle === null) throw new Error("Local executor plan not initialized");
      return executeWorkerTask(ctx, planHandle, task);
    },
    close: async () => {
      if (planHandle !== null) {
        ctx.wasm.exports.destroy_plan(planHandle);
        planHandle = null;
      }
    }
  };
}
