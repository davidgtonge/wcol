import { execChunkOnPlan } from "../exec/chunk-exec.ts";
import { callStatus } from "../wasm/helpers.ts";
import { loadWasm, type WasmBindings } from "../wasm/wasm.ts";
import { applyPlan } from "../query/plan.ts";
import { bindRuntimeFromInitBytes } from "../wasm/runtime-init.ts";

type BindRuntimeFn = typeof bindRuntimeFromInitBytes;
import { readAggregateBytes, readGroupBytes, readRowBytes, readRowCandidateBytes } from "../exec/results.ts";
import type { WcolContext } from "../core/context.ts";
import type {
  WorkerInboundMessage,
  WorkerOutboundMessage,
  WorkerPartialResult,
  WorkerPlanMessage,
  WorkerTaskPayload
} from "./protocol.ts";
import { resultTransferables } from "./protocol.ts";

type WorkerCoreDeps = {
  loadWasm: () => Promise<WasmBindings>;
  bindRuntime: BindRuntimeFn;
  applyPlan: typeof applyPlan;
  readRowBytes: typeof readRowBytes;
  readRowCandidateBytes: typeof readRowCandidateBytes;
  readAggregateBytes: typeof readAggregateBytes;
  readGroupBytes: typeof readGroupBytes;
};

type WorkerCoreConfig = {
  postMessage: (msg: WorkerOutboundMessage, transfer?: ArrayBuffer[]) => void;
  deps?: Partial<WorkerCoreDeps>;
};

const defaultDeps: WorkerCoreDeps = {
  loadWasm,
  bindRuntime: bindRuntimeFromInitBytes,
  applyPlan,
  readRowBytes,
  readRowCandidateBytes,
  readAggregateBytes,
  readGroupBytes
};

function stringifyError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

async function applySql(wasm: WasmBindings, plan: number, sql: string): Promise<void> {
  const encoder = new TextEncoder();
  const sqlBytes = encoder.encode(sql);
  const ptr = wasm.alloc(sqlBytes.byteLength);
  wasm.memoryU8().set(sqlBytes, ptr);
  try {
    const code = wasm.exports.plan_apply_sql(plan, ptr, sqlBytes.byteLength);
    if (code < 0) {
      if (code === -1000) {
        throw new Error("WASM SQL API disabled. Rebuild with WCOL_WASM_SQL_API=1.");
      }
      throw new Error(`Failed to apply SQL (${code})`);
    }
  } finally {
    wasm.free(ptr, sqlBytes.byteLength);
  }
}

export async function prepareWorkerPlan(
  ctx: WcolContext,
  msg: WorkerPlanMessage,
  existingPlanHandle: number | null,
  applyPlanFn: typeof applyPlan = applyPlan
): Promise<number> {
  if (existingPlanHandle !== null) {
    ctx.wasm.exports.destroy_plan(existingPlanHandle);
  }
  const planHandle = ctx.wasm.exports.create_plan(ctx.runtime);
  ctx.wasm.exports.plan_reset_results(planHandle);
  if (msg.planSpec) {
    await applyPlanFn(ctx, msg.planSpec, planHandle);
  } else if (msg.sql) {
    await applySql(ctx.wasm, planHandle, msg.sql);
  } else {
    throw new Error("Missing plan payload");
  }
  return planHandle;
}

export async function executeWorkerTask(
  ctx: WcolContext,
  planHandle: number,
  task: WorkerTaskPayload,
  deps: Pick<
    WorkerCoreDeps,
    "readRowBytes" | "readRowCandidateBytes" | "readAggregateBytes" | "readGroupBytes"
  > = defaultDeps
): Promise<WorkerPartialResult> {
  ctx.wasm.exports.plan_reset_results(planHandle);
  execChunkOnPlan(ctx, planHandle, task);

  const rowOrderByLen = ctx.wasm.exports.plan_row_order_by_len(planHandle);
  const groupKeyCount = ctx.wasm.exports.plan_group_key_count(planHandle);
  const planLimit = ctx.wasm.exports.plan_limit(planHandle);
  const disableGroupLimit = groupKeyCount > 0 && rowOrderByLen === 0 && planLimit > 0;
  if (disableGroupLimit) {
    callStatus(ctx.wasm.exports.plan_set_limit(planHandle, 0));
  }
  if (rowOrderByLen === 0) {
    callStatus(ctx.wasm.exports.plan_finalize_rows(planHandle));
  }

  const rows = rowOrderByLen === 0 ? await deps.readRowBytes(ctx.wasm, planHandle) : new Uint8Array(0);
  const rowCandidates = rowOrderByLen > 0 ? await deps.readRowCandidateBytes(ctx.wasm, planHandle) : new Uint8Array(0);
  const aggs = await deps.readAggregateBytes(ctx.wasm, planHandle);
  const groups = await deps.readGroupBytes(ctx.wasm, planHandle);

  if (disableGroupLimit) {
    callStatus(ctx.wasm.exports.plan_set_limit(planHandle, planLimit));
  }

  return {
    chunkId: task.chunkId,
    rows,
    aggs,
    groups,
    rowCandidates: rowCandidates.byteLength ? rowCandidates : undefined
  };
}

export function createWorkerCore(config: WorkerCoreConfig): { onMessage: (msg: unknown) => Promise<void> } {
  const deps: WorkerCoreDeps = { ...defaultDeps, ...(config.deps ?? {}) };
  let ctx: WcolContext | null = null;
  let planHandle: number | null = null;

  const postError = (err: unknown) => {
    config.postMessage({ type: "error", error: stringifyError(err) });
  };

  const onMessage = async (rawMsg: unknown): Promise<void> => {
    const msg = rawMsg as WorkerInboundMessage | undefined;
    try {
      if (!msg || typeof msg !== "object" || !("type" in msg)) {
        throw new Error("Invalid worker message");
      }

      if (msg.type === "init") {
        const wasm = await deps.loadWasm();
        const { runtime, header } = await deps.bindRuntime(wasm, msg);
        ctx = {
          source: {
            read: async () => {
              throw new Error("Worker runtime does not support source reads");
            }
          },
          wasm,
          runtime,
          header,
          columnInfoCache: new Map(),
          columnNameCache: new Map(),
          dicts: new Map()
        };
        config.postMessage({ type: "ready" });
        return;
      }

      if (msg.type === "plan") {
        if (!ctx) {
          throw new Error("Worker not initialized");
        }
        planHandle = await prepareWorkerPlan(ctx, msg, planHandle, deps.applyPlan);
        config.postMessage({ type: "planned" });
        return;
      }

      if (msg.type === "task") {
        if (!ctx || planHandle === null) {
          throw new Error("Worker plan not initialized");
        }
        const result = await executeWorkerTask(ctx, planHandle, msg, deps);
        config.postMessage(
          {
            type: "result",
            taskId: msg.taskId,
            result
          },
          resultTransferables(result)
        );
        return;
      }

      throw new Error(`Unsupported worker message type: ${String((msg as { type?: unknown }).type)}`);
    } catch (err) {
      postError(err);
    }
  };

  return { onMessage };
}
