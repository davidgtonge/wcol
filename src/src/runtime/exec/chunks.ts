import { callStatus, withBytes } from "../wasm/helpers.ts";
import type { WcolContext } from "../core/context.ts";
import { getColumnName } from "../schema/columns.ts";
import {
  execChunkOnPlan,
  loadChunkPayload,
  loadChunkPayloadForMaterialize,
  materializeChunkOnPlan,
  type ChunkPayload
} from "./chunk-exec.ts";
import { groupRowIdsByChunk, readRowProjection } from "./projection.ts";
import {
  createLocalHandle,
  createWorkerPool,
  resolveWorkerCount,
  resolveWorkerRuntimeKind,
  type PartialResult,
  type PlanMsg,
  type WorkerHandle,
  type WorkerPool
} from "./chunk-workers.ts";
import { planUsesApproxDistinct, readAggregates, readGroups, readRows } from "./results.ts";
import type { ExecuteOptions, QueryOptions, QueryPlan, QueryResult, RowProjection, U64 } from "../core/types.ts";

const emptyPartial = (chunkId: number): PartialResult => ({
  chunkId,
  rows: new Uint8Array(0),
  aggs: new Uint8Array(0),
  groups: new Uint8Array(0)
});

async function withContextLock<T>(ctx: WcolContext, fn: () => Promise<T>): Promise<T> {
  const prev = ctx.queryChain ?? Promise.resolve();
  let release!: () => void;
  const gate = new Promise<void>((r) => {
    release = r;
  });
  ctx.queryChain = prev.then(() => gate);
  await prev;
  try {
    return await fn();
  } finally {
    release();
  }
}

export async function executePlanFromPlan(
  ctx: WcolContext,
  plan: number,
  options?: ExecuteOptions
): Promise<QueryResult> {
  return withContextLock(ctx, () => runChunkQuery(ctx, plan, undefined, options));
}

export async function executePlan(
  ctx: WcolContext,
  planSpec: QueryPlan,
  plan: number,
  options?: QueryOptions
): Promise<QueryResult> {
  return withContextLock(ctx, () => runChunkQuery(ctx, plan, planSpec, options));
}

function resolvePlanMsg(
  planSpec?: QueryPlan,
  options?: QueryOptions | ExecuteOptions
): PlanMsg | null {
  if (planSpec !== undefined) return { planSpec };
  const sql = (options as ExecuteOptions | undefined)?.sql;
  return sql ? { sql } : null;
}

async function runLocalChunks(
  ctx: WcolContext,
  plan: number,
  hasFilters: boolean,
  earlyLimit?: number
): Promise<QueryResult> {
  await runChunkTasks(ctx, plan, hasFilters, 1, async (task) => {
    if (task.payload) execChunkOnPlan(ctx, plan, task.payload);
  }, { earlyLimit });
  callStatus(ctx.wasm.exports.plan_finalize_rows(plan));
  return readPlanResult(ctx, plan);
}

async function runChunkQuery(
  ctx: WcolContext,
  plan: number,
  planSpec?: QueryPlan,
  options?: QueryOptions | ExecuteOptions
): Promise<QueryResult> {
  const explicitWorkers = options?.workers !== undefined;
  const workers = Math.max(1, Math.min(await resolveWorkerCount(options), ctx.header.nchunks));
  const hasFilters = ctx.wasm.exports.plan_filters_len(plan) > 0;
  const planMsg = resolvePlanMsg(planSpec, options);
  const earlyLimit = planSpec?.limit;

  if (!planMsg || (await planUsesApproxDistinct(ctx.wasm, plan))) {
    return runLocalChunks(ctx, plan, hasFilters, earlyLimit);
  }

  let closeExecutor: (() => Promise<void>) | undefined;
  try {
    const { workers: handles, close } = await acquireExecutors(ctx, workers, planMsg);
    closeExecutor = close;
    const partials = new Array<PartialResult>(ctx.header.nchunks);
    await runChunkTasks(
      ctx,
      plan,
      hasFilters,
      handles.length,
      async (task, handle) => {
        if (!handle) throw new Error("Missing worker handle");
        partials[task.chunkId] = task.payload ? await handle.runTask(task.payload) : emptyPartial(task.chunkId);
      },
      { handles }
    );
    return reducePartials(ctx, plan, partials);
  } catch (err) {
    if (explicitWorkers) {
      throw err;
    }
    return runLocalChunks(ctx, plan, hasFilters, earlyLimit);
  } finally {
    await closeExecutor?.();
  }
}

type ChunkTask = { chunkId: number; payload: ChunkPayload | null };

function createChunkQueue(ctx: WcolContext, plan: number, hasFilters: boolean, prefetchCap: number) {
  const nchunks = ctx.header.nchunks;
  const staged = new Map<number, Promise<ChunkPayload | null>>();
  let build = 0;
  let dispatch = 0;
  const stage = () => {
    while (build < nchunks && staged.size < prefetchCap) {
      staged.set(build, loadChunkPayload(ctx, plan, build, hasFilters));
      build += 1;
    }
  };
  return async (): Promise<ChunkTask | null> => {
    stage();
    if (dispatch >= nchunks) return null;
    const chunkId = dispatch++;
    const payload = await staged.get(chunkId)!;
    staged.delete(chunkId);
    stage();
    return { chunkId, payload };
  };
}

async function runChunkTasks(
  ctx: WcolContext,
  plan: number,
  hasFilters: boolean,
  concurrency: number,
  onChunk: (task: ChunkTask, handle?: WorkerHandle) => Promise<void>,
  options?: { earlyLimit?: number; handles?: WorkerHandle[] }
): Promise<void> {
  const next = createChunkQueue(ctx, plan, hasFilters, Math.max(concurrency * 2, 4));
  const runOne = async (handle?: WorkerHandle) => {
    for (;;) {
      const task = await next();
      if (!task) break;
      await onChunk(task, handle);
      if (options?.earlyLimit && ctx.wasm.exports.plan_rows_len(plan) >= options.earlyLimit) break;
    }
  };
  if (options?.handles?.length) {
    await Promise.all(options.handles.map((handle) => runOne(handle)));
    return;
  }
  await runOne();
}

export async function warmWorkerPool(
  ctx: WcolContext,
  workers: number,
  planMsg: PlanMsg = { planSpec: { limit: 0 } }
): Promise<void> {
  const count = Math.max(1, Math.min(workers, ctx.header.nchunks));
  if (count <= 1 || !ctx.init) return;
  await acquireExecutors(ctx, count, planMsg);
}

async function acquireExecutors(
  ctx: WcolContext,
  workers: number,
  planMsg: PlanMsg
): Promise<{ workers: WorkerHandle[]; close?: () => Promise<void> }> {
  if (workers > 1 && ctx.init) {
    const kind = resolveWorkerRuntimeKind();
    if (kind === "node" || kind === "browser") {
      let pool: WorkerPool;
      const existing = ctx.workerPool;
      if (existing && existing.kind === kind && (existing.pool as WorkerPool).size === workers) {
        pool = existing.pool as WorkerPool;
      } else {
        if (existing) await (existing.pool as WorkerPool).close();
        pool = await createWorkerPool(kind, ctx.init, workers);
        ctx.workerPool = { kind, size: workers, pool };
      }
      await pool.setPlan(planMsg);
      return { workers: pool.workers };
    }
  }
  const local = await createLocalHandle(ctx);
  await local.setPlan(planMsg);
  return { workers: [local], close: () => local.close() };
}

async function reducePartials(
  ctx: WcolContext,
  basePlan: number,
  partials: PartialResult[]
): Promise<QueryResult> {
  const reducer = ctx.wasm.exports.plan_reducer_new(basePlan);
  if (!reducer) throw new Error("Failed to create reducer plan");
  const merge = (
    buf: Uint8Array | undefined,
    fn: (plan: number, ptr: number, len: number) => number
  ) => {
    if (!buf?.byteLength) return;
    withBytes(ctx.wasm, buf, (ptr, len) => callStatus(fn(reducer, ptr, len)));
  };
  try {
    for (const partial of partials) {
      if (!partial) continue;
      merge(partial.rows, ctx.wasm.exports.plan_reducer_merge_rows);
      merge(partial.rowCandidates, ctx.wasm.exports.plan_reducer_merge_row_candidates);
      merge(partial.aggs, ctx.wasm.exports.plan_reducer_merge_aggs);
      merge(partial.groups, ctx.wasm.exports.plan_reducer_merge_groups);
    }
    callStatus(ctx.wasm.exports.plan_reducer_finalize(reducer));
    return await readPlanResult(ctx, reducer);
  } finally {
    ctx.wasm.exports.destroy_plan(reducer);
  }
}

async function mapParallel<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  const workers = Math.min(Math.max(1, limit), items.length);
  await Promise.all(
    Array.from({ length: workers }, async () => {
      for (;;) {
        const i = next++;
        if (i >= items.length) break;
        results[i] = await fn(items[i], i);
      }
    })
  );
  return results;
}

async function materializeSelectProjection(ctx: WcolContext, plan: number, rows: U64[]): Promise<RowProjection | null> {
  const selectCount = ctx.wasm.exports.plan_select_count(plan);
  if (selectCount <= 0 || rows.length === 0) {
    return null;
  }
  callStatus(ctx.wasm.exports.plan_projection_begin(plan));
  const grouped = groupRowIdsByChunk(rows, ctx.header.rowsPerChunk);
  const chunkIds = [...grouped.keys()].sort((a, b) => a - b);
  const concurrency = Math.min(8, chunkIds.length);
  const payloads = await mapParallel(chunkIds, concurrency, (chunkId) =>
    loadChunkPayloadForMaterialize(ctx, plan, chunkId)
  );
  for (let i = 0; i < chunkIds.length; i += 1) {
    const chunkId = chunkIds[i]!;
    const payload = payloads[i];
    if (!payload) continue;
    const { local, dst } = grouped.get(chunkId)!;
    materializeChunkOnPlan(
      ctx,
      plan,
      payload,
      new Uint32Array(local),
      new Uint32Array(dst)
    );
  }
  const nameCtx = { getColumnName: async (colId: number) => getColumnName(ctx, colId) };
  return readRowProjection(nameCtx, ctx.wasm, plan);
}

async function readPlanResult(ctx: WcolContext, plan: number): Promise<QueryResult> {
  const nameCtx = { getColumnName: async (colId: number) => getColumnName(ctx, colId) };
  const [aggregates, groups, rows] = await Promise.all([
    readAggregates(nameCtx, ctx.wasm, plan),
    readGroups(ctx.wasm, plan),
    readRows(ctx.wasm, plan)
  ]);
  const projection = await materializeSelectProjection(ctx, plan, rows);
  return { rows, projection, aggregates, groups };
}
