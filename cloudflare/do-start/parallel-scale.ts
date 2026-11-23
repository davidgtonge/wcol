import {
  touchColdDo,
  type DoBinding,
  type DoLocationHint,
  roundMs,
  type TouchBreakdown,
} from "./bench-lib.ts";

/** Cloudflare: at most six in-flight DO RPCs per Worker invocation (Slipstream, 2026). */
export const DO_RPC_INFLIGHT_PER_INVOCATION = 6;

export type ParallelChunkRequest = {
  runId: string;
  namePrefix: string;
  count: number;
  chunkIndex: number;
  locationHint?: DoLocationHint;
};

export type ChunkAccept = {
  accepted: true;
  runId: string;
  chunkIndex: number;
};

export type ParallelChunkResult = {
  count: number;
  totalMs: number;
  perDoMs: { min: number; max: number; p50: number };
};

export type ParallelScaleArm = {
  total: number;
  strategy: "single-invocation" | "fan-out-chunks";
  chunkSize: number;
  chunks: number;
  /** Wall time until every /chunk returned accepted (dispatch only). */
  dispatchMs: number;
  /** Wall time until all DO cold starts finished (waitUntil + poll). */
  combinedMs: number;
  budgetMs: number;
  withinBudget: boolean;
  chunkMs: { min: number; max: number; p50: number };
  perDoMs: { min: number; max: number; p50: number };
};

export type ParallelScaleResult = {
  runId: string;
  target: string;
  locationHint: DoLocationHint | "default";
  budgetMs: number;
  arms: ParallelScaleArm[];
};

function summarizeMs(values: number[]): { min: number; max: number; p50: number } {
  if (values.length === 0) return { min: 0, max: 0, p50: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const p50 = sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
  return { min: sorted[0]!, max: sorted[sorted.length - 1]!, p50 };
}

export async function runParallelChunk(
  binding: DoBinding,
  req: ParallelChunkRequest,
): Promise<ParallelChunkResult> {
  const names = Array.from({ length: req.count }, (_, i) => `${req.namePrefix}${i}`);
  const t0 = performance.now();
  const touches: TouchBreakdown[] = await Promise.all(
    names.map((name) => touchColdDo(binding, name, "rpc", req.locationHint)),
  );
  return {
    count: req.count,
    totalMs: performance.now() - t0,
    perDoMs: summarizeMs(touches.map((t) => t.totalMs)),
  };
}

export function chunkPlan(total: number, chunkSize: number): number[] {
  const sizes: number[] = [];
  for (let left = total; left > 0; left -= chunkSize) {
    sizes.push(Math.min(chunkSize, left));
  }
  return sizes;
}

export function chunkResultKey(runId: string, chunkIndex: number): string {
  return `cr:${runId}:${chunkIndex}`;
}

export async function pollChunkResults(
  kv: { get(key: string): Promise<string | null> },
  runId: string,
  chunkCount: number,
  timeoutMs: number,
): Promise<ParallelChunkResult[]> {
  const deadline = Date.now() + timeoutMs;
  const results: Array<ParallelChunkResult | undefined> = Array.from({ length: chunkCount });
  while (Date.now() < deadline) {
    let found = 0;
    for (let i = 0; i < chunkCount; i++) {
      if (results[i]) {
        found++;
        continue;
      }
      const row = await kv.get(chunkResultKey(runId, i));
      if (!row) continue;
      results[i] = JSON.parse(row) as ParallelChunkResult;
      found++;
    }
    if (found === chunkCount) return results as ParallelChunkResult[];
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  const got = results.filter(Boolean).length;
  throw new Error(`chunk results incomplete for ${runId}: ${got}/${chunkCount}`);
}

/** Stay under per-invocation subrequest limits when fanning out /chunk accepts. */
export const DISPATCH_WAVE_SIZE = 40;

async function dispatchChunkWaves(
  sizes: number[],
  armRunId: string,
  locationHint: DoLocationHint | undefined,
  dispatchChunk: (req: ParallelChunkRequest) => Promise<ChunkAccept>,
): Promise<void> {
  for (let waveStart = 0; waveStart < sizes.length; waveStart += DISPATCH_WAVE_SIZE) {
    const wave = sizes.slice(waveStart, waveStart + DISPATCH_WAVE_SIZE);
    await Promise.all(
      wave.map((count, offset) => {
        const chunkIndex = waveStart + offset;
        return dispatchChunk({
          runId: armRunId,
          namePrefix: `${armRunId}:fan:c${chunkIndex}:`,
          count,
          chunkIndex,
          locationHint,
        });
      }),
    );
  }
}

export async function runParallelScaleOnBinding(
  target: string,
  dispatchChunk: (req: ParallelChunkRequest) => Promise<ChunkAccept>,
  waitForChunks: (runId: string, chunkCount: number) => Promise<ParallelChunkResult[]>,
  options: {
    totals?: number[];
    budgetMs?: number;
    locationHint?: DoLocationHint;
    chunkSize?: number;
    runId?: string;
    /** When false, only fan-out-chunks (respects chunkSize). Default: both strategies. */
    includeSingleInvocation?: boolean;
    includeFanOut?: boolean;
    pollTimeoutMs?: number;
  },
): Promise<ParallelScaleResult> {
  const totals = options.totals ?? [10, 100, 1000];
  const budgetMs = options.budgetMs ?? 400;
  const chunkSize = options.chunkSize ?? DO_RPC_INFLIGHT_PER_INVOCATION;
  const runId = options.runId ?? crypto.randomUUID();
  const locationHint = options.locationHint;
  const includeSingle = options.includeSingleInvocation !== false;
  const includeFanOut = options.includeFanOut !== false;
  const pollTimeoutMs = options.pollTimeoutMs ?? 120_000;
  const arms: ParallelScaleArm[] = [];

  for (const total of totals) {
    const strategies: Array<"single-invocation" | "fan-out-chunks"> = [];
    if (includeFanOut) strategies.push("fan-out-chunks");
    if (includeSingle) strategies.push("single-invocation");

    for (const strategy of strategies) {
      const sizes = chunkPlan(total, strategy === "single-invocation" ? total : chunkSize);
      const armRunId = `${runId}:${strategy}:${total}`;
      const t0 = performance.now();
      if (strategy === "single-invocation") {
        await dispatchChunk({
          runId: armRunId,
          namePrefix: `${armRunId}:single:`,
          count: total,
          chunkIndex: 0,
          locationHint,
        });
      } else {
        await dispatchChunkWaves(sizes, armRunId, locationHint, dispatchChunk);
      }
      const dispatchMs = performance.now() - t0;
      const chunkResults = await waitForChunks(armRunId, sizes.length);
      const combinedMs = performance.now() - t0;
      const perDoValues = chunkResults.flatMap((c) => {
        const spread = c.perDoMs;
        return [spread.min, spread.p50, spread.max];
      });
      const perDoMs =
        strategy === "single-invocation" && chunkResults[0]
          ? chunkResults[0].perDoMs
          : summarizeMs(perDoValues);
      arms.push({
        total,
        strategy,
        chunkSize: strategy === "single-invocation" ? total : chunkSize,
        chunks: sizes.length,
        dispatchMs,
        combinedMs,
        budgetMs,
        withinBudget: combinedMs <= budgetMs,
        chunkMs: summarizeMs(chunkResults.map((c) => c.totalMs)),
        perDoMs,
      });
    }
  }

  return {
    runId,
    target,
    locationHint: locationHint ?? "default",
    budgetMs,
    arms,
  };
}

export function compactParallelScale(payload: ParallelScaleResult): unknown {
  return {
    runId: payload.runId,
    target: payload.target,
    locationHint: payload.locationHint,
    budgetMs: payload.budgetMs,
    inflightLimitPerInvocation: DO_RPC_INFLIGHT_PER_INVOCATION,
    arms: payload.arms.map((a) => ({
      total: a.total,
      strategy: a.strategy,
      chunkSize: a.chunkSize,
      chunks: a.chunks,
      combinedMs: roundMs(a.combinedMs),
      dispatchMs: roundMs(a.dispatchMs),
      withinBudget: a.withinBudget,
      chunkMs: {
        p50: roundMs(a.chunkMs.p50),
        max: roundMs(a.chunkMs.max),
      },
      perDoMs: {
        p50: roundMs(a.perDoMs.p50),
        max: roundMs(a.perDoMs.max),
      },
    })),
  };
}
