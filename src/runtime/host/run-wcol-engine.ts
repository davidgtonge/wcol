import type { PoolWasm, StreamRunStats } from "./wasm-stream.ts";
import { INPUT_RINGS, OUTPUT_SLOTS, portKiB } from "./port-budget.ts";
import { createStreamPool, type StreamPool } from "./stream-pool.ts";
import { readResidentMemory } from "./resident-graph.ts";
import {
  readDictTraffic,
  readMemGrowth,
  readSpillBoundaries,
  type RunWcolEngineOptions,
  type WcolHost,
} from "./host-types.ts";
import {
  createPoolSpillDriver,
  deleteSpillRuns,
  emptySpillIo,
  type PoolSpillDriver,
} from "./spill-r2-pool-driver.ts";

export type {
  DictTraffic,
  MemGrowthSample,
  RunWcolEngineOptions,
  SpillBoundary,
  WcolHost,
} from "./host-types.ts";
export { readDictTraffic, readMemGrowth } from "./host-types.ts";

function readSpillProgress(wasm: PoolWasm) {
  return {
    spillRuns: Number(wasm.wcs_last_spill_runs()),
    spillGroupsFlushed: Number(wasm.wcs_last_spill_groups_flushed()),
    spillPeakLiveBytes: Number(wasm.wcs_last_spill_peak_live_bytes()),
    spillPeakInternerBytes: Number(wasm.wcs_last_spill_peak_interner_bytes()),
    spillBoundaries: readSpillBoundaries(wasm),
    residentGroups: Number(wasm.wcs_last_resident_groups()),
    residentGroupBytes: Number(wasm.wcs_last_resident_group_bytes()),
  };
}

function wirePoolOutputs(pool: StreamPool, host: WcolHost, outputs: number): Promise<void>[] {
  return Array.from({ length: outputs }, async (_, port) => {
    const reader = pool.outputStreams[port]!.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value.byteLength > 0) {
          await host.writePort(port, value);
          pool.wake();
        }
      }
      await host.closePort?.(port);
    } catch (error) {
      pool.abort(error);
      throw error;
    } finally {
      reader.releaseLock();
    }
  });
}

/** Run one graph program on the stream pool (only supported host path). */
export async function runWcolEngine(options: RunWcolEngineOptions): Promise<StreamRunStats> {
  const { host, wasm } = options;
  const spillStats = host.spillR2Bucket
    ? { spillExec: 0, spillExecByOp: { begin: 0, scan: 0, end: 0, commit: 0 }, io: emptySpillIo() }
    : undefined;
  let spillDriver: PoolSpillDriver | undefined;
  let pollIterations = 0;

  const pool = createStreamPool({
    inputs: options.layout.inputs,
    outputs: options.layout.outputs,
    inputSize: options.inputSizeKiB ?? portKiB(INPUT_RINGS, options.layout.inputs),
    outputSize: options.outputSizeKiB ?? portKiB(OUTPUT_SLOTS, options.layout.outputs),
    wasm,
    residentMemoryLimitBytes: options.residentMemoryLimitBytes,
    graph: { program: options.plan, scratchBytes: options.scratchBytes ?? 256 * 1024, budgetShare: options.budgetShare },
    hostBytes: options.hostBytes,
    readAt: options.readAt ?? host.readAt,
    yieldToEventLoop: options.yieldToEventLoop ?? (() => host.yield()),
    onSpillExec: host.spillR2Bucket
      ? async (handle) => {
          spillDriver ??= createPoolSpillDriver(wasm, handle, host, spillStats, options.hostBytes);
          await spillDriver.advance();
          pollIterations += 1;
          const { profile } = pool.stats();
          options.onProgress?.({
            pollIterations,
            inputBytes: profile.inputBytes,
            outputBytes: profile.outputBytes,
            wasmMemoryBytes: wasm.memory.buffer.byteLength,
            ...readSpillProgress(wasm),
          });
        }
      : undefined,
  });

  let retained = false;
  try {
    options.inputLatch?.bind(pool);
    options.onPoolReady?.(pool);
    const owner = options.onResidentReady;
    if (pool.resident && !owner) throw new Error("resident plan requires a graph owner");
    await Promise.all([
      pool.resident ? pool.ready : pool.finished,
      ...wirePoolOutputs(pool, host, options.layout.outputs),
    ]);
    if (pool.resident && owner) {
      owner({
        metadata: pool.response(),
        memory: () => readResidentMemory((kind, bytes) => pool.request(kind, bytes), wasm),
        request: (kind, bytes) => pool.request(kind, bytes),
        release: () => pool.close(),
      });
      retained = true;
    }
    const { profile } = pool.stats();
    return {
      inputsEndedAt: pool.inputsEndedAt(),
      inputBytes: profile.inputBytes,
      outputBytes: profile.outputBytes,
      maxWasmMemoryBytes: wasm.memory.buffer.byteLength,
      ...readSpillProgress(wasm),
      spillDiag: spillStats && { pollIterations, ...spillStats },
      dictTraffic: readDictTraffic(wasm),
      memGrowth: readMemGrowth(wasm),
    };
  } finally {
    if (!retained) await pool.close();
    // Only a task that spilled has runs to delete: the others skip the R2 LIST.
    if (spillDriver && host.spillR2Bucket && host.spillKeyPrefix) {
      // Uploads run in the background: let them land before listing what to delete.
      await spillDriver.settle();
      // Scratch cleanup must not replace the task's own outcome.
      await deleteSpillRuns(host.spillR2Bucket, host.spillKeyPrefix).catch((error: unknown) => {
        console.warn(JSON.stringify({ event: "spill_cleanup_failed", prefix: host.spillKeyPrefix, error: String(error) }));
      });
    }
  }
}
