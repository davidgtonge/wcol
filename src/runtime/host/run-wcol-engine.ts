import type { WcolStreamWasm } from "./wasm-stream.ts";
import { INPUT_RINGS, OUTPUT_SLOTS, portKiB } from "./port-budget.ts";
import { createStreamPool } from "./stream-pool.ts";
import { readResidentMemory } from "./resident-graph.ts";
import {
  readDictTraffic,
  readMemGrowth,
  readSpillBoundaries,
  type RunWcolEngineOptions,
  type SpillBoundary,
  type StreamRunStats,
  type WcolHost,
} from "./host-types.ts";
import {
  createPoolSpillDriver,
  deleteSpillRuns,
  emptySpillIo,
} from "./spill-r2-pool-driver.ts";

export type {
  DictTraffic,
  MemGrowthSample,
  RunWcolEngineOptions,
  SpillBoundary,
  StreamRunStats,
  WcolHost,
} from "./host-types.ts";
export {
  readDictTraffic,
  readMemGrowth,
  readSpillBoundaries,
} from "./host-types.ts";

function wasmU64(fn?: () => number): number | undefined {
  if (!fn) return undefined;
  return Number(fn());
}

function readSpillProgress(
  wasm: WcolStreamWasm,
): Pick<
  StreamRunStats,
  | "spillRuns"
  | "spillGroupsFlushed"
  | "spillPeakLiveBytes"
  | "spillPeakInternerBytes"
  | "spillBoundaries"
> {
  return {
    spillRuns: wasmU64(wasm.wcs_last_spill_runs),
    spillGroupsFlushed: wasmU64(wasm.wcs_last_spill_groups_flushed),
    spillPeakLiveBytes: wasmU64(wasm.wcs_last_spill_peak_live_bytes),
    spillPeakInternerBytes: wasmU64(wasm.wcs_last_spill_peak_interner_bytes),
    spillBoundaries: readSpillBoundaries(wasm),
  };
}

function wirePoolOutputs(
  pool: ReturnType<typeof createStreamPool>,
  host: WcolHost,
  outputs: number,
  stats: { outputBytes: number },
): Promise<void>[] {
  return Array.from({ length: outputs }, (_, port) =>
    (async () => {
      const reader = pool.outputStreams[port]!.getReader();
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value.byteLength > 0) {
            await host.writePort(port, value);
            stats.outputBytes += value.byteLength;
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
    })(),
  );
}

/** Run one graph program on the stream pool (only supported host path). */
export async function runWcolEngine(
  options: RunWcolEngineOptions,
): Promise<StreamRunStats> {
  const scratchBytes = options.scratchBytes ?? 256 * 1024;
  const spillStats = options.host.spillR2Bucket
    ? {
        spillExec: 0,
        spillExecByOp: { begin: 0, append: 0, scan: 0, end: 0, commit: 0 },
        io: emptySpillIo(),
      }
    : undefined;
  const host = options.host;

  const readAt =
    options.readAt ??
    (host.readAt
      ? (source: number, offset: bigint, length: number) =>
          host.readAt!(source, Number(offset), length)
      : undefined);

  let spillDriver: ReturnType<typeof createPoolSpillDriver> | undefined;
  let pollIterations = 0;
  const ioStats = { inputBytes: 0, outputBytes: 0 };

  const yieldToEventLoop =
    options.yieldToEventLoop ??
    (async () => {
      await host.yield();
    });

  const pool = createStreamPool({
    inputs: options.layout.inputs,
    outputs: options.layout.outputs,
    inputSize: options.inputSizeKiB ?? portKiB(INPUT_RINGS, options.layout.inputs),
    outputSize: options.outputSizeKiB ?? portKiB(OUTPUT_SLOTS, options.layout.outputs),
    wasm: options.wasm,
    residentMemoryLimitBytes: options.residentMemoryLimitBytes,
    graph: { program: options.plan, scratchBytes, budgetShare: options.budgetShare },
    hostBytes: options.hostBytes,
    readAt,
    yieldToEventLoop,
    onSpillExec: host.spillR2Bucket
      ? async (handle) => {
          if (!spillDriver) {
            spillDriver = createPoolSpillDriver(
              options.wasm,
              handle,
              host,
              spillStats,
              options.hostBytes,
            );
          }
          await spillDriver.advance();
          pollIterations += 1;
          options.onProgress?.({
            pollIterations,
            inputBytes: ioStats.inputBytes,
            outputBytes: ioStats.outputBytes,
            wasmMemoryBytes: options.wasm.memory.buffer.byteLength,
            ...readSpillProgress(options.wasm),
            residentGroups: wasmU64(options.wasm.wcs_last_resident_groups),
            residentGroupBytes: wasmU64(
              options.wasm.wcs_last_resident_group_bytes,
            ),
          });
        }
      : undefined,
  });

  let retained = false;

  try {
    options.inputLatch?.bind(pool);
    options.onPoolReady?.(pool);
    const owner = options.onResidentReady;
    if (pool.resident && !owner)
      throw new Error("resident plan requires a graph owner");
    const outputTasks = wirePoolOutputs(
      pool,
      host,
      options.layout.outputs,
      ioStats,
    );
    await Promise.all([
      pool.resident ? pool.ready : pool.finished,
      ...outputTasks,
    ]);
    if (pool.resident && owner) {
      owner({
        metadata: pool.response(),
        memory: () =>
          readResidentMemory(
            (kind, bytes) => pool.request(kind, bytes),
            options.wasm,
          ),
        request: (kind, bytes) => pool.request(kind, bytes),
        release: () => pool.close(),
      });
      retained = true;
    }
    return {
      inputsEndedAt: pool.inputsEndedAt(),
      inputBytes: ioStats.inputBytes,
      outputBytes: ioStats.outputBytes,
      maxWasmMemoryBytes: options.wasm.memory.buffer.byteLength,
      ...readSpillProgress(options.wasm),
      spillDiag: spillStats
        ? {
            pollIterations,
            spillExec: spillStats.spillExec,
            spillExecByOp: spillStats.spillExecByOp,
            io: spillStats.io,
          }
        : undefined,
      residentGroups: wasmU64(options.wasm.wcs_last_resident_groups),
      residentGroupBytes: wasmU64(options.wasm.wcs_last_resident_group_bytes),
      dictTraffic: readDictTraffic(options.wasm),
      memGrowth: readMemGrowth(options.wasm),
    };
  } finally {
    if (!retained) await pool.close();
    if (host.spillR2Bucket && host.spillKeyPrefix) {
      // Uploads run in the background: let them land before listing what to delete.
      await spillDriver?.settle();
      // Scratch cleanup must not replace the task's own outcome.
      await deleteSpillRuns(host.spillR2Bucket, host.spillKeyPrefix).catch(
        (error: unknown) => {
          console.warn(
            JSON.stringify({
              event: "spill_cleanup_failed",
              prefix: host.spillKeyPrefix,
              error: String(error),
            }),
          );
        },
      );
    }
  }
}
