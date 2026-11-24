/** Run one node program on the stream-pool engine, and stream its bytes back over RPC. */
import { HttpRangeSource } from "../../../src/runtime/host/ports/http-range.ts";
import { R2RangeSource } from "../../../src/runtime/host/ports/r2-read.ts";
import { readMemGrowth, runWcolEngine } from "../../../src/runtime/host/run-wcol-engine.ts";
import { claimingReadAt } from "../../../src/runtime/coordinator/unit-claims.ts";
import { readDictTraffic } from "../../../src/runtime/host/host-types.ts";
import { isolateId } from "../colo.ts";
import { createHostBytes } from "../../../src/runtime/host/host-bytes.ts";
import { createIsolatePeak, isolateLoad, joinIsolate } from "./isolate-load.ts";
import { type StreamPool } from "../../../src/runtime/host/stream-pool.ts";
import { parseR2BindingName, validateObjectKey, type R2Env, type R2OutputBucket } from "../r2.ts";
import { type NodeWasm } from "./session.ts";
import {
  type GraphJobSource,
  type GraphTaskEnv,
  type IsolateProgress,
  type RunProgramOptions,
  type TaskTiming,
} from "./types.ts";
import { recordTaskError, writeIsolateProgress, writeTaskTiming } from "./records.ts";

const STREAM_BUFFER_BYTES = 256 * 1024;

/** A macrotask yield. `Promise.resolve()` does not let a queued RPC enter the isolate. */
export function eventLoopYield(): Promise<void> {
  const scheduler = (globalThis as { scheduler?: { wait?: (delay: number) => Promise<void> } }).scheduler;
  if (typeof scheduler?.wait === "function") return scheduler.wait(0);
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

export function r2Bucket(env: R2Env, name: string) {
  const binding = parseR2BindingName(name);
  if (!binding) throw new Error(`unknown bucket ${name}`);
  if (!env.OUTPUT) throw new Error(`R2 binding ${binding} is missing`);
  return env.OUTPUT;
}

type SourceReader = {
  read(offset: number, length: number): Uint8Array | Promise<Uint8Array>;
};

/** Ask the job's coordinator for a scan's next unit of work. */
export function nextUnitFromCoordinator(env: GraphTaskEnv, jobId: string, attempt: number, taskId: number): Promise<number | null> {
  const coordinators = env.WCOL_COORDINATOR;
  if (!coordinators) throw new Error("scan work queues need the WCOL_COORDINATOR binding");
  const coordinator = coordinators.get(coordinators.idFromName(jobId)) as unknown as {
    nextUnit(params: { jobId: string; attempt: number; taskId: number }): Promise<number | null>;
  };
  return coordinator.nextUnit({ jobId, attempt, taskId });
}

export function sourceReader(env: GraphTaskEnv, source: GraphJobSource, etag: string | undefined): SourceReader {
  if ("url" in source) {
    const http = new HttpRangeSource(source.url, { rangeOnly: true, etag: etag || null });
    return {
      read: (offset, length) => http.read(offset, length),
    };
  }
  if (!validateObjectKey(source.key)) throw new Error("object key rejected");
  const bucket = r2Bucket(env, source.bucket);
  const range = new R2RangeSource(bucket, source.key, etag || undefined);
  return {
    read(offset, length) {
      const hit = range.readIfLoaded(offset, length);
      if (hit) return hit;
      return range.read(offset, length);
    },
  };
}

function byteStream(): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(
    {},
    new ByteLengthQueuingStrategy({ highWaterMark: STREAM_BUFFER_BYTES }),
    new ByteLengthQueuingStrategy({ highWaterMark: STREAM_BUFFER_BYTES }),
  );
}

export function streamed(
  waitUntil: (promise: Promise<unknown>) => void,
  meta: { env: R2Env; wasm: NodeWasm; jobId: string; attempt: number; taskId: number; slot: number },
  run: (write: (chunk: Uint8Array) => Promise<void>, chunks: () => number) => Promise<void>,
  onDone?: () => void,
  ownsSession: () => boolean = () => true,
  deliverError = false,
): ReadableStream<Uint8Array> {
  const rpc = byteStream();
  const writer = rpc.writable.getWriter();
  let written = 0;
  const work = run(
    async (chunk) => {
      await writer.write(chunk);
      written += 1;
    },
    () => written,
  )
    .then(() => writer.close())
    .catch(async (error) => {
      const message = error instanceof Error ? error.message : String(error);
      const admits = (error as { admits?: unknown }).admits;
      await recordTaskError(meta.env, meta.jobId, meta.attempt, meta.taskId, meta.slot, "stream", error, admits).catch(() => undefined);
      if (deliverError) {
        const body = new TextEncoder().encode(JSON.stringify({ error: message, admits: admits ?? [] }));
        await writer.write(body).catch(() => undefined);
        await writer.close().catch(() => writer.abort(error));
        return;
      }
      await writer.abort(error).catch(() => {});
    })
    .finally(() => {
      // A newer execution may already own this object. Do not reset its Wasm.
      if (!ownsSession()) return;
      onDone?.();
      meta.wasm.reset();
    });
  waitUntil(work);
  return rpc.readable;
}

/** Run one program. Attach producer bytes via `onPoolReady` or `inputLatch`. */
export async function runProgram(options: RunProgramOptions): Promise<TaskTiming> {
  const {
    env,
    params,
    poolLayout,
    write,
    chunks,
    closePort,
    openPort,
    onActivity,
    onYield,
    inputLatch,
    onPoolReady,
    onResidentReady,
    rpcAt,
  } = options;
  const runAt = Date.now();
  const wasmStarted = performance.now();
  const wasm = await options.wasm.load();
  const wasmLoadMs = performance.now() - wasmStarted;
  const readers = params.sources.map((source, index) => sourceReader(env, source, params.etags?.[index]));
  const started = performance.now();
  const startedAt = Date.now();
  let pool: StreamPool | undefined;
  let outcome: "ok" | "failed" = "failed";
  let inputWaitMs = 0;
  let writeWaitMs = 0;
  let lastProgressAt = 0;
  let progressInFlight = false;
  let lastProgress: IsolateProgress | undefined;
  let yieldCount = 0;
  const spillBoundaries: NonNullable<IsolateProgress["spillBoundaries"]> = [];
  const publishProgress = (partial: Omit<IsolateProgress, "at" | "wallMs">) => {
    onActivity?.();
    lastProgress = {
      at: Date.now(),
      wallMs: performance.now() - started,
      ...partial,
      retainedIngressBytes: 0,
      ingressLocks: 0,
    };
    const now = performance.now();
    if (progressInFlight || (lastProgressAt > 0 && now - lastProgressAt < 2000)) return;
    lastProgressAt = now;
    progressInFlight = true;
    void writeIsolateProgress(env, params.jobId, params.executionAttempt, params.taskId, lastProgress)
      .catch(() => undefined)
      .finally(() => {
        progressInFlight = false;
      });
  };
  let spillDiag: Awaited<ReturnType<typeof runWcolEngine>>["spillDiag"];
  const hostBytes = options.hostBytes ?? createHostBytes();
  const leaveIsolate = joinIsolate("reducer", params.taskId, () => ({
    wasmBytes: wasm.memory.buffer.byteLength,
    hostBytes: hostBytes.total(),
  }));
  const isolatePeak = createIsolatePeak();
  // Taken once, at open: a task that joins later sees this one and takes its own share.
  const budgetShare = isolateLoad().tasks;
  // A scan with claim units asks for its next unit through the same ReadAt.
  const readSource = claimingReadAt(
    async (source, offset, length) => {
      const reader = readers[source];
      if (!reader) throw new Error(`ReadAt source ${source} out of range (${readers.length} sources)`);
      return reader.read(Number(offset), length);
    },
    () => nextUnitFromCoordinator(env, params.jobId, params.executionAttempt, params.taskId),
  );
  // Spill-only progress left 90s gaps after retained-capacity reclaim (few runs).
  // Heartbeat on every yield so the coordinator stall watchdog sees live work.
  const heartbeat = async (): Promise<void> => {
    yieldCount += 1;
    onYield?.();
    isolatePeak.observe();
    publishProgress({
      pollIterations: lastProgress?.pollIterations ?? 0,
      inputBytes: lastProgress?.inputBytes ?? 0,
      outputBytes: lastProgress?.outputBytes ?? 0,
      wasmMemoryBytes: wasm.memory.buffer.byteLength,
      spillBoundaries,
      yieldCount,
      spillRuns: lastProgress?.spillRuns,
      spillPeakLiveBytes: lastProgress?.spillPeakLiveBytes,
      spillPeakInternerBytes: lastProgress?.spillPeakInternerBytes,
    });
    await eventLoopYield();
  };
  try {
    const stats = await runWcolEngine({
      wasm,
      plan: params.program,
      scratchBytes: 1024 * 1024,
      budgetShare,
      hostBytes,
      layout: poolLayout,
      inputLatch,
      onResidentReady,
      residentMemoryLimitBytes: onResidentReady && env.WCOL_RESIDENT_MEMORY_BYTES !== undefined
        ? Number(env.WCOL_RESIDENT_MEMORY_BYTES) : undefined,
      onPoolReady: (ready) => {
        pool = ready;
        onPoolReady?.(ready);
      },
      readAt: (source, offset, length) => readSource(source, Number(offset), length),
      yieldToEventLoop: heartbeat,
      onProgress: (info) => {
        const rows = info.spillBoundaries ?? [];
        while (spillBoundaries.length < rows.length) {
          const row = rows[spillBoundaries.length]!;
          spillBoundaries.push({
            ...row,
            hostRetainedBytes: 0,
          });
        }
        publishProgress({
          pollIterations: info.pollIterations,
          inputBytes: info.inputBytes,
          outputBytes: info.outputBytes,
          wasmMemoryBytes: info.wasmMemoryBytes,
          spillBoundaries,
          yieldCount,
          spillRuns: info.spillRuns,
          spillPeakLiveBytes: info.spillPeakLiveBytes,
          spillPeakInternerBytes: info.spillPeakInternerBytes,
        });
      },
      host: {
        readAt: readSource,
        writePort: (port, chunk) => {
          const wait = performance.now();
          const wrote = write(port, chunk);
          const done = (): void => {
            writeWaitMs += performance.now() - wait;
            if (params.failTaskId === params.taskId && chunks() === 1) {
              throw new Error("injected mid-stream task failure");
            }
          };
          if (wrote && typeof wrote.then === "function") return wrote.then(done);
          done();
        },
        closePort,
        openPort,
        spillR2Bucket: env.OUTPUT ?? undefined,
        spillKeyPrefix: `_jobs/${params.jobId}/a${params.executionAttempt}/t${params.taskId}/`,
        yield: heartbeat,
      },
    });
    spillDiag = stats.spillDiag;
    if (lastProgress) {
      await writeIsolateProgress(env, params.jobId, params.executionAttempt, params.taskId, {
        ...lastProgress,
        memGrowth: readMemGrowth(wasm),
        dictTraffic: readDictTraffic(wasm),
      });
    }
    outcome = "ok";
    const profile = pool?.stats().profile;
    return {
      wallMs: performance.now() - started,
      sourceReads: profile?.reads ?? 0,
      sourceReadBytes: profile?.readBytes ?? 0,
      wasmLoadMs,
      inputWaitMs,
      writeWaitMs,
      readAtWaitMs: 0,
      readAtCalls: 0,
      inputBytes: stats.inputBytes,
      outputBytes: stats.outputBytes,
      wasmMemoryBytes: stats.maxWasmMemoryBytes,
      residentGroups: stats.residentGroups ?? 0,
      residentGroupBytes: stats.residentGroupBytes ?? 0,
      spillRuns: stats.spillRuns ?? 0,
      spillGroupsFlushed: stats.spillGroupsFlushed ?? 0,
      spillPeakLiveBytes: stats.spillPeakLiveBytes ?? 0,
      spillPeakInternerBytes: stats.spillPeakInternerBytes ?? 0,
      spillDiag: stats.spillDiag,
    };
  } catch (error) {
    if (lastProgress) {
      await writeIsolateProgress(env, params.jobId, params.executionAttempt, params.taskId, {
        ...lastProgress,
        memGrowth: readMemGrowth(wasm),
        dictTraffic: readDictTraffic(wasm),
      }).catch(() => undefined);
    }
    await recordTaskError(env, params.jobId, params.executionAttempt, params.taskId, params.slot, "runProgram", error);
    throw error;
  } finally {
    leaveIsolate();
    // PROFILE: one line per gather task or reducer. Logs only: no billed writes.
    const poolStats = pool?.stats();
    console.log(JSON.stringify({
      event: "task_profile",
      isolate: isolateId(),
      jobId: params.jobId,
      attempt: params.executionAttempt,
      taskId: params.taskId,
      inputs: poolLayout.inputs,
      outcome,
      rpcAt: rpcAt ?? runAt,
      runAt,
      wasmMs: Math.round(wasmLoadMs),
      inputsEndedAt: pool?.inputsEndedAt(),
      poolDoneAt: Date.now(),
      profile: poolStats?.profile,
      steps: poolStats?.steps,
      spillIo: spillDiag?.io,
      wasmMemoryBytes: wasm.memory.buffer.byteLength,
      hostBytes: hostBytes.snapshot(),
      budgetShare,
      isolatePeak: isolatePeak.peak(),
    }));
    // One record per task, success or failure: the coordinator folds the
    // ingest/finish split into the job's stage timeline.
    await writeTaskTiming(env, params, {
      slots: [params.slot],
      outcome,
      startedAt,
      inputsEndedAt: pool?.inputsEndedAt(),
      endedAt: Date.now(),
      wasmLoadMs,
      writeWaitMs,
      yieldCount,
      spillRuns: lastProgress?.spillRuns ?? 0,
      sourceReads: poolStats?.profile.reads,
      sourceReadBytes: poolStats?.profile.readBytes,
      spillIo: spillDiag?.io,
    }).catch(() => undefined);
  }
}

export function outputBucketFor(env: GraphTaskEnv, bindingName: string): R2OutputBucket {
  if (bindingName) return r2Bucket(env, bindingName);
  if (!env.OUTPUT) throw new Error("R2 output binding is missing");
  return env.OUTPUT;
}
