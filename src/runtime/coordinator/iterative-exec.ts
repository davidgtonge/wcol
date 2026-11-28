import { createLimiter } from "../concurrency.ts";

export type IterativeResident = {
  readonly slot: number;
  runRound(params: {
    jobId: string;
    attempt: number;
    sessionId: string;
    iteration: number;
    globalState: Uint8Array;
  }): Promise<Uint8Array>;
  release(params: {
    jobId: string;
    attempt: number;
    sessionId: string;
  }): Promise<void>;
};

export type IterativeKernel<State, Partial> = {
  encodeState(state: State): Uint8Array;
  decodePartial(bytes: Uint8Array): Partial;
  merge(left: Partial, right: Partial): Partial;
  update(state: State, partial: Partial): { state: State; converged: boolean };
};

export type IterativeCheckpoint<State> = {
  load(): Promise<{
    iteration: number;
    state: State;
    converged?: boolean;
  } | null>;
  /** Persist a barrier only after every worker partial has been merged and applied. */
  commit(params: {
    attempt: number;
    iteration: number;
    state: State;
    converged: boolean;
  }): Promise<void>;
};

export type IterativeExecResult<State> = {
  state: State;
  iterations: number;
  converged: boolean;
};

/**
 * Run a resident BSP loop over nodes that have completed their one-time load phase.
 * Every round is attempt- and iteration-fenced. Partials merge in deterministic bounded
 * levels; only a committed global barrier is eligible for recovery.
 */
export async function runIterativeExec<State, Partial>(options: {
  readonly jobId: string;
  readonly attempt: number;
  readonly sessionId: string;
  readonly residents: readonly IterativeResident[];
  readonly kernel: IterativeKernel<State, Partial>;
  readonly initialState: State;
  readonly maxIterations: number;
  readonly fanIn: number;
  readonly rpcConcurrency: number;
  readonly checkpoint: IterativeCheckpoint<State>;
}): Promise<IterativeExecResult<State>> {
  if (
    !options.jobId ||
    !options.sessionId ||
    options.attempt < 1 ||
    options.maxIterations < 1
  ) {
    throw new Error("invalid iterative execution identity or iteration limit");
  }
  if (options.residents.length === 0)
    throw new Error("iterative execution has no resident workers");
  if (options.fanIn < 2 || options.rpcConcurrency < 1)
    throw new Error("invalid iterative execution concurrency");
  const checkpoint = await options.checkpoint.load();
  let state = checkpoint?.state ?? options.initialState;
  let iteration = checkpoint?.iteration ?? 0;
  if (iteration > options.maxIterations)
    throw new Error("checkpoint exceeds iteration limit");
  const limit = createLimiter(options.rpcConcurrency);
  let converged = checkpoint?.converged ?? false;
  let executionFailed = false;
  try {
    while (iteration < options.maxIterations && !converged) {
      const globalState = options.kernel.encodeState(state);
      const results = await Promise.allSettled(
        options.residents.map((resident) =>
          limit(() =>
            resident.runRound({
              jobId: options.jobId,
              attempt: options.attempt,
              sessionId: options.sessionId,
              iteration: iteration + 1,
              globalState,
            }),
          ),
        ),
      );
      // Drain every RPC before releasing resident graphs or retrying the barrier.
      const failed = results.find(
        (result): result is PromiseRejectedResult =>
          result.status === "rejected",
      );
      if (failed) throw failed.reason;
      const partials = results.flatMap((result) =>
        result.status === "fulfilled"
          ? [options.kernel.decodePartial(result.value)]
          : [],
      );
      const merged = mergeTree(partials, options.fanIn, options.kernel.merge);
      const updated = options.kernel.update(state, merged);
      state = updated.state;
      iteration += 1;
      converged = updated.converged;
      await options.checkpoint.commit({
        attempt: options.attempt,
        iteration,
        state,
        converged,
      });
    }
    return { state, iterations: iteration, converged };
  } catch (error) {
    executionFailed = true;
    throw error;
  } finally {
    const settled = await Promise.allSettled(
      options.residents.map((resident) =>
        limit(() =>
          resident.release({
            jobId: options.jobId,
            attempt: options.attempt,
            sessionId: options.sessionId,
          }),
        ),
      ),
    );
    const failed = settled.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (failed && !executionFailed) throw failed.reason;
  }
}

function mergeTree<T>(
  values: T[],
  fanIn: number,
  merge: (left: T, right: T) => T,
): T {
  if (values.length === 0)
    throw new Error("iterative round returned no partials");
  while (values.length > 1) {
    const next: T[] = [];
    for (let at = 0; at < values.length; at += fanIn) {
      let current: T = values[at]!;
      const end = Math.min(values.length, at + fanIn);
      for (let index = at + 1; index < end; index++)
        current = merge(current, values[index]!);
      next.push(current);
    }
    values = next;
  }
  return values[0]!;
}
