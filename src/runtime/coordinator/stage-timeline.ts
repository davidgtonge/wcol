/**
 * Per-job stage timeline. The coordinator observes every stage boundary, so it
 * records spans as work starts and ends; summaries are derived, never stored twice.
 */

export type StageName =
  | "plan"
  | "prepareReducer"
  | "scanWait"
  | "scan"
  | "completeScan"
  | "seal"
  | "finalize"
  | "gatherTask"
  | "reducerIngest"
  | "reducerFinish"
  | "iterativeModel"
  | "iterativeLoad"
  | "iterativeNormalize"
  | "iterativeRound";

export type StageIds = {
  readonly taskId?: number;
  readonly scanIndex?: number;
};

export type StageOutcome = "ok" | "failed";

export type StageSpan = StageIds & {
  readonly stage: StageName;
  readonly startMs: number;
  readonly endMs: number;
  readonly outcome: StageOutcome;
};

/** A span that has started and not yet ended. */
export type OpenSpan = StageIds & {
  readonly stage: StageName;
  readonly startMs: number;
};

export type StageTimeline = {
  /** Run `work` and record its span, including when it fails. */
  span<T>(stage: StageName, ids: StageIds, work: () => Promise<T>): Promise<T>;
  /** Record a span measured elsewhere (eg. a queue wait, or reported by a node). */
  record(span: StageSpan): void;
  /** The timeline's clock, for spans recorded with `record`. */
  now(): number;
  spans(): readonly StageSpan[];
  /** Spans in progress, for a live view of the attempt. */
  open(): readonly OpenSpan[];
};

export function createStageTimeline(clock: () => number = Date.now): StageTimeline {
  const spans: StageSpan[] = [];
  const open = new Set<OpenSpan>();
  return {
    async span(stage, ids, work) {
      const started: OpenSpan = { stage, ...ids, startMs: clock() };
      open.add(started);
      let outcome: StageOutcome = "failed";
      try {
        const value = await work();
        outcome = "ok";
        return value;
      } finally {
        open.delete(started);
        spans.push({ ...started, endMs: clock(), outcome });
      }
    },
    record(span) {
      spans.push(span);
    },
    spans: () => spans,
    open: () => [...open],
    now: clock,
  };
}

export type StageSummary = {
  readonly stage: StageName;
  readonly count: number;
  readonly failed: number;
  /** First start and last end, relative to the job's first span. */
  readonly fromMs: number;
  readonly toMs: number;
  /** Sum of span durations; above `toMs - fromMs` when spans overlap. */
  readonly totalMs: number;
  readonly maxMs: number;
};

/** One row per stage, in order of first start. */
export function summarizeStages(spans: readonly StageSpan[]): StageSummary[] {
  if (spans.length === 0) return [];
  const origin = Math.min(...spans.map((span) => span.startMs));
  const byStage = new Map<StageName, StageSpan[]>();
  for (const span of spans) {
    const group = byStage.get(span.stage);
    if (group) group.push(span);
    else byStage.set(span.stage, [span]);
  }
  return [...byStage.entries()]
    .map(([stage, group]) => {
      const durations = group.map((span) => span.endMs - span.startMs);
      return {
        stage,
        count: group.length,
        failed: group.filter((span) => span.outcome === "failed").length,
        fromMs: Math.min(...group.map((span) => span.startMs)) - origin,
        toMs: Math.max(...group.map((span) => span.endMs)) - origin,
        totalMs: durations.reduce((sum, ms) => sum + ms, 0),
        maxMs: Math.max(...durations),
      };
    })
    .sort((left, right) => left.fromMs - right.fromMs);
}
