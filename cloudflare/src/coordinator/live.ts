/**
 * The console's view of a job, and its estimated cost. Pure functions of a snapshot: the
 * physical graph, the attempt's spans (ended and in progress) and what tasks reported.
 * The coordinator serves the snapshot; the page builds the view from it at any moment, so a
 * live job and a replay of a finished one are drawn by the same code.
 */
import type { OpenSpan, StageSpan } from "../../../src/runtime/coordinator/stage-timeline.ts";
import { CONTAINER_SPECS, type ContainerInstance } from "../node/containers.ts";
import type { ReducerPlacementFacts } from "../node/types.ts";

/** What a task reports about itself: where it ran and what it moved. Times are epoch ms. */
export type TaskFacts =
  | {
      kind: "scan";
      /** When the coordinator had the report: the scan's end. */
      reportedAt: number;
      colo: string;
      sourceReads: number;
      sourceReadBytes: number;
      /** Bytes sent to each partition's reducer, keyed by partition (none for a gather). */
      partitionBytes: Readonly<Record<string, number>>;
      /** Each partition edge's send, and when its reducer had taken it all. */
      edgeTimes: Readonly<Record<string, readonly [number, number]>>;
      /** Everything the task sent: its partitions, or a gather task's partial result. */
      outputBytes: number;
    }
  | ({ kind: "reducer"; reportedAt: number } & ReducerPlacementFacts);

export type LiveGraph = {
  tasks: readonly { id: number; slot: number; role: "scan" | "reducer" }[];
  edges: readonly { from: number; to: number; partition?: number }[];
  rootTaskId: number;
};

/**
 * The graph as the console draws it: which tasks scan, which reduce, and their edges. The
 * root is the finalizer, drawn on its own, so it is not one of the tasks.
 */
export function liveGraph(
  plan: { tasks: readonly { id: number; slot: number; program: Uint8Array }[]; edges: readonly LiveGraph["edges"][number][]; rootTaskId: number },
  isReducer: (program: Uint8Array) => boolean,
): LiveGraph {
  return {
    tasks: plan.tasks
      .filter((task) => task.id !== plan.rootTaskId)
      .map((task) => ({ id: task.id, slot: task.slot, role: isReducer(task.program) ? "reducer" : "scan" })),
    edges: plan.edges.map((edge) => ({ from: edge.from, to: edge.to, ...(edge.partition == null ? {} : { partition: edge.partition }) })),
    rootTaskId: plan.rootTaskId,
  };
}

export type ReducerPlacement = { runtime: "node" | "container"; instance?: ContainerInstance };

/** What `GET /jobs/:id/live` returns: everything the view is built from. */
export type LiveSnapshot = {
  jobId: string;
  state: string;
  attempt: number;
  startedAt: number;
  /** The coordinator's clock when it answered, or the job's end once it ended. */
  now: number;
  graph: LiveGraph;
  spans: readonly StageSpan[];
  open: readonly OpenSpan[];
  facts: readonly (readonly [number, TaskFacts])[];
  reducers: ReducerPlacement;
  reducerReason: string | null;
  coordinatorColo: string | null;
  /** Size of the job's source objects. */
  sourceBytes: number;
  error: string | null;
  result: unknown;
};

export type LiveInput = {
  jobId: string;
  state: string;
  attempt: number;
  startedAt: number;
  now: number;
  graph: LiveGraph;
  spans: readonly StageSpan[];
  open: readonly OpenSpan[];
  facts: ReadonlyMap<number, TaskFacts>;
  reducers: ReducerPlacement;
  coordinatorColo: string | null;
  sourceBytes: number;
};

/**
 * The snapshot as it stood at `t` (epoch ms), for replay: spans that end later are still
 * open, spans that start later have not begun, and facts appear when they were reported.
 */
export function inputAt(snapshot: LiveSnapshot, t: number): LiveInput {
  const at = Math.min(t, snapshot.now);
  const started = (span: { startMs: number }) => span.startMs <= at;
  return {
    jobId: snapshot.jobId,
    state: at < snapshot.now ? "running" : snapshot.state,
    attempt: snapshot.attempt,
    startedAt: snapshot.startedAt,
    now: at,
    graph: snapshot.graph,
    spans: snapshot.spans.filter((span) => span.endMs <= at),
    open: [
      ...snapshot.open.filter(started),
      ...snapshot.spans
        .filter((span) => started(span) && span.endMs > at)
        .map(({ stage, taskId, scanIndex, startMs }) => ({ stage, taskId, scanIndex, startMs })),
    ],
    facts: new Map(snapshot.facts.filter(([, facts]) => facts.reportedAt <= at)),
    reducers: snapshot.reducers,
    coordinatorColo: snapshot.coordinatorColo,
    sourceBytes: snapshot.sourceBytes,
  };
}

export type TaskState = "waiting" | "starting" | "running" | "done" | "failed";

export type LiveTask = {
  id: number;
  role: "scan" | "reducer" | "finalizer";
  /** Position among tasks of its role: scan 0, reducer 0 and so on. */
  index: number;
  state: TaskState;
  /** Milliseconds from the attempt's start. */
  startMs: number | null;
  endMs: number | null;
  /** When a reducer was ready to take input: the end of its prepare. */
  readyAtMs: number | null;
  colo: string | null;
  runtime: "node" | "container" | "coordinator";
  instance: ContainerInstance | null;
  /** A container reducer's start-up time. */
  readyMs: number | null;
  sourceReads: number;
  sourceReadBytes: number;
  bytesIn: number;
  bytesOut: number;
};

export type EdgeState = "idle" | "flowing" | "done";

export type LiveEdge = {
  from: number;
  to: number;
  partition: number | null;
  bytes: number;
  state: EdgeState;
  /** Send and end, ms from the attempt's start, when the scan reported them. */
  startMs: number | null;
  endMs: number | null;
};

export type CostItem = { label: string; detail: string; usd: number };
export type CostEstimate = { usd: number; items: CostItem[]; athenaUsd: number };

/** A coordinator stage, for the timeline's top lane. */
export type CoordinatorPhase = { stage: string; startMs: number; endMs: number | null };

export type LiveJobView = {
  jobId: string;
  state: string;
  attempt: number;
  elapsedMs: number;
  reducers: ReducerPlacement;
  tasks: LiveTask[];
  edges: LiveEdge[];
  phases: CoordinatorPhase[];
  totals: {
    fileBytes: number;
    sourceReads: number;
    sourceReadBytes: number;
    shuffleBytes: number;
    scans: number;
    reducers: number;
  };
  /** Tasks per data centre. */
  colos: { colo: string; scans: number; reducers: number; coordinator: boolean }[];
  /** Bytes between data centres, scan location to the location it sent to. */
  flows: { from: string; to: string; bytes: number }[];
  cost: CostEstimate;
};

/** Published list prices (USD) the estimate uses. */
export const PRICES = {
  doRequestPerMillion: 0.15,
  /** Durable Object duration is billed per GB-second of 128 MB objects. */
  doGbSecondPerMillion: 12.5,
  doGbPerObject: 0.128,
  r2ClassBPerMillion: 0.36,
  containerVcpuSecond: 0.00002,
  containerGibSecond: 0.0000025,
  containerDiskGbSecond: 0.00000007,
  /** Athena bills bytes scanned, at least 10 MB a query. */
  athenaPerTb: 5,
  athenaMinBytes: 10_000_000,
} as const;

type SpanIndex = { ended: Map<string, StageSpan>; open: Map<string, OpenSpan> };

const key = (stage: string, taskId: number | undefined) => `${stage}:${taskId ?? "-"}`;

function indexSpans(spans: readonly StageSpan[], open: readonly OpenSpan[]): SpanIndex {
  // A retried span ends more than once in an attempt; the last one is current.
  const ended = new Map(spans.map((span) => [key(span.stage, span.taskId), span] as const));
  return { ended, open: new Map(open.map((span) => [key(span.stage, span.taskId), span] as const)) };
}

type Phase = { state: TaskState; startMs: number | null; endMs: number | null };

/** A task's phase from one stage's span: waiting, running, or ended ok or failed. */
function phaseOf(index: SpanIndex, stage: string, taskId: number | undefined, base: number): Phase {
  const ended = index.ended.get(key(stage, taskId));
  if (ended) return { state: ended.outcome === "ok" ? "done" : "failed", startMs: ended.startMs - base, endMs: ended.endMs - base };
  const open = index.open.get(key(stage, taskId));
  return open ? { state: "running", startMs: open.startMs - base, endMs: null } : { state: "waiting", startMs: null, endMs: null };
}

const sum = (values: readonly number[]) => values.reduce((total, value) => total + value, 0);

export function liveJobView(input: LiveInput): LiveJobView {
  const base = input.startedAt;
  const index = indexSpans(input.spans, input.open);
  const finalize = phaseOf(index, "finalize", input.graph.rootTaskId, base);
  const scanFacts = (id: number) => {
    const facts = input.facts.get(id);
    return facts?.kind === "scan" ? facts : undefined;
  };
  // A partition edge carries its partition's bytes; a gather edge, the scan's whole output.
  const edgeBytes = (edge: LiveGraph["edges"][number]) => {
    const facts = scanFacts(edge.from);
    return edge.partition == null ? (facts?.outputBytes ?? 0) : (facts?.partitionBytes[String(edge.partition)] ?? 0);
  };
  const roleIndex = new Map<string, number>();
  const nextIndex = (role: string) => {
    const position = roleIndex.get(role) ?? 0;
    roleIndex.set(role, position + 1);
    return position;
  };

  const tasks: LiveTask[] = input.graph.tasks.map((task) => {
    const facts = input.facts.get(task.id);
    if (task.role === "scan") {
      // A hash graph's scans stream to reducers; a gather graph's tasks are gathered whole.
      const streamed = phaseOf(index, "scan", task.id, base);
      const phase = streamed.state === "waiting" ? phaseOf(index, "gatherTask", task.id, base) : streamed;
      const scan = facts?.kind === "scan" ? facts : undefined;
      return {
        id: task.id,
        role: "scan",
        index: nextIndex("scan"),
        ...phase,
        readyAtMs: null,
        colo: facts?.colo ?? null,
        runtime: "node",
        instance: null,
        readyMs: null,
        sourceReads: scan?.sourceReads ?? 0,
        sourceReadBytes: scan?.sourceReadBytes ?? 0,
        bytesIn: 0,
        bytesOut: scan?.outputBytes ?? 0,
      };
    }
    const prepare = phaseOf(index, "prepareReducer", task.id, base);
    // A reducer starts while it is prepared, then runs until the finalizer has its output.
    const state: TaskState =
      prepare.state === "failed" || finalize.state === "failed"
        ? "failed"
        : prepare.state === "running"
          ? "starting"
          : prepare.state === "waiting"
            ? "waiting"
            : finalize.state === "done"
              ? "done"
              : "running";
    const reducer = facts?.kind === "reducer" ? facts : undefined;
    return {
      id: task.id,
      role: "reducer",
      index: nextIndex("reducer"),
      state,
      startMs: prepare.startMs,
      endMs: state === "done" ? finalize.endMs : null,
      readyAtMs: prepare.state === "done" ? prepare.endMs : null,
      colo: reducer?.colo ?? null,
      runtime: reducer?.runtime ?? input.reducers.runtime,
      instance: reducer?.runtime === "container" ? reducer.instance : (input.reducers.instance ?? null),
      readyMs: reducer?.runtime === "container" ? reducer.readyMs : null,
      sourceReads: 0,
      sourceReadBytes: 0,
      bytesIn: sum(input.graph.edges.filter((edge) => edge.to === task.id).map(edgeBytes)),
      bytesOut: 0,
    };
  });
  tasks.push({
    id: input.graph.rootTaskId,
    role: "finalizer",
    index: 0,
    ...finalize,
    readyAtMs: null,
    colo: input.coordinatorColo,
    runtime: "coordinator",
    instance: null,
    readyMs: null,
    sourceReads: 0,
    sourceReadBytes: 0,
    bytesIn: 0,
    bytesOut: 0,
  });

  const byId = new Map(tasks.map((task) => [task.id, task] as const));
  const now = input.now;
  const edges: LiveEdge[] = input.graph.edges.map((edge) => {
    const times = edge.partition == null ? undefined : scanFacts(edge.from)?.edgeTimes[String(edge.partition)];
    const from = byId.get(edge.from);
    const to = byId.get(edge.to);
    const state: EdgeState = times
      ? times[1] <= now
        ? "done"
        : times[0] <= now
          ? "flowing"
          : "idle"
      : from?.state === "done"
        ? "done"
        : from?.state === "running" && to?.state === "running"
          ? "flowing"
          : "idle";
    return {
      from: edge.from,
      to: edge.to,
      partition: edge.partition ?? null,
      bytes: edgeBytes(edge),
      state,
      startMs: times ? times[0] - base : null,
      endMs: times ? times[1] - base : null,
    };
  });

  const scans = tasks.filter((task) => task.role === "scan");
  const reducers = tasks.filter((task) => task.role === "reducer");
  const elapsedMs = now - base;
  const sourceReads = sum(scans.map((task) => task.sourceReads));
  const sourceReadBytes = sum(scans.map((task) => task.sourceReadBytes));
  const coordinatorStages = new Set(["plan", "seal", "finalize"]);
  return {
    jobId: input.jobId,
    state: input.state,
    attempt: input.attempt,
    elapsedMs,
    reducers: input.reducers,
    tasks,
    edges,
    phases: [
      ...input.spans.map((span) => ({ stage: span.stage, startMs: span.startMs - base, endMs: span.endMs - base })),
      ...input.open.map((span) => ({ stage: span.stage, startMs: span.startMs - base, endMs: null })),
    ].filter((phase) => coordinatorStages.has(phase.stage)),
    totals: {
      fileBytes: input.sourceBytes,
      sourceReads,
      sourceReadBytes,
      shuffleBytes: sum(edges.map((edge) => edge.bytes)),
      scans: scans.length,
      reducers: reducers.length,
    },
    colos: coloSummary(tasks),
    flows: coloFlows(edges, byId),
    cost: estimateCost({
      elapsedMs,
      scanMs: scans.map((task) => activeMs(task, elapsedMs)),
      reducerMs: reducers.map((task) => activeMs(task, elapsedMs)),
      rpcCalls: input.spans.length + input.open.length + edges.filter((edge) => edge.state !== "idle").length,
      sourceReads,
      sourceReadBytes,
      container: input.reducers.runtime === "container" ? (input.reducers.instance ?? null) : null,
    }),
  };
}

/** How long a task has been active, ended or not. */
function activeMs(task: LiveTask, elapsedMs: number): number {
  return task.startMs == null ? 0 : (task.endMs ?? elapsedMs) - task.startMs;
}

function coloSummary(tasks: readonly LiveTask[]): LiveJobView["colos"] {
  const colos = new Map<string, { scans: number; reducers: number; coordinator: boolean }>();
  for (const task of tasks) {
    if (!task.colo) continue;
    const entry = colos.get(task.colo) ?? { scans: 0, reducers: 0, coordinator: false };
    if (task.role === "scan") entry.scans++;
    else if (task.role === "reducer") entry.reducers++;
    else entry.coordinator = true;
    colos.set(task.colo, entry);
  }
  return [...colos].map(([colo, counts]) => ({ colo, ...counts })).sort((a, b) => b.scans + b.reducers - (a.scans + a.reducers));
}

/** Edge bytes summed by the data centres at either end; edges with an unknown end are left out. */
function coloFlows(edges: readonly LiveEdge[], byId: ReadonlyMap<number, LiveTask>): LiveJobView["flows"] {
  const flows = new Map<string, { from: string; to: string; bytes: number }>();
  for (const edge of edges) {
    const from = byId.get(edge.from)?.colo;
    const to = byId.get(edge.to)?.colo;
    if (!from || !to || edge.bytes === 0) continue;
    const entry = flows.get(`${from}>${to}`) ?? { from, to, bytes: 0 };
    entry.bytes += edge.bytes;
    flows.set(`${from}>${to}`, entry);
  }
  return [...flows.values()].sort((a, b) => b.bytes - a.bytes);
}

export type CostUsage = {
  elapsedMs: number;
  scanMs: readonly number[];
  reducerMs: readonly number[];
  /** Durable Object calls: one per span, plus each edge's accept. */
  rpcCalls: number;
  sourceReads: number;
  sourceReadBytes: number;
  /** Instance type when reducers run in containers. */
  container: ContainerInstance | null;
};

/**
 * Cost at list prices. Durable Objects bill requests and 128 MB of duration while active; R2
 * bills reads; a container bills its vCPU, memory and disk while it runs (CPU is billed on
 * use, so charging the full vCPU is an upper bound). Athena, for comparison, bills the bytes
 * it scans.
 */
export function estimateCost(usage: CostUsage): CostEstimate {
  const seconds = (ms: readonly number[]) => sum(ms) / 1000;
  const doSeconds = usage.elapsedMs / 1000 + seconds(usage.scanMs) + seconds(usage.reducerMs);
  const reducerSeconds = seconds(usage.reducerMs);
  const items: CostItem[] = [
    {
      label: "Durable Object requests",
      detail: `${usage.rpcCalls} calls`,
      usd: (usage.rpcCalls * PRICES.doRequestPerMillion) / 1e6,
    },
    {
      label: "Durable Object duration",
      detail: `${doSeconds.toFixed(1)} object-seconds × 128 MB`,
      usd: (doSeconds * PRICES.doGbPerObject * PRICES.doGbSecondPerMillion) / 1e6,
    },
    {
      label: "R2 reads",
      detail: `${usage.sourceReads} range reads`,
      usd: (usage.sourceReads * PRICES.r2ClassBPerMillion) / 1e6,
    },
  ];
  if (usage.container) {
    const spec = CONTAINER_SPECS[usage.container];
    items.push({
      label: `Containers (${usage.container})`,
      detail: `${reducerSeconds.toFixed(1)} container-seconds`,
      usd:
        reducerSeconds *
        (spec.vcpu * PRICES.containerVcpuSecond +
          (spec.memoryMib / 1024) * PRICES.containerGibSecond +
          spec.diskGb * PRICES.containerDiskGbSecond),
    });
  }
  return {
    usd: sum(items.map((item) => item.usd)),
    items,
    athenaUsd: (Math.max(usage.sourceReadBytes, PRICES.athenaMinBytes) / 1e12) * PRICES.athenaPerTb,
  };
}
