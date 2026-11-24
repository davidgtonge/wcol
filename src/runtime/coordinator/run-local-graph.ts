/**
 * Run one planned job in this process. Rust chooses ports and reads.
 * TypeScript copies bytes from the file and between tasks.
 */

import { COORDINATOR_SLOT, edgesInto, type ExecutionPlan } from "./execution-graph.ts";
import { runWcolEngine } from "../host/run-wcol-engine.ts";
import { concatBytes } from "../host/bytes.ts";
import { attachBytesInputs } from "../host/pool-streams.ts";
import { planQueryDispatch, type ReadAt } from "./plan-dispatch.ts";
import type { WcolStreamWasm } from "../host/wasm-stream.ts";
import { type LocalSpillR2, runLocalHashGraph } from "./run-local-hash-graph.ts";
import { claimingReadAt, localWork } from "./unit-claims.ts";
import { createMemoryR2SpillBucket } from "../../../tests/helpers/memory-r2-spill-bucket.ts";

export type LocalGraphWasm = {
  node: WcolStreamWasm;
  coordinator: WcolStreamWasm;
};

async function runTask(
  wasm: WcolStreamWasm,
  program: Uint8Array,
  readAt: ReadAt | undefined,
  inputs: Uint8Array[],
  spillR2Bucket: LocalSpillR2,
  next: () => number | null = () => null,
): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  // One source here; a scan with claim units asks for its next unit through the same ReadAt.
  const read = readAt ? claimingReadAt(async (_source, offset, length) => readAt(Number(offset), length), next) : undefined;
  await runWcolEngine({
    wasm,
    plan: program,
    scratchBytes: 1024 * 1024,
    layout: { inputs: inputs.length, outputs: 1 },
    onPoolReady: attachBytesInputs(inputs),
    readAt: read,
    host: {
      readAt: read,
      writePort: async (_port, chunk) => {
        parts.push(chunk);
      },
      yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
      spillR2Bucket,
    },
  });
  return concatBytes(parts);
}

/** Gather plan: each task runs after its producers, on the ABI v1 loop. */
export async function runGatherGraph(
  wasm: LocalGraphWasm,
  plan: Extract<ExecutionPlan, { kind: "graph" }>,
  readAt: ReadAt,
  spillR2Bucket: LocalSpillR2 = createMemoryR2SpillBucket(),
): Promise<Uint8Array> {
  const outputs = new Map<number, Uint8Array>();
  const pending = new Set(plan.tasks.map((task) => task.id));
  // Tasks run one at a time here, so the first scan of a group claims every unit.
  const nextUnit = localWork(plan.tasks);
  while (pending.size > 0) {
    const ready = plan.tasks.filter((task) => {
      if (!pending.has(task.id)) return false;
      return edgesInto(task.id, plan.edges).every((edge) => outputs.has(edge.from));
    });
    if (ready.length === 0) throw new Error("execution graph has a cycle or a missing producer");
    for (const task of ready) {
      const incoming = edgesInto(task.id, plan.edges).map((edge) => outputs.get(edge.from)!);
      const engine = task.slot === COORDINATOR_SLOT ? wasm.coordinator : wasm.node;
      try {
        outputs.set(task.id, await runTask(engine, task.program, incoming.length ? undefined : readAt, incoming, spillR2Bucket, () =>
          nextUnit(task.id),
        ));
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`task ${task.id} slot ${task.slot}: ${message}`);
      }
      pending.delete(task.id);
    }
  }
  const root = outputs.get(plan.rootTaskId);
  if (!root) throw new Error("graph root produced no bytes");
  return root;
}

export type PlannedQueryOptions = {
  sql: string;
  sourceKey: string;
  footer: Uint8Array;
  readAt: ReadAt;
  nodeCount: number;
  batchRows?: number;
  maxRowGroups?: number;
};

export type PlannedQueryResult = {
  partial: Uint8Array;
  rows?: Uint8Array;
  plannedRequests: number;
  plannedBytes: number;
};

/** Plan with the coordinator, then run the graph locally (gather or hash). */
export async function runPlannedQuery(
  wasm: LocalGraphWasm,
  options: PlannedQueryOptions,
): Promise<PlannedQueryResult> {
  const dispatch = planQueryDispatch(wasm.coordinator, {
    sql: options.sql,
    objectId: options.sourceKey,
    footer: options.footer,
    nodeCount: options.nodeCount,
    batchRows: options.batchRows,
    maxRowGroups: options.maxRowGroups,
  });
  if (dispatch.kind === "metadata_partial") {
    return { partial: dispatch.partial, plannedRequests: 0, plannedBytes: 0 };
  }
  const scanTasks = dispatch.tasks.filter((task) => edgesInto(task.id, dispatch.edges).length === 0);
  const plannedRequests = scanTasks.reduce((n, task) => n + task.requestCount, 0);
  const plannedBytes = scanTasks.reduce((n, task) => n + task.fetchedBytes, 0);
  let bytes: Uint8Array;
  if (dispatch.edges.some((edge) => edge.partition !== undefined)) {
    bytes = (await runLocalHashGraph(wasm, dispatch, options.readAt)).bytes;
  } else {
    bytes = await runGatherGraph(wasm, dispatch, options.readAt);
  }
  if (dispatch.rootOutput === "rows") {
    return { partial: new Uint8Array(), rows: bytes, plannedRequests, plannedBytes };
  }
  return { partial: bytes, plannedRequests, plannedBytes };
}

/** Run one node scan program against object-store reads (stream pool). */
export async function runNodeProgram(
  wasm: WcolStreamWasm,
  program: Uint8Array,
  readAt: ReadAt,
  ownUnits: readonly number[] = [],
): Promise<void> {
  // With no other scan to steal from, a scan runs exactly its own planned units.
  const units = [...ownUnits];
  const read = claimingReadAt(async (_source, offset, length) => readAt(Number(offset), length), () => units.shift() ?? null);
  await runWcolEngine({
    wasm,
    plan: program,
    scratchBytes: 256 * 1024,
    layout: { inputs: 0, outputs: 1 },
    readAt: read,
    host: {
      readAt: read,
      writePort: async () => {},
      yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
    },
  });
}
