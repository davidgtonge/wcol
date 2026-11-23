/** The job record's node count, from the slots an attempt used. */
import { COORDINATOR_SLOT } from "../../../src/runtime/coordinator/execution-graph.ts";
import type { GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { type GraphJobContext, jobKey, requireJobRecord } from "./record.ts";

export async function rememberSlots(ctx: GraphJobContext, jobId: string, slots: number[]): Promise<void> {
  const record = requireJobRecord(await ctx.store.get(jobKey(jobId)));
  record.nodeCount = new Set(slots).size;
  record.updatedAt = Date.now();
  await ctx.store.put(jobKey(jobId), record);
}

export async function rememberGraphSlots(ctx: GraphJobContext, jobId: string, graph: GraphPlan): Promise<void> {
  const slots = [...new Set(graph.tasks.map((task) => task.slot))].filter((slot) => slot !== COORDINATOR_SLOT);
  await rememberSlots(ctx, jobId, slots.length > 0 ? slots : [0]);
}
