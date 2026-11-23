import type { GraphPlan } from "./job-replay.ts";

const PLAN_V2 = 0xff;
const OP_COORDINATOR = 13;
const OP_FINALIZE = 15;
/** Rust `OP_SHARD_EXPORT`: one reducer's final groups written as Parquet frame by frame. */
const OP_SHARD_EXPORT = 17;

export function programProducerCount(program: Uint8Array): number | undefined {
  if (program[0] !== PLAN_V2 || program.length < 10) return undefined;
  const opcode = program[5]!;
  if (opcode !== OP_COORDINATOR && opcode !== OP_FINALIZE) return undefined;
  return new DataView(program.buffer, program.byteOffset, program.byteLength).getUint32(6, true);
}

export function localExportProgram(plan: GraphPlan, reducerTaskId: number): Uint8Array | undefined {
  const edge = plan.edges.find((item) => item.from === reducerTaskId && item.partition === undefined);
  if (!edge) return undefined;
  const task = plan.tasks.find((item) => item.id === edge.to);
  if (!task || task.program[5] !== OP_SHARD_EXPORT) return undefined;
  return task.program;
}

/** Range-sort workers or hash export finalizers: one Parquet writer per partition. */
export function partitionParquetWriterTasks(plan: GraphPlan): number[] {
  const partitionTargets = new Set(
    plan.edges.filter((edge) => edge.partition !== undefined).map((edge) => edge.to),
  );
  const writers = plan.tasks
    .filter((task) => {
      if (!partitionTargets.has(task.id)) return false;
      const opcode = task.program[5];
      if (opcode !== OP_COORDINATOR && opcode !== OP_FINALIZE) return false;
      return programProducerCount(task.program) === 1;
    })
    .map((task) => task.id)
    .sort((left, right) => left - right);
  return writers.length > 1 ? writers : [];
}

export function hashShardedParquetExport(plan: GraphPlan, reducerTaskIds: number[]): boolean {
  if (plan.rootOutput !== "parquet" || reducerTaskIds.length === 0) return false;
  return reducerTaskIds.every((id) => localExportProgram(plan, id) != null);
}

export function rangeSortParquetExport(plan: GraphPlan): boolean {
  return partitionParquetWriterTasks(plan).length > 1
    && plan.tasks.some((task) => task.program[5] === OP_COORDINATOR && programProducerCount(task.program) === 1);
}
