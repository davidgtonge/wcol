/** Immutable execution graph from Rust `encode_execution_plan_wire`. */

import type { TaskWork } from "./unit-claims.ts";

export const WIRE_TAG_METADATA_PARTIAL = 0;
export const WIRE_TAG_EXECUTION_GRAPH_V1 = 4;
/** Tag 4 plus a partition id on every edge. `0xffffffff` is the whole stream. */
export const WIRE_TAG_EXECUTION_GRAPH_V2 = 5;
/** Tag 5 body plus output plan and finaliser trailer. */
export const WIRE_TAG_EXECUTION_GRAPH_V3 = 6;
export const WHOLE_STREAM = 0xffff_ffff;

export const COORDINATOR_SLOT = 0xffff_ffff;

/** Rust `OP_HASH_REDUCER`: a program that reduces one hash partition. */
const OP_HASH_REDUCER = 16;

export function isHashReducer(program: Uint8Array): boolean {
  return program[5] === OP_HASH_REDUCER;
}

export type EdgeTransport = "in_process" | "do_rpc_wcs" | "do_fetch_wcs" | "colocated";

/** Task wire: a task without run-time work carries this group. */
const NO_WORK_GROUP = 0xffff_ffff;

export type ExecutionTask = {
  id: number;
  slot: number;
  program: Uint8Array;
  requestCount: number;
  fetchedBytes: number;
  /** A scan that takes units of work at run time: its planned queue (see unit-claims.ts). */
  work?: TaskWork;
};

export type ExecutionEdge = {
  from: number;
  to: number;
  transport: EdgeTransport;
  /** Set when this edge carries one hash partition. Omitted for the whole producer stream. */
  partition?: number;
};

export type RootOutputKind = "partial" | "rows" | "parquet";

export type OutputFormat = "json" | "parquet";
/** `hive`: files split by `partitionBy` values, framed by the node sink (`HiveUploadSink`). */
export type OutputLayout = "single_file" | "partitioned" | "hive";
const OUTPUT_LAYOUTS: readonly OutputLayout[] = ["single_file", "partitioned", "hive"];

export type OutputDestination = {
  bucket: string;
  prefix: string;
  objectKey?: string;
};

export type OutputPlan = {
  format: OutputFormat;
  destination: OutputDestination;
  layout: OutputLayout;
};

export type OutputFinalizer =
  | { kind: "none" }
  | { kind: "concat_parquet"; partitionOrder: number[] }
  | { kind: "concat_json"; partitionOrder: number[] };

/**
 * Result columns (Rust `encode_result_columns`): each output name and the root's row-frame
 * column that holds it. Empty when the planner wrote none. Only `wcol_result_json` reads it.
 */
export type ExecutionPlan =
  | { kind: "metadata_partial"; partial: Uint8Array; resultColumns: Uint8Array }
  | {
      kind: "graph";
      tasks: ExecutionTask[];
      edges: ExecutionEdge[];
      rootTaskId: number;
      rootOutput: RootOutputKind;
      output?: OutputPlan;
      finalizer: OutputFinalizer;
      resultColumns: Uint8Array;
    };

function edgeTransportFromWire(byte: number | undefined): EdgeTransport {
  switch (byte) {
    case 0:
      return "in_process";
    case 1:
      return "do_rpc_wcs";
    case 2:
      return "do_fetch_wcs";
    case 3:
      return "colocated";
    default:
      throw new Error(`unknown edge transport ${byte}`);
  }
}

const ROOT_OUTPUTS: readonly RootOutputKind[] = ["partial", "rows", "parquet"];
const OUTPUT_FORMATS: readonly OutputFormat[] = ["json", "parquet"];

/** `table[byte]`, or an error naming the field: an unknown wire byte is a broken plan. */
function wireEnum<T>(table: readonly T[], byte: number | undefined, what: string): T {
  const value = byte === undefined ? undefined : table[byte];
  if (value === undefined) throw new Error(`unknown ${what} ${byte} in execution graph wire`);
  return value;
}

/** A wire flag: 0 or 1, nothing else. */
function wireFlag(byte: number | undefined, what: string): boolean {
  return wireEnum([false, true], byte, what);
}

/** Optional trailer: `u32` length + result columns. */
function resultColumnsAt(out: Uint8Array, view: DataView, at: number): Uint8Array {
  if (at === out.byteLength) return new Uint8Array();
  const len = view.getUint32(at, true);
  if (at + 4 + len !== out.byteLength) throw new Error("trailing bytes after execution graph");
  return out.subarray(at + 4, at + 4 + len);
}

/** Decode `wcol_plan_query` output. */
export function decodeExecutionPlanWire(out: Uint8Array): ExecutionPlan {
  if (out.length === 0) throw new Error("empty plan wire");
  const tag = out[0]!;
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  if (tag === WIRE_TAG_METADATA_PARTIAL) {
    const len = view.getUint32(1, true);
    const at = 5;
    return {
      kind: "metadata_partial",
      partial: out.subarray(at, at + len),
      resultColumns: resultColumnsAt(out, view, at + len),
    };
  }
  if (tag !== WIRE_TAG_EXECUTION_GRAPH_V1 && tag !== WIRE_TAG_EXECUTION_GRAPH_V2 && tag !== WIRE_TAG_EXECUTION_GRAPH_V3) {
    throw new Error(`unsupported plan wire tag ${tag}`);
  }
  const partitioned = tag === WIRE_TAG_EXECUTION_GRAPH_V2 || tag === WIRE_TAG_EXECUTION_GRAPH_V3;
  let at = 1;
  const u32 = () => {
    const value = view.getUint32(at, true);
    at += 4;
    return value;
  };
  const tasks: ExecutionTask[] = [];
  for (let count = u32(); count > 0; count--) {
    const id = u32();
    const slot = u32();
    const requestCount = u32();
    const fetchedBytes = Number(view.getBigUint64(at, true));
    at += 8;
    const len = u32();
    const program = out.subarray(at, at + len);
    at += len;
    const group = u32();
    let work: TaskWork | undefined;
    if (group !== NO_WORK_GROUP) {
      const ownUnits: number[] = [];
      for (let units = u32(); units > 0; units--) ownUnits.push(u32());
      work = { group, ownUnits };
    }
    tasks.push({ id, slot, requestCount, fetchedBytes, program, ...(work ? { work } : {}) });
  }
  const edges: ExecutionEdge[] = [];
  for (let count = u32(); count > 0; count--) {
    const from = u32();
    const to = u32();
    const transport = edgeTransportFromWire(out[at]);
    at += 1;
    let partition: number | undefined;
    if (partitioned) {
      const id = u32();
      if (id !== WHOLE_STREAM) partition = id;
    }
    edges.push({ from, to, transport, partition });
  }
  const rootTaskId = u32();
  const rootOutput = wireEnum(ROOT_OUTPUTS, out[at], "root output");
  at += 1;
  let output: OutputPlan | undefined;
  let finalizer: OutputFinalizer = { kind: "none" };
  if (tag === WIRE_TAG_EXECUTION_GRAPH_V3) {
    const readStr = () => {
      const len = u32();
      const bytes = out.subarray(at, at + len);
      at += len;
      return new TextDecoder().decode(bytes);
    };
    const hasOutput = wireFlag(out[at], "output flag");
    at += 1;
    if (hasOutput) {
      const format = wireEnum(OUTPUT_FORMATS, out[at], "output format");
      at += 1;
      const layout = wireEnum(OUTPUT_LAYOUTS, out[at], "output layout");
      at += 1;
      const bucket = readStr();
      const prefix = readStr();
      const hasKey = wireFlag(out[at], "object key flag");
      at += 1;
      const objectKey = hasKey ? readStr() : undefined;
      output = { format, destination: { bucket, prefix, objectKey }, layout };
    }
    const finTag = wireEnum(["none", "concat_parquet", "concat_json"] as const, out[at], "finalizer");
    at += 1;
    if (finTag !== "none") {
      const partitionOrder: number[] = [];
      for (let count = u32(); count > 0; count--) partitionOrder.push(u32());
      finalizer = { kind: finTag, partitionOrder };
    }
  }
  const resultColumns = resultColumnsAt(out, view, at);
  return { kind: "graph", tasks, edges, rootTaskId, rootOutput, output, finalizer, resultColumns };
}

export function edgesInto(taskId: number, edges: ExecutionEdge[]): ExecutionEdge[] {
  return edges.filter((edge) => edge.to === taskId);
}
