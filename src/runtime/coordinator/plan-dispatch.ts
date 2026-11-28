/**
 * The coordinator Wasm's planning calls: job sources, job plans, late read-backs, concat
 * programs and result JSON. Rust plans from SQL and the Parquet footers; placement, reads and
 * operators all come from that one plan. This module only encodes requests and decodes plans.
 */

import { decodeExecutionPlanWire, type ExecutionPlan } from "./execution-graph.ts";
import { readWasmLastError, wasmMemoryOffset, wasmMemoryU8 } from "../host/wasm.ts";
import { plannerWasm, type PlannerWasm, type WcolStreamWasm } from "../host/wasm-stream.ts";

/** Range reads of one object. */
export type ReadAt = (offset: number, length: number) => Promise<Uint8Array>;

/** Footer bytes of a Parquet object: the last 8 bytes are `u32 footer length` + `PAR1`. */
export async function readParquetFooter(read: (offset: number, length: number) => Uint8Array | Promise<Uint8Array>, fileSize: number): Promise<Uint8Array> {
  if (fileSize < 12) throw new Error("file is too small to be Parquet");
  const tail = await read(fileSize - 8, 8);
  const magic = String.fromCharCode(tail[4]!, tail[5]!, tail[6]!, tail[7]!);
  if (magic !== "PAR1") throw new Error("missing Parquet footer magic");
  const len = new DataView(tail.buffer, tail.byteOffset, 8).getUint32(0, true);
  if (len === 0 || len > fileSize - 12) throw new Error("invalid Parquet footer length");
  if (len > 32 * 1024 * 1024) throw new Error(`parquet footer of ${len} bytes is unreasonably large`);
  return read(fileSize - 8 - len, len);
}

const encoder = new TextEncoder();

function encodePlanRequest(options: {
  sql: string;
  objectId: string;
  footer: Uint8Array;
  nodeCount: number;
  batchRows: number;
  maxRowGroups?: number;
}): Uint8Array {
  const sql = encoder.encode(options.sql);
  const objectId = encoder.encode(options.objectId);
  const out = new Uint8Array(
    12 + 12 + objectId.byteLength + options.footer.byteLength + sql.byteLength,
  );
  const view = new DataView(out.buffer);
  let at = 0;
  const u32 = (value: number) => {
    view.setUint32(at, value, true);
    at += 4;
  };
  const bytes = (value: Uint8Array) => {
    u32(value.byteLength);
    out.set(value, at);
    at += value.byteLength;
  };
  u32(options.batchRows);
  u32(options.nodeCount);
  u32(options.maxRowGroups ?? 0);
  bytes(objectId);
  bytes(options.footer);
  bytes(sql);
  return out;
}

/** Plan `sql` over one Parquet object for `nodeCount` node slots. */
export function planQueryDispatch(
  wasm: WcolStreamWasm,
  options: {
    sql: string;
    objectId: string;
    footer: Uint8Array;
    nodeCount: number;
    batchRows?: number;
    /** Plan over the first row groups only (0 or absent: all). */
    maxRowGroups?: number;
  },
): ExecutionPlan {
  const request = encodePlanRequest({ ...options, batchRows: options.batchRows ?? 32_768 });
  return decodeExecutionPlanWire(callPlanner(wasm, "wcol_plan_query", request));
}

/**
 * Client JSON for a finished query: the root task's row frames (or a footer-only partial),
 * named by the plan's result columns. Serialization only; the graph already sorted and limited.
 */
export function resultJson(
  module: WcolStreamWasm,
  plan: ExecutionPlan,
  data: { rows: Uint8Array } | { partial: Uint8Array },
  rowCap = 0,
): string {
  const wasm = plannerWasm(module, "wcol_result_json");
  if (plan.resultColumns.byteLength === 0) throw new Error("plan has no result columns");
  const kind = "rows" in data ? 0 : 1;
  const bytes = "rows" in data ? data.rows : data.partial;
  const total = plan.resultColumns.byteLength + bytes.byteLength;
  const ptr = wasm.wcs_alloc(total);
  if (!ptr) throw new Error(`wcs_alloc failed for ${total} bytes`);
  try {
    const mem = wasmMemoryU8(wasm, ptr, total);
    mem.set(plan.resultColumns, 0);
    mem.set(bytes, plan.resultColumns.byteLength);
    const columnsLen = plan.resultColumns.byteLength;
    const n = wasm.wcol_result_json(ptr, columnsLen, kind, wasmMemoryOffset(ptr) + columnsLen, bytes.byteLength, rowCap);
    if (!n) throw new Error(readWasmLastError(wasm, "result json failed"));
    return new TextDecoder().decode(wasmMemoryU8(wasm, wasm.wcol_result_output_ptr(), n));
  } finally {
    wasm.wcs_dealloc(ptr, total);
  }
}

/** One object a job reads: an R2 binding name and key, or an http(s) URL. */
export type JobSourceLocation = { bucket: string; key: string } | { url: string };

/** What `POST /jobs` asks for, and the objects whose footers the planner needs (in order). */
export type JobSources = {
  kind: "json" | "parquet" | "model";
  sources: JobSourceLocation[];
  idempotencyKey?: string;
  modelName?: string;
  modelOptions?: { numClusters: number; maxIterations: number; seed: number; tolerance: number };
};

export type JobPlan =
  /**
   * `late`: the graph only picks the rows (it prints their ids); `planLateRead` plans the
   * graph that reads them back, and `rowsByIdJson` prints the result.
   */
  /**
   * `groupBytes`: the planner's estimate of the aggregate's group state, which picks where
   * reducers run. Fresh planning sets it; a plan read back from storage has no use for it.
   */
  | { kind: "json"; rowCap: number; graph: ExecutionPlan; graphWire: Uint8Array; late: boolean; groupBytes?: number }
  | { kind: "parquet"; bucket: string; prefix: string; graph: ExecutionPlan; graphWire: Uint8Array; groupBytes?: number }
  | {
      kind: "model";
      name: string;
      options: { numClusters: number; maxIterations: number; seed: number; tolerance: number };
      graph: ExecutionPlan;
      graphWire: Uint8Array;
    };

/** Planner calls that take one request buffer and answer at `wcol_plan_output_ptr`. */
type PlannerCall = "wcol_plan_query" | "wcol_job_sources" | "wcol_plan_job" | "wcol_plan_late_read" | "wcol_concat_program";

function callPlanner(module: WcolStreamWasm, call: PlannerCall, input: Uint8Array): Uint8Array {
  const wasm: PlannerWasm = plannerWasm(module, call);
  const ptr = wasm.wcs_alloc(input.byteLength);
  if (!ptr) throw new Error(`wcs_alloc failed for a ${input.byteLength}-byte ${call} request`);
  try {
    wasmMemoryU8(wasm, ptr, input.byteLength).set(input);
    const n = wasm[call](ptr, input.byteLength);
    if (!n) throw new Error(readWasmLastError(wasm, `${call} failed`));
    return wasmMemoryU8(wasm, wasm.wcol_plan_output_ptr(), n).slice();
  } finally {
    wasm.wcs_dealloc(ptr, input.byteLength);
  }
}

/** Validate a `POST /jobs` body; returns its kind and the sources to read footers from. */
export function jobSources(wasm: WcolStreamWasm, body: string): JobSources {
  const out = callPlanner(wasm, "wcol_job_sources", encoder.encode(body));
  return JSON.parse(new TextDecoder().decode(out)) as JobSources;
}

/** Plan a job from its body and one footer per [`jobSources`] entry. */
export type JobPlanRequest = {
  body: string;
  footers: { fileSize: number; footer: Uint8Array }[];
  maxNodes: number;
  batchRows?: number;
  /** Planned-read floor per scan node. Omitted keeps the coordinator default (64 KiB). */
  minBytesPerNode?: number;
  /** Power-of-two hash-reducer override. `1` keeps the gather plan. Omitted uses the cardinality estimate. */
  hashReducers?: number;
};

/** The `wcol_plan_job` request bytes (Rust `parse_plan_request`). */
function encodeJobPlanRequest(options: JobPlanRequest): Uint8Array {
  const body = encoder.encode(options.body);
  const extra = options.hashReducers != null ? 8 : options.minBytesPerNode != null ? 4 : 0;
  const size =
    12 + body.byteLength + 4 + options.footers.reduce((n, f) => n + 12 + f.footer.byteLength, 0) + extra;
  const input = new Uint8Array(size);
  const view = new DataView(input.buffer);
  let at = 0;
  const u32 = (value: number) => {
    view.setUint32(at, value, true);
    at += 4;
  };
  const bytes = (value: Uint8Array) => {
    u32(value.byteLength);
    input.set(value, at);
    at += value.byteLength;
  };
  u32(options.maxNodes);
  u32(options.batchRows ?? 32_768);
  bytes(body);
  u32(options.footers.length);
  for (const footer of options.footers) {
    view.setBigUint64(at, BigInt(footer.fileSize), true);
    at += 8;
    bytes(footer.footer);
  }
  if (options.minBytesPerNode != null || options.hashReducers != null) {
    u32(options.minBytesPerNode ?? 0);
  }
  if (options.hashReducers != null) u32(options.hashReducers);
  return input;
}

export function planJob(wasm: WcolStreamWasm, options: JobPlanRequest): JobPlan {
  const out = callPlanner(wasm, "wcol_plan_job", encodeJobPlanRequest(options));
  const outView = new DataView(out.buffer, out.byteOffset, out.byteLength);
  let pos = 1;
  const read32 = () => {
    const value = outView.getUint32(pos, true);
    pos += 4;
    return value;
  };
  const readBytes = () => {
    const len = read32();
    const value = out.subarray(pos, pos + len);
    pos += len;
    return value;
  };
  const text = () => new TextDecoder().decode(readBytes());
  /** The planner's estimate of the aggregate's group-state bytes (0 without an aggregate). */
  const groupBytes = () => Number(outView.getBigUint64(pos, true));
  switch (out[0]) {
    case 0:
    case 4: {
      const rowCap = read32();
      const graphWire = readBytes().slice();
      return { kind: "json", rowCap, graph: decodeExecutionPlanWire(graphWire), graphWire, late: out[0] === 4, groupBytes: groupBytes() };
    }
    case 1: {
      const bucket = text();
      const prefix = text();
      const graphWire = readBytes().slice();
      return { kind: "parquet", bucket, prefix, graph: decodeExecutionPlanWire(graphWire), graphWire, groupBytes: groupBytes() };
    }
    case 3: {
      const name = text();
      const numClusters = read32();
      const maxIterations = read32();
      const seed = Number(outView.getBigUint64(pos, true));
      pos += 8;
      const tolerance = outView.getFloat64(pos, true);
      pos += 8;
      const graphWire = readBytes().slice();
      return {
        kind: "model", name,
        options: { numClusters, maxIterations, seed, tolerance },
        graph: decodeExecutionPlanWire(graphWire), graphWire,
      };
    }
    default:
      throw new Error(`unknown job plan kind ${out[0]}`);
  }
}

/**
 * The read-back graph of a late row job (`plan.late`), from its pick graph's root row frames.
 * `graph` is `null` when nothing was picked; `resultColumns` names the job's result either way.
 */
export function planLateRead(
  wasm: WcolStreamWasm,
  request: JobPlanRequest,
  picked: Uint8Array,
): { graph: ExecutionPlan | null; resultColumns: Uint8Array } {
  const job = encodeJobPlanRequest(request);
  const input = new Uint8Array(4 + picked.byteLength + job.byteLength);
  new DataView(input.buffer).setUint32(0, picked.byteLength, true);
  input.set(picked, 4);
  input.set(job, 4 + picked.byteLength);
  const out = callPlanner(wasm, "wcol_plan_late_read", input);
  if (out[0] === 0) return { graph: null, resultColumns: out.slice(1) };
  const graph = decodeExecutionPlanWire(out.slice(1));
  return { graph, resultColumns: graph.resultColumns };
}

/** Client JSON of a late row job: the read-back's rows in the pick's order. */
export function rowsByIdJson(
  module: WcolStreamWasm,
  pick: { resultColumns: Uint8Array; rows: Uint8Array },
  read: { resultColumns: Uint8Array; rows: Uint8Array },
  rowCap: number,
): string {
  const wasm = plannerWasm(module, "wcol_rows_by_id_json");
  const parts = [pick.resultColumns, pick.rows, read.resultColumns, read.rows];
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const ptr = wasm.wcs_alloc(Math.max(total, 1));
  if (!ptr) throw new Error(`wcs_alloc failed for ${total} bytes`);
  try {
    const mem = wasmMemoryU8(wasm, ptr, total);
    const at: number[] = [];
    let offset = 0;
    for (const part of parts) {
      mem.set(part, offset);
      at.push(wasmMemoryOffset(ptr) + offset);
      offset += part.byteLength;
    }
    const n = wasm.wcol_rows_by_id_json(
      at[0]!, parts[0]!.byteLength,
      at[1]!, parts[1]!.byteLength,
      at[2]!, parts[2]!.byteLength,
      at[3]!, parts[3]!.byteLength,
      rowCap,
    );
    if (!n) throw new Error(readWasmLastError(wasm, "late row json failed"));
    return new TextDecoder().decode(wasmMemoryU8(wasm, wasm.wcol_result_output_ptr(), n));
  } finally {
    wasm.wcs_dealloc(ptr, Math.max(total, 1));
  }
}

/** Build the opcode-11 concat program from shard footers (row groups are kept as written). */
export function planConcatProgram(
  wasm: WcolStreamWasm,
  footers: { fileSize: number; footer: Uint8Array }[],
): Uint8Array {
  // `u32` count, then per shard `u64` file size, `u32` footer length and the footer.
  const input = new Uint8Array(4 + footers.reduce((n, item) => n + 12 + item.footer.byteLength, 0));
  const view = new DataView(input.buffer);
  view.setUint32(0, footers.length, true);
  let at = 4;
  for (const item of footers) {
    view.setBigUint64(at, BigInt(item.fileSize), true);
    view.setUint32(at + 8, item.footer.byteLength, true);
    input.set(item.footer, at + 12);
    at += 12 + item.footer.byteLength;
  }
  return callPlanner(wasm, "wcol_concat_program", input);
}
