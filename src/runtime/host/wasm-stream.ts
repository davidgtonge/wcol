/**
 * The Wasm ABI the host reads. Pool input and output are byte streams: a write may split a
 * WCS1 frame, so transports carry arbitrary chunks.
 */

import type { DictTraffic, MemGrowthSample, SpillBoundary } from "./host-types.ts";
import type { SpillIo } from "./spill-r2-pool-driver.ts";

/** Exports of every WCOL module: memory, allocator, last error, the stream pool and its diagnostics. */
export interface PoolWasm {
  memory: WebAssembly.Memory;
  wcs_alloc(length: number): number;
  wcs_dealloc(ptr: number, length: number): void;
  /** Last error from a stateless FFI call (plan, reduce, stream-pool open). */
  wcs_last_error_ptr(): number;
  wcs_last_error_len(): number;
  /** Groups in the hash table after the last aggregate finish (a Rust `u64`). */
  wcs_last_resident_groups(): bigint;
  /** Group-table bytes after the last aggregate finish. */
  wcs_last_resident_group_bytes(): bigint;
  /** Sorted runs flushed by the current reducer. */
  wcs_last_spill_runs(): bigint;
  wcs_last_spill_groups_flushed(): bigint;
  wcs_last_spill_peak_live_bytes(): bigint;
  wcs_last_spill_peak_interner_bytes(): bigint;
  /** Little-endian u64 counters; see `readDictTraffic`. */
  wcs_dict_traffic_len(): number;
  wcs_dict_traffic_ptr(): number;
  /** Eleven little-endian u64s per Wasm-growth sample. */
  wcs_mem_growth_len(): number;
  wcs_mem_growth_ptr(): number;
  /** Byte length of the spill-boundary table: little-endian u32 words per run. */
  wcs_spill_boundary_len(): number;
  wcs_spill_boundary_ptr(): number;
  wcs_sp_open_echo(inputPorts: number, outputPorts: number, inputKib: number, outputKib: number): number;
  wcs_sp_open_graph(
    programPtr: number,
    programLen: number,
    scratchBytes: number,
    inputPorts: number,
    outputPorts: number,
    inputKib: number,
    outputKib: number,
    budgetShare: number,
  ): number;
  wcs_sp_close(handle: number): void;
  wcs_sp_step(handle: number, budget: number): number;
  wcs_sp_in_ptr(handle: number, port: number): number;
  wcs_sp_in_free(handle: number, port: number): number;
  wcs_sp_in_commit(handle: number, port: number, length: number): number;
  wcs_sp_in_end(handle: number, port: number): number;
  wcs_sp_in_absent(handle: number, port: number): number;
  wcs_sp_out_len(handle: number, port: number): number;
  wcs_sp_out_ptr(handle: number, port: number): number;
  wcs_sp_out_release(handle: number, port: number): number;
  wcs_sp_out_done(handle: number, port: number): number;
  wcs_sp_read_at_ticket(handle: number): number;
  wcs_sp_read_at_source(handle: number): number;
  /** A Rust `u64`, so a `BigInt` in JS. */
  wcs_sp_read_at_offset(handle: number): bigint;
  wcs_sp_read_at_len(handle: number): number;
  /** Takes ownership of `ptr` (a `wcs_alloc` buffer) on success and on failure. */
  wcs_sp_complete_read_at(handle: number, ticket: number, ptr: number, len: number): number;
  /** Claim the exposed ReadAt so another can be exposed while it is in flight. */
  wcs_sp_take_read_at(handle: number): void;
  wcs_sp_error_ptr(handle: number): number;
  wcs_sp_error_len(handle: number): number;
  wcs_sp_spill_op(handle: number): number;
  wcs_sp_spill_append_len(handle: number): number;
  wcs_sp_spill_append_ptr(handle: number): number;
  wcs_sp_spill_run_upload_len(handle: number): number;
  wcs_sp_spill_run_upload_ptr(handle: number): number;
  /** Reused buffer for the pending run read (0 on error); fill it, then complete. */
  wcs_sp_spill_read_slot(handle: number, len: number): number;
  wcs_sp_spill_complete(handle: number, ok: number): number;
  /** Resident graph control; the graph's plan determines its lifetime and kernel. */
  wcs_sp_is_resident(handle: number): number;
  wcs_sp_command(handle: number, kind: number, ptr: number, len: number): number;
  wcs_sp_response_ptr(handle: number): number;
  wcs_sp_response_len(handle: number): number;
}

/** Planning, result and model exports. Only the coordinator module has them. */
export interface PlannerExports {
  /** Plan SQL over one Parquet footer (`planQueryDispatch`). */
  wcol_plan_query(ptr: number, len: number): number;
  wcol_plan_output_ptr(): number;
  /** `POST /jobs` body → its kind and sources (JSON at `wcol_plan_output_ptr`). */
  wcol_job_sources(ptr: number, len: number): number;
  /** Plan a job from its body and footers (binary at `wcol_plan_output_ptr`). */
  wcol_plan_job(ptr: number, len: number): number;
  wcol_concat_program(ptr: number, len: number): number;
  /** Plan a late row job's read-back from its pick's row frames and the job request. */
  wcol_plan_late_read(ptr: number, len: number): number;
  /**
   * Client JSON (`{"columns":[..],"rows":[[..],..]}`) from the plan's result columns and the
   * root's row frames (`kind` 0) or a footer-only partial (`kind` 1).
   */
  wcol_result_json(
    columnsPtr: number,
    columnsLen: number,
    kind: number,
    dataPtr: number,
    dataLen: number,
    rowCap: number,
  ): number;
  wcol_result_output_ptr(): number;
  /** Client JSON of a late row job: the pick's columns and row frames, then the read-back's. */
  wcol_rows_by_id_json(
    pickColumnsPtr: number,
    pickColumnsLen: number,
    pickPtr: number,
    pickLen: number,
    columnsPtr: number,
    columnsLen: number,
    rowsPtr: number,
    rowsLen: number,
    rowCap: number,
  ): number;
  wcol_iterative_kmeans_merge_moments(
    leftPtr: number, leftLen: number, rightPtr: number, rightLen: number, dimensions: number,
  ): number;
  wcol_iterative_kmeans_prepare(
    momentsPtr: number, momentsLen: number, dimensions: number, clusters: number,
    maxIterations: number, tolerance: number, seed: bigint,
  ): number;
  wcol_iterative_kmeans_merge(
    leftPtr: number, leftLen: number, rightPtr: number, rightLen: number,
    dimensions: number, clusters: number, maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_update(
    statePtr: number, stateLen: number, partialPtr: number, partialLen: number,
    dimensions: number, clusters: number, maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_centroids_json(
    statePtr: number, stateLen: number, dimensions: number, clusters: number,
    maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_output_ptr(): number;
}

/** The coordinator module: the stream pool plus the planner. */
export type PlannerWasm = PoolWasm & PlannerExports;

/**
 * An instantiated module. `streamWasmExports` checked every pool export, and every planner
 * export when the module has the planner; `plannerWasm` narrows to the coordinator module.
 */
export type WcolStreamWasm = PoolWasm & Partial<PlannerExports> & {
  /** Present on the coordinator module; only the Worker's role check reads it. */
};

const POOL_EXPORTS = [
  "wcs_alloc", "wcs_dealloc", "wcs_last_error_ptr", "wcs_last_error_len",
  "wcs_last_resident_groups", "wcs_last_resident_group_bytes", "wcs_last_spill_runs",
  "wcs_last_spill_groups_flushed", "wcs_last_spill_peak_live_bytes", "wcs_last_spill_peak_interner_bytes",
  "wcs_dict_traffic_len", "wcs_dict_traffic_ptr", "wcs_mem_growth_len", "wcs_mem_growth_ptr",
  "wcs_spill_boundary_len", "wcs_spill_boundary_ptr",
  "wcs_sp_open_echo", "wcs_sp_open_graph", "wcs_sp_close", "wcs_sp_step",
  "wcs_sp_in_ptr", "wcs_sp_in_free", "wcs_sp_in_commit", "wcs_sp_in_end", "wcs_sp_in_absent",
  "wcs_sp_out_len", "wcs_sp_out_ptr", "wcs_sp_out_release", "wcs_sp_out_done",
  "wcs_sp_read_at_ticket", "wcs_sp_read_at_source", "wcs_sp_read_at_offset", "wcs_sp_read_at_len",
  "wcs_sp_complete_read_at", "wcs_sp_take_read_at", "wcs_sp_error_ptr", "wcs_sp_error_len",
  "wcs_sp_spill_op", "wcs_sp_spill_append_len", "wcs_sp_spill_append_ptr",
  "wcs_sp_spill_run_upload_len", "wcs_sp_spill_run_upload_ptr", "wcs_sp_spill_read_slot", "wcs_sp_spill_complete",
  "wcs_sp_is_resident", "wcs_sp_command", "wcs_sp_response_ptr", "wcs_sp_response_len",
] as const satisfies readonly (keyof PoolWasm)[];

const PLANNER_EXPORTS = [
  "wcol_plan_query", "wcol_plan_output_ptr", "wcol_job_sources", "wcol_plan_job",
  "wcol_concat_program", "wcol_plan_late_read", "wcol_result_json", "wcol_result_output_ptr",
  "wcol_rows_by_id_json", "wcol_iterative_kmeans_merge_moments", "wcol_iterative_kmeans_prepare",
  "wcol_iterative_kmeans_merge", "wcol_iterative_kmeans_update", "wcol_iterative_kmeans_centroids_json",
  "wcol_iterative_kmeans_output_ptr",
] as const satisfies readonly (keyof PlannerExports)[];

function missingFunctions(exports: WebAssembly.Exports, names: readonly string[]): string[] {
  return names.filter((name) => typeof exports[name] !== "function");
}

/**
 * The stream ABI of an instantiated node or coordinator module. WebAssembly types exports
 * loosely, so this is the one place they are read as `WcolStreamWasm`: every pool export
 * must be there, and a module with any planner export must have all of them.
 */
export function streamWasmExports(instance: WebAssembly.Instance): WcolStreamWasm {
  const exports = instance.exports;
  if (!(exports.memory instanceof WebAssembly.Memory)) throw new Error("stream wasm missing memory export");
  const missingPool = missingFunctions(exports, POOL_EXPORTS);
  if (missingPool.length > 0) throw new Error(`stream wasm missing exports: ${missingPool.join(", ")}`);
  const missingPlanner = missingFunctions(exports, PLANNER_EXPORTS);
  if (missingPlanner.length > 0 && missingPlanner.length < PLANNER_EXPORTS.length) {
    throw new Error(`coordinator wasm missing planner exports: ${missingPlanner.join(", ")}`);
  }
  return exports as unknown as WcolStreamWasm;
}

/** The coordinator module, or an error naming `what` needed it. */
export function plannerWasm(wasm: WcolStreamWasm, what: string): PlannerWasm {
  // `streamWasmExports` checked the planner exports are all present or all absent.
  if (typeof wasm.wcol_plan_job !== "function") throw new Error(`${what} needs the coordinator Wasm`);
  return wasm as PlannerWasm;
}

/** Coordinator plans and reduces. The node Wasm runs fragment programs. */
export type GraphWasm = {
  node: WcolStreamWasm;
  coordinator: WcolStreamWasm;
};

export type StreamRunStats = {
  /** Epoch ms when every input had ended; the rest of the run is finish work (eg. spill merge). */
  inputsEndedAt?: number;
  /** Bytes the host wrote into ingress ports. */
  inputBytes: number;
  outputBytes: number;
  maxWasmMemoryBytes: number;
  /** Groups in the hash table after the last aggregate finish. */
  residentGroups: number;
  /** Group-table bytes after the last aggregate finish. */
  residentGroupBytes: number;
  spillRuns: number;
  spillGroupsFlushed: number;
  spillPeakLiveBytes: number;
  spillPeakInternerBytes: number;
  spillBoundaries: SpillBoundary[];
  dictTraffic?: DictTraffic;
  memGrowth: MemGrowthSample[];
  /** Spill driver totals; absent when the host has no spill bucket. */
  spillDiag?: {
    pollIterations: number;
    spillExec: number;
    spillExecByOp: { begin: number; scan: number; end: number; commit: number };
    io: SpillIo;
  };
};
