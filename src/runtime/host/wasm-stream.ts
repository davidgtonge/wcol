/**
 * Tiny host for wcol-stream-core (wasm32). All columnar work stays in Rust.
 *
 * Input/output are BYTE streams: writes may split a WCS1 frame. Network/DO
 * transports must support arbitrary byte chunks; do not pass chunks directly
 * to the existing `StreamSink.write(frame)` frame-oriented API.
 */


export interface WcolStreamWasm {
  memory: WebAssembly.Memory;
  wcs_alloc(length: number): number;
  wcs_dealloc(ptr: number, length: number): void;
  /** Last error from a stateless FFI call (plan, reduce, partial helpers). */
  wcs_last_error_ptr?(): number;
  wcs_last_error_len?(): number;
  /** `[page_replay_count, page_events_visited, page_events_committed]` after Parquet ingest graphs. */
  wcs_ingest_replay_totals?(outPtr: number): void;
  /** Groups in the hash table after the last aggregate finish. */
  wcs_last_resident_groups?(): number;
  /** Group-table bytes after the last aggregate finish. */
  wcs_last_resident_group_bytes?(): number;
  /** Sorted runs flushed by the current reducer (discard or host store). */
  wcs_last_spill_runs?(): number;
  wcs_last_spill_groups_flushed?(): number;
  wcs_last_spill_peak_live_bytes?(): number;
  wcs_last_spill_peak_interner_bytes?(): number;
  /** Nine little-endian u64s: sent entries/bytes, referenced entries/bytes, retained, released, peak, max stream, streams released. */
  wcs_dict_traffic_len?(): number;
  wcs_dict_traffic_ptr?(): number;
  /** 11 little-endian u64s per Wasm-growth sample. */
  wcs_mem_growth_len?(): number;
  wcs_mem_growth_ptr?(): number;
  /** Byte length of the spill-boundary table. Twelve little-endian u32s per run. */
  wcs_spill_boundary_len?(): number;
  wcs_spill_boundary_ptr?(): number;
  wcs_spill_note_host_ack?(pendingOps: number, writeBytes: number, pendingSqlBytes: number): void;
  /** Frame bytes not yet copied to the host. */
  wcs_last_egress_pending_bytes?(): number;
  /** High-water mark of egress bytes waiting to be copied. */
  wcs_last_egress_peak_pending_bytes?(): number;
  wcs_sp_open_echo?(
    inputPorts: number,
    outputPorts: number,
    inputKib: number,
    outputKib: number,
  ): number;
  wcs_sp_open_graph?(
    programPtr: number,
    programLen: number,
    scratchBytes: number,
    inputPorts: number,
    outputPorts: number,
    inputKib: number,
    outputKib: number,
    budgetShare: number,
  ): number;
  wcs_sp_close?(handle: number): void;
  wcs_sp_step?(handle: number, budget: number): number;
  wcs_sp_in_ptr?(handle: number, port: number): number;
  wcs_sp_in_free?(handle: number, port: number): number;
  wcs_sp_in_commit?(handle: number, port: number, length: number): number;
  wcs_sp_in_end?(handle: number, port: number): number;
  wcs_sp_in_absent?(handle: number, port: number): number;
  wcs_sp_out_len?(handle: number, port: number): number;
  wcs_sp_out_ptr?(handle: number, port: number): number;
  wcs_sp_out_release?(handle: number, port: number): number;
  wcs_sp_out_done?(handle: number, port: number): number;
  wcs_sp_read_at_ticket?(handle: number): number;
  wcs_sp_read_at_source?(handle: number): number;
  /** A Rust `u64`, so a `BigInt` in JS. */
  wcs_sp_read_at_offset?(handle: number): bigint;
  wcs_sp_read_at_len?(handle: number): number;
  wcs_sp_complete_read_at?(handle: number, ticket: number, ptr: number, len: number): number;
  /** Claim the exposed ReadAt so another can be exposed while it is in flight. */
  wcs_sp_take_read_at?(handle: number): void;
  wcs_sp_error_ptr?(handle: number): number;
  wcs_sp_error_len?(handle: number): number;
  wcs_sp_spill_op?(handle: number): number;
  wcs_sp_spill_append_len?(handle: number): number;
  wcs_sp_spill_append_ptr?(handle: number): number;
  wcs_sp_spill_run_upload_len?(handle: number): number;
  wcs_sp_spill_run_upload_ptr?(handle: number): number;
  /** Reused buffer for the pending run read (0 on error); fill it, then complete with no bytes. */
  wcs_sp_spill_read_slot?(handle: number, len: number): number;
  wcs_sp_spill_complete?(handle: number, ok: number, scanPtr: number, scanLen: number): number;
  /** PWCL payload; first aggregate sum or `Number.MIN_SAFE_INTEGER` on error. */
  wcol_partial_first_sum_i(ptr: number, len: number): number;
  /** `1` when two PWCL bodies are equivalent (`encoded_partials_equivalent` in Rust). */
  wcol_partials_wire_equivalent?(
    leftPtr: number,
    leftLen: number,
    rightPtr: number,
    rightLen: number,
  ): number;
  /** Stream catalog + SQL → needed column ids (`u32` count then ids). Returns bytes written or `0`. */
  wcol_columns_for_sql?(
    catalogPtr: number,
    catalogLen: number,
    sqlPtr: number,
    sqlLen: number,
    outPtr: number,
    outCap: number,
  ): number;
  /** Plan SQL over a Parquet footer into node programs (`planQueryDispatch`). */
  wcol_plan_query?(ptr: number, len: number): number;
  wcol_plan_output_ptr?(): number;
  /** Incremental coordinator reduce (`CoordinatorReducer`). */
  wcol_reduce_open?(): number;
  wcol_reduce_push?(handle: number, ptr: number, len: number): number;
  wcol_reduce_finish?(handle: number): number;
  wcol_reduce_output_ptr?(handle: number): number;
  wcol_reduce_close?(handle: number): void;
  /** Packed node programs: `u32` count, then `u32` length plus bytes. Returns byte length or `0`. */
  wcol_split_row_group_programs?(ptr: number, len: number): number;
  wcol_split_row_group_programs_ptr?(): number;
  /** Resident graph control; the graph's plan determines its lifetime and kernel. */
  wcs_sp_is_resident?(handle: number): number;
  wcs_sp_command?(handle: number, kind: number, ptr: number, len: number): number;
  wcs_sp_response_ptr?(handle: number): number;
  wcs_sp_response_len?(handle: number): number;
  wcol_iterative_kmeans_merge_moments?(
    leftPtr: number, leftLen: number, rightPtr: number, rightLen: number, dimensions: number,
  ): number;
  wcol_iterative_kmeans_prepare?(
    momentsPtr: number, momentsLen: number, dimensions: number, clusters: number,
    maxIterations: number, tolerance: number, seed: bigint,
  ): number;
  wcol_iterative_kmeans_merge?(
    leftPtr: number, leftLen: number, rightPtr: number, rightLen: number,
    dimensions: number, clusters: number, maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_update?(
    statePtr: number, stateLen: number, partialPtr: number, partialLen: number,
    dimensions: number, clusters: number, maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_centroids_json?(
    statePtr: number, stateLen: number, dimensions: number, clusters: number,
    maxIterations: number, tolerance: number,
  ): number;
  wcol_iterative_kmeans_output_ptr?(): number;
  /**
   * Client JSON (`{"columns":[..],"rows":[[..],..]}`) from the plan's result columns and the
   * root's row frames (`kind` 0) or a footer-only partial (`kind` 1). Coordinator Wasm only.
   */
  wcol_result_json?(
    columnsPtr: number,
    columnsLen: number,
    kind: number,
    dataPtr: number,
    dataLen: number,
    rowCap: number,
  ): number;
  wcol_result_output_ptr?(): number;
  /**
   * Client JSON of a late row job: the pick's result columns and row frames (ids in result
   * order), then the read-back's. Output at `wcol_result_output_ptr`. Coordinator Wasm only.
   */
  wcol_rows_by_id_json?(
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
  /** `POST /jobs` body → its kind and sources (JSON at `wcol_plan_output_ptr`). */
  wcol_job_sources?(ptr: number, len: number): number;
  /** Plan a job from its body and footers (binary at `wcol_plan_output_ptr`). */
  wcol_plan_job?(ptr: number, len: number): number;
  wcol_concat_program?(ptr: number, len: number): number;
  /** Plan a late row job's read-back from its pick's row frames and the job request. */
  wcol_plan_late_read?(ptr: number, len: number): number;
}

/** Fragment session. The node Wasm exports these and does not export planning or reduce. */
/**
 * The stream ABI of an instantiated node or coordinator module. WebAssembly types exports
 * loosely, so this is the one place they are read as `WcolStreamWasm`; instantiation then
 * checks the exports its role needs.
 */
export function streamWasmExports(instance: WebAssembly.Instance): WcolStreamWasm {
  const exports = instance.exports as unknown as WcolStreamWasm;
  if (!exports.memory) throw new Error("stream wasm missing memory export");
  return exports;
}

export type NodeSessionWasm = WcolStreamWasm;

/** Planning and streaming reduce. The coordinator Wasm also exports the fragment session. */
export type CoordinatorSessionWasm = WcolStreamWasm;

/** Coordinator plans and reduces. The node Wasm runs fragment programs. */
export type GraphWasm = {
  node: NodeSessionWasm;
  coordinator: CoordinatorSessionWasm;
};

export function asGraphWasm(wasm: WcolStreamWasm | GraphWasm): GraphWasm {
  if ("node" in wasm && "coordinator" in wasm) return wasm;
  return { node: wasm, coordinator: wasm };
}

export type StreamRunStats = {
  /** Epoch ms when every input had ended; the rest of the run is finish work (eg. spill merge). */
  inputsEndedAt?: number;
  inputBytes: number;
  outputBytes: number;
  maxWasmMemoryBytes: number;
  /** Groups in the hash table after the last aggregate finish. */
  residentGroups?: number;
  /** Group-table bytes after the last aggregate finish. */
  residentGroupBytes?: number;
  spillRuns?: number;
  spillGroupsFlushed?: number;
  spillPeakLiveBytes?: number;
  spillPeakInternerBytes?: number;
  spillBoundaries?: import("./host-types.ts").SpillBoundary[];
  dictTraffic?: import("./host-types.ts").DictTraffic;
  memGrowth?: import("./host-types.ts").MemGrowthSample[];
  /** Host time blocked in `readAt` (ms). */
  readAtWaitMs?: number;
  readAtCalls?: number;
  ingestReplay?: {
    pageReplayCount: number;
    pageEventsVisited: number;
    pageEventsCommitted: number;
    amplification: number;
  };
  /** Poll-loop diagnostics (spill debugging). */
  spillDiag?: {
    pollIterations: number;
    spillExec: number;
    spillExecByOp: { begin: number; append: number; scan: number; end: number; commit: number };
    io: import("./spill-r2-pool-driver.ts").SpillIo;
  };
  maxPollsBetweenYields?: number;
  sqliteBatchesBetweenYields?: number;
};

export { decodeWasmUtf8, readWasmLastError } from "./wasm.ts";
export { runWcolEngine, type WcolHost } from "./run-wcol-engine.ts";
