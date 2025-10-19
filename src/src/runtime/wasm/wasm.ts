import { wasmExports, alloc, free, memoryU8, setInstance } from "../../../rust/wcol-wasm/pkg/core.js";

export type WasmExports = {
  create_runtime(): number;
  destroy_plan(plan: number): void;
  create_plan(runtime: number): number;
  plan_apply_sql(plan: number, sqlPtr: number, sqlLen: number): number;
  plan_reset_results(plan: number): void;
  plan_set_limit(plan: number, limit: number): number;
  plan_set_select(plan: number, ptr: number, len: number): number;
  plan_select_count(plan: number): number;
  plan_projection_begin(plan: number): number;
  plan_materialize_chunk(
    runtime: number,
    plan: number,
    chunkId: number,
    descPtr: number,
    descLen: number,
    dataPtr: number,
    dataLen: number,
    localRowsPtr: number,
    localRowsLen: number,
    dstRowsPtr: number,
    dstRowsLen: number
  ): number;
  plan_materialize_required_pages(
    runtime: number,
    plan: number,
    chunkId: number,
    indexPtr: number,
    indexLen: number,
    rawLen: number,
    outPtr: number,
    outLen: number
  ): number;
  plan_materialize_required_pages_cached(
    runtime: number,
    plan: number,
    chunkId: number,
    outPtr: number,
    outLen: number
  ): number;
  plan_copy_row_projection(plan: number, outPtr: number, outLen: number): number;
  plan_add_filter_in(plan: number, colId: number, op: number, ptr: number, len: number): number;
  plan_add_filter(plan: number, colId: number, op: number, value: number, value2: number): number;
  plan_set_filter_value_str(plan: number, idx: number, ptr: number, len: number): number;
  plan_set_combine(plan: number, ptr: number, len: number): number;
  plan_set_group_by(plan: number, key1: number, key2: number, valueCol: number): number;
  plan_set_group_order_by_count(plan: number, enabled: number): number;
  plan_prepare_optimizations(plan: number): number;
  plan_group_dict_hist_active(plan: number): number;
  plan_group_dict_hist_dict_len(plan: number): number;
  plan_copy_group_hist_partial(plan: number, outPtr: number, outLen: number): number;
  plan_add_aggregate(plan: number, colId: number): number;
  plan_required_pages(
    runtime: number,
    plan: number,
    chunkId: number,
    indexPtr: number,
    indexLen: number,
    rawLen: number,
    outPtr: number,
    outLen: number
  ): number;
  plan_exec_chunk(
    runtime: number,
    plan: number,
    chunkId: number,
    descPtr: number,
    descLen: number,
    dataPtr: number,
    dataLen: number
  ): number;
  plan_filters_len(plan: number): number;
  plan_rows_len(plan: number): number;
  plan_finalize_rows(plan: number): number;
  plan_copy_row_candidates(plan: number, outPtr: number, outLen: number): number;
  plan_reducer_new(plan: number): number;
  plan_reducer_merge_aggs(plan: number, ptr: number, len: number): number;
  plan_reducer_merge_groups(plan: number, ptr: number, len: number): number;
  plan_reducer_merge_rows(plan: number, ptr: number, len: number): number;
  plan_reducer_merge_row_candidates(plan: number, ptr: number, len: number): number;
  plan_reducer_finalize(plan: number): number;
  plan_limit(plan: number): number;
  plan_row_order_by_len(plan: number): number;
  plan_group_order_by_count(plan: number): number;
  plan_copy_rows(plan: number, outPtr: number, outLen: number): number;
  plan_copy_timing(plan: number, outPtr: number, outLen: number): number;
  plan_filter_timing_len(plan: number): number;
  plan_copy_filter_timing(plan: number, outPtr: number, outLen: number): number;
  plan_filter_value_str_len(plan: number, idx: number): number;
  plan_copy_filter_value_str(plan: number, idx: number, outPtr: number, outLen: number): number;
  plan_agg_count(plan: number): number;
  plan_copy_aggs(plan: number, outPtr: number, outLen: number): number;
  plan_group_count(plan: number): number;
  plan_group_key_count(plan: number): number;
  plan_group_key_info(plan: number, outPtr: number, outLen: number): number;
  plan_group_agg_count(plan: number): number;
  plan_copy_group_aggs(plan: number, outPtr: number, outLen: number): number;
  plan_copy_groups(plan: number, outPtr: number, outLen: number): number;
  bench_f64_filter(op: number, rhs: number, rhs2: number, rows: number, iters: number, seed: bigint): bigint;
  runtime_set_header(runtime: number, ptr: number, len: number): number;
  runtime_header_info(runtime: number, outPtr: number, outLen: number): number;
  runtime_set_schema(runtime: number, ptr: number, len: number): number;
  runtime_set_toc(runtime: number, ptr: number, len: number): number;
  runtime_set_dicts(runtime: number, ptr: number, len: number): number;
  runtime_dict_blob_info(runtime: number, colId: number, outPtr: number, outLen: number): number;
  runtime_dict_len(runtime: number, colId: number): number;
  runtime_dict_value(runtime: number, colId: number, valueId: number, outPtr: number, outLen: number): number;
  runtime_dict_lookup(runtime: number, colId: number, valuePtr: number, valueLen: number): number;
  runtime_chunk_index_span(runtime: number, chunkId: number, outPtr: number, outLen: number): number;
  runtime_column_id_by_name(runtime: number, ptr: number, len: number): number;
  runtime_column_info(runtime: number, colId: number, outPtr: number, outLen: number): number;
  runtime_column_name(runtime: number, colId: number, outPtr: number, outLen: number): number;
  lz4_decompress(
    inputPtr: number,
    inputLen: number,
    rawLen: number,
    outputPtr: number,
    outputLen: number
  ): number;
};

export type WasmBindings = {
  exports: WasmExports;
  alloc: (size: number) => number;
  free: (ptr: number, len: number) => void;
  memoryU8: () => Uint8Array;
};

export type ByteInput = Uint8Array | ArrayBuffer | ArrayBufferView | string;

type WasmBackend = "auto" | "simd" | "base";

type WasmInitOptions = {
  backend?: WasmBackend;
};

const textEncoder = new TextEncoder();

let readyPromise: Promise<void> | null = null;
let initPromise: Promise<void> | null = null;
let initBackend: string | null = null;
const BROWSER_BUILD = typeof __WCOL_BROWSER_BUILD__ !== "undefined" && __WCOL_BROWSER_BUILD__;

function browserWasmUrl(): URL {
  if (typeof __WCOL_BROWSER_WASM_URL__ !== "undefined" && __WCOL_BROWSER_WASM_URL__) {
    return new URL(__WCOL_BROWSER_WASM_URL__, import.meta.url);
  }
  return new URL("../wasm/wcol_wasm.simd.wasm", import.meta.url);
}

function nodeWasmModuleSpecifier(): string {
  return ["..", "..", "..", "rust", "wcol-wasm", "pkg", "node.js"].join("/");
}

function browserWasmModuleSpecifier(): string {
  return ["..", "..", "..", "rust", "wcol-wasm", "pkg", "browser.js"].join("/");
}

function resolveBackend(backend?: WasmBackend): WasmBackend {
  if (BROWSER_BUILD) {
    return "simd";
  }
  if (backend) {
    return backend;
  }
  if (typeof process !== "undefined" && process.env && process.env.WCOL_WASM_BACKEND) {
    const env = process.env.WCOL_WASM_BACKEND;
    if (env === "simd" || env === "base") {
      return env;
    }
  }
  return "auto";
}

async function ensureInit(backend?: WasmBackend): Promise<void> {
  const resolved = resolveBackend(backend);
  if (!initPromise || initBackend !== resolved) {
    initBackend = resolved;
    if (BROWSER_BUILD) {
      const wasmBytes = await fetch(browserWasmUrl()).then((res) => {
        if (!res.ok) {
          throw new Error(`Failed to fetch SIMD wasm: ${res.status}`);
        }
        return res.arrayBuffer();
      });
      const nowMs = () => {
        if (typeof performance !== "undefined" && typeof performance.now === "function") {
          return performance.now();
        }
        return Date.now();
      };
      initPromise = WebAssembly.instantiate(wasmBytes, { env: { wcol_now_ms: nowMs } }).then(
        ({ instance }) => {
          setInstance(instance);
        }
      );
      return initPromise;
    }
    const isNode =
      typeof process !== "undefined" &&
      process.versions &&
      process.versions.node;
    const mod = isNode
      ? await import(nodeWasmModuleSpecifier())
      : await import(browserWasmModuleSpecifier());
    const nowMs = () => {
      if (typeof performance !== "undefined" && typeof performance.now === "function") {
        return performance.now();
      }
      return Date.now();
    };
    initPromise = mod.init({ env: { wcol_now_ms: nowMs } }, { backend: resolved });
  }
  return initPromise;
}

export async function loadWasm(options: WasmInitOptions = {}): Promise<WasmBindings> {
  const backend = resolveBackend(options.backend);
  if (!readyPromise || initBackend !== backend) {
    readyPromise = ensureInit(backend);
  }
  await readyPromise;
  return {
    exports: wasmExports() as WasmExports,
    alloc,
    free,
    memoryU8
  };
}

export function toBytes(input: ByteInput): Uint8Array {
  if (input instanceof Uint8Array) {
    return input;
  }
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  if (input instanceof ArrayBuffer) {
    return new Uint8Array(input);
  }
  if (typeof input === "string") {
    return textEncoder.encode(input);
  }
  throw new TypeError("Expected a TypedArray, ArrayBuffer, or string");
}

export function readFromWasm(wasm: WasmBindings, ptr: number, len: number): Uint8Array {
  return wasm.memoryU8().slice(ptr, ptr + len);
}

export function freeWasm(wasm: WasmBindings, ptr: number, len: number): void {
  wasm.free(ptr, len);
}
