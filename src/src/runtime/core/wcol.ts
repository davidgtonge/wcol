import { ByteSource } from "../io/byte-source.ts";
import { loadWasm } from "../wasm/wasm.ts";
import type { WasmBindings } from "../wasm/wasm.ts";
import { normalizeSource } from "../io/sources.ts";
import {
  decodeDictValue,
  loadRuntimeDict,
  loadRuntimeDictBlob,
  lookupRuntimeDictValue,
} from "../schema/dicts.ts";
import { bindRuntimeFromInitBytes, loadRuntimeInitBytes } from "../wasm/runtime-init.ts";
import { applyPlan } from "../query/plan.ts";
import { executePlan, executePlanFromPlan, warmWorkerPool } from "../exec/chunks.ts";
import { getColumnInfo, getColumnName, normalizeInList, normalizeValue, resolveColumnId } from "../schema/columns.ts";
import { FLAG_DICT } from "./constants.ts";
import type { WcolContext } from "./context.ts";
import type {
  ColumnInfo,
  ColumnRef,
  DictLookup,
  DictsMap,
  HeaderInfo,
  QueryPlan,
  QueryOptions,
  QueryResult,
  RuntimeInitBytes
} from "./types.ts";

export type { QueryPlan, QueryOptions, QueryResult } from "./types.ts";
export { FilterOp, CombineOp } from "./constants.ts";
export { executePlanFromPlan } from "../exec/chunks.ts";
export {
  buildPlan,
  FILTER_OPS,
  EXAMPLE_PLAN,
  type FilterSpec,
  type AggregateSpec,
  type GroupBySpec,
} from "../query/plan-format.ts";

export class WcolFile {
  static async open(source: ByteSource | File | string): Promise<WcolFile> {
    const byteSource = normalizeSource(source);
    const wasm = await loadWasm();
    const initBytes = await loadRuntimeInitBytes(wasm, byteSource);
    const { runtime, header } = await bindRuntimeFromInitBytes(wasm, initBytes);
    return new WcolFile(byteSource, wasm, runtime, header, new Map(), initBytes);
  }

  source: ByteSource;
  wasm: WasmBindings;
  runtime: number;
  header: HeaderInfo;
  columnInfoCache: Map<number, ColumnInfo>;
  columnNameCache: Map<number, string>;
  dicts: DictsMap;
  dictBlobs: Map<number, { offsets: Uint32Array; blob: Uint8Array }>;
  ctx: WcolContext;
  init: RuntimeInitBytes;

  constructor(
    source: ByteSource,
    wasm: WasmBindings,
    runtime: number,
    header: HeaderInfo,
    dicts: DictsMap,
    init: RuntimeInitBytes
  ) {
    this.source = source;
    this.wasm = wasm;
    this.runtime = runtime;
    this.header = header;
    this.columnInfoCache = new Map();
    this.columnNameCache = new Map();
    this.dicts = dicts ?? new Map();
    this.dictBlobs = new Map();
    this.init = init;
    this.ctx = {
      source,
      wasm,
      runtime,
      header,
      columnInfoCache: this.columnInfoCache,
      columnNameCache: this.columnNameCache,
      dicts: this.dicts,
      init
    };
  }

  /** Spawn worker threads and apply an optional plan shell (no chunk scan). */
  async warmWorkers(workers: number, planInput?: QueryPlan): Promise<void> {
    await warmWorkerPool(this.ctx, workers, { planSpec: planInput ?? { limit: 0 } });
  }

  async query(planInput?: QueryPlan, options?: QueryOptions): Promise<QueryResult> {
    const planSpec = planInput ?? {};
    const plan = this.wasm.exports.create_plan(this.runtime);
    this.wasm.exports.plan_reset_results(plan);

    await applyPlan(this.ctx, planSpec, plan);
    const result = await executePlan(this.ctx, planSpec, plan, options);

    this.wasm.exports.destroy_plan(plan);
    return result;
  }

  resolveColumnId(ref: ColumnRef): Promise<number> {
    return resolveColumnId(this.ctx, ref);
  }

  getColumnInfo(colId: number): Promise<ColumnInfo> {
    return getColumnInfo(this.ctx, colId);
  }

  getColumnName(colId: number): Promise<string> {
    return getColumnName(this.ctx, colId);
  }

  normalizeValue(colId: number, info: ColumnInfo, value: unknown): Promise<number> {
    return normalizeValue(this.ctx, colId, info, value);
  }

  normalizeInList(colId: number, info: ColumnInfo, values: unknown): Promise<number[]> {
    return normalizeInList(this.ctx, colId, info, values);
  }

  async getColumnDict(colId: number): Promise<DictLookup | undefined> {
    const info = await this.getColumnInfo(colId);
    const cached = this.dicts.get(info.dictId);
    if (cached) {
      return cached;
    }
    if ((info.flags & FLAG_DICT) === 0) {
      return undefined;
    }
    const lookup = await loadRuntimeDict(this.wasm, this.runtime, colId);
    this.dicts.set(info.dictId, lookup);
    return lookup;
  }

  async getColumnDictValue(colId: number, valueId: number): Promise<string | undefined> {
    const info = await this.getColumnInfo(colId);
    if ((info.flags & FLAG_DICT) === 0) {
      return undefined;
    }
    let blobInfo = this.dictBlobs.get(info.dictId);
    if (!blobInfo) {
      try {
        blobInfo = await loadRuntimeDictBlob(this.wasm, this.runtime, colId);
        this.dictBlobs.set(info.dictId, blobInfo);
      } catch {
        // Dict stored as runtime value table, not offset/blob layout.
      }
    }
    if (blobInfo && blobInfo.offsets.length > 0) {
      const label = decodeDictValue(blobInfo.offsets, blobInfo.blob, valueId);
      if (label !== undefined) {
        return label;
      }
    }
    return lookupRuntimeDictValue(this.wasm, this.runtime, colId, valueId);
  }

  async getRuntimeDictValue(colId: number, poolId: number): Promise<string> {
    const label = await lookupRuntimeDictValue(this.wasm, this.runtime, colId, poolId);
    return label ?? String(poolId);
  }
}
