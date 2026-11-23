import { COLUMN_INFO_BYTES, FLAG_DICT, TYPE_STRING, textDecoder } from "../core/constants.ts";
import { readOutBytes, withBytes } from "../wasm/helpers.ts";
import { toBytes } from "../wasm/wasm.ts";
import { loadRuntimeDict } from "./dicts.ts";
import type { WcolContext } from "../core/context.ts";
import type { ColumnInfo, ColumnRef } from "../core/types.ts";

export async function resolveColumnId(ctx: WcolContext, ref: ColumnRef): Promise<number> {
  if (typeof ref === "number") {
    return ref;
  }
  return withBytes(ctx.wasm, toBytes(String(ref)), (ptr, len) => {
    const id = ctx.wasm.exports.runtime_column_id_by_name(ctx.runtime, ptr, len);
    if (id < 0) {
      throw new Error(`Unknown column: ${ref}`);
    }
    return id;
  });
}

export async function getColumnInfo(ctx: WcolContext, colId: number): Promise<ColumnInfo> {
  const cached = ctx.columnInfoCache.get(colId);
  if (cached) {
    return cached;
  }
  const infoBytes = await readOutBytes(
    ctx.wasm,
    (outPtr, outLen) => ctx.wasm.exports.runtime_column_info(ctx.runtime, colId, outPtr, outLen),
    COLUMN_INFO_BYTES
  );
  const view = new DataView(infoBytes.buffer, infoBytes.byteOffset, infoBytes.byteLength);
  const info: ColumnInfo = {
    logicalType: infoBytes[0],
    physicalType: infoBytes[1],
    flags: infoBytes[2],
    encoding: infoBytes[3],
    dictId: view.getUint32(4, true),
    scale: view.getInt32(8, true)
  };
  ctx.columnInfoCache.set(colId, info);
  return info;
}

/** ISO date (YYYY-MM-DD) → days since Unix epoch (matches DuckDB DATE / wcol numeric dates). */
export function epochDaysFromIsoDate(value: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }
  const ms = Date.parse(`${value}T00:00:00Z`);
  if (Number.isNaN(ms)) {
    return null;
  }
  return Math.floor(ms / 86_400_000);
}

export async function getColumnName(ctx: WcolContext, colId: number): Promise<string> {
  const cached = ctx.columnNameCache.get(colId);
  if (cached) {
    return cached;
  }
  const nameBytes = await readOutBytes(
    ctx.wasm,
    (outPtr, outLen) => ctx.wasm.exports.runtime_column_name(ctx.runtime, colId, outPtr, outLen),
    64
  );
  const name = textDecoder.decode(nameBytes);
  ctx.columnNameCache.set(colId, name);
  return name;
}

export async function normalizeValue(
  ctx: WcolContext,
  colId: number,
  info: ColumnInfo,
  value: unknown
): Promise<number> {
  if (value === undefined || value === null) {
    return 0;
  }
  if (info.physicalType === TYPE_STRING) {
    throw new Error("String filters require dictionary-encoded columns");
  }
  if ((info.flags & FLAG_DICT) !== 0 && typeof value === "string") {
    let dict = ctx.dicts.get(info.dictId);
    if (!dict) {
      dict = await loadRuntimeDict(ctx.wasm, ctx.runtime, colId);
      ctx.dicts.set(info.dictId, dict);
    }
    const id = dict.get(value);
    return id !== undefined ? id : 0xffffffff;
  }
  if (typeof value === "string") {
    const asDate = epochDaysFromIsoDate(value);
    if (asDate !== null) {
      return asDate;
    }
  }
  return Number(value);
}

export async function normalizeInList(
  ctx: WcolContext,
  colId: number,
  info: ColumnInfo,
  values: unknown
): Promise<number[]> {
  if (!Array.isArray(values)) {
    throw new Error("Filter 'in' requires an array of values");
  }
  if ((info.flags & FLAG_DICT) !== 0) {
    let dict = ctx.dicts.get(info.dictId);
    if (!dict) {
      dict = await loadRuntimeDict(ctx.wasm, ctx.runtime, colId);
      ctx.dicts.set(info.dictId, dict);
    }
    const out = new Array<number>(values.length);
    for (let i = 0; i < values.length; i += 1) {
      const value = values[i];
      if (value === undefined || value === null) {
        out[i] = 0;
        continue;
      }
      if (typeof value === "string") {
        const id = dict.get(value);
        out[i] = id !== undefined ? id : 0xffffffff;
      } else {
        out[i] = Number(value);
      }
    }
    return out;
  }
  const out = new Array<number>(values.length);
  for (let i = 0; i < values.length; i += 1) {
    out[i] = await normalizeValue(ctx, colId, info, values[i]);
  }
  return out;
}
