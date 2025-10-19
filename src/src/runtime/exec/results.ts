import { readOutBytes } from "../wasm/helpers.ts";
import { readU64 } from "../io/header.ts";
import type { WasmBindings } from "../wasm/wasm.ts";
import type { AggregateStats, ColumnNameResolver, GroupAggInfo, GroupKeyInfo, GroupResult, U64 } from "../core/types.ts";

export async function readRows(wasm: WasmBindings, plan: number): Promise<U64[]> {
  const bytes = await readRowBytes(wasm, plan);
  if (!bytes.byteLength) return [];
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = bytes.byteLength / 8;
  const rows = new Array<U64>(count);
  for (let i = 0; i < count; i += 1) {
    rows[i] = readU64(view, i * 8);
  }
  return rows;
}

export async function readRowBytes(wasm: WasmBindings, plan: number): Promise<Uint8Array> {
  const count = wasm.exports.plan_rows_len(plan);
  if (count <= 0) {
    return new Uint8Array(0);
  }
  return readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_rows(plan, outPtr, outLen),
    count * 8
  );
}

export type RowKeyPayload =
  | { kind: "num"; num: number }
  | { kind: "bytes"; bytes: Uint8Array }
  | { kind: "null" };

export type RowCandidatePayload = {
  rowId: U64;
  k1: RowKeyPayload;
  k2?: RowKeyPayload;
};

export async function readRowCandidates(
  wasm: WasmBindings,
  plan: number
): Promise<RowCandidatePayload[]> {
  const bytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_row_candidates(plan, outPtr, outLen),
    256
  );
  if (bytes.byteLength === 0) {
    return [];
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const out: RowCandidatePayload[] = [];
  let offset = 0;
  while (offset < bytes.byteLength) {
    const rowId = readU64(view, offset);
    offset += 8;
    const k1Type = view.getUint8(offset);
    const k2Type = view.getUint8(offset + 1);
    offset += 4;
    const k1Num = view.getFloat64(offset, true);
    offset += 8;
    const k2Num = view.getFloat64(offset, true);
    offset += 8;
    const k1Len = view.getUint32(offset, true);
    offset += 4;
    const k2Len = view.getUint32(offset, true);
    offset += 4;

    let k1: RowKeyPayload;
    if (k1Type === 0) {
      k1 = { kind: "num", num: k1Num };
    } else if (k1Type === 2) {
      k1 = { kind: "null" };
    } else {
      const bytesView = bytes.subarray(offset, offset + k1Len);
      k1 = { kind: "bytes", bytes: bytesView };
      offset += k1Len;
    }

    let k2: RowKeyPayload | undefined;
    if (k2Type === 1) {
      k2 = { kind: "num", num: k2Num };
    } else if (k2Type === 3) {
      k2 = { kind: "null" };
    } else if (k2Type === 2) {
      const bytesView = bytes.subarray(offset, offset + k2Len);
      k2 = { kind: "bytes", bytes: bytesView };
      offset += k2Len;
    }

    out.push({ rowId, k1, k2 });
  }
  return out;
}

export async function readRowCandidateBytes(
  wasm: WasmBindings,
  plan: number
): Promise<Uint8Array> {
  return readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_row_candidates(plan, outPtr, outLen),
    256
  );
}

export const AGG_KIND_COUNT_STAR = 0;
export const AGG_KIND_SUM = 1;
export const AGG_KIND_AVG = 2;
export const AGG_KIND_MIN = 3;
export const AGG_KIND_MAX = 4;
export const AGG_KIND_COUNT = 5;
export const AGG_KIND_APPROX_DISTINCT = 6;

function aggregateMean(kind: number, sum: number, countValue: number): number {
  if (kind === AGG_KIND_AVG) return countValue ? sum / countValue : 0;
  if (kind === AGG_KIND_APPROX_DISTINCT) return sum;
  if (kind === AGG_KIND_COUNT || kind === AGG_KIND_COUNT_STAR) return countValue;
  return countValue ? sum / countValue : 0;
}

export type AggRecord = {
  colId: number;
  kind: number;
  offset: number;
  sum: number;
  min: number;
  max: number;
  count: number;
};

/** True when plan or group aggregates use approx distinct (forces single-threaded merge). */
export async function planUsesApproxDistinct(wasm: WasmBindings, plan: number): Promise<boolean> {
  const aggs = await readAggregateRecords(wasm, plan);
  if (aggs.some((a) => a.kind === AGG_KIND_APPROX_DISTINCT)) return true;
  const count = wasm.exports.plan_group_agg_count(plan);
  if (count <= 0) return false;
  const bytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_group_aggs(plan, outPtr, outLen),
    count * 8
  );
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0, off = 0; i < count; i += 1, off += 8) {
    if (view.getUint8(off + 4) === AGG_KIND_APPROX_DISTINCT) return true;
  }
  return false;
}

export async function readAggregateRecords(wasm: WasmBindings, plan: number): Promise<AggRecord[]> {
  const count = wasm.exports.plan_agg_count(plan);
  if (count <= 0) {
    return [];
  }
  const recordSize = 4 + 1 + 3 + 8 + 8 + 8 + 4;
  const bytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_aggs(plan, outPtr, outLen),
    count * recordSize
  );
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const output: AggRecord[] = [];
  let offset = 0;
  for (let i = 0; i < count; i += 1) {
    const colId = view.getUint32(offset, true);
    offset += 4;
    const kind = view.getUint8(offset);
    offset += 1;
    const offRaw = view.getUint8(offset);
    const aggOffset = (offRaw << 24) >> 24;
    offset += 3;
    const sum = view.getFloat64(offset, true);
    offset += 8;
    const min = view.getFloat64(offset, true);
    offset += 8;
    const max = view.getFloat64(offset, true);
    offset += 8;
    const countValue = view.getUint32(offset, true);
    offset += 4;
    output.push({ colId, kind, offset: aggOffset, sum, min, max, count: countValue });
  }
  return output;
}

export async function readAggregateBytes(wasm: WasmBindings, plan: number): Promise<Uint8Array> {
  const count = wasm.exports.plan_agg_count(plan);
  if (count <= 0) {
    return new Uint8Array(0);
  }
  const recordSize = 4 + 1 + 3 + 8 + 8 + 8 + 4;
  return readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_aggs(plan, outPtr, outLen),
    count * recordSize
  );
}

async function aggregateRecordLabel(file: ColumnNameResolver, rec: AggRecord): Promise<string> {
  const colName = await file.getColumnName(rec.colId);
  const expr =
    rec.offset === 0
      ? colName
      : `${colName} ${rec.offset > 0 ? "+" : "-"} ${Math.abs(rec.offset)}`;
  if (rec.kind === AGG_KIND_COUNT_STAR) return "count_star()";
  if (rec.kind === AGG_KIND_COUNT) return `count(${expr})`;
  if (rec.kind === AGG_KIND_SUM) return `sum(${expr})`;
  if (rec.kind === AGG_KIND_AVG) return `avg(${expr})`;
  if (rec.kind === AGG_KIND_MIN) return `min(${expr})`;
  if (rec.kind === AGG_KIND_MAX) return `max(${expr})`;
  if (rec.kind === AGG_KIND_APPROX_DISTINCT) return `approx_count_distinct(${expr})`;
  return expr;
}

export async function readAggregates(
  file: ColumnNameResolver,
  wasm: WasmBindings,
  plan: number
): Promise<Record<string, AggregateStats>> {
  const records = await readAggregateRecords(wasm, plan);
  const output: Record<string, AggregateStats> = {};
  for (const rec of records) {
    const name = await aggregateRecordLabel(file, rec);
    output[name] = {
      count: rec.count,
      sum: rec.sum,
      min: rec.min,
      max: rec.max,
      mean: aggregateMean(rec.kind, rec.sum, rec.count),
    };
  }
  return output;
}

export async function readGroups(wasm: WasmBindings, plan: number): Promise<GroupResult | null> {
  const count = wasm.exports.plan_group_count(plan);
  if (count <= 0) {
    return null;
  }
  const keyCount = wasm.exports.plan_group_key_count(plan);
  let keyInfo: GroupKeyInfo[] | undefined;
  if (keyCount > 0) {
    const infoBytes = await readOutBytes(
      wasm,
      (outPtr, outLen) => wasm.exports.plan_group_key_info(plan, outPtr, outLen),
      keyCount * 8
    );
    const infoView = new DataView(infoBytes.buffer, infoBytes.byteOffset, infoBytes.byteLength);
    keyInfo = [];
    let infoOffset = 0;
    for (let i = 0; i < keyCount; i += 1) {
      const colId = infoView.getUint32(infoOffset, true);
      infoOffset += 4;
      const physicalType = infoView.getUint8(infoOffset);
      infoOffset += 1;
      const flags = infoView.getUint8(infoOffset);
      infoOffset += 1;
      infoOffset += 2;
      keyInfo.push({ colId, physicalType, flags });
    }
  }
  const aggCount = wasm.exports.plan_group_agg_count(plan);
  const aggs: GroupAggInfo[] = [];
  if (aggCount > 0) {
    const aggBytes = await readOutBytes(
      wasm,
      (outPtr, outLen) => wasm.exports.plan_copy_group_aggs(plan, outPtr, outLen),
      aggCount * 8
    );
    const aggView = new DataView(aggBytes.buffer, aggBytes.byteOffset, aggBytes.byteLength);
    let aggOffset = 0;
    for (let i = 0; i < aggCount; i += 1) {
      const colId = aggView.getUint32(aggOffset, true);
      aggOffset += 4;
      const kind = aggView.getUint8(aggOffset);
      aggOffset += 4;
      aggs.push({ colId, kind });
    }
  }
  const recordSize = 16 + aggs.length * 32;
  const bytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_groups(plan, outPtr, outLen),
    count * recordSize
  );
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const output: GroupResult = {
    keys: [],
    keys2: keyCount > 1 ? [] : undefined,
    keyInfo,
    aggs,
    values: []
  };
  let offset = 0;
  for (let i = 0; i < count; i += 1) {
    const key = readU64(view, offset);
    offset += 8;
    const key2 = readU64(view, offset);
    offset += 8;
    const aggValues: AggregateStats[] = [];
    for (let aggIdx = 0; aggIdx < aggs.length; aggIdx += 1) {
      const sum = view.getFloat64(offset, true);
      offset += 8;
      const min = view.getFloat64(offset, true);
      offset += 8;
      const max = view.getFloat64(offset, true);
      offset += 8;
      const countValue = view.getUint32(offset, true);
      offset += 4;
      offset += 4;
      const kind = aggs[aggIdx]?.kind ?? AGG_KIND_SUM;
      aggValues.push({
        count: countValue,
        sum,
        min,
        max,
        mean: aggregateMean(kind, sum, countValue),
      });
    }

    output.keys.push(key);
    if (output.keys2) {
      output.keys2.push(key2);
    }
    output.values.push(aggValues);
  }
  return output;
}

export async function readGroupBytes(wasm: WasmBindings, plan: number): Promise<Uint8Array> {
  if (wasm.exports.plan_group_dict_hist_active(plan) > 0) {
    const dictLen = wasm.exports.plan_group_dict_hist_dict_len(plan);
    if (dictLen <= 0) {
      return new Uint8Array(0);
    }
    const n = dictLen;
    const need = 12 + n * 4 + n * 8;
    return readOutBytes(
      wasm,
      (outPtr, outLen) => wasm.exports.plan_copy_group_hist_partial(plan, outPtr, outLen),
      need
    );
  }

  const count = wasm.exports.plan_group_count(plan);
  if (count <= 0) {
    return new Uint8Array(0);
  }
  const aggCount = wasm.exports.plan_group_agg_count(plan);
  if (aggCount <= 0) {
    return new Uint8Array(0);
  }
  const recordSize = 16 + aggCount * (8 + 8 + 8 + 4 + 4);
  if (recordSize <= 0) {
    return new Uint8Array(0);
  }
  return readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_groups(plan, outPtr, outLen),
    count * recordSize
  );
}
