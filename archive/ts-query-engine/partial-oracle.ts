/**
 * Oracle helpers for WCOL partial payloads on the WCS edge.
 *
 * Stream/engine partials are `PWCL`. The host reads those columns directly.
 */

import type { AggregateStats, QueryResult } from "./core/types.ts";
import type { ParquetPartialQuery } from "./parquet.ts";
import { loadStreamWasm } from "../../src/runtime/host/wasm.ts";
import { partialWireFormat } from "./partial-wire.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

export type PartialOracleColumn = { name: string };

const SUM_I_ERROR = BigInt(Number.MIN_SAFE_INTEGER);

/** First aggregate sum_i from a PWCL partial (Rust decode). */
export function partialFirstSumFromWire(wasm: WcolStreamWasm, partial: Uint8Array): number {
  if (partial.byteLength === 0) throw new Error("empty partial");
  const ptr = wasm.wcs_alloc(partial.byteLength);
  if (!ptr) throw new Error("wcs_alloc failed");
  try {
    new Uint8Array(wasm.memory.buffer, ptr, partial.byteLength).set(partial);
    const raw = wasm.wcol_partial_first_sum_i(ptr, partial.byteLength);
    const sum = typeof raw === "bigint" ? raw : BigInt(raw);
    if (sum === SUM_I_ERROR) throw new Error("partial sum decode failed");
    return Number(sum);
  } finally {
    wasm.wcs_dealloc(ptr, partial.byteLength);
  }
}

export async function partialFirstSumFromWireAuto(partial: Uint8Array): Promise<number> {
  const wasm = await loadStreamWasm();
  return partialFirstSumFromWire(wasm, partial);
}

/** Parse a PWCL partial body into query-shaped oracle data. */
export function parsePartialQueryFromWire(
  _wasm: WcolStreamWasm,
  columns: PartialOracleColumn[],
  partial: Uint8Array,
): ParquetPartialQuery {
  return parsePwclPartial(columns, partial);
}

export async function parsePartialQueryFromWireAuto(
  columns: PartialOracleColumn[],
  partial: Uint8Array,
): Promise<ParquetPartialQuery> {
  const wasm = await loadStreamWasm();
  return parsePartialQueryFromWire(wasm, columns, partial);
}

export function partialQueryResultsEquivalent(
  left: ParquetPartialQuery,
  right: ParquetPartialQuery,
): boolean {
  return sameAggregates(left.query.aggregates, right.query.aggregates)
    && sameGroups(left.query.groups, right.query.groups);
}

/** Compare two wire partial bodies via Rust decode (numeric and string group keys). */
export function partialsWireEquivalent(
  wasm: WcolStreamWasm,
  left: Uint8Array,
  right: Uint8Array,
): boolean {
  const eq = wasm.wcol_partials_wire_equivalent;
  if (!eq) return false;
  const leftPtr = wasm.wcs_alloc(left.byteLength);
  const rightPtr = wasm.wcs_alloc(right.byteLength);
  if (!leftPtr || !rightPtr) {
    if (leftPtr) wasm.wcs_dealloc(leftPtr, left.byteLength);
    if (rightPtr) wasm.wcs_dealloc(rightPtr, right.byteLength);
    throw new Error("wcs_alloc failed");
  }
  try {
    new Uint8Array(wasm.memory.buffer, leftPtr, left.byteLength).set(left);
    new Uint8Array(wasm.memory.buffer, rightPtr, right.byteLength).set(right);
    return eq(leftPtr, left.byteLength, rightPtr, right.byteLength) === 1;
  } finally {
    wasm.wcs_dealloc(leftPtr, left.byteLength);
    wasm.wcs_dealloc(rightPtr, right.byteLength);
  }
}

/** Compare two PWCL partial bodies. */
export async function partialsEquivalent(
  columns: PartialOracleColumn[],
  left: Uint8Array,
  right: Uint8Array,
  wasm?: WcolStreamWasm,
): Promise<boolean> {
  wasm ??= await loadStreamWasm();
  if (wasm.wcol_partials_wire_equivalent) {
    return partialsWireEquivalent(wasm, left, right);
  }
  const a = parsePartialQueryFromWire(wasm, columns, left);
  const b = parsePartialQueryFromWire(wasm, columns, right);
  return partialQueryResultsEquivalent(a, b);
}

export function assertPartialWireFormat(partial: Uint8Array): void {
  const format = partialWireFormat(partial);
  if (format === "unknown") {
    throw new Error("partial wire format is not PWCL");
  }
}

const KEY_COL_BASE = 10_000;
const STAT_COL_BASE = 20_000;
const STAT_FIELDS = 11;
const AGG_NAMES = ["count_star()", "count", "sum", "avg", "min", "max"];

type I64Column = { id: number; values: BigInt64Array; nulls: Uint8Array };

function parsePwclPartial(columns: PartialOracleColumn[], partial: Uint8Array): ParquetPartialQuery {
  if (partial.byteLength < 6 || String.fromCharCode(...partial.subarray(0, 4)) !== "PWCL") {
    throw new Error("expected PWCL partial bytes");
  }
  const data = new DataView(partial.buffer, partial.byteOffset, partial.byteLength);
  const version = partial[4]!;
  if (version < 1 || version > 4) throw new Error("unsupported PWCL version");
  const flags = partial[5]!;
  let at = 6;
  const limit = data.getUint32(at, true);
  at += 4;
  const offset = data.getUint32(at, true);
  at += 4;
  const groupKeyLen = partial[at]!;
  at += 1;
  const floatMask = version >= 2 ? data.getUint32(at, true) : 0;
  if (version >= 2) at += 4;
  const stringMask = version >= 4 ? data.getUint32(at, true) : 0;
  if (version >= 4) at += 4;
  const aggCount = data.getUint32(at, true);
  at += 4;
  const aggs: Array<{ kind: number; column: number }> = [];
  for (let index = 0; index < aggCount; index += 1) {
    const kind = partial[at];
    if (kind === undefined) throw new Error("PWCL partial is truncated");
    aggs.push({ kind, column: data.getUint32(at + 1, true) });
    at += 5;
  }
  at += 8;
  const rowCount = data.getUint32(at, true);
  at += 4;
  const projLen = data.getUint32(at, true);
  at += 4;
  const projection = decodeLegacyI64Projection(partial.subarray(at, at + projLen), rowCount);
  at += projLen;
  const byId = new Map(projection.map((column) => [column.id, column]));
  let sketches: Array<{ agg: number; bytes: Uint8Array[] }> = [];
  if (version >= 3) {
    const sketchCount = data.getUint32(at, true);
    at += 4;
    sketches = [];
    for (let index = 0; index < sketchCount; index += 1) {
      const agg = data.getUint32(at, true);
      at += 4;
      const offsets: number[] = [];
      for (let row = 0; row < rowCount + 1; row += 1) {
        offsets.push(data.getUint32(at, true));
        at += 4;
      }
      const byteLen = data.getUint32(at, true);
      at += 4;
      const bytes = partial.subarray(at, at + byteLen);
      at += byteLen;
      const rows: Uint8Array[] = [];
      for (let row = 0; row < rowCount; row += 1) {
        rows.push(bytes.subarray(offsets[row]!, offsets[row + 1]!));
      }
      sketches.push({ agg, bytes: rows });
    }
  }
  const stringKeys = new Map<number, { bytes: Uint8Array; offsets: number[]; nulls: Uint8Array }>();
  if (version >= 4) {
    const keyCount = data.getUint32(at, true);
    at += 4;
    for (let index = 0; index < keyCount; index += 1) {
      const keyIndex = partial[at]!;
      at += 1;
      const offsets: number[] = [];
      for (let row = 0; row < rowCount + 1; row += 1) {
        offsets.push(data.getUint32(at, true));
        at += 4;
      }
      const byteLen = data.getUint32(at, true);
      at += 4;
      const bytes = partial.subarray(at, at + byteLen);
      at += byteLen;
      const nullLen = data.getUint32(at, true);
      at += 4;
      const nulls = partial.subarray(at, at + nullLen);
      at += nullLen;
      stringKeys.set(keyIndex, { bytes, offsets, nulls });
    }
  }
  if (at !== partial.byteLength) throw new Error("PWCL partial has trailing bytes");

  const groups: Array<{ keys: number[]; stats: AggregateStats[] }> = [];
  for (let row = 0; row < rowCount; row += 1) {
    const keys: number[] = [];
    for (let key = 0; key < groupKeyLen; key += 1) {
      const column = byId.get(KEY_COL_BASE + key);
      if (!column || column.nulls[row] === 0) {
        keys.push(Number.NaN);
        continue;
      }
      if ((stringMask & (1 << key)) !== 0) {
        keys.push(Number.NaN);
        continue;
      }
      const value = column.values[row]!;
      keys.push((floatMask & (1 << key)) !== 0 ? floatFromBits(value) : Number(value));
    }
    const stats = aggs.map((agg, index) => statAt(byId, index, row, agg.kind));
    void sketches;
    groups.push({ keys, stats });
  }
  return queryFromGroups(columns, aggs, groups, flags, limit, offset);
}

function statAt(byId: Map<number, I64Column>, aggIndex: number, row: number, kind: number): AggregateStats {
  const fields: bigint[] = [];
  for (let field = 0; field < STAT_FIELDS; field += 1) {
    const column = byId.get(STAT_COL_BASE + aggIndex * STAT_FIELDS + field);
    fields.push(column?.values[row] ?? 0n);
  }
  const count = Number(fields[0]);
  const flags = Number(fields[1]);
  const integer = (flags & 1) !== 0;
  const integerExtreme = (flags & 4) !== 0;
  const sum = integer ? Number(loHi(fields[2]!, fields[3]!)) : floatFromBits(fields[8]!);
  const min = integerExtreme ? Number(loHi(fields[4]!, fields[5]!)) : floatFromBits(fields[9]!);
  const max = integerExtreme ? Number(loHi(fields[6]!, fields[7]!)) : floatFromBits(fields[10]!);
  const mean = count === 0 ? Number.NaN : kind === 0 || kind === 1 ? count : sum / count;
  return kind === 0 || kind === 1
    ? { count, sum, min: count, max: count, mean }
    : { count, sum, min, max, mean };
}

function queryFromGroups(
  columns: PartialOracleColumn[],
  aggs: Array<{ kind: number; column: number }>,
  groups: Array<{ keys: number[]; stats: AggregateStats[] }>,
  flags: number,
  limit: number,
  offset: number,
): ParquetPartialQuery {
  const grouped = (flags & 1) !== 0;
  const sums: Record<string, number> = {};
  const aggregates: QueryResult["aggregates"] = {};
  if (!grouped) {
    const stats = groups[0]?.stats ?? [];
    for (let index = 0; index < aggs.length; index += 1) {
      const agg = aggs[index]!;
      const stat = stats[index];
      if (!stat) continue;
      const name = aggName(agg.kind, agg.column === 0xffffffff ? undefined : columns[agg.column]?.name);
      aggregates[name] = stat;
      if (agg.kind === 2) sums[name] = stat.sum;
    }
    return { query: { rows: [], projection: null, aggregates, groups: null }, rows: stats[0]?.count ?? 0, sums };
  }
  const ordered = (flags & 2) !== 0
    ? groups.slice().sort((left, right) => (right.stats[0]?.count ?? 0) - (left.stats[0]?.count ?? 0))
    : groups;
  const sliced = ordered.slice(offset, limit === 0xffffffff ? undefined : offset + limit);
  return {
    query: {
      rows: [],
      projection: null,
      aggregates,
      groups: {
        keys: sliced.map((group) => group.keys[0] ?? Number.NaN),
        keys2: sliced.some((group) => group.keys.length > 1) ? sliced.map((group) => group.keys[1] ?? Number.NaN) : undefined,
        aggs: aggs.map((agg) => ({ colId: agg.column, kind: agg.kind })),
        values: sliced.map((group) => group.stats),
      },
    },
    rows: sliced.reduce((sum, group) => sum + (group.stats[0]?.count ?? 0), 0),
    sums,
  };
}

function decodeLegacyI64Projection(body: Uint8Array, rows: number): I64Column[] {
  if (body.byteLength < 16) throw new Error("truncated PWCL projection");
  const magic = String.fromCharCode(...body.subarray(0, 8));
  if (magic !== "WCOLpjv1") throw new Error("invalid projection magic");
  const data = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const declaredRows = data.getUint32(8, true);
  if (declaredRows !== rows) throw new Error("PWCL projection row count mismatch");
  const colCount = data.getUint32(12, true);
  const columns: I64Column[] = [];
  for (let index = 0; index < colCount; index += 1) {
    const cursor = 16 + index * 16;
    const id = data.getUint32(cursor, true);
    const kind = body[cursor + 4];
    const valueWidth = body[cursor + 5] ?? 0;
    const nullEncoding = body[cursor + 6] ?? 0;
    if (kind !== 3 || valueWidth !== 0 || nullEncoding !== 0) {
      throw new Error("PWCL projection column is not legacy i64");
    }
    const section = data.getUint32(cursor + 8, true);
    const values = new BigInt64Array(rows);
    for (let row = 0; row < rows; row += 1) values[row] = data.getBigInt64(section + row * 8, true);
    const nulls = body.subarray(section + rows * 8, section + rows * 8 + rows).slice();
    columns.push({ id, values, nulls });
  }
  return columns;
}

function loHi(lo: bigint, hi: bigint): bigint {
  return (hi << 64n) | (lo & 0xffffffffffffffffn);
}

function floatFromBits(bits: bigint): number {
  const buf = new ArrayBuffer(8);
  const view = new DataView(buf);
  view.setBigUint64(0, bits & 0xffffffffffffffffn, true);
  return view.getFloat64(0, true);
}

function aggName(kind: number, column: string | undefined): string {
  if (kind === 0) return "count_star()";
  const fn = AGG_NAMES[kind] ?? "agg";
  return `${fn}(${column ?? "?"})`;
}

function sameAggregates(
  left: Record<string, { count: number; sum: number; mean: number }> | undefined,
  right: Record<string, { count: number; sum: number; mean: number }> | undefined,
): boolean {
  const a = left ?? {};
  const b = right ?? {};
  const keys = Object.keys(a);
  if (keys.length !== Object.keys(b).length) return false;
  for (const key of keys) {
    const other = b[key];
    const stat = a[key];
    if (!other || !stat) return false;
    if (stat.count !== other.count || stat.sum !== other.sum) return false;
    if (Number.isNaN(stat.mean) !== Number.isNaN(other.mean)) return false;
    if (!Number.isNaN(stat.mean) && Math.abs(stat.mean - other.mean) > 1e-6) return false;
  }
  return true;
}

function sameGroups(
  left: QueryResult["groups"],
  right: QueryResult["groups"],
): boolean {
  const keep = (group: { key: number | null; count: number }) => group.count !== 0;
  const toMap = (groups: QueryResult["groups"]) =>
    new Map(
      groupsOf(groups).filter(keep).map((group) => [String(group.key), group.count]),
    );
  const a = toMap(left);
  const b = toMap(right);
  if (a.size !== b.size) return false;
  for (const [key, count] of a) if (b.get(key) !== count) return false;
  return true;
}

function groupsOf(groups: QueryResult["groups"]) {
  if (!groups) return [];
  return groups.keys.map((key, index) => ({
    key,
    count: groups.values[index]?.[0]?.count ?? 0,
  }));
}
