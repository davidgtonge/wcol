import { readOutBytes } from "../wasm/helpers.ts";
import { PROJ_KIND_BOOL, PROJ_KIND_DICT_ID, PROJ_KIND_F64 } from "../core/types.ts";
import type { WasmBindings } from "../wasm/wasm.ts";
import type {
  ProjectionColumn,
  ProjectionColumnMeta,
  RowProjection,
  U64
} from "../core/types.ts";
import type { ColumnNameResolver } from "../core/types.ts";

const PROJ_MAGIC = new TextEncoder().encode("WCOLpjv1");

function magicMatches(bytes: Uint8Array): boolean {
  if (bytes.byteLength < 8) return false;
  for (let i = 0; i < 8; i += 1) {
    if (bytes[i] !== PROJ_MAGIC[i]) return false;
  }
  return true;
}

export async function readRowProjection(
  file: ColumnNameResolver,
  wasm: WasmBindings,
  plan: number
): Promise<RowProjection | null> {
  const bytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.plan_copy_row_projection(plan, outPtr, outLen),
    64
  );
  if (!bytes.byteLength || !magicMatches(bytes)) {
    return null;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const rowCount = view.getUint32(8, true);
  const colCount = view.getUint32(12, true);
  const columns: ProjectionColumnMeta[] = [];
  const data: ProjectionColumn[] = [];
  let cursor = 16;
  for (let c = 0; c < colCount; c += 1) {
    const colId = view.getUint32(cursor, true);
    cursor += 4;
    const kind = view.getUint8(cursor);
    cursor += 4;
    const byteOffset = view.getUint32(cursor, true);
    cursor += 4;
    const byteLen = view.getUint32(cursor, true);
    cursor += 4;
    const name = await file.getColumnName(colId);
    columns.push({ name, colId, kind });
    const section = bytes.subarray(byteOffset, byteOffset + byteLen);
    if (kind === PROJ_KIND_F64) {
      const valuesLen = rowCount * 8;
      const valueBytes = section.slice(0, valuesLen);
      const values = new Float64Array(valueBytes.buffer, valueBytes.byteOffset, rowCount);
      const nulls = section.subarray(valuesLen, valuesLen + rowCount);
      data.push({ kind: PROJ_KIND_F64, values, nulls });
    } else if (kind === PROJ_KIND_DICT_ID) {
      const valuesLen = rowCount * 4;
      const valueBytes = section.slice(0, valuesLen);
      const values = new Uint32Array(valueBytes.buffer, valueBytes.byteOffset, rowCount);
      const nulls = section.subarray(valuesLen, valuesLen + rowCount);
      data.push({ kind: PROJ_KIND_DICT_ID, values, nulls });
    } else {
      const values = section.subarray(0, rowCount);
      const nulls = section.subarray(rowCount, rowCount * 2);
      data.push({ kind: PROJ_KIND_BOOL, values, nulls });
    }
  }
  return { columns, data };
}

export function groupRowIdsByChunk(
  rows: U64[],
  rowsPerChunk: number
): Map<number, { local: number[]; dst: number[] }> {
  const rpc = BigInt(rowsPerChunk);
  const grouped = new Map<number, { local: number[]; dst: number[] }>();
  for (let dst = 0; dst < rows.length; dst += 1) {
    const id = BigInt(rows[dst] ?? 0);
    const chunkId = Number(id / rpc);
    const local = Number(id % rpc);
    let entry = grouped.get(chunkId);
    if (!entry) {
      entry = { local: [], dst: [] };
      grouped.set(chunkId, entry);
    }
    entry.local.push(local);
    entry.dst.push(dst);
  }
  return grouped;
}

/** Resolve dict-id column cells to strings (demo / display). */
export function projectionCellToString(
  col: ProjectionColumn,
  rowIndex: number,
  dictValue: (valueId: number) => string | undefined
): string {
  if (col.nulls[rowIndex] === 0) {
    return "";
  }
  if (col.kind === PROJ_KIND_F64) {
    return String(col.values[rowIndex]);
  }
  if (col.kind === PROJ_KIND_DICT_ID) {
    const id = col.values[rowIndex]!;
    return dictValue(id) ?? String(id);
  }
  return col.values[rowIndex] ? "1" : "0";
}
