import type { ParquetColumn } from "../ts-query-engine/parquet-job.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";
import { loadStreamWasm } from "../../src/runtime/host/wasm.ts";

/** Rust `encode_catalog` layout for `wcol_columns_for_sql`. */
export function encodeStreamCatalog(columns: ParquetColumn[]): Uint8Array {
  const parts: Uint8Array[] = [];
  const push = (bytes: Uint8Array) => parts.push(bytes);
  const u8 = (value: number) => push(Uint8Array.of(value));
  const u16 = (value: number) => {
    const out = new Uint8Array(2);
    new DataView(out.buffer).setUint16(0, value, true);
    push(out);
  };
  const u32 = (value: number) => {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setUint32(0, value, true);
    push(out);
  };
  const i32 = (value: number) => {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setInt32(0, value, true);
    push(out);
  };
  u32(columns.length);
  for (const column of columns) {
    u8(column.kind === "text" ? 1 : 0);
    i32(column.physical);
    i32(column.typeLength);
    u8(column.maxDef);
    u8(column.maxRep);
    const name = new TextEncoder().encode(column.name);
    u16(name.byteLength);
    push(name);
  }
  const length = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(length);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.byteLength;
  }
  return out;
}

/** Column indexes the stream ingest+query plan must decode (Rust `columns_for_sql`). */
export async function scanColumnsForStreamSql(
  sql: string,
  columns: ParquetColumn[],
): Promise<number[]> {
  const wasm = await loadStreamWasm();
  return scanColumnsForStreamSqlWasm(wasm, sql, columns);
}

export function scanColumnsForStreamSqlWasm(
  wasm: WcolStreamWasm,
  sql: string,
  columns: ParquetColumn[],
): number[] {
  const plan = wasm.wcol_columns_for_sql;
  if (!plan) {
    throw new Error("wasm missing wcol_columns_for_sql");
  }
  const catalog = encodeStreamCatalog(columns);
  const sqlBytes = new TextEncoder().encode(sql);
  const outCap = 4 + columns.length * 4;
  const catalogPtr = wasm.wcs_alloc(catalog.byteLength);
  const sqlPtr = wasm.wcs_alloc(sqlBytes.byteLength);
  const outPtr = wasm.wcs_alloc(outCap);
  if (!catalogPtr || !sqlPtr || !outPtr) {
    if (catalogPtr) wasm.wcs_dealloc(catalogPtr, catalog.byteLength);
    if (sqlPtr) wasm.wcs_dealloc(sqlPtr, sqlBytes.byteLength);
    if (outPtr) wasm.wcs_dealloc(outPtr, outCap);
    throw new Error("wcs_alloc failed");
  }
  try {
    new Uint8Array(wasm.memory.buffer, catalogPtr, catalog.byteLength).set(catalog);
    new Uint8Array(wasm.memory.buffer, sqlPtr, sqlBytes.byteLength).set(sqlBytes);
    const written = plan(
      catalogPtr,
      catalog.byteLength,
      sqlPtr,
      sqlBytes.byteLength,
      outPtr,
      outCap,
    );
    if (written < 4) {
      throw new Error("columns_for_sql failed (see wcs_last_error)");
    }
    const view = new DataView(wasm.memory.buffer, outPtr, written);
    const count = view.getUint32(0, true);
    const ids: number[] = [];
    for (let i = 0; i < count; i += 1) {
      ids.push(view.getUint32(4 + i * 4, true));
    }
    return ids;
  } finally {
    wasm.wcs_dealloc(catalogPtr, catalog.byteLength);
    wasm.wcs_dealloc(sqlPtr, sqlBytes.byteLength);
    wasm.wcs_dealloc(outPtr, outCap);
  }
}
