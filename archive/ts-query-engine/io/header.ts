import type { HeaderInfo, U64 } from "../core/types.ts";

/** Parse `runtime_header_info` output (wcol v7, 92 bytes). */
export function parseHeaderInfo(bytes: Uint8Array): HeaderInfo {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    version: view.getUint32(0, true),
    flags: view.getUint32(4, true),
    ncols: view.getUint32(8, true),
    nchunks: view.getUint32(12, true),
    rowsPerChunk: view.getUint32(16, true),
    totalRows: readU64(view, 20),
    schemaOff: readU64(view, 28),
    schemaLen: readU64(view, 36),
    indexOff: readU64(view, 44),
    indexLen: readU64(view, 52),
    dictOff: readU64(view, 60),
    dictLen: readU64(view, 68),
    dataOff: readU64(view, 76),
    dictRawLen: readU64(view, 84)
  };
}

export function readU64(view: DataView, offset: number): U64 {
  if (typeof view.getBigUint64 === "function") {
    const value = view.getBigUint64(offset, true);
    if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(value);
    }
    return value;
  }
  const low = view.getUint32(offset, true);
  const high = view.getUint32(offset + 4, true);
  const value = high * 2 ** 32 + low;
  if (value > Number.MAX_SAFE_INTEGER) {
    return (BigInt(high) << 32n) | BigInt(low);
  }
  return value;
}
