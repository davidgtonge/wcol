/**
 * Spill sorted-run wire payloads: the part a run put carries and the range a run read asks for.
 */

/** One part of a sorted-run upload (`RunPutPlan` in `spill/r2/codec.rs`). */
export type RunPutPart = {
  objectKey: string;
  /** 1-based multipart part number. */
  part: number;
  last: boolean;
  /** Bytes in this part. */
  byteLength: number;
  /** Records in the whole run. */
  recordCount: number;
};

const RUN_PUT_TRAILER = 4 + 1 + 8 + 8;

export function decodeRunPutOpen(payload: Uint8Array): RunPutPart {
  if (payload.byteLength < 4 + RUN_PUT_TRAILER) throw new Error("run put open payload truncated");
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const keyLen = view.getUint32(0, true);
  const end = 4 + keyLen;
  if (end + RUN_PUT_TRAILER > payload.byteLength) throw new Error("run put key truncated");
  const objectKey = new TextDecoder().decode(payload.subarray(4, end));
  return {
    objectKey,
    part: view.getUint32(end, true),
    last: view.getUint8(end + 4) !== 0,
    byteLength: Number(view.getBigUint64(end + 5, true)),
    recordCount: Number(view.getBigUint64(end + 13, true)),
  };
}

export function decodeRunReadOpen(payload: Uint8Array): {
  objectKey: string;
  byteLength: number;
  offset: number;
  length: number;
} {
  if (payload.byteLength < 20) throw new Error("run read open payload truncated");
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const keyLen = view.getUint32(0, true);
  const end = 4 + keyLen;
  if (end + 20 > payload.byteLength) throw new Error("run read key truncated");
  const objectKey = new TextDecoder().decode(payload.subarray(4, end));
  const byteLength = Number(view.getBigUint64(end, true));
  const offset = Number(view.getBigUint64(end + 8, true));
  const length = view.getUint32(end + 16, true);
  return { objectKey, byteLength, offset, length };
}
