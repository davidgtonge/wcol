import { EMPTY_U8, textDecoder } from "../core/constants.ts";
import { readOutBytes } from "../wasm/helpers.ts";
import { readFromWasm, freeWasm, toBytes } from "../wasm/wasm.ts";
import type { WasmBindings } from "../wasm/wasm.ts";
import type { DictLookup, DictsMap } from "../core/types.ts";

export function decompressDictBytes(
  wasm: WasmBindings,
  bytes: Uint8Array,
  rawLen: number
): Uint8Array {
  if (rawLen === 0) {
    return EMPTY_U8;
  }
  const input = toBytes(bytes);
  const inputPtr = wasm.alloc(input.byteLength);
  const outputPtr = wasm.alloc(rawLen);
  let output: Uint8Array | null = null;
  try {
    wasm.memoryU8().set(input, inputPtr);
    const code = wasm.exports.lz4_decompress(
      inputPtr,
      input.byteLength,
      rawLen,
      outputPtr,
      rawLen
    );
    if (code < 0) {
      throw new Error(`lz4 decompression failed (${code})`);
    }
    output = readFromWasm(wasm, outputPtr, rawLen);
  } finally {
    freeWasm(wasm, inputPtr, input.byteLength);
    freeWasm(wasm, outputPtr, rawLen);
  }
  return output ?? EMPTY_U8;
}

export async function loadRuntimeDict(
  wasm: WasmBindings,
  runtime: number,
  colId: number
): Promise<DictLookup> {
  const { offsets, blob } = await loadRuntimeDictBlob(wasm, runtime, colId);
  if (offsets.length === 0) {
    return new Map();
  }
  const lookup: DictLookup = new Map();
  for (let idx = 0; idx < offsets.length - 1; idx += 1) {
    const start = offsets[idx];
    const end = offsets[idx + 1];
    const value = textDecoder.decode(blob.subarray(start, end));
    lookup.set(value, idx);
  }
  return lookup;
}

export async function loadRuntimeDictBlob(
  wasm: WasmBindings,
  runtime: number,
  colId: number
): Promise<{ offsets: Uint32Array; blob: Uint8Array }> {
  const infoBytes = await readOutBytes(
    wasm,
    (outPtr, outLen) => wasm.exports.runtime_dict_blob_info(runtime, colId, outPtr, outLen),
    16
  );
  const view = new DataView(infoBytes.buffer, infoBytes.byteOffset, infoBytes.byteLength);
  const offsetsPtr = view.getUint32(0, true);
  const offsetsLen = view.getUint32(4, true);
  const blobPtr = view.getUint32(8, true);
  const blobLen = view.getUint32(12, true);
  const memory = wasm.memoryU8();
  const offsets = new Uint32Array(memory.buffer, offsetsPtr, offsetsLen);
  const blob = memory.subarray(blobPtr, blobPtr + blobLen);
  return { offsets, blob };
}

export function decodeDictValue(
  offsets: Uint32Array,
  blob: Uint8Array,
  valueId: number
): string | undefined {
  if (valueId < 0 || valueId + 1 >= offsets.length) {
    return undefined;
  }
  const start = offsets[valueId];
  const end = offsets[valueId + 1];
  return textDecoder.decode(blob.subarray(start, end));
}

/** Resolve one runtime dictionary entry (works for offset blobs and in-memory value tables). */
export async function lookupRuntimeDictValue(
  wasm: WasmBindings,
  runtime: number,
  colId: number,
  valueId: number
): Promise<string | undefined> {
  let outLen = 64;
  for (;;) {
    const outPtr = wasm.alloc(outLen);
    const written = wasm.exports.runtime_dict_value(runtime, colId, valueId, outPtr, outLen);
    if (written < 0) {
      freeWasm(wasm, outPtr, outLen);
      const needed = -written;
      if (needed > outLen) {
        outLen = needed;
        continue;
      }
      return undefined;
    }
    const bytes = readFromWasm(wasm, outPtr, written);
    freeWasm(wasm, outPtr, outLen);
    return textDecoder.decode(bytes);
  }
}
