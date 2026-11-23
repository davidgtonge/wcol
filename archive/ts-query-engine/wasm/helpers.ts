import { readFromWasm, freeWasm, toBytes } from "./wasm.ts";
import type { ByteInput, WasmBindings } from "./wasm.ts";

export function callStatus(code: number): void {
  if (code < 0) {
    if (code === -1100) {
      throw new Error("Aggregates require numeric columns");
    }
    if (code === -2) {
      throw new Error(
        "Unsupported .wcol file (expected version 7, 65504 rows/chunk). Re-encode with current wcol-cli."
      );
    }
    throw new Error(`WASM call failed (${code})`);
  }
}

export function withBytes<T>(
  wasm: WasmBindings,
  bytes: ByteInput,
  fn: (ptr: number, len: number) => T
): T {
  const view = toBytes(bytes);
  const ptr = wasm.alloc(view.byteLength);
  wasm.memoryU8().set(view, ptr);
  try {
    return fn(ptr, view.byteLength);
  } finally {
    freeWasm(wasm, ptr, view.byteLength);
  }
}

export function withArray<T>(
  wasm: WasmBindings,
  array: ArrayBufferView & { length: number },
  fn: (ptr: number, len: number) => T
): T {
  const view = toBytes(array);
  const ptr = wasm.alloc(view.byteLength);
  wasm.memoryU8().set(view, ptr);
  try {
    return fn(ptr, array.length);
  } finally {
    freeWasm(wasm, ptr, view.byteLength);
  }
}

export function withTwoBuffers<T>(
  wasm: WasmBindings,
  first: ArrayBufferView & { length: number },
  second: ByteInput,
  fn: (firstPtr: number, firstLen: number, secondPtr: number, secondLen: number) => T
): T {
  const firstView = toBytes(first);
  const secondView = toBytes(second);
  const firstPtr = wasm.alloc(firstView.byteLength);
  const secondPtr = wasm.alloc(secondView.byteLength);
  wasm.memoryU8().set(firstView, firstPtr);
  wasm.memoryU8().set(secondView, secondPtr);
  try {
    return fn(firstPtr, first.length, secondPtr, secondView.byteLength);
  } finally {
    freeWasm(wasm, firstPtr, firstView.byteLength);
    freeWasm(wasm, secondPtr, secondView.byteLength);
  }
}

export async function readOutBytes(
  wasm: WasmBindings,
  call: (outPtr: number, outLen: number) => number,
  initialLen: number
): Promise<Uint8Array> {
  let outLen = initialLen;
  for (;;) {
    const outPtr = wasm.alloc(outLen);
    const written = call(outPtr, outLen);
    if (written < 0) {
      freeWasm(wasm, outPtr, outLen);
      const needed = -written;
      if (needed <= outLen) {
        throw new Error(`WASM call failed (${written})`);
      }
      outLen = needed;
      continue;
    }
    const bytes = readFromWasm(wasm, outPtr, written);
    freeWasm(wasm, outPtr, outLen);
    return bytes;
  }
}
