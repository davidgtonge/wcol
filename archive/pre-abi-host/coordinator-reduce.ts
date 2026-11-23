import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";
import { readWasmLastError } from "../../src/runtime/host/wasm.ts";

/**
 * Coordinator merge of worker PWCL partials with Rust `ReduceOp`. Partials are pushed one at a
 * time, so Wasm holds only the merged state plus the partial being pushed.
 */
export class CoordinatorReducer {
  private handle: number;

  constructor(private readonly wasm: WcolStreamWasm) {
    if (!wasm.wcol_reduce_open) throw new Error("wasm missing wcol_reduce_open");
    this.handle = wasm.wcol_reduce_open();
    if (!this.handle) throw new Error("wcol_reduce_open failed");
  }

  push(partial: Uint8Array): void {
    const wasm = this.wasm;
    if (!partial.byteLength) throw new Error("coordinator reduce: empty partial");
    const ptr = wasm.wcs_alloc(partial.byteLength);
    if (!ptr) throw new Error("wcs_alloc failed");
    try {
      new Uint8Array(wasm.memory.buffer, ptr, partial.byteLength).set(partial);
      if (!wasm.wcol_reduce_push!(this.handle, ptr, partial.byteLength)) {
        throw new Error(readWasmLastError(wasm, "coordinator reduce failed"));
      }
    } finally {
      wasm.wcs_dealloc(ptr, partial.byteLength);
    }
  }

  /** Merged PWCL. Call once, then [`close`]. */
  finish(): Uint8Array {
    const wasm = this.wasm;
    const n = wasm.wcol_reduce_finish!(this.handle);
    if (!n) throw new Error(readWasmLastError(wasm, "coordinator reduce failed"));
    const ptr = wasm.wcol_reduce_output_ptr!(this.handle);
    return new Uint8Array(wasm.memory.buffer, ptr, n).slice();
  }

  close(): void {
    if (this.handle) {
      this.wasm.wcol_reduce_close!(this.handle);
      this.handle = 0;
    }
  }
}

/** Merge partials already in memory (in-process coordinator, tests). */
export function coordinatorReducePartials(
  wasm: WcolStreamWasm,
  workerPartials: Uint8Array[],
): Uint8Array {
  if (workerPartials.length === 0) {
    throw new Error("coordinator reduce: no worker partials");
  }
  const reducer = new CoordinatorReducer(wasm);
  try {
    for (const partial of workerPartials) reducer.push(partial);
    return reducer.finish();
  } finally {
    reducer.close();
  }
}
