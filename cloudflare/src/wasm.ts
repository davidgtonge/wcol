/**
 * Worker Wasm registry. Each entry installs its own module so the node bundle
 * does not contain the coordinator planner, and the other way around.
 */

import { streamWasmExports, type WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";
import { streamWasmImports } from "../../src/runtime/host/wasm-imports.ts";

type WasmLoader = () => Promise<WcolStreamWasm>;

let nodeLoader: WasmLoader | null = null;
let coordinatorLoader: WasmLoader | null = null;

/** Called from the node Worker entry before any request. */
export function installNodeWasm(loader: WasmLoader): void {
  nodeLoader = loader;
}

/** Called from the coordinator Worker entry before any request. */
export function installCoordinatorWasm(loader: WasmLoader): void {
  coordinatorLoader = loader;
}

/**
 * A new node Wasm instance. Module scope is shared by every Durable Object the runtime puts
 * in one isolate, so the instance is cached per object (`NodeSessionLease`), never here:
 * two objects sharing one instance would share its linear memory and engine globals.
 */
export function createNodeWasm(): Promise<WcolStreamWasm> {
  if (!nodeLoader) throw new Error("node wasm is not installed in this worker");
  return nodeLoader();
}

export function loadCoordinatorWasm(): Promise<WcolStreamWasm> {
  if (!coordinatorLoader) throw new Error("coordinator wasm is not installed in this worker");
  return coordinatorLoader();
}

/** Instantiate one bundled module and check the exports that role is allowed to have. */
export async function instantiateWorkerWasm(
  wasmModule: WebAssembly.Module,
  role: "node" | "coordinator",
): Promise<WcolStreamWasm> {
  const bind: { memory?: WebAssembly.Memory } = {};
  const exports = streamWasmExports(await WebAssembly.instantiate(wasmModule, streamWasmImports(bind)));
  bind.memory = exports.memory;
  if (!exports.wcs_sp_open_graph) {
    throw new Error(`${role} wasm is missing wcs_sp_open_graph`);
  }
  if (role === "node" && exports.wcol_plan_query) {
    throw new Error("node wasm must not export the planner");
  }
  if (role === "coordinator" && (!exports.wcol_plan_query || !exports.wcol_reduce_open)) {
    throw new Error("coordinator wasm is missing plan or reduce exports");
  }
  return exports;
}
