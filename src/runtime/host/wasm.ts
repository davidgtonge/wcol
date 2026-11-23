import { streamWasmExports, type WcolStreamWasm } from "./wasm-stream.ts";
import { streamWasmImports } from "./wasm-imports.ts";

export type { PhaseMark } from "./wasm-imports.ts";
export { setPhaseSink, streamWasmImports } from "./wasm-imports.ts";

let wasmPromise: Promise<WcolStreamWasm> | null = null;


function nodeSpecifier(name: string): string {
  return ["node", name].join(":");
}

/** Node host: read the release artifact. Durable Objects call [`setStreamWasm`] instead. */
export async function loadStreamWasm(): Promise<WcolStreamWasm> {
  if (!wasmPromise) {
    const fs = (await import(nodeSpecifier("fs"))) as {
      existsSync(path: string): boolean;
      readFileSync(path: string): Uint8Array;
    };
    const path = (await import(nodeSpecifier("path"))) as {
      join(...parts: string[]): string;
      resolve(...parts: string[]): string;
      dirname(path: string): string;
    };
    const url = (await import(nodeSpecifier("url"))) as {
      fileURLToPath(url: string): string;
    };
    const root = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "../../..");
    const candidates = [
      path.join(root, "rust/target/wasm32-unknown-unknown/release/wcol.wasm"),
      path.join(root, "rust/target/wasm32-unknown-unknown/release/wcol_stream_operators.wasm"),
    ];
    const found = candidates.find((candidate) => fs.existsSync(candidate));
    if (!found) {
      throw new Error(
        "stream wasm not found; build with: cargo build -p wcol --target wasm32-unknown-unknown --release",
      );
    }
    wasmPromise = instantiateWithTrace(fs.readFileSync(found));
  }
  return wasmPromise;
}

let nodeWasmPromise: Promise<WcolStreamWasm> | null = null;
let coordinatorWasmPromise: Promise<WcolStreamWasm> | null = null;
let nodeWasmModuleBytes: Uint8Array | null = null;
let coordinatorWasmModuleBytes: Uint8Array | null = null;
let nodeWasmInstanceSeq = 0;
const wasmInstanceIds = new WeakMap<WcolStreamWasm, number>();

/** Stable id assigned at instantiate (`instantiateNodeWasm` / `loadNodeWasm`). */
export function wasmInstanceId(wasm: WcolStreamWasm): number {
  return wasmInstanceIds.get(wasm) ?? 0;
}

function tagWasmInstance(wasm: WcolStreamWasm): WcolStreamWasm {
  if (!wasmInstanceIds.has(wasm)) {
    nodeWasmInstanceSeq += 1;
    wasmInstanceIds.set(wasm, nodeWasmInstanceSeq);
  }
  return wasm;
}

async function readReleaseWasmBytes(fileName: string): Promise<Uint8Array> {
  const fs = (await import(nodeSpecifier("fs"))) as {
    existsSync(path: string): boolean;
    readFileSync(path: string): Uint8Array;
  };
  const path = (await import(nodeSpecifier("path"))) as {
    join(...parts: string[]): string;
    resolve(...parts: string[]): string;
    dirname(path: string): string;
  };
  const url = (await import(nodeSpecifier("url"))) as {
    fileURLToPath(url: string): string;
  };
  const root = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), "../../..");
  const override = fileName === "wcol_node.wasm" ? process.env.WCOL_NODE_WASM_FILE : undefined;
  if (override) {
    if (!fs.existsSync(override)) throw new Error(`missing WCOL_NODE_WASM_FILE ${override}`);
    return fs.readFileSync(override);
  }
  const found = path.join(root, "rust/target/wasm32-unknown-unknown/release", fileName);
  if (!fs.existsSync(found)) {
    throw new Error(`missing ${found}`);
  }
  return fs.readFileSync(found);
}

async function instantiateRelease(fileName: string): Promise<WcolStreamWasm> {
  const bytes =
    fileName === "wcol_node.wasm"
      ? (nodeWasmModuleBytes ??= await readReleaseWasmBytes(fileName))
      : (coordinatorWasmModuleBytes ??= await readReleaseWasmBytes(fileName));
  return instantiateWithTrace(bytes).then(tagWasmInstance);
}

/** Node fragment Wasm (`npm run build:node-wasm`). */
export function loadNodeWasm(): Promise<WcolStreamWasm> {
  nodeWasmPromise ??= instantiateRelease("wcol_node.wasm");
  return nodeWasmPromise;
}

/**
 * Fresh node Wasm instance (one isolate). Spill boundary counters and linear memory
 * are per-module; hash jobs must not share a singleton across concurrent reducers.
 */
export function instantiateNodeWasm(): Promise<WcolStreamWasm> {
  return instantiateRelease("wcol_node.wasm");
}

/** Coordinator Wasm, default build without DataFusion (`npm run build:coordinator-wasm`). */
export function loadCoordinatorWasm(): Promise<WcolStreamWasm> {
  coordinatorWasmPromise ??= instantiateRelease("wcol_coordinator.wasm");
  return coordinatorWasmPromise;
}

export function setStreamWasm(wasm: Uint8Array | WebAssembly.Module): void {
  wasmPromise = instantiateWithTrace(wasm);
}

/** Wasm32 linear-memory offsets from FFI are unsigned; JS number can look negative above 2GiB. */
export function wasmMemoryOffset(ptr: number): number {
  return ptr >>> 0;
}

export function wasmMemoryU8(wasm: WcolStreamWasm, ptr: number, byteLength: number): Uint8Array {
  if (byteLength <= 0) return new Uint8Array();
  const offset = wasmMemoryOffset(ptr);
  const buffer = wasm.memory.buffer;
  if (offset + byteLength > buffer.byteLength) {
    throw new Error(
      `wasm memory view out of bounds: offset=${offset} len=${byteLength} mem=${buffer.byteLength}`,
    );
  }
  return new Uint8Array(buffer, offset, byteLength);
}

export function decodeWasmUtf8(wasm: WcolStreamWasm, ptr: number, len: number): string {
  if (!ptr || !len) return "";
  return new TextDecoder().decode(wasmMemoryU8(wasm, ptr, len));
}

/** Memory is filled in after instantiate. The trace import reads it synchronously. */
async function instantiateWithTrace(source: Uint8Array | WebAssembly.Module): Promise<WcolStreamWasm> {
  const bind: { memory?: WebAssembly.Memory } = {};
  const imports = streamWasmImports(bind);
  const instance =
    source instanceof WebAssembly.Module
      ? await WebAssembly.instantiate(source, imports)
      : (await WebAssembly.instantiate(source as Uint8Array<ArrayBuffer>, imports)).instance;
  const exports = streamWasmExports(instance);
  bind.memory = exports.memory;
  return exports;
}

/** Last error from a stateless Wasm FFI call (reduce, partial helpers, stream-pool open). */
export function readWasmLastError(wasm: WcolStreamWasm, fallback: string): string {
  if (wasm.wcs_last_error_ptr && wasm.wcs_last_error_len) {
    const ptr = wasm.wcs_last_error_ptr();
    const n = wasm.wcs_last_error_len();
    const message = decodeWasmUtf8(wasm, ptr, n);
    if (message) return message;
  }
  return fallback;
}
