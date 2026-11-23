import { installNodeWasm } from "../../cloudflare/src/wasm.ts";
import { loadNodeWasmModule } from "../../cloudflare/src/node-wasm.ts";

installNodeWasm(loadNodeWasmModule);

export { WcolNode } from "./wcol-node-bench.ts";
