import { installCoordinatorWasm } from "../../cloudflare/src/wasm.ts";
import { loadCoordinatorWasmModule } from "../../cloudflare/src/coordinator-wasm.ts";

installCoordinatorWasm(loadCoordinatorWasmModule);

export { WcolCoordinator } from "../../cloudflare/src/worker.ts";
export { default } from "./worker.bench.ts";
