import coordinatorModule from "./wcol_coordinator.wasm";
import { installCoordinatorWasm, instantiateWorkerWasm } from "../wasm.ts";

let coordinatorWasm: Promise<Awaited<ReturnType<typeof instantiateWorkerWasm>>> | null = null;
installCoordinatorWasm(() => {
  coordinatorWasm ??= instantiateWorkerWasm(coordinatorModule, "coordinator");
  return coordinatorWasm;
});

export { WcolCoordinator } from "../worker.ts";
export { default } from "../worker.ts";
