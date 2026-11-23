import nodeModule from "./wcol_node.wasm";
import { installNodeWasm, instantiateWorkerWasm } from "../wasm.ts";
import type { GraphTaskEnv } from "./node.ts";

installNodeWasm(() => instantiateWorkerWasm(nodeModule, "node"));

export { WcolNode } from "./node.ts";
export { WcolReducer } from "./reducer-container.ts";

export default {
  async fetch(_request: Request, _env: GraphTaskEnv): Promise<Response> {
    return new Response("not found", { status: 404 });
  },
};
