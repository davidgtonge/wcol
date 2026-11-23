import { parentPort } from "node:worker_threads";
import { createWorkerCore } from "./core.ts";
import { resultTransferables } from "./protocol.ts";

const core = createWorkerCore({
  postMessage: (msg) => {
    if (msg.type === "result") {
      parentPort?.postMessage(msg, resultTransferables(msg.result));
      return;
    }
    parentPort?.postMessage(msg);
  }
});

parentPort?.on("message", (msg: unknown) => {
  void core.onMessage(msg);
});
