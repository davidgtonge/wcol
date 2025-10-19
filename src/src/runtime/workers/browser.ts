import { createWorkerCore } from "./core.ts";
import { resultTransferables } from "./protocol.ts";

type WorkerScope = {
  postMessage: (msg: unknown, transfer?: ArrayBuffer[]) => void;
  onmessage: ((event: MessageEvent<unknown>) => void) | null;
};

const scope = self as unknown as WorkerScope;

const core = createWorkerCore({
  postMessage: (msg) => {
    if (msg.type === "result") {
      scope.postMessage(msg, resultTransferables(msg.result));
      return;
    }
    scope.postMessage(msg);
  }
});

scope.onmessage = (event: MessageEvent<unknown>) => {
  void core.onMessage(event.data);
};
