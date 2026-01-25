/// <reference lib="webworker" />
import { decodeWorkerOutput, encodeWorkerInput } from "@dtonge/engine-shell";
import type { WireMessage } from "@dtonge/engine-shell";
import type { WorkerInput, WorkerOutput } from "../protocol/types.ts";
import { initEngine, openFile, runEventPipeline } from "./wcol-effect-loop.ts";

function postOutput(output: WorkerOutput) {
  const encoded = encodeWorkerInput(output);
  const bytes = encoded.buffer.slice(
    encoded.byteOffset,
    encoded.byteOffset + encoded.byteLength,
  ) as ArrayBuffer;
  const msg: WireMessage = { bytes };
  self.postMessage(msg, [bytes]);
}

function postError(err: unknown) {
  const message = err instanceof Error ? err.message : String(err);
  postOutput({ kind: "error", message });
}

self.onmessage = async (ev: MessageEvent) => {
  const data = ev.data;
  try {
    if (data?.type === "openFile" && data.file instanceof File) {
      const { patches, viewModel } = await openFile(data.file);
      postOutput({
        kind: "response",
        patches,
        effects: [],
        viewModel,
        diagnostics: [],
      });
      return;
    }

    if (!(data?.bytes instanceof ArrayBuffer)) {
      throw new Error("Expected wire bytes or openFile");
    }

    const input = decodeWorkerOutput<WorkerInput>(data.bytes);

    if (input.kind === "init") {
      const viewModel = await initEngine();
      postOutput({ kind: "initialized", viewModel, effects: [] });
      return;
    }

    if (input.kind === "event") {
      const { patches, viewModel } = await runEventPipeline([input.event]);
      postOutput({
        kind: "response",
        patches,
        effects: [],
        viewModel,
        diagnostics: [],
      });
      return;
    }

    throw new Error(`Unknown worker input: ${(input as { kind: string }).kind}`);
  } catch (err) {
    postError(err);
  }
};
