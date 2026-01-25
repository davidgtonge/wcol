import init, { WcolEngine } from "../../pkg/wcol_engine.js";
import {
  decodeWorkerOutput,
  encodeWorkerInput,
} from "@dtonge/engine-shell";
import type {
  AppEvent,
  EffectCommand,
  ViewModel,
  ViewModelPatch,
  WorkerInput,
  WorkerOutput,
} from "../protocol/types.ts";
import { runEffect, type WorkerEffect } from "./effects.handlers.ts";
import { normalizeWireValue } from "./normalize.ts";

let engineReady: Promise<WcolEngine> | null = null;

function getEngine(): Promise<WcolEngine> {
  if (!engineReady) {
    engineReady = init().then(() => new WcolEngine());
  }
  return engineReady;
}

function encodeInput(input: WorkerInput): Uint8Array {
  if (input.kind === "event") {
    return encodeWorkerInput({
      kind: "event",
      event: normalizeWireValue(input.event),
    });
  }
  return encodeWorkerInput(input);
}

function decodeStep(bytes: Uint8Array): WorkerOutput {
  const ab = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  return decodeWorkerOutput<WorkerOutput>(ab as ArrayBuffer);
}

export type PipelineResult = {
  patches: ViewModelPatch[];
  viewModel: ViewModel;
};

async function runPipeline(seed: AppEvent[]): Promise<PipelineResult> {
  const engine = await getEngine();
  const queue = [...seed];
  const patches: ViewModelPatch[] = [];
  let viewModel: ViewModel | null = null;

  while (queue.length > 0) {
    const event = queue.shift()!;
    const outBytes = engine.handle_input(encodeInput({ kind: "event", event }));
    const output = decodeStep(outBytes);

    if (output.kind === "error") {
      throw new Error(output.message);
    }
    if (output.kind !== "response") {
      throw new Error("expected engine response");
    }

    patches.push(...output.patches);
    viewModel = output.viewModel;

    for (const effect of output.effects ?? []) {
      const { events } = await runEffect(effect as WorkerEffect);
      queue.push(...events);
    }
  }

  if (!viewModel) {
    throw new Error("engine produced no view model");
  }

  return { patches, viewModel };
}

export async function initEngine(): Promise<ViewModel> {
  const engine = await getEngine();
  const outBytes = engine.init(encodeInput({ kind: "init" }));
  const output = decodeStep(outBytes);
  if (output.kind === "error") throw new Error(output.message);
  if (output.kind !== "initialized") throw new Error("expected initialized");
  return output.viewModel;
}

export async function runEventPipeline(events: AppEvent[]): Promise<PipelineResult> {
  return runPipeline(events);
}

export async function openFile(file: File): Promise<PipelineResult> {
  const { events } = await runEffect({
    type: "OPEN_SOURCE",
    source: file,
    label: file.name,
  });
  return runPipeline(events);
}

export async function drainRustEffects(effects: EffectCommand[]): Promise<PipelineResult | null> {
  if (effects.length === 0) return null;
  const events: AppEvent[] = [];
  for (const effect of effects) {
    const result = await runEffect(effect as WorkerEffect);
    events.push(...result.events);
  }
  return runPipeline(events);
}
