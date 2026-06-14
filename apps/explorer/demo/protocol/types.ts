/**
 * Protocol types generated from Rust (`demo/generated/engine-types.ts`).
 */

export type {
  AppEvent,
  DatasetKind,
  DatasetMeta,
  EffectCommand,
  FilterDraft,
  LoadPhase,
  PatchSegment,
  PresetOption,
  QueryDraft,
  QueryMode,
  QueryPhase,
  QuerySummary,
  SchemaColumn,
  ViewModel,
  ViewModelPatch,
  WorkerInput,
  WorkerOutput,
} from "../generated/engine-types.ts";

export type { WireMessage } from "@dtonge/engine-shell";

import type { PatchSegment } from "../generated/engine-types.ts";

/** Path segment for generic view-model patches. */
export type Path = PatchSegment[];

/** Worker-only openFile side channel (File cannot cross CBOR). */
export type OpenFileWireMessage = {
  type: "openFile";
  file: File;
};
