/** Generic view-model patch ops (stable contract with `engine-kernel`). */

export type PatchSegment = number | string;

export type Path = PatchSegment[];

export type ViewModelPatch =
  | { op: "replace"; path: Path; value: unknown }
  | { op: "remove"; path: Path }
  | { op: "insert"; path: Path; value: unknown };

/** Worker `postMessage` wire — CBOR bytes only; semantics live in the payload. */
export type WireMessage = {
  bytes: ArrayBuffer;
};

export type EngineUpdate<TViewModel, TPatch extends ViewModelPatch, TEffect> = {
  viewModel?: TViewModel;
  patches: TPatch[];
  effects: TEffect[];
  diagnostics: string[];
};
