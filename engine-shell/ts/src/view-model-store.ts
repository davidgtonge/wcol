import { applyPatches } from "./patch";
import type { ViewModelPatch } from "./types";

type Listener = () => void;

export function createViewModelStore<TViewModel>(initial: TViewModel) {
  let snapshot = initial;
  const listeners = new Set<Listener>();

  return {
    getSnapshot(): TViewModel {
      return snapshot;
    },
    subscribe(listener: Listener): () => void {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    replace(next: TViewModel): void {
      snapshot = next;
      listeners.forEach((l) => l());
    },
    applyPatchBatch(patches: ViewModelPatch[]): void {
      if (patches.length === 0) return;
      snapshot = applyPatches(snapshot, patches);
      listeners.forEach((l) => l());
    },
  };
}

export type ViewModelStore<TViewModel> = ReturnType<typeof createViewModelStore<TViewModel>>;
