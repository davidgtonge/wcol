import { useSelector as useEngineSelector } from "@dtonge/engine-shell";
import type { ViewModel } from "../protocol/types.ts";
import { useViewModelStore } from "./app-context.tsx";

/**
 * Subscribe to a slice of the view model. Re-renders when the selected value changes.
 * Pair with reselect `createSelector` for object slices.
 */
export function useSelector<T>(selector: (vm: ViewModel) => T): T {
  const store = useViewModelStore();
  return useEngineSelector(store, selector);
}

export const useViewModel = () => useSelector((vm) => vm);
