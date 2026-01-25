import { createContext } from "preact";
import { useContext } from "preact/hooks";
import type { ComponentChildren } from "preact";
import type { ViewModelStore } from "@dtonge/engine-shell";
import type { ViewModel } from "../protocol/types.ts";
import type { Event } from "./events.ts";

const StoreContext = createContext<ViewModelStore<ViewModel> | null>(null);
const OnEventContext = createContext<((event: Event) => void) | null>(null);

type AppProviderProps = {
  store: ViewModelStore<ViewModel>;
  onEvent: (event: Event) => void;
  children: ComponentChildren;
};

export function AppProvider({ store, onEvent, children }: AppProviderProps) {
  return (
    <StoreContext.Provider value={store}>
      <OnEventContext.Provider value={onEvent}>{children}</OnEventContext.Provider>
    </StoreContext.Provider>
  );
}

export function useViewModelStore(): ViewModelStore<ViewModel> {
  const store = useContext(StoreContext);
  if (!store) throw new Error("useViewModelStore requires AppProvider");
  return store;
}

/** Wiring-layer hook — presentational components receive `onEvent` via props. */
export function useOnEvent(): (event: Event) => void {
  const onEvent = useContext(OnEventContext);
  if (!onEvent) throw new Error("useOnEvent requires AppProvider");
  return onEvent;
}
