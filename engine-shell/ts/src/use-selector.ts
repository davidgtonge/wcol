import { useEffect, useRef, useState } from "preact/hooks";
import type { ViewModelStore } from "./view-model-store";

export function useSelector<TViewModel, T>(
  store: ViewModelStore<TViewModel>,
  selector: (vm: TViewModel) => T,
): T {
  const selectorRef = useRef(selector);
  selectorRef.current = selector;
  const [value, setValue] = useState(() => selector(store.getSnapshot()));

  useEffect(() => {
    return store.subscribe(() => {
      setValue(selectorRef.current(store.getSnapshot()));
    });
  }, [store]);

  return value;
}
