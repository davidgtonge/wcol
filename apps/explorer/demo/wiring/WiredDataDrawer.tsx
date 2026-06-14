import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectDataDrawerInput } from "../arch/selectors.ts";
import { DataDrawer } from "../components/DataDrawer.tsx";

export function WiredDataDrawer() {
  traceRender("WiredDataDrawer");
  const input = useSelector(selectDataDrawerInput);
  const onEvent = useOnEvent();
  return <DataDrawer input={input} onEvent={onEvent} />;
}
