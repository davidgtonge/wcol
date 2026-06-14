import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectExploreSidebarInput } from "../arch/selectors.ts";
import { ExploreSidebar } from "../components/ExploreSidebar.tsx";

export function WiredExploreSidebar() {
  traceRender("WiredExploreSidebar");
  const input = useSelector(selectExploreSidebarInput);
  const onEvent = useOnEvent();
  return <ExploreSidebar input={input} onEvent={onEvent} />;
}
