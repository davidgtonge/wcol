import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectIsLoaded, selectLoadInput } from "../arch/selectors.ts";
import { LoadPanel } from "../components/LoadPanel.tsx";
import { WiredExploreWorkspace } from "./WiredExploreWorkspace.tsx";
import { WiredPlaceholderRoute } from "./WiredPlaceholderRoute.tsx";

export function WiredMainContent() {
  traceRender("WiredMainContent");
  const loaded = useSelector(selectIsLoaded);
  const load = useSelector(selectLoadInput);
  const onEvent = useOnEvent();

  if (!loaded) {
    return <LoadPanel input={load} onEvent={onEvent} />;
  }

  return (
    <div class="animate-fade-in">
      <WiredExploreWorkspace />
      <WiredPlaceholderRoute />
    </div>
  );
}
