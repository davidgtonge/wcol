import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectCrateDetailInput, selectExploreRoute } from "../arch/selectors.ts";
import { CrateDetailPanel } from "../components/CrateDetailPanel.tsx";
import { WiredExploreSidebar } from "./WiredExploreSidebar.tsx";
import { WiredResultsPanel } from "./WiredResultsPanel.tsx";

export function WiredExploreWorkspace() {
  traceRender("WiredExploreWorkspace");
  const route = useSelector(selectExploreRoute);
  const crateDetail = useSelector(selectCrateDetailInput);
  const onEvent = useOnEvent();

  if (route !== "explore") return null;

  return (
    <div class="grid gap-5 lg:grid-cols-[minmax(17rem,20rem)_minmax(0,1fr)_minmax(13rem,18rem)] lg:items-start">
      <WiredExploreSidebar />
      <WiredResultsPanel />
      <CrateDetailPanel input={crateDetail} onEvent={onEvent} />
    </div>
  );
}
