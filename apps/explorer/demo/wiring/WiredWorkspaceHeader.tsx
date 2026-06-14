import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectDemoChromeInput, selectWorkspaceChromeInput } from "../arch/selectors.ts";
import { Hero } from "../components/Hero.tsx";
import { WorkspaceHeader } from "../components/WorkspaceHeader.tsx";

export function WiredWorkspaceHeader() {
  traceRender("WiredWorkspaceHeader");
  const demo = useSelector(selectDemoChromeInput);
  const chrome = useSelector(selectWorkspaceChromeInput);
  const onEvent = useOnEvent();

  if (!demo.loaded) {
    return <Hero />;
  }

  return (
    <WorkspaceHeader
      input={{
        route: chrome.route,
        canUndo: chrome.canUndo,
        canRedo: chrome.canRedo,
        savedViews: chrome.savedViews,
        shareableUrl: chrome.shareableUrl,
        pinnedCrates: chrome.pinnedCrates,
        rowCount: demo.meta ? Number(demo.meta.rows) : null,
        columnCount: demo.meta?.columns ?? null,
      }}
      onEvent={onEvent}
    />
  );
}
