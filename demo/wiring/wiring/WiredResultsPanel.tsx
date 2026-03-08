import { useSelector } from "../arch/use-selector.ts";
import { useOnEvent } from "../arch/app-context.tsx";
import { traceRender } from "../arch/debug-renders.ts";
import { selectResultsPanelInput } from "../arch/selectors.ts";
import { ResultsPanel } from "../components/ResultsPanel.tsx";

export function WiredResultsPanel() {
  traceRender("WiredResultsPanel");
  const input = useSelector(selectResultsPanelInput);
  const onEvent = useOnEvent();
  return <ResultsPanel input={input} onEvent={onEvent} />;
}
