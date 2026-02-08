import { renderView } from "./views.tsx";
import type { ResultInteractionEvent } from "../arch/events.ts";
import type { ResultView } from "../arch/types.ts";
import { formatCount } from "./format.ts";
import { row, statPill } from "../ui/classes.ts";

export type ResultViewInput = {
  view: ResultView;
  timingMs: number;
  workers: number;
  rowsScanned: number;
  resultCount: number;
};

type Props = {
  input: ResultViewInput;
  showStats?: boolean;
  cratesInteractive?: boolean;
  onEvent?: (event: ResultInteractionEvent) => void;
};

export function ResultViewPanel({
  input: { view, timingMs, workers, resultCount },
  showStats = true,
  cratesInteractive = false,
  onEvent,
}: Props) {
  return (
    <div>
      {showStats ? (
        <div class={`${row} mb-4`}>
          <span class={statPill}>{formatCount(resultCount)} rows</span>
          <span class={statPill}>{timingMs.toFixed(1)} ms</span>
          <span class={statPill}>
            {workers} worker{workers === 1 ? "" : "s"}
          </span>
        </div>
      ) : null}
      {renderView(view, { cratesInteractive, onEvent })}
    </div>
  );
}
