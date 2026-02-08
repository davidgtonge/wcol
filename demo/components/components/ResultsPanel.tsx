import { useState } from "preact/hooks";
import { ResultViewPanel, type ResultViewInput } from "../charts/ResultViewPanel.tsx";
import type { QueryBuilderEvent, ResultInteractionEvent, WorkspaceEvent } from "../arch/events.ts";
import type { QueryDraft, ResultView } from "../arch/types.ts";
import { RankingTable } from "./RankingTable.tsx";
import { QuerySummaryBar } from "./QuerySummaryBar.tsx";
import { TimingDisplay, type TimingInput } from "./TimingDisplay.tsx";
import { panel } from "../ui/classes.ts";

export type ResultsPanelInput = {
  draft: QueryDraft;
  datasetKind: "crates" | "dependencies" | "categories" | "maintainers" | "trends" | "hits" | null;
  resultLabel: string | null;
  result: ResultViewInput | null;
  view: ResultView | null;
  timing: TimingInput | null;
  loading: boolean;
  cratesInteractive?: boolean;
  selectedCrate: string | null;
};

type Props = {
  input: ResultsPanelInput;
  onEvent: (event: ResultInteractionEvent | QueryBuilderEvent | WorkspaceEvent) => void;
};

function isBarRanking(view: ResultView | null): view is Extract<ResultView, { kind: "bar-h" | "bar-v" }> {
  return view?.kind === "bar-h" || view?.kind === "bar-v";
}

export function ResultsPanel({ input, onEvent }: Props) {
  const [selectedRow, setSelectedRow] = useState<{ name: string; rank: number; value: number } | null>(null);

  const handleResultEvent = (event: ResultInteractionEvent) => {
    if (event.type === "CRATE_SELECT") {
      const barItems = isBarRanking(input.view) ? input.view.items : [];
      const idx = barItems.findIndex((i) => i.label === event.name);
      if (idx >= 0) {
        setSelectedRow({ name: event.name, rank: idx + 1, value: barItems[idx].value });
      }
      onEvent(event);
      return;
    }
    if (event.type === "CRATE_PIN") {
      onEvent(event);
      return;
    }
    if (event.type === "CRATE_COMPARE") {
      onEvent({ type: "CRATE_PIN", name: event.name });
      onEvent({ type: "ROUTE_SET", route: "compare" });
    }
  };

  const showTiming = input.loading || input.timing;
  const barView = isBarRanking(input.view) ? input.view : null;

  return (
    <section class={`${panel} min-h-[24rem]`} id="result-panel">
      <QuerySummaryBar
        input={{
          draft: input.draft,
          datasetKind: input.datasetKind,
          resultLabel: input.resultLabel,
          selectedCrate: selectedRow?.name ?? input.selectedCrate,
          selectedRank: selectedRow?.rank ?? null,
          selectedValue: selectedRow?.value ?? null,
          resultGroupCount: barView?.items.length ?? null,
        }}
        onEvent={onEvent}
      />

      {input.result ? (
        <div id="result-out" class="result-enter">
          <ResultViewPanel
            input={input.result}
            showStats={false}
            cratesInteractive={input.cratesInteractive}
            onEvent={handleResultEvent}
          />
          {barView && input.cratesInteractive ? (
            <RankingTable
              items={barView.items}
              valueLabel={barView.valueLabel ?? "Downloads"}
              cratesInteractive
              selectedCrate={input.selectedCrate}
              onEvent={handleResultEvent}
            />
          ) : null}
        </div>
      ) : input.loading ? (
        <div class="space-y-3" aria-hidden="true">
          <div class="skeleton-bar h-40 w-full rounded-lg" />
          <div class="skeleton-bar h-4 w-2/3 rounded" />
        </div>
      ) : null}

      {showTiming ? (
        <div class="mt-4 border-t border-slate-200/60 pt-3 dark:border-wcol-border/60">
          <TimingDisplay
            compact
            input={
              input.loading && !input.timing
                ? { timingMs: 0, rowsScanned: 0, workers: 0, loading: true }
                : { ...input.timing!, loading: input.loading }
            }
          />
        </div>
      ) : null}
    </section>
  );
}
