import type { QueryBuilderEvent } from "../arch/events.ts";
import type { QueryDraft } from "../arch/types.ts";
import { describeQuery, headlineFromResultLabel, type QueryNarrative } from "../query/query-narrative.ts";
import { btn, hint } from "../ui/classes.ts";

export type QuerySummaryBarInput = {
  draft: QueryDraft;
  datasetKind: "crates" | "dependencies" | "categories" | "maintainers" | "trends" | "hits" | null;
  resultLabel?: string | null;
  selectedCrate?: string | null;
  selectedRank?: number | null;
  selectedValue?: number | null;
  resultGroupCount?: number | null;
};

type Props = {
  input: QuerySummaryBarInput;
  onEvent?: (event: QueryBuilderEvent) => void;
};

function Chip({
  chip,
  onClick,
}: {
  chip: QueryNarrative["chips"][number];
  onClick?: () => void;
}) {
  return (
    <span class="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs dark:border-wcol-border dark:bg-[#0f1419]">
      <span class="text-slate-500">{chip.label}</span>
      {onClick ? (
        <button type="button" class="font-medium text-blue-600 hover:underline dark:text-blue-400" onClick={onClick}>
          {chip.value}
        </button>
      ) : (
        <span class="font-medium text-slate-800 dark:text-slate-200">{chip.value}</span>
      )}
    </span>
  );
}

export function QuerySummaryBar({ input, onEvent }: Props) {
  const narrative = describeQuery(input.draft, input.datasetKind);
  const headline = headlineFromResultLabel(input.resultLabel ?? null, input.draft);

  const focusSidebar = (section?: string) => {
    const el = document.getElementById("explore-sidebar");
    el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    if (section) el?.querySelector(`[data-section="${section}"]`)?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  };

  const chipClick = (id: string) => {
    if (!onEvent) return focusSidebar(id === "filter" ? "facets" : undefined);
    if (id === "limit") focusSidebar();
    else if (id === "filter") focusSidebar("facets");
    else focusSidebar();
  };

  const rowExplain =
    input.selectedCrate && input.selectedRank != null && input.selectedValue != null
      ? `${input.selectedCrate} is ranked #${input.selectedRank} with ${input.selectedValue.toLocaleString()} in this result set.`
      : null;

  return (
    <div class="mb-4 space-y-2 rounded-lg border border-slate-200/80 bg-slate-50/50 p-3 dark:border-wcol-border dark:bg-[#0f1419]/40">
      <div>
        <h2 class="text-base font-semibold leading-snug tracking-tight">{headline}</h2>
        <p class={`${hint} mt-0.5`}>{narrative.subline}</p>
      </div>
      <div class="flex flex-wrap items-center gap-1.5">
        {narrative.chips.map((chip) => (
          <Chip key={chip.id} chip={chip} onClick={chip.editable ? () => chipClick(chip.id) : undefined} />
        ))}
      </div>
      <p class="text-[11px] leading-relaxed text-slate-500 dark:text-slate-400">{narrative.explanation}</p>
      {rowExplain ? (
        <p class="rounded-md border border-blue-500/20 bg-blue-500/5 px-2 py-1.5 text-xs text-blue-800 dark:text-blue-200">
          {rowExplain}
          {onEvent && input.selectedCrate ? (
            <button
              type="button"
              class={`${btn} ml-2 border-0 bg-transparent px-1 py-0 text-xs text-blue-600 shadow-none dark:text-blue-400`}
              onClick={() => {
                onEvent({
                  type: "FILTER_ADD_PREFILLED",
                  column: "crate_name",
                  op: "=",
                  value: input.selectedCrate!,
                });
                onEvent({ type: "QUERY_DRAFT_PATCH", patch: { mode: "table" } });
                onEvent({ type: "RUN_QUERY" });
              }}
            >
              Show underlying rows
            </button>
          ) : null}
        </p>
      ) : null}
    </div>
  );
}
