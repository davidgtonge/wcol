import type { ComponentChild } from "preact";
import type { ResultInteractionEvent } from "../arch/events.ts";
import type { ResultView } from "../arch/types.ts";
import { applyKind, type KindHandlerMap } from "../arch/typed.ts";
import { chartCaption, chartSub, row, rowChip } from "../ui/classes.ts";
import { BarChart } from "./BarChart.tsx";
import { GroupedBarChart } from "./GroupedBarChart.tsx";
import { DataTable } from "./DataTable.tsx";

type ViewOpts = {
  cratesInteractive?: boolean;
  onEvent?: (event: ResultInteractionEvent) => void;
};

function buildHandlers(opts: ViewOpts): KindHandlerMap<ResultView, ComponentChild> {
  const barView = (orientation: "h" | "v") => (view: Extract<ResultView, { kind: "bar-h" | "bar-v" }>) => (
    <BarChart
      input={{ orientation, ...view }}
      onEvent={
        opts.cratesInteractive && opts.onEvent
          ? (e) => opts.onEvent!({ type: "CRATE_SELECT", name: view.items[e.index]?.label ?? "" })
          : undefined
      }
    />
  );

  return {
    "bar-h": barView("h"),
    "bar-v": barView("v"),
    "grouped-bar": (view) => <GroupedBarChart input={view} />,
    table: (view) => (
      <DataTable
        input={view}
        onCrateSelect={
          opts.cratesInteractive && opts.onEvent
            ? (name) => opts.onEvent!({ type: "CRATE_SELECT", name })
            : undefined
        }
      />
    ),
    rows: (view) => (
      <figure>
        <figcaption class={chartCaption}>
          <strong class="text-sm">{view.title}</strong>
          <span class={chartSub}>{view.rowCount.toLocaleString()} matching rows</span>
        </figcaption>
        <ul class={`${row} list-none p-0`}>
          {view.rowIds.map((id) => (
            <li key={id} class={rowChip}>
              {id}
            </li>
          ))}
        </ul>
      </figure>
    ),
  };
}

export const renderView = (view: ResultView, opts: ViewOpts = {}) => applyKind(buildHandlers(opts), view);
