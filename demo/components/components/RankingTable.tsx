import type { ChartItem } from "../arch/types.ts";
import type { ResultInteractionEvent } from "../arch/events.ts";
import { formatCount } from "../charts/format.ts";
import { btnGhost, table, tableWrap, td, th } from "../ui/classes.ts";

type Props = {
  items: ChartItem[];
  valueLabel?: string;
  cratesInteractive?: boolean;
  selectedCrate?: string | null;
  onEvent?: (event: ResultInteractionEvent) => void;
};

export function RankingTable({
  items,
  valueLabel = "Downloads",
  cratesInteractive,
  selectedCrate,
  onEvent,
}: Props) {
  if (!items.length) return null;

  return (
    <div class="mt-4">
      <h3 class="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Ranking table</h3>
      <div class={tableWrap}>
        <table class={table}>
          <thead>
            <tr>
              <th class={th}>#</th>
              <th class={th}>Crate</th>
              <th class={`${th} text-right`}>{valueLabel}</th>
              {cratesInteractive ? <th class={th}>Actions</th> : null}
            </tr>
          </thead>
          <tbody>
            {items.map((item, i) => {
              const active = selectedCrate === item.label;
              return (
                <tr
                  key={item.label}
                  class={active ? "bg-blue-500/5" : ""}
                >
                  <td class={`${td} tabular-nums text-slate-500`}>{i + 1}</td>
                  <td class={td}>
                    {cratesInteractive && onEvent ? (
                      <button
                        type="button"
                        class="font-medium text-blue-600 hover:underline dark:text-blue-400"
                        onClick={() => onEvent({ type: "CRATE_SELECT", name: item.label })}
                      >
                        {item.label}
                      </button>
                    ) : (
                      item.label
                    )}
                  </td>
                  <td class={`${td} text-right tabular-nums`}>{formatCount(item.value)}</td>
                  {cratesInteractive && onEvent ? (
                    <td class={td}>
                      <div class="flex gap-1">
                        <button
                          type="button"
                          class={`${btnGhost} px-1.5 py-0.5 text-[10px]`}
                          onClick={() => onEvent({ type: "CRATE_SELECT", name: item.label })}
                        >
                          View
                        </button>
                        <button
                          type="button"
                          class={`${btnGhost} px-1.5 py-0.5 text-[10px]`}
                          onClick={() => onEvent({ type: "CRATE_PIN", name: item.label })}
                        >
                          Pin
                        </button>
                        <button
                          type="button"
                          class={`${btnGhost} px-1.5 py-0.5 text-[10px]`}
                          onClick={() => onEvent({ type: "CRATE_COMPARE", name: item.label })}
                        >
                          + Compare
                        </button>
                      </div>
                    </td>
                  ) : null}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
