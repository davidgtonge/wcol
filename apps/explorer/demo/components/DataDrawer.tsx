import type { DataDrawerEvent } from "../arch/events.ts";
import { btn, btnGhost, h2, panel } from "../ui/classes.ts";
import { LoadPanel, type LoadPanelInput } from "./LoadPanel.tsx";
import { DatasetPanel, type DatasetPanelInput } from "./DatasetPanel.tsx";
import type { DatasetMeta } from "../arch/types.ts";

export type DataDrawerInput = {
  open: boolean;
  load: LoadPanelInput;
  dataset: DatasetPanelInput | null;
  meta: DatasetMeta | null;
  workers: number;
  warmStatus: string;
};

type Props = {
  input: DataDrawerInput;
  onEvent: (event: DataDrawerEvent) => void;
};

export function DataDrawer({ input, onEvent }: Props) {
  if (!input.open) return null;

  return (
    <div
      class="fixed inset-0 z-50 flex justify-end bg-slate-900/50 backdrop-blur-sm animate-fade-in"
      role="presentation"
      onClick={() => onEvent({ type: "DATA_DRAWER_SET", open: false })}
    >
      <aside
        class="flex h-full w-full max-w-md flex-col border-l border-slate-200 bg-white shadow-2xl dark:border-wcol-border dark:bg-wcol-surface animate-slide-up"
        role="dialog"
        aria-label="Data and schema"
        onClick={(e) => e.stopPropagation()}
      >
        <header class="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-wcol-border">
          <h2 class={h2}>Data</h2>
          <button type="button" class={btnGhost} onClick={() => onEvent({ type: "DATA_DRAWER_SET", open: false })}>
            Close
          </button>
        </header>

        <div class="flex-1 overflow-y-auto p-4">
          <LoadPanel input={{ ...input.load, compact: true }} onEvent={onEvent} />

          {input.meta ? (
            <div class={`${panel} mt-4 border-0 bg-slate-50 p-0 dark:bg-transparent`}>
              <dl class="mb-4 grid grid-cols-2 gap-2 text-sm">
                <div>
                  <dt class="text-xs text-slate-500">Rows</dt>
                  <dd class="font-medium tabular-nums">
                    {typeof input.meta.rows === "bigint"
                      ? input.meta.rows.toLocaleString()
                      : Number(input.meta.rows).toLocaleString()}
                  </dd>
                </div>
                <div>
                  <dt class="text-xs text-slate-500">Columns</dt>
                  <dd class="font-medium">{input.meta.columns}</dd>
                </div>
                <div class="col-span-2">
                  <dt class="text-xs text-slate-500">Source</dt>
                  <dd class="truncate text-xs" title={input.meta.label}>
                    {input.meta.label}
                  </dd>
                </div>
              </dl>

              <label class="mb-3 flex items-center gap-2 text-sm">
                Workers
                <input
                  type="number"
                  class="w-14 rounded-lg border border-slate-200 px-2 py-1 text-center dark:border-wcol-border dark:bg-[#0f1419]"
                  min={1}
                  max={16}
                  value={input.workers}
                  onInput={(e) =>
                    onEvent({
                      type: "WORKERS_CHANGED",
                      workers: Number((e.currentTarget as HTMLInputElement).value) || 1,
                    })
                  }
                />
                <button type="button" class={btn} onClick={() => onEvent({ type: "WARM_WORKERS" })}>
                  Warm pool
                </button>
              </label>
              {input.warmStatus ? <p class="text-xs text-slate-500">{input.warmStatus}</p> : null}
            </div>
          ) : null}

          {input.dataset ? <DatasetPanel input={input.dataset} onEvent={() => {}} /> : null}
        </div>
      </aside>
    </div>
  );
}
