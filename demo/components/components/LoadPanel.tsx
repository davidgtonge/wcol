import type { LoadPanelEvent } from "../arch/events.ts";
import type { DatasetId } from "../data/datasets.ts";
import { DEMO_DATASETS } from "../data/datasets.ts";
import {
  btn,
  btnPrimary,
  hint,
  h2,
  inputUrl,
  panel,
  panelInset,
  row,
  status,
  statusError,
} from "../ui/classes.ts";

export type LoadPanelInput = {
  status: string;
  isError: boolean;
  loading: boolean;
  url: string;
  compact?: boolean;
};

type Props = {
  input: LoadPanelInput;
  onEvent: (event: LoadPanelEvent) => void;
};

export function LoadPanel({ input, onEvent }: Props) {
  const loadDataset = (id: DatasetId) => onEvent({ type: "LOAD_DATASET", id });

  return (
    <section class={panel} id="load-panel">
      {!input.compact ? (
        <h2 class={`${h2} mb-4`}>
          Get started
        </h2>
      ) : (
        <h2 class={`${h2} mb-3`}>Data source</h2>
      )}

      <div class={`${panelInset} mb-4`}>
        <p class="mb-3 text-sm text-slate-600 dark:text-slate-300">
          Pick a bundled dataset — each is a single <code class="font-mono text-xs">.wcol</code> table
          you can filter, rank, and browse in the browser.
        </p>
        <ul class="space-y-2">
          {DEMO_DATASETS.map((ds) => (
            <li
              key={ds.id}
              class="flex flex-col gap-2 rounded-lg border border-slate-200/80 bg-white/60 p-3 sm:flex-row sm:items-center sm:justify-between dark:border-wcol-border dark:bg-[#0f1419]/40"
            >
              <div class="min-w-0">
                <p class="font-medium text-slate-800 dark:text-slate-100">{ds.title}</p>
                <p class="text-sm text-slate-500 dark:text-slate-400">{ds.description}</p>
                <p class="mt-1 text-xs text-slate-400">
                  {ds.rowsHint} · {ds.sizeHint}
                </p>
              </div>
              <button
                type="button"
                class={ds.featured ? btnPrimary : btn}
                disabled={input.loading}
                onClick={() => loadDataset(ds.id)}
              >
                {input.loading ? "Loading…" : "Load"}
              </button>
            </li>
          ))}
        </ul>
        <p class={`${hint} mt-3`}>
          Or pick any local <code class="font-mono text-xs">.wcol</code> via the file picker below.
        </p>
        <label class="mt-3 flex cursor-pointer flex-col gap-1 text-sm">
          <span class="text-slate-500 dark:text-slate-400">Local file</span>
          <input
            type="file"
            class="text-sm file:mr-3 file:rounded-md file:border-0 file:bg-blue-500 file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-white hover:file:bg-blue-600 disabled:opacity-50"
            accept=".wcol,application/octet-stream"
            disabled={input.loading}
            onChange={(e) => {
              const f = (e.currentTarget as HTMLInputElement).files?.[0];
              if (f) onEvent({ type: "LOAD_FILE", file: f });
            }}
          />
        </label>
      </div>

      <p class="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
        Remote URL
      </p>
      <div class={row}>
        <input
          type="url"
          id="url-input"
          class={inputUrl}
          placeholder="https://…/dataset.wcol (Range + CORS)"
          value={input.url}
          disabled={input.loading}
          onInput={(e) =>
            onEvent({ type: "URL_CHANGED", url: (e.currentTarget as HTMLInputElement).value })
          }
        />
        <button
          type="button"
          id="url-load"
          class={btn}
          disabled={input.loading}
          onClick={() => onEvent({ type: "LOAD_URL" })}
        >
          Open
        </button>
      </div>

      <p id="load-status" class={`mt-3 ${input.isError ? statusError : status}`}>
        {input.status}
      </p>
    </section>
  );
}
