import { useState } from "preact/hooks";
import type { AppRoute, PinnedCrate, SavedViewOption } from "../generated/engine-types.ts";
import type { WorkspaceEvent } from "../arch/events.ts";
import { badge, btn, btnGhost, hint, input as inputClass } from "../ui/classes.ts";
import { formatCount } from "../charts/format.ts";

const ROUTES: { id: AppRoute; label: string }[] = [
  { id: "explore", label: "Explore" },
  { id: "compare", label: "Compare" },
  { id: "trends", label: "Trends" },
  { id: "board", label: "Board" },
];

export type WorkspaceHeaderInput = {
  route: AppRoute;
  canUndo: boolean;
  canRedo: boolean;
  savedViews: SavedViewOption[];
  shareableUrl: string;
  pinnedCrates: PinnedCrate[];
  rowCount: number | null;
  columnCount: number | null;
};

type Props = {
  input: WorkspaceHeaderInput;
  onEvent: (event: WorkspaceEvent) => void;
};

function BrandMark() {
  return (
    <div class="flex shrink-0 items-center gap-2">
      <span class="flex h-7 w-7 items-center justify-center rounded-md bg-gradient-to-br from-blue-500 to-blue-600 text-xs font-bold text-white shadow-md shadow-blue-500/20">
        w
      </span>
      <span class="text-sm font-semibold tracking-tight">wcol</span>
      <span class={`${badge} hidden sm:inline-flex`}>crates.io</span>
    </div>
  );
}

export function WorkspaceHeader({ input, onEvent }: Props) {
  const [viewName, setViewName] = useState("");

  const copyLink = async () => {
    const url = `${window.location.origin}${window.location.pathname}${input.shareableUrl}`;
    try {
      await navigator.clipboard.writeText(url);
    } catch {
      // hash remains in the address bar
    }
  };

  const saveView = () => {
    const name = viewName.trim();
    if (!name) return;
    onEvent({ type: "SAVED_VIEW_SAVE", name });
    setViewName("");
  };

  const meta =
    input.rowCount != null && input.columnCount != null
      ? `${formatCount(input.rowCount)} rows · ${input.columnCount} cols`
      : null;

  return (
    <header class="mb-4 border-b border-slate-200/80 pb-3 dark:border-wcol-border/80">
      <div class="flex flex-wrap items-center gap-x-3 gap-y-2">
        <BrandMark />

        <div
          class="inline-flex rounded-lg border border-slate-200 bg-white p-0.5 dark:border-wcol-border dark:bg-[#0f1419]"
          role="tablist"
          aria-label="Workspace"
        >
          {ROUTES.map((r) => {
            const active = input.route === r.id;
            const soon = r.id !== "explore";
            return (
              <button
                key={r.id}
                type="button"
                role="tab"
                aria-selected={active}
                class={`rounded-md px-2.5 py-1 text-xs font-medium transition sm:px-3 sm:text-sm ${
                  active
                    ? "bg-blue-500 text-white shadow-sm"
                    : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-[#141c28]"
                }`}
                onClick={() => onEvent({ type: "ROUTE_SET", route: r.id })}
              >
                {r.label}
                {soon && !active ? <span class="ml-0.5 text-[10px] opacity-60">β</span> : null}
              </button>
            );
          })}
        </div>

        <div class="ml-auto flex items-center gap-1 sm:gap-2">
          {meta ? (
            <span class={`${hint} hidden tabular-nums sm:inline`}>{meta}</span>
          ) : null}
          <button
            type="button"
            class={btnGhost}
            disabled={!input.canUndo}
            onClick={() => onEvent({ type: "UNDO" })}
          >
            Undo
          </button>
          <button
            type="button"
            class={btnGhost}
            disabled={!input.canRedo}
            onClick={() => onEvent({ type: "REDO" })}
          >
            Redo
          </button>
          <button
            type="button"
            class={`${btn} px-2.5 py-1.5 text-xs sm:px-3 sm:py-2 sm:text-sm`}
            onClick={() => onEvent({ type: "DATA_DRAWER_SET", open: true })}
          >
            Data
          </button>
        </div>
      </div>

      <div class="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1.5 border-t border-slate-200/60 pt-2 dark:border-wcol-border/60">
        <span class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Views</span>
        {input.savedViews.length > 0 ? (
          <div class="flex flex-wrap gap-1">
            {input.savedViews.map((view) => (
              <span
                key={view.id}
                class="inline-flex items-center rounded-md border border-slate-200 bg-white dark:border-wcol-border dark:bg-[#0f1419]"
              >
                <button
                  type="button"
                  class={`px-2 py-0.5 text-xs font-medium ${
                    view.active ? "text-blue-600 dark:text-blue-400" : "text-slate-700 dark:text-slate-300"
                  }`}
                  onClick={() => onEvent({ type: "SAVED_VIEW_APPLY", id: view.id })}
                >
                  {view.name}
                </button>
                <button
                  type="button"
                  class="px-1 py-0.5 text-xs text-slate-400 hover:text-red-500"
                  aria-label={`Remove ${view.name}`}
                  onClick={() => onEvent({ type: "SAVED_VIEW_REMOVE", id: view.id })}
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        ) : (
          <span class="text-[11px] text-slate-500">none saved</span>
        )}
        <span class="hidden h-3 w-px bg-slate-200 dark:bg-wcol-border sm:inline" aria-hidden="true" />
        <input
          class={`${inputClass} min-w-0 max-w-[10rem] py-1 text-xs`}
          placeholder="Save this exploration"
          value={viewName}
          onInput={(e) => setViewName((e.currentTarget as HTMLInputElement).value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") saveView();
          }}
        />
        <button type="button" class={`${btn} px-2 py-1 text-xs`} disabled={!viewName.trim()} onClick={saveView}>
          Save
        </button>
        <button type="button" class={`${btnGhost} px-2 py-1 text-xs`} onClick={() => void copyLink()}>
          Copy link
        </button>

        {input.pinnedCrates.length > 0 ? (
          <>
            <span class="hidden h-3 w-px bg-slate-200 dark:bg-wcol-border sm:inline" aria-hidden="true" />
            <span class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Pinned</span>
            <div class="flex flex-wrap gap-1">
              {input.pinnedCrates.map((crate) => (
                <span
                  key={crate.name}
                  class="inline-flex items-center rounded-full border border-blue-500/30 bg-blue-500/5 px-2 py-0.5 text-xs font-medium text-blue-700 dark:text-blue-300"
                >
                  <button type="button" onClick={() => onEvent({ type: "CRATE_SELECT", name: crate.name })}>
                    {crate.name}
                  </button>
                  <button
                    type="button"
                    class="ml-0.5 text-blue-400 hover:text-red-500"
                    aria-label={`Unpin ${crate.name}`}
                    onClick={() => onEvent({ type: "CRATE_UNPIN", name: crate.name })}
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
            {input.pinnedCrates.length >= 2 ? (
              <button
                type="button"
                class={`${btn} px-2 py-1 text-xs`}
                onClick={() => onEvent({ type: "ROUTE_SET", route: "compare" })}
              >
                Compare selected
              </button>
            ) : null}
          </>
        ) : null}
      </div>
    </header>
  );
}
