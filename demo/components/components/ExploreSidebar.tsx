import { useEffect, useState } from "preact/hooks";
import type { ComponentChildren } from "preact";
import type { FilterDraft } from "../generated/engine-types.ts";
import type { QueryBuilderEvent } from "../arch/events.ts";
import type { QueryDraft, QueryMode } from "../arch/types.ts";
import { DEFAULT_TOP_K } from "../query/constants.ts";
import { describeQuery } from "../query/query-narrative.ts";
import {
  btn,
  btnPrimary,
  hint,
  input as inputClass,
  panel,
  pre,
  presetCard,
  row,
  status,
  statusError,
} from "../ui/classes.ts";

export type PresetOption = { id: string; label: string; description: string };

export type ExploreSidebarInput = {
  draft: QueryDraft;
  columns: string[];
  presets: PresetOption[];
  planPreview: string;
  status: string;
  isError: boolean;
  running: boolean;
  ready: boolean;
  pinnedFilterCount: number;
};

type Props = {
  input: ExploreSidebarInput;
  onEvent: (event: QueryBuilderEvent) => void;
};

const MODES: { id: QueryMode; label: string; hint: string }[] = [
  { id: "search", label: "Search crates", hint: "Find versions matching text and facets" },
  { id: "aggregate", label: "Rank crates", hint: "Group and rank by summed downloads or metrics" },
  { id: "table", label: "Browse data", hint: "Browse matching version rows with chosen columns" },
];

const SEARCH_PLACEHOLDERS = [
  "serde",
  "MIT crates with high downloads",
  "top crates with yanked versions",
  "compare tokio vs async-std",
  "spider",
];

const FACET_TEMPLATES: { label: string; column: string; op: string; value: string }[] = [
  { label: "MIT license", column: "license", op: "=", value: "MIT" },
  { label: "Downloads > 10k", column: "downloads", op: ">", value: "10000" },
  { label: "Yanked", column: "yanked", op: "=", value: "true" },
  { label: "Crate contains…", column: "crate_name", op: "contains", value: "" },
];

const FILTER_OPS = [
  { value: "=", label: "=" },
  { value: "!=", label: "≠" },
  { value: ">", label: ">" },
  { value: ">=", label: "≥" },
  { value: "<", label: "<" },
  { value: "<=", label: "≤" },
  { value: "contains", label: "contains" },
  { value: "in", label: "in (comma list)" },
  { value: "between", label: "between (a,b)" },
];

function Section({
  title,
  hint: sectionHint,
  children,
}: {
  title: string;
  hint?: string;
  children: ComponentChildren;
}) {
  return (
    <section class="border-t border-slate-200 pt-4 first:border-t-0 first:pt-0 dark:border-wcol-border">
      <div class="mb-2 flex items-baseline justify-between gap-2">
        <h3 class="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</h3>
        {sectionHint ? <span class="text-[10px] text-slate-400">{sectionHint}</span> : null}
      </div>
      {children}
    </section>
  );
}

function ColumnSelect({
  value,
  columns,
  disabled,
  onChange,
  className = "",
}: {
  value: string;
  columns: string[];
  disabled: boolean;
  onChange: (v: string) => void;
  className?: string;
}) {
  return (
    <select
      class={`${inputClass} min-w-0 flex-1 py-1.5 text-xs ${className}`}
      disabled={disabled}
      value={value}
      onChange={(e) => onChange((e.currentTarget as HTMLSelectElement).value)}
    >
      {columns.map((c) => (
        <option key={c} value={c}>
          {c}
        </option>
      ))}
    </select>
  );
}

function FacetRow({
  filter,
  columns,
  disabled,
  onEvent,
}: {
  filter: FilterDraft;
  columns: string[];
  disabled: boolean;
  onEvent: (event: QueryBuilderEvent) => void;
}) {
  return (
    <li class="rounded-lg border border-slate-200 p-2 dark:border-wcol-border">
      <div class={`${row} mb-2`}>
        <button
          type="button"
          class={`text-sm ${filter.pinned ? "text-blue-600" : "text-slate-400"}`}
          title={filter.pinned ? "Unpin facet" : "Pin facet across routes"}
          disabled={disabled}
          onClick={() => onEvent({ type: "FILTER_PIN_SET", id: filter.id, pinned: !filter.pinned })}
        >
          {filter.pinned ? "◆" : "◇"}
        </button>
        <ColumnSelect
          value={filter.column}
          columns={columns}
          disabled={disabled}
          onChange={(column) => onEvent({ type: "FILTER_PATCH", id: filter.id, patch: { column } })}
        />
        {!filter.pinned ? (
          <button
            type="button"
            class="text-xs text-slate-400 hover:text-red-500"
            disabled={disabled}
            onClick={() => onEvent({ type: "FILTER_REMOVE", id: filter.id })}
          >
            ×
          </button>
        ) : null}
      </div>
      <div class={row}>
        <select
          class={`${inputClass} w-20 py-1 text-xs`}
          disabled={disabled}
          value={filter.op}
          onChange={(e) =>
            onEvent({
              type: "FILTER_PATCH",
              id: filter.id,
              patch: { op: (e.currentTarget as HTMLSelectElement).value },
            })
          }
        >
          {FILTER_OPS.map((op) => (
            <option key={op.value} value={op.value}>
              {op.label}
            </option>
          ))}
        </select>
        <input
          class={`${inputClass} min-w-0 flex-1 py-1 text-xs`}
          value={filter.value}
          placeholder="value"
          disabled={disabled}
          onInput={(e) =>
            onEvent({
              type: "FILTER_PATCH",
              id: filter.id,
              patch: { value: (e.currentTarget as HTMLInputElement).value },
            })
          }
        />
      </div>
    </li>
  );
}

function QueryRecipe({ draft }: { draft: QueryDraft }) {
  const narrative = describeQuery(draft);
  if (draft.mode === "aggregate") {
    const keys = draft.groupKeys.filter(Boolean).join(" × ") || "crate_name";
    const metric = draft.aggColumn || "downloads";
    return (
      <p class="text-[11px] leading-relaxed text-slate-600 dark:text-slate-300">
        Show me the top <strong>{draft.topK}</strong> groups by <strong>sum({metric})</strong>, grouped by{" "}
        <strong>{keys}</strong>
        {draft.filters.length || draft.searchText ? ", with facets applied" : ""}.
      </p>
    );
  }
  if (draft.mode === "table") {
    return (
      <p class="text-[11px] leading-relaxed text-slate-600 dark:text-slate-300">
        Browse up to <strong>{draft.topK}</strong> version rows with selected columns.
      </p>
    );
  }
  return (
    <p class="text-[11px] leading-relaxed text-slate-600 dark:text-slate-300">
      {narrative.headline}
    </p>
  );
}

export function ExploreSidebar({ input, onEvent }: Props) {
  const draft = input.draft;
  const columns = input.columns.length ? input.columns : ["—"];
  const disabled = !input.ready || input.running;
  const modeHint = MODES.find((m) => m.id === draft.mode)?.hint;
  const [placeholderIdx, setPlaceholderIdx] = useState(0);

  useEffect(() => {
    const id = window.setInterval(() => setPlaceholderIdx((i) => (i + 1) % SEARCH_PLACEHOLDERS.length), 4000);
    return () => window.clearInterval(id);
  }, []);

  return (
    <aside
      class={`${panel} flex max-h-[calc(100vh-7rem)] flex-col lg:sticky lg:top-6`}
      id="explore-sidebar"
    >
      <header class="mb-4 shrink-0">
        <div class="flex items-center justify-between gap-2">
          <h2 class="text-sm font-semibold">Explore</h2>
          <label class="flex items-center gap-1.5 text-[10px] uppercase tracking-wide text-slate-500">
            Top K
            <input
              type="number"
              class={`${inputClass} w-14 py-0.5 text-center text-xs`}
              min={1}
              max={500}
              value={draft.topK}
              disabled={disabled}
              onInput={(e) =>
                onEvent({
                  type: "QUERY_DRAFT_PATCH",
                  patch: { topK: Number((e.currentTarget as HTMLInputElement).value) || DEFAULT_TOP_K },
                })
              }
            />
          </label>
        </div>
        <div class="mt-3 flex flex-wrap gap-1.5">
          {MODES.map((m) => (
            <button
              type="button"
              key={m.id}
              class={`${btn} flex-1 px-2 py-1.5 text-xs ${
                draft.mode === m.id ? "border-blue-500 text-blue-600 dark:text-blue-400" : ""
              }`}
              disabled={disabled}
              onClick={() => onEvent({ type: "QUERY_DRAFT_PATCH", patch: { mode: m.id } })}
            >
              {m.label}
            </button>
          ))}
        </div>
        {modeHint ? <p class={`${hint} mt-2 text-[11px]`}>{modeHint}</p> : null}
        <div class="mt-3 rounded-md border border-slate-200/80 bg-white/60 p-2 dark:border-wcol-border dark:bg-[#0f1419]/40">
          <QueryRecipe draft={draft} />
        </div>
      </header>

      <div class="min-h-0 flex-1 space-y-4 overflow-y-auto pr-1">
        <Section title="Search crates">
          <input
            type="search"
            class={`${inputClass} w-full py-2 text-sm`}
            placeholder={SEARCH_PLACEHOLDERS[placeholderIdx]}
            value={draft.searchText}
            disabled={disabled}
            onInput={(e) =>
              onEvent({
                type: "QUERY_DRAFT_PATCH",
                patch: { searchText: (e.currentTarget as HTMLInputElement).value, searchColumn: "crate_name" },
              })
            }
          />
          <p class={`${hint} mt-1.5 text-[10px]`}>Search crate names, licenses, or narrow with facets below.</p>
        </Section>

        <Section title="Questions">
          <div class="space-y-1.5" id="preset-buttons">
            {input.presets.map((p) => (
              <button
                type="button"
                key={p.id}
                data-preset={p.id}
                class={`${presetCard(false)} w-full px-2.5 py-2 text-left`}
                disabled={disabled}
                onClick={() => onEvent({ type: "PRESET_SELECTED", id: p.id })}
              >
                <span class="block text-xs font-semibold leading-tight">{p.label}</span>
                <span class="mt-0.5 block text-[10px] leading-snug text-slate-500 dark:text-slate-400">
                  {p.description}
                </span>
              </button>
            ))}
          </div>
        </Section>

        <Section
          title="Facets"
          hint={input.pinnedFilterCount > 0 ? `${input.pinnedFilterCount} pinned` : undefined}
        >
          <div class="mb-2 flex flex-wrap gap-1" data-section="facets">
            {FACET_TEMPLATES.map((t) => (
              <button
                key={t.label}
                type="button"
                class={`${btn} px-2 py-0.5 text-[10px]`}
                disabled={disabled}
                onClick={() =>
                  onEvent({
                    type: "FILTER_ADD_PREFILLED",
                    column: t.column,
                    op: t.op,
                    value: t.value,
                  })
                }
              >
                {t.label}
              </button>
            ))}
          </div>
          <p class={`${hint} mb-2 text-[11px]`}>Narrow results — pinned facets persist across routes.</p>
          {draft.filters.length === 0 ? (
            <p class="text-xs text-slate-500">No facets yet.</p>
          ) : (
            <ul class="space-y-2">
              {draft.filters.map((f) => (
                <FacetRow
                  key={f.id}
                  filter={f}
                  columns={columns}
                  disabled={disabled}
                  onEvent={onEvent}
                />
              ))}
            </ul>
          )}
          <button
            type="button"
            class={`${btn} mt-2 w-full text-xs`}
            disabled={disabled}
            onClick={() => onEvent({ type: "FILTER_ADD" })}
          >
            Add facet
          </button>
        </Section>

        {draft.mode === "aggregate" ? (
          <Section title="Group & metric">
            <label class="mb-1 block text-[10px] text-slate-500">Group by</label>
            <div class={`${row} mb-2`}>
              <ColumnSelect
                value={draft.groupKeys[0] ?? ""}
                columns={columns}
                disabled={disabled}
                onChange={(v) =>
                  onEvent({
                    type: "QUERY_DRAFT_PATCH",
                    patch: { groupKeys: [v, draft.groupKeys[1]].filter(Boolean) },
                  })
                }
              />
              <ColumnSelect
                value={draft.groupKeys[1] ?? ""}
                columns={["", ...columns]}
                disabled={disabled}
                onChange={(v) =>
                  onEvent({
                    type: "QUERY_DRAFT_PATCH",
                    patch: {
                      groupKeys: v ? [draft.groupKeys[0], v].filter(Boolean) : [draft.groupKeys[0]].filter(Boolean),
                    },
                  })
                }
              />
            </div>
            <p class={`${hint} mb-2 text-[10px]`}>Second column optional</p>
            <label class="mb-1 block text-[10px] text-slate-500">Sum metric</label>
            <ColumnSelect
              value={draft.aggColumn}
              columns={columns}
              disabled={disabled}
              onChange={(aggColumn) => onEvent({ type: "QUERY_DRAFT_PATCH", patch: { aggColumn } })}
            />
          </Section>
        ) : null}

        {draft.mode === "table" ? (
          <Section title="Columns">
            <div class="flex flex-wrap gap-1.5">
              {columns.slice(0, 24).map((col) => {
                const on = draft.selectColumns.includes(col);
                return (
                  <button
                    type="button"
                    key={col}
                    class={`${btn} px-2 py-0.5 text-[10px] ${on ? "border-blue-500 text-blue-600" : ""}`}
                    disabled={disabled}
                    onClick={() => {
                      const next = on
                        ? draft.selectColumns.filter((c) => c !== col)
                        : [...draft.selectColumns, col];
                      onEvent({ type: "QUERY_DRAFT_PATCH", patch: { selectColumns: next } });
                    }}
                  >
                    {col}
                  </button>
                );
              })}
            </div>
          </Section>
        ) : null}

        <details class="plan-details">
          <summary class={`${hint} cursor-pointer text-[11px] font-medium`}>Query plan</summary>
          <pre id="query-preview" class={`${pre} mt-2 max-h-32 overflow-auto text-[10px]`}>
            {input.planPreview}
          </pre>
        </details>
      </div>

      <footer class="mt-4 shrink-0 border-t border-slate-200 pt-4 dark:border-wcol-border">
        <button
          type="button"
          id="run-query"
          class={`${btnPrimary} w-full`}
          disabled={disabled}
          onClick={() => onEvent({ type: "RUN_QUERY" })}
        >
          {input.running ? "Running…" : "Run exploration"}
        </button>
        {input.status ? (
          <p id="query-status" class={`mt-2 text-xs ${input.isError ? statusError : status}`}>
            {input.status}
          </p>
        ) : null}
      </footer>
    </aside>
  );
}
