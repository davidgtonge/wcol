/** The query list: recorded runs first (playable without a token), then the other ClickBench queries. */
import { escape, seconds } from "./format.ts";

export type PickerQuery = { name: string; sql: string };
export type Recorded = { name: string; ms: number; tasks: number };

/** What each ClickBench query asks, in words. */
const ABOUT: Readonly<Record<string, string>> = {
  Q0: "Count every row",
  Q1: "Count hits with an ad engine",
  Q2: "Sum, count and average in one pass",
  Q3: "Average user id",
  Q4: "Distinct users",
  Q5: "Distinct search phrases",
  Q6: "First and last date",
  Q7: "Hits per ad engine",
  Q8: "Regions by distinct users",
  Q9: "Region totals with distinct users",
  Q10: "Phone models by distinct users",
  Q11: "Phones and models by distinct users",
  Q12: "Top search phrases",
  Q13: "Search phrases by distinct users",
  Q14: "Top phrases per search engine",
  Q15: "Most active users",
  Q16: "Top user and phrase pairs",
  Q19: "Look up one user",
  Q20: "Count URLs containing “google”",
  Q23: "First ten “google” hits, every column",
  Q24: "Earliest ten search phrases",
  Q25: "First ten phrases alphabetically",
  Q26: "Earliest ten phrases, ties by phrase",
  Q29: "Ninety sums over one column",
  Q30: "Engine and client IP totals for searches",
  Q31: "Watch and client IP totals for searches",
  Q32: "Top watch and client IP pairs: about 100M groups",
  Q33: "Most viewed URLs",
  Q36: "Top URLs for one site in July 2013",
  Q37: "Top titles for one site in July 2013",
  Q38: "One site's URLs, a page at a time",
  Q40: "URL hashes by date for one site",
  Q41: "Window sizes for one site",
};

export type PickerState = {
  queries: readonly PickerQuery[];
  recorded: readonly Recorded[];
  selected: string | undefined;
  filter: string;
  /** Without a token only recorded runs can play. */
  hasToken: boolean;
};

function matches(query: PickerQuery, filter: string): boolean {
  if (!filter) return true;
  const needle = filter.toLowerCase();
  return [query.name, ABOUT[query.name] ?? "", query.sql].some((text) => text.toLowerCase().includes(needle));
}

function row(query: PickerQuery, run: Recorded | undefined, state: PickerState): string {
  const selected = query.name === state.selected;
  const locked = !run && !state.hasToken;
  const meta = run
    ? `<span class="badge">▶ ${seconds(run.ms)}${run.tasks ? ` · ${run.tasks} tasks` : ""}</span>`
    : `<span class="muted small">${locked ? "needs token" : ""}</span>`;
  return `<button type="button" role="option" class="query-row${selected ? " selected" : ""}${locked ? " locked" : ""}"
    aria-selected="${selected}" data-name="${escape(query.name)}">
    <b class="mono">${escape(query.name)}</b><span class="about">${escape(ABOUT[query.name] ?? "")}</span>${meta}
    <code class="sql">${escape(query.sql)}</code></button>`;
}

export function renderPicker(container: HTMLElement, state: PickerState, onPick: (name: string) => void): void {
  const runs = new Map(state.recorded.map((run) => [run.name, run]));
  const shown = state.queries.filter((query) => matches(query, state.filter));
  const recorded = shown.filter((query) => runs.has(query.name));
  const others = shown.filter((query) => !runs.has(query.name));
  const group = (title: string, queries: readonly PickerQuery[]) =>
    queries.length ? `<div class="group">${title}</div>${queries.map((query) => row(query, runs.get(query.name), state)).join("")}` : "";
  container.innerHTML =
    group("Recorded on Cloudflare · replays without a token", recorded) +
      group(state.hasToken ? "More ClickBench queries" : "More ClickBench queries · run live with an API token", others) ||
    `<p class="muted small">No query matches “${escape(state.filter)}”.</p>`;
  container.querySelectorAll<HTMLButtonElement>(".query-row").forEach((button) => {
    button.addEventListener("click", () => onPick(button.dataset.name ?? ""));
  });
  // Keep the chosen query in view inside the list; `scrollIntoView` would move the page too.
  const chosen = container.querySelector<HTMLElement>(".query-row.selected");
  if (chosen && (chosen.offsetTop < container.scrollTop || chosen.offsetTop + chosen.offsetHeight > container.scrollTop + container.clientHeight)) {
    container.scrollTop = chosen.offsetTop - container.clientHeight / 3;
  }
}
