import type { AppRoute } from "../generated/engine-types.ts";
import { panel } from "../ui/classes.ts";

const COPY: Record<Exclude<AppRoute, "explore">, { title: string; body: string }> = {
  compare: {
    title: "Compare",
    body: "Side-by-side crate and saved-view comparison lands in Phase 2.",
  },
  trends: {
    title: "Trends",
    body: "Time-windowed ecosystem trends and synchronized filters are planned for Phase 2.",
  },
  board: {
    title: "Board",
    body: "Save findings with captions into a narrative board in Phase 3.",
  },
};

type Props = { route: Exclude<AppRoute, "explore"> };

export function PlaceholderRoute({ route }: Props) {
  const { title, body } = COPY[route];
  return (
    <section class={`${panel} py-12 text-center`}>
      <h2 class="text-lg font-semibold">{title}</h2>
      <p class="mx-auto mt-2 max-w-md text-sm text-slate-500 dark:text-slate-400">{body}</p>
    </section>
  );
}
