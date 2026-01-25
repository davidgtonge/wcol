import { badge, featurePill } from "../ui/classes.ts";

const FEATURES = [
  "WASM + SIMD",
  "Parallel workers",
  "No server DB",
  "HTTP range I/O",
];

export function Hero() {
  return (
    <header class="mb-8 animate-slide-up text-center sm:text-left">
      <div class="mb-4 inline-flex items-center gap-2">
        <span class="flex h-11 w-11 items-center justify-center rounded-xl bg-gradient-to-br from-blue-500 to-blue-600 text-lg font-bold text-white shadow-lg shadow-blue-500/30">
          w
        </span>
        <span class={badge}>Browser-native</span>
      </div>
      <h1 class="bg-gradient-to-r from-slate-900 via-slate-700 to-slate-900 bg-clip-text text-4xl font-bold tracking-tight text-transparent dark:from-white dark:via-slate-200 dark:to-white sm:text-5xl">
        Query millions of rows
        <span class="block text-blue-600 dark:text-blue-400">without a server</span>
      </h1>
      <p class="mx-auto mt-4 max-w-2xl text-base leading-relaxed text-slate-600 dark:text-slate-400 sm:mx-0">
        Load a <code class="rounded bg-slate-200/80 px-1.5 py-0.5 font-mono text-sm dark:bg-wcol-border/60">.wcol</code>{" "}
        file, pick a preset, and watch aggregations finish in milliseconds — WASM compute,
        dictionary-encoded strings, parallel chunk workers.
      </p>
      <ul class="mt-5 flex flex-wrap justify-center gap-2 sm:justify-start">
        {FEATURES.map((f) => (
          <li key={f} class={featurePill}>
            {f}
          </li>
        ))}
      </ul>
    </header>
  );
}
