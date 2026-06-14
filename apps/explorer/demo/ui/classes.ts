/** Shared Tailwind class strings (Play CDN scans `app.js` for these). */

export const page =
  "min-h-screen bg-slate-50 text-slate-900 dark:bg-[#0a0e14] dark:text-slate-100";

export const shell = "mx-auto max-w-[90rem] px-4 py-4 sm:px-6 sm:py-5";

export const panel =
  "rounded-xl border border-slate-200/80 bg-white/90 p-4 shadow-sm backdrop-blur-sm sm:p-5 dark:border-wcol-border/80 dark:bg-wcol-surface/90";

export const panelInset =
  "rounded-lg border border-slate-200 bg-slate-50/80 p-3 dark:border-wcol-border dark:bg-[#0f1419]/80";

export const h2 = "text-base font-semibold tracking-tight sm:text-lg";

export const h3 = "mb-2 text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400";

export const row = "flex flex-wrap items-center gap-3";

export const btn =
  "inline-flex cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3.5 py-2 text-sm font-medium shadow-sm transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-wcol-border dark:bg-[#0f1419] dark:hover:border-slate-500 dark:hover:bg-[#141c28]";

export const btnPrimary =
  "inline-flex cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-blue-500 bg-blue-500 px-4 py-2.5 text-sm font-semibold text-white shadow-md shadow-blue-500/20 transition hover:bg-blue-600 hover:shadow-blue-500/30 disabled:cursor-not-allowed disabled:opacity-50 dark:shadow-blue-500/15";

export const btnGhost =
  "inline-flex cursor-pointer items-center rounded-lg px-2.5 py-1.5 text-sm text-slate-600 transition hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-[#141c28]";

export const input =
  "min-w-48 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm shadow-sm transition focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-wcol-border dark:bg-[#0f1419] disabled:opacity-50";

export const inputUrl = `${input} min-w-0 flex-1`;

export const badge =
  "inline-flex items-center rounded-full border border-blue-500/30 bg-blue-500/10 px-2.5 py-0.5 text-xs font-medium text-blue-600 dark:border-blue-400/30 dark:bg-blue-400/10 dark:text-blue-300";

export const status = "text-sm text-slate-600 dark:text-slate-400";

export const statusError = "text-sm text-red-500 dark:text-red-400";

export const hint = "text-xs leading-relaxed text-slate-500 dark:text-slate-400";

export const presetCard = (active: boolean) =>
  `preset-card w-full rounded-xl border p-3 text-left disabled:cursor-not-allowed disabled:opacity-50 ${
    active
      ? "preset-card-active border-blue-500 bg-blue-500/5 dark:border-blue-400 dark:bg-blue-400/10"
      : "border-slate-200 bg-white hover:border-slate-300 dark:border-wcol-border dark:bg-[#0f1419] dark:hover:border-slate-500"
  }`;

export const pre =
  "max-h-48 overflow-auto rounded-lg border border-slate-200 bg-slate-50 p-3 font-mono text-xs leading-relaxed dark:border-wcol-border dark:bg-[#0a0e14]";

export const metaDl = "grid grid-cols-2 gap-x-4 gap-y-2 text-sm sm:grid-cols-3";

export const metaDt = "text-xs text-slate-500 dark:text-slate-400";

export const metaDd = "font-medium tabular-nums";

export const schemaList = "columns-2 gap-x-4 pl-4 text-xs leading-relaxed";

export const footer = "mt-10 border-t border-slate-200/80 pt-6 text-center text-sm text-slate-500 dark:border-wcol-border/80 dark:text-slate-400";

export const footerLink = "text-blue-600 hover:underline dark:text-blue-400";

export const chartCaption = "mb-3 flex flex-wrap items-baseline gap-x-4 gap-y-1";

export const chartSub = "text-sm text-slate-500 dark:text-slate-400";

export const statPill =
  "inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-600 dark:border-wcol-border dark:bg-[#0f1419] dark:text-slate-300";

export const tableWrap =
  "max-h-[24rem] overflow-auto rounded-lg border border-slate-200 dark:border-wcol-border";

export const table = "w-full border-collapse text-xs";

export const th =
  "sticky top-0 z-10 bg-slate-100 px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wide text-slate-500 dark:bg-[#0f1419] dark:text-slate-400";

export const td = "whitespace-nowrap border-b border-slate-100 px-3 py-2 dark:border-wcol-border/60";

export const rowChip =
  "rounded-md border border-slate-200 bg-slate-50 px-2 py-1 font-mono text-xs dark:border-wcol-border dark:bg-[#0f1419]";

export const featurePill =
  "rounded-full border border-slate-200/80 bg-white/60 px-3 py-1 text-xs text-slate-600 backdrop-blur dark:border-wcol-border dark:bg-wcol-surface/60 dark:text-slate-300";

export const timingHero =
  "relative overflow-hidden rounded-xl border border-blue-500/25 bg-gradient-to-br from-blue-500/10 via-transparent to-transparent p-5 dark:from-blue-400/15";

export const timingMs =
  "timing-digit font-mono text-5xl font-semibold tracking-tight text-blue-600 sm:text-6xl dark:text-blue-400";
