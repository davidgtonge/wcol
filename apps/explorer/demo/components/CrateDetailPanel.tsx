import { useState } from "preact/hooks";
import type { CrateDetailPhase, CrateDetailSummary, CrateVersionRow } from "../generated/engine-types.ts";
import type { WorkspaceEvent } from "../arch/events.ts";
import { formatCount } from "../charts/format.ts";
import { btn, btnGhost, h3, panel, table, tableWrap, td, th } from "../ui/classes.ts";

export type CrateDetailInput = {
  selectedCrate: string | null;
  detail: CrateDetailSummary | null;
  phase: CrateDetailPhase;
  status: string;
};

type Props = {
  input: CrateDetailInput;
  onEvent: (event: WorkspaceEvent) => void;
};

type Tab = "overview" | "versions" | "yanked";

function topVersion(versions: CrateVersionRow[]): CrateVersionRow | null {
  if (!versions.length) return null;
  return versions.reduce((a, b) => (b.downloads > a.downloads ? b : a));
}

function VersionTable({ versions, highlight }: { versions: CrateVersionRow[]; highlight?: (v: CrateVersionRow) => boolean }) {
  return (
    <div class={tableWrap}>
      <table class={table}>
        <thead>
          <tr>
            <th class={th}>Version</th>
            <th class={th}>Downloads</th>
            <th class={th}>License</th>
            <th class={th}>Yanked</th>
          </tr>
        </thead>
        <tbody>
          {versions.slice(0, 30).map((v) => (
            <tr key={v.version} class={highlight?.(v) ? "bg-amber-500/10" : ""}>
              <td class={td}>{v.version}</td>
              <td class={td}>{formatCount(Number(v.downloads))}</td>
              <td class={td}>{v.license}</td>
              <td class={td}>{v.yanked ? "yes" : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function CrateDetailPanel({ input, onEvent }: Props) {
  const [tab, setTab] = useState<Tab>("overview");

  if (!input.selectedCrate) return null;

  const { detail, phase, status } = input;
  const name = input.selectedCrate;
  const top = detail ? topVersion(detail.versions) : null;
  const yankedVersions = detail?.versions.filter((v) => v.yanked) ?? [];

  return (
    <aside class={`${panel} lg:sticky lg:top-6`}>
      <div class="mb-3 flex items-start justify-between gap-2">
        <div>
          <p class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Crate profile</p>
          <h2 class="text-base font-semibold">{name}</h2>
          {top ? (
            <p class="text-xs text-slate-500 dark:text-slate-400">
              Top version {top.version} · {formatCount(Number(top.downloads))} downloads
            </p>
          ) : (
            <p class="text-xs text-slate-500 dark:text-slate-400">{status}</p>
          )}
        </div>
        <div class="flex gap-1">
          <button type="button" class={`${btn} px-2 py-1 text-xs`} onClick={() => onEvent({ type: "CRATE_PIN", name })}>
            Pin
          </button>
          <button
            type="button"
            class={`${btnGhost} px-2 py-1 text-xs`}
            onClick={() => {
              onEvent({ type: "CRATE_PIN", name });
              onEvent({ type: "ROUTE_SET", route: "compare" });
            }}
          >
            Compare
          </button>
          <button type="button" class={btnGhost} onClick={() => onEvent({ type: "CRATE_DETAIL_CLOSE" })}>
            ×
          </button>
        </div>
      </div>

      {phase === "loading" ? (
        <div class="space-y-2" aria-hidden="true">
          <div class="skeleton-bar h-4 w-2/3 rounded" />
          <div class="skeleton-bar h-24 w-full rounded-lg" />
        </div>
      ) : phase === "error" ? (
        <p class="text-sm text-red-500">{status}</p>
      ) : detail ? (
        <div class="space-y-4">
          <dl class="grid grid-cols-2 gap-x-3 gap-y-2 rounded-lg border border-slate-200/80 bg-slate-50/50 p-3 text-sm dark:border-wcol-border dark:bg-[#0f1419]/50">
            <dt class="text-xs text-slate-500">Total downloads</dt>
            <dd class="font-medium tabular-nums">{formatCount(Number(detail.totalDownloads))}</dd>
            <dt class="text-xs text-slate-500">Versions</dt>
            <dd class="font-medium tabular-nums">{detail.versionCount}</dd>
            <dt class="text-xs text-slate-500">License</dt>
            <dd>{detail.primaryLicense ?? "—"}</dd>
            <dt class="text-xs text-slate-500">Yanked versions</dt>
            <dd class="font-medium tabular-nums">{detail.yankedCount}</dd>
          </dl>

          <div class="flex gap-1 border-b border-slate-200 dark:border-wcol-border">
            {(["overview", "versions", "yanked"] as const).map((t) => (
              <button
                key={t}
                type="button"
                class={`px-2.5 py-1.5 text-xs font-medium capitalize ${
                  tab === t
                    ? "border-b-2 border-blue-500 text-blue-600 dark:text-blue-400"
                    : "text-slate-500 hover:text-slate-700 dark:hover:text-slate-300"
                }`}
                onClick={() => setTab(t)}
              >
                {t}
              </button>
            ))}
          </div>

          {tab === "overview" ? (
            <div class="space-y-3 text-xs text-slate-600 dark:text-slate-300">
              {top ? (
                <p>
                  <strong class="text-slate-800 dark:text-slate-100">Most downloaded:</strong> {top.version} (
                  {formatCount(Number(top.downloads))})
                </p>
              ) : null}
              {detail.yankedCount > 0 ? (
                <p>
                  <strong class="text-slate-800 dark:text-slate-100">Yanked:</strong> {detail.yankedCount} of{" "}
                  {detail.versionCount} versions were yanked.
                </p>
              ) : (
                <p>No yanked versions in the loaded sample.</p>
              )}
              <p class="text-slate-500">Click a bar in the chart or use Compare to analyze alongside other crates.</p>
            </div>
          ) : null}

          {tab === "versions" ? (
            <div>
              <h3 class={h3}>All versions</h3>
              <VersionTable
                versions={detail.versions}
                highlight={(v) => v.version === top?.version}
              />
            </div>
          ) : null}

          {tab === "yanked" ? (
            <div>
              <h3 class={h3}>Yanked versions</h3>
              {yankedVersions.length ? (
                <VersionTable versions={yankedVersions} />
              ) : (
                <p class="text-xs text-slate-500">No yanked versions for this crate.</p>
              )}
            </div>
          ) : null}
        </div>
      ) : null}
    </aside>
  );
}
