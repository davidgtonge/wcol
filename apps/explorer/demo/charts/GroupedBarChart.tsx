import { CHART_COLORS, formatCount, truncateLabel } from "./format.ts";
import { chartCaption, chartSub, hint, row } from "../ui/classes.ts";

export type GroupedBarChartInput = {
  title: string;
  subtitle?: string;
  groups: string[];
  series: { name: string; values: number[] }[];
  valueLabel?: string;
};

type Props = {
  input: GroupedBarChartInput;
  onEvent?: (event: never) => void;
};

export function GroupedBarChart({ input }: Props) {
  const { groups, series } = input;
  if (!groups.length || !series.length) return <p class={hint}>No data</p>;

  const max = Math.max(...series.flatMap((s) => s.values), 1);
  const w = 520;
  const h = 280;
  const pad = { top: 16, right: 16, bottom: 56, left: 48 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const groupSlot = innerW / groups.length;
  const barGap = 3;
  const barW = Math.min(18, (groupSlot - barGap) / series.length - barGap);
  const palette = [CHART_COLORS.bar, "#f59e0b", "#10b981", "#a78bfa"];

  return (
    <figure class="chart">
      <figcaption class={chartCaption}>
        <strong class="text-sm">{input.title}</strong>
        {input.subtitle ? <span class={chartSub}>{input.subtitle}</span> : null}
        <div class={`${row} w-full gap-x-3 text-sm text-slate-500 dark:text-slate-400`}>
          {series.map((s, i) => (
            <span key={i} class="inline-flex items-center gap-1.5">
              <span
                class="inline-block h-2.5 w-2.5 rounded-sm"
                style={{ background: palette[i % palette.length] }}
              />
              {s.name}
            </span>
          ))}
        </div>
      </figcaption>
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" role="img" aria-label={input.title}>
        <line
          x1={pad.left}
          y1={pad.top + innerH}
          x2={w - pad.right}
          y2={pad.top + innerH}
          stroke={CHART_COLORS.grid}
        />
        {groups.map((group, gi) => {
          const gx = pad.left + groupSlot * gi + groupSlot / 2;
          return (
            <g key={gi}>
              {series.map((s, si) => {
                const v = s.values[gi] ?? 0;
                const barH = (v / max) * innerH;
                const totalW = series.length * barW + (series.length - 1) * barGap;
                const x = gx - totalW / 2 + si * (barW + barGap);
                const y = pad.top + innerH - barH;
                return (
                  <rect
                    key={si}
                    x={x}
                    y={y}
                    width={barW}
                    height={Math.max(barH, v > 0 ? 2 : 0)}
                    fill={palette[si % palette.length]}
                    rx={2}
                  />
                );
              })}
              <text x={gx} y={h - 12} text-anchor="middle" class="chart-label">
                {truncateLabel(group, 12)}
              </text>
            </g>
          );
        })}
      </svg>
      <p class={`${hint} mt-1`}>
        {input.valueLabel ?? "sum downloads"} · max {formatCount(max)}
      </p>
    </figure>
  );
}
