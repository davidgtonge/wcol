import type { ChartItem } from "../arch/types.ts";
import { CHART_COLORS, formatCount, truncateLabel } from "./format.ts";
import { chartCaption, chartSub, hint } from "../ui/classes.ts";

export type BarChartInput = {
  orientation: "h" | "v";
  title: string;
  subtitle?: string;
  items: ChartItem[];
  valueLabel?: string;
};

export type BarChartEvent = { type: "BAR_CLICK"; index: number };

type Props = {
  input: BarChartInput;
  onEvent?: (event: BarChartEvent) => void;
};

const barColor = (i: number) => (i % 2 === 0 ? CHART_COLORS.bar : CHART_COLORS.barAlt);

function BarChartH({ input, onEvent }: Props) {
  const { items } = input;
  const max = Math.max(...items.map((i) => i.value), 1);
  const labelW = 148;
  const valueW = 64;
  const chartW = 360;
  const barH = 20;
  const gap = 5;
  const pad = { top: 8, right: 12, bottom: 8, left: 8 };
  const h = pad.top + items.length * (barH + gap) + pad.bottom;
  const w = labelW + chartW + valueW + pad.left + pad.right;
  const x0 = pad.left + labelW;

  return (
    <svg class="bar-animate" viewBox={`0 0 ${w} ${h}`} width="100%" role="img" aria-label={input.title}>
      {items.map((item, i) => {
        const y = pad.top + i * (barH + gap);
        const barLen = (item.value / max) * chartW;
        return (
          <g key={i}>
            <text
              x={x0 - 6}
              y={y + barH * 0.72}
              text-anchor="end"
              class={`chart-label ${onEvent ? "cursor-pointer" : ""}`}
              onClick={() => onEvent?.({ type: "BAR_CLICK", index: i })}
            >
              {truncateLabel(item.label, 22)}
            </text>
            <rect x={x0} y={y} width={chartW} height={barH} fill={CHART_COLORS.grid} rx={3} />
            <rect
              x={x0}
              y={y}
              width={Math.max(barLen, 2)}
              height={barH}
              fill={barColor(i)}
              rx={3}
              class={`bar-fill ${onEvent ? "cursor-pointer" : ""}`}
              onClick={() => onEvent?.({ type: "BAR_CLICK", index: i })}
            />
            <text x={x0 + chartW + 8} y={y + barH * 0.72} class="chart-value">
              {formatCount(item.value)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function BarChartV({ input, onEvent }: Props) {
  const { items } = input;
  const max = Math.max(...items.map((i) => i.value), 1);
  const w = 520;
  const h = 280;
  const pad = { top: 16, right: 16, bottom: 72, left: 48 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const slot = innerW / items.length;
  const barW = Math.min(28, slot * 0.65);
  const ticks = 4;
  const tickStep = max / ticks;

  return (
    <svg class="bar-animate-v" viewBox={`0 0 ${w} ${h}`} width="100%" role="img" aria-label={input.title}>
      {[...Array(ticks + 1)].map((_, i) => {
        const v = tickStep * i;
        const y = pad.top + innerH - (v / max) * innerH;
        return (
          <g key={i}>
            <line x1={pad.left} y1={y} x2={w - pad.right} y2={y} stroke={CHART_COLORS.grid} />
            <text x={pad.left - 6} y={y + 4} text-anchor="end" class="chart-tick">
              {formatCount(v)}
            </text>
          </g>
        );
      })}
      {items.map((item, i) => {
        const barLen = (item.value / max) * innerH;
        const cx = pad.left + slot * i + slot / 2;
        const y = pad.top + innerH - barLen;
        return (
          <g key={i}>
            <rect
              x={cx - barW / 2}
              y={y}
              width={barW}
              height={Math.max(barLen, 2)}
              fill={barColor(i)}
              rx={3}
              class="bar-fill"
              onClick={() => onEvent?.({ type: "BAR_CLICK", index: i })}
            />
            <text
              x={cx}
              y={h - pad.bottom + 14}
              text-anchor="end"
              class="chart-label"
              transform={`rotate(-42, ${cx}, ${h - pad.bottom + 14})`}
            >
              {truncateLabel(item.label, 16)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

export function BarChart({ input, onEvent }: Props) {
  if (!input.items.length) return <p class={hint}>No data</p>;

  const Chart = input.orientation === "h" ? BarChartH : BarChartV;
  return (
    <figure class="chart">
      <figcaption class={chartCaption}>
        <strong class="text-sm">{input.title}</strong>
        {input.subtitle ? <span class={chartSub}>{input.subtitle}</span> : null}
      </figcaption>
      <Chart input={input} onEvent={onEvent} />
      {input.valueLabel ? <p class={`${hint} mt-1`}>{input.valueLabel}</p> : null}
    </figure>
  );
}
