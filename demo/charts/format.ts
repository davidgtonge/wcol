export function formatCount(value: number | null | undefined): string {
  if (value === undefined || value === null || Number.isNaN(value)) return "—";
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 10_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

export function truncateLabel(label: string, max = 28): string {
  if (label.length <= max) return label;
  return `${label.slice(0, max - 1)}…`;
}

export const CHART_COLORS = {
  bar: "#3d8bfd",
  barAlt: "#60a5fa",
  grid: "rgba(139, 156, 179, 0.18)",
};
