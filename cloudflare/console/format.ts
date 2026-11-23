/** Formatting and small helpers shared by the console's panels. */

export function bytes(value: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = value;
  let unit = 0;
  while (n >= 1000 && unit < units.length - 1) {
    n /= 1000;
    unit++;
  }
  return `${n >= 100 || unit === 0 ? Math.round(n) : n.toFixed(1)} ${units[unit]}`;
}

export const seconds = (ms: number) => `${(ms / 1000).toFixed(1)} s`;

export function usd(value: number): string {
  if (value === 0) return "$0";
  if (value < 0.0001) return `$${value.toExponential(1)}`;
  return `$${value < 0.01 ? value.toFixed(5) : value.toFixed(4)}`;
}

export function escape(text: string): string {
  return text.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c] ?? c);
}

export function element<T extends Element>(id: string): T {
  const found = document.getElementById(id);
  if (!found) throw new Error(`console markup has no #${id}`);
  return found as unknown as T;
}

export const median = (values: readonly number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length === 0 ? 0 : (sorted[Math.floor(sorted.length / 2)] ?? 0);
};

/** The theme's colours, read from CSS variables so canvas drawing follows light and dark. */
export type Palette = Record<"ink" | "muted" | "line" | "panel" | "accent" | "accentSoft" | "ok" | "okSoft" | "warn" | "bad" | "idle" | "purple", string>;

export function palette(): Palette {
  const style = getComputedStyle(document.documentElement);
  const read = (name: string) => style.getPropertyValue(name).trim();
  return {
    ink: read("--ink"),
    muted: read("--muted"),
    line: read("--line"),
    panel: read("--panel"),
    accent: read("--accent"),
    accentSoft: read("--accent-soft"),
    ok: read("--ok"),
    okSoft: read("--ok-soft"),
    warn: read("--warn"),
    bad: read("--bad"),
    idle: read("--idle"),
    purple: read("--purple"),
  };
}

/** A canvas sized to its CSS box at the device's pixel ratio; returns its context and size. */
export function fitCanvas(canvas: HTMLCanvasElement, height: number): { ctx: CanvasRenderingContext2D; width: number; height: number } {
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  if (canvas.width !== Math.round(width * ratio) || canvas.height !== Math.round(height * ratio)) {
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    canvas.style.height = `${height}px`;
  }
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("no 2d canvas");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  return { ctx, width, height };
}

export const taskName = (task: { role: string; index: number }) =>
  task.role === "scan" ? `s${task.index}` : task.role === "reducer" ? `r${task.index}` : "finalizer";
