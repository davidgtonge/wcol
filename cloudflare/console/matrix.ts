/**
 * The shuffle matrix: scans down, the tasks they send to across, one cell per edge. A cell is
 * grey until its edge starts, blue while it flows, then coloured by its bytes or by when it
 * finished (late edges stand out). Bars at the right and bottom total each row and column.
 */
import type { LiveEdge, LiveJobView, LiveTask } from "../src/coordinator/live.ts";
import { fitCanvas, type Palette } from "./format.ts";

export type MatrixMode = "bytes" | "finish";
export type MatrixHit = (x: number, y: number) => { edge: LiveEdge; from: LiveTask; to: LiveTask } | null;

const LEFT = 30;
const TOP = 14;
const MARGIN = 34;
const MAX_CELL = 14;

export function drawMatrix(canvas: HTMLCanvasElement, view: LiveJobView, mode: MatrixMode, colors: Palette): MatrixHit {
  const rows = view.tasks.filter((task) => task.role === "scan");
  const targets = new Map<number, LiveTask>();
  const byId = new Map(view.tasks.map((task) => [task.id, task] as const));
  for (const edge of view.edges) {
    const to = byId.get(edge.to);
    if (to) targets.set(to.id, to);
  }
  const cols = [...targets.values()].sort((a, b) => (a.role === b.role ? a.index - b.index : a.role === "reducer" ? -1 : 1));
  const width = canvas.clientWidth;
  const cell = Math.max(2, Math.min(MAX_CELL, Math.floor((width - LEFT - MARGIN) / Math.max(cols.length, 1)), Math.floor(360 / Math.max(rows.length, 1))));
  const height = TOP + rows.length * cell + MARGIN;
  const { ctx } = fitCanvas(canvas, height);
  const rowAt = new Map(rows.map((task, i) => [task.id, i] as const));
  const colAt = new Map(cols.map((task, i) => [task.id, i] as const));

  ctx.font = "9px ui-monospace, Menlo, monospace";
  ctx.textBaseline = "middle";
  ctx.fillStyle = colors.muted;
  ctx.fillText("scans", 0, TOP + 4);
  ctx.fillText(cols[0]?.role === "finalizer" ? "→ finalizer" : "→ reducers", LEFT, 6);

  const maxBytes = Math.max(1, ...view.edges.map((edge) => edge.bytes));
  const ends = view.edges.flatMap((edge) => (edge.endMs == null ? [] : [edge.endMs]));
  const first = ends.length ? Math.min(...ends) : 0;
  const last = ends.length ? Math.max(...ends) : 1;
  const late = (ms: number) => (last > first ? (ms - first) / (last - first) : 0);

  for (const edge of view.edges) {
    const row = rowAt.get(edge.from);
    const col = colAt.get(edge.to);
    if (row == null || col == null) continue;
    const px = LEFT + col * cell;
    const py = TOP + row * cell;
    const size = cell > 3 ? cell - 1 : cell;
    if (edge.state === "idle") {
      ctx.fillStyle = colors.line;
    } else if (edge.state === "flowing") {
      ctx.fillStyle = colors.accent;
    } else if (mode === "finish" && edge.endMs != null) {
      // Early edges green, the last ones red.
      const t = late(edge.endMs);
      ctx.fillStyle = t > 0.85 ? colors.bad : t > 0.6 ? colors.warn : colors.ok;
      ctx.globalAlpha = 0.35 + 0.65 * t;
    } else {
      ctx.fillStyle = colors.ok;
      ctx.globalAlpha = 0.2 + 0.8 * Math.sqrt(edge.bytes / maxBytes);
    }
    ctx.fillRect(px, py, size, size);
    ctx.globalAlpha = 1;
  }

  // Row totals (bytes each scan sent) and column totals (bytes each target received).
  const rowBytes = rows.map((task) => task.bytesOut);
  const colBytes = cols.map((task) => view.edges.filter((edge) => edge.to === task.id).reduce((sum, edge) => sum + edge.bytes, 0));
  const maxRow = Math.max(1, ...rowBytes);
  const maxCol = Math.max(1, ...colBytes);
  const right = LEFT + cols.length * cell + 4;
  const bottom = TOP + rows.length * cell + 4;
  ctx.fillStyle = colors.accent;
  rowBytes.forEach((value, i) => ctx.fillRect(right, TOP + i * cell, ((MARGIN - 8) * value) / maxRow, Math.max(1, cell - 1)));
  colBytes.forEach((value, i) => ctx.fillRect(LEFT + i * cell, bottom, Math.max(1, cell - 1), ((MARGIN - 8) * value) / maxCol));

  return (x, y) => {
    const col = Math.floor((x - LEFT) / cell);
    const row = Math.floor((y - TOP) / cell);
    const from = rows[row];
    const to = cols[col];
    if (!from || !to || x < LEFT || y < TOP) return null;
    const edge = view.edges.find((candidate) => candidate.from === from.id && candidate.to === to.id);
    return edge ? { edge, from, to } : null;
  };
}
