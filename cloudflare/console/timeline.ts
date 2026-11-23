/**
 * The timeline: one lane per task, time across. Scans show when they read and sent; reducers
 * show starting (container or prepare) then receiving; the top lane is the coordinator's
 * stages. In a replay the finished job is drawn faintly behind the moment shown.
 */
import type { LiveJobView, LiveTask } from "../src/coordinator/live.ts";
import { fitCanvas, taskName, type Palette } from "./format.ts";

const GUTTER = 84;
const AXIS = 18;
const COORDINATOR_LANE = 12;
const GROUP_GAP = 10;
const MAX_HEIGHT = 520;

type Lane = { task: LiveTask; y: number };
export type TimelineHit = (x: number, y: number) => { task: LiveTask | null; ms: number };

/** Lanes for scans then reducers, each `lane` px tall, below the axis and coordinator lane. */
function lanes(view: LiveJobView): { rows: Lane[]; lane: number; height: number } {
  const scans = view.tasks.filter((task) => task.role === "scan");
  const reducers = view.tasks.filter((task) => task.role === "reducer");
  const count = Math.max(scans.length + reducers.length, 1);
  const lane = Math.max(3, Math.min(12, Math.floor((MAX_HEIGHT - AXIS - COORDINATOR_LANE - GROUP_GAP * 2) / count)));
  let y = AXIS + COORDINATOR_LANE + GROUP_GAP;
  const rows: Lane[] = [];
  for (const task of scans) {
    rows.push({ task, y });
    y += lane;
  }
  if (reducers.length) y += GROUP_GAP;
  for (const task of reducers) {
    rows.push({ task, y });
    y += lane;
  }
  return { rows, lane, height: y + 6 };
}

/** A round step for axis ticks: about six across. */
function tickStep(durationMs: number): number {
  const raw = durationMs / 6;
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(raw, 1)));
  return [1, 2, 5, 10].map((m) => m * magnitude).find((step) => step >= raw) ?? raw;
}

export function drawTimeline(
  canvas: HTMLCanvasElement,
  view: LiveJobView,
  ghost: LiveJobView | null,
  durationMs: number,
  selected: number | null,
  colors: Palette,
): TimelineHit {
  const layout = lanes(ghost ?? view);
  const { ctx, width } = fitCanvas(canvas, layout.height);
  const span = Math.max(durationMs, 1);
  const x = (ms: number) => GUTTER + ((width - GUTTER - 8) * Math.max(0, ms)) / span;

  // Axis and grid.
  ctx.font = "10px ui-monospace, Menlo, monospace";
  ctx.textBaseline = "middle";
  const step = tickStep(span);
  for (let ms = 0; ms <= span; ms += step) {
    ctx.fillStyle = colors.line;
    ctx.fillRect(x(ms), AXIS - 4, 1, layout.height - AXIS);
    ctx.fillStyle = colors.muted;
    ctx.fillText(`${(ms / 1000).toFixed(step < 1000 ? 1 : 0)}s`, x(ms) + 3, 8);
  }
  ctx.fillStyle = colors.muted;
  ctx.fillText("coordinator", 4, AXIS + COORDINATOR_LANE / 2);
  const firstScan = layout.rows.find((row) => row.task.role === "scan");
  const firstReducer = layout.rows.find((row) => row.task.role === "reducer");
  if (firstScan) ctx.fillText("scans", 4, firstScan.y + 5);
  if (firstReducer) ctx.fillText("reducers", 4, firstReducer.y + 5);

  const paint = (source: LiveJobView, alpha: number) => {
    ctx.globalAlpha = alpha;
    const now = source.elapsedMs;
    for (const phase of source.phases) {
      ctx.fillStyle = phase.stage === "finalize" ? colors.purple : colors.muted;
      ctx.fillRect(x(phase.startMs), AXIS + 2, Math.max(1.5, x(phase.endMs ?? now) - x(phase.startMs)), COORDINATOR_LANE - 4);
    }
    const byId = new Map(source.tasks.map((task) => [task.id, task] as const));
    for (const row of layout.rows) {
      const task = byId.get(row.task.id);
      if (!task || task.startMs == null) continue;
      // A pixel between lanes keeps neighbouring tasks apart.
      const top = row.y;
      const h = Math.max(1, layout.lane - 1);
      const end = task.endMs ?? now;
      const bar = (from: number, to: number, color: string) => {
        ctx.fillStyle = color;
        ctx.fillRect(x(from), top, Math.max(1, x(to) - x(from)), h);
      };
      const finished = task.state === "done" ? colors.ok : task.state === "failed" ? colors.bad : colors.accent;
      if (task.role === "reducer") {
        const ready = task.readyAtMs ?? end;
        bar(task.startMs, ready, colors.warn);
        if (task.readyAtMs != null) bar(task.readyAtMs, end, finished);
      } else {
        bar(task.startMs, end, finished);
      }
    }
    ctx.globalAlpha = 1;
  };
  if (ghost) paint(ghost, 0.16);
  paint(view, 1);

  const chosen = layout.rows.find((row) => row.task.id === selected);
  if (chosen) {
    ctx.strokeStyle = colors.ink;
    ctx.lineWidth = 1;
    ctx.strokeRect(GUTTER - 2, chosen.y - 0.5, width - GUTTER - 4, layout.lane + 1);
    ctx.fillStyle = colors.ink;
    ctx.fillText(taskName(chosen.task), 4, chosen.y + layout.lane / 2);
  }
  // The moment shown.
  ctx.fillStyle = colors.ink;
  ctx.fillRect(x(view.elapsedMs), AXIS - 4, 1.5, layout.height - AXIS);

  return (px, py) => {
    const row = layout.rows.find((candidate) => py >= candidate.y && py < candidate.y + Math.max(layout.lane, 3));
    return { task: row?.task ?? null, ms: ((px - GUTTER) / (width - GUTTER - 8)) * span };
  };
}
