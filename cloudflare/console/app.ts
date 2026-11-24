/**
 * The WCOL console. Submits a query, polls `GET /jobs/:id/live` for the job's snapshot, and
 * draws the view of it at the moment shown: live while the job runs, or any moment of a
 * finished job (replay). The view, its cost and its findings are the coordinator's own pure
 * functions (`live.ts`, `live-findings.ts`), bundled into this page. Without a token, Run
 * plays a recorded run of the chosen ClickBench query (`GET /recordings/:name.json`).
 */
import { findings } from "../src/coordinator/live-findings.ts";
import { inputAt, liveJobView, type LiveJobView, type LiveSnapshot } from "../src/coordinator/live.ts";
import { element, escape, palette, seconds, taskName } from "./format.ts";
import { loadRuns, recordRun, renderHistory, type Run } from "./history.ts";
import { renderMap } from "./map.ts";
import { renderPicker, type Recorded } from "./picker.ts";
import { drawMatrix, type MatrixHit, type MatrixMode } from "./matrix.ts";
import { hoverText, renderCost, renderDetail, renderFindings, renderFunnel, renderResult, renderStats, type Result } from "./panels.ts";
import { drawTimeline, type TimelineHit } from "./timeline.ts";

const POLL_MS = 400;
const FRAME_MS = 80;
/** The supported ClickBench queries, injected by the build (`scripts/clickbench-catalog.ts`). */
declare const __CLICKBENCH__: readonly { name: string; sql: string }[];
const DEFAULT_QUERY = "Q32";
/** ClickBench queries with a recorded run, injected by the build. */
declare const __RECORDED_QUERIES__: readonly Recorded[];

const ui = {
  sql: element<HTMLTextAreaElement>("sql"),
  picker: element<HTMLDivElement>("picker"),
  filter: element<HTMLInputElement>("filter"),
  reducers: element<HTMLSelectElement>("reducers"),
  instance: element<HTMLSelectElement>("instance"),
  count: element<HTMLSelectElement>("count"),
  scans: element<HTMLSelectElement>("scans"),
  key: element<HTMLInputElement>("key"),
  token: element<HTMLInputElement>("token"),
  run: element<HTMLButtonElement>("run"),
  state: element<HTMLSpanElement>("state"),
  elapsed: element<HTMLSpanElement>("elapsed"),
  job: element<HTMLSpanElement>("job"),
  reason: element<HTMLDivElement>("reason"),
  findings: element<HTMLUListElement>("findings"),
  error: element<HTMLDivElement>("error"),
  timeline: element<HTMLCanvasElement>("timeline"),
  play: element<HTMLButtonElement>("play"),
  slider: element<HTMLInputElement>("slider"),
  follow: element<HTMLButtonElement>("follow"),
  clock: element<HTMLSpanElement>("clock"),
  hover: element<HTMLDivElement>("hover"),
  matrix: element<HTMLCanvasElement>("matrix"),
  matrixMode: element<HTMLDivElement>("matrix-mode"),
  matrixHover: element<HTMLDivElement>("matrix-hover"),
  map: element<HTMLDivElement>("map"),
  funnel: element<HTMLDivElement>("funnel"),
  stats: element<HTMLDivElement>("stats"),
  detail: element<HTMLDivElement>("detail"),
  cost: element<HTMLDivElement>("cost"),
  history: element<HTMLDivElement>("history"),
  resultSection: element<HTMLElement>("result-section"),
  result: element<HTMLDivElement>("result"),
};

// --- state ------------------------------------------------------------------------------------

/** What is shown: the latest snapshot, and which moment of it. */
const view = {
  snapshot: null as LiveSnapshot | null,
  fetchedAt: 0,
  /** Follow the job's present; otherwise show `at` (epoch ms). */
  follow: true,
  at: 0,
  playing: false,
  playedFrom: { wall: 0, at: 0 },
  selected: null as number | null,
  matrixMode: "bytes" as MatrixMode,
  /** A synchronous job's rows come back on its `POST /jobs`; a later one's in its snapshot. */
  postedResult: null as Result | null,
  submitted: { sql: "", label: "" },
  recorded: new Set<string>(),
  runs: loadRuns() as Run[],
  /** A recorded run being played: its rows and findings wait for the end, like a live run's. */
  recording: null as { recordedAt: string; revealed: boolean } | null,
};

const ended = (snapshot: LiveSnapshot) => snapshot.state === "completed" || snapshot.state === "failed";

/** The moment to draw, epoch ms. A live job runs on between polls. */
function moment(snapshot: LiveSnapshot): number {
  if (view.playing) return Math.min(snapshot.now, view.playedFrom.at + (Date.now() - view.playedFrom.wall));
  if (!view.follow) return view.at;
  return ended(snapshot) ? snapshot.now : snapshot.now + (Date.now() - view.fetchedAt);
}

// --- token, kept per browser ------------------------------------------------------------------

const TOKEN_KEY = "wcol-console-token";
try {
  ui.token.value = localStorage.getItem(TOKEN_KEY) ?? "";
} catch {
  // Storage may be blocked; the field still works for this page.
}
ui.token.addEventListener("change", () => {
  try {
    localStorage.setItem(TOKEN_KEY, ui.token.value);
  } catch {
    // As above.
  }
});
const authHeaders = (): Record<string, string> => (ui.token.value ? { authorization: `Bearer ${ui.token.value}` } : {});

// --- drawing ------------------------------------------------------------------------------------

let timelineHit: TimelineHit | null = null;
let matrixHit: MatrixHit | null = null;
let finalView: LiveJobView | null = null;
let lastFrame = 0;

function frame(time: number): void {
  requestAnimationFrame(frame);
  const snapshot = view.snapshot;
  if (!snapshot || time - lastFrame < FRAME_MS) return;
  lastFrame = time;
  const at = moment(snapshot);
  if (view.playing && at >= snapshot.now) {
    // Rest on the end; otherwise the view would fall back to the moment shown before playing.
    view.at = snapshot.now;
    setPlaying(false);
  }
  if (view.recording && !view.recording.revealed && at >= snapshot.now) {
    view.recording.revealed = true;
    onSnapshot(snapshot);
  }
  const current = liveJobView(inputAt(snapshot, at));
  // A live job runs on between polls: the clock keeps time, the view waits for the next poll.
  const shownMs = Math.max(current.elapsedMs, at - snapshot.startedAt);
  const replaying = !view.follow || view.playing;
  const durationMs = Math.max(snapshot.now - snapshot.startedAt, shownMs, 1);
  const colors = palette();
  const hasTasks = current.tasks.length > 1;

  ui.elapsed.textContent = seconds(ended(snapshot) && !replaying ? snapshot.now - snapshot.startedAt : shownMs);
  ui.clock.textContent = `${seconds(Math.min(at, snapshot.now) - snapshot.startedAt)} / ${seconds(snapshot.now - snapshot.startedAt)}`;
  ui.slider.max = String(snapshot.now - snapshot.startedAt);
  if (!replaying || view.playing) ui.slider.value = String(Math.min(at, snapshot.now) - snapshot.startedAt);
  ui.follow.classList.toggle("active", view.follow && !view.playing);

  if (hasTasks) {
    timelineHit = drawTimeline(ui.timeline, current, replaying ? finalView : null, durationMs, view.selected, colors);
    matrixHit = current.edges.length ? drawMatrix(ui.matrix, current, view.matrixMode, colors) : null;
  }
  renderStats(ui.stats, current);
  renderCost(ui.cost, current);
  renderDetail(ui.detail, current, view.selected);
  renderMap(ui.map, current);
  const result = (snapshot.result as Result | null) ?? view.postedResult;
  renderFunnel(ui.funnel, current, result?.rows ? result.rows.length : null);
}
requestAnimationFrame(frame);

/** Panels drawn once per snapshot rather than per frame. */
function onSnapshot(snapshot: LiveSnapshot): void {
  view.snapshot = snapshot;
  view.fetchedAt = Date.now();
  const playing = view.recording !== null && !view.recording.revealed;
  ui.state.textContent = playing ? "running" : snapshot.state;
  ui.state.className = `pill ${playing ? "running" : snapshot.state}`;
  ui.job.textContent = view.recording
    ? `recorded run · ${new Date(view.recording.recordedAt).toLocaleString()}`
    : `${snapshot.jobId}${snapshot.attempt > 1 ? ` · attempt ${snapshot.attempt}` : ""}`;
  const reducerCount = snapshot.graph.tasks.filter((task) => task.role === "reducer").length;
  const where = snapshot.reducers.runtime === "container" ? `containers (${snapshot.reducers.instance ?? "?"})` : "Durable Objects";
  ui.reason.textContent =
    snapshot.graph.tasks.length === 0
      ? ""
      : reducerCount > 0
        ? `${reducerCount} reducers on ${where}: ${snapshot.reducerReason ?? "default"}.`
        : "No reducers: scans send partial results straight to the coordinator.";
  ui.error.classList.toggle("hidden", !snapshot.error);
  ui.error.textContent = snapshot.error ?? "";
  if (playing) return;
  if (ended(snapshot)) {
    finalView = liveJobView(inputAt(snapshot, snapshot.now));
    renderFindings(ui.findings, findings(finalView));
    if (!view.recorded.has(snapshot.jobId) && snapshot.state === "completed" && view.submitted.sql) {
      view.recorded.add(snapshot.jobId);
      view.runs = recordRun(finalView, view.submitted.sql, view.submitted.label);
    }
  }
  renderResult(ui.resultSection, ui.result, (snapshot.result as Result | null) ?? view.postedResult);
  renderHistory(ui.history, view.runs, ui.sql.value.trim(), (jobId) => void open(jobId));
}

// --- replay controls ----------------------------------------------------------------------------

function setPlaying(playing: boolean): void {
  view.playing = playing;
  ui.play.textContent = playing ? "Pause" : "Replay";
}

ui.play.addEventListener("click", () => {
  const snapshot = view.snapshot;
  if (!snapshot) return;
  if (view.playing) {
    view.at = moment(snapshot);
    view.follow = false;
    setPlaying(false);
    return;
  }
  const from = !view.follow && view.at < snapshot.now ? view.at : snapshot.startedAt;
  view.playedFrom = { wall: Date.now(), at: from };
  view.follow = false;
  setPlaying(true);
});
ui.slider.addEventListener("input", () => {
  if (!view.snapshot) return;
  setPlaying(false);
  view.follow = false;
  view.at = view.snapshot.startedAt + Number(ui.slider.value);
});
ui.follow.addEventListener("click", () => {
  setPlaying(false);
  view.follow = true;
});

// --- hovering and selecting -------------------------------------------------------------------

const local = (event: MouseEvent, canvas: HTMLCanvasElement) => {
  const box = canvas.getBoundingClientRect();
  return [event.clientX - box.left, event.clientY - box.top] as const;
};
ui.timeline.addEventListener("mousemove", (event) => {
  const hit = timelineHit?.(...local(event, ui.timeline));
  ui.hover.textContent = hit?.task ? hoverText(hit.task) : "";
});
ui.timeline.addEventListener("click", (event) => {
  const hit = timelineHit?.(...local(event, ui.timeline));
  if (hit?.task) view.selected = hit.task.id;
});
ui.matrix.addEventListener("mousemove", (event) => {
  const hit = matrixHit?.(...local(event, ui.matrix));
  ui.matrixHover.textContent = hit
    ? `${taskName(hit.from)} → ${taskName(hit.to)} · ${hit.edge.state}${hit.edge.bytes ? ` · ${(hit.edge.bytes / 1e6).toFixed(2)} MB` : ""}${
        hit.edge.endMs != null ? ` · ${seconds(hit.edge.startMs ?? 0)} → ${seconds(hit.edge.endMs)}` : ""
      }`
    : "";
});
ui.matrix.addEventListener("click", (event) => {
  const hit = matrixHit?.(...local(event, ui.matrix));
  if (hit) view.selected = event.shiftKey ? hit.to.id : hit.from.id;
});
ui.matrixMode.querySelectorAll<HTMLButtonElement>("button").forEach((button) => {
  button.addEventListener("click", () => {
    view.matrixMode = button.dataset.mode === "finish" ? "finish" : "bytes";
    ui.matrixMode.querySelectorAll("button").forEach((other) => other.classList.toggle("active", other === button));
  });
});

// --- running a query ----------------------------------------------------------------------------

/**
 * Poll the snapshot until the job ends. A job short enough to run inside its `POST /jobs` is
 * polled while that request is open; until the coordinator has its record it says 404.
 */
async function poll(jobId: string, posted: { done: boolean }): Promise<void> {
  for (;;) {
    const response = await fetch(`/jobs/${encodeURIComponent(jobId)}/live`, { headers: authHeaders() });
    if (response.status === 404 && !posted.done) {
      await new Promise((resolve) => setTimeout(resolve, POLL_MS));
      continue;
    }
    if (!response.ok) throw new Error(`live view: ${response.status} ${await response.text()}`);
    const snapshot = (await response.json()) as LiveSnapshot;
    onSnapshot(snapshot);
    if (ended(snapshot)) return;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
}

function reset(): void {
  view.snapshot = null;
  view.follow = true;
  view.selected = null;
  view.postedResult = null;
  view.submitted = { sql: "", label: "" };
  view.recording = null;
  finalView = null;
  setPlaying(false);
  ui.error.classList.add("hidden");
  ui.resultSection.classList.add("hidden");
  ui.findings.classList.add("hidden");
  ui.reason.textContent = "";
  for (const canvas of [ui.timeline, ui.matrix]) canvas.getContext("2d")?.clearRect(0, 0, canvas.width, canvas.height);
}

function showError(error: unknown): void {
  ui.state.textContent = "error";
  ui.state.className = "pill failed";
  ui.error.classList.remove("hidden");
  ui.error.textContent = error instanceof Error ? error.message : String(error);
}

function jobBody(): Record<string, unknown> {
  const count = ui.count.value ? Number(ui.count.value) : undefined;
  const scans = ui.scans.value ? Number(ui.scans.value) : undefined;
  return {
    sources: { hits: { bucket: "output", key: ui.key.value.trim() } },
    sql: ui.sql.value.trim().replace(/;$/, ""),
    output: { format: "json", rowCap: 1000 },
    reducers: ui.reducers.value,
    reducerInstance: ui.instance.value,
    ...(count ? { hashReducers: count } : {}),
    ...(scans ? { maxNodes: scans } : {}),
  };
}

/** The recorded ClickBench query whose SQL is in the editor, if any. */
function recordedQuery(): string | undefined {
  const sql = ui.sql.value.trim().replace(/;$/, "");
  const preset = __CLICKBENCH__.find((query) => query.sql === sql);
  return preset && __RECORDED_QUERIES__.some((run) => run.name === preset.name) ? preset.name : undefined;
}

/** Play a recorded run from its start, at the speed it ran. */
async function replayRecording(name: string): Promise<void> {
  reset();
  try {
    const response = await fetch(`/recordings/${encodeURIComponent(name)}.json`);
    if (!response.ok) throw new Error(`recording ${name}: ${response.status}`);
    const recording = (await response.json()) as { recordedAt: string; snapshot: LiveSnapshot; result: Result | null };
    view.recording = { recordedAt: recording.recordedAt, revealed: false };
    view.postedResult = recording.result;
    view.playedFrom = { wall: Date.now(), at: recording.snapshot.startedAt };
    view.follow = false;
    setPlaying(true);
    onSnapshot(recording.snapshot);
  } catch (error) {
    showError(error);
  }
}

async function run(): Promise<void> {
  if (!ui.token.value) {
    const recorded = recordedQuery();
    if (recorded) return replayRecording(recorded);
    return showError(new Error("Live runs need an API token. The recorded queries at the top of the list replay real runs without one."));
  }
  ui.run.disabled = true;
  reset();
  ui.state.textContent = "submitting";
  ui.state.className = "pill";
  const jobId = `console-${crypto.randomUUID()}`;
  const sql = ui.sql.value.trim();
  const preset = __CLICKBENCH__.find((query) => query.sql === sql.replace(/;$/, ""));
  view.submitted = { sql, label: preset?.name ?? `${sql.slice(0, 24)}…` };
  const posted = { done: false };
  try {
    const submit = fetch("/jobs", {
      method: "POST",
      headers: { "content-type": "application/json", ...authHeaders() },
      body: JSON.stringify({ ...jobBody(), jobId }),
    })
      .then(async (response) => {
        const body = (await response.json()) as Result & { error?: string };
        if (!response.ok) throw new Error(body.error ?? `submit: ${response.status}`);
        if (body.rows) view.postedResult = body;
      })
      .finally(() => {
        posted.done = true;
      });
    // Both must finish: the POST carries a short job's rows, the poll its final snapshot.
    await Promise.all([submit, poll(jobId, posted)]);
    if (view.snapshot) onSnapshot(view.snapshot);
  } catch (error) {
    showError(error);
  } finally {
    ui.run.disabled = false;
  }
}

/** Reopen an earlier run from the history, if its coordinator still holds it. */
async function open(jobId: string): Promise<void> {
  reset();
  try {
    const response = await fetch(`/jobs/${encodeURIComponent(jobId)}/live`, { headers: authHeaders() });
    if (!response.ok) throw new Error(`${jobId}: ${response.status} ${await response.text()}`);
    onSnapshot((await response.json()) as LiveSnapshot);
    if (view.snapshot && view.snapshot.graph.tasks.length === 0) {
      ui.reason.innerHTML = `<span class="muted">${escape("The coordinator no longer holds this run's timeline; only its record remains.")}</span>`;
    }
  } catch (error) {
    showError(error);
  }
}

// --- query controls -----------------------------------------------------------------------------

/** The catalog query whose SQL is in the editor, if the editor holds one unchanged. */
const editorQuery = () => __CLICKBENCH__.find((query) => query.sql === ui.sql.value.trim().replace(/;$/, ""))?.name;

function refreshQueryControls(): void {
  renderPicker(
    ui.picker,
    { queries: __CLICKBENCH__, recorded: __RECORDED_QUERIES__, selected: editorQuery(), filter: ui.filter.value.trim(), hasToken: Boolean(ui.token.value) },
    pick,
  );
  ui.run.textContent = !ui.token.value && recordedQuery() ? "Replay run" : "Run";
  renderHistory(ui.history, view.runs, ui.sql.value.trim(), (jobId) => void open(jobId));
}

function pick(name: string): void {
  const query = __CLICKBENCH__.find((candidate) => candidate.name === name);
  if (!query) return;
  ui.sql.value = query.sql;
  refreshQueryControls();
}

// A visitor without a token starts on a recorded run, so Run shows something at once.
const start = __RECORDED_QUERIES__.some((run) => run.name === DEFAULT_QUERY) ? DEFAULT_QUERY : __RECORDED_QUERIES__[0]?.name;
ui.sql.value = __CLICKBENCH__.find((query) => query.name === (start ?? DEFAULT_QUERY))?.sql ?? "";
ui.filter.addEventListener("input", refreshQueryControls);
ui.sql.addEventListener("input", refreshQueryControls);
ui.token.addEventListener("input", refreshQueryControls);
ui.run.addEventListener("click", () => void run());
ui.sql.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) void run();
});
refreshQueryControls();
