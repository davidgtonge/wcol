import { WcolFile, HttpRangeSource, LocalFileSource } from "../wcol-query.ts";
import type { AppEvent, CrateDetailSummary, EffectCommand } from "../protocol/types.ts";
import { crateDetailPlan, summarizeCrateDetail } from "../data/crate-detail.ts";
import { fileStore } from "../arch/file-store.ts";
import { resolveProjectionCell } from "../data/resolve-values.ts";
import { detectDatasetKind, loadSchema, summarizeResult } from "../data/summarize.ts";
import { applyHandler, errMsg, type HandlerMap } from "../arch/typed.ts";
import type { DatasetMeta } from "../arch/types.ts";
import { DEFAULT_TOP_K } from "../query/constants.ts";
import { buildQueryPlan } from "../query/build-plan.ts";
import { presetById } from "../data/presets.ts";
import { presetPlan } from "../query/preset-plan.ts";
import type { QueryPlan } from "../query/plan-types.ts";
import { datasetById, parseSampleSourceToken, resolveSampleSource } from "../data/datasets.ts";

/** Opaque byte source handle — resolved in the worker Wasm runtime. */
export type ByteSource = unknown;
export type OpenSource = "sample" | ByteSource | File | string;

export type WorkerEffect =
  | Exclude<EffectCommand, { type: "OPEN_SOURCE" }>
  | { type: "OPEN_SOURCE"; source: OpenSource; label: string };

const resolveSource = (source: OpenSource) => {
  if (typeof source === "string") {
    const sampleId = parseSampleSourceToken(source);
    if (sampleId) {
      const resolved = resolveSampleSource(sampleId);
      const ds = datasetById(sampleId);
      return {
        byteSource: resolved.byteSource,
        label: ds?.label ?? resolved.label,
        datasetId: sampleId,
      };
    }
    if (source.startsWith("http://") || source.startsWith("https://")) {
      return { byteSource: new HttpRangeSource(source), label: source, datasetId: null };
    }
  }
  if (source instanceof File) {
    return { byteSource: new LocalFileSource(source), label: source.name, datasetId: null };
  }
  if (typeof source === "string") {
    return { byteSource: new HttpRangeSource(source), label: source, datasetId: null };
  }
  return { byteSource: source, label: "remote", datasetId: null };
};

const openSource = async (source: OpenSource, label: string): Promise<AppEvent[]> => {
  const t0 = performance.now();
  try {
    const { byteSource, label: resolvedLabel, datasetId: loadedDatasetId } = resolveSource(source);
    fileStore.clear();
    const file = await WcolFile.open(byteSource);
    const kind = await detectDatasetKind(file);
    fileStore.set(file, kind, loadedDatasetId);
    const { schema, columnNames } = await loadSchema(file);
    const h = file.header;
    const meta: DatasetMeta = {
      kind,
      label: label || resolvedLabel,
      datasetId: loadedDatasetId ?? undefined,
      rows: h.totalRows,
      columns: h.ncols,
      chunks: h.nchunks,
      rowsPerChunk: h.rowsPerChunk,
      openMs: performance.now() - t0,
    };
    return [{ type: "FILE_OPENED", meta, schema, columnNames }];
  } catch (err) {
    fileStore.clear();
    const message = errMsg(err);
    const hint =
      message.includes("fetch") || message.includes("404")
        ? `${message} — run npm run prepare:datasets to stage demo/data/*.wcol`
        : message;
    return [{ type: "FILE_OPEN_FAILED", message: hint }];
  }
};

export const effectHandlers: HandlerMap<WorkerEffect, Promise<AppEvent[]>> = {
  OPEN_SOURCE: ({ source, label }) => openSource(source, label),

  WARM_WORKERS: async ({ workers }) => {
    const file = fileStore.get();
    if (!file) return [];
    const t0 = performance.now();
    await fileStore.warmIfNeeded(workers);
    return [{ type: "WORKERS_WARMED", ms: performance.now() - t0 }];
  },

  RUN_QUERY_DRAFT: async ({ draft, workers, label, chartHint }) => {
    const plan = buildQueryPlan(draft);
    return effectHandlers.RUN_QUERY({ type: "RUN_QUERY", plan, workers, label, chartHint });
  },

  RUN_PRESET: async ({ id, workers }) => {
    const file = fileStore.get();
    if (!file) return [{ type: "QUERY_FAILED", message: "No file loaded" }];
    const kind = fileStore.getKind();
    const preset = presetById(kind, id, fileStore.getDatasetId());
    if (!preset) return [{ type: "QUERY_FAILED", message: `Unknown preset: ${id}` }];
    return effectHandlers.RUN_QUERY({
      type: "RUN_QUERY",
      plan: presetPlan(preset),
      workers,
      label: preset.label,
      chartHint: preset.chartHint,
    });
  },

  LOAD_CRATE_DETAIL: async ({ crateName, workers }) => {
    const file = fileStore.get();
    if (!file) return [{ type: "CRATE_DETAIL_FAILED", message: "No file loaded" }];
    try {
      await fileStore.warmIfNeeded(workers);
      const plan = crateDetailPlan(crateName);
      const result = await file.query({ ...plan }, { workers });
      if (!result.projection) {
        return [{ type: "CRATE_DETAIL_FAILED", message: "No version rows for crate" }];
      }
      const rows: Record<string, string | number | boolean | null>[] = [];
      const proj = result.projection;
      const n = Math.min(result.rows.length, 100);
      for (let r = 0; r < n; r += 1) {
        const row: Record<string, string | number | boolean | null> = {};
        for (let c = 0; c < proj.columns.length; c += 1) {
          const meta = proj.columns[c];
          const col = proj.data[c];
          if (col.nulls[r] === 0) {
            row[meta.name] = null;
            continue;
          }
          row[meta.name] = await resolveProjectionCell(file, meta, col, r);
        }
        rows.push(row);
      }
      const detail: CrateDetailSummary = summarizeCrateDetail(crateName, rows);
      return [{ type: "CRATE_DETAIL_DONE", detail }];
    } catch (err) {
      return [{ type: "CRATE_DETAIL_FAILED", message: errMsg(err) }];
    }
  },

  RUN_QUERY: async ({ plan, workers, label, chartHint }) => {
    const file = fileStore.get();
    if (!file) return [{ type: "QUERY_FAILED", message: "No file loaded" }];
    try {
      await fileStore.warmIfNeeded(workers);
      const t0 = performance.now();
      const result = await file.query({ ...(plan as QueryPlan) }, { workers });
      const rowsScanned = Number(file.header.totalRows);
      const summary = await summarizeResult(
        file,
        result,
        { label, chartHint: chartHint ?? undefined, topK: (plan as QueryPlan).limit ?? DEFAULT_TOP_K, rowsScanned },
        performance.now() - t0,
        workers,
      );
      return [{ type: "QUERY_DONE", result: summary }];
    } catch (err) {
      return [{ type: "QUERY_FAILED", message: errMsg(err) }];
    }
  },
};

export const runEffect = async (effect: WorkerEffect): Promise<{ events: AppEvent[] }> => ({
  events: await applyHandler(effectHandlers, effect),
});
