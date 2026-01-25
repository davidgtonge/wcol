import { createSelector } from "reselect";
import type { ViewModel } from "../protocol/types.ts";
import type { ResultView } from "./types.ts";

export const isLoaded = (vm: ViewModel) => vm.loadPhase === "ready" && vm.meta !== null;

export const isRunning = (vm: ViewModel) =>
  vm.queryPhase === "running" || vm.queryPhase === "warming";

const selectLoadStatus = (vm: ViewModel) => vm.loadStatus;
const selectLoadError = (vm: ViewModel) => vm.loadError;
const selectLoadPhase = (vm: ViewModel) => vm.loadPhase;
const selectUrlInput = (vm: ViewModel) => vm.urlInput;
const selectDataDrawerOpen = (vm: ViewModel) => vm.dataDrawerOpen;
const selectSchema = (vm: ViewModel) => vm.schema;
const selectMeta = (vm: ViewModel) => vm.meta;
const selectWorkers = (vm: ViewModel) => vm.workers;
const selectWarmMs = (vm: ViewModel) => vm.warmMs;
const selectQueryPhase = (vm: ViewModel) => vm.queryPhase;
const selectQueryDraft = (vm: ViewModel) => vm.queryDraft;
const selectColumns = (vm: ViewModel) => vm.columns;
const selectPresets = (vm: ViewModel) => vm.presets;
const selectPlanPreview = (vm: ViewModel) => vm.planPreview;
const selectQueryStatus = (vm: ViewModel) => vm.queryStatus;
const selectQueryError = (vm: ViewModel) => vm.queryError;
const selectResult = (vm: ViewModel) => vm.result;
const selectExplore = (vm: ViewModel) => vm.explore;

export const selectExploreRoute = createSelector([selectExplore], (explore) => explore.route);

export const selectResultsInteraction = createSelector([selectMeta], (meta) => ({
  cratesDataset: meta?.kind === "crates",
}));

export const selectLoadInput = createSelector(
  [selectLoadStatus, selectLoadError, selectLoadPhase, selectUrlInput],
  (status, isError, loadPhase, url) => ({
    status,
    isError,
    loading: loadPhase === "loading",
    url,
    compact: true as const,
  })
);

export const selectDatasetInput = createSelector([selectSchema], (schema) => ({
  schema,
}));

export const selectDataDrawerInput = createSelector(
  [
    selectDataDrawerOpen,
    selectLoadInput,
    selectSchema,
    selectMeta,
    selectWorkers,
    selectWarmMs,
    selectQueryPhase,
  ],
  (open, load, schema, meta, workers, warmMs, queryPhase) => ({
    open,
    load,
    dataset: schema.length ? { schema } : null,
    meta,
    workers,
    warmStatus:
      warmMs != null && queryPhase !== "warming"
        ? `Pool warmed in ${warmMs.toFixed(0)} ms`
        : queryPhase === "warming"
          ? "Warming…"
          : "",
  })
);

export const selectExploreSidebarInput = createSelector(
  [
    selectQueryDraft,
    selectColumns,
    selectPresets,
    selectPlanPreview,
    selectQueryStatus,
    selectQueryError,
    selectQueryPhase,
    selectLoadPhase,
    selectMeta,
    selectExplore,
  ],
  (draft, columns, presets, planPreview, status, isError, queryPhase, loadPhase, meta, explore) => ({
    draft,
    columns,
    presets,
    planPreview,
    status,
    isError,
    running: queryPhase === "running" || queryPhase === "warming",
    ready: loadPhase === "ready" && meta !== null,
    pinnedFilterCount: explore.pinnedFilterCount,
  })
);

export const selectResultInput = createSelector([selectResult], (result) =>
  result
    ? {
        view: result.view,
        timingMs: result.timingMs,
        workers: result.workers,
        rowsScanned: result.rowsScanned,
        resultCount: result.resultCount,
      }
    : null
);

export const selectResultsPanelInput = createSelector(
  [
    selectResultInput,
    selectResult,
    selectQueryDraft,
    selectQueryPhase,
    selectMeta,
    selectWorkers,
    selectWarmMs,
    selectResultsInteraction,
    selectExplore,
  ],
  (result, rawResult, draft, queryPhase, meta, workers, warmMs, interaction, explore) => {
    const running = queryPhase === "running" || queryPhase === "warming";
    const totalRows = meta ? Number(meta.rows) : 0;
    const timing =
      result || running
        ? {
            timingMs: result?.timingMs ?? 0,
            rowsScanned: result?.rowsScanned ?? totalRows,
            resultCount: result?.resultCount ?? 0,
            workers: result?.workers ?? workers,
            openMs: meta?.openMs,
            warmMs,
          }
        : null;

    const view = (result?.view ?? null) as ResultView | null;

    return {
      draft,
      datasetKind: meta?.kind ?? null,
      resultLabel: rawResult?.label ?? null,
      result,
      view: view ?? null,
      timing,
      loading: running,
      cratesInteractive: interaction.cratesDataset,
      selectedCrate: explore.selectedCrate,
    };
  }
);

export const selectDemoChromeInput = createSelector(
  [selectLoadPhase, selectLoadStatus, selectLoadError, selectMeta],
  (loadPhase, loadStatus, loadError, meta) => ({
    loadPhase,
    loadStatus,
    loadError,
    meta,
    loaded: loadPhase === "ready" && meta !== null,
  })
);

export const selectIsLoaded = createSelector(
  [selectLoadPhase, selectMeta],
  (loadPhase, meta) => loadPhase === "ready" && meta !== null
);

export const selectWorkspaceChromeInput = createSelector(
  [selectExplore, selectMeta],
  (explore, meta) => ({
    route: explore.route,
    canUndo: explore.canUndo,
    canRedo: explore.canRedo,
    shareableUrl: explore.shareableUrl,
    savedViews: explore.savedViews,
    pinnedCrates: explore.pinnedCrates,
    datasetKind: meta?.kind ?? null,
  })
);

export const selectCrateDetailInput = createSelector([selectExplore], (explore) => ({
  selectedCrate: explore.selectedCrate,
  detail: explore.crateDetail,
  phase: explore.crateDetailPhase,
  status: explore.crateDetailStatus,
}));
