import type { WcolFile } from "../wcol-query.ts";
import type { DatasetId } from "../data/datasets.ts";
import type { DatasetKind } from "./types.ts";

/** Non-serializable wcol handle — kept outside reducer state. */
let current: WcolFile | null = null;
let kind: DatasetKind | null = null;
let datasetId: DatasetId | null = null;
let warmedWorkers: number | null = null;

export const fileStore = {
  get(): WcolFile | null {
    return current;
  },
  getKind(): DatasetKind | null {
    return kind;
  },
  getDatasetId(): DatasetId | null {
    return datasetId;
  },
  set(file: WcolFile, datasetKind: DatasetKind, loadedDatasetId?: DatasetId | null): void {
    current = file;
    kind = datasetKind;
    datasetId = loadedDatasetId ?? null;
    warmedWorkers = null;
  },
  clear(): void {
    current = null;
    kind = null;
    datasetId = null;
    warmedWorkers = null;
  },
  async warmIfNeeded(workers: number): Promise<void> {
    const file = current;
    if (!file) return;
    if (warmedWorkers === workers) return;
    await file.warmWorkers(workers);
    warmedWorkers = workers;
  },
};
