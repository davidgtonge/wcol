import type { QueryPlan } from "../query/plan-types.ts";
import type {
  DatasetKind,
  DatasetMeta,
  FilterDraft,
  LoadPhase,
  QueryDraft,
  QueryMode,
  QueryPhase,
  SchemaColumn,
} from "../generated/engine-types.ts";

export type {
  DatasetKind,
  DatasetMeta,
  FilterDraft,
  LoadPhase,
  QueryDraft,
  QueryMode,
  QueryPhase,
  SchemaColumn,
};

export type ChartItem = {
  label: string;
  value: number;
  secondary?: string;
};

export type ResultView =
  | {
      kind: "bar-h";
      title: string;
      subtitle?: string;
      items: ChartItem[];
      valueLabel?: string;
    }
  | {
      kind: "bar-v";
      title: string;
      subtitle?: string;
      items: ChartItem[];
      valueLabel?: string;
    }
  | {
      kind: "grouped-bar";
      title: string;
      subtitle?: string;
      groups: string[];
      series: { name: string; values: number[] }[];
      valueLabel?: string;
    }
  | {
      kind: "table";
      title: string;
      columns: string[];
      rows: Record<string, string | number | boolean | null>[];
    }
  | {
      kind: "rows";
      title: string;
      rowIds: string[];
      rowCount: number;
    };

export type ChartHint = "bar-h" | "bar-v" | "grouped" | "table" | "rows";

export type QuerySummary = {
  label: string;
  chartHint?: ChartHint;
  timingMs: number;
  workers: number;
  /** Rows in the dataset scan (not the same as result row ids returned). */
  rowsScanned: number;
  /** Matching rows or groups in the result payload. */
  resultCount: number;
  view: ResultView;
  aggregates?: Record<string, unknown>;
};

export type PresetDef = {
  id: string;
  label: string;
  description: string;
  plan: QueryPlan;
  chartHint?: ChartHint;
};

export type AppState = {
  loadPhase: LoadPhase;
  loadStatus: string;
  loadError: boolean;
  urlInput: string;
  dataDrawerOpen: boolean;

  meta: DatasetMeta | null;
  schema: SchemaColumn[];
  /** Canonical only — projected to ViewModel.columns for the UI. */
  columnNames: string[];

  workers: number;
  queryDraft: QueryDraft;
  /** Canonical only — serialized plan when a query is built; projected to ViewModel.planPreview. */
  builtPlanJson: string | null;
  queryPhase: QueryPhase;
  queryStatus: string;
  queryError: boolean;
  warmMs: number | null;

  result: QuerySummary | null;
};
