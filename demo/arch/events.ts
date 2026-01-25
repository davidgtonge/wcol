import type { AppEvent } from "../protocol/types.ts";
import type { QueryPlan } from "../query/plan-types.ts";
import type { ChartHint } from "./types.ts";

/** UI events — `LOAD_FILE` uses a worker side channel (not CBOR). */
export type Event = AppEvent | { type: "LOAD_FILE"; file: File };

export type LoadPanelEvent = Extract<
  Event,
  { type: "LOAD_SAMPLE" | "LOAD_DATASET" | "LOAD_FILE" | "LOAD_URL" | "URL_CHANGED" }
>;

export type DataDrawerEvent =
  | LoadPanelEvent
  | Extract<Event, { type: "DATA_DRAWER_SET" | "WORKERS_CHANGED" | "WARM_WORKERS" }>;

export type QueryBuilderEvent = Extract<
  Event,
  {
    type:
      | "QUERY_DRAFT_PATCH"
      | "FILTER_ADD"
      | "FILTER_ADD_PREFILLED"
      | "FILTER_REMOVE"
      | "FILTER_PATCH"
      | "FILTER_PIN_SET"
      | "PRESET_SELECTED"
      | "RUN_QUERY";
  }
>;

export type WorkspaceEvent = Extract<
  Event,
  {
    type:
      | "ROUTE_SET"
      | "CRATE_SELECT"
      | "CRATE_DETAIL_CLOSE"
      | "CRATE_PIN"
      | "CRATE_UNPIN"
      | "SAVED_VIEW_SAVE"
      | "SAVED_VIEW_APPLY"
      | "SAVED_VIEW_REMOVE"
      | "UNDO"
      | "REDO"
      | "WORKSPACE_HYDRATE"
      | "DATA_DRAWER_SET";
  }
>;

export type ResultInteractionEvent =
  | { type: "CRATE_SELECT"; name: string }
  | { type: "CRATE_PIN"; name: string }
  | { type: "CRATE_COMPARE"; name: string };

export type QueryRunMeta = {
  plan: QueryPlan;
  label: string;
  chartHint?: ChartHint;
};
