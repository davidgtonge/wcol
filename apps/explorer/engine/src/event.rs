use crate::state::{DatasetMeta, QuerySummary, SchemaColumn};
use crate::workspace::{AppRoute, CrateDetailSummary};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(
    feature = "typegen",
    ts(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")
)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AppEvent {
    LoadSample,
    LoadDataset {
        id: String,
    },
    LoadUrl,
    UrlChanged { url: String },
    DataDrawerSet { open: bool },
    FileOpened {
        meta: DatasetMeta,
        schema: Vec<SchemaColumn>,
        #[serde(rename = "columnNames")]
        #[cfg_attr(feature = "typegen", ts(rename = "columnNames"))]
        column_names: Vec<String>,
    },
    FileOpenFailed { message: String },
    QueryDraftPatch {
        #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
        patch: Value,
    },
    FilterAdd,
    FilterAddPrefilled {
        column: String,
        op: String,
        value: String,
    },
    FilterRemove { id: String },
    FilterPatch {
        id: String,
        #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
        patch: Value,
    },
    PresetSelected { id: String },
    WorkersChanged { workers: u32 },
    WarmWorkers,
    WorkersWarmed { ms: f64 },
    RunQuery,
    QueryDone { result: QuerySummary },
    QueryFailed { message: String },
    RouteSet { route: AppRoute },
    CrateSelect { name: String },
    CrateDetailClose,
    CratePin { name: String },
    CrateUnpin { name: String },
    CrateDetailDone { detail: CrateDetailSummary },
    CrateDetailFailed { message: String },
    SavedViewSave { name: String },
    SavedViewApply { id: String },
    SavedViewRemove { id: String },
    FilterPinSet { id: String, pinned: bool },
    Undo,
    Redo,
    WorkspaceHydrate { hash: String },
}
