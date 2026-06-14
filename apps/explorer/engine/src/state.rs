use crate::workspace::{
    AppRoute, CrateDetailPhase, CrateDetailSummary, PinnedCrate, SavedView, UndoHistory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasetKind {
    Crates,
    Dependencies,
    Categories,
    Maintainers,
    Trends,
    Hits,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaColumn {
    pub id: u32,
    pub name: String,
    pub physical_type: String,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetMeta {
    pub kind: DatasetKind,
    pub label: String,
    /// Bundled sample id when opened via `sample:{id}` (e.g. trends-crate-30d).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    #[serde(rename = "rows")]
    pub rows: u64,
    pub columns: u32,
    pub chunks: u32,
    pub rows_per_chunk: u64,
    pub open_ms: f64,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryMode {
    Search,
    Aggregate,
    Table,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterDraft {
    pub id: String,
    pub column: String,
    pub op: String,
    pub value: String,
    #[serde(default)]
    pub pinned: bool,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDraft {
    pub mode: QueryMode,
    pub search_text: String,
    pub search_column: String,
    pub filters: Vec<FilterDraft>,
    pub group_keys: Vec<String>,
    pub agg_column: String,
    pub select_columns: Vec<String>,
    pub top_k: u32,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoadPhase {
    Idle,
    Loading,
    Ready,
    Error,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryPhase {
    Idle,
    Warming,
    Running,
    Done,
    Error,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySummary {
    pub label: String,
    pub chart_hint: Option<String>,
    pub timing_ms: f64,
    pub workers: u32,
    pub rows_scanned: u64,
    pub result_count: u64,
    #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
    pub view: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
    pub aggregates: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppState {
    pub load_phase: LoadPhase,
    pub load_status: String,
    pub load_error: bool,
    pub url_input: String,
    pub data_drawer_open: bool,
    pub meta: Option<DatasetMeta>,
    pub schema: Vec<SchemaColumn>,
    pub column_names: Vec<String>,
    pub workers: u32,
    pub route: AppRoute,
    pub query_draft: QueryDraft,
    pub built_plan_json: Option<String>,
    pub query_phase: QueryPhase,
    pub query_status: String,
    pub query_error: bool,
    pub warm_ms: Option<f64>,
    pub result: Option<QuerySummary>,
    pub selected_crate: Option<String>,
    pub crate_detail: Option<CrateDetailSummary>,
    pub crate_detail_phase: CrateDetailPhase,
    pub crate_detail_status: String,
    pub pinned_crates: Vec<PinnedCrate>,
    pub saved_views: Vec<SavedView>,
    pub active_saved_view_id: Option<String>,
    #[serde(skip)]
    pub history: UndoHistory,
}

impl AppState {
    pub fn initial() -> Self {
        Self {
            load_phase: LoadPhase::Idle,
            load_status: "Open data settings to load a .wcol file".into(),
            load_error: false,
            url_input: String::new(),
            data_drawer_open: false,
            meta: None,
            schema: Vec::new(),
            column_names: Vec::new(),
            workers: 4,
            route: AppRoute::Explore,
            query_draft: default_query_draft(None),
            built_plan_json: None,
            query_phase: QueryPhase::Idle,
            query_status: String::new(),
            query_error: false,
            warm_ms: None,
            result: None,
            selected_crate: None,
            crate_detail: None,
            crate_detail_phase: CrateDetailPhase::Idle,
            crate_detail_status: String::new(),
            pinned_crates: Vec::new(),
            saved_views: Vec::new(),
            active_saved_view_id: None,
            history: UndoHistory::default(),
        }
    }
}

pub fn default_query_draft_for_load(
    kind: Option<DatasetKind>,
    dataset_id: Option<&str>,
    column_names: &[String],
) -> QueryDraft {
    if dataset_id == Some("trends-crate-30d") {
        return QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "crate_name".into(),
            filters: Vec::new(),
            group_keys: vec!["crate_name".into()],
            agg_column: "downloads".into(),
            select_columns: vec!["crate_name".into(), "downloads".into()],
            top_k: 25,
        };
    }
    if dataset_id == Some("trends-serde-versions") {
        return QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "version".into(),
            filters: Vec::new(),
            group_keys: vec!["version".into()],
            agg_column: "downloads".into(),
            select_columns: vec!["version".into(), "downloads".into()],
            top_k: 25,
        };
    }
    if kind == Some(DatasetKind::Trends) {
        let has_date = column_names.iter().any(|c| c == "date");
        let has_crate = column_names.iter().any(|c| c == "crate_name");
        let has_version = column_names.iter().any(|c| c == "version");
        if !has_date && has_crate {
            return default_query_draft_for_load(Some(DatasetKind::Trends), Some("trends-crate-30d"), column_names);
        }
        if !has_date && has_version && !has_crate {
            return default_query_draft_for_load(
                Some(DatasetKind::Trends),
                Some("trends-serde-versions"),
                column_names,
            );
        }
    }
    default_query_draft(kind)
}

pub fn default_query_draft(kind: Option<DatasetKind>) -> QueryDraft {
    match kind {
        Some(DatasetKind::Dependencies) => QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "parent_crate_name".into(),
            filters: Vec::new(),
            group_keys: vec!["dep_crate_name".into()],
            agg_column: "dependency_id".into(),
            select_columns: vec![
                "parent_crate_name".into(),
                "dep_crate_name".into(),
                "optional".into(),
            ],
            top_k: 25,
        },
        Some(DatasetKind::Categories) => QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "crate_name".into(),
            filters: Vec::new(),
            group_keys: vec!["category_name".into()],
            agg_column: "crate_downloads".into(),
            select_columns: vec![
                "crate_name".into(),
                "category_name".into(),
                "crate_downloads".into(),
            ],
            top_k: 25,
        },
        Some(DatasetKind::Maintainers) => QueryDraft {
            mode: QueryMode::Search,
            search_text: String::new(),
            search_column: "owner_login".into(),
            filters: Vec::new(),
            group_keys: vec!["crate_name".into()],
            agg_column: "crate_downloads".into(),
            select_columns: vec![
                "crate_name".into(),
                "owner_login".into(),
                "crate_downloads".into(),
            ],
            top_k: 25,
        },
        Some(DatasetKind::Trends) => QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "crate_name".into(),
            filters: Vec::new(),
            group_keys: vec!["crate_name".into()],
            agg_column: "downloads".into(),
            select_columns: vec![
                "date".into(),
                "crate_name".into(),
                "version".into(),
                "downloads".into(),
            ],
            top_k: 25,
        },
        Some(DatasetKind::Hits) => QueryDraft {
            mode: QueryMode::Search,
            search_text: String::new(),
            search_column: "URL".into(),
            filters: Vec::new(),
            group_keys: vec!["CounterID".into()],
            agg_column: "ResolutionWidth".into(),
            select_columns: vec![
                "CounterID".into(),
                "EventDate".into(),
                "URL".into(),
            ],
            top_k: 25,
        },
        _ => QueryDraft {
            mode: QueryMode::Aggregate,
            search_text: String::new(),
            search_column: "crate_name".into(),
            filters: Vec::new(),
            group_keys: vec!["crate_name".into()],
            agg_column: "downloads".into(),
            select_columns: vec![
                "crate_name".into(),
                "license".into(),
                "downloads".into(),
            ],
            top_k: 25,
        },
    }
}
