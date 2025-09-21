use crate::presets::presets_for_dataset;
use crate::query_plan::plan_preview;
use crate::state::AppState;
use crate::workspace::{
    encode_shareable_url, pinned_filter_count, AppRoute, CrateDetailPhase, CrateDetailSummary,
    PinnedCrate,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresetOption {
    pub id: String,
    pub label: String,
    pub description: String,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedViewOption {
    pub id: String,
    pub name: String,
    pub active: bool,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExploreViewModel {
    pub route: AppRoute,
    pub shareable_url: String,
    pub can_undo: bool,
    pub can_redo: bool,
    pub selected_crate: Option<String>,
    pub crate_detail: Option<CrateDetailSummary>,
    pub crate_detail_phase: CrateDetailPhase,
    pub crate_detail_status: String,
    pub pinned_crates: Vec<PinnedCrate>,
    pub saved_views: Vec<SavedViewOption>,
    pub pinned_filter_count: u32,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewModel {
    pub load_phase: crate::state::LoadPhase,
    pub load_status: String,
    pub load_error: bool,
    pub url_input: String,
    pub data_drawer_open: bool,
    pub meta: Option<crate::state::DatasetMeta>,
    pub schema: Vec<crate::state::SchemaColumn>,
    pub workers: u32,
    pub query_draft: crate::state::QueryDraft,
    pub plan_preview: String,
    pub query_phase: crate::state::QueryPhase,
    pub query_status: String,
    pub query_error: bool,
    pub warm_ms: Option<f64>,
    pub result: Option<crate::state::QuerySummary>,
    pub columns: Vec<String>,
    pub presets: Vec<PresetOption>,
    pub explore: ExploreViewModel,
}

pub fn select_view_model(state: &AppState) -> ViewModel {
    let kind = state.meta.as_ref().map(|m| m.kind);
    let dataset_id = state.meta.as_ref().and_then(|m| m.dataset_id.as_deref());
    ViewModel {
        load_phase: state.load_phase,
        load_status: state.load_status.clone(),
        load_error: state.load_error,
        url_input: state.url_input.clone(),
        data_drawer_open: state.data_drawer_open,
        meta: state.meta.clone(),
        schema: state.schema.clone(),
        workers: state.workers,
        query_draft: state.query_draft.clone(),
        plan_preview: state
            .built_plan_json
            .clone()
            .unwrap_or_else(|| plan_preview(&state.query_draft)),
        query_phase: state.query_phase,
        query_status: state.query_status.clone(),
        query_error: state.query_error,
        warm_ms: state.warm_ms,
        result: state.result.clone(),
        columns: state.column_names.clone(),
        presets: presets_for_dataset(kind, dataset_id)
            .iter()
            .map(|p| PresetOption {
            id: p.id.into(),
            label: p.label.into(),
            description: p.description.into(),
        }).collect(),
        explore: ExploreViewModel {
            route: state.route,
            shareable_url: encode_shareable_url(state),
            can_undo: state.history.can_undo(),
            can_redo: state.history.can_redo(),
            selected_crate: state.selected_crate.clone(),
            crate_detail: state.crate_detail.clone(),
            crate_detail_phase: state.crate_detail_phase,
            crate_detail_status: state.crate_detail_status.clone(),
            pinned_crates: state.pinned_crates.clone(),
            saved_views: saved_view_options(state),
            pinned_filter_count: pinned_filter_count(&state.query_draft.filters),
        },
    }
}

fn saved_view_options(state: &AppState) -> Vec<SavedViewOption> {
    state
        .saved_views
        .iter()
        .map(|v| SavedViewOption {
            id: v.id.clone(),
            name: v.name.clone(),
            active: state.active_saved_view_id.as_deref() == Some(v.id.as_str()),
        })
        .collect()
}

/// Normalize for stable JSON diff (bigint-safe numbers).
pub fn normalize_view_model(vm: &ViewModel) -> serde_json::Value {
    serde_json::to_value(vm).expect("view model serializes")
}
