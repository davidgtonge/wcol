use crate::effect::EffectCommand;
use crate::event::AppEvent;
use crate::presets::{default_preset_id, preset_exists};
use crate::query_plan::{build_query_plan, plan_preview};
use crate::state::{
    default_query_draft_for_load, AppState, FilterDraft, LoadPhase, QueryDraft, QueryMode,
    QueryPhase,
};
use crate::workspace::{
    apply_url_state, decode_shareable_url, new_saved_view_id, CrateDetailPhase,
    PinnedCrate, SavedView, UndoHistory,
};
use serde::Deserialize;

pub struct Transition {
    pub effects: Vec<EffectCommand>,
}

pub fn reduce(state: &mut AppState, event: &AppEvent) -> Transition {
    match event {
        AppEvent::LoadSample => loading(
            state,
            "Fetching dataset…",
            vec![EffectCommand::OpenSource {
                source: "sample:crates-versions".into(),
                label: "crates_versions.wcol".into(),
            }],
        ),
        AppEvent::LoadDataset { id } => loading(
            state,
            "Fetching dataset…",
            vec![EffectCommand::OpenSource {
                source: format!("sample:{id}"),
                label: id.clone(),
            }],
        ),
        AppEvent::LoadUrl => {
            let url = state.url_input.trim().to_string();
            if url.is_empty() {
                state.load_status = "Enter a URL".into();
                state.load_error = true;
                Transition { effects: vec![] }
            } else {
                loading(
                    state,
                    "Opening…",
                    vec![EffectCommand::OpenSource {
                        source: url.clone(),
                        label: url,
                    }],
                )
            }
        }
        AppEvent::UrlChanged { url } => {
            state.url_input = url.clone();
            Transition { effects: vec![] }
        }
        AppEvent::DataDrawerSet { open } => {
            state.data_drawer_open = *open;
            Transition { effects: vec![] }
        }
        AppEvent::FileOpenFailed { message } => {
            let url = state.url_input.clone();
            *state = AppState::initial();
            state.url_input = url;
            state.load_phase = LoadPhase::Error;
            state.load_status = message.clone();
            state.load_error = true;
            state.data_drawer_open = true;
            Transition { effects: vec![] }
        }
        AppEvent::FileOpened {
            meta,
            schema,
            column_names,
        } => {
            let kind = Some(meta.kind);
            let dataset_id = meta.dataset_id.as_deref();
            let draft = default_query_draft_for_load(kind, dataset_id, &column_names);
            state.load_phase = LoadPhase::Ready;
            state.load_status = format!("Loaded in {:.1} ms", meta.open_ms);
            state.load_error = false;
            state.meta = Some(meta.clone());
            state.schema = schema.clone();
            state.column_names = column_names.clone();
            state.data_drawer_open = false;
            state.query_phase = QueryPhase::Running;
            state.query_status = "Running…".into();
            state.query_error = false;
            state.warm_ms = None;
            state.result = None;
            state.history = UndoHistory::default();
            sync_draft(state, draft);
            let mut effects = vec![EffectCommand::WarmWorkers {
                workers: state.workers,
            }];
            let preset_id = default_preset_id(kind, dataset_id);
            effects.extend(run_preset_fx(state, preset_id).effects);
            Transition { effects }
        }
        AppEvent::QueryDraftPatch { patch } => {
            if let Ok(partial) = serde_json::from_value::<QueryDraftPatch>(patch.clone()) {
                let mut next = state.query_draft.clone();
                apply_draft_patch(&mut next, &partial);
                sync_draft(state, next);
            }
            Transition { effects: vec![] }
        }
        AppEvent::FilterAdd => {
            let column = state.column_names.first().cloned().unwrap_or_default();
            push_filter(state, column, "=", "");
            Transition { effects: vec![] }
        }
        AppEvent::FilterAddPrefilled { column, op, value } => {
            push_filter(state, column.clone(), op, value);
            Transition { effects: vec![] }
        }
        AppEvent::FilterRemove { id } => {
            let mut draft = state.query_draft.clone();
            draft.filters.retain(|f| f.id != *id);
            sync_draft(state, draft);
            Transition { effects: vec![] }
        }
        AppEvent::FilterPatch { id, patch } => {
            if let Ok(partial) = serde_json::from_value::<FilterPatch>(patch.clone()) {
                let mut draft = state.query_draft.clone();
                for f in &mut draft.filters {
                    if f.id == *id {
                        if let Some(column) = &partial.column {
                            f.column = column.clone();
                        }
                        if let Some(op) = &partial.op {
                            f.op = op.clone();
                        }
                        if let Some(value) = &partial.value {
                            f.value = value.clone();
                        }
                    }
                }
                sync_draft(state, draft);
            }
            Transition { effects: vec![] }
        }
        AppEvent::PresetSelected { id } => {
            if state.load_phase != LoadPhase::Ready {
                return Transition { effects: vec![] };
            }
            run_preset_fx(state, id)
        }
        AppEvent::WorkersChanged { workers } => {
            state.workers = (*workers).clamp(1, 16);
            Transition { effects: vec![] }
        }
        AppEvent::WarmWorkers => {
            if state.load_phase != LoadPhase::Ready {
                return Transition { effects: vec![] };
            }
            state.query_phase = QueryPhase::Warming;
            state.query_status = format!("Warming {} worker(s)…", state.workers);
            state.query_error = false;
            Transition {
                effects: vec![EffectCommand::WarmWorkers {
                    workers: state.workers,
                }],
            }
        }
        AppEvent::WorkersWarmed { ms } => {
            state.warm_ms = Some(*ms);
            if state.query_phase != QueryPhase::Running {
                state.query_phase = QueryPhase::Idle;
                state.query_status = format!("Workers ready ({:.1} ms)", ms);
            }
            Transition { effects: vec![] }
        }
        AppEvent::RunQuery => {
            if state.load_phase != LoadPhase::Ready {
                return Transition { effects: vec![] };
            }
            run_builder_fx(state, "Custom query")
        }
        AppEvent::QueryDone { result } => {
            state.query_phase = QueryPhase::Done;
            state.query_status = format!(
                "Done in {:.1} ms ({} worker(s))",
                result.timing_ms, result.workers
            );
            state.query_error = false;
            state.result = Some(result.clone());
            Transition { effects: vec![] }
        }
        AppEvent::QueryFailed { message } => {
            state.query_phase = QueryPhase::Error;
            state.query_status = message.clone();
            state.query_error = true;
            Transition { effects: vec![] }
        }
        AppEvent::RouteSet { route } => {
            state.route = *route;
            Transition { effects: vec![] }
        }
        AppEvent::CrateSelect { name } => select_crate(state, name.clone()),
        AppEvent::CrateDetailClose => {
            state.selected_crate = None;
            state.crate_detail = None;
            state.crate_detail_phase = CrateDetailPhase::Idle;
            state.crate_detail_status.clear();
            Transition { effects: vec![] }
        }
        AppEvent::CratePin { name } => {
            if !state.pinned_crates.iter().any(|p| p.name == *name) {
                state.pinned_crates.push(PinnedCrate { name: name.clone() });
            }
            Transition { effects: vec![] }
        }
        AppEvent::CrateUnpin { name } => {
            state.pinned_crates.retain(|p| p.name != *name);
            Transition { effects: vec![] }
        }
        AppEvent::CrateDetailDone { detail } => {
            state.crate_detail = Some(detail.clone());
            state.crate_detail_phase = CrateDetailPhase::Ready;
            state.crate_detail_status = format!("{} versions", detail.version_count);
            Transition { effects: vec![] }
        }
        AppEvent::CrateDetailFailed { message } => {
            state.crate_detail = None;
            state.crate_detail_phase = CrateDetailPhase::Error;
            state.crate_detail_status = message.clone();
            Transition { effects: vec![] }
        }
        AppEvent::SavedViewSave { name } => {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Transition { effects: vec![] };
            }
            let view = SavedView {
                id: new_saved_view_id(),
                name: trimmed.into(),
                query_draft: state.query_draft.clone(),
            };
            state.active_saved_view_id = Some(view.id.clone());
            state.saved_views.push(view);
            Transition { effects: vec![] }
        }
        AppEvent::SavedViewApply { id } => {
            let Some(view) = state.saved_views.iter().find(|v| v.id == *id).cloned() else {
                return Transition { effects: vec![] };
            };
            state.active_saved_view_id = Some(view.id);
            sync_draft(state, view.query_draft);
            if state.load_phase == LoadPhase::Ready {
                run_builder_fx(state, &format!("Saved view: {}", view.name))
            } else {
                Transition { effects: vec![] }
            }
        }
        AppEvent::SavedViewRemove { id } => {
            state.saved_views.retain(|v| v.id != *id);
            if state.active_saved_view_id.as_deref() == Some(id.as_str()) {
                state.active_saved_view_id = None;
            }
            Transition { effects: vec![] }
        }
        AppEvent::FilterPinSet { id, pinned } => {
            let mut draft = state.query_draft.clone();
            for f in &mut draft.filters {
                if f.id == *id {
                    f.pinned = *pinned;
                }
            }
            sync_draft(state, draft);
            Transition { effects: vec![] }
        }
        AppEvent::Undo | AppEvent::Redo => Transition { effects: vec![] },
        AppEvent::WorkspaceHydrate { hash } => {
            if let Some(url_state) = decode_shareable_url(hash) {
                apply_url_state(state, &url_state);
                if let Some(name) = state.selected_crate.clone() {
                    return select_crate(state, name);
                }
            }
            Transition { effects: vec![] }
        }
    }
}

fn select_crate(state: &mut AppState, name: String) -> Transition {
    state.selected_crate = Some(name.clone());
    state.crate_detail = None;
    state.crate_detail_phase = CrateDetailPhase::Loading;
    state.crate_detail_status = format!("Loading {name}…");
    Transition {
        effects: vec![EffectCommand::LoadCrateDetail {
            crate_name: name,
            workers: state.workers,
        }],
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryDraftPatch {
    #[serde(default)]
    mode: Option<QueryMode>,
    #[serde(default)]
    search_text: Option<String>,
    #[serde(default)]
    search_column: Option<String>,
    #[serde(default)]
    group_keys: Option<Vec<String>>,
    #[serde(default)]
    agg_column: Option<String>,
    #[serde(default)]
    select_columns: Option<Vec<String>>,
    #[serde(default)]
    top_k: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FilterPatch {
    #[serde(default)]
    column: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    value: Option<String>,
}

fn apply_draft_patch(draft: &mut QueryDraft, patch: &QueryDraftPatch) {
    if let Some(mode) = &patch.mode {
        draft.mode = *mode;
    }
    if let Some(v) = &patch.search_text {
        draft.search_text = v.clone();
    }
    if let Some(v) = &patch.search_column {
        draft.search_column = v.clone();
    }
    if let Some(v) = &patch.group_keys {
        draft.group_keys = v.clone();
    }
    if let Some(v) = &patch.agg_column {
        draft.agg_column = v.clone();
    }
    if let Some(v) = &patch.select_columns {
        draft.select_columns = v.clone();
    }
    if let Some(v) = patch.top_k {
        draft.top_k = v;
    }
}

fn sync_draft(state: &mut AppState, draft: QueryDraft) {
    state.query_draft = draft;
    state.built_plan_json = None;
}

fn loading(state: &mut AppState, status: &str, effects: Vec<EffectCommand>) -> Transition {
    state.load_phase = LoadPhase::Loading;
    state.load_status = status.into();
    state.load_error = false;
    Transition { effects }
}

pub fn requery_after_workspace_restore(state: &mut AppState) -> Transition {
    if state.load_phase != LoadPhase::Ready {
        return Transition { effects: vec![] };
    }
    run_builder_fx(state, "Restored exploration")
}

fn run_preset_fx(state: &mut AppState, id: &str) -> Transition {
    if state.load_phase != LoadPhase::Ready {
        return Transition { effects: vec![] };
    }
    let kind = state.meta.as_ref().map(|m| m.kind);
    let dataset_id = state.meta.as_ref().and_then(|m| m.dataset_id.as_deref());
    if !preset_exists(kind, dataset_id, id) {
        return run_builder_fx(state, "Custom query");
    }
    state.query_phase = QueryPhase::Running;
    state.query_status = "Running…".into();
    state.query_error = false;
    state.result = None;
    state.built_plan_json = None;
    Transition {
        effects: vec![EffectCommand::RunPreset {
            id: id.into(),
            workers: state.workers,
        }],
    }
}

fn run_builder_fx(state: &mut AppState, label: &str) -> Transition {
    let chart_hint = chart_hint_for_draft(&state.query_draft);
    match build_query_plan(&state.query_draft) {
        Ok(_plan) => {
            state.query_phase = QueryPhase::Running;
            state.query_status = "Running…".into();
            state.query_error = false;
            state.result = None;
            state.built_plan_json = Some(plan_preview(&state.query_draft));
            Transition {
                effects: vec![EffectCommand::RunQueryDraft {
                    draft: state.query_draft.clone(),
                    workers: state.workers,
                    label: label.into(),
                    chart_hint,
                }],
            }
        }
        Err(message) => {
            state.query_phase = QueryPhase::Error;
            state.query_status = message;
            state.query_error = true;
            Transition { effects: vec![] }
        }
    }
}

fn chart_hint_for_draft(draft: &QueryDraft) -> Option<String> {
    match draft.mode {
        QueryMode::Aggregate => {
            if draft.group_keys.len() > 1 {
                Some("grouped".into())
            } else {
                Some("bar-h".into())
            }
        }
        QueryMode::Table => Some("table".into()),
        QueryMode::Search => Some("rows".into()),
    }
}

fn push_filter(state: &mut AppState, column: String, op: &str, value: &str) {
    let mut draft = state.query_draft.clone();
    draft.filters.push(FilterDraft {
        id: new_filter_id(),
        column,
        op: op.into(),
        value: value.into(),
        pinned: false,
    });
    sync_draft(state, draft);
}

fn new_filter_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("f-{n}")
}
