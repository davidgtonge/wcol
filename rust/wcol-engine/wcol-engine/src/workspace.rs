use crate::state::{AppState, FilterDraft, QueryDraft};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AppRoute {
    Explore,
    Compare,
    Trends,
    Board,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedView {
    pub id: String,
    pub name: String,
    pub query_draft: QueryDraft,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedCrate {
    pub name: String,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "lowercase"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CrateDetailPhase {
    Idle,
    Loading,
    Ready,
    Error,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrateVersionRow {
    pub version: String,
    pub license: String,
    pub downloads: u64,
    pub yanked: bool,
    pub edition: Option<String>,
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrateDetailSummary {
    pub crate_name: String,
    pub total_downloads: u64,
    pub version_count: u32,
    pub primary_license: Option<String>,
    pub yanked_count: u32,
    pub versions: Vec<CrateVersionRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceUrlState {
    pub route: AppRoute,
    pub query_draft: QueryDraft,
    pub selected_crate: Option<String>,
    pub pinned_crates: Vec<String>,
    pub active_saved_view_id: Option<String>,
}

const MAX_HISTORY: usize = 50;

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceSnapshot {
    pub route: AppRoute,
    pub query_draft: QueryDraft,
    pub selected_crate: Option<String>,
    pub pinned_crates: Vec<PinnedCrate>,
    pub active_saved_view_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct UndoHistory {
    undo: Vec<WorkspaceSnapshot>,
    redo: Vec<WorkspaceSnapshot>,
}

impl UndoHistory {
    pub fn can_undo(&self) -> bool {
        !self.undo.is_empty()
    }

    pub fn can_redo(&self) -> bool {
        !self.redo.is_empty()
    }

    pub fn record(&mut self, snap: WorkspaceSnapshot) {
        self.undo.push(snap);
        if self.undo.len() > MAX_HISTORY {
            self.undo.remove(0);
        }
        self.redo.clear();
    }

    pub fn undo(&mut self, current: WorkspaceSnapshot) -> Option<WorkspaceSnapshot> {
        let snap = self.undo.pop()?;
        self.redo.push(current);
        Some(snap)
    }

    pub fn redo(&mut self, current: WorkspaceSnapshot) -> Option<WorkspaceSnapshot> {
        let snap = self.redo.pop()?;
        self.undo.push(current);
        Some(snap)
    }
}

pub fn snapshot_from_state(state: &AppState) -> WorkspaceSnapshot {
    WorkspaceSnapshot {
        route: state.route,
        query_draft: state.query_draft.clone(),
        selected_crate: state.selected_crate.clone(),
        pinned_crates: state.pinned_crates.clone(),
        active_saved_view_id: state.active_saved_view_id.clone(),
    }
}

pub fn apply_snapshot(state: &mut AppState, snap: WorkspaceSnapshot) {
    state.route = snap.route;
    state.query_draft = snap.query_draft;
    state.selected_crate = snap.selected_crate;
    state.pinned_crates = snap.pinned_crates;
    state.active_saved_view_id = snap.active_saved_view_id;
    state.built_plan_json = None;
    state.crate_detail = None;
    state.crate_detail_phase = CrateDetailPhase::Idle;
    state.crate_detail_status.clear();
}

pub fn workspace_url_state(state: &AppState) -> WorkspaceUrlState {
    WorkspaceUrlState {
        route: state.route,
        query_draft: state.query_draft.clone(),
        selected_crate: state.selected_crate.clone(),
        pinned_crates: state.pinned_crates.iter().map(|p| p.name.clone()).collect(),
        active_saved_view_id: state.active_saved_view_id.clone(),
    }
}

pub fn encode_shareable_url(state: &AppState) -> String {
    let payload = workspace_url_state(state);
    match serde_json::to_string(&payload) {
        Ok(json) => format!("#{}", url_encode(&json)),
        Err(_) => format!("#route={}", route_slug(state.route)),
    }
}

pub fn decode_shareable_url(hash: &str) -> Option<WorkspaceUrlState> {
    let raw = hash.strip_prefix('#').unwrap_or(hash).trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(json) = url_decode(raw) {
        if let Ok(state) = serde_json::from_str::<WorkspaceUrlState>(&json) {
            return Some(state);
        }
    }
    None
}

pub fn apply_url_state(state: &mut AppState, url: &WorkspaceUrlState) {
    state.route = url.route;
    state.query_draft = url.query_draft.clone();
    state.selected_crate = url.selected_crate.clone();
    state.pinned_crates = url
        .pinned_crates
        .iter()
        .map(|name| PinnedCrate { name: name.clone() })
        .collect();
    state.active_saved_view_id = url.active_saved_view_id.clone();
    state.built_plan_json = None;
}

fn route_slug(route: AppRoute) -> &'static str {
    match route {
        AppRoute::Explore => "explore",
        AppRoute::Compare => "compare",
        AppRoute::Trends => "trends",
        AppRoute::Board => "board",
    }
}

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn url_decode(s: &str) -> Result<String, ()> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|_| ())?;
            let byte = u8::from_str_radix(hex, 16).map_err(|_| ())?;
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

pub fn is_undoable(event: &crate::event::AppEvent) -> bool {
    use crate::event::AppEvent;
    matches!(
        event,
        AppEvent::RouteSet { .. }
            | AppEvent::QueryDraftPatch { .. }
            | AppEvent::FilterAdd
            | AppEvent::FilterAddPrefilled { .. }
            | AppEvent::FilterRemove { .. }
            | AppEvent::FilterPatch { .. }
            | AppEvent::FilterPinSet { .. }
            | AppEvent::PresetSelected { .. }
            | AppEvent::RunQuery
            | AppEvent::SavedViewApply { .. }
            | AppEvent::CrateSelect { .. }
            | AppEvent::CrateDetailClose
    )
}

pub fn new_saved_view_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("view-{n}")
}

pub fn pinned_filter_count(filters: &[FilterDraft]) -> u32 {
    filters.iter().filter(|f| f.pinned).count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::QueryMode;

    #[test]
    fn url_round_trip() {
        let state = WorkspaceUrlState {
            route: AppRoute::Explore,
            query_draft: QueryDraft {
                mode: QueryMode::Aggregate,
                search_text: "tokio".into(),
                search_column: "crate_name".into(),
                filters: vec![],
                group_keys: vec!["crate_name".into()],
                agg_column: "downloads".into(),
                select_columns: vec![],
                top_k: 25,
            },
            selected_crate: Some("tokio".into()),
            pinned_crates: vec!["serde".into()],
            active_saved_view_id: None,
        };
        let encoded = url_encode(&serde_json::to_string(&state).unwrap());
        let decoded = decode_shareable_url(&format!("#{encoded}")).unwrap();
        assert_eq!(decoded.selected_crate, Some("tokio".into()));
    }
}
