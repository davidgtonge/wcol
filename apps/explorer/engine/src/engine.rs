use crate::event::AppEvent;
use crate::protocol::{WorkerInput, WorkerOutput};
use crate::state::AppState;
use crate::state::LoadPhase;
use crate::update::{reduce, requery_after_workspace_restore};
use crate::view_model::{normalize_view_model, select_view_model, ViewModel};
use crate::workspace::{apply_snapshot, is_undoable, snapshot_from_state};
use engine_kernel::diff_value;

pub struct Engine {
    state: AppState,
    view_model: ViewModel,
}

impl Engine {
    pub fn new() -> Self {
        let state = AppState::initial();
        let view_model = select_view_model(&state);
        Self { state, view_model }
    }

    pub fn init(&mut self) -> WorkerOutput {
        self.state = AppState::initial();
        self.view_model = select_view_model(&self.state);
        WorkerOutput::Initialized {
            view_model: self.view_model.clone(),
            effects: vec![],
        }
    }

    pub fn handle_event(&mut self, event: AppEvent) -> WorkerOutput {
        let prev = normalize_view_model(&self.view_model);
        if is_undoable(&event) {
            self.state
                .history
                .record(snapshot_from_state(&self.state));
        }
        let transition = match &event {
            AppEvent::Undo => {
                let current = snapshot_from_state(&self.state);
                if let Some(snap) = self.state.history.undo(current) {
                    apply_snapshot(&mut self.state, snap);
                    if self.state.load_phase == LoadPhase::Ready {
                        requery_after_workspace_restore(&mut self.state)
                    } else {
                        crate::update::Transition { effects: vec![] }
                    }
                } else {
                    crate::update::Transition { effects: vec![] }
                }
            }
            AppEvent::Redo => {
                let current = snapshot_from_state(&self.state);
                if let Some(snap) = self.state.history.redo(current) {
                    apply_snapshot(&mut self.state, snap);
                    if self.state.load_phase == LoadPhase::Ready {
                        requery_after_workspace_restore(&mut self.state)
                    } else {
                        crate::update::Transition { effects: vec![] }
                    }
                } else {
                    crate::update::Transition { effects: vec![] }
                }
            }
            _ => reduce(&mut self.state, &event),
        };
        self.view_model = select_view_model(&self.state);
        let next = normalize_view_model(&self.view_model);
        let patches = diff_value(&prev, &next);
        WorkerOutput::Response {
            patches,
            effects: transition.effects,
            view_model: self.view_model.clone(),
            diagnostics: vec![],
        }
    }

    pub fn handle_input(&mut self, input: WorkerInput) -> WorkerOutput {
        match input {
            WorkerInput::Init => self.init(),
            WorkerInput::Event { event } => self.handle_event(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::AppEvent;

    #[test]
    fn json_url_changed_wire() {
        let json = r#"{"kind":"event","event":{"type":"URL_CHANGED","url":"hello"}}"#;
        let input: crate::protocol::WorkerInput = serde_json::from_str(json).unwrap();
        let mut engine = Engine::new();
        engine.init();
        let out = engine.handle_input(input);
        match out {
            crate::protocol::WorkerOutput::Response { patches, .. } => {
                assert!(!patches.is_empty());
                let json = serde_json::to_string(&patches).unwrap();
                assert!(json.contains("urlInput"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn url_changed_emits_narrow_patch() {
        let mut engine = Engine::new();
        let out = engine.handle_event(AppEvent::UrlChanged {
            url: "https://example.com".into(),
        });
        match out {
            WorkerOutput::Response { patches, .. } => {
                assert!(!patches.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
