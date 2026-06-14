pub mod effect;
pub mod engine;
pub mod event;
pub mod presets;
pub mod protocol;
pub mod query_plan;
pub mod state;
pub mod update;
pub mod view_model;
pub mod workspace;

#[cfg(feature = "typegen")]
mod typegen;

#[cfg(feature = "typegen")]
pub use typegen::export_types;

pub use engine::Engine;
pub use engine_kernel::{apply_patches, diff_value, ViewModelPatch};
pub use protocol::{decode_input, decode_output, encode_input, encode_output, WorkerInput, WorkerOutput};
pub use view_model::ViewModel;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WcolEngine {
    inner: Engine,
    ready: bool,
}

#[wasm_bindgen]
impl WcolEngine {
    #[wasm_bindgen(constructor)]
    pub fn new() -> WcolEngine {
        WcolEngine {
            inner: Engine::new(),
            ready: false,
        }
    }

    /// CBOR `WorkerInput` bytes in, CBOR `WorkerOutput` bytes out.
    pub fn init(&mut self, payload: &[u8]) -> Vec<u8> {
        match decode_input(payload) {
            Ok(input) => {
                let output = self.inner.handle_input(input);
                self.ready = true;
                encode_output(&output)
            }
            Err(message) => encode_output(&WorkerOutput::Error { message }),
        }
    }

    /// CBOR `WorkerInput` bytes in, CBOR `WorkerOutput` bytes out.
    pub fn handle_input(&mut self, payload: &[u8]) -> Vec<u8> {
        if !self.ready {
            return encode_output(&WorkerOutput::Error {
                message: "engine not initialized".into(),
            });
        }
        match decode_input(payload) {
            Ok(input) => encode_output(&self.inner.handle_input(input)),
            Err(message) => encode_output(&WorkerOutput::Error { message }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn apply_round_trip() {
        let prev = json!({
            "urlInput": "",
            "panel": { "status": "idle", "count": 0 }
        });
        let next = json!({
            "urlInput": "abc",
            "panel": { "status": "idle", "count": 0 }
        });
        let patches = diff_value(&prev, &next);
        let applied = apply_patches(&prev, &patches);
        assert_eq!(applied, next);
    }
}
