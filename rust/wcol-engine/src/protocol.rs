use crate::effect::EffectCommand;
use crate::event::AppEvent;
use crate::view_model::ViewModel;
use engine_kernel::ViewModelPatch;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(tag = "kind", rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum WorkerInput {
    Init,
    Event { event: AppEvent },
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(feature = "typegen", ts(tag = "kind", rename_all = "camelCase"))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum WorkerOutput {
    Initialized {
        #[serde(rename = "viewModel")]
        #[cfg_attr(feature = "typegen", ts(rename = "viewModel"))]
        view_model: ViewModel,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        effects: Vec<EffectCommand>,
    },
    Response {
        patches: Vec<ViewModelPatch>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        effects: Vec<EffectCommand>,
        #[serde(rename = "viewModel")]
        #[cfg_attr(feature = "typegen", ts(rename = "viewModel"))]
        view_model: ViewModel,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        diagnostics: Vec<String>,
    },
    Error {
        message: String,
    },
}

pub fn encode_input(input: &WorkerInput) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(input, &mut buf).expect("encode WorkerInput");
    buf
}

pub fn decode_input(bytes: &[u8]) -> Result<WorkerInput, String> {
    ciborium::from_reader(bytes).map_err(|e| e.to_string())
}

pub fn encode_output(output: &WorkerOutput) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(output, &mut buf).expect("encode WorkerOutput");
    buf
}

pub fn decode_output(bytes: &[u8]) -> Result<WorkerOutput, String> {
    ciborium::from_reader(bytes).map_err(|e| e.to_string())
}
