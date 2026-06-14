use crate::state::QueryDraft;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(
    feature = "typegen",
    ts(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")
)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EffectCommand {
    OpenSource {
        /// `"sample"` or a URL string — resolved in the worker I/O runtime.
        source: String,
        label: String,
    },
    WarmWorkers {
        workers: u32,
    },
    RunQuery {
        #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
        plan: Value,
        workers: u32,
        label: String,
        #[serde(rename = "chartHint", default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "typegen", ts(rename = "chartHint"))]
        chart_hint: Option<String>,
    },
    RunQueryDraft {
        draft: QueryDraft,
        workers: u32,
        label: String,
        #[serde(rename = "chartHint", default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "typegen", ts(rename = "chartHint"))]
        chart_hint: Option<String>,
    },
    RunPreset {
        id: String,
        workers: u32,
    },
    LoadCrateDetail {
        #[serde(rename = "crateName")]
        #[cfg_attr(feature = "typegen", ts(rename = "crateName"))]
        crate_name: String,
        workers: u32,
    },
}
