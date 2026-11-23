//! Query output layout and post-graph finalisation (concat, manifest).

use crate::plan::physical_plan::{OpKind, WcolPhysicalPlan};
use crate::plan::query_dispatch::QueryDispatch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    Json,
    Parquet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputLayout {
    SingleFile,
    Partitioned,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputDestination {
    pub bucket: String,
    /// Public prefix for parquet jobs (`{prefix}_manifest.json` pointer).
    pub prefix: String,
    /// When set, the final single object is written at this key (concat jobs).
    pub object_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputPlan {
    pub format: OutputFormat,
    pub destination: OutputDestination,
    pub layout: OutputLayout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputFinalizer {
    None,
    /// Concatenate committed Parquet shards in this task-id order.
    ConcatParquet {
        partition_order: Vec<u32>,
    },
    /// Reserved; not implemented.
    #[allow(dead_code)]
    ConcatJson {
        partition_order: Vec<u32>,
    },
}

impl OutputPlan {
    pub fn parquet(bucket: String, prefix: String, layout: OutputLayout) -> Self {
        Self {
            format: OutputFormat::Parquet,
            destination: OutputDestination {
                bucket,
                prefix,
                object_key: None,
            },
            layout,
        }
    }
}

/// Default layout for an unordered multi-task parquet export.
pub fn default_parquet_export_layout() -> OutputLayout {
    OutputLayout::Partitioned
}

/// Whether the physical plan has a global sort or limit after exchange.
pub fn plan_has_global_sort_or_limit(plan: &WcolPhysicalPlan) -> bool {
    plan.operators.iter().any(|op| {
        op.node.is_none() && matches!(op.kind, OpKind::Sort { .. } | OpKind::Limit { .. })
    })
}

/// Whether the dispatch is an unordered per-node parquet export (no edges).
pub fn dispatch_is_unordered_parquet_parts(dispatch: &QueryDispatch) -> bool {
    matches!(dispatch, QueryDispatch::Parquet(_))
}

/// Pick layout when the job body did not specify one.
pub fn default_layout_for_parquet_job(
    dispatch: &QueryDispatch,
    plan: &WcolPhysicalPlan,
    estimated_output_bytes: u64,
    target_file_bytes: u64,
) -> OutputLayout {
    if dispatch_is_unordered_parquet_parts(dispatch) {
        return OutputLayout::Partitioned;
    }
    if plan_has_global_sort_or_limit(plan) {
        // Phase 1: coordinator still gathers; one writer → single file when not multi-part.
        return OutputLayout::SingleFile;
    }
    if estimated_output_bytes <= target_file_bytes.max(1) {
        OutputLayout::SingleFile
    } else {
        OutputLayout::Partitioned
    }
}

/// Build finaliser for single-file parquet: concat shard tasks in stable task-id order.
pub fn concat_finalizer_for_graph_tasks(task_ids: &[u32]) -> OutputFinalizer {
    if task_ids.is_empty() {
        OutputFinalizer::None
    } else {
        OutputFinalizer::ConcatParquet {
            partition_order: task_ids.to_vec(),
        }
    }
}

/// Task ids that produce Parquet shard objects (all tasks except optional finalizer-only root).
pub fn parquet_shard_task_ids(tasks: &[(u32, bool)], // (id, writes_parquet_shard)
) -> Vec<u32> {
    tasks
        .iter()
        .filter(|(_, writes)| *writes)
        .map(|(id, _)| *id)
        .collect()
}

pub fn parse_layout_from_job(body: &str) -> Option<OutputLayout> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let output = value.get("output")?;
    let layout = output.get("layout")?.as_str()?;
    match layout {
        "single-file" | "single_file" => Some(OutputLayout::SingleFile),
        "partitioned" => Some(OutputLayout::Partitioned),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlannerMetrics;

    #[test]
    fn unordered_dispatch_defaults_partitioned() {
        let dispatch = QueryDispatch::Parquet(vec![]);
        let plan = WcolPhysicalPlan {
            root: 0,
            operators: vec![],
            fetches: vec![],
            logical_reads: vec![],
            metrics: PlannerMetrics {
                nodes: vec![],
                request_count: 0,
                logical_bytes: 0,
                fetched_bytes: 0,
                over_read_bytes: 0,
                over_read_ppm: 0,
                hash_reducers: 1,
                estimated_distinct_groups: 0,
                estimated_group_bytes: 0,
            },
        };
        assert!(matches!(
            default_layout_for_parquet_job(&dispatch, &plan, 1, 1),
            OutputLayout::Partitioned
        ));
    }
}
