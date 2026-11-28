//! Physical plan for locality-aware R2 reads.
//!
//! [`lower_to_physical`] is a pure function of the logical plan, cached Parquet
//! metadata, the node list, and [`PhysicalPlannerConfig`]. It does not read
//! files, open the network, or inspect coordinator state.
//!
//! A row group is the placement unit: columns that a filter or aggregate must
//! see together stay on one node. Range coalescing only runs inside that
//! assignment, and only up to the configured gap and request size.

use std::collections::BTreeMap;

use thiserror::Error;

use wcol_engine::ir::Scalar;

pub use crate::program::types::{
    AggExpr, AggKind, ByteSpan, CmpOp, Compare, DecodeGroup, FetchPiece, FetchRequest, Literal,
    LogicalRead, NodeId, Predicate, SortKey, SortTarget,
};

/// Failure while building a [`WcolPhysicalPlan`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlanError {
    #[error("physical planning needs at least one node")]
    EmptyNodes,
    #[error("node ids must be unique")]
    DuplicateNode,
    #[error("max_request_bytes must be positive")]
    BadConfig,
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("duplicate table {0}")]
    DuplicateTable(String),
    #[error("duplicate object id {0}")]
    DuplicateObject(String),
    #[error("table {0} has no row groups")]
    NoRowGroups(String),
    #[error("table {table} is missing column {column} of row group {row_group} in {object}")]
    MissingColumn {
        table: String,
        object: String,
        row_group: u32,
        column: u32,
    },
    #[error("duplicate column {column} in row group {row_group} of {object}")]
    DuplicateColumn {
        object: String,
        row_group: u32,
        column: u32,
    },
    #[error("row group {row_group} in {object} has conflicting row counts")]
    InconsistentRows { object: String, row_group: u32 },
    #[error("byte range overflows")]
    Overflow,
    #[error("column chunk length must be non-zero")]
    EmptyRange,
    #[error("{0}")]
    Unsupported(&'static str),
    #[error("range sort boundary planning failed: {0}")]
    RangeSort(String),
    #[error("physical plan failed its own consistency check")]
    Inconsistent,
}

mod audit;
mod catalog;
mod config;
mod display;
mod exchange;
mod fetch;
mod logical;
mod operators;
mod ops;
mod shape;
#[cfg(test)]
mod tests;
mod units;

use audit::*;
pub use catalog::*;
pub use config::*;
use display::*;
pub use exchange::*;
use fetch::*;
pub use logical::*;
pub use operators::*;
use ops::*;
use shape::*;
pub use units::*;

pub fn lower_to_physical(
    logical: &WcolLogicalPlan,
    metadata: &ParquetMetadataCatalog,
    nodes: &[NodeId],
    config: &PhysicalPlannerConfig,
) -> Result<WcolPhysicalPlan, PlanError> {
    if nodes.is_empty() {
        return Err(PlanError::EmptyNodes);
    }
    if config.max_request_bytes == 0 {
        return Err(PlanError::BadConfig);
    }
    config.validate()?;
    let mut seen = nodes.to_vec();
    seen.sort();
    if seen.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(PlanError::DuplicateNode);
    }

    let shape = analyze(&logical.root)?;
    if let Some(answer) = metadata_answer(&shape, metadata)? {
        return build_metadata_plan(answer);
    }

    let mut units = resolve(0, &shape.scan, metadata)?;
    for unit in units.iter_mut() {
        unit.weight = unit_cost(&unit.drafts, config);
    }
    let planned_bytes = units
        .iter()
        .fold(0u64, |sum, unit| sum.saturating_add(unit.weight));
    let cap = (nodes.len() as u32).min(config.max_nodes).max(1);
    let mut capped = config.clone();
    capped.max_nodes = cap;
    // A scan node reads one object (its fetch and decode state is keyed by offset in that
    // object), so a table of several objects needs at least one node per object.
    let objects = object_count(&units);
    if objects > cap as usize {
        return Err(PlanError::Unsupported(
            "the table has more objects than scan nodes",
        ));
    }
    let count = (planned_node_slots(planned_bytes, &capped) as usize).max(objects);
    let placed: Vec<NodeId> = nodes.iter().copied().take(count).collect();
    let nodes = placed.as_slice();
    place(&mut units, nodes);

    let logical_reads = number_reads(&mut units, nodes)?;
    let (fetches, piece_lists) = coalesce(&logical_reads, nodes, config)?;
    let logical_reads = attach_pieces(logical_reads, piece_lists)?;
    let metrics = metrics_of(&logical_reads, &fetches, nodes)?;
    let source_rows = units
        .iter()
        .fold(0u64, |sum, unit| sum.saturating_add(unit.rows));
    let estimated_groups = shape
        .aggregate
        .as_ref()
        .map(|agg| exchange_keys(&agg.group_by, &agg.aggregates))
        .filter(|keys| !keys.is_empty())
        .map(|keys| estimated_distinct_groups(&keys, metadata, source_rows))
        .unwrap_or(0);
    let (operators, root) = build_ops(
        &shape,
        &units,
        nodes,
        &fetches,
        config,
        estimated_groups,
        metadata,
    )?;
    let mut plan = WcolPhysicalPlan {
        root,
        operators,
        fetches,
        logical_reads,
        metrics,
    };
    plan.metrics.estimated_distinct_groups = estimated_groups;
    plan.metrics.estimated_group_bytes = shape.aggregate.as_ref().map_or(0, |agg| {
        estimated_groups.saturating_mul(group_state_bytes(&agg.aggregates))
    });
    if let Some(partitions) = plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::HashExchange { partitions, .. } => Some(*partitions),
        _ => None,
    }) {
        plan.metrics.hash_reducers = partitions;
    }
    audit(&plan, config)?;
    Ok(plan)
}
