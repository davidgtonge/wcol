//! Coordinator steps taken off a physical plan. The wire format lives in [`crate::program::coordinator`].

pub use crate::program::coordinator::*;

use crate::plan::physical_plan::{OpKind, WcolPhysicalPlan};

/// Operators with `node = coordinator`, in plan order. Exchange and the sink are not steps.
pub fn coordinator_steps(plan: &WcolPhysicalPlan) -> Vec<CoordinatorStep> {
    plan.operators
        .iter()
        .filter(|op| op.node.is_none())
        .filter_map(|op| match &op.kind {
            OpKind::Project { columns } => Some(CoordinatorStep::Project(columns.clone())),
            OpKind::Sort { keys } => Some(CoordinatorStep::Sort(keys.clone())),
            OpKind::Limit { skip, fetch } => Some(CoordinatorStep::Limit {
                skip: *skip,
                fetch: *fetch,
            }),
            _ => None,
        })
        .collect()
}
