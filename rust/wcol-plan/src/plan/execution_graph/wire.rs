//! Wire encoding of an execution plan for the TypeScript host.

use super::*;

/// Host wire tag: execution graph v1 (replaces legacy dispatch tags 1–3).
pub const WIRE_TAG_EXECUTION_GRAPH_V1: u8 = 4;
/// Same body as v1, plus a partition id on every edge.
pub const WIRE_TAG_EXECUTION_GRAPH_V2: u8 = 5;
/// V2 body plus output plan and finaliser trailer.
pub const WIRE_TAG_EXECUTION_GRAPH_V3: u8 = 6;
/// Host wire tag: metadata partial only (same body as legacy tag 2).
pub const WIRE_TAG_METADATA_PARTIAL: u8 = 0;

pub fn encode_execution_plan_wire(plan: &ExecutionPlan) -> Vec<u8> {
    match plan {
        ExecutionPlan::MetadataPartial(partial) => {
            let mut out = Vec::with_capacity(5 + partial.len());
            out.push(WIRE_TAG_METADATA_PARTIAL);
            out.extend_from_slice(&(partial.len() as u32).to_le_bytes());
            out.extend_from_slice(partial);
            out
        }
        ExecutionPlan::Graph {
            tasks,
            edges,
            root_task_id,
            root_output,
            output,
            finalizer,
        } => {
            let partitioned = edges.iter().any(|edge| edge.partition != WHOLE_STREAM);
            let use_v3 = output.is_some() || !matches!(finalizer, OutputFinalizer::None);
            let mut out = Vec::new();
            out.push(if use_v3 {
                WIRE_TAG_EXECUTION_GRAPH_V3
            } else if partitioned {
                WIRE_TAG_EXECUTION_GRAPH_V2
            } else {
                WIRE_TAG_EXECUTION_GRAPH_V1
            });
            out.extend_from_slice(&(tasks.len() as u32).to_le_bytes());
            for task in tasks {
                out.extend_from_slice(&task.id.to_le_bytes());
                out.extend_from_slice(&task.slot.0.to_le_bytes());
                out.extend_from_slice(&task.request_count.to_le_bytes());
                out.extend_from_slice(&task.fetched_bytes.to_le_bytes());
                out.extend_from_slice(&(task.program.len() as u32).to_le_bytes());
                out.extend_from_slice(&task.program);
                // Run-time work: group (u32::MAX for none), then the task's own unit ids.
                match &task.work {
                    None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
                    Some(work) => {
                        out.extend_from_slice(&work.group.to_le_bytes());
                        out.extend_from_slice(&(work.own_units.len() as u32).to_le_bytes());
                        for unit in &work.own_units {
                            out.extend_from_slice(&unit.to_le_bytes());
                        }
                    }
                }
            }
            out.extend_from_slice(&(edges.len() as u32).to_le_bytes());
            for edge in edges {
                out.extend_from_slice(&edge.from.to_le_bytes());
                out.extend_from_slice(&edge.to.to_le_bytes());
                out.push(edge.transport.wire_byte());
                if partitioned || use_v3 {
                    out.extend_from_slice(&edge.partition.to_le_bytes());
                }
            }
            out.extend_from_slice(&root_task_id.to_le_bytes());
            out.push(match root_output {
                RootOutputKind::Partial => 0,
                RootOutputKind::Rows => 1,
                RootOutputKind::Parquet => 2,
            });
            if use_v3 {
                encode_output_trailer(&mut out, output.as_ref(), finalizer);
            }
            out
        }
    }
}

pub(super) fn push_wire_str(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
}

pub(super) fn encode_output_trailer(
    out: &mut Vec<u8>,
    output: Option<&OutputPlan>,
    finalizer: &OutputFinalizer,
) {
    if let Some(plan) = output {
        out.push(1);
        out.push(match plan.format {
            crate::plan::output_plan::OutputFormat::Json => 0,
            crate::plan::output_plan::OutputFormat::Parquet => 1,
        });
        out.push(match plan.layout {
            crate::plan::output_plan::OutputLayout::SingleFile => 0,
            crate::plan::output_plan::OutputLayout::Partitioned => 1,
        });
        push_wire_str(out, &plan.destination.bucket);
        push_wire_str(out, &plan.destination.prefix);
        if let Some(key) = &plan.destination.object_key {
            out.push(1);
            push_wire_str(out, key);
        } else {
            out.push(0);
        }
    } else {
        out.push(0);
    }
    match finalizer {
        OutputFinalizer::None => out.push(0),
        OutputFinalizer::ConcatParquet { partition_order } => {
            out.push(1);
            out.extend_from_slice(&(partition_order.len() as u32).to_le_bytes());
            for id in partition_order {
                out.extend_from_slice(&id.to_le_bytes());
            }
        }
        OutputFinalizer::ConcatJson { partition_order } => {
            out.push(2);
            out.extend_from_slice(&(partition_order.len() as u32).to_le_bytes());
            for id in partition_order {
                out.extend_from_slice(&id.to_le_bytes());
            }
        }
    }
}
