//! Consistency check of a finished plan.

use super::*;

pub(super) fn audit(
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
) -> Result<(), PlanError> {
    if plan.operators.get(plan.root as usize).is_none() {
        return Err(PlanError::Inconsistent);
    }
    for (index, op) in plan.operators.iter().enumerate() {
        if op.id != index as u32 {
            return Err(PlanError::Inconsistent);
        }
        for input in &op.inputs {
            if *input >= op.id {
                return Err(PlanError::Inconsistent);
            }
        }
        if let OpKind::Decode { groups } = &op.kind {
            let fetch_ids = match op.inputs.as_slice() {
                [] => None,
                [id] => match &plan.operators[*id as usize].kind {
                    OpKind::R2Fetch { fetch_ids } => Some(fetch_ids.as_slice()),
                    _ => return Err(PlanError::Inconsistent),
                },
                _ => return Err(PlanError::Inconsistent),
            };
            for group in groups {
                for read_id in &group.reads {
                    let read = plan
                        .logical_reads
                        .get(*read_id as usize)
                        .ok_or(PlanError::Inconsistent)?;
                    if read.object_id != group.object_id
                        || read.row_group != group.row_group
                        || !group.columns.contains(&read.column)
                    {
                        return Err(PlanError::Inconsistent);
                    }
                    match fetch_ids {
                        Some(ids) => {
                            if read.pieces.is_empty() {
                                if !ids.contains(&read.fetch_id) {
                                    return Err(PlanError::Inconsistent);
                                }
                            } else {
                                for piece in &read.pieces {
                                    if !ids.contains(&piece.fetch_id) {
                                        return Err(PlanError::Inconsistent);
                                    }
                                }
                            }
                        }
                        None => {
                            if !read.pieces.is_empty() || read.length > 0 {
                                return Err(PlanError::Inconsistent);
                            }
                        }
                    }
                }
            }
        }
    }
    for read in &plan.logical_reads {
        let mut offset = 0u64;
        for piece in &read.pieces {
            if piece.read_offset != offset {
                return Err(PlanError::Inconsistent);
            }
            offset = offset
                .checked_add(piece.length)
                .ok_or(PlanError::Overflow)?;
            let fetch = plan
                .fetches
                .get(piece.fetch_id as usize)
                .ok_or(PlanError::Inconsistent)?;
            if fetch.length > config.max_request_bytes {
                return Err(PlanError::Inconsistent);
            }
            let piece_end = piece
                .fetch_offset
                .checked_add(piece.length)
                .ok_or(PlanError::Overflow)?;
            if piece.fetch_offset > fetch.length || piece_end > fetch.length {
                return Err(PlanError::Inconsistent);
            }
        }
        if !read.pieces.is_empty() && offset != read.length {
            return Err(PlanError::Inconsistent);
        }
    }
    for fetch in &plan.fetches {
        if fetch.length > config.max_request_bytes {
            return Err(PlanError::Inconsistent);
        }
        let mut spans = Vec::new();
        for read_id in &fetch.logical_reads {
            let read = plan
                .logical_reads
                .get(*read_id as usize)
                .ok_or(PlanError::Inconsistent)?;
            if read.node != fetch.node || read.object_id != fetch.object_id {
                return Err(PlanError::Inconsistent);
            }
            for piece in &read.pieces {
                if piece.fetch_id != fetch.id {
                    continue;
                }
                let start = fetch
                    .offset
                    .checked_add(piece.fetch_offset)
                    .ok_or(PlanError::Overflow)?;
                let end = start.checked_add(piece.length).ok_or(PlanError::Overflow)?;
                spans.push((start, end));
            }
        }
        let covered = covered_len(&spans)?;
        let fetch_end = range_end(fetch.offset, fetch.length)?;
        if covered > fetch.length || fetch_end < fetch.offset {
            return Err(PlanError::Inconsistent);
        }
    }
    for op in &plan.operators {
        for &input in &op.inputs {
            let input_op = &plan.operators[input as usize];
            if op.node == input_op.node {
                continue;
            }
            // Cross-site edges are only legal into or out of an exchange.
            if is_exchange(&op.kind) || is_exchange(&input_op.kind) {
                continue;
            }
            return Err(PlanError::Inconsistent);
        }
    }
    if plan.metrics.request_count != plan.fetches.len() as u32 {
        return Err(PlanError::Inconsistent);
    }
    Ok(())
}
