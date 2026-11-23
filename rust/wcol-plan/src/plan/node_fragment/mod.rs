//! Build node fragments from a physical plan. Encoding lives in [`crate::program::fragment`].

pub use crate::program::fragment::*;

use std::collections::BTreeMap;

use crate::plan::physical_plan::{AggKind, OpKind, SortTarget, WcolPhysicalPlan};
use crate::program::types::{ClaimUnit, NodeId};

/// Split a physical plan into node-local fragments (coordinator operators omitted).
pub fn node_fragments(
    plan: &WcolPhysicalPlan,
    fetch_depth: usize,
) -> Result<BTreeMap<NodeId, NodeFragment>, FragmentError> {
    let tail = coordinator_tail(plan);
    let fetch_depth = u32::try_from(fetch_depth.max(1)).unwrap_or(u32::MAX);
    let mut out: BTreeMap<NodeId, NodeFragment> = BTreeMap::new();
    for op in &plan.operators {
        let Some(node) = op.node else {
            continue;
        };
        let entry = out.entry(node).or_insert_with(|| NodeFragment {
            node,
            decode: Vec::new(),
            logical_reads: plan
                .logical_reads
                .iter()
                .filter(|read| read.node == node)
                .cloned()
                .collect(),
            fetches: plan
                .fetches
                .iter()
                .filter(|fetch| fetch.node == node)
                .cloned()
                .collect(),
            steps: Vec::new(),
            output_op: op.id,
            tail: FragmentTail::default(),
            fetch_depth,
            // Set by `assign_sources` once the job's object order is known.
            source: 0,
            row_id: None,
            // Set by `add_claim_units` for scans that balance work at run time.
            claim_units: Vec::new(),
            own_units: Vec::new(),
        });
        match &op.kind {
            OpKind::Decode { groups } => entry.decode.extend(groups.clone()),
            OpKind::Filter { predicates } if !predicates.is_empty() => {
                entry.steps.push(FragmentStep::Filter(predicates.clone()));
            }
            OpKind::Project { columns } => {
                entry.steps.push(FragmentStep::Project(columns.clone()));
            }
            OpKind::PartialAggregate {
                group_by,
                aggregates,
            } => {
                entry.tail = FragmentTail {
                    order_groups_by_count: tail.order_groups_by_count && !group_by.is_empty(),
                    ..tail
                };
                entry.steps.push(FragmentStep::PartialAggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                });
            }
            OpKind::TopK { keys, n } => {
                entry.steps.push(FragmentStep::TopK {
                    keys: keys.clone(),
                    n: *n,
                });
            }
            OpKind::Sink {
                kind:
                    crate::plan::physical_plan::SinkKind::Parquet {
                        target_row_group_rows,
                        partition_columns,
                    },
            } => {
                entry.steps.push(FragmentStep::Parquet {
                    target_row_group_rows: *target_row_group_rows,
                    partition_columns: partition_columns.clone(),
                });
            }
            _ => {}
        }
        if matches!(
            op.kind,
            OpKind::PartialAggregate { .. }
                | OpKind::TopK { .. }
                | OpKind::Sink {
                    kind: crate::plan::physical_plan::SinkKind::Parquet { .. }
                }
        ) {
            entry.output_op = op.id;
        }
    }
    if out.is_empty() {
        return Err(FragmentError::NoNodeWork);
    }
    let hash = plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::HashExchange {
            keys,
            partitions,
            hash_version,
        } => Some(FragmentStep::HashPartition {
            input: PartitionInput::Rows { keys: keys.clone() },
            partitions: *partitions,
            hash_version: *hash_version,
        }),
        OpKind::RangeExchange {
            keys,
            partitions,
            splits,
        } => Some(FragmentStep::RangePartition {
            keys: keys.clone(),
            partitions: *partitions,
            splits: splits.clone(),
        }),
        _ => None,
    });
    let single = out.len() == 1 && hash.is_none();
    let rank = if single {
        rank_count_aggregate(plan)
    } else {
        None
    };
    for entry in out.values_mut() {
        entry.tail.final_aggregate = single;
        entry.tail.rank_aggregate = rank;
        if let Some(step) = &hash {
            let step = partition_step(step, &entry.steps)?;
            entry.steps.push(step);
        }
    }
    Ok(out)
}

/// A hash partition after a partial aggregate splits its partial states: each exchange key
/// becomes its position in the partial key, which is the GROUP BY columns and then any
/// `COUNT(DISTINCT)` column. Without one, the step splits rows.
fn partition_step(
    step: &FragmentStep,
    steps: &[FragmentStep],
) -> Result<FragmentStep, FragmentError> {
    let FragmentStep::HashPartition {
        input: PartitionInput::Rows { keys },
        partitions,
        hash_version,
    } = step
    else {
        return Ok(step.clone());
    };
    let Some((group_by, aggregates)) = steps.iter().find_map(|step| match step {
        FragmentStep::PartialAggregate {
            group_by,
            aggregates,
        } => Some((group_by, aggregates)),
        _ => None,
    }) else {
        return Ok(step.clone());
    };
    let mut partial_key = group_by.clone();
    if let Some(column) = aggregates
        .iter()
        .find(|agg| agg.kind == AggKind::CountDistinct)
        .and_then(|agg| agg.column)
    {
        partial_key.push(column);
    }
    let keys = keys
        .iter()
        .map(|&column| {
            partial_key
                .iter()
                .position(|&key| key == column)
                .map(|position| (position as u32, column))
                .ok_or(FragmentError::ExchangeKeyNotGrouped { column })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FragmentStep::HashPartition {
        input: PartitionInput::Partials { keys },
        partitions: *partitions,
        hash_version: *hash_version,
    })
}

/// Point each fragment at its object's ReadAt source: its index in `objects` (job-source
/// order). The planner gives every node one object; a fragment reading two is an error.
pub fn assign_sources(
    fragments: &mut BTreeMap<NodeId, NodeFragment>,
    objects: &[&str],
) -> Result<(), String> {
    for fragment in fragments.values_mut() {
        let mut ids = fragment.decode.iter().map(|group| group.object_id.as_str());
        let Some(first) = ids.next() else {
            continue;
        };
        if ids.any(|id| id != first) {
            return Err(format!(
                "node {} reads more than one object",
                fragment.node.0
            ));
        }
        let index = objects
            .iter()
            .position(|id| *id == first)
            .ok_or_else(|| format!("node {} reads unknown object {first}", fragment.node.0))?;
        fragment.source =
            u16::try_from(index).map_err(|_| "more than 65536 source objects".to_string())?;
    }
    Ok(())
}

/// What decoding one column chunk costs, for ordering scan work: `(object, row group, leaf)`.
pub type ChunkCost<'a> = dyn Fn(&str, u32, u32) -> u64 + 'a;

/// Let the hash scans of one object balance at run time. Each scan's fragment can run any
/// unit of that object (its catalog holds them all) and names the units planned for it; the
/// coordinator queues those per scan and hands out work, so an idle scan takes units from
/// the back of a slower scan's queue. A unit is the row groups one scan's fetches join.
///
/// Queues are dealt by cost (`chunk_cost`), most expensive first and snaking across the
/// scans, so every scan starts on one of the heaviest units and queues carry similar work.
/// What is stolen from the back, and what any scan runs last, is the cheapest work left:
/// no scan starts a long unit once the others are idle, and no slow scan keeps the heavy
/// units its plan happened to hold while others steal its light ones.
pub fn add_claim_units(fragments: &mut BTreeMap<NodeId, NodeFragment>, chunk_cost: &ChunkCost) {
    let hash_scan = |fragment: &NodeFragment| {
        fragment
            .steps
            .iter()
            .any(|step| matches!(step, FragmentStep::HashPartition { .. }))
    };
    let mut by_object: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
    for (node, fragment) in fragments.iter() {
        if let (true, Some(group)) = (hash_scan(fragment), fragment.decode.first()) {
            by_object
                .entry(group.object_id.clone())
                .or_default()
                .push(*node);
        }
    }
    for nodes in by_object.into_values().filter(|nodes| nodes.len() > 1) {
        let owned: Vec<(NodeFragment, Vec<ClaimUnit>)> = nodes
            .iter()
            .map(|node| {
                let fragment = fragments[node].clone();
                let units = claim_units_of(&fragment);
                (fragment, units)
            })
            .collect();
        let catalog: Vec<ClaimUnit> = owned
            .iter()
            .flat_map(|(_, units)| units.iter().cloned())
            .collect();
        let queues = deal_by_cost(&owned, chunk_cost);
        for (index, node) in nodes.iter().enumerate() {
            let fragment = fragments.get_mut(node).expect("fragment");
            for (other, (theirs, _)) in owned.iter().enumerate() {
                if other != index {
                    fragment.decode.extend(theirs.decode.iter().cloned());
                    fragment
                        .logical_reads
                        .extend(theirs.logical_reads.iter().cloned());
                    fragment.fetches.extend(theirs.fetches.iter().cloned());
                }
            }
            fragment.own_units = queues[index].clone();
            fragment.claim_units = catalog.clone();
        }
    }
}

/// Every unit of `owned` by descending decode cost of its reads (ties by id), dealt to one
/// queue per scan in snake order: 0, 1, .., n-1, n-1, .., 0, 0, 1, ..
fn deal_by_cost(owned: &[(NodeFragment, Vec<ClaimUnit>)], chunk_cost: &ChunkCost) -> Vec<Vec<u32>> {
    let mut costed: Vec<(u64, u32)> = Vec::new();
    for (fragment, units) in owned {
        let mut group_cost: BTreeMap<u32, u64> = BTreeMap::new();
        for read in &fragment.logical_reads {
            *group_cost.entry(read.row_group).or_default() +=
                chunk_cost(&read.object_id, read.row_group, read.column);
        }
        costed.extend(units.iter().map(|unit| {
            let cost = unit
                .row_groups
                .iter()
                .map(|group| group_cost.get(group).copied().unwrap_or(0))
                .sum();
            (cost, unit.id)
        }));
    }
    costed.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let scans = owned.len();
    let mut queues = vec![Vec::new(); scans];
    for (turn, (_, id)) in costed.into_iter().enumerate() {
        let (round, at) = (turn / scans, turn % scans);
        queues[if round % 2 == 0 { at } else { scans - 1 - at }].push(id);
    }
    queues
}

/// One fragment's row groups, joined where a fetch serves several, in row-group order.
fn claim_units_of(fragment: &NodeFragment) -> Vec<ClaimUnit> {
    let mut units: Vec<Vec<u32>> = fragment
        .decode
        .iter()
        .map(|group| vec![group.row_group])
        .collect();
    let group_of = |read: u32| {
        fragment
            .logical_reads
            .iter()
            .find(|r| r.id == read)
            .map(|r| r.row_group)
    };
    for fetch in &fragment.fetches {
        let groups: Vec<u32> = fetch
            .logical_reads
            .iter()
            .filter_map(|read| group_of(*read))
            .collect();
        let mut joined: Vec<u32> = Vec::new();
        units.retain(|unit| {
            if unit.iter().any(|group| groups.contains(group)) {
                joined.extend(unit);
                false
            } else {
                true
            }
        });
        if !joined.is_empty() {
            units.push(joined);
        }
    }
    let mut units: Vec<ClaimUnit> = units
        .into_iter()
        .map(|mut row_groups| {
            row_groups.sort_unstable();
            row_groups.dedup();
            ClaimUnit {
                id: row_groups[0],
                row_groups,
            }
        })
        .collect();
    units.sort_by_key(|unit| unit.id);
    units
}

/// The count aggregate a final top-k orders by.
///
/// Only a single descending `Count` or `CountStar`, with a limit, and no `COUNT(DISTINCT)`
/// hiding in the same aggregate. Anything else still emits every group.
pub(crate) fn rank_count_aggregate(plan: &WcolPhysicalPlan) -> Option<u32> {
    let mut sort_keys = None;
    let mut fetch = None;
    let mut aggregates = None;
    let mut group_by: &[u32] = &[];
    for op in plan.operators.iter().filter(|op| op.node.is_none()) {
        match &op.kind {
            OpKind::Limit { fetch: n, .. } => fetch = Some(*n),
            OpKind::Sort { keys } => sort_keys = Some(keys.as_slice()),
            OpKind::FinalAggregate {
                aggregates: aggs,
                group_by: groups,
            } => {
                aggregates = Some(aggs.as_slice());
                group_by = groups.as_slice();
            }
            _ => {}
        }
    }
    // A distinct count is final on a reducer only when the exchange kept each group whole.
    let groups_whole = !group_by.is_empty()
        && plan.operators.iter().any(|op| {
            matches!(&op.kind, OpKind::HashExchange { keys, .. } if keys.as_slice() == group_by)
        });
    let keys = sort_keys?;
    if fetch.flatten().is_none() || keys.len() != 1 || !keys[0].descending {
        return None;
    }
    let SortTarget::Aggregate(index) = keys[0].target else {
        return None;
    };
    let aggregates = aggregates?;
    let distinct = aggregates
        .iter()
        .any(|agg| agg.kind == AggKind::CountDistinct);
    let agg = aggregates.get(index as usize)?;
    let rankable = if distinct {
        groups_whole && agg.kind == AggKind::CountDistinct
    } else {
        matches!(agg.kind, AggKind::Count | AggKind::CountStar)
    };
    rankable.then_some(index)
}

/// The coordinator's limit and sort, read off the operators that run there.
fn coordinator_tail(plan: &WcolPhysicalPlan) -> FragmentTail {
    let mut tail = FragmentTail::default();
    for op in plan.operators.iter().filter(|op| op.node.is_none()) {
        match &op.kind {
            OpKind::Limit { skip, fetch } => {
                tail.skip = *skip;
                tail.fetch = *fetch;
            }
            OpKind::Sort { .. } => tail.order_groups_by_count = true,
            _ => {}
        }
    }
    tail
}

#[cfg(test)]
mod tests;
