//! Drive a hash-partitioned aggregate: each scan runs once, its rows are demuxed by the
//! partition stamped in the WCS header, and each reducer aggregates only its own key range.

use crate::plan::distributed::coordinator_reduce_partials;
use crate::plan::execution_graph::{
    execution_plan_for, ExecutionPlan, ExecutionTask, WHOLE_STREAM,
};
use crate::plan::node_fragment::rank_count_aggregate;
use crate::plan::physical_plan::{OpKind, PhysicalPlannerConfig, WcolPhysicalPlan};
use crate::plan::query_dispatch::{NodeProgram, QueryDispatch};
use crate::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use crate::stream::partial_wcs::partial_state_from_wire_payload;
use crate::stream::partials::{
    partial_snapshot_from_state, partial_state_from_snapshot, PartialStateWcol,
};

const RING: usize = 256 * 1024;

/// What one hash aggregate run moved, for the planner's memory check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashAggregateStats {
    pub reducers: u32,
    /// Groups still in each reducer's hash table at finish, before a local top-k.
    pub peak_groups: Vec<usize>,
    /// Groups each reducer sent to the finalizer, after its local top-k.
    pub output_groups: Vec<usize>,
    pub shuffle_bytes: u64,
}

/// Run the planned hash exchange. `read` serves scan `ReadAt` from the source object.
pub fn run_hash_aggregate(
    programs: &[NodeProgram],
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
    read: &mut dyn FnMut(u64, u32) -> Result<Vec<u8>, String>,
) -> Result<(PartialStateWcol, HashAggregateStats), String> {
    let exec = execution_plan_for(&QueryDispatch::Nodes(programs.to_vec()), plan, config, None)?;
    let ExecutionPlan::Graph { tasks, edges, .. } = exec else {
        return Err("hash aggregate did not produce an execution graph".into());
    };
    let partitions = plan
        .operators
        .iter()
        .find_map(|op| match &op.kind {
            OpKind::HashExchange { partitions, .. } => Some(*partitions),
            _ => None,
        })
        .ok_or("hash aggregate plan has no hash exchange")?;
    let scan_ids: Vec<u32> = edges
        .iter()
        .filter(|edge| edge.partition != WHOLE_STREAM)
        .map(|edge| edge.from)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let reducer_ids: Vec<u32> = edges
        .iter()
        .filter(|edge| edge.partition != WHOLE_STREAM)
        .map(|edge| edge.to)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if scan_ids.len() != programs.len() || reducer_ids.len() != partitions as usize {
        return Err(format!(
            "hash graph has {} scans and {} reducers, expected {} and {partitions}",
            scan_ids.len(),
            reducer_ids.len(),
            programs.len()
        ));
    }
    let mut parts = vec![Vec::new(); partitions as usize];
    let mut shuffle_bytes = 0u64;
    for scan in &scan_ids {
        let task = task(&tasks, *scan)?;
        let wire = drive(&task.program, None, read)?;
        shuffle_bytes += wire.len() as u64;
        let split = crate::ops::demux_partition_stream(&wire, partitions)?;
        for (index, bytes) in split.into_iter().enumerate() {
            parts[index].extend_from_slice(&bytes);
        }
    }
    let mut partials = Vec::with_capacity(reducer_ids.len());
    let mut peak_groups = Vec::with_capacity(reducer_ids.len());
    let mut output_groups = Vec::with_capacity(reducer_ids.len());
    for (index, reducer) in reducer_ids.iter().enumerate() {
        let task = task(&tasks, *reducer)?;
        if parts[index].is_empty() {
            peak_groups.push(0);
            output_groups.push(0);
            continue;
        }
        let wire = drive(&task.program, Some(&parts[index]), read)?;
        let partial = partial_payload(&wire)?;
        let state = partial_state_from_wire_payload(&partial)?;
        peak_groups.push(crate::ops::query::batch::last_resident_groups() as usize);
        output_groups.push(state.row_count);
        partials.push(state);
    }
    let mut merged = merge_reducer_partials(partials)?;
    if let Some(rank) = rank_count_aggregate(plan) {
        let (skip, fetch) = plan_limit(plan);
        let keep = skip.saturating_add(fetch.unwrap_or(0));
        if fetch.is_some() && keep > 0 {
            let mut snap = partial_snapshot_from_state(&merged)?;
            crate::ops::query::keep_top_count_groups(&mut snap, rank as usize, keep as usize);
            merged = partial_state_from_snapshot(&snap)?;
        }
    }
    Ok((
        merged,
        HashAggregateStats {
            reducers: partitions,
            peak_groups,
            output_groups,
            shuffle_bytes,
        },
    ))
}

/// An empty partition has no key to size the schema. Drop those partials; their row count is 0.
fn merge_reducer_partials(partials: Vec<PartialStateWcol>) -> Result<PartialStateWcol, String> {
    let Some(first) = partials.iter().position(|state| state.row_count > 0) else {
        return partials
            .into_iter()
            .next()
            .ok_or_else(|| "hash aggregate produced no reducer partial".to_string());
    };
    let mut rest = partials;
    let head = rest.remove(first);
    let occupied: Vec<_> = rest
        .into_iter()
        .filter(|state| state.row_count > 0)
        .collect();
    if occupied.is_empty() {
        return Ok(head);
    }
    coordinator_reduce_partials(std::iter::once(head).chain(occupied).collect())
}

fn plan_limit(plan: &WcolPhysicalPlan) -> (u64, Option<u64>) {
    let mut skip = 0u64;
    let mut fetch = None;
    for op in plan.operators.iter().filter(|op| op.node.is_none()) {
        if let OpKind::Limit {
            skip: n,
            fetch: limit,
        } = &op.kind
        {
            skip = *n;
            fetch = *limit;
        }
    }
    (skip, fetch)
}

fn task(tasks: &[ExecutionTask], id: u32) -> Result<&ExecutionTask, String> {
    tasks
        .iter()
        .find(|task| task.id == id)
        .ok_or_else(|| format!("execution graph is missing task {id}"))
}

fn partial_payload(wire: &[u8]) -> Result<Vec<u8>, String> {
    let mut offset = 0usize;
    let mut last = None;
    while offset + WCS_HEADER_BYTES <= wire.len() {
        let header = &wire[offset..offset + WCS_HEADER_BYTES];
        if u32::from_le_bytes(header[..4].try_into().unwrap()) != WCS_MAGIC {
            break;
        }
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let end = offset + WCS_HEADER_BYTES + len;
        if end > wire.len() {
            return Err("truncated partial frame".into());
        }
        if header[5] == 4 {
            last = Some(wire[offset + WCS_HEADER_BYTES..end].to_vec());
        }
        offset = end;
    }
    last.ok_or_else(|| "reducer produced no partial".into())
}

/// `feed` is the reducer's already-demuxed input. A scan passes `None` and uses `read`.
fn drive(
    program: &[u8],
    feed: Option<&[u8]>,
    read: &mut dyn FnMut(u64, u32) -> Result<Vec<u8>, String>,
) -> Result<Vec<u8>, String> {
    let mut pipe = crate::exec::open_wcol_plan(program, RING).map_err(|err| err.to_string())?;
    let mut sent = 0usize;
    let mut ended = feed.is_none();
    let mut wire = Vec::new();
    for _ in 0..10_000_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let bytes = read(offset, len)?;
                if bytes.len() != len as usize {
                    return Err("short read".into());
                }
                for part in bytes.chunks(RING) {
                    pipe.input()[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len())
                        .map_err(|err| err.to_string())?;
                    pipe.complete_read(ticket).map_err(|err| err.to_string())?;
                }
            }
            Action::NeedInput | Action::Yield => {
                if let Some(input) = feed {
                    if sent < input.len() {
                        let room = pipe.input().len();
                        let n = room.min(input.len() - sent);
                        if n == 0 {
                            continue;
                        }
                        pipe.input()[..n].copy_from_slice(&input[sent..sent + n]);
                        pipe.commit_input(n).map_err(|err| err.to_string())?;
                        sent += n;
                    } else if !ended {
                        pipe.end_input().map_err(|err| err.to_string())?;
                        ended = true;
                    }
                }
            }
            Action::HaveOutput => {
                wire.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len())
                    .map_err(|err| err.to_string())?;
            }
            Action::Finished => return Ok(wire),
            Action::SpillExec => {
                return Err("group spill requires a Wasm host (SpillExec)".into());
            }
            Action::Failed => {
                return Err(pipe.error().unwrap_or("program failed").to_string());
            }
        }
    }
    Err("hash exchange did not finish".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::physical_plan::OpKind;
    use crate::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
    use std::path::PathBuf;

    fn flat() -> (Vec<u8>, crate::formats::parquet::Plan) {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
        let bytes = std::fs::read(&path).unwrap();
        let (_, footer) = crate::formats::parquet::open_plan(&path).unwrap();
        (bytes, footer)
    }

    fn read_of(bytes: &[u8]) -> impl FnMut(u64, u32) -> Result<Vec<u8>, String> + '_ {
        move |offset, len| {
            let at = offset as usize;
            bytes
                .get(at..at + len as usize)
                .map(|slice| slice.to_vec())
                .ok_or_else(|| "read past end of file".to_string())
        }
    }

    fn groups_of(state: &PartialStateWcol) -> Vec<(Vec<crate::ops::query::PartialKeyPart>, u64)> {
        let snap = partial_snapshot_from_state(state).unwrap();
        let mut groups: Vec<_> = snap
            .groups
            .iter()
            .map(|group| (group.key.clone(), group.stats[0].count))
            .collect();
        groups.sort();
        groups
    }

    #[test]
    fn partitioned_group_by_matches_one_aggregate() {
        let (bytes, footer) = flat();
        let sql = "SELECT a, COUNT(*) AS c FROM flat GROUP BY a";
        let mut gather_cfg = PhysicalPlannerConfig::default();
        gather_cfg.hash_aggregate.reducers = Some(1);
        let (dispatch, plan) =
            plan_query_dispatch(sql, "flat", &footer, 1, 32, &gather_cfg).unwrap();
        assert!(plan
            .operators
            .iter()
            .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })));
        let QueryDispatch::Nodes(programs) = dispatch else {
            panic!("expected node programs");
        };
        let wire = drive(&programs[0].program, None, &mut read_of(&bytes)).unwrap();
        let gathered = partial_state_from_wire_payload(&partial_payload(&wire).unwrap()).unwrap();
        let expected = groups_of(&gathered);

        for reducers in [2u32, 4] {
            let mut config = PhysicalPlannerConfig::default();
            config.hash_aggregate.reducers = Some(reducers);
            let (dispatch, plan) =
                plan_query_dispatch(sql, "flat", &footer, 1, 32, &config).unwrap();
            let QueryDispatch::Nodes(programs) = dispatch else {
                panic!("expected node programs");
            };
            let (state, stats) =
                run_hash_aggregate(&programs, &plan, &config, &mut read_of(&bytes)).unwrap();
            assert_eq!(stats.reducers, reducers);
            assert_eq!(stats.peak_groups.len(), reducers as usize);
            assert_eq!(
                stats.peak_groups.iter().sum::<usize>(),
                expected.len(),
                "a group landed on more than one reducer"
            );
            assert_eq!(groups_of(&state), expected);
        }
    }

    #[test]
    fn string_group_by_matches_one_aggregate() {
        let (bytes, footer) = flat();
        let sql = "SELECT b, COUNT(*) AS c FROM flat GROUP BY b";
        let mut gather_cfg = PhysicalPlannerConfig::default();
        gather_cfg.hash_aggregate.reducers = Some(1);
        let (dispatch, plan) =
            plan_query_dispatch(sql, "flat", &footer, 1, 32, &gather_cfg).unwrap();
        assert!(plan
            .operators
            .iter()
            .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })));
        let QueryDispatch::Nodes(programs) = dispatch else {
            panic!("expected node programs");
        };
        let wire = drive(&programs[0].program, None, &mut read_of(&bytes)).unwrap();
        let expected =
            groups_of(&partial_state_from_wire_payload(&partial_payload(&wire).unwrap()).unwrap());
        assert!(!expected.is_empty());

        let mut config = PhysicalPlannerConfig::default();
        config.hash_aggregate.reducers = Some(2);
        let (dispatch, plan) = plan_query_dispatch(sql, "flat", &footer, 1, 32, &config).unwrap();
        assert!(plan
            .operators
            .iter()
            .any(|op| matches!(op.kind, OpKind::HashExchange { partitions: 2, .. })));
        // Text has no footer bound, so each scan combines before the exchange.
        assert!(plan
            .operators
            .iter()
            .any(|op| op.node.is_some() && matches!(op.kind, OpKind::PartialAggregate { .. })));
        let QueryDispatch::Nodes(programs) = dispatch else {
            panic!("expected node programs");
        };
        let (state, stats) =
            run_hash_aggregate(&programs, &plan, &config, &mut read_of(&bytes)).unwrap();
        assert_eq!(stats.reducers, 2);
        assert_eq!(stats.peak_groups.iter().sum::<usize>(), expected.len());
        assert_eq!(groups_of(&state), expected);
    }

    #[test]
    fn region_id_footer_stats_keep_gather() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
        if !path.exists() {
            return;
        }
        let footer = crate::formats::parquet::open_plan(&path).unwrap().1;
        let config = PhysicalPlannerConfig::default();
        let (_, plan) = plan_query_dispatch(
            "SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID",
            "hits",
            &footer,
            1,
            32,
            &config,
        )
        .unwrap();
        assert!(
            plan.operators
                .iter()
                .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })),
            "{}",
            plan.canonical_text()
        );
        assert_eq!(plan.metrics.hash_reducers, 1);
        assert!(
            plan.metrics.estimated_distinct_groups < 10_000,
            "estimated {}",
            plan.metrics.estimated_distinct_groups
        );
    }
}
