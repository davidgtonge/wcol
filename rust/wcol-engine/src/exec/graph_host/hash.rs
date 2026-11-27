//! Hash fan-out: partition plans, the hash reducer and partition egress.

use super::*;

/// How a scan's output is split across reducers.
pub(super) enum FanoutPlan {
    /// Row batches, split as they arrive.
    Rows(BatchPartitionPlan),
    /// A scan-side combiner's encoded partials, already routed by the aggregate.
    Partials { partitions: u32 },
}

impl FanoutPlan {
    pub(super) fn partitions(&self) -> u32 {
        match self {
            FanoutPlan::Rows(plan) => plan.partitions(),
            FanoutPlan::Partials { partitions } => *partitions,
        }
    }
}

pub(super) struct HashFanout {
    pub(super) plan: FanoutPlan,
    pub(super) egress: Vec<RowBatchWcsEgress>,
    pub(super) cursor: usize,
    pub(super) ends_queued: bool,
    /// Partition received at least one data frame. Empty partitions stay closed.
    pub(super) opened: Vec<bool>,
}

pub(super) fn fanout_plan_of(
    fragment: &crate::program::NodeFragment,
) -> WcolResult<Option<FanoutPlan>> {
    for step in &fragment.steps {
        if let crate::program::FragmentStep::HashPartition {
            input,
            partitions,
            hash_version,
        } = step
        {
            return Ok(Some(match input {
                crate::program::PartitionInput::Rows { keys } => {
                    FanoutPlan::Rows(BatchPartitionPlan::Hash(
                        HashPartitionPlan::new(keys.clone(), *partitions, *hash_version)
                            .map_err(WcolError::msg)?,
                    ))
                }
                crate::program::PartitionInput::Partials { .. } => FanoutPlan::Partials {
                    partitions: *partitions,
                },
            }));
        }
        if let crate::program::FragmentStep::RangePartition {
            keys,
            partitions,
            splits,
        } = step
        {
            let splits = splits
                .iter()
                .map(|split| split.iter().cloned().map(Some).collect())
                .collect();
            let plan = crate::ops::RangePartitionPlan::new(keys.clone(), *partitions, splits)
                .map_err(WcolError::msg)?;
            return Ok(Some(FanoutPlan::Rows(BatchPartitionPlan::Range(plan))));
        }
    }
    Ok(None)
}

/// A hash reducer's spill budget when `budget_share` tasks share its isolate's memory.
/// The runtime can place several Durable Objects in one isolate; the planned budget assumes
/// the isolate is the reducer's own. Only the flush target shrinks: the hard ceiling stays,
/// so a grouping that fits alone is never rejected for sharing.
pub(super) fn shared_spill_budget(planned: u64, budget_share: u32) -> u64 {
    planned / u64::from(budget_share.max(1))
}

impl GraphHost {
    /// Row batches from `producer_count` scans, aggregated for one hash partition.
    pub fn open_hash_reducer(
        cur: &mut PlanCursor<'_>,
        capacity: usize,
        budget_share: u32,
    ) -> WcolResult<Self> {
        crate::exec::dict_traffic::reset();
        let producer_count = cur.u32()?;
        if producer_count == 0 {
            return Err(WcolError::from("hash reducer has no scan producers"));
        }
        let catalog = cur.catalog()?;
        let key_len = cur.u32()? as usize;
        let mut group_by = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            group_by.push(cur.u32()?);
        }
        let agg_bytes = cur.u32()? as usize;
        let mut agg_cur = cur.bytes(agg_bytes)?;
        let agg_len = crate::program::codec::read_u32(&mut agg_cur)
            .map_err(|err| WcolError::msg(format!("hash reducer aggregate: {err:?}")))?
            as usize;
        let mut aggregates = Vec::with_capacity(agg_len);
        for _ in 0..agg_len {
            aggregates.push(
                crate::program::codec::decode_agg(&mut agg_cur)
                    .map_err(|err| WcolError::msg(format!("hash reducer aggregate: {err:?}")))?,
            );
        }
        if !agg_cur.is_empty() {
            return Err(WcolError::from("trailing bytes in hash reducer aggregates"));
        }
        let skip = cur.u64()?;
        let fetch = match cur.u8()? {
            0 => None,
            _ => Some(cur.u64()?),
        };
        let rank_raw = cur.u32()?;
        let rank = if rank_raw == u32::MAX {
            None
        } else {
            Some(rank_raw)
        };
        let group_budget = cur.u64()?;
        let group_spill = cur.u8()? != 0;
        let (spill_combined, partition) = if group_spill {
            (cur.u64()?, cur.u32()?)
        } else {
            (0, 0)
        };
        if !cur.exhausted() {
            return Err(WcolError::from("trailing bytes after hash reducer"));
        }
        let limit = fetch.map(|n| u32::try_from(n).unwrap_or(u32::MAX));
        let offset = u32::try_from(skip).unwrap_or(u32::MAX);
        let ir = crate::program::to_ir::aggregates(&aggregates).map_err(WcolError::msg)?;
        let mut aggregate =
            AggregateOp::from_aggregate(&group_by, &ir, limit, offset, rank.is_some(), &catalog)
                .map_err(WcolError::msg)?;
        aggregate.set_final_aggregate(true);
        // A COUNT(DISTINCT) table holds (group, value) pairs: rank the folded counts instead.
        // The planner only ranks one when the exchange partitioned on the group alone.
        let distinct = ir
            .iter()
            .any(|agg| agg.kind == crate::ir::AggKind::CountDistinct);
        let fold = match (rank, fetch) {
            (Some(agg), Some(fetch)) if distinct => {
                DistinctFold::Rank(Some(crate::ops::query::FoldedTopCounts::new(
                    agg as usize,
                    usize::try_from(skip.saturating_add(fetch)).unwrap_or(usize::MAX),
                )))
            }
            _ => {
                aggregate.set_final_rank(rank);
                DistinctFold::Merge(None)
            }
        };
        // The hard ceiling applies with spill on. Spill flushes near the target;
        // this rejects a grouping structure that still exceeds `group_budget`.
        if group_budget > 0 {
            aggregate.set_group_budget(group_budget);
        }
        let graph = LinearColocatedGraph::from_plan_parts(None, vec![], vec![], false);
        if group_spill {
            let combined = shared_spill_budget(spill_combined, budget_share).max(64 * 1024);
            // A small encode reserve, so the flush starts near the target.
            // The hard ceiling is `group_budget`, above this target.
            let mut scratch = (combined / 32).clamp(4 * 1024, 1024 * 1024);
            if scratch >= combined {
                scratch = combined / 4;
            }
            let config = crate::spill::SpillConfig {
                combined_budget_bytes: combined,
                scratch_reserve_bytes: scratch,
                ..crate::spill::SpillConfig::default()
            };
            let session = u64::from(partition) + 1;
            let bridge = Rc::new(RefCell::new(HostSpillBridge::new(session)));
            let store: Box<dyn crate::spill::SpillStore> =
                Box::new(HostR2SortedRunStore::new(bridge.clone()));
            aggregate
                .enable_group_spill(config, store)
                .map_err(WcolError::msg)?;
            let mut host = Self::new(graph, wcs_row_batches(producer_count), capacity)?;
            host.core.row_aggregate = Some(aggregate);
            host.core.spill_drive.distinct = fold;
            host.core.spill_bridge = Some(bridge);
            return Ok(host);
        }
        let mut host = Self::new(graph, wcs_row_batches(producer_count), capacity)?;
        host.core.row_aggregate = Some(aggregate);
        host.core.spill_drive.distinct = fold;
        Ok(host)
    }
}

impl GraphHostCore {
    pub(super) fn stage_hash_frame(&mut self) -> WcolResult<()> {
        if self.row_egress_pending() {
            return Ok(());
        }
        if matches!(
            self.hash_fanout.as_ref().map(|fanout| &fanout.plan),
            Some(FanoutPlan::Partials { .. })
        ) {
            return self.stage_hash_partials();
        }
        if let Some(batch) = self.graph.pop_row_batch() {
            return self.push_hash_batch(&batch);
        }
        if !self.graph.ingest_complete() {
            return Ok(());
        }
        self.graph
            .flush_row_ops()
            .map_err(|e| WcolError::msg(format!("row flush failed: {e}")))?;
        if let Some(batch) = self.graph.pop_row_batch() {
            return self.push_hash_batch(&batch);
        }
        self.finish_hash_ends()
    }

    /// A combiner scan sends its aggregate's partial state once ingest completes: each row
    /// to the reducer its raw rows would reach, then every opened partition's end.
    /// A combiner routes its groups itself (`AggregateOp::pop_routed_payload`): send each
    /// encoded partial as it comes, and once ingest completes, finish the combiner, send the
    /// rest, then every opened partition's end.
    pub(super) fn stage_hash_partials(&mut self) -> WcolResult<()> {
        if self
            .hash_fanout
            .as_ref()
            .is_some_and(|fanout| fanout.ends_queued)
        {
            return Ok(());
        }
        if self.send_routed_payloads()? {
            return Ok(());
        }
        if !self.graph.ingest_complete() {
            return Ok(());
        }
        self.graph
            .finish_routed()
            .map_err(|e| WcolError::msg(format!("combiner finish failed: {e}")))?;
        self.send_routed_payloads()?;
        self.finish_hash_ends()
    }

    /// Queue every payload the combiner has ready. `true` when there were any.
    pub(super) fn send_routed_payloads(&mut self) -> WcolResult<bool> {
        let mut sent = false;
        while let Some((partition, payload)) = self.graph.pop_routed_payload() {
            let fanout = self
                .hash_fanout
                .as_mut()
                .ok_or_else(|| WcolError::from("missing hash fanout"))?;
            let egress = fanout
                .egress
                .get_mut(partition as usize)
                .ok_or_else(|| WcolError::from("combiner routed past the last partition"))?;
            egress
                .push_partial_payload(payload, partition)
                .map_err(|e| WcolError::msg(format!("hash partition partial: {e}")))?;
            fanout.opened[partition as usize] = true;
            sent = true;
        }
        Ok(sent)
    }

    pub(super) fn push_hash_batch(
        &mut self,
        batch: &crate::stream::OwnedWcolBatch,
    ) -> WcolResult<()> {
        let fanout = self
            .hash_fanout
            .as_mut()
            .ok_or_else(|| WcolError::from("missing hash fanout"))?;
        let FanoutPlan::Rows(plan) = &fanout.plan else {
            return Err(WcolError::from("hash fanout routes partials, not rows"));
        };
        let parts = crate::ops::split_owned_batch(plan, batch).map_err(WcolError::msg)?;
        for (index, part) in parts.into_iter().enumerate() {
            if part.row_count() == 0 {
                continue;
            }
            fanout.egress[index]
                .push_batch_partition(part, index as u32)
                .map_err(|e| WcolError::msg(format!("hash partition frame: {e}")))?;
            fanout.opened[index] = true;
        }
        Ok(())
    }

    pub(super) fn finish_hash_ends(&mut self) -> WcolResult<()> {
        let fanout = self
            .hash_fanout
            .as_mut()
            .ok_or_else(|| WcolError::from("missing hash fanout"))?;
        if fanout.ends_queued {
            return Ok(());
        }
        for (index, egress) in fanout.egress.iter_mut().enumerate() {
            if !fanout.opened[index] {
                continue;
            }
            egress
                .push_end_partition(index as u32)
                .map_err(|e| WcolError::msg(format!("hash partition end: {e}")))?;
        }
        fanout.ends_queued = true;
        self.row_end_queued = true;
        Ok(())
    }

    pub(super) fn write_hash_frame(&mut self, output: &mut [u8]) -> WcolResult<Option<usize>> {
        let fanout = self
            .hash_fanout
            .as_mut()
            .ok_or_else(|| WcolError::from("missing hash fanout"))?;
        let parts = fanout.egress.len();
        if parts == 0 {
            return Ok(None);
        }
        let mid = (0..parts).find(|index| fanout.egress[*index].mid_frame());
        let index = match mid {
            Some(index) => index,
            None => {
                let start = fanout.cursor % parts;
                let Some(index) = (0..parts)
                    .map(|offset| (start + offset) % parts)
                    .find(|index| fanout.egress[*index].has_pending_output())
                else {
                    return Ok(None);
                };
                fanout.cursor = index.wrapping_add(1);
                index
            }
        };
        let n = fanout.egress[index].poll_write(output)?;
        if n > 0 {
            return Ok(Some(n));
        }
        Ok(None)
    }
}

pub(super) fn push_row_frame(
    egress: &mut RowBatchWcsEgress,
    batch: &crate::stream::OwnedWcolBatch,
) -> WcolResult<()> {
    match egress.push_batch(batch) {
        Ok(()) => Ok(()),
        Err(err) if batch.row_count() > 1 && err.as_str().contains("too large") => {
            let mid = batch.row_count() / 2;
            let mut left = crate::stream::OwnedWcolBatch::empty();
            let mut right = crate::stream::OwnedWcolBatch::empty();
            left.append_rows_from(batch, &(0..mid).collect::<Vec<_>>())
                .map_err(WcolError::msg)?;
            right
                .append_rows_from(batch, &(mid..batch.row_count()).collect::<Vec<_>>())
                .map_err(WcolError::msg)?;
            push_row_frame(egress, &left)?;
            push_row_frame(egress, &right)
        }
        Err(err) => Err(err),
    }
}
