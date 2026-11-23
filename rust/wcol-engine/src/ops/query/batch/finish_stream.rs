//! Bounded spill finalization: emit partial-state chunks without holding all groups.

use super::BatchQuery;
use crate::spill::SpillProgress;
use crate::spill_pull;
use crate::stream::partials::{
    partial_state_concat, PartialColumnsBuilder, PartialStateSchemaV1, PartialStateWcol,
    PARTIAL_SCHEMA_VERSION,
};

pub(crate) struct BatchQueryFinish {
    key_len: usize,
    schema: PartialStateSchemaV1,
    passed_rows: u64,
    chunk_rows: usize,
    phase: FinishPhase,
    main_slots: Vec<u32>,
    main_index: usize,
    rank: Option<FinalizeRank>,
    pending_groups: Vec<crate::ops::query::PartialGroup>,
    total_groups: u64,
}

enum FinishPhase {
    MainToRank,
    MainChunks,
    SpillToRank,
    SpillChunks,
    EmitRank,
    Done,
}

struct FinalizeRank {
    agg: usize,
    keep: usize,
    heap: Vec<crate::ops::query::PartialGroup>,
}

impl FinalizeRank {
    fn new(agg: usize, keep: usize) -> Self {
        Self {
            agg,
            keep,
            heap: Vec::new(),
        }
    }

    fn consider(&mut self, query: &BatchQuery, group: crate::ops::query::PartialGroup) {
        if self.heap.len() < self.keep {
            self.heap.push(group);
            let last = self.heap.len() - 1;
            sift_up(query, &mut self.heap, last, self.agg);
        } else if ranks_above_group(query, &group, &self.heap[0], self.agg) {
            self.heap[0] = group;
            sift_down(query, &mut self.heap, self.agg);
        }
    }

    fn into_sorted(mut self, query: &BatchQuery) -> Vec<crate::ops::query::PartialGroup> {
        self.heap
            .sort_by(|a, b| compare_groups_rank(query, a, b, self.agg));
        self.heap
    }
}

impl BatchQueryFinish {
    pub(crate) fn begin(query: &mut BatchQuery) -> Result<Self, String> {
        let key_len = if query.group_by.is_empty() {
            0
        } else {
            query.group_by.len()
        };
        let schema = PartialStateSchemaV1 {
            version: PARTIAL_SCHEMA_VERSION,
            grouped: query.shell.grouped,
            order_groups_by_count: query.shell.order_groups_by_count,
            limit: query.shell.limit,
            offset: query.shell.offset,
            group_key_len: key_len as u8,
            group_key_float_mask: query.group_key_float,
            group_key_string_mask: query.group_key_text,
            aggs: query.partial_aggs(),
        };
        let chunk_rows = query
            .spill
            .as_ref()
            .map(|s| s.config().finalize_batch_rows)
            .unwrap_or(256);
        let rank = finalize_rank_plan(query);
        let main_slots = if key_len == 0 {
            Vec::new()
        } else if query.spill.is_some() {
            query.sorted_slots()
        } else {
            query.emit_slots()
        };
        let phase = if query.spill.is_none() {
            FinishPhase::Done
        } else if rank.is_some() {
            FinishPhase::MainToRank
        } else {
            FinishPhase::MainChunks
        };
        Ok(Self {
            key_len,
            schema,
            passed_rows: query.passed_rows,
            chunk_rows,
            phase,
            main_slots,
            main_index: 0,
            rank,
            pending_groups: Vec::new(),
            total_groups: 0,
        })
    }

    pub(crate) fn next_chunk(
        &mut self,
        query: &mut BatchQuery,
    ) -> Result<SpillProgress<Option<PartialStateWcol>>, String> {
        if matches!(self.phase, FinishPhase::Done) && self.pending_groups.is_empty() {
            return Ok(SpillProgress::Ready(None));
        }
        if query.spill.is_none() {
            let state = query.finish_partial_state_eager();
            self.phase = FinishPhase::Done;
            return Ok(SpillProgress::Ready(Some(state)));
        }

        loop {
            match self.phase {
                FinishPhase::MainToRank => {
                    let rank = self.rank.as_mut().expect("rank");
                    while self.main_index < self.main_slots.len() {
                        let slot = self.main_slots[self.main_index];
                        self.main_index += 1;
                        rank.consider(query, query.partial_group_from_main_slot(slot));
                    }
                    spill_pull!(query.spill.as_mut().expect("spill").begin_merge_scan());
                    self.phase = FinishPhase::SpillToRank;
                }
                FinishPhase::MainChunks => {
                    if self.main_index < self.main_slots.len() {
                        return Ok(SpillProgress::Ready(self.emit_main_chunk(query)?));
                    }
                    spill_pull!(query.spill.as_mut().expect("spill").begin_merge_scan());
                    self.phase = FinishPhase::SpillChunks;
                }
                FinishPhase::SpillToRank => {
                    let partial_aggs = query.partial_aggs();
                    let spill = query.spill.as_mut().expect("spill");
                    let (completed, exhausted) = spill_pull!(spill.merge_step(&partial_aggs));
                    query.stamp_memory_phase(crate::exec::mem_growth::PHASE_SPILL_MERGE);
                    let rank = self.rank.as_mut().expect("rank");
                    for group in completed {
                        rank.consider(query, group);
                    }
                    if exhausted {
                        self.phase = FinishPhase::EmitRank;
                    }
                }
                FinishPhase::SpillChunks => {
                    while self.pending_groups.len() < self.chunk_rows {
                        let partial_aggs = query.partial_aggs();
                        let (completed, exhausted) = {
                            let spill = query.spill.as_mut().expect("spill");
                            spill_pull!(spill.merge_step(&partial_aggs))
                        };
                        query.stamp_memory_phase(crate::exec::mem_growth::PHASE_SPILL_MERGE);
                        self.pending_groups.extend(completed);
                        if exhausted {
                            self.phase = FinishPhase::Done;
                            query.spill.as_mut().expect("spill").end_merge_scan()?;
                            break;
                        }
                        if !self.pending_groups.is_empty() {
                            break;
                        }
                    }
                    if self.pending_groups.is_empty() && matches!(self.phase, FinishPhase::Done) {
                        return Ok(SpillProgress::Ready(None));
                    }
                    return Ok(SpillProgress::Ready(self.emit_pending_chunk(query)?));
                }
                FinishPhase::EmitRank => {
                    let rank = self.rank.take().expect("rank");
                    let groups = rank.into_sorted(query);
                    self.total_groups = groups.len() as u64;
                    self.phase = FinishPhase::Done;
                    query.note_groups(query.groups.len() as u64, self.total_groups);
                    query.spill.as_mut().expect("spill").end_merge_scan()?;
                    if query.group_key_text != 0 || query.group_key_float != 0 {
                        return Ok(SpillProgress::Ready(Some(
                            query.partial_from_groups(groups)?,
                        )));
                    }
                    let mut builder =
                        PartialColumnsBuilder::new(self.key_len, &self.schema.aggs, groups.len());
                    for group in groups {
                        query.push_partial_group_to_builder(&mut builder, &group);
                    }
                    return Ok(SpillProgress::Ready(Some(builder.finish(
                        self.schema.clone(),
                        self.passed_rows,
                        Vec::new(),
                    ))));
                }
                // The exhausting merge step can complete more groups than one chunk holds.
                FinishPhase::Done if !self.pending_groups.is_empty() => {
                    return Ok(SpillProgress::Ready(self.emit_pending_chunk(query)?));
                }
                FinishPhase::Done => return Ok(SpillProgress::Ready(None)),
            }
        }
    }

    fn emit_main_chunk(
        &mut self,
        query: &mut BatchQuery,
    ) -> Result<Option<PartialStateWcol>, String> {
        if query.group_key_text != 0 || query.group_key_float != 0 {
            let mut groups = Vec::new();
            while groups.len() < self.chunk_rows && self.main_index < self.main_slots.len() {
                let slot = self.main_slots[self.main_index];
                self.main_index += 1;
                groups.push(query.partial_group_from_main_slot(slot));
            }
            self.total_groups += groups.len() as u64;
            query.note_groups(query.groups.len() as u64, self.total_groups);
            return query.partial_from_groups(groups).map(Some);
        }
        let mut builder =
            PartialColumnsBuilder::new(self.key_len, &self.schema.aggs, self.chunk_rows);
        let mut pushed = 0usize;
        while pushed < self.chunk_rows && self.main_index < self.main_slots.len() {
            let slot = self.main_slots[self.main_index];
            self.main_index += 1;
            query.push_partial_group_to_builder(
                &mut builder,
                &query.partial_group_from_main_slot(slot),
            );
            pushed += 1;
            self.total_groups += 1;
        }
        query.note_groups(query.groups.len() as u64, self.total_groups);
        Ok(Some(builder.finish(
            self.schema.clone(),
            self.passed_rows,
            Vec::new(),
        )))
    }

    fn emit_pending_chunk(
        &mut self,
        query: &mut BatchQuery,
    ) -> Result<Option<PartialStateWcol>, String> {
        let take = self.pending_groups.len().min(self.chunk_rows);
        if query.group_key_text != 0 || query.group_key_float != 0 {
            let groups: Vec<_> = self.pending_groups.drain(..take).collect();
            self.total_groups += groups.len() as u64;
            query.note_groups(query.groups.len() as u64, self.total_groups);
            return query.partial_from_groups(groups).map(Some);
        }
        let mut builder = PartialColumnsBuilder::new(self.key_len, &self.schema.aggs, take);
        for group in self.pending_groups.drain(..take) {
            query.push_partial_group_to_builder(&mut builder, &group);
            self.total_groups += 1;
        }
        query.note_groups(query.groups.len() as u64, self.total_groups);
        Ok(Some(builder.finish(
            self.schema.clone(),
            self.passed_rows,
            Vec::new(),
        )))
    }

    pub(crate) fn finalize_pending_rows(&self) -> usize {
        self.pending_groups.len().min(self.chunk_rows)
    }
}

fn finalize_rank_plan(query: &BatchQuery) -> Option<FinalizeRank> {
    if !query.final_aggregate {
        return None;
    }
    let agg = query.final_rank? as usize;
    if agg >= query.aggs.len() {
        return None;
    }
    let limit = query.shell.limit;
    if limit == u32::MAX {
        return None;
    }
    let keep = (query.shell.offset as usize).saturating_add(limit as usize);
    if keep == 0 {
        return None;
    }
    Some(FinalizeRank::new(agg, keep))
}

fn group_count(_query: &BatchQuery, group: &crate::ops::query::PartialGroup, agg: usize) -> u64 {
    group.stats[agg].count
}

fn ranks_above_group(
    query: &BatchQuery,
    a: &crate::ops::query::PartialGroup,
    b: &crate::ops::query::PartialGroup,
    agg: usize,
) -> bool {
    let ca = group_count(query, a, agg);
    let cb = group_count(query, b, agg);
    ca > cb || (ca == cb && compare_key_parts_slice(&a.key, &b.key) == std::cmp::Ordering::Less)
}

fn compare_groups_rank(
    query: &BatchQuery,
    a: &crate::ops::query::PartialGroup,
    b: &crate::ops::query::PartialGroup,
    agg: usize,
) -> std::cmp::Ordering {
    if ranks_above_group(query, a, b, agg) {
        std::cmp::Ordering::Less
    } else if ranks_above_group(query, b, a, agg) {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

fn compare_key_parts_slice(
    a: &[crate::ops::query::PartialKeyPart],
    b: &[crate::ops::query::PartialKeyPart],
) -> std::cmp::Ordering {
    use crate::ops::query::PartialKeyPart::*;
    for (left, right) in a.iter().zip(b.iter()) {
        let ord = match (left, right) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Null, _) => std::cmp::Ordering::Less,
            (_, Null) => std::cmp::Ordering::Greater,
            (Int(x), Int(y)) => x.cmp(y),
            (Float(x), Float(y)) => x.cmp(y),
            (Bytes(x), Bytes(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Less,
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn sift_up(
    query: &BatchQuery,
    heap: &mut [crate::ops::query::PartialGroup],
    mut index: usize,
    agg: usize,
) {
    while index > 0 {
        let parent = (index - 1) / 2;
        if ranks_above_group(query, &heap[parent], &heap[index], agg) {
            heap.swap(parent, index);
            index = parent;
        } else {
            break;
        }
    }
}

fn sift_down(query: &BatchQuery, heap: &mut [crate::ops::query::PartialGroup], agg: usize) {
    let mut index = 0;
    loop {
        let left = 2 * index + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let mut worst = left;
        if right < heap.len() && ranks_above_group(query, &heap[left], &heap[right], agg) {
            worst = right;
        }
        if ranks_above_group(query, &heap[index], &heap[worst], agg) {
            heap.swap(index, worst);
            index = worst;
        } else {
            break;
        }
    }
}

pub(crate) fn finish_partial_state_streaming(
    query: &mut BatchQuery,
) -> Result<PartialStateWcol, String> {
    let mut finish = BatchQueryFinish::begin(query)?;
    let mut chunks: Vec<PartialStateWcol> = Vec::new();
    loop {
        match finish.next_chunk(query)? {
            SpillProgress::AwaitHost => return Err("spill finish suspended".into()),
            SpillProgress::Ready(Some(chunk)) => chunks.push(chunk),
            SpillProgress::Ready(None) => break,
        }
    }
    if chunks.is_empty() {
        return Ok(query.empty_grouped_partial());
    }
    if chunks.len() == 1 {
        return Ok(chunks.pop().expect("one chunk"));
    }
    let mut iter = chunks.into_iter();
    let first = iter.next().expect("non-empty");
    iter.try_fold(first, partial_state_concat)
}
