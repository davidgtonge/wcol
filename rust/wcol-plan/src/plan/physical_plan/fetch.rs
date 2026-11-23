//! Coalesce each node's reads into bounded fetch requests.

use super::*;

pub(super) struct PieceUnit {
    pub(super) node: NodeId,
    pub(super) object_id: String,
    pub(super) abs_offset: u64,
    pub(super) length: u64,
    pub(super) read_id: u32,
    pub(super) read_offset: u64,
}

pub(super) struct OpenFetch {
    pub(super) node: NodeId,
    pub(super) object_id: String,
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) read_ids: BTreeMap<u32, ()>,
}

pub(super) fn split_read_pieces(read: &LogicalRead, max: u64) -> Result<Vec<PieceUnit>, PlanError> {
    if read.length == 0 {
        return Err(PlanError::EmptyRange);
    }
    let cap = max.max(1);
    let mut out = Vec::new();
    let mut read_offset = 0u64;
    let mut abs = read.offset;
    while read_offset < read.length {
        let take = (read.length - read_offset).min(cap);
        out.push(PieceUnit {
            node: read.node,
            object_id: read.object_id.clone(),
            abs_offset: abs,
            length: take,
            read_id: read.id,
            read_offset,
        });
        read_offset += take;
        abs += take;
    }
    Ok(out)
}

pub(super) fn coalesce(
    reads: &[LogicalRead],
    nodes: &[NodeId],
    config: &PhysicalPlannerConfig,
) -> Result<(Vec<FetchRequest>, Vec<Vec<FetchPiece>>), PlanError> {
    let mut units = Vec::new();
    for read in reads {
        units.extend(split_read_pieces(read, config.max_request_bytes)?);
    }
    let mut fetches = Vec::new();
    for node in nodes {
        let mut mine: Vec<&PieceUnit> = units.iter().filter(|unit| unit.node == *node).collect();
        mine.sort_by(|left, right| {
            left.object_id
                .cmp(&right.object_id)
                .then(left.abs_offset.cmp(&right.abs_offset))
                .then(left.read_id.cmp(&right.read_id))
                .then(left.read_offset.cmp(&right.read_offset))
        });
        let mut open: Option<OpenFetch> = None;
        for unit in mine {
            let end = range_end(unit.abs_offset, unit.length)?;
            let can_merge = match &open {
                Some(group)
                    if group.object_id == unit.object_id && unit.abs_offset >= group.start =>
                {
                    let gap = unit.abs_offset.saturating_sub(group.end);
                    let new_end = group.end.max(end);
                    let new_len = new_end - group.start;
                    let overlaps = unit.abs_offset < group.end;
                    gap <= config.max_gap_bytes && (new_len <= config.max_request_bytes || overlaps)
                }
                _ => false,
            };
            if can_merge {
                let group = open.as_mut().ok_or(PlanError::Inconsistent)?;
                group.end = group.end.max(end);
                group.read_ids.insert(unit.read_id, ());
            } else {
                if let Some(group) = open.take() {
                    fetches.push(finish_fetch(fetches.len() as u32, group)?);
                }
                let mut read_ids = BTreeMap::new();
                read_ids.insert(unit.read_id, ());
                open = Some(OpenFetch {
                    node: *node,
                    object_id: unit.object_id.clone(),
                    start: unit.abs_offset,
                    end,
                    read_ids,
                });
            }
        }
        if let Some(group) = open.take() {
            fetches.push(finish_fetch(fetches.len() as u32, group)?);
        }
    }

    let mut piece_lists = vec![Vec::new(); reads.len()];
    for fetch in &fetches {
        let fetch_end = range_end(fetch.offset, fetch.length)?;
        for read_id in &fetch.logical_reads {
            let read = reads
                .get(*read_id as usize)
                .ok_or(PlanError::Inconsistent)?;
            let read_end = range_end(read.offset, read.length)?;
            let start = read.offset.max(fetch.offset);
            let end = read_end.min(fetch_end);
            if start >= end {
                continue;
            }
            piece_lists[*read_id as usize].push(FetchPiece {
                fetch_id: fetch.id,
                fetch_offset: start - fetch.offset,
                read_offset: start - read.offset,
                length: end - start,
            });
        }
    }
    for pieces in &mut piece_lists {
        pieces.sort_by(|left, right| {
            left.read_offset
                .cmp(&right.read_offset)
                .then(left.fetch_id.cmp(&right.fetch_id))
        });
    }
    Ok((fetches, piece_lists))
}

pub(super) fn finish_fetch(id: u32, group: OpenFetch) -> Result<FetchRequest, PlanError> {
    let logical_reads = group.read_ids.keys().copied().collect::<Vec<_>>();
    Ok(FetchRequest {
        id,
        node: group.node,
        object_id: group.object_id,
        offset: group.start,
        length: group
            .end
            .checked_sub(group.start)
            .ok_or(PlanError::Overflow)?,
        logical_reads,
    })
}

pub(super) fn attach_pieces(
    mut reads: Vec<LogicalRead>,
    piece_lists: Vec<Vec<FetchPiece>>,
) -> Result<Vec<LogicalRead>, PlanError> {
    for (read, pieces) in reads.iter_mut().zip(piece_lists) {
        read.pieces = pieces;
        read.fetch_id = read.pieces.first().map(|piece| piece.fetch_id).unwrap_or(0);
    }
    Ok(reads)
}

pub(super) fn metrics_of(
    reads: &[LogicalRead],
    fetches: &[FetchRequest],
    nodes: &[NodeId],
) -> Result<PlannerMetrics, PlanError> {
    let mut node_metrics = Vec::new();
    for node in nodes {
        let logical_bytes = reads
            .iter()
            .filter(|read| read.node == *node)
            .try_fold(0u64, |sum, read| sum.checked_add(read.length))
            .ok_or(PlanError::Overflow)?;
        let mine: Vec<_> = fetches.iter().filter(|fetch| fetch.node == *node).collect();
        if mine.is_empty() && logical_bytes == 0 {
            continue;
        }
        let fetched_bytes = mine
            .iter()
            .try_fold(0u64, |sum, fetch| sum.checked_add(fetch.length))
            .ok_or(PlanError::Overflow)?;
        node_metrics.push(NodeMetrics {
            node: *node,
            logical_bytes,
            fetched_bytes,
            request_count: mine.len() as u32,
        });
    }
    let logical_bytes = node_metrics
        .iter()
        .try_fold(0u64, |sum, node| sum.checked_add(node.logical_bytes))
        .ok_or(PlanError::Overflow)?;
    let fetched_bytes = node_metrics
        .iter()
        .try_fold(0u64, |sum, node| sum.checked_add(node.fetched_bytes))
        .ok_or(PlanError::Overflow)?;
    let mut over_read_bytes = 0u64;
    for fetch in fetches {
        let mut spans = Vec::new();
        for read_id in &fetch.logical_reads {
            let read = reads
                .get(*read_id as usize)
                .ok_or(PlanError::Inconsistent)?;
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
        let over = fetch
            .length
            .checked_sub(covered)
            .ok_or(PlanError::Inconsistent)?;
        over_read_bytes = over_read_bytes
            .checked_add(over)
            .ok_or(PlanError::Overflow)?;
    }
    let over_read_ppm = if fetched_bytes == 0 {
        0
    } else {
        (u128::from(over_read_bytes) * 1_000_000 / u128::from(fetched_bytes)) as u64
    };
    Ok(PlannerMetrics {
        nodes: node_metrics,
        request_count: fetches.len() as u32,
        logical_bytes,
        fetched_bytes,
        over_read_bytes,
        over_read_ppm,
        hash_reducers: 1,
        estimated_distinct_groups: 0,
        estimated_group_bytes: 0,
    })
}

pub(super) fn covered_len(spans: &[(u64, u64)]) -> Result<u64, PlanError> {
    let mut spans = spans.to_vec();
    spans.sort_unstable();
    let mut total = 0u64;
    let mut current: Option<(u64, u64)> = None;
    for (start, end) in spans {
        if end < start {
            return Err(PlanError::Overflow);
        }
        match current {
            None => current = Some((start, end)),
            Some((open, close)) if start <= close => current = Some((open, close.max(end))),
            Some((open, close)) => {
                total = total.checked_add(close - open).ok_or(PlanError::Overflow)?;
                current = Some((start, end));
            }
        }
    }
    if let Some((open, close)) = current {
        total = total.checked_add(close - open).ok_or(PlanError::Overflow)?;
    }
    Ok(total)
}
