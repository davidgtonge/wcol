//! Row-group units: pruning, request costs and placement on nodes.

use super::*;

pub(super) struct DraftRead {
    pub(super) column: u32,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) dictionary: bool,
    pub(super) codec: i32,
}

pub(super) struct Unit {
    pub(super) scan: u32,
    pub(super) slot: usize,
    pub(super) node: NodeId,
    pub(super) object_id: String,
    pub(super) row_group: u32,
    pub(super) rows: u64,
    pub(super) columns: Vec<u32>,
    pub(super) drafts: Vec<DraftRead>,
    pub(super) read_ids: Vec<u32>,
    pub(super) origin: u64,
    pub(super) weight: u64,
}

pub(super) fn resolve(
    scan: u32,
    linear: &Linear,
    metadata: &ParquetMetadataCatalog,
) -> Result<Vec<Unit>, PlanError> {
    let table = find_table(metadata, &linear.table)?;
    let mut objects = table.objects.clone();
    objects.sort_by(|left, right| left.object_id.cmp(&right.object_id));
    if let Some(pair) = objects
        .windows(2)
        .find(|pair| pair[0].object_id == pair[1].object_id)
    {
        return Err(PlanError::DuplicateObject(pair[0].object_id.clone()));
    }
    let predicates: Vec<&Predicate> = linear
        .steps
        .iter()
        .flat_map(|step| match step {
            Step::Filter(predicates) => predicates.as_slice(),
            Step::Project(_) => &[],
        })
        .collect();
    let groups: Vec<(&ParquetObject, u32, Vec<&ChunkMeta>)> = objects
        .iter()
        .flat_map(|object| {
            let mut groups: BTreeMap<u32, Vec<&ChunkMeta>> = BTreeMap::new();
            for chunk in &object.chunks {
                groups.entry(chunk.row_group).or_default().push(chunk);
            }
            groups
                .into_iter()
                .map(move |(row_group, chunks)| (object, row_group, chunks))
        })
        .collect();
    let mut kept: Vec<_> = groups
        .iter()
        .filter(|(_, _, chunks)| !no_row_can_pass(&predicates, chunks))
        .collect();
    if kept.is_empty() {
        // Every row group was pruned. Keep one so the plan still has a scan, and the query
        // answers from an input that filters to nothing, as any other empty match does.
        kept.extend(groups.first());
    }
    let units = kept
        .into_iter()
        .map(|(object, row_group, chunks)| {
            row_group_unit(scan, linear, metadata, object, *row_group, chunks)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if units.is_empty() && !linear.columns.is_empty() {
        return Err(PlanError::NoRowGroups(linear.table.clone()));
    }
    Ok(units)
}

/// No row of the row group whose `chunks` these are can pass every predicate, by the footer
/// bounds of the columns they test. Only integer comparisons and `IN` lists prune.
pub(super) fn no_row_can_pass(predicates: &[&Predicate], chunks: &[&ChunkMeta]) -> bool {
    predicates.iter().any(|predicate| {
        let column = predicate_column(predicate);
        let Some((min, max)) = chunks
            .iter()
            .find(|chunk| chunk.column == column)
            .and_then(|chunk| chunk.int_range)
        else {
            return false;
        };
        match predicate {
            Predicate::Compare(Compare {
                op,
                value: Literal::Int(value),
                ..
            }) => !int_compare_may_pass(*op, *value, min, max),
            Predicate::In {
                values,
                negated: false,
                ..
            } => values
                .iter()
                .all(|value| matches!(value, Literal::Int(value) if !(min..=max).contains(value))),
            _ => false,
        }
    })
}

/// Some value in `[min, max]` satisfies `value op literal`.
pub(super) fn int_compare_may_pass(op: CmpOp, literal: i64, min: i64, max: i64) -> bool {
    match op {
        CmpOp::Eq => min <= literal && literal <= max,
        CmpOp::NotEq => !(min == literal && max == literal),
        CmpOp::Lt => min < literal,
        CmpOp::LtEq => min <= literal,
        CmpOp::Gt => max > literal,
        CmpOp::GtEq => max >= literal,
    }
}

pub(super) fn row_group_unit(
    scan: u32,
    linear: &Linear,
    metadata: &ParquetMetadataCatalog,
    object: &ParquetObject,
    row_group: u32,
    chunks: &[&ChunkMeta],
) -> Result<Unit, PlanError> {
    let rows = chunks[0].rows;
    if chunks.iter().any(|chunk| chunk.rows != rows) {
        return Err(PlanError::InconsistentRows {
            object: object.object_id.clone(),
            row_group,
        });
    }
    let mut origin = chunks.iter().map(|chunk| chunk.offset).min().unwrap_or(0);
    let mut drafts = Vec::new();
    if linear.columns.is_empty() {
        for chunk in chunks {
            if let Some(page) = chunk.dictionary_page {
                origin = origin.min(page.offset);
            }
        }
    } else {
        // The row id is decoded from the row's position: it has no chunk to read.
        let read = linear
            .columns
            .iter()
            .filter(|&&column| Some(column) != metadata.row_id_column());
        for &column in read {
            let matches: Vec<_> = chunks
                .iter()
                .filter(|chunk| chunk.column == column)
                .collect();
            if matches.len() > 1 {
                return Err(PlanError::DuplicateColumn {
                    object: object.object_id.clone(),
                    row_group,
                    column,
                });
            }
            let Some(chunk) = matches.first() else {
                return Err(PlanError::MissingColumn {
                    table: linear.table.clone(),
                    object: object.object_id.clone(),
                    row_group,
                    column,
                });
            };
            range_end(chunk.offset, chunk.length)?;
            if chunk.length == 0 {
                return Err(PlanError::EmptyRange);
            }
            let mut dictionary = chunk.dictionary_in_span;
            if let Some(page) = chunk.dictionary_page {
                range_end(page.offset, page.length)?;
                if page.length == 0 {
                    return Err(PlanError::EmptyRange);
                }
                if span_contains(chunk.offset, chunk.length, page)? {
                    dictionary = true;
                } else {
                    drafts.push(DraftRead {
                        column,
                        offset: page.offset,
                        length: page.length,
                        dictionary: true,
                        codec: chunk.codec,
                    });
                }
            }
            drafts.push(DraftRead {
                column,
                offset: chunk.offset,
                length: chunk.length,
                dictionary,
                codec: chunk.codec,
            });
        }
    }
    drafts.sort_by(|left, right| {
        left.column
            .cmp(&right.column)
            .then(left.offset.cmp(&right.offset))
            .then(u8::from(left.dictionary).cmp(&u8::from(right.dictionary)))
    });
    if let Some(first) = drafts.iter().map(|draft| draft.offset).min() {
        origin = first;
    }
    let mut bytes = 0u64;
    for draft in &drafts {
        bytes = bytes.checked_add(draft.length).ok_or(PlanError::Overflow)?;
    }
    Ok(Unit {
        scan,
        slot: 0,
        node: NodeId(0),
        object_id: object.object_id.clone(),
        row_group,
        rows,
        columns: linear.columns.clone(),
        drafts,
        read_ids: Vec::new(),
        origin,
        weight: bytes.max(1),
    })
}

pub(super) fn find_table<'a>(
    metadata: &'a ParquetMetadataCatalog,
    name: &str,
) -> Result<&'a TableMeta, PlanError> {
    let mut found = None;
    for table in &metadata.tables {
        if table.table == name {
            if found.is_some() {
                return Err(PlanError::DuplicateTable(name.to_string()));
            }
            found = Some(table);
        }
    }
    found.ok_or_else(|| PlanError::UnknownTable(name.to_string()))
}

pub(super) fn span_contains(offset: u64, length: u64, inner: ByteSpan) -> Result<bool, PlanError> {
    let outer_end = range_end(offset, length)?;
    let inner_end = range_end(inner.offset, inner.length)?;
    Ok(inner.offset >= offset && inner_end <= outer_end)
}

pub(super) fn range_end(offset: u64, length: u64) -> Result<u64, PlanError> {
    offset.checked_add(length).ok_or(PlanError::Overflow)
}

/// What one host fetch costs in byte terms when sizing and balancing scan nodes.
/// A ranged R2 GET measured ~170ms on hits; at that latency a node is bound by how many
/// requests it makes, not how many bytes. Weighting each request as 1 MiB keeps row groups
/// with many tiny chunks from piling onto one node.
pub const REQUEST_COST_BYTES: u64 = 1024 * 1024;

/// Fetches one unit will need: its reads merged the way [`coalesce`] merges them
/// (gap at most `max_gap_bytes`, size at most `max_request_bytes`), then split at that cap.
pub(super) fn estimated_requests(drafts: &[DraftRead], config: &PhysicalPlannerConfig) -> u64 {
    let mut spans: Vec<(u64, u64)> = drafts
        .iter()
        .map(|draft| (draft.offset, draft.length))
        .collect();
    spans.sort_unstable();
    let cap = config.max_request_bytes.max(1);
    let mut requests = 0u64;
    let mut current: Option<(u64, u64)> = None;
    for (offset, length) in spans {
        let end = offset.saturating_add(length);
        current = match current {
            Some((start, cur_end))
                if offset <= cur_end.saturating_add(config.max_gap_bytes)
                    && end.max(cur_end) - start <= cap =>
            {
                Some((start, end.max(cur_end)))
            }
            Some((start, cur_end)) => {
                requests = requests.saturating_add((cur_end - start).div_ceil(cap).max(1));
                Some((offset, end))
            }
            None => Some((offset, end)),
        };
    }
    if let Some((start, end)) = current {
        requests = requests.saturating_add((end - start).div_ceil(cap).max(1));
    }
    requests
}

/// Scan cost of one unit: its bytes plus its requests at [`REQUEST_COST_BYTES`] each.
pub(super) fn unit_cost(drafts: &[DraftRead], config: &PhysicalPlannerConfig) -> u64 {
    let bytes = drafts
        .iter()
        .fold(0u64, |sum, draft| sum.saturating_add(draft.length));
    bytes
        .saturating_add(estimated_requests(drafts, config).saturating_mul(REQUEST_COST_BYTES))
        .max(1)
}

pub(super) fn object_count(units: &[Unit]) -> usize {
    let mut objects: Vec<&str> = units.iter().map(|unit| unit.object_id.as_str()).collect();
    objects.sort_unstable();
    objects.dedup();
    objects.len()
}

/// Each object gets its own nodes (at least one; the rest in proportion to its weight),
/// and its units are split across those nodes as one object's would be.
pub(super) fn place(units: &mut [Unit], nodes: &[NodeId]) {
    units.sort_by(|left, right| {
        left.object_id
            .cmp(&right.object_id)
            .then(left.origin.cmp(&right.origin))
            .then(left.row_group.cmp(&right.row_group))
    });
    let mut runs: Vec<std::ops::Range<usize>> = Vec::new();
    for (index, unit) in units.iter().enumerate() {
        match runs.last_mut() {
            Some(run) if units[run.start].object_id == unit.object_id => run.end = index + 1,
            _ => runs.push(index..index + 1),
        }
    }
    let weight = |run: &std::ops::Range<usize>| {
        units[run.clone()]
            .iter()
            .fold(0u64, |sum, unit| sum.saturating_add(unit.weight))
    };
    let shares = node_shares(&runs.iter().map(weight).collect::<Vec<_>>(), nodes.len());
    let mut base = 0;
    for (run, share) in runs.into_iter().zip(shares) {
        let weights: Vec<u64> = units[run.clone()].iter().map(|unit| unit.weight).collect();
        for (unit, slot) in units[run].iter_mut().zip(assign(&weights, share)) {
            unit.slot = base + slot;
            unit.node = nodes[base + slot];
        }
        base += share;
    }
}

/// `nodes` split across objects of these weights: one each, the rest by largest remainder.
/// Needs `nodes >= weights.len()`.
pub(super) fn node_shares(weights: &[u64], nodes: usize) -> Vec<usize> {
    let spare = nodes.saturating_sub(weights.len()) as u128;
    let total = weights
        .iter()
        .map(|&weight| u128::from(weight))
        .sum::<u128>()
        .max(1);
    let ideal: Vec<u128> = weights
        .iter()
        .map(|&weight| spare * u128::from(weight))
        .collect();
    let mut shares: Vec<usize> = ideal
        .iter()
        .map(|share| 1 + (share / total) as usize)
        .collect();
    let mut left = nodes.saturating_sub(shares.iter().sum());
    let mut order: Vec<usize> = (0..weights.len()).collect();
    order.sort_by_key(|&index| std::cmp::Reverse(ideal[index] % total));
    for index in order {
        if left == 0 {
            break;
        }
        shares[index] += 1;
        left -= 1;
    }
    shares
}

/// Contiguous partition that minimises the largest node load.
///
/// Units stay in canonical order (object, origin offset, row group). Tie-break: among
/// splits with the same max load, binary search picks the smallest feasible cap, and
/// greedy fill yields the earliest cut points.
pub(super) fn assign(weights: &[u64], node_count: usize) -> Vec<usize> {
    if weights.is_empty() {
        return Vec::new();
    }
    if node_count <= 1 {
        return vec![0; weights.len()];
    }
    let total: u64 = weights
        .iter()
        .fold(0, |sum, weight| sum.saturating_add(*weight));
    let mut lo = weights.iter().copied().max().unwrap_or(1).max(1);
    let mut hi = total.max(lo);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if nodes_for_cap(weights, mid) <= node_count {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    greedy_slots(weights, lo, node_count)
}

pub(super) fn nodes_for_cap(weights: &[u64], cap: u64) -> usize {
    let mut nodes = 1usize;
    let mut used = 0u64;
    for &weight in weights {
        if used > 0 && used.saturating_add(weight) > cap {
            nodes += 1;
            used = 0;
        }
        used = used.saturating_add(weight);
    }
    nodes
}

pub(super) fn greedy_slots(weights: &[u64], cap: u64, node_count: usize) -> Vec<usize> {
    let mut out = Vec::with_capacity(weights.len());
    let mut node = 0usize;
    let mut used = 0u64;
    for &weight in weights {
        if node + 1 < node_count && used > 0 && used.saturating_add(weight) > cap {
            node += 1;
            used = 0;
        }
        out.push(node);
        used = used.saturating_add(weight);
    }
    out
}

pub(super) fn number_reads(
    units: &mut [Unit],
    nodes: &[NodeId],
) -> Result<Vec<LogicalRead>, PlanError> {
    let mut order: Vec<usize> = (0..units.len()).collect();
    order.sort_by(|&left, &right| {
        units[left]
            .scan
            .cmp(&units[right].scan)
            .then(units[left].slot.cmp(&units[right].slot))
            .then(units[left].object_id.cmp(&units[right].object_id))
            .then(units[left].row_group.cmp(&units[right].row_group))
    });
    let mut reads = Vec::new();
    for index in order {
        let unit = &mut units[index];
        if nodes.iter().all(|node| *node != unit.node) && !unit.drafts.is_empty() {
            return Err(PlanError::Inconsistent);
        }
        let mut ids = Vec::with_capacity(unit.drafts.len());
        for draft in &unit.drafts {
            let id = reads.len() as u32;
            ids.push(id);
            reads.push(LogicalRead {
                id,
                scan: unit.scan,
                node: unit.node,
                object_id: unit.object_id.clone(),
                row_group: unit.row_group,
                column: draft.column,
                offset: draft.offset,
                length: draft.length,
                dictionary: draft.dictionary,
                codec: draft.codec,
                fetch_id: 0,
                pieces: Vec::new(),
            });
        }
        unit.read_ids = ids;
    }
    Ok(reads)
}
