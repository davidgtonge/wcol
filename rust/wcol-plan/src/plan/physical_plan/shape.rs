//! Logical plan shape: the linear scan, aggregate and post-steps the planner accepts.

use super::*;

pub(super) struct AggSpec {
    pub(super) group_by: Vec<u32>,
    pub(super) aggregates: Vec<AggExpr>,
}

pub(super) enum After {
    Sort(Vec<SortKey>),
    Limit { skip: u64, fetch: Option<u64> },
}

pub(super) enum Step {
    Filter(Vec<Predicate>),
    Project(Vec<u32>),
}

pub(super) struct Linear {
    pub(super) table: String,
    pub(super) columns: Vec<u32>,
    pub(super) steps: Vec<Step>,
}

pub(super) struct Shape {
    pub(super) scan: Linear,
    pub(super) aggregate: Option<AggSpec>,
    pub(super) after: Vec<After>,
}

pub(super) fn analyze(op: &LogicalOp) -> Result<Shape, PlanError> {
    let (after, core) = peel(op);
    let shape = match core {
        LogicalOp::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            if group_by.is_empty() && aggregates.is_empty() {
                return Err(PlanError::Unsupported("empty aggregate"));
            }
            check_agg_args(aggregates)?;
            Shape {
                scan: linear(input)?,
                aggregate: Some(AggSpec {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                }),
                after,
            }
        }
        other => Shape {
            scan: linear(other)?,
            aggregate: None,
            after,
        },
    };
    check_shape(&shape)?;
    Ok(shape)
}

/// What a footer-only plan answers: the table's rows and each aggregate's bounds.
pub(super) struct MetadataAnswer {
    rows: u64,
    aggregates: Vec<AggExpr>,
    bounds: Vec<Option<(i64, i64)>>,
}

/// An ungrouped, unfiltered aggregate the footer answers alone: `COUNT(*)` from row counts,
/// `MIN`/`MAX` of an integer column from every chunk's statistics. A column with any chunk
/// lacking trustworthy bounds (see [`int_range`]) is read instead.
pub(super) fn metadata_answer(
    shape: &Shape,
    metadata: &ParquetMetadataCatalog,
) -> Result<Option<MetadataAnswer>, PlanError> {
    let Some(agg) = &shape.aggregate else {
        return Ok(None);
    };
    if !shape.after.is_empty() || !agg.group_by.is_empty() || agg.aggregates.is_empty() {
        return Ok(None);
    }
    let linear = &shape.scan;
    if linear
        .steps
        .iter()
        .any(|step| matches!(step, Step::Filter(p) if !p.is_empty()))
    {
        return Ok(None);
    }
    // Only the aggregated columns may be read; anything else needs the rows.
    if linear.columns.iter().any(|column| {
        !agg.aggregates
            .iter()
            .any(|aggregate| aggregate.column == Some(*column))
    }) {
        return Ok(None);
    }
    let table = find_table(metadata, &linear.table)?;
    let mut bounds = Vec::with_capacity(agg.aggregates.len());
    for aggregate in &agg.aggregates {
        let bound = match (aggregate.kind, aggregate.column, aggregate.offset) {
            (AggKind::CountStar, None, _) => None,
            (AggKind::Min | AggKind::Max, Some(column), 0)
                if matches!(metadata.leaf_physical.get(column as usize), Some(1 | 2)) =>
            {
                match table_int_range(table, column) {
                    Some(range) => Some(range),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        bounds.push(bound);
    }
    let mut rows = 0u64;
    for object in &table.objects {
        let mut seen = BTreeMap::<u32, u64>::new();
        for chunk in &object.chunks {
            seen.entry(chunk.row_group).or_insert(chunk.rows);
        }
        for count in seen.values() {
            rows = rows.checked_add(*count).ok_or(PlanError::Overflow)?;
        }
    }
    Ok(Some(MetadataAnswer {
        rows,
        aggregates: agg.aggregates.clone(),
        bounds,
    }))
}

/// `column`'s integer `(min, max)` over the whole table, when every chunk of it has bounds.
fn table_int_range(table: &TableMeta, column: u32) -> Option<(i64, i64)> {
    let mut chunks = table
        .objects
        .iter()
        .flat_map(|object| &object.chunks)
        .filter(|chunk| chunk.column == column)
        .peekable();
    chunks.peek()?;
    chunks.try_fold((i64::MAX, i64::MIN), |(lo, hi), chunk| {
        chunk.int_range.map(|(min, max)| (lo.min(min), hi.max(max)))
    })
}

pub(super) fn build_metadata_plan(answer: MetadataAnswer) -> Result<WcolPhysicalPlan, PlanError> {
    let plan = WcolPhysicalPlan {
        root: 0,
        operators: vec![Operator {
            id: 0,
            node: None,
            kind: OpKind::MetadataAggregate {
                rows: answer.rows,
                aggregates: answer.aggregates,
                bounds: answer.bounds,
            },
            inputs: Vec::new(),
        }],
        fetches: Vec::new(),
        logical_reads: Vec::new(),
        metrics: PlannerMetrics {
            nodes: Vec::new(),
            request_count: 0,
            logical_bytes: 0,
            fetched_bytes: 0,
            over_read_bytes: 0,
            over_read_ppm: 0,
            hash_reducers: 1,
            estimated_distinct_groups: 0,
            estimated_group_bytes: 0,
        },
    };
    audit(&plan, &PhysicalPlannerConfig::default())?;
    Ok(plan)
}

pub(super) fn row_topk_spec(after: &[After]) -> Option<(Vec<SortKey>, u64)> {
    let [After::Limit {
        skip,
        fetch: Some(fetch),
    }, After::Sort(keys)] = after
    else {
        return None;
    };
    if keys
        .iter()
        .any(|key| matches!(key.target, SortTarget::Aggregate(_)))
    {
        return None;
    }
    Some((keys.clone(), skip.saturating_add(*fetch)))
}

pub(super) fn check_agg_args(aggregates: &[AggExpr]) -> Result<(), PlanError> {
    for agg in aggregates {
        match (agg.kind, agg.column) {
            (AggKind::CountStar, None) => {}
            (AggKind::CountStar, Some(_)) => {
                return Err(PlanError::Unsupported("count star takes no column"));
            }
            (AggKind::CountDistinct | AggKind::ApproxDistinct, None) => {
                return Err(PlanError::Unsupported("aggregate argument required"));
            }
            (_, None) => return Err(PlanError::Unsupported("aggregate argument required")),
            (_, Some(_)) => {}
        }
    }
    Ok(())
}

pub(super) fn peel(op: &LogicalOp) -> (Vec<After>, &LogicalOp) {
    let mut after = Vec::new();
    let mut cur = op;
    loop {
        match cur {
            LogicalOp::Limit { input, skip, fetch } => {
                after.push(After::Limit {
                    skip: *skip,
                    fetch: *fetch,
                });
                cur = input;
            }
            LogicalOp::Sort { input, keys } => {
                after.push(After::Sort(keys.clone()));
                cur = input;
            }
            _ => break,
        }
    }
    (after, cur)
}

pub(super) fn linear(op: &LogicalOp) -> Result<Linear, PlanError> {
    let mut steps = Vec::new();
    let mut cur = op;
    loop {
        match cur {
            LogicalOp::Project { input, columns } => {
                steps.push(Step::Project(columns.clone()));
                cur = input;
            }
            LogicalOp::Filter { input, predicates } => {
                steps.push(Step::Filter(predicates.clone()));
                cur = input;
            }
            LogicalOp::Scan { table, columns } => {
                steps.reverse();
                let mut columns = columns.clone();
                columns.sort_unstable();
                columns.dedup();
                return Ok(Linear {
                    table: table.clone(),
                    columns,
                    steps,
                });
            }
            _ => {
                return Err(PlanError::Unsupported(
                    "scan pipeline only allows filter and project",
                ));
            }
        }
    }
}

pub(super) fn check_shape(shape: &Shape) -> Result<(), PlanError> {
    let available = output_columns(&shape.scan)?;
    if let Some(agg) = &shape.aggregate {
        require_columns(&agg.group_by, &available)?;
        require_agg_columns(&agg.aggregates, &available)?;
    }
    Ok(())
}

pub(super) fn output_columns(scan: &Linear) -> Result<Vec<u32>, PlanError> {
    let mut available = scan.columns.clone();
    for step in &scan.steps {
        match step {
            Step::Filter(preds) => require_columns(
                &preds.iter().map(predicate_column).collect::<Vec<_>>(),
                &available,
            )?,
            Step::Project(columns) => {
                require_columns(columns, &available)?;
                available = columns.clone();
            }
        }
    }
    Ok(available)
}

pub(super) fn predicate_column(pred: &Predicate) -> u32 {
    match pred {
        Predicate::Compare(compare) => compare.column,
        Predicate::IsNull { column } | Predicate::IsNotNull { column } => *column,
        Predicate::Like { column, .. } | Predicate::In { column, .. } => *column,
    }
}

pub(super) fn require_columns(needed: &[u32], available: &[u32]) -> Result<(), PlanError> {
    if needed.iter().any(|column| !available.contains(column)) {
        return Err(PlanError::Unsupported(
            "operator column is not available from the scan",
        ));
    }
    Ok(())
}

pub(super) fn require_agg_columns(
    aggregates: &[AggExpr],
    available: &[u32],
) -> Result<(), PlanError> {
    for agg in aggregates {
        if let Some(column) = agg.column {
            require_columns(&[column], available)?;
        }
    }
    Ok(())
}
