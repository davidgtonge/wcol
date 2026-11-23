//! Build the operator graph for a placed plan.

use super::*;

pub(super) struct Side<'a> {
    pub(super) scan: u32,
    pub(super) linear: &'a Linear,
    pub(super) partial: Option<&'a AggSpec>,
    /// Columns a hash exchange sends its reducers. Filter-only columns stay on the scan:
    /// each partition would otherwise get every row group's dictionary for them.
    pub(super) exchange_columns: Option<Vec<u32>>,
}

/// Group keys, then aggregate inputs, each once.
pub(super) fn reducer_columns(agg: &AggSpec) -> Vec<u32> {
    let mut columns = agg.group_by.clone();
    for column in agg.aggregates.iter().filter_map(|agg| agg.column) {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    columns
}

pub(super) fn build_ops(
    shape: &Shape,
    units: &[Unit],
    nodes: &[NodeId],
    fetches: &[FetchRequest],
    row_topk: Option<(Vec<SortKey>, u64)>,
    export_row_group_rows: Option<u32>,
    partition_columns: &[u32],
    hash: &HashAggregateConfig,
    estimated_groups: u64,
    metadata: &ParquetMetadataCatalog,
) -> Result<(Vec<Operator>, u32), PlanError> {
    let (local_export, coordinator_export) =
        export_placement(shape, export_row_group_rows, !partition_columns.is_empty())?;
    let source_rows = units
        .iter()
        .fold(0u64, |sum, unit| sum.saturating_add(unit.rows));
    let estimated_row_bytes = source_rows.saturating_mul(16);
    let hash_parts = hash_exchange_for(shape, hash, estimated_groups, source_rows, metadata)?;
    let range_parts = range_exchange_for(
        shape,
        nodes.len(),
        &row_topk,
        coordinator_export.is_some(),
        estimated_row_bytes,
        metadata,
    )?;
    let sides: Vec<Side<'_>> = match &shape.input {
        Input::Scan(scan) => {
            let partial = shape
                .aggregate
                .as_ref()
                .filter(|agg| hash_parts.is_none() || scan_combines(agg, metadata, source_rows));
            // Raw rows to reducers carry only the reducer columns; a combiner sends partials.
            let raw_rows = hash_parts.as_ref().and(shape.aggregate.as_ref());
            vec![Side {
                scan: 0,
                linear: scan,
                partial,
                exchange_columns: raw_rows.filter(|_| partial.is_none()).map(reducer_columns),
            }]
        }
        Input::Join { left, right, .. } => vec![
            Side {
                scan: 0,
                linear: left,
                partial: None,
                exchange_columns: None,
            },
            Side {
                scan: 1,
                linear: right,
                partial: None,
                exchange_columns: None,
            },
        ],
    };
    let mut operators = Vec::new();
    let mut producers = vec![Vec::new(); sides.len()];
    for node in nodes {
        let fetch_ids: Vec<u32> = fetches
            .iter()
            .filter(|fetch| fetch.node == *node)
            .map(|fetch| fetch.id)
            .collect();
        let fetch_op = if fetch_ids.is_empty() {
            None
        } else {
            Some(push(
                &mut operators,
                Some(*node),
                OpKind::R2Fetch { fetch_ids },
                Vec::new(),
            ))
        };
        for (index, side) in sides.iter().enumerate() {
            let groups = decode_groups(units, *node, side.scan);
            if groups.is_empty() {
                continue;
            }
            let output = lower_fragment(
                &mut operators,
                *node,
                fetch_op,
                groups,
                side,
                row_topk.as_ref(),
                local_export.is_some(),
            )?;
            producers[index].push(output);
        }
    }
    for (index, side) in sides.iter().enumerate() {
        if !producers[index].is_empty() {
            continue;
        }
        if !side.linear.columns.is_empty() {
            return Err(PlanError::Inconsistent);
        }
        let output = lower_fragment(
            &mut operators,
            nodes[0],
            None,
            Vec::new(),
            side,
            row_topk.as_ref(),
            local_export.is_some(),
        )?;
        producers[index].push(output);
    }

    if let Some(row_group_rows) = local_export {
        if producers[0].is_empty() {
            return Err(PlanError::Inconsistent);
        }
        let mut root = 0;
        for output in producers[0].clone() {
            let node = operators[output as usize].node;
            root = push(
                &mut operators,
                node,
                OpKind::Sink {
                    kind: SinkKind::Parquet {
                        target_row_group_rows: row_group_rows,
                        partition_columns: partition_columns.to_vec(),
                    },
                },
                vec![output],
            );
        }
        return Ok((operators, root));
    }

    let mut current = match &shape.input {
        Input::Scan(_) => {
            let gathered = if let Some((keys, partitions)) = &hash_parts {
                push(
                    &mut operators,
                    None,
                    OpKind::HashExchange {
                        keys: keys.clone(),
                        partitions: *partitions,
                        hash_version: crate::ops::WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
                    },
                    producers[0].clone(),
                )
            } else if let Some((keys, partitions, splits)) = &range_parts {
                push(
                    &mut operators,
                    None,
                    OpKind::RangeExchange {
                        keys: keys.clone(),
                        partitions: *partitions,
                        splits: splits.clone(),
                    },
                    producers[0].clone(),
                )
            } else {
                gather(&mut operators, &producers[0])?
            };
            if let Some(agg) = &shape.aggregate {
                push(
                    &mut operators,
                    None,
                    OpKind::FinalAggregate {
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                    },
                    vec![gathered],
                )
            } else {
                gathered
            }
        }
        Input::Join {
            left_keys,
            right_keys,
            ..
        } => {
            let left = gather(&mut operators, &producers[0])?;
            let right = gather(&mut operators, &producers[1])?;
            let join = push(
                &mut operators,
                None,
                OpKind::Join {
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                },
                vec![left, right],
            );
            if let Some(agg) = &shape.aggregate {
                push(
                    &mut operators,
                    None,
                    OpKind::Aggregate {
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                    },
                    vec![join],
                )
            } else {
                join
            }
        }
    };
    for step in shape.after.iter().rev() {
        if range_parts.is_some() && matches!(step, After::Sort(_)) {
            continue;
        }
        current = match step {
            After::Sort(keys) => push(
                &mut operators,
                None,
                OpKind::Sort { keys: keys.clone() },
                vec![current],
            ),
            After::Limit { skip, fetch } => push(
                &mut operators,
                None,
                OpKind::Limit {
                    skip: *skip,
                    fetch: *fetch,
                },
                vec![current],
            ),
        };
    }
    if matches!(shape.input, Input::Scan(_)) && shape.aggregate.is_none() {
        if let Input::Scan(scan) = &shape.input {
            if let Some(columns) = scan.steps.iter().find_map(|step| match step {
                Step::Project(columns) => Some(columns.clone()),
                Step::Filter(_) => None,
            }) {
                current = push(
                    &mut operators,
                    None,
                    OpKind::Project { columns },
                    vec![current],
                );
            }
        }
        let kind = match coordinator_export {
            Some(target_row_group_rows) => SinkKind::Parquet {
                target_row_group_rows,
                partition_columns: Vec::new(),
            },
            None => SinkKind::Rows,
        };
        current = push(&mut operators, None, OpKind::Sink { kind }, vec![current]);
    } else if let Some(target_row_group_rows) = coordinator_export {
        // Aggregate (and any sort or limit above it): the sink reads final rows.
        current = push(
            &mut operators,
            None,
            OpKind::Sink {
                kind: SinkKind::Parquet {
                    target_row_group_rows,
                    partition_columns: Vec::new(),
                },
            },
            vec![current],
        );
    }
    Ok((operators, current))
}

pub(super) fn decode_groups(units: &[Unit], node: NodeId, scan: u32) -> Vec<DecodeGroup> {
    units
        .iter()
        .filter(|unit| unit.node == node && unit.scan == scan)
        .map(|unit| DecodeGroup {
            object_id: unit.object_id.clone(),
            row_group: unit.row_group,
            rows: unit.rows,
            columns: unit.columns.clone(),
            reads: unit.read_ids.clone(),
        })
        .collect()
}

/// Where a Parquet sink goes.
///
/// Scan, filter, and projection: one sink on each node, because those nodes already
/// emit final rows. Aggregate, `ORDER BY`, or `LIMIT`: one sink on the coordinator,
/// after the final reduction or global step. `partitionBy` is only legal on the
/// distributed shape. Joins are not exported yet.
pub(super) fn export_placement(
    shape: &Shape,
    export_row_group_rows: Option<u32>,
    partitioned: bool,
) -> Result<(Option<u32>, Option<u32>), PlanError> {
    let Some(rows) = export_row_group_rows else {
        if partitioned {
            return Err(PlanError::Unsupported(
                "partitionBy requires a parquet sink",
            ));
        }
        return Ok((None, None));
    };
    if matches!(shape.input, Input::Join { .. }) {
        return Err(PlanError::Unsupported(
            "parquet export does not run joins yet",
        ));
    }
    let single_writer = shape.aggregate.is_some()
        || shape
            .after
            .iter()
            .any(|step| matches!(step, After::Sort(_) | After::Limit { .. }));
    if partitioned && single_writer {
        return Err(PlanError::Unsupported(
            "partitionBy is only supported on scan, filter, and projection",
        ));
    }
    if single_writer {
        Ok((None, Some(rows)))
    } else {
        Ok((Some(rows), None))
    }
}

pub(super) fn lower_fragment(
    operators: &mut Vec<Operator>,
    node: NodeId,
    fetch_op: Option<u32>,
    groups: Vec<DecodeGroup>,
    side: &Side<'_>,
    row_topk: Option<&(Vec<SortKey>, u64)>,
    project_on_node: bool,
) -> Result<u32, PlanError> {
    let inputs = fetch_op.into_iter().collect();
    let mut current = push(operators, Some(node), OpKind::Decode { groups }, inputs);
    for step in &side.linear.steps {
        current = match step {
            Step::Filter(predicates) if !predicates.is_empty() => push(
                operators,
                Some(node),
                OpKind::Filter {
                    predicates: predicates.clone(),
                },
                vec![current],
            ),
            Step::Filter(_) => current,
            // Sort keys must survive the exchange, so a coordinator sort projects afterwards.
            // A per-node Parquet sink projects here: there is no later stage.
            Step::Project(columns) if project_on_node => push(
                operators,
                Some(node),
                OpKind::Project {
                    columns: columns.clone(),
                },
                vec![current],
            ),
            Step::Project(_) => current,
        };
    }
    if let Some(columns) = &side.exchange_columns {
        current = push(
            operators,
            Some(node),
            OpKind::Project {
                columns: columns.clone(),
            },
            vec![current],
        );
    }
    if let Some(agg) = side.partial {
        current = push(
            operators,
            Some(node),
            OpKind::PartialAggregate {
                group_by: agg.group_by.clone(),
                aggregates: agg.aggregates.clone(),
            },
            vec![current],
        );
    } else if let Some((keys, n)) = row_topk {
        current = push(
            operators,
            Some(node),
            OpKind::TopK {
                keys: keys.clone(),
                n: *n,
            },
            vec![current],
        );
    }
    Ok(current)
}
