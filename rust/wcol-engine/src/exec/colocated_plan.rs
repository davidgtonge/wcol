//! Physical plan fragments → [`LinearColocatedGraph`](crate::exec::graph_scheduler::LinearColocatedGraph).
//!
//! Operators come from the fragment. The executor does not read SQL.

use crate::exec::graph_scheduler::LinearColocatedGraph;
use crate::exec::operator::Operator;
use crate::ir::CatalogColumn;
use crate::ops::{AggregateOp, FilterOp, ParquetSourceOp};
use crate::program::to_ir;
use crate::program::IngestMemoryProfile;

/// One node's share of a physical plan: plan fetches → decode → the fragment's operators.
/// The output is the node's partial state; the coordinator merges partials.
pub fn open_fragment_graph(
    batch_rows: usize,
    catalog: Vec<CatalogColumn>,
    fragment: &crate::program::NodeFragment,
    memory_profile: IngestMemoryProfile,
) -> Result<LinearColocatedGraph, String> {
    use crate::program::FragmentStep;

    let mut predicates = Vec::new();
    let mut aggregate = None;
    let mut project_ids = None;
    let mut parquet_rows = None;
    let mut row_steps: Vec<Box<dyn Operator>> = Vec::new();
    for step in &fragment.steps {
        match step {
            FragmentStep::Filter(step) => predicates.extend(step.iter().cloned()),
            FragmentStep::PartialAggregate {
                group_by,
                aggregates,
            } => {
                aggregate = Some((group_by, aggregates));
            }
            FragmentStep::Project(columns) => {
                project_ids = Some(columns.clone());
                row_steps.push(Box::new(crate::ops::ProjectOp::from_columns(
                    columns.clone(),
                )?));
            }
            FragmentStep::TopK { keys, n } => {
                let keys = keys
                    .iter()
                    .map(row_sort_key)
                    .collect::<Result<Vec<_>, _>>()?;
                let n = usize::try_from(*n).map_err(|_| format!("topk {n} does not fit"))?;
                row_steps.push(Box::new(crate::ops::TopKOp::new(keys, n)));
            }
            FragmentStep::Parquet {
                target_row_group_rows,
                partition_columns,
            } => {
                if !partition_columns.is_empty() {
                    return Err(
                        "partitioned parquet is written by the host demux, not one node file"
                            .to_string(),
                    );
                }
                parquet_rows = Some(*target_row_group_rows);
            }
            FragmentStep::HashPartition { .. }
            | FragmentStep::RangePartition { .. }
            | FragmentStep::Resident(_) => {}
        }
    }
    let groups = fragment.ingest_groups().map_err(|err| err.to_string())?;
    let claimed = !fragment.claim_units.is_empty();
    let (planned, claimable) = if claimed {
        (Vec::new(), groups)
    } else {
        (groups, Vec::new())
    };
    let mut source = ParquetSourceOp::with_fetches(
        batch_rows,
        catalog.clone(),
        planned,
        memory_profile,
        fragment.fetches.clone(),
        fragment.logical_reads.clone(),
        fragment.fetch_depth as usize,
    )
    .with_source(fragment.source);
    if claimed {
        source = source.with_claims(fragment.claim_units.clone(), claimable)?;
    }
    if let Some(column) = fragment.row_id {
        source = source.with_row_id(column);
    }
    let source = Some(Box::new(source) as Box<dyn Operator>);
    if let Some((group_by, aggregates)) = aggregate {
        if parquet_rows.is_some() {
            return Err("node fragment mixes a partial aggregate with a parquet sink".to_string());
        }
        if !row_steps.is_empty() {
            return Err("node fragment mixes a partial aggregate with row steps".to_string());
        }
        let tail = fragment.tail;
        let narrow = |value: u64, what: &str| {
            u32::try_from(value)
                .map_err(|_| format!("{what} {value} does not fit the partial header"))
        };
        let limit = tail.fetch.map(|n| narrow(n, "limit")).transpose()?;
        let offset = narrow(tail.skip, "offset")?;
        let filter = FilterOp::from_predicates(&to_ir::predicates(&predicates)?, &catalog)?;
        let mut aggregate = AggregateOp::from_aggregate(
            group_by,
            &to_ir::aggregates(aggregates)?,
            limit,
            offset,
            tail.order_groups_by_count,
            &catalog,
        )?;
        aggregate.set_final_aggregate(tail.final_aggregate);
        if tail.final_aggregate {
            aggregate.set_final_rank(tail.rank_aggregate);
        }
        // A combiner routes its groups to reducers as it goes, so its table stays bounded.
        if let Some(route) = fragment.steps.iter().find_map(|step| match step {
            FragmentStep::HashPartition {
                input: crate::program::PartitionInput::Partials { keys },
                partitions,
                hash_version,
            } => Some(crate::ops::exchange::CombinerRoute {
                keys: keys
                    .iter()
                    .map(|(position, column)| (*position as usize, *column))
                    .collect(),
                partitions: *partitions,
                hash_version: *hash_version,
            }),
            _ => None,
        }) {
            aggregate.set_combiner(crate::ops::aggregate::COMBINER_FLUSH_BYTES, route)?;
        }
        return Ok(LinearColocatedGraph::from_plan_parts(
            source,
            vec![Box::new(filter), Box::new(aggregate)],
            Vec::new(),
            false,
        ));
    }
    let mut stream_ops = Vec::new();
    if !predicates.is_empty() {
        stream_ops.push(Box::new(FilterOp::from_predicates(
            &to_ir::predicates(&predicates)?,
            &catalog,
        )?) as Box<dyn Operator>);
    }
    stream_ops.extend(row_steps);
    let mut graph = LinearColocatedGraph::from_plan_parts(source, stream_ops, Vec::new(), false);
    if let Some(row_group_rows) = parquet_rows {
        let ids = project_ids.unwrap_or_else(|| {
            fragment
                .decode
                .first()
                .map(|group| group.columns.clone())
                .unwrap_or_default()
        });
        let columns = sink_columns(&catalog, &ids)?;
        let encode = crate::formats::parquet::ParquetEncode::open_typed(
            columns,
            crate::formats::parquet::ParquetEncodeConfig {
                max_row_group_rows: row_group_rows as usize,
                max_row_groups: 8_192,
            },
        )?;
        graph.attach_sink(Box::new(encode));
    } else {
        graph.set_emit_rows();
    }
    Ok(graph)
}

pub fn open_coordinator_graph(
    steps: &[crate::program::CoordinatorStep],
) -> Result<LinearColocatedGraph, String> {
    let mut stream_ops: Vec<Box<dyn Operator>> = Vec::new();
    let mut parquet = None;
    for step in steps {
        match step {
            crate::program::CoordinatorStep::Project(columns) => {
                stream_ops.push(Box::new(crate::ops::ProjectOp::from_columns(
                    columns.clone(),
                )?));
            }
            crate::program::CoordinatorStep::Sort(keys) => {
                let keys = keys
                    .iter()
                    .map(row_sort_key)
                    .collect::<Result<Vec<_>, _>>()?;
                stream_ops.push(Box::new(crate::ops::SortOp::with_keys(keys)));
            }
            crate::program::CoordinatorStep::Limit { skip, fetch } => {
                stream_ops.push(Box::new(crate::ops::LimitOp::new(*skip, *fetch)));
            }
            crate::program::CoordinatorStep::Parquet {
                target_row_group_rows,
                columns,
            } => {
                let columns = columns
                    .iter()
                    .map(|column| crate::formats::parquet::ParquetSinkColumn {
                        name: column.name.clone(),
                        optional: column.optional,
                        physical: column.physical,
                        type_length: column.type_length,
                        date: false,
                    })
                    .collect();
                parquet = Some(crate::formats::parquet::ParquetEncode::open_typed(
                    columns,
                    crate::formats::parquet::ParquetEncodeConfig {
                        max_row_group_rows: *target_row_group_rows as usize,
                        max_row_groups: 8_192,
                    },
                )?);
            }
        }
    }
    let mut graph = LinearColocatedGraph::from_plan_parts(None, stream_ops, Vec::new(), false);
    if let Some(encode) = parquet {
        graph.attach_sink(Box::new(encode));
    } else {
        graph.set_emit_rows();
    }
    Ok(graph)
}

fn sink_columns(
    catalog: &[CatalogColumn],
    ids: &[u32],
) -> Result<Vec<crate::formats::parquet::ParquetSinkColumn>, String> {
    if ids.is_empty() {
        return Err("parquet sink has no output columns".to_string());
    }
    ids.iter()
        .map(|id| {
            let column = catalog
                .get(*id as usize)
                .ok_or_else(|| format!("parquet sink column {id} is not in the catalog"))?;
            Ok(crate::formats::parquet::ParquetSinkColumn {
                name: column.name.clone(),
                optional: column.max_def > 0,
                physical: column.physical,
                type_length: column.type_length,
                date: false,
            })
        })
        .collect()
}

fn row_sort_key(
    key: &crate::program::types::SortKey,
) -> Result<crate::ops::sort::RowSortKey, String> {
    to_ir::sort_key(key)
}
