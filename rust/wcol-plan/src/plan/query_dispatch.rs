//! SQL + one Parquet footer → what the coordinator dispatches: one `wcs_open` program per node
//! slot, or (for a footer-only count) the finished partial. This is the single entry hosts use;
//! placement, reads and operators all come from [`lower_to_physical`].

use crate::formats::parquet::{plan_footer, Plan};
use crate::ir::CatalogColumn;
use crate::ir::ColumnKind;
use crate::ops::AggregateOp;
use crate::plan::coordinator_fragment::{CoordinatorStep, ExportColumn};
use crate::plan::logical_from_parser::logical_plan_and_output_from_footer;
use crate::plan::node_fragment::{encode_node_fragment_program, node_fragments};
use crate::plan::physical_plan::{
    lower_to_physical, LogicalOp, NodeId, OpKind, ParquetMetadataCatalog, PhysicalPlannerConfig,
    SinkKind, WcolLogicalPlan, WcolPhysicalPlan,
};
use crate::plan::result::{encode_result_columns, export_steps, result_columns, ResultColumn};
use crate::stream::partial_wcs::partial_state_to_wcs_payload;

/// One worker slot's program and the reads the plan expects it to make.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeProgram {
    pub node: NodeId,
    pub program: Vec<u8>,
    pub request_count: u32,
    pub fetched_bytes: u64,
    pub work: Option<crate::plan::execution_graph::TaskWork>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryDispatch {
    /// Run every program, then merge their partials with `ReduceOp` (the plan's exchange and
    /// final aggregate).
    Nodes(Vec<NodeProgram>),
    /// The footer answers the query: this is the merged partial, no node work.
    Partial(Vec<u8>),
    /// Row query: run every node program, then the coordinator program over their row frames
    /// concatenated in node order. The coordinator writes row frames, or one Parquet file when
    /// the plan's sink is Parquet.
    Rows {
        nodes: Vec<NodeProgram>,
        coordinator: Vec<u8>,
    },
    /// Unordered row export: each program writes its own Parquet part. There is no merge.
    Parquet(Vec<NodeProgram>),
}

/// Parquet leaves as the batch engine's catalog: BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY are text.
pub fn catalog_from_footer(footer: &Plan) -> Vec<CatalogColumn> {
    footer
        .leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: if matches!(leaf.physical, 6 | 7) {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect()
}

/// The catalog entry of a scan's row-id column (an INT64 with no nulls).
fn row_id_catalog_column() -> CatalogColumn {
    CatalogColumn {
        name: "__row_id".to_string(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }
}

/// Plan `sql` over one Parquet object for `node_count` slots.
pub fn plan_query_dispatch(
    sql: &str,
    object_id: &str,
    footer: &Plan,
    node_count: u32,
    batch_rows: u32,
    config: &PhysicalPlannerConfig,
) -> Result<(QueryDispatch, WcolPhysicalPlan), String> {
    plan_query_job(sql, &[(object_id, footer)], node_count, batch_rows, config)
        .map(|(dispatch, plan, _)| (dispatch, plan))
}

/// [`plan_query_dispatch`] plus the result columns: each output name and the row-frame column
/// of the graph's root that holds it.
/// `objects` are one table's Parquet objects in job-source order (an object's index is its
/// ReadAt source). They share one schema; the first footer gives it to the SQL frontend.
pub fn plan_query_job(
    sql: &str,
    objects: &[(&str, &Plan)],
    node_count: u32,
    batch_rows: u32,
    config: &PhysicalPlannerConfig,
) -> Result<(QueryDispatch, WcolPhysicalPlan, Vec<ResultColumn>), String> {
    if node_count == 0 {
        return Err("query needs at least one node slot".to_string());
    }
    let footer = same_schema(objects)?;
    let (logical, output) = logical_plan_and_output_from_footer(sql, footer)
        .map_err(|err| format!("sql: sql is not supported by the planner: {err}"))?;
    let (dispatch, plan) = dispatch_prepared(&logical, objects, node_count, batch_rows, config)?;
    let names: Vec<String> = footer.leaves.iter().map(|leaf| leaf.name.clone()).collect();
    let columns = result_columns(&plan, Some(output.as_slice()), &names)?;
    Ok((dispatch, plan, columns))
}

/// The schema every object shares: the first footer's, once each object's columns match it.
pub(crate) fn same_schema<'a>(objects: &[(&str, &'a Plan)]) -> Result<&'a Plan, String> {
    let [(_, first), rest @ ..] = objects else {
        return Err("a table needs at least one object".to_string());
    };
    if let Some((id, _)) = rest
        .iter()
        .find(|(_, footer)| footer.leaves != first.leaves)
    {
        return Err(format!(
            "object {id} has a different schema from the table's first object"
        ));
    }
    Ok(first)
}

pub(crate) fn dispatch_prepared(
    logical: &WcolLogicalPlan,
    objects: &[(&str, &Plan)],
    node_count: u32,
    batch_rows: u32,
    config: &PhysicalPlannerConfig,
) -> Result<(QueryDispatch, WcolPhysicalPlan), String> {
    let footer = same_schema(objects)?;
    let mut catalog = catalog_from_footer(footer);
    let table = scan_table_name(&logical.root)?;
    let metadata = ParquetMetadataCatalog::from_footers(table, objects);
    let nodes: Vec<NodeId> = (0..node_count).map(NodeId).collect();
    let plan =
        lower_to_physical(logical, &metadata, &nodes, config).map_err(|err| err.to_string())?;

    if let Some((rows, aggregates, bounds)) = plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::MetadataAggregate {
            rows,
            aggregates,
            bounds,
        } => Some((*rows, aggregates, bounds)),
        _ => None,
    }) {
        let aggregates = wcol_engine::program::to_ir::aggregates(aggregates)?;
        let mut answer = AggregateOp::from_aggregate(&[], &aggregates, None, 0, false, &catalog)?;
        answer.apply_metadata(rows, bounds)?;
        let partial = partial_state_to_wcs_payload(
            &answer
                .finish_partial_state()?
                .expect_ready("a metadata aggregate does not spill"),
        )?;
        return Ok((QueryDispatch::Partial(partial), plan));
    }

    let mut fragments = node_fragments(&plan, config.fetch_depth).map_err(|err| err.to_string())?;
    let row_id = metadata.row_id_column().filter(|column| {
        fragments.values().any(|fragment| {
            fragment
                .decode
                .iter()
                .any(|group| group.columns.contains(column))
        })
    });
    if let Some(column) = row_id {
        catalog.push(row_id_catalog_column());
        for fragment in fragments.values_mut() {
            fragment.row_id = Some(column);
        }
    }
    let object_ids: Vec<&str> = objects.iter().map(|(id, _)| *id).collect();
    crate::plan::node_fragment::assign_sources(&mut fragments, &object_ids)?;
    // Uncompressed bytes are the decode work; compressed sizes hide dense string columns.
    let uncompressed: std::collections::HashMap<(&str, u32, u32), u64> = objects
        .iter()
        .flat_map(|(id, footer)| {
            footer
                .chunks
                .iter()
                .map(move |chunk| ((*id, chunk.row_group, chunk.leaf), chunk.uncompressed_size))
        })
        .collect();
    crate::plan::node_fragment::add_claim_units(&mut fragments, &|object, row_group, leaf| {
        uncompressed
            .get(&(object, row_group, leaf))
            .copied()
            .unwrap_or(0)
    });
    let programs: Vec<NodeProgram> = fragments
        .values()
        .map(|fragment| {
            let metrics = plan.metrics.nodes.iter().find(|m| m.node == fragment.node);
            NodeProgram {
                node: fragment.node,
                program: encode_node_fragment_program(batch_rows, &catalog, fragment),
                request_count: metrics.map_or(0, |m| m.request_count),
                fetched_bytes: metrics.map_or(0, |m| m.fetched_bytes),
                work: (!fragment.own_units.is_empty()).then(|| {
                    crate::plan::execution_graph::TaskWork {
                        group: u32::from(fragment.source),
                        own_units: fragment.own_units.clone(),
                    }
                }),
            }
        })
        .collect();
    if programs_write_parquet(&plan) {
        return Ok((QueryDispatch::Parquet(programs), plan));
    }
    if crate::plan::execution_graph::plan_range_exchange(&plan).is_some() {
        return Ok((QueryDispatch::Nodes(programs), plan));
    }
    // An aggregate's nodes emit partials, so even with a coordinator sink it is not a row
    // query: its finalizer writes the sink (`execution_plan_for`).
    let aggregate = plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::FinalAggregate { .. }));
    let row_query = !aggregate
        && plan.operators.iter().any(|op| {
            matches!(
                op.kind,
                OpKind::Sink {
                    kind: SinkKind::Rows
                }
            ) || (op.node.is_none()
                && matches!(
                    op.kind,
                    OpKind::Sink {
                        kind: SinkKind::Parquet { .. }
                    }
                ))
        });
    if row_query {
        let mut steps = crate::plan::coordinator_steps(&plan);
        if let Some(target_row_group_rows) = coordinator_parquet_rows(&plan) {
            steps.push(CoordinatorStep::Parquet {
                target_row_group_rows,
                columns: export_columns(&plan, &catalog)?,
            });
        }
        let coordinator = crate::plan::encode_coordinator_program(programs.len() as u32, &steps);
        return Ok((
            QueryDispatch::Rows {
                nodes: programs,
                coordinator,
            },
            plan,
        ));
    }
    Ok((QueryDispatch::Nodes(programs), plan))
}

fn scan_table_name(op: &LogicalOp) -> Result<&str, String> {
    match op {
        LogicalOp::Scan { table, .. } => Ok(table.as_str()),
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Aggregate { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. } => scan_table_name(input),
        LogicalOp::Join { .. } => Err("query is not a single table scan".into()),
    }
}

fn programs_write_parquet(plan: &WcolPhysicalPlan) -> bool {
    plan.operators.iter().any(|op| {
        op.node.is_some()
            && matches!(
                op.kind,
                OpKind::Sink {
                    kind: SinkKind::Parquet { .. }
                }
            )
    })
}

fn coordinator_parquet_rows(plan: &WcolPhysicalPlan) -> Option<u32> {
    plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::Sink {
            kind:
                SinkKind::Parquet {
                    target_row_group_rows,
                    ..
                },
        } if op.node.is_none() => Some(*target_row_group_rows),
        _ => None,
    })
}

fn export_columns(
    plan: &WcolPhysicalPlan,
    catalog: &[CatalogColumn],
) -> Result<Vec<ExportColumn>, String> {
    let ids = plan
        .operators
        .iter()
        .find_map(|op| match &op.kind {
            OpKind::Project { columns } if op.node.is_none() => Some(columns.clone()),
            _ => None,
        })
        .or_else(|| {
            plan.operators.iter().find_map(|op| match &op.kind {
                OpKind::Decode { groups } => groups.first().map(|group| group.columns.clone()),
                _ => None,
            })
        })
        .ok_or_else(|| "parquet export has no output columns".to_string())?;
    ids.iter()
        .map(|id| {
            let column = catalog
                .get(*id as usize)
                .ok_or_else(|| format!("export column {id} is not in the catalog"))?;
            Ok(ExportColumn {
                name: column.name.clone(),
                optional: column.max_def > 0,
                physical: column.physical,
                type_length: column.type_length,
            })
        })
        .collect()
}

/// Finalizer steps for a [`QueryDispatch::Nodes`] parquet job: aggregate exports use
/// [`export_steps`]; ordered row exports (range exchange) sort and encode on each worker.
pub fn nodes_parquet_finalize_steps(
    plan: &WcolPhysicalPlan,
    columns: &[ResultColumn],
    footer: &Plan,
    target_row_group_rows: u32,
) -> Result<Vec<CoordinatorStep>, String> {
    if plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::FinalAggregate { .. }))
    {
        return export_steps(plan, columns, &footer.leaves, target_row_group_rows);
    }
    let catalog = catalog_from_footer(footer);
    let mut steps = crate::plan::coordinator_steps(plan);
    if let Some((keys, _)) = crate::plan::execution_graph::plan_range_exchange(plan) {
        if !steps
            .iter()
            .any(|step| matches!(step, CoordinatorStep::Sort { .. }))
        {
            steps.push(CoordinatorStep::Sort(keys));
        }
    }
    steps.push(CoordinatorStep::Parquet {
        target_row_group_rows,
        columns: export_columns(plan, &catalog)?,
    });
    Ok(steps)
}

/// Wire form for hosts: the execution plan, then `u32`-length result columns. Input: `u32 batch_rows, u32 max_nodes, u32 max_row_groups (0 = all),
/// str object_id, bytes footer, str sql` (strings and bytes are `u32` length + data).
pub fn plan_query_dispatch_wire(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut cur = input;
    let batch_rows = take_u32(&mut cur)?;
    let max_nodes = take_u32(&mut cur)?;
    let max_row_groups = take_u32(&mut cur)?;
    let object_id =
        std::str::from_utf8(take_bytes(&mut cur)?).map_err(|_| "object id is not UTF-8")?;
    let footer_bytes = take_bytes(&mut cur)?;
    let sql = std::str::from_utf8(take_bytes(&mut cur)?).map_err(|_| "sql is not UTF-8")?;
    if !cur.is_empty() {
        return Err("trailing bytes after plan request".to_string());
    }
    let mut footer = plan_footer(footer_bytes).map_err(|err| format!("parquet footer: {err:?}"))?;
    if max_row_groups > 0 {
        // Plan over the first row groups only (tests and benchmarks on a prefix of a large file).
        footer
            .chunks
            .retain(|chunk| chunk.row_group < max_row_groups);
    }
    let config = PhysicalPlannerConfig::for_coordinator(max_nodes);
    let (dispatch, plan, columns) = plan_query_job(
        sql,
        &[(object_id, &footer)],
        config.max_nodes,
        batch_rows,
        &config,
    )?;
    let exec = crate::plan::execution_plan_from_dispatch(&dispatch, &plan, &config)?;
    let mut out = crate::plan::encode_execution_plan_wire(&exec);
    // Trailer: `u32` length + result columns. Only the coordinator reads it, to print JSON.
    let columns = encode_result_columns(&columns);
    out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    out.extend_from_slice(&columns);
    Ok(out)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err("plan request is truncated".to_string());
    }
    let value = u32::from_le_bytes(cur[..4].try_into().unwrap());
    *cur = &cur[4..];
    Ok(value)
}

fn take_bytes<'a>(cur: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let len = take_u32(cur)? as usize;
    if cur.len() < len {
        return Err("plan request is truncated".to_string());
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}
