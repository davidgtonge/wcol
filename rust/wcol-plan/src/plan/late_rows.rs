//! Late materialization for a row top-k over wide rows (`SELECT * … ORDER BY … LIMIT k`).
//!
//! Decoding every selected column of a row group at once does not fit a node when the row
//! is wide. So the query runs as two ordinary row graphs:
//!
//! 1. The filter and sort columns pick the winners. The graph prints only their row ids, in
//!    result order.
//! 2. Scans read the `SELECT` list back for those ids only. Each scan reads a group of
//!    columns sized to fit a node, over the row groups that hold winners.
//!
//! [`rows_by_id_json`](crate::plan::result::rows_by_id_json) joins the second graph's rows
//! back in the first graph's order.

use std::collections::BTreeSet;

use crate::formats::parquet::Plan;
use crate::plan::execution_graph::{execution_plan_for, ExecutionPlan, TaskWork};
use crate::plan::logical_from_parser::logical_plan_and_output_from_footer;
use crate::plan::physical_plan::{LogicalOp, PhysicalPlannerConfig, WcolLogicalPlan};
use crate::plan::query_dispatch::{dispatch_prepared, same_schema, NodeProgram, QueryDispatch};
use crate::plan::result::{i64_column_values, row_result_columns, ResultColumn};
use crate::plan::sql_parser::OutputItem;
use crate::program::row_id::{row_position, MAX_GROUP_ROWS, MAX_ROW_GROUPS, MAX_SOURCES};
use crate::program::types::{Literal, Predicate, SortKey, SortTarget};
use crate::program::NodeId;

/// Largest `OFFSET + LIMIT` read late: the second graph filters by a list of this many ids.
pub const LATE_MAX_ROWS: u64 = 1024;

/// Memory bounds that decide when and how a row top-k reads its printed columns late.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LateRowsConfig {
    /// Uncompressed bytes of the printed-only columns in one row group above which a query
    /// reads them late. Below it, one pass decodes them alongside the filter.
    pub wide_bytes: u64,
    /// Uncompressed bytes per row group that one second-graph scan decodes. A larger column
    /// gets a scan of its own.
    pub group_bytes: u64,
}

impl Default for LateRowsConfig {
    fn default() -> Self {
        Self {
            wide_bytes: 64 << 20,
            group_bytes: 32 << 20,
        }
    }
}

/// A row top-k whose printed columns are worth reading late.
#[derive(Debug, PartialEq)]
struct LateQuery {
    table: String,
    predicates: Vec<Predicate>,
    keys: Vec<SortKey>,
    skip: u64,
    fetch: u64,
    /// The `SELECT` list's columns, each once, in order.
    projection: Vec<u32>,
    /// Columns the filter and sort read.
    narrow: Vec<u32>,
    row_id: u32,
}

/// The parts of `Limit(Sort(Project? / Filter* / Scan))`, or `None` for any other shape.
fn row_topk(root: &LogicalOp, row_id: u32) -> Option<LateQuery> {
    let LogicalOp::Limit {
        input,
        skip,
        fetch: Some(fetch),
    } = root
    else {
        return None;
    };
    let LogicalOp::Sort { input, keys } = input.as_ref() else {
        return None;
    };
    let mut narrow = BTreeSet::new();
    for key in keys {
        let SortTarget::Column(column) = key.target else {
            return None;
        };
        narrow.insert(column);
    }
    let mut projection = None;
    let mut predicates = Vec::new();
    let mut cur = input.as_ref();
    let (table, scanned) = loop {
        match cur {
            LogicalOp::Project { input, columns } => {
                projection.get_or_insert_with(|| columns.clone());
                cur = input;
            }
            LogicalOp::Filter {
                input,
                predicates: step,
            } => {
                predicates.extend(step.iter().cloned());
                cur = input;
            }
            LogicalOp::Scan { table, columns } => break (table, columns),
            _ => return None,
        }
    };
    narrow.extend(predicates.iter().map(predicate_column));
    let mut seen = BTreeSet::new();
    let projection = projection
        .unwrap_or_else(|| scanned.clone())
        .into_iter()
        .filter(|column| seen.insert(*column))
        .collect();
    Some(LateQuery {
        table: table.clone(),
        predicates,
        keys: keys.clone(),
        skip: *skip,
        fetch: *fetch,
        projection,
        narrow: narrow.into_iter().collect(),
        row_id,
    })
}

fn predicate_column(predicate: &Predicate) -> u32 {
    match predicate {
        Predicate::Compare(compare) => compare.column,
        Predicate::IsNull { column }
        | Predicate::IsNotNull { column }
        | Predicate::Like { column, .. }
        | Predicate::In { column, .. } => *column,
    }
}

/// Whether `query` over `objects` reads its printed columns late: a small top-k, rows that
/// fit a row id, and printed-only columns too large to decode with the filter.
fn worth_reading_late(
    query: &LateQuery,
    objects: &[(&str, &Plan)],
    config: &LateRowsConfig,
) -> bool {
    let wide: Vec<u32> = query
        .projection
        .iter()
        .copied()
        .filter(|column| !query.narrow.contains(column))
        .collect();
    let fits_ids = objects.len() as u64 <= u64::from(MAX_SOURCES)
        && objects.iter().all(|(_, footer)| {
            footer.chunks.iter().all(|chunk| {
                chunk.row_group < MAX_ROW_GROUPS && (chunk.rows.max(0) as u64) < MAX_GROUP_ROWS
            })
        });
    let widest = objects
        .iter()
        .flat_map(|(_, footer)| group_bytes(footer, &wide, |_| true))
        .max()
        .unwrap_or(0);
    query.skip.saturating_add(query.fetch) <= LATE_MAX_ROWS
        && !wide.is_empty()
        && fits_ids
        && widest > config.wide_bytes
}

/// Uncompressed bytes of `columns` in each row group of `footer` that `keep` accepts.
fn group_bytes<'a>(
    footer: &'a Plan,
    columns: &'a [u32],
    keep: impl Fn(u32) -> bool + 'a,
) -> impl Iterator<Item = u64> + 'a {
    let mut groups = std::collections::BTreeMap::<u32, u64>::new();
    for chunk in footer
        .chunks
        .iter()
        .filter(|chunk| columns.contains(&chunk.leaf) && keep(chunk.row_group))
    {
        *groups.entry(chunk.row_group).or_default() += chunk.uncompressed_size;
    }
    groups.into_values()
}

/// A late query and its `SELECT` list.
type Late = (LateQuery, Vec<OutputItem>);

/// The late query `sql` plans to over `objects`, or `None` when one pass reads it.
fn late_query(
    sql: &str,
    objects: &[(&str, &Plan)],
    config: &LateRowsConfig,
) -> Result<Option<Late>, String> {
    let footer = same_schema(objects)?;
    let (logical, output) = logical_plan_and_output_from_footer(sql, footer)?;
    let row_id = footer.leaves.len() as u32;
    Ok(row_topk(&logical.root, row_id)
        .filter(|query| worth_reading_late(query, objects, config))
        .map(|query| (query, output)))
}

/// `Project(project) ← Filter(predicates) ← Scan(project ∪ read)`, the filter left out when
/// there is none.
fn scan_plan(
    table: &str,
    read: &[u32],
    predicates: Vec<Predicate>,
    project: Vec<u32>,
) -> LogicalOp {
    let columns: BTreeSet<u32> = read.iter().chain(&project).copied().collect();
    let mut root = LogicalOp::Scan {
        table: table.to_string(),
        columns: columns.into_iter().collect(),
    };
    if !predicates.is_empty() {
        root = LogicalOp::Filter {
            input: Box::new(root),
            predicates,
        };
    }
    LogicalOp::Project {
        input: Box::new(root),
        columns: project,
    }
}

impl LateQuery {
    /// The winners' row ids, in result order.
    fn pick(&self) -> WcolLogicalPlan {
        let scan = scan_plan(
            &self.table,
            &self.narrow,
            self.predicates.clone(),
            vec![self.row_id],
        );
        WcolLogicalPlan {
            root: LogicalOp::Limit {
                input: Box::new(LogicalOp::Sort {
                    input: Box::new(scan),
                    keys: self.keys.clone(),
                }),
                skip: self.skip,
                fetch: Some(self.fetch),
            },
        }
    }

    /// `columns` and the row id of the rows `ids` names.
    fn read_back(&self, columns: &[u32], ids: &[i64]) -> WcolLogicalPlan {
        let filter = Predicate::In {
            column: self.row_id,
            values: ids.iter().map(|&id| Literal::Int(id)).collect(),
            negated: false,
        };
        let mut project = columns.to_vec();
        project.push(self.row_id);
        WcolLogicalPlan {
            root: scan_plan(&self.table, &[], vec![filter], project),
        }
    }
}

/// The first graph of a late row query: its winners' row ids, under one result column
/// that names the row-id column. `None` when `sql` reads its rows in one pass.
pub fn plan_late_pick(
    sql: &str,
    objects: &[(&str, &Plan)],
    batch_rows: u32,
    config: &PhysicalPlannerConfig,
) -> Result<
    Option<(
        QueryDispatch,
        crate::plan::WcolPhysicalPlan,
        Vec<ResultColumn>,
    )>,
    String,
> {
    let Some((query, _)) = late_query(sql, objects, &config.late_rows)? else {
        return Ok(None);
    };
    let (dispatch, plan) =
        dispatch_prepared(&query.pick(), objects, config.max_nodes, batch_rows, config)?;
    let columns = vec![ResultColumn {
        name: "__row_id".to_string(),
        column: query.row_id,
    }];
    Ok(Some((dispatch, plan, columns)))
}

/// The second graph of a late row query, under the query's result columns.
pub enum LateRead {
    /// The first graph picked no rows: there is nothing to read.
    Nothing(Vec<ResultColumn>),
    /// Scans that read the picked rows back. The root prints them in no particular order.
    Graph(ExecutionPlan, Vec<ResultColumn>),
}

/// Scans that read the `SELECT` list back for the row ids in `picked` (the first graph's
/// row frames), one per column group and row group holding a picked row.
pub fn plan_late_read(
    sql: &str,
    objects: &[(&str, &Plan)],
    picked: &[u8],
    batch_rows: u32,
    config: &PhysicalPlannerConfig,
) -> Result<LateRead, String> {
    let (query, output) = late_query(sql, objects, &config.late_rows)?
        .ok_or("this query does not read its rows late")?;
    let columns = row_result_columns(&output)?;
    let ids = i64_column_values(picked, query.row_id)?;
    if ids.is_empty() {
        return Ok(LateRead::Nothing(columns));
    }
    let winners = winning_groups(&ids)?;
    let narrowed: Vec<Plan> = objects
        .iter()
        .enumerate()
        .map(|(source, (_, footer))| Plan {
            chunks: footer
                .chunks
                .iter()
                .filter(|chunk| winners.contains(&(source as u16, chunk.row_group)))
                .cloned()
                .collect(),
            ..(*footer).clone()
        })
        .collect();
    let narrowed: Vec<(&str, &Plan)> = objects
        .iter()
        .zip(&narrowed)
        .map(|((id, _), footer)| (*id, footer))
        .collect();
    let width = |column: u32| {
        narrowed
            .iter()
            .flat_map(|(_, footer)| group_bytes(footer, std::slice::from_ref(&column), |_| true))
            .max()
            .unwrap_or(0)
    };
    let groups = column_groups(&query.projection, width, config.late_rows.group_bytes);
    // The job's scan budget, shared by the column groups: each group spreads its winning row
    // groups over its share, so no scan decodes every row group holding a pick.
    let share = (config.max_nodes / groups.len().max(1) as u32).max(1);
    let holders = (winners.len() as u32).clamp(1, share);
    let mut group_config = config.clone();
    group_config.max_nodes = holders;
    let mut nodes: Vec<NodeProgram> = Vec::new();
    let mut last_plan = None;
    for (index, group) in groups.into_iter().enumerate() {
        let logical = query.read_back(&group, &ids);
        let (dispatch, plan) =
            dispatch_prepared(&logical, &narrowed, holders, batch_rows, &group_config)?;
        let QueryDispatch::Rows {
            nodes: programs, ..
        } = dispatch
        else {
            return Err("a late read-back is not a row query".into());
        };
        // Every group plans nodes from 0: shift them so each scan has its own slot. Its scans
        // balance among themselves only: another group's units are not in their catalog.
        let base = nodes.len() as u32;
        nodes.extend(programs.into_iter().map(|program| NodeProgram {
            node: NodeId(base + program.node.0),
            work: program.work.map(|work| TaskWork {
                group: ((index as u32) << 16) | work.group,
                ..work
            }),
            ..program
        }));
        last_plan = Some(plan);
    }
    let plan = last_plan.ok_or("a late read-back has no columns")?;
    let coordinator = crate::plan::encode_coordinator_program(nodes.len() as u32, &[]);
    let exec = execution_plan_for(
        &QueryDispatch::Rows { nodes, coordinator },
        &plan,
        config,
        None,
    )?;
    Ok(LateRead::Graph(exec, columns))
}

/// The (source, row group) of every id.
fn winning_groups(ids: &[i64]) -> Result<BTreeSet<(u16, u32)>, String> {
    ids.iter()
        .map(|&id| {
            row_position(id)
                .map(|position| (position.source, position.row_group))
                .ok_or_else(|| format!("{id} is not a row id"))
        })
        .collect()
}

/// `columns` in order, cut into runs whose widths sum to at most `budget`. A column wider
/// than `budget` is a run of its own.
fn column_groups(columns: &[u32], width: impl Fn(u32) -> u64, budget: u64) -> Vec<Vec<u32>> {
    let mut groups: Vec<Vec<u32>> = Vec::new();
    let mut used = 0u64;
    for &column in columns {
        let bytes = width(column);
        match groups.last_mut() {
            Some(group) if used.saturating_add(bytes) <= budget => {
                group.push(column);
                used += bytes;
            }
            _ => {
                groups.push(vec![column]);
                used = bytes;
            }
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_groups_fill_the_budget_in_order() {
        let width = |column: u32| [10, 20, 5, 50, 1][column as usize];
        assert_eq!(
            column_groups(&[0, 1, 2, 3, 4], width, 35),
            vec![vec![0, 1, 2], vec![3], vec![4]]
        );
    }

    #[test]
    fn only_a_sorted_limited_scan_is_a_row_topk() {
        let scan = LogicalOp::Scan {
            table: "t".into(),
            columns: vec![0, 1, 2],
        };
        let sorted = LogicalOp::Sort {
            input: Box::new(LogicalOp::Filter {
                input: Box::new(scan.clone()),
                predicates: vec![Predicate::IsNotNull { column: 2 }],
            }),
            keys: vec![SortKey {
                target: SortTarget::Column(1),
                descending: false,
                nulls_first: false,
            }],
        };
        let topk = LogicalOp::Limit {
            input: Box::new(sorted.clone()),
            skip: 0,
            fetch: Some(10),
        };
        let query = row_topk(&topk, 3).unwrap();
        assert_eq!(query.projection, vec![0, 1, 2]);
        assert_eq!(query.narrow, vec![1, 2]);
        assert!(row_topk(&sorted, 3).is_none());
        assert!(row_topk(&scan, 3).is_none());
    }
}
