//! What the client sees: result column names, the finalizer's steps, and JSON at the edge.
//!
//! Nodes ship partial state. The graph's finalizer task merges it, turns the groups into rows
//! (keys at columns `0..k`, aggregates at `k + i`), and runs the plan's sort and limit in the
//! engine. Its row frames come back to the coordinator, which only names columns and prints
//! them. Nothing here orders, windows or merges anything.

use std::collections::HashMap;

use crate::exec::operators::{owned_batch_from_projection_body_with_dicts, WcsIngress};
use crate::formats::parquet::stream_wcs::apply_dictionary_payload;
use crate::ops::finalize::finalized_groups_batch;
use crate::plan::coordinator_fragment::CoordinatorStep;
use crate::plan::physical_plan::{AggExpr, AggKind, OpKind, WcolPhysicalPlan};
use crate::plan::sql_parser::{OutputItem, OutputSource};
use crate::program::codec::{push_str, push_u32, read_str, read_u32, LogicalWireError};
use crate::program::types::{SortKey, SortTarget};
use crate::stream::partials::partial_snapshot_from_wire;
use crate::stream::{
    DictEntries, DictionaryValues, OwnedWcolBatch, WcolColumn, FRAME_DATA_BATCH, FRAME_DICTIONARY,
};

/// One result column: its name and the row-frame column id that holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub column: u32,
}

/// The aggregate under a plan's coordinator operators: group keys and aggregates.
fn plan_aggregate(plan: &WcolPhysicalPlan) -> Option<(&[u32], &[AggExpr])> {
    plan.operators.iter().find_map(|op| match &op.kind {
        OpKind::FinalAggregate {
            group_by,
            aggregates,
        }
        | OpKind::Aggregate {
            group_by,
            aggregates,
        } => Some((group_by.as_slice(), aggregates.as_slice())),
        OpKind::MetadataAggregate { aggregates, .. } => Some((&[][..], aggregates.as_slice())),
        _ => None,
    })
}

/// Result columns for `plan`. `output` is the frontend's `SELECT` list; without one an
/// aggregate is its keys then its aggregates, and a row query is the columns it emits.
pub fn result_columns(
    plan: &WcolPhysicalPlan,
    output: Option<&[OutputItem]>,
    column_names: &[String],
) -> Result<Vec<ResultColumn>, String> {
    let name = |column: u32| {
        column_names
            .get(column as usize)
            .cloned()
            .unwrap_or_else(|| format!("#{column}"))
    };
    let keys = plan_aggregate(plan).map(|(group_by, _)| group_by.len() as u32);
    match (output, keys) {
        (Some(output), None) => return row_result_columns(output),
        (Some(output), Some(keys)) => {
            return output
                .iter()
                .map(|item| {
                    let column = match item.source {
                        OutputSource::Key(index) => index,
                        OutputSource::Aggregate(index) => keys + index,
                        OutputSource::Column(_) => return Err(misfit(item)),
                    };
                    Ok(ResultColumn {
                        name: item.name.clone(),
                        column,
                    })
                })
                .collect();
        }
        (None, _) => {}
    }
    if let Some((group_by, aggregates)) = plan_aggregate(plan) {
        let keys = group_by
            .iter()
            .enumerate()
            .map(|(index, &column)| ResultColumn {
                name: name(column),
                column: index as u32,
            });
        let aggs = aggregates
            .iter()
            .enumerate()
            .map(|(index, agg)| ResultColumn {
                name: aggregate_name(agg, &name),
                column: (group_by.len() + index) as u32,
            });
        return Ok(keys.chain(aggs).collect());
    }
    // A row query emits its coordinator projection, or else the decoded columns.
    let emitted = plan
        .operators
        .iter()
        .rev()
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
        .unwrap_or_default();
    Ok(emitted
        .into_iter()
        .map(|column| ResultColumn {
            name: name(column),
            column,
        })
        .collect())
}

/// A row query's result columns: each `SELECT` item prints the scan column it names.
pub fn row_result_columns(output: &[OutputItem]) -> Result<Vec<ResultColumn>, String> {
    output
        .iter()
        .map(|item| match item.source {
            OutputSource::Column(column) => Ok(ResultColumn {
                name: item.name.clone(),
                column,
            }),
            OutputSource::Key(_) | OutputSource::Aggregate(_) => Err(misfit(item)),
        })
        .collect()
}

fn misfit(item: &OutputItem) -> String {
    format!("result column {} does not fit the plan", item.name)
}

/// DuckDB's name for an unaliased aggregate: `count_star()`, `sum(a)`, `sum((a + 1))`.
fn aggregate_name(agg: &AggExpr, name: &impl Fn(u32) -> String) -> String {
    let arg = agg.column.map(name).unwrap_or_default();
    let arg = match agg.offset {
        0 => arg,
        offset if offset > 0 => format!("({arg} + {offset})"),
        offset => format!("({arg} - {})", -i16::from(offset)),
    };
    match agg.kind {
        AggKind::CountStar => "count_star()".to_string(),
        AggKind::Count => format!("count({arg})"),
        AggKind::CountDistinct => format!("count(DISTINCT {arg})"),
        AggKind::Sum => format!("sum({arg})"),
        AggKind::Avg => format!("avg({arg})"),
        AggKind::Min => format!("min({arg})"),
        AggKind::Max => format!("max({arg})"),
        AggKind::ApproxDistinct => format!("approx_count_distinct({arg})"),
    }
}

/// The finalizer's steps for an aggregate plan: the plan's coordinator sort and limit, with
/// sort targets moved onto the finalized row columns. `None` for a plan with no aggregate.
pub fn finalize_steps(plan: &WcolPhysicalPlan) -> Option<Result<Vec<CoordinatorStep>, String>> {
    let (group_by, _) = plan_aggregate(plan)?;
    let keys = group_by.len() as u32;
    let steps = crate::plan::coordinator_steps(plan)
        .into_iter()
        .filter_map(|step| match step {
            CoordinatorStep::Sort(sort) => Some(
                sort.into_iter()
                    .map(|key| {
                        let column = match key.target {
                            SortTarget::Aggregate(index) => keys + index,
                            SortTarget::Column(column) => {
                                group_by.iter().position(|&c| c == column).ok_or_else(|| {
                                    format!("sort column {column} is not a group key")
                                })? as u32
                            }
                        };
                        Ok(SortKey {
                            target: SortTarget::Column(column),
                            ..key
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map(CoordinatorStep::Sort),
            ),
            step @ CoordinatorStep::Limit { .. } => Some(Ok(step)),
            CoordinatorStep::Project(_) | CoordinatorStep::Parquet { .. } => None,
        })
        .collect();
    Some(steps)
}

/// An aggregate export's finalizer steps: [`finalize_steps`], then the result columns in
/// `SELECT` order, then one Parquet file. Columns are typed from the source: group keys keep
/// their Parquet type (floats widen to DOUBLE, as the finalizer does); counts and integer
/// `SUM` / `MIN` / `MAX` are INT64; `AVG` and float aggregates are DOUBLE.
pub fn export_steps(
    plan: &WcolPhysicalPlan,
    columns: &[ResultColumn],
    leaves: &[crate::formats::parquet::Leaf],
    target_row_group_rows: u32,
) -> Result<Vec<CoordinatorStep>, String> {
    const INT64: i32 = 2;
    const DOUBLE: i32 = 5;
    let (group_by, aggregates) = plan_aggregate(plan).ok_or("export plan has no aggregate")?;
    let mut steps = finalize_steps(plan).ok_or("export plan has no aggregate")??;
    let leaf = |column: u32| {
        leaves
            .get(column as usize)
            .ok_or_else(|| format!("column {column} is not in the footer"))
    };
    let float = |physical: i32| matches!(physical, 4 | 5);
    let mut export = Vec::with_capacity(columns.len());
    for result in columns {
        let index = result.column as usize;
        let (physical, type_length, optional) = if index < group_by.len() {
            let source = leaf(group_by[index])?;
            if float(source.physical) {
                (DOUBLE, 0, true)
            } else {
                (source.physical, source.type_length, true)
            }
        } else {
            let agg = aggregates
                .get(index - group_by.len())
                .ok_or_else(|| format!("result column {} is not in the plan", result.name))?;
            match agg.kind {
                AggKind::CountStar
                | AggKind::Count
                | AggKind::CountDistinct
                | AggKind::ApproxDistinct => (INT64, 0, false),
                AggKind::Avg => (DOUBLE, 0, true),
                AggKind::Sum | AggKind::Min | AggKind::Max => {
                    let column = agg.column.ok_or("aggregate has no column")?;
                    let source = leaf(column)?;
                    if float(source.physical) {
                        (DOUBLE, 0, true)
                    } else if matches!(source.physical, 1 | 2) {
                        (INT64, 0, true)
                    } else {
                        return Err(format!("cannot export {} of a text column", result.name));
                    }
                }
            }
        };
        export.push(crate::program::ExportColumn {
            name: result.name.clone(),
            optional,
            physical,
            type_length,
        });
    }
    steps.push(CoordinatorStep::Project(
        columns.iter().map(|c| c.column).collect(),
    ));
    steps.push(CoordinatorStep::Parquet {
        target_row_group_rows,
        columns: export,
    });
    Ok(steps)
}

pub fn encode_result_columns(columns: &[ResultColumn]) -> Vec<u8> {
    let mut out = Vec::new();
    push_u32(&mut out, columns.len() as u32);
    for column in columns {
        push_str(&mut out, &column.name);
        push_u32(&mut out, column.column);
    }
    out
}

pub fn decode_result_columns(mut bytes: &[u8]) -> Result<Vec<ResultColumn>, LogicalWireError> {
    let count = read_u32(&mut bytes)? as usize;
    let mut columns = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let name = read_str(&mut bytes)?;
        let column = read_u32(&mut bytes)?;
        columns.push(ResultColumn { name, column });
    }
    if !bytes.is_empty() {
        return Err(LogicalWireError::UnknownTag);
    }
    Ok(columns)
}

/// `{"columns":[..],"rows":[[..],..]}` from the root task's WCS1 row frames. With `row_cap`
/// above zero, at most that many rows print and a capped body ends `"truncated":true`.
pub fn rows_json(columns: &[ResultColumn], wcs: &[u8], row_cap: u32) -> Result<String, String> {
    let mut out = String::new();
    write_header(columns, &mut out);
    let mut first = true;
    let mut left = if row_cap == 0 {
        usize::MAX
    } else {
        row_cap as usize
    };
    let mut truncated = false;
    for_each_batch(wcs, |batch| {
        truncated = write_batch_rows(columns, batch, &mut first, &mut left, &mut out)?;
        Ok(truncated)
    })?;
    out.push(']');
    if truncated {
        out.push_str(",\"truncated\":true");
    }
    out.push('}');
    Ok(out)
}

/// Decode WCS1 row frames and hand each data batch to `visit`, until it returns `true`.
fn for_each_batch(
    wcs: &[u8],
    mut visit: impl FnMut(&OwnedWcolBatch) -> Result<bool, String>,
) -> Result<(), String> {
    let mut ingress = WcsIngress::new();
    let mut dicts: HashMap<(u32, u32), DictionaryValues> = HashMap::new();
    let mut rest = wcs;
    while !rest.is_empty() {
        let (consumed, frames) = ingress.feed(rest).map_err(|e| e.to_string())?;
        if consumed == 0 && frames.is_empty() {
            return Err("row frames are truncated".to_string());
        }
        rest = &rest[consumed..];
        for frame in frames {
            match frame.frame_type {
                FRAME_DICTIONARY => {
                    apply_dictionary_payload(&mut dicts, &frame.payload)
                        .map_err(|e| e.to_string())?;
                }
                FRAME_DATA_BATCH => {
                    let batch = owned_batch_from_projection_body_with_dicts(&frame.payload, &dicts)
                        .map_err(|e| e.to_string())?;
                    if visit(&batch)? {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// The selected values of I64 column `column`, in frame order.
pub fn i64_column_values(wcs: &[u8], column: u32) -> Result<Vec<i64>, String> {
    let mut values = Vec::new();
    for_each_batch(wcs, |batch| {
        let mut storage = Vec::new();
        let view = batch.view_with(&mut storage);
        let Some(WcolColumn::I64 { values: ids, .. }) =
            view.columns.iter().find(|c| column_id(c) == column)
        else {
            return Err(format!("row frames have no integer column {column}"));
        };
        let mask = batch.effective_selection_mask();
        values.extend(
            (0..view.row_count)
                .filter(|&row| mask.is_selected(row))
                .map(|row| ids[row]),
        );
        Ok(false)
    })?;
    Ok(values)
}

/// Rows put back together by id. `order` lists the row ids in result order; `parts` holds
/// row frames whose batches each carry the id column `row_id` and some of the result columns.
/// Every result column of every listed row must appear in exactly one batch.
pub fn rows_by_id_json(
    columns: &[ResultColumn],
    row_id: u32,
    order: &[i64],
    parts: &[u8],
    row_cap: u32,
) -> Result<String, String> {
    let shown = if row_cap == 0 {
        order.len()
    } else {
        order.len().min(row_cap as usize)
    };
    let wanted: HashMap<i64, usize> = order[..shown]
        .iter()
        .enumerate()
        .map(|(index, &id)| (id, index))
        .collect();
    let mut cells: Vec<Option<String>> = vec![None; shown * columns.len()];
    for_each_batch(parts, |batch| {
        let mut storage = Vec::new();
        let view = batch.view_with(&mut storage);
        let Some(WcolColumn::I64 { values: ids, .. }) =
            view.columns.iter().find(|c| column_id(c) == row_id)
        else {
            return Err(format!("row frames have no row id column {row_id}"));
        };
        let present: Vec<(usize, &WcolColumn<'_>)> = columns
            .iter()
            .enumerate()
            .filter_map(|(at, result)| {
                view.columns
                    .iter()
                    .find(|c| column_id(c) == result.column)
                    .map(|c| (at, c))
            })
            .collect();
        let mask = batch.effective_selection_mask();
        for row in (0..view.row_count).filter(|&row| mask.is_selected(row)) {
            let Some(&index) = wanted.get(&ids[row]) else {
                continue;
            };
            for &(at, column) in &present {
                let mut cell = String::new();
                write_cell(&mut cell, column, row)?;
                if cells[index * columns.len() + at].replace(cell).is_some() {
                    return Err(format!("row {} has column {} twice", ids[row], at));
                }
            }
        }
        Ok(false)
    })?;
    let mut out = String::new();
    write_header(columns, &mut out);
    for (index, row) in cells.chunks(columns.len().max(1)).enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('[');
        for (at, cell) in row.iter().enumerate() {
            if at > 0 {
                out.push(',');
            }
            let cell = cell.as_deref().ok_or_else(|| {
                format!(
                    "row {} is missing column {}",
                    order[index], columns[at].name
                )
            })?;
            out.push_str(cell);
        }
        out.push(']');
    }
    out.push(']');
    if shown < order.len() {
        out.push_str(",\"truncated\":true");
    }
    out.push('}');
    Ok(out)
}

/// JSON for a footer-only answer: the planner's merged partial, finalized here.
pub fn partial_json(columns: &[ResultColumn], partial: &[u8]) -> Result<String, String> {
    let snap = partial_snapshot_from_wire(partial).map_err(|err| format!("partial: {err}"))?;
    let batch = finalized_groups_batch(&snap)?;
    let mut out = String::new();
    write_header(columns, &mut out);
    let mut left = usize::MAX;
    write_batch_rows(columns, &batch, &mut true, &mut left, &mut out)?;
    out.push_str("]}");
    Ok(out)
}

fn write_header(columns: &[ResultColumn], out: &mut String) {
    out.push_str("{\"columns\":[");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_string(out, column.name.as_bytes());
    }
    out.push_str("],\"rows\":[");
}

/// Print `batch`'s selected rows, at most `left` of them. Returns whether rows were left out.
fn write_batch_rows(
    columns: &[ResultColumn],
    batch: &OwnedWcolBatch,
    first: &mut bool,
    left: &mut usize,
    out: &mut String,
) -> Result<bool, String> {
    let mut storage = Vec::new();
    let view = batch.view_with(&mut storage);
    let cells = columns
        .iter()
        .map(|result| {
            view.columns
                .iter()
                .find(|column| column_id(column) == result.column)
                .ok_or_else(|| format!("row frames have no column {}", result.column))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mask = batch.effective_selection_mask();
    for row in 0..view.row_count {
        if !mask.is_selected(row) {
            continue;
        }
        if *left == 0 {
            return Ok(true);
        }
        *left -= 1;
        if !*first {
            out.push(',');
        }
        *first = false;
        out.push('[');
        for (index, column) in cells.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_cell(out, column, row)?;
        }
        out.push(']');
    }
    Ok(false)
}

fn column_id(column: &WcolColumn<'_>) -> u32 {
    match column {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

fn write_cell(out: &mut String, column: &WcolColumn<'_>, row: usize) -> Result<(), String> {
    match column {
        WcolColumn::I64 { values, nulls, .. } => {
            if nulls[row] == 0 {
                out.push_str("null");
            } else {
                write_i64(out, values[row]);
            }
        }
        WcolColumn::F64 { values, nulls, .. } => {
            if nulls[row] == 0 {
                out.push_str("null");
            } else {
                write_f64(out, values[row]);
            }
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            if nulls[row] == 0 {
                out.push_str("null");
            } else {
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                write_json_string(out, &bytes[start..end]);
            }
        }
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            if nulls[row] == 0 {
                out.push_str("null");
                return Ok(());
            }
            let id = ids[row] as usize;
            let missing = || format!("dictionary id {id} is out of range");
            match &dictionary.entries {
                DictEntries::Int(entries) => write_i64(out, *entries.get(id).ok_or_else(missing)?),
                DictEntries::Float(entries) => {
                    write_f64(out, *entries.get(id).ok_or_else(missing)?)
                }
                DictEntries::Bytes { offsets, bytes } => {
                    if id + 1 >= offsets.len() {
                        return Err(missing());
                    }
                    write_json_string(out, &bytes[offsets[id] as usize..offsets[id + 1] as usize]);
                }
            }
        }
    }
    Ok(())
}

/// Largest integer a JSON client reading numbers as doubles (every JavaScript client) keeps exact.
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

/// Integers outside ±(2^53 − 1) are strings: as numbers, `JSON.parse` would round them.
fn write_i64(out: &mut String, value: i64) {
    if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
        out.push_str(&value.to_string());
    } else {
        out.push('"');
        out.push_str(&value.to_string());
        out.push('"');
    }
}

/// Non-finite floats are strings, since JSON numbers cannot carry them.
fn write_f64(out: &mut String, value: f64) {
    if value.is_nan() {
        out.push_str("\"NaN\"");
    } else if value.is_infinite() {
        out.push_str(if value > 0.0 {
            "\"Infinity\""
        } else {
            "\"-Infinity\""
        });
    } else if value == 0.0 {
        out.push('0');
    } else {
        out.push_str(&value.to_string());
    }
}

fn write_json_string(out: &mut String, bytes: &[u8]) {
    out.push('"');
    for ch in String::from_utf8_lossy(bytes).chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::operators::RowBatchWcsEgress;

    fn frames(batch: &OwnedWcolBatch) -> Vec<u8> {
        let mut egress = RowBatchWcsEgress::new();
        egress.push_batch(batch).unwrap();
        egress.push_end().unwrap();
        let mut out = Vec::new();
        let mut buf = vec![0u8; 4096];
        while egress.has_pending_output() {
            let n = egress.poll_write(&mut buf).unwrap();
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    #[test]
    fn row_frames_print_in_result_column_order() {
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![1 << 53, 0], vec![1, 0]);
        batch.push_utf8_column(1, vec![0, 3, 6], b"a\"bc\nd".to_vec(), vec![1, 1]);
        batch.push_f64_column(2, vec![f64::NAN, 1.5], vec![1, 1]);
        batch.set_row_count(2).unwrap();
        let columns = vec![
            ResultColumn {
                name: "f".into(),
                column: 2,
            },
            ResultColumn {
                name: "k".into(),
                column: 0,
            },
            ResultColumn {
                name: "s".into(),
                column: 1,
            },
            ResultColumn {
                name: "k2".into(),
                column: 0,
            },
        ];
        let json = rows_json(&columns, &frames(&batch), 0).unwrap();
        let capped = rows_json(&columns[1..2], &frames(&batch), 1).unwrap();
        assert_eq!(
            capped,
            r#"{"columns":["k"],"rows":[["9007199254740992"]],"truncated":true}"#
        );
        assert_eq!(
            json,
            r#"{"columns":["f","k","s","k2"],"rows":[["NaN","9007199254740992","a\"b","9007199254740992"],[1.5,null,"c\nd",null]]}"#
        );
    }

    #[test]
    fn result_columns_round_trip() {
        let columns = vec![ResultColumn {
            name: "count_star()".into(),
            column: 3,
        }];
        assert_eq!(
            decode_result_columns(&encode_result_columns(&columns)).unwrap(),
            columns
        );
    }
}
