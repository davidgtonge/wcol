//! Storage-free logical plan and its text form.

use super::*;

/// Storage-free plan. Byte offsets and node ids are not representable here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalOp {
    Scan {
        table: String,
        columns: Vec<u32>,
    },
    Filter {
        input: Box<LogicalOp>,
        predicates: Vec<Predicate>,
    },
    Project {
        input: Box<LogicalOp>,
        columns: Vec<u32>,
    },
    Aggregate {
        input: Box<LogicalOp>,
        group_by: Vec<u32>,
        aggregates: Vec<AggExpr>,
    },
    Sort {
        input: Box<LogicalOp>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalOp>,
        skip: u64,
        fetch: Option<u64>,
    },
    Join {
        left: Box<LogicalOp>,
        right: Box<LogicalOp>,
        left_keys: Vec<u32>,
        right_keys: Vec<u32>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WcolLogicalPlan {
    pub root: LogicalOp,
}

impl WcolLogicalPlan {
    pub fn display(&self) -> String {
        let mut out = String::new();
        write_logical(&self.root, 0, &mut out);
        out
    }
}

pub(super) fn write_logical(op: &LogicalOp, indent: usize, out: &mut String) {
    for _ in 0..indent {
        out.push_str("  ");
    }
    match op {
        LogicalOp::Scan { table, columns } => {
            out.push_str(&format!("Scan({table}, {columns:?})\n"));
        }
        LogicalOp::Filter { input, predicates } => {
            out.push_str("Filter(");
            for (i, pred) in predicates.iter().enumerate() {
                if i > 0 {
                    out.push_str(" and ");
                }
                out.push_str(&fmt_predicate(pred));
            }
            out.push_str(")\n");
            write_logical(input, indent + 1, out);
        }
        LogicalOp::Project { input, columns } => {
            out.push_str(&format!("Project({columns:?})\n"));
            write_logical(input, indent + 1, out);
        }
        LogicalOp::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let aggs = aggregates
                .iter()
                .map(fmt_agg)
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("Aggregate(group={group_by:?}, {aggs})\n"));
            write_logical(input, indent + 1, out);
        }
        LogicalOp::Sort { input, keys } => {
            out.push_str("Sort(");
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!(
                    "{} {} {}",
                    fmt_sort_target(key.target),
                    if key.descending { "desc" } else { "asc" },
                    if key.nulls_first {
                        "nulls_first"
                    } else {
                        "nulls_last"
                    }
                ));
            }
            out.push_str(")\n");
            write_logical(input, indent + 1, out);
        }
        LogicalOp::Limit { input, skip, fetch } => {
            out.push_str(&format!(
                "Limit(skip={skip}, fetch={})\n",
                fetch
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "none".into())
            ));
            write_logical(input, indent + 1, out);
        }
        LogicalOp::Join {
            left,
            right,
            left_keys,
            right_keys,
        } => {
            out.push_str(&format!("Join(left={left_keys:?}, right={right_keys:?})\n"));
            write_logical(left, indent + 1, out);
            write_logical(right, indent + 1, out);
        }
    }
}
