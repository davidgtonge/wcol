//! Physical-plan values → engine IR. The batch kernels never see planner types.

use super::types::{self as physical_plan, Literal};
use crate::ir::{self, AggExpr, Predicate, Scalar, SortKey};

pub fn predicates(predicates: &[physical_plan::Predicate]) -> Result<Vec<Predicate>, String> {
    predicates.iter().map(predicate).collect()
}

pub fn aggregates(aggregates: &[physical_plan::AggExpr]) -> Result<Vec<AggExpr>, String> {
    aggregates.iter().map(aggregate).collect()
}

pub fn aggregate(agg: &physical_plan::AggExpr) -> Result<AggExpr, String> {
    let kind = match agg.kind {
        physical_plan::AggKind::CountStar => ir::AggKind::CountStar,
        physical_plan::AggKind::Count => ir::AggKind::Count,
        physical_plan::AggKind::CountDistinct => ir::AggKind::CountDistinct,
        physical_plan::AggKind::Sum => ir::AggKind::Sum,
        physical_plan::AggKind::Avg => ir::AggKind::Avg,
        physical_plan::AggKind::Min => ir::AggKind::Min,
        physical_plan::AggKind::Max => ir::AggKind::Max,
        physical_plan::AggKind::ApproxDistinct => ir::AggKind::ApproxDistinct,
    };
    if kind != ir::AggKind::CountStar && agg.column.is_none() {
        return Err(format!("{:?} needs a column", agg.kind));
    }
    Ok(AggExpr {
        column: if kind == ir::AggKind::CountStar {
            None
        } else {
            agg.column
        },
        kind,
        offset: agg.offset,
    })
}

/// A row sort key. An aggregate ordinal is not a scan column.
pub fn sort_key(key: &physical_plan::SortKey) -> Result<SortKey, String> {
    let physical_plan::SortTarget::Column(column) = key.target else {
        return Err("row sort key is not a column".to_string());
    };
    Ok(SortKey {
        column,
        descending: key.descending,
        nulls_first: key.nulls_first,
    })
}

fn predicate(predicate: &physical_plan::Predicate) -> Result<Predicate, String> {
    Ok(match predicate {
        physical_plan::Predicate::Compare(compare) => Predicate::Compare {
            column: compare.column,
            op: match compare.op {
                physical_plan::CmpOp::Eq => ir::CmpOp::Eq,
                physical_plan::CmpOp::NotEq => ir::CmpOp::Ne,
                physical_plan::CmpOp::Lt => ir::CmpOp::Lt,
                physical_plan::CmpOp::LtEq => ir::CmpOp::Le,
                physical_plan::CmpOp::Gt => ir::CmpOp::Gt,
                physical_plan::CmpOp::GtEq => ir::CmpOp::Ge,
            },
            value: scalar(&compare.value),
        },
        physical_plan::Predicate::In {
            column,
            values,
            negated,
        } => Predicate::In {
            column: *column,
            values: values.iter().map(scalar).collect(),
            negated: *negated,
        },
        physical_plan::Predicate::Like {
            column,
            pattern,
            negated,
        } => Predicate::Like {
            column: *column,
            pattern: pattern.clone(),
            negated: *negated,
        },
        physical_plan::Predicate::IsNull { .. } | physical_plan::Predicate::IsNotNull { .. } => {
            return Err("null predicates are not supported by the batch filter".to_string());
        }
    })
}

fn scalar(literal: &Literal) -> Scalar {
    match literal {
        Literal::Int(v) => Scalar::Int(*v),
        Literal::Float(bits) => Scalar::Number(f64::from_bits(*bits)),
        Literal::Bool(v) => Scalar::Int(i64::from(*v)),
        Literal::Text(v) => Scalar::Text(v.clone()),
    }
}
