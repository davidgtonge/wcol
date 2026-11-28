//! Text form of operators, predicates and aggregates.

use super::*;

pub(super) fn fmt_sort_target(target: SortTarget) -> String {
    match target {
        SortTarget::Column(column) => column.to_string(),
        SortTarget::Aggregate(index) => format!("agg:{index}"),
    }
}

pub(super) fn fmt_predicate(pred: &Predicate) -> String {
    match pred {
        Predicate::Compare(compare) => format!(
            "{} {} {}",
            compare.column,
            fmt_cmp(compare.op),
            fmt_literal(&compare.value)
        ),
        Predicate::IsNull { column } => format!("{column} is null"),
        Predicate::IsNotNull { column } => format!("{column} is not null"),
        Predicate::Like {
            column,
            pattern,
            negated,
        } => format!(
            "{column} {}like text:{pattern:?}",
            if *negated { "not " } else { "" }
        ),
        Predicate::In {
            column,
            values,
            negated,
        } => {
            let body = values
                .iter()
                .map(fmt_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{column} {}in [{body}]", if *negated { "not " } else { "" })
        }
    }
}

pub(super) fn fmt_cmp(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "eq",
        CmpOp::NotEq => "ne",
        CmpOp::Lt => "lt",
        CmpOp::LtEq => "le",
        CmpOp::Gt => "gt",
        CmpOp::GtEq => "ge",
    }
}

pub(super) fn fmt_literal(lit: &Literal) -> String {
    match lit {
        Literal::Int(value) => format!("i64:{value}"),
        Literal::Float(bits) => format!("f64:{bits:#x}"),
        Literal::Bool(value) => format!("bool:{value}"),
        Literal::Text(value) => format!("text:{value:?}"),
    }
}

pub(super) fn fmt_agg(agg: &AggExpr) -> String {
    let text = fmt_agg_body(agg);
    match (agg.offset, text.strip_suffix(')')) {
        (0, _) | (_, None) => text,
        (offset, Some(open)) => format!("{open} {offset:+})"),
    }
}

pub(super) fn fmt_agg_body(agg: &AggExpr) -> String {
    match agg.kind {
        AggKind::CountStar => "count(*)".into(),
        AggKind::Count => format!("count({})", agg.column.unwrap_or(0)),
        AggKind::Sum => format!("sum({})", agg.column.unwrap_or(0)),
        AggKind::Min => format!("min({})", agg.column.unwrap_or(0)),
        AggKind::Max => format!("max({})", agg.column.unwrap_or(0)),
        AggKind::Avg => format!("avg({})", agg.column.unwrap_or(0)),
        AggKind::CountDistinct => format!("count(distinct {})", agg.column.unwrap_or(0)),
        AggKind::ApproxDistinct => format!("approx_distinct({})", agg.column.unwrap_or(0)),
    }
}

pub(super) fn fmt_kind(kind: &OpKind) -> String {
    match kind {
        OpKind::R2Fetch { fetch_ids } => format!("r2_fetch {fetch_ids:?}"),
        OpKind::Decode { groups } => {
            let body = groups
                .iter()
                .map(|group| {
                    format!(
                        "{} rg={} rows={} columns={:?} reads={:?}",
                        group.object_id, group.row_group, group.rows, group.columns, group.reads
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            format!("decode [{body}]")
        }
        OpKind::Filter { predicates } => {
            let body = predicates
                .iter()
                .map(fmt_predicate)
                .collect::<Vec<_>>()
                .join(" and ");
            format!("filter {body}")
        }
        OpKind::Project { columns } => format!("project {columns:?}"),
        OpKind::PartialAggregate {
            group_by,
            aggregates,
        } => {
            format!(
                "partial_aggregate group={group_by:?} {}",
                fmt_aggs(aggregates)
            )
        }
        OpKind::FinalAggregate {
            group_by,
            aggregates,
        } => {
            format!(
                "final_aggregate group={group_by:?} {}",
                fmt_aggs(aggregates)
            )
        }
        OpKind::Exchange => "exchange".into(),
        OpKind::HashExchange {
            keys,
            partitions,
            hash_version,
        } => format!("hash_exchange keys={keys:?} partitions={partitions} hash_v={hash_version}"),
        OpKind::RangeExchange {
            keys,
            partitions,
            splits: _,
        } => format!("range_exchange keys={keys:?} partitions={partitions}"),
        OpKind::Sort { keys } => {
            let body = keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {} {}",
                        fmt_sort_target(key.target),
                        if key.descending { "desc" } else { "asc" },
                        if key.nulls_first {
                            "nulls_first"
                        } else {
                            "nulls_last"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("sort {body}")
        }
        OpKind::Limit { skip, fetch } => format!(
            "limit skip={skip} fetch={}",
            fetch
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".into())
        ),
        OpKind::Sink {
            kind: SinkKind::Rows,
        } => "sink rows".into(),
        OpKind::Sink {
            kind:
                SinkKind::Parquet {
                    target_row_group_rows,
                    partition_columns,
                },
        } => format!(
            "sink parquet row_group_rows={target_row_group_rows} partition={partition_columns:?}"
        ),
        OpKind::MetadataAggregate { rows, bounds, .. } => {
            format!("metadata_aggregate rows={rows} bounds={bounds:?}")
        }
        OpKind::TopK { keys, n } => {
            let body = keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {} {}",
                        fmt_sort_target(key.target),
                        if key.descending { "desc" } else { "asc" },
                        if key.nulls_first {
                            "nulls_first"
                        } else {
                            "nulls_last"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("topk n={n} {body}")
        }
    }
}

pub(super) fn fmt_aggs(aggregates: &[AggExpr]) -> String {
    aggregates
        .iter()
        .map(fmt_agg)
        .collect::<Vec<_>>()
        .join(", ")
}
