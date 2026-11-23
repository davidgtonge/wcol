use super::*;

mod exchange;
mod export;
mod grouping;
mod placement;
mod sizing;

pub(super) fn spill(enabled: bool) -> HashAggregateConfig {
    HashAggregateConfig {
        group_spill_enabled: enabled,
        ..HashAggregateConfig::default()
    }
}

pub(super) fn draft(offset: u64, length: u64) -> DraftRead {
    DraftRead {
        column: 0,
        offset,
        length,
        dictionary: false,
        codec: 0,
    }
}

pub(super) fn nodes(count: u32) -> Vec<NodeId> {
    (0..count).map(NodeId).collect()
}

pub(super) fn chunk(column: u32, row_group: u32, offset: u64, length: u64) -> ChunkMeta {
    ChunkMeta {
        column,
        row_group,
        offset,
        length,
        rows: 10,
        dictionary_in_span: false,
        dictionary_page: None,
        codec: 0,
        int_range: None,
    }
}

pub(super) fn catalog(chunks: Vec<ChunkMeta>) -> ParquetMetadataCatalog {
    ParquetMetadataCatalog {
        tables: vec![TableMeta {
            table: "respondents".into(),
            objects: vec![ParquetObject {
                object_id: "respondents.parquet".into(),
                chunks,
            }],
        }],
        leaf_physical: Vec::new(),
        leaf_distinct_upper: Vec::new(),
        leaf_dictionary_bytes: Vec::new(),
        leaf_min_i64: Vec::new(),
        leaf_max_i64: Vec::new(),
        leaf_min_bytes: Vec::new(),
        leaf_max_bytes: Vec::new(),
    }
}

pub(super) fn bounded(column: u32, row_group: u32, min: i64, max: i64) -> ChunkMeta {
    ChunkMeta {
        int_range: Some((min, max)),
        ..chunk(
            column,
            row_group,
            u64::from(row_group) * 100 + u64::from(column) * 10,
            10,
        )
    }
}

pub(super) fn pruned_scan(predicates: Vec<Predicate>, chunks: Vec<ChunkMeta>) -> Vec<u32> {
    let linear = Linear {
        table: "respondents".into(),
        columns: vec![0, 1],
        steps: vec![Step::Filter(predicates)],
    };
    resolve(0, &linear, &catalog(chunks))
        .unwrap()
        .iter()
        .map(|unit| unit.row_group)
        .collect()
}

pub(super) fn compare(column: u32, op: CmpOp, value: i64) -> Predicate {
    Predicate::Compare(Compare {
        column,
        op,
        value: Literal::Int(value),
    })
}

pub(super) fn example() -> WcolLogicalPlan {
    WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: vec![0],
            aggregates: vec![AggExpr {
                kind: AggKind::Avg,
                column: Some(1),
                offset: 0,
            }],
            input: Box::new(LogicalOp::Filter {
                predicates: vec![Predicate::Compare(Compare {
                    column: 2,
                    op: CmpOp::Gt,
                    value: Literal::Int(30),
                })],
                input: Box::new(LogicalOp::Scan {
                    table: "respondents".into(),
                    columns: vec![0, 1, 2],
                }),
            }),
        },
    }
}

pub(super) fn sum_column() -> WcolLogicalPlan {
    WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: vec![],
            aggregates: vec![AggExpr {
                kind: AggKind::Sum,
                column: Some(0),
                offset: 0,
            }],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0],
            }),
        },
    }
}

pub(super) fn nearby_groups() -> Vec<ChunkMeta> {
    let mut chunks = Vec::new();
    for row_group in 0..2 {
        let base = u64::from(row_group) * 5_000_000;
        for column in 0..3 {
            chunks.push(chunk(column, row_group, base + u64::from(column) * 100, 64));
        }
    }
    chunks
}

/// `respondents` as `parts` objects of `groups` row groups each (column 0 only).
pub(super) fn split_catalog(parts: u32, groups: u32) -> ParquetMetadataCatalog {
    let mut meta = catalog(Vec::new());
    meta.tables[0].objects = (0..parts)
        .map(|part| ParquetObject {
            object_id: format!("respondents#{part}"),
            chunks: (0..groups)
                .map(|group| chunk(0, group, 4 + u64::from(group) * 1000, 900))
                .collect(),
        })
        .collect();
    meta
}

pub(super) fn objects_per_node(
    plan: &WcolPhysicalPlan,
) -> std::collections::BTreeMap<NodeId, std::collections::BTreeSet<String>> {
    let mut out = std::collections::BTreeMap::<NodeId, std::collections::BTreeSet<String>>::new();
    for fetch in &plan.fetches {
        out.entry(fetch.node)
            .or_default()
            .insert(fetch.object_id.clone());
    }
    out
}

pub(super) fn brute_min_max_load(weights: &[u64], node_count: usize) -> u64 {
    if weights.is_empty() || node_count == 0 {
        return 0;
    }
    let nodes = node_count.min(weights.len());
    fn rec(weights: &[u64], nodes_left: usize, start: usize, cur_max: u64, best: &mut u64) {
        if nodes_left == 1 {
            let load: u64 = weights[start..].iter().sum();
            *best = (*best).min(cur_max.max(load));
            return;
        }
        for end in (start + 1)..=(weights.len() - (nodes_left - 1)) {
            let load: u64 = weights[start..end].iter().sum();
            rec(weights, nodes_left - 1, end, cur_max.max(load), best);
        }
    }
    let mut best = u64::MAX;
    rec(weights, nodes, 0, 0, &mut best);
    best
}

pub(super) fn example_two_columns() -> WcolLogicalPlan {
    WcolLogicalPlan {
        root: LogicalOp::Filter {
            predicates: vec![Predicate::Compare(Compare {
                column: 1,
                op: CmpOp::Gt,
                value: Literal::Int(0),
            })],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![1, 0],
            }),
        },
    }
}

pub(super) fn decode_row_groups(plan: &WcolPhysicalPlan) -> Vec<Vec<u32>> {
    plan.operators
        .iter()
        .filter_map(|op| match &op.kind {
            OpKind::Decode { groups } => Some(groups.iter().map(|group| group.row_group).collect()),
            _ => None,
        })
        .collect()
}

pub(super) fn export_config(partitioned: bool) -> PhysicalPlannerConfig {
    PhysicalPlannerConfig {
        parquet_row_group_rows: Some(1024),
        partition_columns: if partitioned { vec![0] } else { Vec::new() },
        ..PhysicalPlannerConfig::default()
    }
}

pub(super) fn chain_has(ops: &[Operator], mut id: u32, pred: impl Fn(&OpKind) -> bool) -> bool {
    loop {
        let op = &ops[id as usize];
        if pred(&op.kind) {
            return true;
        }
        if op.inputs.is_empty() {
            return false;
        }
        id = op.inputs[0];
    }
}

pub(super) fn grouped_count(keys: Vec<u32>) -> WcolLogicalPlan {
    WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: keys,
            aggregates: vec![AggExpr {
                kind: AggKind::CountStar,
                column: None,
                offset: 0,
            }],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0, 1],
            }),
        },
    }
}

pub(super) fn million_row_keys() -> ParquetMetadataCatalog {
    let mut left = chunk(0, 0, 0, 64);
    left.rows = 1_000_000;
    let mut right = chunk(1, 0, 64, 64);
    right.rows = 1_000_000;
    let mut meta = catalog(vec![left, right]);
    meta.leaf_physical = vec![1, 1];
    meta
}

pub(super) fn grouped_distinct(keys: Vec<u32>, extra_count: bool) -> WcolLogicalPlan {
    let mut aggregates = vec![AggExpr {
        kind: AggKind::CountDistinct,
        column: Some(1),
        offset: 0,
    }];
    if extra_count {
        aggregates.push(AggExpr {
            kind: AggKind::CountStar,
            column: None,
            offset: 0,
        });
    }
    WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: keys,
            aggregates,
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0, 1],
            }),
        },
    }
}

/// A RegionID-like group column: too few groups to keep each one on a single reducer.
pub(super) fn few_groups() -> ParquetMetadataCatalog {
    let mut meta = million_row_keys();
    meta.leaf_distinct_upper = vec![Some(1_000), None];
    meta
}
