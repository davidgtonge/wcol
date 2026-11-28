use super::*;

#[test]
fn sort_and_limit_run_after_exchange() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Limit {
            skip: 5,
            fetch: Some(10),
            input: Box::new(LogicalOp::Sort {
                keys: vec![SortKey {
                    target: SortTarget::Column(0),
                    descending: true,
                    nulls_first: false,
                }],
                input: Box::new(example().root),
            }),
        },
    };
    let plan = lower_to_physical(
        &logical,
        &catalog(nearby_groups()),
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let limit = &plan.operators[plan.root as usize];
    assert!(matches!(
        limit.kind,
        OpKind::Limit {
            skip: 5,
            fetch: Some(10)
        }
    ));
    let sort = &plan.operators[limit.inputs[0] as usize];
    assert!(matches!(sort.kind, OpKind::Sort { .. }));
    assert!(matches!(
        plan.operators[sort.inputs[0] as usize].kind,
        OpKind::FinalAggregate { .. }
    ));
}

#[test]
fn topk_only_runs_on_nodes_for_plain_row_scans() {
    let top = |input: LogicalOp| WcolLogicalPlan {
        root: LogicalOp::Limit {
            skip: 2,
            fetch: Some(3),
            input: Box::new(LogicalOp::Sort {
                keys: vec![SortKey {
                    target: SortTarget::Column(0),
                    descending: true,
                    nulls_first: false,
                }],
                input: Box::new(input),
            }),
        },
    };
    let topk_nodes = |plan: &WcolPhysicalPlan| -> Vec<(Option<NodeId>, u64)> {
        plan.operators
            .iter()
            .filter_map(|op| match &op.kind {
                OpKind::TopK { n, .. } => Some((op.node, *n)),
                _ => None,
            })
            .collect()
    };
    let scan = || LogicalOp::Scan {
        table: "respondents".into(),
        columns: vec![0],
    };
    let meta = catalog(
        (0..4)
            .map(|row_group| chunk(0, row_group, u64::from(row_group) * 8_000, 100))
            .collect(),
    );
    let lower = |logical: &WcolLogicalPlan| {
        lower_to_physical(logical, &meta, &nodes(2), &PhysicalPlannerConfig::default()).unwrap()
    };

    // Plain rows: each node keeps skip + fetch rows; the coordinator still sorts and limits.
    let rows = lower(&top(scan()));
    assert_eq!(
        topk_nodes(&rows),
        vec![(Some(NodeId(0)), 5), (Some(NodeId(1)), 5)]
    );
    let sink = &rows.operators[rows.root as usize];
    assert!(matches!(
        sink.kind,
        OpKind::Sink {
            kind: SinkKind::Rows
        }
    ));
    assert!(matches!(
        rows.operators[sink.inputs[0] as usize].kind,
        OpKind::Limit {
            skip: 2,
            fetch: Some(3)
        }
    ));

    // Aggregate over a scan, ordered by its group column.
    let scan_grouped = lower(&top(LogicalOp::Aggregate {
        input: Box::new(scan()),
        group_by: vec![0],
        aggregates: vec![AggExpr {
            kind: AggKind::CountStar,
            column: None,
            offset: 0,
        }],
    }));
    assert!(
        topk_nodes(&scan_grouped).is_empty(),
        "{}",
        scan_grouped.canonical_text()
    );
}

#[test]
fn missing_dependencies_are_rejected() {
    let meta = catalog(vec![chunk(0, 0, 0, 10)]);
    let missing = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![3],
        },
    };
    assert!(matches!(
        lower_to_physical(
            &missing,
            &meta,
            &nodes(1),
            &PhysicalPlannerConfig::default()
        ),
        Err(PlanError::MissingColumn { column: 3, .. })
    ));
    let dropped = WcolLogicalPlan {
        root: LogicalOp::Filter {
            predicates: vec![Predicate::Compare(Compare {
                column: 1,
                op: CmpOp::Eq,
                value: Literal::Int(1),
            })],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0],
            }),
        },
    };
    assert!(matches!(
        lower_to_physical(
            &dropped,
            &meta,
            &nodes(1),
            &PhysicalPlannerConfig::default()
        ),
        Err(PlanError::Unsupported(_))
    ));
    assert!(matches!(
        lower_to_physical(&example(), &meta, &[], &PhysicalPlannerConfig::default()),
        Err(PlanError::EmptyNodes)
    ));
}
